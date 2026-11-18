/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/import.hh"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <exception>
#include <new>
#include <random>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/switch_to.hh>
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "alternator/put_or_delete_item.hh"
#include "audit/audit.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/config.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/tags/extension.hh"
#include "db/tags/utils.hh"
#include "db_clock.hh"
#include "gms/gossiper.hh"
#include "replica/database.hh"
#include "service/client_state.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "tracing/trace_state.hh"
#include "utils/UUID.hh"
#include "utils/error_injection.hh"
#include "utils/rjson.hh"
#include "utils/utf8.hh"

namespace alternator {

static logging::logger ilogger("alternator-import");

// Each write of an import row sets this TTL: DynamoDB keeps the history of imports for 90 days.
static constexpr std::chrono::seconds import_row_ttl = std::chrono::days(90);

// The states of an import, see executor::s3_imports. An ImportTableDescription reports FAILING as
// IN_PROGRESS.
static constexpr std::string_view import_status_in_progress = "IN_PROGRESS";
static constexpr std::string_view import_status_failing = "FAILING";
static constexpr std::string_view import_status_completed = "COMPLETED";
static constexpr std::string_view import_status_failed = "FAILED";

// The FailureCodes of an import. Of these, only ItemValidationError is a FailureCode which DynamoDB
// documents.
static constexpr std::string_view failure_item_validation_error = "ItemValidationError";
static constexpr std::string_view failure_table_already_exists = "TableAlreadyExists";
static constexpr std::string_view failure_table_creation_failed = "TableCreationFailed";
static constexpr std::string_view failure_table_deleted = "TableDeleted";
static constexpr std::string_view failure_write_failed = "WriteFailed";
static constexpr std::string_view failure_import_interrupted = "ImportInterrupted";
static constexpr std::string_view failure_internal_error = "InternalError";
static constexpr std::string_view failure_not_implemented = "NotImplemented";

static bool is_terminal_status(std::string_view status) {
    return status == import_status_completed || status == import_status_failed;
}

// ClientToken must match the pattern DynamoDB documents for it, ^[^$]+$:
// at least one character, none of which is '$'. The caller already
// refuses an empty one. The token is also the partition key of the client
// token table, where a long key makes the insert fail, so its length (in
// bytes) is kept well below the limit of a key.
static constexpr size_t max_client_token_length = 1024;
static bool is_valid_client_token(std::string_view client_token) {
    return !client_token.contains('$') && client_token.size() <= max_client_token_length;
}

// Like ExportTableToPointInTime, only check that S3Bucket is a non-empty
// string, and leave validating the bucket name and the prefix to S3.
static std::pair<std::string, std::string> get_bucket_and_prefix(const rjson::value& request) {
    const rjson::value* s3_bucket_source = rjson::find(request, "S3BucketSource");
    if (!s3_bucket_source) {
        throw api_error::validation("Missing attribute S3BucketSource");
    }
    if (!s3_bucket_source->IsObject()) {
        throw api_error::validation("S3BucketSource attribute must be an object");
    }
    if (rjson::find(*s3_bucket_source, "S3BucketOwner")) {
        throw api_error::validation("S3BucketOwner attribute is not supported");
    }
    std::string s3_bucket = get_non_empty_string_attribute(*s3_bucket_source, "S3Bucket");
    std::string s3_key_prefix = get_string_attribute(*s3_bucket_source, "S3KeyPrefix", "");
    return {std::move(s3_bucket), std::move(s3_key_prefix)};
}

// Copy the members of TableCreationParameters which ImportTable accepts.
// DynamoDB drops every other member unread.
static rjson::value get_table_creation_parameters(const rjson::value& request) {
    const rjson::value* params = rjson::find(request, "TableCreationParameters");
    if (!params) {
        throw api_error::validation("Missing attribute TableCreationParameters");
    }
    if (!params->IsObject()) {
        throw api_error::validation("TableCreationParameters attribute must be an object");
    }
    static constexpr std::string_view accepted_members[] = {
            "TableName",
            "AttributeDefinitions",
            "KeySchema",
            "BillingMode",
            "ProvisionedThroughput",
            "OnDemandThroughput",
            "SSESpecification",
            "GlobalSecondaryIndexes",
            "VectorIndexes",
    };
    rjson::value accepted = rjson::empty_object();
    for (std::string_view name : accepted_members) {
        if (const rjson::value* member = rjson::find(*params, name)) {
            // The name is a string literal, so it lives longer than accepted.
            rjson::add(accepted, rjson::string_ref_type(name.data(), name.size()), rjson::copy(*member));
        }
    }
    // Unlike the dropped members, a GSI's WarmThroughput is refused, although
    // ImportTable's documented request syntax lists it.
    const rjson::value* gsis = rjson::find(accepted, "GlobalSecondaryIndexes");
    if (gsis && gsis->IsArray()) {
        for (const rjson::value& gsi : gsis->GetArray()) {
            if (gsi.IsObject() && rjson::find(gsi, "WarmThroughput")) {
                throw api_error::validation("WarmThroughput is not supported on Global Secondary Indexes via ImportTable");
            }
        }
    }
    return accepted;
}

import_table_request parse_import_table_request(const rjson::value& request, const gms::feature_service& feat, db::tablets_mode_t::mode tablets_mode) {
    // Documented as optional, but DynamoDB requires it.
    auto client_token = get_non_empty_string_attribute(request, "ClientToken");
    if (!is_valid_client_token(client_token)) {
        throw api_error::validation(fmt::format("ClientToken attribute: value does not match the required pattern ^[^$]+$, "
                "or is longer than {} bytes", max_client_token_length));
    }

    // FIXME: CSV and ION are not supported yet. A request using them is not
    // refused here, see import_table_request::unsupported.
    std::optional<std::string> unsupported;
    auto input_format = get_non_empty_string_attribute(request, "InputFormat");
    if (input_format == "CSV" || input_format == "ION") {
        unsupported = fmt::format("InputFormat attribute: {} is not supported", input_format);
    } else if (input_format != "DYNAMODB_JSON") {
        throw api_error::validation(fmt::format("InputFormat attribute: must be CSV, DYNAMODB_JSON or ION, not `{}`", input_format));
    }

    // An absent InputCompressionType means NONE.
    auto input_compression_type = get_non_empty_string_attribute(request, "InputCompressionType", "NONE");
    if (input_compression_type == "ZSTD") {
        if (!unsupported) {
            unsupported = "InputCompressionType attribute: ZSTD is not supported";
        }
    } else if (input_compression_type != "NONE" && input_compression_type != "GZIP") {
        throw api_error::validation(fmt::format("InputCompressionType attribute: must be GZIP, ZSTD or NONE, not `{}`", input_compression_type));
    }

    auto [s3_bucket, s3_key_prefix] = get_bucket_and_prefix(request);

    // InputFormatOptions only carries CSV options, which DynamoDB refuses
    // with DYNAMODB_JSON anyway.
    if (rjson::find(request, "InputFormatOptions")) {
        if (input_format == "DYNAMODB_JSON") {
            throw api_error::validation("InputFormatOptions attribute is not supported");
        }
        if (!unsupported) {
            unsupported = "InputFormatOptions attribute is not supported";
        }
    }

    auto table_creation_parameters = get_table_creation_parameters(request);
    auto validated_table_params = validate_create_table_request(table_creation_parameters, feat, tablets_mode, "ImportTable");

    return import_table_request {
        std::move(client_token),
        std::move(s3_bucket),
        std::move(s3_key_prefix),
        import_input_format::dynamodb_json,
        input_compression_type == "GZIP" ? import_input_compression_type::gzip : import_input_compression_type::none,
        std::move(table_creation_parameters),
        std::move(validated_table_params),
        std::move(unsupported),
    };
}

static std::string_view input_format_name(import_input_format input_format) {
    switch (input_format) {
        case import_input_format::dynamodb_json:
            return "DYNAMODB_JSON";
    }
    std::unreachable();
}

static std::string_view input_compression_type_name(import_input_compression_type input_compression_type) {
    switch (input_compression_type) {
        case import_input_compression_type::none:
            return "NONE";
        case import_input_compression_type::gzip:
            return "GZIP";
    }
    std::unreachable();
}

// The request stored in the import row and the client token row: only the members which
// ImportTable supports, validated and with the defaults filled in, so that the stored request is
// small and the import can rely on it.
static rjson::value make_stored_request(const import_table_request& parsed) {
    rjson::value s3_bucket_source = rjson::empty_object();
    rjson::add(s3_bucket_source, "S3Bucket", rjson::from_string(parsed.s3_bucket));
    if (!parsed.s3_key_prefix.empty()) {
        rjson::add(s3_bucket_source, "S3KeyPrefix", rjson::from_string(parsed.s3_key_prefix));
    }
    rjson::value request = rjson::empty_object();
    rjson::add(request, "S3BucketSource", std::move(s3_bucket_source));
    rjson::add(request, "InputFormat", rjson::from_string(input_format_name(parsed.input_format)));
    rjson::add(request, "InputCompressionType", rjson::from_string(input_compression_type_name(parsed.input_compression_type)));
    rjson::add(request, "TableCreationParameters", rjson::copy(parsed.table_creation_parameters));
    return request;
}

// True if a and b have the same regular columns. A put_or_delete_item stores an attribute as a
// regular column's value if the schema it was built with has that column, and in the :attrs map
// otherwise, so it can be built into a mutation of another schema only if they agree on these.
static bool have_same_regular_columns(const schema& a, const schema& b) {
    if (a.regular_columns_count() != b.regular_columns_count()) {
        return false;
    }
    for (const auto& column : a.regular_columns()) {
        const column_definition* other = b.get_column_definition(column.name());
        if (!other || !other->is_regular() || other->type != column.type) {
            return false;
        }
    }
    return true;
}

// The client token table has no table_id column, so the request stored in a client token row
// also holds the TableId of its import, for the answer to a replay before the import row is
// written. Returns the nil id if the request has no TableId which can be read - a corrupt row.
static ::table_id table_id_of_client_token_request(std::string_view request) {
    auto parsed = rjson::try_parse(request);
    const rjson::value* id = parsed ? rjson::find(*parsed, "TableId") : nullptr;
    if (!id || !id->IsString()) {
        return ::table_id();
    }
    try {
        return ::table_id(utils::UUID(rjson::to_string_view(*id)));
    } catch (...) {
        return ::table_id();
    }
}

// The stored request of an import row or a client token row, parsed. The request was validated
// when the import was accepted, so a request which cannot be read back is a corrupt row.
struct stored_import_request {
    rjson::value request;
    sstring s3_bucket;
    sstring s3_key_prefix;
    compression_type compression;
    sstring table_name;

    explicit stored_import_request(std::string_view stored) {
        try {
            request = rjson::parse(stored);
            const rjson::value& s3_bucket_source = rjson::get(request, "S3BucketSource");
            s3_bucket = sstring(rjson::to_string_view(rjson::get(s3_bucket_source, "S3Bucket")));
            if (const rjson::value* prefix = rjson::find(s3_bucket_source, "S3KeyPrefix")) {
                s3_key_prefix = sstring(rjson::to_string_view(*prefix));
            }
            if (rjson::to_string_view(rjson::get(request, "InputCompressionType")) == "GZIP") {
                compression = gzip_compression{};
            } else {
                compression = no_compression{};
            }
            table_name = sstring(rjson::to_string_view(rjson::get(rjson::get(request, "TableCreationParameters"), "TableName")));
        } catch (const rjson::error& e) {
            throw std::runtime_error(fmt::format("malformed stored ImportTable request: {}", e.what()));
        }
    }
};

// An import ARN is the ARN of its table, `/import/` and the import id.
static constexpr std::string_view import_arn_infix = "/import/";

// Generates an import id of DynamoDB's shape: the time in milliseconds since the epoch (14
// digits), a dash and 8 random hex digits, e.g. `01658528578619-c4d4e311`.
static sstring generate_import_id(db_clock::time_point accepted_at) {
    static thread_local std::mt19937 rng(std::random_device{}());
    auto epoch_millis = std::chrono::duration_cast<std::chrono::milliseconds>(accepted_at.time_since_epoch()).count();
    return fmt::format("{:014d}-{:08x}", epoch_millis, std::uint32_t(rng()));
}

// Returns the import id in an import ARN, or an empty view if the ARN has no `/import/`.
static std::string_view import_id_of_arn(std::string_view import_arn) {
    auto pos = import_arn.rfind(import_arn_infix);
    return pos == std::string_view::npos ? std::string_view() : import_arn.substr(pos + import_arn_infix.size());
}

// Returns the time an import was accepted from the id in its ARN (see generate_import_id()), for
// an import whose row is missing. Returns `fallback` for an ARN without a valid id.
static db_clock::time_point accept_time_of_import_arn(std::string_view import_arn, db_clock::time_point fallback) {
    auto import_id = import_id_of_arn(import_arn);
    std::int64_t epoch_millis = 0;
    auto [ptr, ec] = std::from_chars(import_id.data(), import_id.data() + import_id.size(), epoch_millis);
    if (ec != std::errc() || ptr == import_id.data() || ptr == import_id.data() + import_id.size() || *ptr != '-') {
        return fallback;
    }
    return db_clock::time_point(std::chrono::milliseconds(epoch_millis));
}

static double to_epoch_seconds(db_clock::time_point tp) {
    return std::chrono::duration<double>(tp.time_since_epoch()).count();
}

rjson::value make_import_table_description(const import_row& row) {
    stored_import_request stored(row.request);
    std::string_view import_arn = row.import_arn;
    auto table_arn = import_arn.substr(0, std::min(import_arn.size(), import_arn.rfind(import_arn_infix)));
    const bool terminal = is_terminal_status(row.import_status);

    rjson::value description = rjson::empty_object();
    rjson::add(description, "ImportArn", rjson::from_string(row.import_arn));
    // FAILING is not one of DynamoDB's ImportStatus values: report it as IN_PROGRESS.
    rjson::add(description, "ImportStatus", rjson::from_string(terminal ? std::string_view(row.import_status) : import_status_in_progress));
    rjson::add(description, "TableArn", rjson::from_string(table_arn));
    // The id is nil only when it comes from a client token row whose request has no TableId which
    // can be read (see table_id_of_client_token_request()): in the IN_PROGRESS answer to a replay
    // before the import row exists.
    if (row.table_id) {
        rjson::add(description, "TableId", rjson::from_string(row.table_id.to_sstring()));
    }
    rjson::add(description, "ClientToken", rjson::from_string(row.client_token));
    for (std::string_view member : {"S3BucketSource", "InputFormat", "InputCompressionType", "TableCreationParameters"}) {
        if (const rjson::value* value = rjson::find(stored.request, member)) {
            rjson::add_with_string_name(description, member, rjson::copy(*value));
        }
    }
    rjson::add(description, "StartTime", rjson::value(to_epoch_seconds(row.accepted_at)));
    if (terminal && row.completed_at) {
        rjson::add(description, "EndTime", rjson::value(to_epoch_seconds(*row.completed_at)));
    }
    // Alternator reports the counters as the import runs, while DynamoDB reports zeros and no
    // ProcessedSizeBytes until the import ends. ProcessedSizeBytes appears once the import first
    // wrote its progress.
    if (row.processed_size_bytes) {
        rjson::add(description, "ProcessedSizeBytes", rjson::value(*row.processed_size_bytes));
    }
    rjson::add(description, "ProcessedItemCount", rjson::value(row.processed_item_count));
    rjson::add(description, "ImportedItemCount", rjson::value(row.imported_item_count));
    rjson::add(description, "ErrorCount", rjson::value(row.error_count));
    if (row.import_status == import_status_failed) {
        if (row.failure_code) {
            rjson::add(description, "FailureCode", rjson::from_string(*row.failure_code));
        }
        if (row.failure_message) {
            rjson::add(description, "FailureMessage", rjson::from_string(*row.failure_message));
        }
    }
    return description;
}

static sstring make_import_table_response(const import_row& row) {
    rjson::value response = rjson::empty_object();
    rjson::add(response, "ImportTableDescription", make_import_table_description(row));
    return sstring(rjson::print(std::move(response)));
}

// --- The import metadata ---
// All updates of the import rows are conditional on the pair (import_status, node_id), and the
// inserts on the row not existing, so that
// only the owner of an import - the node which runs it, or the garbage collector which took it -
// changes its row.
// (A following commit adds the garbage collector.)

static future<std::optional<import_row>> insert_import(db::system_distributed_keyspace& sdks, const import_row& row) {
    db::alternator_import_table_helper helper(sdks.qp());
    co_return co_await helper.insert_import(row, import_row_ttl);
}

static future<bool> update_import(db::system_distributed_keyspace& sdks, const import_row& row, std::string_view expected_status,
        const sstring& expected_node_id) {
    db::alternator_import_table_helper helper(sdks.qp());
    co_return co_await helper.update_import(row, sstring(expected_status), expected_node_id, import_row_ttl);
}

static future<std::optional<import_row>> get_import(db::system_distributed_keyspace& sdks, const sstring& import_arn) {
    db::alternator_import_table_helper helper(sdks.qp());
    co_return co_await helper.get_import(import_arn);
}

// One attempt of writing the terminal state (COMPLETED or FAILED) of an import of this node: the
// row must be IN_PROGRESS, or FAILING for a FAILED row. If there is no row - the request path can
// fail before it inserts the row - inserts it. Returns false if the row exists but is not ours:
// the garbage collector took it, or an earlier attempt which timed out applied.
// (A following commit adds the garbage collector.)
static future<bool> try_write_final_import_row(db::system_distributed_keyspace& sdks, const import_row& row) {
    // Tests fail this write, to reach finish_import_in_background().
    utils::get_local_injector().inject("alternator_import_from_s3_final_write",
            [] { throw std::runtime_error("injected failure alternator_import_from_s3_final_write"); });
    if (co_await update_import(sdks, row, import_status_in_progress, row.node_id)) {
        co_return true;
    }
    if (row.import_status == import_status_failed && co_await update_import(sdks, row, import_status_failing, row.node_id)) {
        co_return true;
    }
    co_return !co_await insert_import(sdks, row);
}

// Writes the terminal state of an import of this node, retrying while the write throws: the
// garbage collector does not collect the rows of a node which can still run them, so without this
// write the row would stay IN_PROGRESS (or FAILING) while this incarnation of the node lives.
// Throws only if `as` is aborted (the node stops), and then a garbage collector collects the row
// once the node restarted, was unreachable for an hour, or was removed.
// (A following commit adds the garbage collector.)
static future<> write_final_import_row(db::system_distributed_keyspace& sdks, const import_row& row, abort_source& as) {
    constexpr std::chrono::seconds first_retry_delay{1};
    constexpr std::chrono::seconds max_retry_delay{60};
    for (auto delay = first_retry_delay; ; delay = std::min(delay * 2, max_retry_delay)) {
        try {
            if (!co_await try_write_final_import_row(sdks, row)) {
                ilogger.debug("Import {}: the write of the final state {} did not apply", row.import_arn, row.import_status);
            }
            co_return;
        } catch (...) {
            ilogger.warn("Import {}: failed to write the final state {}, retrying in {}s: {}",
                    row.import_arn, row.import_status, delay.count(), std::current_exception());
        }
        co_await seastar::sleep_abortable(delay, as);
    }
}

future<> executor::s3_imports::remove_import_in_progress_tag(::table_id table_id, sstring import_arn) {
    auto table = _e._proxy.data_dictionary().try_find_table(table_id);
    if (!table) {
        co_return;
    }
    auto tag = db::find_tag(*table->schema(), IMPORT_IN_PROGRESS_TAG_KEY);
    if (!tag || *tag != std::string_view(import_arn)) {
        co_return;
    }
    try {
        co_await _e.modify_tags(table->schema()->ks_name(), table->schema()->cf_name(), [&import_arn] (std::map<sstring, sstring>& tags) {
            if (auto it = tags.find(IMPORT_IN_PROGRESS_TAG_KEY); it != tags.end() && it->second == import_arn) {
                tags.erase(it);
            }
        });
    } catch (const data_dictionary::no_such_column_family&) {
        // The table was dropped meanwhile.
        co_return;
    }
    // As CreateTable and UpdateTable do (see wait_for_schema_agreement_after_ddl()), so that
    // DescribeTable on any node reports the table ACTIVE once the import has ended. The tag is
    // removed anyway, so a timeout is only logged.
    try {
        co_await _e._mm.wait_for_schema_agreement(_e._proxy.local_db(), db::timeout_clock::now() + std::chrono::seconds(30), &_e._stop_as);
    } catch (const service::migration_manager::schema_agreement_timeout&) {
        ilogger.warn("Import {}: removed the {} tag of its table, but the schema agreement after it timed out",
                import_arn, IMPORT_IN_PROGRESS_TAG_KEY);
    }
}

// --- The source of an import ---

namespace {

class in_memory_import_source : public import_source {
    std::vector<std::pair<sstring, std::shared_ptr<in_memory_test_storage>>> _objects;
    size_t _next = 0;
public:
    explicit in_memory_import_source(std::vector<std::pair<sstring, std::shared_ptr<in_memory_test_storage>>> objects)
        : _objects(std::move(objects)) {}
    future<std::optional<import_source_object>> next_object() override {
        if (_next == _objects.size()) {
            co_return std::nullopt;
        }
        const auto& [key, storage] = _objects[_next++];
        co_return import_source_object{ .key = key, .size = storage->data().size() };
    }
    future<std::unique_ptr<import_pipeline_interface>> open(const import_source_object& object,
            std::function<future<>(rjson::value)> on_item, compression_type compression) override {
        auto it = std::ranges::find(_objects, object.key, [] (const auto& o) { return o.first; });
        co_return co_await create_source_pipeline(in_memory_target_config{ it->second }, std::move(on_item), compression);
    }
    future<> close() override {
        return make_ready_future<>();
    }
};

} // anonymous namespace

std::unique_ptr<import_source> make_in_memory_import_source(
        std::vector<std::pair<sstring, std::shared_ptr<in_memory_test_storage>>> objects) {
    return std::make_unique<in_memory_import_source>(std::move(objects));
}

// The source of the imports until reading S3 is implemented: it fails every import before it
// creates the table.
// FIXME: list the objects under the prefix with s3::client::bucket_lister and read them with
// create_source_pipeline() (SCYLLADB-4046, SCYLLADB-4047).
static future<std::unique_ptr<import_source>> make_s3_import_source(sstring, sstring, abort_source&) {
    return make_exception_future<std::unique_ptr<import_source>>(import_failure(std::string(failure_not_implemented),
            "Reading the source of an import from S3 is not implemented yet."));
}

// --- The import itself ---

// The number of items which the import writes with one do_batch_write(), at most.
static constexpr size_t import_batch_size = 100;
// A batch is written once its items have this size (put_or_delete_item::length_in_bytes()), so
// that the memory which the batches of an import hold is bounded also for large items.
static constexpr uint64_t import_batch_bytes = 1024 * 1024;
// The number of batches which an import writes at the same time.
static constexpr size_t import_batches_in_flight = 4;
// The size (length_in_bytes() of their items) of the batches which an import writes at the same
// time, at most. A batch larger than this takes the whole budget, see flush_batch().
static constexpr uint64_t import_bytes_in_flight = import_batches_in_flight * import_batch_bytes;
// The number of attempts to write a batch, before the import fails with WriteFailed.
static constexpr int import_write_attempts = 4;
// The import writes its counters to its row after this many processed items, or after this much
// time, whichever comes first, so that the progress of the import can be seen.
static constexpr int64_t import_progress_items = 10000;
static constexpr std::chrono::seconds import_progress_period{10};
// An import whose writes of its progress keep failing for this long fails with ImportInterrupted:
// it cannot tell whether its row is still its own, and a garbage collector of another node (a
// following commit adds it) may take it once this node was unreachable for an hour. Kept well below
// that, it bounds the writes of items which an import cut off from its row makes after it was taken.
static constexpr std::chrono::minutes import_progress_write_failure_timeout{15};

// Thrown by the item callback when the rest of an object is not to be read: as DynamoDB does,
// after a line which is not an item.
struct abandon_object {};

// Thrown when a write of the import row did not apply because the row is not ours any more: the
// garbage collector took the import. The import stops: it starts no more writes of items and no
// longer writes its row, but still removes the import-in-progress tag of the table it created (see
// remove_tag_of_created_table()).
// (A following commit adds the garbage collector.)
struct import_row_taken {};

// Runs one import, see executor::s3_imports. A member of s3_imports, so that it can use the
// write path and the table creation of the executor.
class executor::s3_imports::import_task {
    executor& _e;
    abort_source& _as;
    import_row _row;
    import_source_factory _source_factory;
    // Parsed by run(), not by the constructor, so that a stored request which cannot be parsed - a
    // corrupt row - fails the import.
    std::optional<stored_import_request> _request;
    // The writes of the import do not belong to a user request.
    service::client_state _client_state{service::client_state::internal_tag{}};
    // The target table. Refreshed when its schema changes, see refresh_schema().
    schema_ptr _schema;
    std::optional<write_item_index_info> _index_info;
    // True if this import created the target table, and thus tagged it.
    bool _created_table = false;

    // Progress of reading the source: processed_size_bytes() of the objects already read, and the
    // pipeline of the object being read, if any.
    uint64_t _bytes_of_read_objects = 0;
    import_pipeline_interface* _pipeline = nullptr;
    // The number of the current line of the current object.
    uint64_t _line = 0;
    uint32_t _logged_errors = 0;

    // The batch being collected. An item with the key of an earlier one replaces it in place (see
    // _batch_positions, which maps the key of each item to its index): the last one wins.
    using builders = std::vector<std::pair<schema_ptr, put_or_delete_item>>;
    builders _batch;
    // The number of items added to the batch, also those which a later item with the key replaced.
    int64_t _batch_items = 0;
    uint64_t _batch_bytes = 0;
    // _batch_positions and _keys_in_flight are engaged by the first refresh_schema(): their hash
    // and equality need the schema of the target table, which may not exist before prepare_table().
    std::optional<std::unordered_map<primary_key, size_t, primary_key_hash, primary_key_equal>> _batch_positions;
    // The keys of the batches being written. An item with such a key waits for these writes, so
    // that the writes of one key are ordered as the items in the source.
    std::optional<std::unordered_set<primary_key, primary_key_hash, primary_key_equal>> _keys_in_flight;
    seastar::semaphore _write_slots{import_batches_in_flight};
    seastar::semaphore _write_byte_budget{import_bytes_in_flight};
    seastar::gate _writes;
    // The first failure of a write, which fails the import.
    std::exception_ptr _write_failure;

    // When maybe_write_progress() writes the counters next, unless import_progress_items items
    // come first.
    lowres_clock::time_point _next_progress_write = lowres_clock::now() + import_progress_period;
    int64_t _processed_at_last_progress_write = 0;
    // When the first of the writes of the progress which failed since the last one which applied
    // failed, see import_progress_write_failure_timeout. Counted from the first failure, not from
    // the last write which applied, so that one failure after a long time without a write (e.g. a
    // slow object) does not fail the import.
    std::optional<lowres_clock::time_point> _first_failed_progress_write;

    uint64_t processed_size_bytes() const noexcept {
        return _bytes_of_read_objects + (_pipeline ? _pipeline->processed_size_bytes() : 0);
    }

    // A logged reason is cut at this length, so that a long value which it quotes - e.g. a
    // malformed number, which can be as long as its line - does not flood the log.
    static constexpr size_t max_logged_reason_length = 512;
    // Returns the longest prefix of `s` of at most `max_length` bytes which does not end inside a
    // UTF-8 sequence, so that the cut does not make a valid string invalid UTF-8.
    static std::string_view truncate_at_utf8_boundary(std::string_view s, size_t max_length) {
        if (s.size() <= max_length) {
            return s;
        }
        size_t cut = max_length;
        while (cut > 0 && (static_cast<unsigned char>(s[cut]) & 0xc0) == 0x80) {
            --cut;
        }
        return s.substr(0, cut);
    }
    // Escapes as \xNN what could garble or forge a line of the log, as the object key and the
    // reason come from the user's data: control characters, DEL, the backslash (so that the escaped
    // form is unambiguous), and - if `s` is not valid UTF-8 - every non-ASCII byte.
    static std::string escape_for_log(std::string_view s) {
        const bool valid_utf8 = utils::utf8::validate(reinterpret_cast<const uint8_t*>(s.data()), s.size());
        std::string escaped;
        escaped.reserve(s.size());
        for (char c : s) {
            const auto u = static_cast<unsigned char>(c);
            if (u < 0x20 || u == 0x7f || c == '\\' || (u >= 0x80 && !valid_utf8)) {
                fmt::format_to(std::back_inserter(escaped), "\\x{:02x}", u);
            } else {
                escaped.push_back(c);
            }
        }
        return escaped;
    }
    // Counts an error of the import (ErrorCount) - an item which fails the validation, a line which
    // is not an item, or an object which cannot be read - and logs it with the object key and the
    // line. The reason may quote a value of the item (e.g. a malformed number); it is logged anyway,
    // cut at max_logged_reason_length and escaped by escape_for_log(). This is on purpose: the log
    // is the only place where the user finds why an item was skipped - DynamoDB writes these errors
    // to CloudWatch - and the object and line alone do not tell it.
    // alternator_import_max_logged_item_errors limits how many are logged.
    void count_error(std::string_view object_key, std::string_view reason) {
        ++_row.error_count;
        const auto max_logged = _e._proxy.data_dictionary().get_config().alternator_import_max_logged_item_errors();
        if (_logged_errors < max_logged) {
            ++_logged_errors;
            ilogger.warn("Import {}: object {}, line {}: {}{}", _row.import_arn, escape_for_log(object_key), _line,
                    escape_for_log(truncate_at_utf8_boundary(reason, max_logged_reason_length)),
                    reason.size() > max_logged_reason_length ? "..." : "");
            if (_logged_errors == max_logged) {
                ilogger.warn("Import {}: {} errors were logged, the following ones are only counted "
                        "(configuration variable alternator_import_max_logged_item_errors)", _row.import_arn, _logged_errors);
            }
        }
    }

    void rethrow_write_failure() const {
        if (_write_failure) {
            std::rethrow_exception(_write_failure);
        }
    }

    // Makes `_schema` the current schema of the target table: items are validated against it. Fails
    // the import with TableDeleted if no table has the import's table id any more.
    void refresh_schema() {
        auto table = _e._proxy.data_dictionary().try_find_table(_row.table_id);
        if (!table) {
            throw import_failure(std::string(failure_table_deleted), fmt::format("The table {} was deleted during the import.", _request->table_name));
        }
        if (!_schema || table->schema()->version() != _schema->version()) {
            // _schema changes only once _index_info is built: if building it throws, the next call
            // tries again instead of leaving _index_info empty for the new version.
            auto schema = table->schema();
            _index_info.emplace(_e._proxy, *schema);
            _schema = std::move(schema);
            // The key columns of a table do not change, so the hash and equality of its first schema
            // serve for all its versions.
            if (!_batch_positions) {
                _batch_positions.emplace(1, primary_key_hash{_schema}, primary_key_equal{_schema});
                _keys_in_flight.emplace(1, primary_key_hash{_schema}, primary_key_equal{_schema});
            }
        }
    }

    // Creates the target table from the TableCreationParameters of the request, with the id
    // allocated when the import was accepted and with the import-in-progress tag.
    future<> create_table() {
        const auto tablets_mode = _e._proxy.data_dictionary().get_config().tablets_mode_for_new_keyspaces();
        auto parameters = sstring(rjson::print(rjson::get(_request->request, "TableCreationParameters")));
        std::optional<std::pair<sstring, sstring>> error;
        std::exception_ptr ex;
        try {
            // FIXME: the table is created by an internal client, so the role which called
            // ImportTable (requester_role) is not given the permissions on it which CreateTable gives
            // to the role which creates a table (SCYLLADB-4045).
            error = co_await _e.container().invoke_on(0, [parameters = std::move(parameters), table_id = _row.table_id,
                    import_arn = _row.import_arn, tablets_mode] (executor& e) -> future<std::optional<std::pair<sstring, sstring>>> {
                rjson::value request = rjson::parse(parameters);
                std::optional<std::pair<sstring, sstring>> error;
                try {
                    auto validated = validate_create_table_request(request, e._proxy.features(), tablets_mode, "ImportTable");
                    validated.builder.set_uuid(table_id);
                    validated.tags_map[IMPORT_IN_PROGRESS_TAG_KEY] = import_arn;
                    validated.builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(validated.tags_map));
                    auto result = co_await e.commit_table_creation(std::move(request), std::move(validated),
                            service::client_state(service::client_state::internal_tag{}), tablets_mode);
                    if (auto* err = std::get_if<api_error>(&result)) {
                        error.emplace(err->_type, err->_msg);
                    }
                } catch (const api_error& err) {
                    // The cluster changed since the request was validated, e.g. its tablets mode.
                    error.emplace(err._type, err._msg);
                }
                co_return error;
            });
        } catch (...) {
            ex = std::current_exception();
        }
        // A creation which failed late, e.g. waiting for schema agreement, may have created the
        // table. The id was allocated for this import, so a table with it is ours.
        if (_e._proxy.data_dictionary().try_find_table(_row.table_id)) {
            _created_table = true;
            co_return;
        }
        // The creation succeeded, but there is no table with our id any more: it was dropped right
        // after it was created (DeleteTable is not blocked during an import).
        if (!ex && !error) {
            throw import_failure(std::string(failure_table_deleted),
                    fmt::format("The table {} was deleted during the import.", _request->table_name));
        }
        if (ex) {
            // FIXME: the schema change of the creation may still be applied later, e.g. after a
            // timeout or an abort at shutdown, leaving a table with the import-in-progress tag which
            // this import does not know of, and which nothing removes.
            std::rethrow_exception(ex);
        }
        if (error && error->first == "ResourceInUseException") {
            throw import_failure(std::string(failure_table_already_exists), fmt::format(
                    "The table {} was created by someone else after the import was accepted.", _request->table_name));
        }
        throw import_failure(std::string(failure_table_creation_failed), fmt::format("The table {} could not be created: {}",
                _request->table_name, error ? std::string_view(error->second) : std::string_view("unknown error")));
    }

    future<> prepare_table() {
        if (!_row.target_table_existed) {
            co_await create_table();
        }
        // refresh_schema() finds the target table by its id: a table which replaced the target under
        // the same name after the import was accepted is not written, the import fails with TableDeleted.
        refresh_schema();
    }

    // Writes one batch. Does not throw: a failure is kept in _write_failure - only the first one,
    // which the import reports - and fails the import. Writes the batch at most
    // import_write_attempts times, waiting 1, 2, 3... seconds between the attempts.
    future<> write_batch(builders batch, int64_t items) {
        for (int attempt = 1; ; ++attempt) {
            std::exception_ptr ex;
            try {
                utils::get_local_injector().inject("alternator_import_from_s3_write",
                        [] { throw std::runtime_error("injected failure alternator_import_from_s3_write"); });
                // The batch may have waited long, for a write slot or between attempts: write it
                // with the current schema, which decides its write isolation and whether it goes
                // to CDC, unless the regular columns changed (a CQL ALTER of the table) - then the
                // mutation is built with the old schema, with which its items were encoded, and the
                // replica upgrades it. This holds also for an LWT write which moves to the shard of
                // its partition, see cas_write().
                // The items stay validated against the schema they were built with, as in
                // BatchWriteItem: an item which an index added meanwhile (UpdateTable) would refuse
                // is written, as if the import wrote it just before the index was added.
                if (auto table = _e._proxy.data_dictionary().try_find_table(_row.table_id)) {
                    auto current = table->schema();
                    for (auto& [schema, _] : batch) {
                        if (have_same_regular_columns(*schema, *current)) {
                            schema = current;
                        }
                    }
                }
                // do_batch_write() takes the batch by value, so `batch` stays for a retry.
                co_await _e.do_batch_write(batch, _client_state, tracing::trace_state_ptr(), empty_service_permit());
                _row.imported_item_count += items;
                co_return;
            } catch (...) {
                ex = std::current_exception();
            }
            if (_as.abort_requested()) {
                _write_failure = _write_failure ? _write_failure : _as.abort_requested_exception_ptr();
                co_return;
            }
            if (!_e._proxy.data_dictionary().try_find_table(_row.table_id)) {
                _write_failure = _write_failure ? _write_failure : std::make_exception_ptr(import_failure(std::string(failure_table_deleted),
                        fmt::format("The table {} was deleted during the import.", _request->table_name)));
                co_return;
            }
            if (attempt == import_write_attempts) {
                _write_failure = _write_failure ? _write_failure : std::make_exception_ptr(import_failure(std::string(failure_write_failed),
                        fmt::format("Writing items to the table {} failed {} times, the last time with: {}", _request->table_name, attempt, ex)));
                co_return;
            }
            ilogger.warn("Import {}: writing {} items failed, retrying: {}", _row.import_arn, batch.size(), ex);
            try {
                co_await seastar::sleep_abortable(std::chrono::seconds(attempt), _as);
            } catch (...) {
                _write_failure = _write_failure ? _write_failure : std::current_exception();
                co_return;
            }
        }
    }

    // Starts writing the collected batch. Waits while `import_batches_in_flight` batches are
    // being written, or while the batches being written hold too much of import_bytes_in_flight
    // for this one. A batch larger than the whole budget waits for all of it, not for ever.
    future<> flush_batch() {
        if (_batch.empty()) {
            co_return;
        }
        auto units = co_await seastar::get_units(_write_slots, 1, _as);
        auto byte_units = co_await seastar::get_units(_write_byte_budget, std::min(_batch_bytes, import_bytes_in_flight), _as);
        rethrow_write_failure();
        std::vector<primary_key> keys;
        keys.reserve(_batch_positions->size());
        for (auto& [key, _] : *_batch_positions) {
            _keys_in_flight->insert(key);
            keys.push_back(key);
        }
        _batch_positions->clear();
        auto batch = std::exchange(_batch, builders());
        auto items = std::exchange(_batch_items, 0);
        _batch_bytes = 0;
        (void)with_gate(_writes, [this, batch = std::move(batch), items, keys = std::move(keys), units = std::move(units),
                byte_units = std::move(byte_units)] () mutable {
            // write_batch() keeps its failures in _write_failure itself; this catches what could
            // still escape it, e.g. bad_alloc, so that no batch is lost silently.
            return write_batch(std::move(batch), items).handle_exception([this] (std::exception_ptr ex) {
                if (!_write_failure) {
                    _write_failure = std::move(ex);
                }
            }).finally([this, keys = std::move(keys), units = std::move(units), byte_units = std::move(byte_units)] {
                for (const auto& key : keys) {
                    _keys_in_flight->erase(key);
                }
            });
        });
    }

    // Waits for the batches being written.
    future<> wait_for_writes() {
        // Taking all the slots waits until no batch is being written.
        auto units = co_await seastar::get_units(_write_slots, import_batches_in_flight, _as);
        rethrow_write_failure();
    }

    // Adds an item to the batch. An item with the key of one in the batch replaces it; an item with
    // the key of a batch being written first waits for all the writes in flight. Flushes the batch
    // when it took import_batch_size items - replaced ones included, so that a source repeating a
    // few keys is still written (and its progress shown) as it is read - or reached
    // import_batch_bytes.
    future<> add_to_batch(put_or_delete_item builder) {
        primary_key key{builder.pk(), builder.ck()};
        const auto bytes = builder.length_in_bytes();
        if (auto it = _batch_positions->find(key); it != _batch_positions->end()) {
            _batch_bytes -= _batch[it->second].second.length_in_bytes();
            _batch[it->second].second = std::move(builder);
        } else {
            if (_keys_in_flight->contains(key)) {
                co_await wait_for_writes();
            }
            _batch_positions->emplace(std::move(key), _batch.size());
            _batch.emplace_back(_schema, std::move(builder));
        }
        _batch_bytes += bytes;
        // ImportedItemCount counts every item with a duplicate key, also one which a later item
        // replaced in the batch and which is thus never written, so that ProcessedItemCount =
        // ImportedItemCount + item errors, as in DynamoDB.
        ++_batch_items;
        if (_batch_items >= int64_t(import_batch_size) || _batch_bytes >= import_batch_bytes) {
            co_await flush_batch();
        }
    }

    // Writes the counters to the import row, now and then: called after each item and after each
    // object.
    // The error injection makes it write the progress at each item, so that a test sees it while the
    // import runs.
    // This write is also the only check that the import is still ours. Nothing fences the writes of
    // items: an import which continues after the garbage collector took it - after a restart of
    // gossip on this node, or after a stall or a partition longer than node_unreachable_timeout() -
    // writes its batches into the table until this write finds the row taken, although the import
    // is already reported FAILED. An import which cannot write its row at all stops after
    // import_progress_write_failure_timeout.
    // (A following commit adds the garbage collector.)
    future<> maybe_write_progress() {
        if (lowres_clock::now() < _next_progress_write
                && _row.processed_item_count - _processed_at_last_progress_write < import_progress_items
                && !utils::get_local_injector().enter("alternator_import_from_s3_progress_at_each_item")) {
            co_return;
        }
        _next_progress_write = lowres_clock::now() + import_progress_period;
        _processed_at_last_progress_write = _row.processed_item_count;
        auto row = _row;
        row.processed_size_bytes = processed_size_bytes();
        bool ours;
        try {
            ours = co_await update_import(_e._sdks, row, import_status_in_progress, row.node_id);
        } catch (...) {
            const auto now = lowres_clock::now();
            if (!_first_failed_progress_write) {
                _first_failed_progress_write = now;
            } else if (now - *_first_failed_progress_write > import_progress_write_failure_timeout) {
                throw import_failure(std::string(failure_import_interrupted), fmt::format(
                        "The import could not write its progress for {} minutes, the last time with: {}",
                        import_progress_write_failure_timeout.count(), std::current_exception()));
            }
            // The next write of the progress may succeed.
            ilogger.debug("Import {}: failed to write the progress: {}", _row.import_arn, std::current_exception());
            co_return;
        }
        if (!ours) {
            throw import_row_taken();
        }
        _first_failed_progress_write.reset();
    }

    future<> on_item(std::string_view object_key, rjson::value line) {
        _as.check();
        rethrow_write_failure();
        _line = _pipeline->processed_lines();
        ++_row.processed_item_count;
        // Pauses the import at each item, so that a test can act while it runs, e.g. check that its
        // table is CREATING or drop the table. The wait ends on a message - each message lets one
        // item go on - when the injection is disabled, or when the node stops.
        co_await utils::get_local_injector().inject("alternator_import_from_s3_pause_in_item_handler",
                [this] (auto& handler) -> future<> {
                    co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{5}, &_as);
                }, false);
        const rjson::value* item = line.IsObject() ? rjson::find(line, "Item") : nullptr;
        if (!item) {
            // As in DynamoDB, a line without an item ends the reading of its object.
            count_error(object_key, "the line has no Item member, the rest of the object is skipped");
            throw abandon_object();
        }
        // Once per batch, not per item: a new schema rebuilds _index_info. So the items of a batch
        // are built with one schema, which add_to_batch() relies on when an item replaces another.
        if (_batch.empty()) {
            refresh_schema();
        }
        std::optional<put_or_delete_item> builder;
        std::string error;
        try {
            if (!item->IsObject()) {
                throw api_error::validation("Item must be an object");
            }
            // The validation of PutItem.
            builder.emplace(*item, _schema, put_or_delete_item::put_item{}, *_index_info);
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const api_error& e) {
            error = e._msg;
        } catch (const std::exception& e) {
            // Other errors of the validation, e.g. a malformed number, also skip only this item.
            error = e.what();
        }
        // The parsed line is not needed any more, and add_to_batch() may wait for writes.
        line.SetNull();
        if (builder) {
            co_await add_to_batch(std::move(*builder));
        } else {
            count_error(object_key, error);
        }
        co_await maybe_write_progress();
    }

    // True for the failures which end the import, rather than only the reading of an object.
    bool fails_the_import(std::exception_ptr ex) const {
        if (_as.abort_requested()) {
            return true;
        }
        try {
            std::rethrow_exception(ex);
        } catch (const import_failure&) {
            return true;
        } catch (const import_row_taken&) {
            return true;
        } catch (const abort_requested_exception&) {
            return true;
        } catch (const std::bad_alloc&) {
            return true;
        } catch (...) {
            return false;
        }
    }

    future<> import_object(import_source& source, const import_source_object& object) {
        if (object.size == 0) {
            co_return;
        }
        _line = 0;
        std::unique_ptr<import_pipeline_interface> pipeline;
        std::exception_ptr ex;
        try {
            pipeline = co_await source.open(object, [this, &object] (rjson::value line) {
                return on_item(object.key, std::move(line));
            }, _request->compression);
        } catch (...) {
            ex = std::current_exception();
        }
        if (pipeline) {
            _pipeline = pipeline.get();
            auto read = co_await coroutine::as_future(pipeline->read_all());
            auto closed = co_await coroutine::as_future(pipeline->close());
            _bytes_of_read_objects += pipeline->processed_size_bytes();
            // close() parses a last line without a newline: its number, for count_error().
            _line = pipeline->processed_lines();
            _pipeline = nullptr;
            if (read.failed()) {
                ex = read.get_exception();
                closed.ignore_ready_future();
            } else if (closed.failed()) {
                ex = closed.get_exception();
            }
        }
        if (!ex) {
            co_return;
        }
        if (fails_the_import(ex)) {
            std::rethrow_exception(ex);
        }
        try {
            std::rethrow_exception(ex);
        } catch (const abandon_object&) {
            // Already counted.
        } catch (const rjson::error& e) {
            // As in DynamoDB, a line which is not JSON counts as processed and as an error, and
            // ends the reading of its object.
            ++_row.processed_item_count;
            count_error(object.key, fmt::format("the line is not valid JSON, the rest of the object is skipped: {}", e.what()));
        } catch (const api_error& e) {
            if (e._type == "PayloadTooLarge") {
                // A line too long to be parsed is not an item either.
                ++_row.processed_item_count;
                count_error(object.key, fmt::format("the line is too long, the rest of the object is skipped: {}", e._msg));
            } else {
                // E.g. a corrupt gzip stream. The rest of the object cannot be read.
                count_error(object.key, fmt::format("the object cannot be read, the rest of it is skipped: {}", e._msg));
            }
        } catch (...) {
            // E.g. a failure to read the object. The rest of it cannot be read.
            count_error(object.key, fmt::format("the object cannot be read, the rest of it is skipped: {}", std::current_exception()));
        }
    }

    // Removes the import-in-progress tag of the table, if this import created it. Also when the garbage
    // collector took the import: it may have taken it before this import created the table, and then
    // found no tag to remove.
    // (A following commit adds the garbage collector.)
    future<> remove_tag_of_created_table() {
        if (!_created_table) {
            co_return;
        }
        try {
            co_await _e.container().invoke_on(0, [table_id = _row.table_id, import_arn = _row.import_arn] (executor& e) {
                return e._s3_imports->remove_import_in_progress_tag(table_id, import_arn);
            });
        } catch (...) {
            // The table stays CREATING for DescribeTable, but it is usable. A failure must not keep
            // run() from writing the final state of the import.
            // FIXME: nothing removes the tag then.
            ilogger.warn("Import {}: failed to remove the {} tag of the table: {}", _row.import_arn,
                    IMPORT_IN_PROGRESS_TAG_KEY, std::current_exception());
        }
    }

    // Reads the source and writes its items. Throws import_failure (or another exception) if the
    // import fails, and import_row_taken if the row is not ours any more. Sets `source` as soon as
    // it is made, so that run() closes it also when this throws.
    future<> import_all(std::unique_ptr<import_source>& source) {
        source = co_await _source_factory(_request->s3_bucket, _request->s3_key_prefix, _as);
        // The first object is listed before the table is created, so that a source which cannot
        // be listed leaves no table behind, as in DynamoDB.
        auto object = co_await source->next_object();
        co_await prepare_table();
        while (object) {
            co_await import_object(*source, *object);
            // Also for an object whose reading ended without reaching on_item(), e.g. at a first
            // line which is not JSON.
            co_await maybe_write_progress();
            object = co_await source->next_object();
        }
        co_await flush_batch();
        co_await wait_for_writes();
        // Fails the import with TableDeleted if its table was dropped after the last write, which
        // no write saw.
        refresh_schema();
    }

public:
    // Closes _writes unless run() did, e.g. because it threw: the gate must be closed - its write
    // fibers ended - before the task is destroyed. Called by start_import().
    future<> close_writes() noexcept {
        return _writes.is_closed() ? make_ready_future<>() : _writes.close();
    }

    import_task(executor& e, abort_source& as, import_row row, import_source_factory source_factory)
        : _e(e), _as(as), _row(std::move(row)), _source_factory(std::move(source_factory)) {}

    // Runs the import. Returns the row whose terminal state still has to be written, if that write
    // failed: the caller retries it in a background fiber (see finish_import_in_background()).
    future<std::optional<import_row>> run() {
        // An import is maintenance work, as an export is. The fiber starts in the scheduling
        // group of the request, so change it.
        co_await coroutine::switch_to(_e._proxy.local_db().get_export_import_scheduling_group());
        std::optional<std::pair<std::string, std::string>> failure;
        std::unique_ptr<import_source> source;
        try {
            _request.emplace(_row.request);
            co_await import_all(source);
        } catch (const import_row_taken&) {
            // An empty code: the garbage collector took the import, see below.
            // (A following commit adds the garbage collector.)
            failure.emplace(std::string(), std::string());
        } catch (const import_failure& e) {
            failure.emplace(e.code(), e.what());
        } catch (...) {
            // request_stop() aborts _stop_as, and with it the creation of the table, before stop()
            // aborts _as.
            if (_as.abort_requested() || _e._stop_as.abort_requested()) {
                failure.emplace(failure_import_interrupted, "The node which ran the import was shut down before the import could finish.");
            } else {
                failure.emplace(failure_internal_error, fmt::format("{}", std::current_exception()));
            }
        }
        if (source) {
            try {
                co_await source->close();
            } catch (...) {
                ilogger.warn("Import {}: failed to close the source: {}", _row.import_arn, std::current_exception());
            }
        }
        // A failed import still waits for the writes it started: they hold the batches, and the
        // final counters count them.
        co_await _writes.close();
        if (failure && failure->first.empty()) {
            ilogger.warn("Import {}: the garbage collector took the import, stopping", _row.import_arn);
            co_await remove_tag_of_created_table();
            co_return std::nullopt;
        }

        _row.processed_size_bytes = processed_size_bytes();
        if (failure) {
            ilogger.warn("Import {} failed with {}: {}", _row.import_arn, failure->first, failure->second);
            _row.failure_code = std::move(failure->first);
            _row.failure_message = std::move(failure->second);
            // FAILING keeps the failure in the row while the import cleans up, in case this node
            // stops meanwhile: the garbage collector then finishes the cleanup.
            // (A following commit adds the garbage collector.)
            _row.import_status = sstring(import_status_failing);
            try {
                if (!co_await update_import(_e._sdks, _row, import_status_in_progress, _row.node_id)) {
                    ilogger.warn("Import {}: the garbage collector took the import, stopping", _row.import_arn);
                    co_await remove_tag_of_created_table();
                    co_return std::nullopt;
                }
            } catch (...) {
                // The final write below tries both IN_PROGRESS and FAILING.
                ilogger.debug("Import {}: failed to write FAILING: {}", _row.import_arn, std::current_exception());
            }
            _row.import_status = sstring(import_status_failed);
        } else if (_row.error_count) {
            // Not through FAILING, which keeps a failure for the garbage collector: if this node stops
            // before the final write, the import ends as ImportInterrupted.
            // (A following commit adds the garbage collector.)
            _row.import_status = sstring(import_status_failed);
            _row.failure_code = sstring(failure_item_validation_error);
            _row.failure_message = "Some of the items failed validation checks and were not imported. "
                    "Please check the ScyllaDB log for more details.";
        } else {
            _row.import_status = sstring(import_status_completed);
        }
        co_await remove_tag_of_created_table();
        _row.completed_at = db_clock::now();
        try {
            if (!co_await try_write_final_import_row(_e._sdks, _row)) {
                ilogger.warn("Import {} ended as {}, but the garbage collector took the import before", _row.import_arn, _row.import_status);
            }
        } catch (...) {
            ilogger.warn("Import {}: failed to write the final state {}, retrying in the background: {}",
                    _row.import_arn, _row.import_status, std::current_exception());
            co_return std::move(_row);
        }
        ilogger.info("Import {} ended as {}: {} items processed, {} imported, {} errors", _row.import_arn, _row.import_status,
                _row.processed_item_count, _row.imported_item_count, _row.error_count);
        co_return std::nullopt;
    }
};

executor::s3_imports::s3_imports(executor& e)
    : _e(e)
    , _source_factory(make_s3_import_source)
{}

// Reads the live configuration on each call. If the limit decreases below the number of running
// imports, they continue.
auto executor::s3_imports::try_reserve_import_slot() -> std::optional<slot> {
    if (_reserved_import_slots >= _e._proxy.data_dictionary().get_config().alternator_max_concurrent_imports_per_shard()) {
        return std::nullopt;
    }
    return slot(_reserved_import_slots);
}

void executor::s3_imports::start_import(slot import_slot, import_row row) {
    auto import_arn = row.import_arn;
    // run() writes failures to the import row, so an exception from it is unexpected (e.g.
    // bad_alloc). The continuation holds the slot until run() ends, and passes it to
    // finish_import_in_background() if the final write is still to be done.
    // The task is allocated before the fiber, so that a failure to allocate it throws to the
    // caller.
    auto task = std::make_unique<import_task>(_e, _as, std::move(row), _source_factory);
    (void)with_gate(_gate, [&] {
        auto& task_ref = *task;
        return task_ref.run().then([this, import_slot = std::move(import_slot)] (std::optional<import_row> unwritten) mutable {
            if (unwritten) {
                finish_import_in_background(std::move(import_slot), std::move(*unwritten));
            }
        }).handle_exception([import_arn = std::move(import_arn)] (std::exception_ptr ep) {
            ilogger.error("Background fiber of import {} failed unexpectedly: {}", import_arn, ep);
        }).finally([task = std::move(task)] () mutable {
            // run() closed the task's gate already, unless it threw.
            auto& task_ref = *task;
            return task_ref.close_writes().finally([task = std::move(task)] {});
        });
    });
}

void executor::s3_imports::finish_import_in_background(slot import_slot, import_row row) {
    if (_gate.is_closed()) {
        // The executor stops. A garbage collector collects the row once the node restarted, was
        // unreachable for an hour, or was removed.
        // (A following commit adds the garbage collector.)
        return;
    }
    (void)with_gate(_gate, [&] {
        auto row_ptr = std::make_unique<import_row>(std::move(row));
        auto& row_ref = *row_ptr;
        // The caller may run in the scheduling group of a request.
        return with_scheduling_group(_e._proxy.local_db().get_export_import_scheduling_group(), [this, &row_ref] {
            return write_final_import_row(_e._sdks, row_ref, _as);
        }).handle_exception([&row_ref] (std::exception_ptr ep) {
            ilogger.warn("Background fiber finishing import {} gave up: {}", row_ref.import_arn, ep);
        }).finally([row_ptr = std::move(row_ptr), import_slot = std::move(import_slot)] {});
    });
}

future<> executor::s3_imports::stop() {
    _as.request_abort();
    co_await _gate.close();
}

// --- ImportTable ---

future<executor::request_return_type> executor::import_table(service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.import_table++;
    // Audit before the rest of validation, so that requests failing it are
    // audited too. As in CreateTable, a request with an invalid TableName is
    // rejected before it is audited, and one without a TableName is not
    // audited: it always fails the validation below.
    if (const rjson::value* params = rjson::find(request, "TableCreationParameters"); params && params->IsObject()) {
        if (const rjson::value* name = rjson::find(*params, "TableName"); name && name->IsString()) {
            std::string table_name = rjson::to_string(*name);
            validate_table_name(table_name);
            maybe_audit(audit_info, audit::statement_category::DDL, executor::KEYSPACE_NAME_PREFIX + table_name, table_name, "ImportTable", request);
            tracing::add_alternator_table_name(trace_state, table_name);
        }
    }

    // FIXME: check permissions (SCYLLADB-4045): CREATE for a new table,
    // MODIFY on an existing one. Check them before the ClientToken lookup,
    // so that a replay, which answers with the stored import, is checked too.

    // As on DynamoDB, everything is validated before the ClientToken is
    // looked up: a replay with invalid parameters fails even if its token
    // is known. Refused only after the lookup, because a replay starts no
    // import: what Alternator does not support yet (see
    // import_table_request::unsupported), the cluster-dependent checks of
    // check_new_table(), the shutdown refusal and the concurrency limit.
    const auto tablets_mode = _proxy.data_dictionary().get_config().tablets_mode_for_new_keyspaces();
    import_table_request parsed = parse_import_table_request(request, _proxy.features(), tablets_mode);
    const auto& keyspace_name = parsed.validated_table_params.keyspace_name;
    const auto& table_name = parsed.validated_table_params.table_name;
    const sstring client_token(parsed.client_token);
    const sstring stored_request(rjson::print(make_stored_request(parsed)));

    // A replay of a ClientToken returns the import of the token, unless it names another table:
    // DynamoDB compares nothing else.
    auto describe_token_import = [&] (const db::alternator_import_table_helper::client_token_row& ct) -> future<sstring> {
        auto stored = rjson::try_parse(ct.request);
        const rjson::value* params = stored ? rjson::find(*stored, "TableCreationParameters") : nullptr;
        const rjson::value* stored_table_name = params ? rjson::find(*params, "TableName") : nullptr;
        if (!stored_table_name || rjson::to_string_view(*stored_table_name) != table_name) {
            throw api_error::import_conflict("Duplicate request detected with conflicting parameters: "
                    "an import with this ClientToken was already accepted for a different table");
        }
        auto row = co_await get_import(_sdks, ct.import_arn);
        if (!row) {
            // The node which accepted the import did not write its row yet, or it stopped before it
            // did. Report the import as IN_PROGRESS.
            // FIXME: the import of a node which stopped before it wrote the import row stays
            // IN_PROGRESS, until the client token row expires.
            row = import_row{
                .import_arn = ct.import_arn,
                .client_token = ct.client_token,
                .request = ct.request,
                .table_id = table_id_of_client_token_request(ct.request),
                .import_status = sstring(import_status_in_progress),
                .accepted_at = accept_time_of_import_arn(ct.import_arn, db_clock::now()),
                .node_id = ct.node_id,
            };
        }
        co_return make_import_table_response(*row);
    };

    db::alternator_import_table_helper helper(_sdks.qp());
    // A replay does not start an import, so answer it before the checks below. If another request
    // inserts the token row after this read, the insert below finds it.
    if (auto ct = co_await helper.get_client_token(client_token)) {
        co_return co_await describe_token_import(*ct);
    }
    if (parsed.unsupported) {
        co_return api_error::validation(*parsed.unsupported);
    }
    // While the server drains its requests at shutdown, a new import would fail at once and use up
    // its ClientToken: refuse it with an error which the client retries, on another node.
    if (_stop_as.abort_requested()) {
        co_return api_error::internal("This node is shutting down, so it can't start an import. Please retry the operation.");
    }
    // Checked after the replay, which the node answers without owning an import.
    auto node_id_opt = get_self_node_id(_gossiper);
    if (!node_id_opt) {
        // Without an identifier, this node cannot own the import.
        co_return api_error::internal("This node is not gossiping, so it can't start an import. Please retry the operation.");
    }
    const sstring node_id = node_id_opt->to_string();

    // An import into an existing table is an Alternator extension: DynamoDB refuses it with
    // ResourceInUseException. The live table's schema governs the import, and the table keeps its
    // status. A table created later with the same name is not the target: the id tells.
    // FIXME: consider refusing an existing table whose key schema differs from the requested one.
    bool target_table_existed = false;
    ::table_id table_id;
    if (auto table = _proxy.data_dictionary().try_find_table(keyspace_name, table_name)) {
        target_table_existed = true;
        table_id = table->schema()->id();
    } else {
        // Refuse now what the creation of the table would refuse. The creation checks it again,
        // because the cluster can change before the table is created.
        if (auto error = check_new_table(parsed.validated_table_params, tablets_mode)) {
            co_return std::move(*error);
        }
        // The id of the table which the import will create: DynamoDB reports TableId from the start.
        table_id = parsed.validated_table_params.builder.uuid();
    }

    // Reserve the slot before the first write, so that a refused request writes no rows. If the
    // slot is given neither to start_import() nor to finish_import_in_background(), it is released
    // when this function returns.
    auto import_slot = _s3_imports->try_reserve_import_slot();
    if (!import_slot) {
        co_return api_error::limit_exceeded(fmt::format(
                "Too many imports are already running - at most {} of them may run at once on one shard "
                "(configuration variable alternator_max_concurrent_imports_per_shard). Please retry the operation later.",
                _proxy.data_dictionary().get_config().alternator_max_concurrent_imports_per_shard()));
    }

    const auto accepted_at = db_clock::now();
    const auto import_id = generate_import_id(accepted_at);
    rjson::value table_arn = generate_arn_for_table(keyspace_name, table_name);
    const sstring import_arn = fmt::format("{}{}{}", rjson::to_string_view(table_arn), import_arn_infix, import_id);
    // The client token table has no table_id column, so the TableId goes into its request, see
    // table_id_of_client_token_request().
    rjson::value client_token_request = make_stored_request(parsed);
    rjson::add(client_token_request, "TableId", rjson::from_string(table_id.to_sstring()));
    const auto& user = client_state.user();
    import_row row{
        .import_arn = import_arn,
        .client_token = client_token,
        .request = stored_request,
        .requester_role = user && user->name ? *user->name : sstring(),
        .table_id = table_id,
        .target_table_existed = target_table_existed,
        .import_status = sstring(import_status_in_progress),
        .import_id_token = import_id,
        .accepted_at = accepted_at,
        .node_id = node_id,
    };

    // FIXME: if the request fails after its insert of the client token row applied, no import
    // row is written, and a replay of the token reports the import as IN_PROGRESS until the
    // token row expires.
    auto existing_client_token = co_await helper.insert_client_token(db::alternator_import_table_helper::client_token_row{
        .client_token = client_token,
        .import_arn = import_arn,
        .request = sstring(rjson::print(client_token_request)),
        .node_id = node_id,
    });
    // A concurrent CAS can commit our value and report it as not applied, so a token row with
    // our ARN - which only this request knows - is ours.
    if (existing_client_token && existing_client_token->import_arn != import_arn) {
        co_return co_await describe_token_import(*existing_client_token);
    }
    auto existing_import = co_await insert_import(_sdks, row);
    if (existing_import && (existing_import->client_token != client_token || existing_import->import_status != import_status_in_progress
            || existing_import->node_id != node_id)) {
        // Not our row: the garbage collector took the import (if this node was unreachable for
        // long), or another import got the same id. Report it, without running anything.
        // (A following commit adds the garbage collector.)
        co_return make_import_table_response(*existing_import);
    }
    // Build the response before the fiber starts: a failure after that must not fail the
    // import which the fiber runs.
    auto response = make_import_table_response(row);
    _s3_imports->start_import(std::move(*import_slot), row);
    co_return response;
}

} // namespace alternator
