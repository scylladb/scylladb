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
#include <random>
#include <string_view>
#include <utility>
#include <seastar/core/coroutine.hh>
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "audit/audit.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/config.hh"
#include "db/system_distributed_keyspace.hh"
#include "db_clock.hh"
#include "gms/gossiper.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "tracing/trace_state.hh"
#include "utils/UUID.hh"
#include "utils/rjson.hh"

namespace alternator {

// Each write of an import row sets this TTL: DynamoDB keeps the history of imports for 90 days.
static constexpr std::chrono::seconds import_row_ttl = std::chrono::days(90);

// The states of an import. An ImportTableDescription reports FAILING as IN_PROGRESS.
static constexpr std::string_view import_status_in_progress = "IN_PROGRESS";
static constexpr std::string_view import_status_completed = "COMPLETED";
static constexpr std::string_view import_status_failed = "FAILED";

// The FailureCodes of an import. DynamoDB documents none of these.
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

    // FIXME: CSV and ION are not supported yet.
    auto input_format = get_non_empty_string_attribute(request, "InputFormat");
    if (input_format == "CSV" || input_format == "ION") {
        throw api_error::validation(fmt::format("InputFormat attribute: {} is not supported", input_format));
    }
    if (input_format != "DYNAMODB_JSON") {
        throw api_error::validation(fmt::format("InputFormat attribute: must be CSV, DYNAMODB_JSON or ION, not `{}`", input_format));
    }

    // An absent InputCompressionType means NONE.
    auto input_compression_type = get_non_empty_string_attribute(request, "InputCompressionType", "NONE");
    if (input_compression_type == "ZSTD") {
        throw api_error::validation("InputCompressionType attribute: ZSTD is not supported");
    }
    if (input_compression_type != "NONE" && input_compression_type != "GZIP") {
        throw api_error::validation(fmt::format("InputCompressionType attribute: must be GZIP, ZSTD or NONE, not `{}`", input_compression_type));
    }

    auto [s3_bucket, s3_key_prefix] = get_bucket_and_prefix(request);

    // InputFormatOptions only carries CSV options, which DynamoDB refuses
    // with DYNAMODB_JSON anyway.
    if (rjson::find(request, "InputFormatOptions")) {
        throw api_error::validation("InputFormatOptions attribute is not supported");
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
    // ProcessedSizeBytes appears once the import wrote it, as DynamoDB's does.
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
// The inserts of the import rows are conditional on the row not existing.

static future<std::optional<import_row>> insert_import(db::system_distributed_keyspace& sdks, const import_row& row) {
    db::alternator_import_table_helper helper(sdks.qp());
    co_return co_await helper.insert_import(row, import_row_ttl);
}

static future<std::optional<import_row>> get_import(db::system_distributed_keyspace& sdks, const sstring& import_arn) {
    db::alternator_import_table_helper helper(sdks.qp());
    co_return co_await helper.get_import(import_arn);
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
    // import: the cluster-dependent checks of check_new_table().
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

    const auto accepted_at = db_clock::now();
    const auto import_id = generate_import_id(accepted_at);
    rjson::value table_arn = generate_arn_for_table(keyspace_name, table_name);
    const sstring import_arn = fmt::format("{}{}{}", rjson::to_string_view(table_arn), import_arn_infix, import_id);
    // The client token table has no table_id column, so the TableId goes into its request, see
    // table_id_of_client_token_request().
    rjson::value client_token_request = make_stored_request(parsed);
    rjson::add(client_token_request, "TableId", rjson::from_string(table_id.to_sstring()));
    const auto& user = client_state.user();
    // FIXME: the import is not run yet, so it is stored as FAILED, as the placeholder answered.
    import_row row{
        .import_arn = import_arn,
        .client_token = client_token,
        .request = stored_request,
        .requester_role = user && user->name ? *user->name : sstring(),
        .table_id = table_id,
        .target_table_existed = target_table_existed,
        .import_status = sstring(import_status_failed),
        .failure_code = sstring(failure_not_implemented),
        .failure_message = "Not yet implemented - this is a placeholder response for testing the import API.",
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
    if (existing_import && (existing_import->client_token != client_token || existing_import->import_status != import_status_failed
            || existing_import->node_id != node_id)) {
        // Not our row: another import got the same id. Report it.
        co_return make_import_table_response(*existing_import);
    }
    co_return make_import_table_response(row);
}

} // namespace alternator
