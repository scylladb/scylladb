/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/import.hh"

#include <algorithm>
#include <chrono>
#include <random>
#include <string_view>
#include <utility>
#include <seastar/core/coroutine.hh>
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "audit/audit.hh"
#include "db/system_distributed_keyspace.hh"
#include "db_clock.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "tracing/trace_state.hh"
#include "utils/UUID.hh"
#include "utils/rjson.hh"

namespace alternator {

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
// refuses an empty one.
static bool is_valid_client_token(std::string_view client_token) {
    return !client_token.contains('$');
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
        throw api_error::validation("ClientToken attribute: value does not match the required pattern ^[^$]+$");
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
    // FIXME: commit_table_creation()'s checks of the table's keyspace (vector indexes need
    // tablets, RF-rack validity, object storage) are deferred until the table is created.

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

rjson::value make_import_table_description(const import_row& row) {
    stored_import_request stored(row.request);
    std::string_view import_arn = row.import_arn;
    // The ARN without `/import/<id>`, or all of it if it has no `/import/` (rfind() returns npos).
    auto table_arn = import_arn.substr(0, std::min(import_arn.size(), import_arn.rfind(import_arn_infix)));
    const bool terminal = is_terminal_status(row.import_status);

    rjson::value description = rjson::empty_object();
    rjson::add(description, "ImportArn", rjson::from_string(row.import_arn));
    // FAILING is not one of DynamoDB's ImportStatus values: report it as IN_PROGRESS.
    rjson::add(description, "ImportStatus", rjson::from_string(terminal ? std::string_view(row.import_status) : import_status_in_progress));
    rjson::add(description, "TableArn", rjson::from_string(table_arn));
    rjson::add(description, "TableId", rjson::from_string(row.table_id.to_sstring()));
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

future<std::optional<executor::request_return_type>> get_in_progress_import_description(std::string_view client_token) {
    // FIXME: there is no persistent import metadata yet.
    // Cannot determine if the import with the same token is
    // already running.
    co_return std::nullopt;
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
    // MODIFY on an existing one.

    // As on DynamoDB, everything is validated before the ClientToken is
    // looked up: a replay with invalid parameters fails even if its token
    // is known.
    import_table_request parsed = parse_import_table_request(request, _proxy.features(),
            _proxy.data_dictionary().get_config().tablets_mode_for_new_keyspaces());

    // If there is an import with this token return it, even when parameters differ.
    auto import_description = co_await get_in_progress_import_description(parsed.client_token);
    if (import_description) {
        co_return std::move(*import_description);
    }

    // FIXME: above we return if an import with this token has
    // started (or the token is inside the 8h idempotency window).
    // Here we need to create a table if not exists and start the
    // import job. For alternator extension we allow import into
    // existing table. This handler runs on whichever shard received
    // the request, while tables are created on shard 0, as in
    // create_table_on_shard0().

    // FIXME: consider api::error when table exists but request
    // has TableCreationParameters.

    // FIXME: the import itself is not implemented yet, so every accepted
    // import fails immediately, like ExportTableToPointInTime's placeholder.
    // The import is not saved yet, so DescribeImport, ListImports and a
    // ClientToken replay cannot find it.
    const auto accepted_at = db_clock::now();
    const auto import_id = generate_import_id(accepted_at);
    rjson::value table_arn = generate_arn_for_table(parsed.validated_table_params.keyspace_name, parsed.validated_table_params.table_name);
    import_row row{
        .import_arn = fmt::format("{}{}{}", rjson::to_string_view(table_arn), import_arn_infix, import_id),
        .client_token = sstring(parsed.client_token),
        .request = sstring(rjson::print(make_stored_request(parsed))),
        // The builder's id becomes the table's id once the table is created.
        // FIXME: an import into an existing table must report that table's id,
        // not a freshly allocated one.
        .table_id = parsed.validated_table_params.builder.uuid(),
        .import_status = sstring(import_status_failed),
        .failure_code = sstring(failure_not_implemented),
        .failure_message = "Not yet implemented - this is a placeholder response for testing the import API.",
        .import_id_token = import_id,
        .accepted_at = accepted_at,
    };
    co_return make_import_table_response(row);
}

} // namespace alternator
