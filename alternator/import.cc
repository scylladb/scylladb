/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/import.hh"

#include <array>
#include <chrono>
#include <cmath>
#include <utility>
#include <seastar/core/coroutine.hh>
#include "alternator/error.hh"
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "audit/audit.hh"
#include "db_clock.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "tracing/trace_state.hh"
#include "utils/UUID.hh"
#include "utils/rjson.hh"

namespace alternator {

future<std::optional<executor::request_return_type>> get_in_progress_import_description(std::string_view client_token) {
    // FIXME: there is no persistent import metadata yet.
    // Cannot determine if the import with the same token is
    // already running.
    co_return std::nullopt;
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
    constexpr std::array accepted_members{
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
            rjson::add_with_string_name(accepted, name, rjson::copy(*member));
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

rjson::value make_import_table_description(const import_table_request& parsed) {
    const auto now = db_clock::now();
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    // An import id looks like DynamoDB's: the start time in milliseconds and 8 random hex digits.
    const auto import_id = fmt::format("{:014}-{:08x}", now_ms,
            uint32_t(utils::make_random_uuid().get_least_significant_bits()));
    rjson::value table_arn = generate_arn_for_table(parsed.validated_table_params.keyspace_name, parsed.validated_table_params.table_name);
    // An import ARN is its table's ARN with the import id appended.
    rjson::value import_arn = rjson::from_string(fmt::format("{}/import/{}", rjson::to_string_view(table_arn), import_id));

    rjson::value s3_bucket_source = rjson::empty_object();
    rjson::add(s3_bucket_source, "S3Bucket", rjson::from_string(parsed.s3_bucket));
    if (!parsed.s3_key_prefix.empty()) {
        rjson::add(s3_bucket_source, "S3KeyPrefix", rjson::from_string(parsed.s3_key_prefix));
    }

    rjson::value description = rjson::empty_object();
    rjson::add(description, "ImportArn", std::move(import_arn));
    // FIXME: the import itself is not implemented yet, so every accepted
    // import fails immediately, like ExportTableToPointInTime's placeholder.
    rjson::add(description, "ImportStatus", "FAILED");
    rjson::add(description, "FailureCode", "NotImplemented");
    rjson::add(description, "FailureMessage", "Not yet implemented - this is a placeholder response for testing the import API.");
    rjson::add(description, "TableArn", std::move(table_arn));
    // The builder's id becomes the table's id once the table is created.
    // FIXME: an import into an existing table must report that table's id,
    // not a freshly allocated one.
    rjson::add(description, "TableId", rjson::from_string(parsed.validated_table_params.builder.uuid().to_sstring()));
    rjson::add(description, "ClientToken", rjson::from_string(parsed.client_token));
    rjson::add(description, "S3BucketSource", std::move(s3_bucket_source));
    rjson::add(description, "InputFormat", rjson::from_string(input_format_name(parsed.input_format)));
    rjson::add(description, "InputCompressionType", rjson::from_string(input_compression_type_name(parsed.input_compression_type)));
    rjson::add(description, "TableCreationParameters", rjson::copy(parsed.table_creation_parameters));
    rjson::add(description, "StartTime", rjson::value(std::chrono::duration<double>(now.time_since_epoch()).count()));
    rjson::add(description, "ProcessedItemCount", rjson::value(0));
    rjson::add(description, "ImportedItemCount", rjson::value(0));
    rjson::add(description, "ErrorCount", rjson::value(0));
    return description;
}

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

    // FIXME: the description is not saved yet, so DescribeImport,
    // ListImports and a ClientToken replay cannot find this import.
    rjson::value response = rjson::empty_object();
    rjson::add(response, "ImportTableDescription", make_import_table_description(parsed));
    co_return rjson::print(std::move(response));
}

// A copy of the export.cc function: as for the export operations, DynamoDB reports a malformed ARN as
// ValidationException, while parse_arn reports some malformed ARNs as AccessDeniedException.
static arn_parts parse_arn_reporting_validation(std::string_view arn, std::string_view arn_field_name, std::string_view type_name, std::string_view expected_postfix) {
    try {
        return parse_arn(arn, arn_field_name, type_name, expected_postfix);
    } catch (const api_error& e) {
        if (e._type == "AccessDeniedException") {
            throw api_error::validation(e._msg);
        }
        throw;
    }
}

// An import ARN is a table ARN followed by `/import/<id>`. DynamoDB requires the id to have the shape
// of the ids ImportTable hands out: 14 digits, a dash and 8 hex digits of either case.
static arn_parts parse_import_arn(std::string_view import_arn) {
    static constexpr std::string_view import_infix = "/import/";
    auto parts = parse_arn_reporting_validation(import_arn, "ImportArn", "Import", import_infix);
    auto import_id = parts.postfix.substr(import_infix.size());
    if (import_id.size() != 23 || import_id[14] != '-'
            || import_id.substr(0, 14).find_first_not_of("0123456789") != std::string_view::npos
            || import_id.substr(15).find_first_not_of("0123456789abcdefABCDEF") != std::string_view::npos) {
        throw api_error::validation(fmt::format("ImportArn: Invalid Import ARN `{}` - the import id is not 14 digits, a dash and 8 hex digits",
                import_arn));
    }
    return parts;
}

future<executor::request_return_type> executor::describe_import(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_import++;

    // DynamoDB reports an ImportArn of the wrong JSON type as SerializationException.
    if (const rjson::value* import_arn_v = rjson::find(request, "ImportArn"); import_arn_v && !import_arn_v->IsNull() && !import_arn_v->IsString()) {
        co_return api_error::serialization("ImportArn must be a string");
    }
    auto import_arn = get_non_empty_string_attribute(request, "ImportArn");
    auto parts = parse_import_arn(import_arn);

    // DynamoDB answers ValidationException when no table can have the name the ARN gives, but
    // ImportNotFoundException when the ARN names a table it does not have.
    if (parts.keyspace_name.empty()) {
        co_return api_error::validation(fmt::format("ImportArn: Invalid Import ARN `{}` - empty keyspace name", import_arn));
    }
    validate_table_name(parts.table_name, "ImportArn: Invalid Import ARN - table name");

    maybe_audit(audit_info, audit::statement_category::QUERY, parts.keyspace_name, parts.table_name, "DescribeImport", request);

    // FIXME: check SELECT on the target table (SCYLLADB-4045).

    // Only an existing Alternator table has per-table metrics. DynamoDB also describes the imports of
    // a dropped table.
    if (is_alternator_keyspace(parts.keyspace_name)) {
        if (auto table = _proxy.data_dictionary().try_find_table(parts.keyspace_name, parts.table_name)) {
            get_stats_from_schema(_proxy, *table->schema())->api_operations.describe_import++;
        }
    }

    // FIXME: describe the import from its row in the imports table of SCYLLADB-4041 (SCYLLADB-4043). An
    // import which a dead node left IN_PROGRESS stays so until the garbage collection of SCYLLADB-4044 fails it.
    co_return api_error::import_not_found(fmt::format("ImportArn: Import `{}` not found", import_arn));
}

future<executor::request_return_type> executor::list_imports(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.list_imports++;

    // DynamoDB returns at most 25 summaries per page.
    static constexpr int max_page_size = 25;

    // DynamoDB ignores a null parameter and reports a parameter of the wrong JSON type as
    // SerializationException.
    if (const rjson::value* page_size_v = rjson::find(request, "PageSize"); page_size_v && !page_size_v->IsNull()) {
        if (!page_size_v->IsNumber()) {
            co_return api_error::serialization("PageSize must be an integer");
        }
        // DynamoDB truncates a fractional PageSize.
        auto page_size = std::trunc(page_size_v->GetDouble());
        if (page_size < 1 || page_size > max_page_size) {
            co_return api_error::validation(fmt::format("PageSize must be between 1 and {} inclusive", max_page_size));
        }
    }

    std::optional<arn_parts> table_parts;
    if (const rjson::value* table_arn_v = rjson::find(request, "TableArn"); table_arn_v && !table_arn_v->IsNull()) {
        if (!table_arn_v->IsString()) {
            co_return api_error::serialization("TableArn must be a string");
        }
        auto table_arn = rjson::to_string_view(*table_arn_v);
        if (table_arn.empty() || table_arn.size() > 1024) {
            co_return api_error::validation("TableArn must be between 1 and 1024 characters long");
        }
        table_parts = parse_arn_reporting_validation(table_arn, "TableArn", "table", "");
        // DynamoDB reports a TableArn whose table name no table can have as ValidationException.
        if (table_parts->keyspace_name.empty()) {
            co_return api_error::validation(fmt::format("TableArn: Invalid Table ARN `{}` - empty keyspace name", table_arn));
        }
        validate_table_name(table_parts->table_name, "TableArn: Invalid Table ARN - table name");
    }

    if (const rjson::value* next_token_v = rjson::find(request, "NextToken"); next_token_v && !next_token_v->IsNull()) {
        if (!next_token_v->IsString()) {
            co_return api_error::serialization("NextToken must be a string");
        }
        // DynamoDB requires a NextToken of 112 to 1024 characters matching ([0-9a-f]{16})+.
        auto next_token = rjson::to_string_view(*next_token_v);
        if (next_token.size() < 112 || next_token.size() > 1024) {
            co_return api_error::validation("NextToken must be between 112 and 1024 characters long");
        }
        if (next_token.size() % 16 != 0 || next_token.find_first_not_of("0123456789abcdef") != std::string_view::npos) {
            co_return api_error::validation("NextToken must match the pattern ([0-9a-f]{16})+");
        }
        // FIXME: resume the listing where the page of the NextToken ended (SCYLLADB-4043). No page hands
        // out a NextToken yet, and DynamoDB rejects a NextToken which it did not hand out.
        co_return api_error::validation(fmt::format("NextToken: Provided nextToken is invalid: `{}`", next_token));
    }

    maybe_audit(audit_info, audit::statement_category::QUERY,
                table_parts ? table_parts->keyspace_name : "", table_parts ? table_parts->table_name : "",
                "ListImports", request);

    // FIXME: list the imports from the imports table of SCYLLADB-4041, newest first, PageSize (by default 10)
    // per page, only those of TableArn if given, also of a dropped table (SCYLLADB-4043). Until then there are none.
    rjson::value response = rjson::empty_object();
    rjson::add(response, "ImportSummaryList", rjson::empty_array());
    co_return rjson::print(std::move(response));
}

} // namespace alternator
