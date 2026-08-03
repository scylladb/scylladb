/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file implements the Alternator export operations:
// ExportTableToPointInTime, DescribeExport, and ListExports.

#include "alternator/executor.hh"

#include <algorithm>
#include <chrono>
#include <fmt/format.h>
#include <limits>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <vector>

#include "db/system_keyspace.hh"
#include "data_dictionary/data_dictionary.hh"
#include "mutation/mutation.hh"
#include "mutation/timestamp.hh"
#include "query/query-result-set.hh"
#include "query/query-request.hh"
#include "schema/schema.hh"
#include "service/storage_proxy.hh"
#include "utils/UUID_gen.hh"
#include "utils/assert.hh"
#include "utils/rjson.hh"

namespace alternator {

static std::string_view get_required_string(const rjson::value& request, std::string_view field_name) {
    const rjson::value* value = rjson::find(request, field_name);
    if (!value || !value->IsString()) {
        throw api_error::validation(fmt::format("Missing or invalid required parameter: {}", field_name));
    }
    auto string = rjson::to_string_view(*value);
    if (string.empty()) {
        throw api_error::validation(fmt::format("{} must not be empty", field_name));
    }
    return string;
}

static std::string_view get_required_string(const rjson::value& request, std::string_view field_name, std::string_view error_name) {
    const rjson::value* value = rjson::find(request, field_name);
    if (!value || !value->IsString()) {
        throw api_error::validation(fmt::format("{} parameter: failed to parse - missing or empty", error_name));
    }
    auto string = rjson::to_string_view(*value);
    if (string.empty()) {
        throw api_error::validation(fmt::format("{} parameter: failed to parse - must not be empty", error_name));
    }
    return string;
}

static std::string_view get_optional_string(const rjson::value& request, std::string_view field_name, std::string_view error_name) {
    const rjson::value* value = rjson::find(request, field_name);
    if (!value) {
        return {};
    }
    if (!value->IsString()) {
        throw api_error::validation(fmt::format("{} parameter: failed to parse - must be a string", error_name));
    }
    return rjson::to_string_view(*value);
}

static sstring make_export_arn(std::string_view table_arn, int64_t export_time) {
    return fmt::format("{}/export/{}-{}", table_arn, export_time, utils::UUID_gen::get_time_UUID());
}

static bytes decompose_text(std::string_view value) {
    return utf8_type->decompose(sstring(value));
}

static void add_text_cell(mutation& m, const schema& schema, api::timestamp_type ts, std::string_view name, std::string_view value) {
    if (value.empty()) {
        return;
    }
    const column_definition* cdef = schema.get_column_definition(to_bytes(name));
    throwing_assert(cdef);
    auto& row = m.partition().clustered_row(schema, clustering_key::make_empty());
    row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, decompose_text(value)));
}

static void add_timestamp_cell(mutation& m, const schema& schema, api::timestamp_type ts, std::string_view name, int64_t value_seconds) {
    const column_definition* cdef = schema.get_column_definition(to_bytes(name));
    throwing_assert(cdef);
    auto& row = m.partition().clustered_row(schema, clustering_key::make_empty());
    row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, timestamp_type->decompose(db_clock::time_point(std::chrono::milliseconds(value_seconds * 1000)))));
}

static mutation make_export_metadata_mutation(schema_ptr schema, api::timestamp_type ts,
        std::string_view export_arn, std::string_view table_arn, std::string_view client_token, std::string_view request_json,
        int64_t accepted_at) {
    auto pk = partition_key::from_exploded(*schema, {decompose_text(export_arn)});
    mutation m(schema, pk);
    add_text_cell(m, *schema, ts, "table_arn", table_arn);
    add_text_cell(m, *schema, ts, "client_token", client_token);
    add_text_cell(m, *schema, ts, "request", request_json);
    add_text_cell(m, *schema, ts, "export_status", "IN_PROGRESS");
    add_timestamp_cell(m, *schema, ts, "accepted_at", accepted_at);
    return m;
}

static mutation make_client_token_mutation(schema_ptr schema, api::timestamp_type ts,
        std::string_view client_token, std::string_view table_arn, std::string_view export_arn, std::string_view request_json) {
    auto pk = partition_key::from_exploded(*schema, {decompose_text(client_token)});
    mutation m(schema, pk);
    add_text_cell(m, *schema, ts, "table_arn", table_arn);
    add_text_cell(m, *schema, ts, "export_arn", export_arn);
    add_text_cell(m, *schema, ts, "request", request_json);
    return m;
}

static mutation make_export_summary_mutation(schema_ptr schema, api::timestamp_type ts,
        std::string_view export_arn, std::string_view table_arn, std::string_view export_type) {
    auto pk = partition_key::from_exploded(*schema, {decompose_text(table_arn)});
    auto ck = clustering_key::from_exploded(*schema, {decompose_text(export_arn)});
    mutation m(schema, pk);
    auto& row = m.partition().clustered_row(*schema, ck);
    auto add_summary_cell = [&] (std::string_view name, std::string_view value) {
        const column_definition* cdef = schema->get_column_definition(to_bytes(name));
        throwing_assert(cdef);
        row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, decompose_text(value)));
    };
    add_summary_cell("table_arn", table_arn);
    add_summary_cell("export_status", "IN_PROGRESS");
    add_summary_cell("export_type", export_type);
    return m;
}

static future<> persist_export_metadata(service::storage_proxy& proxy, service_permit permit,
        std::string_view export_arn, std::string_view table_arn, std::string_view client_token, const rjson::value& request,
        int64_t accepted_at, std::string_view export_type) {
    auto request_json = rjson::print(rjson::copy(request));
    auto ts = api::new_timestamp();
    utils::chunked_vector<mutation> mutations;
    auto exports_schema = proxy.data_dictionary().find_table(db::system_keyspace::NAME, db::system_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS).schema();
    mutations.push_back(make_export_metadata_mutation(exports_schema, ts, export_arn, table_arn, client_token, request_json, accepted_at));
    auto summaries_schema = proxy.data_dictionary().find_table(db::system_keyspace::NAME, db::system_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORT_SUMMARIES).schema();
    mutations.push_back(make_export_summary_mutation(summaries_schema, ts, export_arn, table_arn, export_type));
    if (!client_token.empty()) {
        auto tokens_schema = proxy.data_dictionary().find_table(db::system_keyspace::NAME, db::system_keyspace::ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS).schema();
        mutations.push_back(make_client_token_mutation(tokens_schema, ts, client_token, table_arn, export_arn, request_json));
    }
    co_await proxy.mutate(std::move(mutations), db::consistency_level::LOCAL_QUORUM, executor::default_timeout(), {}, std::move(permit), db::allow_per_partition_rate_limit::yes);
}

static future<std::optional<query::result_set_row>> read_system_table_row(service::storage_proxy& proxy, service::client_state& client_state, service_permit permit,
        std::string_view table_name, std::string_view partition_key_value) {
    auto schema = proxy.data_dictionary().find_table(db::system_keyspace::NAME, sstring(table_name)).schema();
    auto pk = partition_key::from_exploded(*schema, {decompose_text(partition_key_value)});
    dht::partition_range_vector partition_ranges{dht::partition_range(dht::decorate_key(*schema, pk))};
    auto partition_slice = schema->full_slice();
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice,
            proxy.get_max_result_size(partition_slice), query::tombstone_limit(proxy.get_tombstone_limit()));
    auto qr = co_await proxy.query(schema, std::move(command), std::move(partition_ranges), db::consistency_level::LOCAL_QUORUM,
            service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state));
    auto rs = query::result_set::from_raw_result(schema, partition_slice, *qr.query_result);
    if (rs.empty()) {
        co_return std::nullopt;
    }
    co_return rs.row(0).copy();
}

static future<std::optional<query::result_set_row>> read_export_metadata(service::storage_proxy& proxy, service::client_state& client_state, service_permit permit,
        std::string_view export_arn) {
    co_return co_await read_system_table_row(proxy, client_state, std::move(permit), db::system_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS, export_arn);
}

static future<std::optional<query::result_set_row>> read_client_token(service::storage_proxy& proxy, service::client_state& client_state, service_permit permit,
        std::string_view client_token) {
    co_return co_await read_system_table_row(proxy, client_state, std::move(permit), db::system_keyspace::ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS, client_token);
}

static future<std::vector<query::result_set_row>> read_export_summaries(service::storage_proxy& proxy, service::client_state& client_state, service_permit permit,
        std::optional<std::string_view> table_arn) {
    auto schema = proxy.data_dictionary().find_table(db::system_keyspace::NAME, db::system_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORT_SUMMARIES).schema();
    dht::partition_range_vector partition_ranges;
    if (table_arn) {
        auto pk = partition_key::from_exploded(*schema, {decompose_text(*table_arn)});
        partition_ranges.emplace_back(dht::partition_range(dht::decorate_key(*schema, pk)));
    } else {
        partition_ranges.emplace_back(dht::partition_range::make_open_ended_both_sides());
    }
    auto partition_slice = schema->full_slice();
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice,
            proxy.get_max_result_size(partition_slice), query::tombstone_limit(proxy.get_tombstone_limit()));
    auto qr = co_await proxy.query(schema, std::move(command), std::move(partition_ranges), db::consistency_level::LOCAL_QUORUM,
            service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state));
    auto rs = query::result_set::from_raw_result(schema, partition_slice, *qr.query_result);
    std::vector<query::result_set_row> rows;
    rows.reserve(rs.rows().size());
    for (const auto& row : rs.rows()) {
        rows.push_back(row.copy());
    }
    co_return rows;
}

static int64_t timestamp_seconds(const query::result_set_row& row, const sstring& column_name) {
    auto value = row.get<db_clock::time_point>(column_name);
    if (!value) {
        return 0;
    }
    return std::chrono::duration_cast<std::chrono::seconds>(value->time_since_epoch()).count();
}

static void add_request_string(rjson::value& export_desc, const rjson::value& request, std::string_view name, rjson::string_ref_type response_name) {
    const rjson::value* value = rjson::find(request, name);
    if (value && value->IsString() && !rjson::to_string_view(*value).empty()) {
        rjson::add(export_desc, response_name, rjson::from_string(rjson::to_string_view(*value)));
    }
}

static void add_row_string(rjson::value& export_desc, const query::result_set_row& row, const sstring& column_name, rjson::string_ref_type response_name) {
    auto value = row.get<sstring>(column_name);
    if (value && !value->empty()) {
        rjson::add(export_desc, response_name, rjson::from_string(*value));
    }
}

static void add_row_int64(rjson::value& export_desc, const query::result_set_row& row, const sstring& column_name, rjson::string_ref_type response_name) {
    auto value = row.get<int64_t>(column_name);
    if (value) {
        rjson::add(export_desc, response_name, rjson::value(*value));
    }
}

static void add_row_timestamp(rjson::value& export_desc, const query::result_set_row& row, const sstring& column_name, rjson::string_ref_type response_name) {
    auto seconds = timestamp_seconds(row, column_name);
    if (seconds != 0) {
        rjson::add(export_desc, response_name, rjson::value(seconds));
    }
}

static rjson::value make_export_summary(std::string_view export_arn, const query::result_set_row& row) {
    rjson::value summary = rjson::empty_object();
    rjson::add(summary, "ExportArn", rjson::from_string(export_arn));
    rjson::add(summary, "ExportStatus", rjson::from_string(row.get<sstring>("export_status").value_or("IN_PROGRESS")));
    rjson::add(summary, "ExportType", rjson::from_string(row.get<sstring>("export_type").value_or("FULL_EXPORT")));
    return summary;
}

static bool request_json_equal(const rjson::value& lhs, const rjson::value& rhs) {
    if (lhs.GetType() != rhs.GetType()) {
        return false;
    }

    switch (lhs.GetType()) {
    case rjson::type::kNullType:
    case rjson::type::kFalseType:
    case rjson::type::kTrueType:
        return true;
    case rjson::type::kObjectType:
        if (lhs.MemberCount() != rhs.MemberCount()) {
            return false;
        }
        for (auto it = lhs.MemberBegin(); it != lhs.MemberEnd(); ++it) {
            const rjson::value* rhs_value = rjson::find(rhs, rjson::to_string_view(it->name));
            if (!rhs_value || !request_json_equal(it->value, *rhs_value)) {
                return false;
            }
        }
        return true;
    case rjson::type::kArrayType:
        if (lhs.Size() != rhs.Size()) {
            return false;
        }
        for (rapidjson::SizeType i = 0; i != lhs.Size(); ++i) {
            if (!request_json_equal(lhs[i], rhs[i])) {
                return false;
            }
        }
        return true;
    case rjson::type::kStringType:
        return rjson::to_string_view(lhs) == rjson::to_string_view(rhs);
    case rjson::type::kNumberType:
        if (lhs.IsInt64() && rhs.IsInt64()) {
            return lhs.GetInt64() == rhs.GetInt64();
        }
        if (lhs.IsUint64() && rhs.IsUint64()) {
            return lhs.GetUint64() == rhs.GetUint64();
        }
        if (lhs.IsInt64() && rhs.IsUint64()) {
            return lhs.GetInt64() >= 0 && static_cast<uint64_t>(lhs.GetInt64()) == rhs.GetUint64();
        }
        if (lhs.IsUint64() && rhs.IsInt64()) {
            return rhs.GetInt64() >= 0 && lhs.GetUint64() == static_cast<uint64_t>(rhs.GetInt64());
        }
        return lhs.GetDouble() == rhs.GetDouble();
    }
    return false;
}

static std::optional<int64_t> get_json_int64(const rjson::value* value) {
    if (!value) {
        return std::nullopt;
    }
    if (value->IsInt64()) {
        return value->GetInt64();
    }
    if (value->IsUint64() && value->GetUint64() <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return static_cast<int64_t>(value->GetUint64());
    }
    return std::nullopt;
}

static rjson::value make_export_description(std::string_view export_arn, const query::result_set_row& row) {
    auto table_arn = row.get<sstring>("table_arn");
    auto request_json = row.get<sstring>("request");
    auto export_status = row.get<sstring>("export_status").value_or("IN_PROGRESS");
    if (!table_arn || !request_json) {
        throw api_error("ExportNotFoundException", fmt::format("Export not found: {}", export_arn));
    }

    auto request = rjson::parse(*request_json);
    const rjson::value* export_format = rjson::find(request, "ExportFormat");
    const rjson::value* export_type = rjson::find(request, "ExportType");
    const rjson::value* export_time = rjson::find(request, "ExportTime");
    auto accepted_at = timestamp_seconds(row, "accepted_at");

    rjson::value export_desc = rjson::empty_object();
    rjson::add(export_desc, "ExportArn", rjson::from_string(export_arn));
    rjson::add(export_desc, "ExportFormat", rjson::from_string(export_format && export_format->IsString() ? rjson::to_string_view(*export_format) : "DYNAMODB_JSON"));
    rjson::add(export_desc, "ExportStatus", rjson::from_string(export_status));
    rjson::add(export_desc, "ExportTime", rjson::value(get_json_int64(export_time).value_or(accepted_at)));
    rjson::add(export_desc, "ExportType", rjson::from_string(export_type && export_type->IsString() ? rjson::to_string_view(*export_type) : "FULL_EXPORT"));
    rjson::add(export_desc, "StartTime", rjson::value(accepted_at));
    rjson::add(export_desc, "TableArn", rjson::from_string(*table_arn));
    add_request_string(export_desc, request, "ClientToken", "ClientToken");
    add_request_string(export_desc, request, "S3Bucket", "S3Bucket");
    add_request_string(export_desc, request, "S3Prefix", "S3Prefix");
    add_request_string(export_desc, request, "S3BucketOwner", "S3BucketOwner");
    add_request_string(export_desc, request, "S3SseAlgorithm", "S3SseAlgorithm");
    add_request_string(export_desc, request, "S3SseKmsKeyId", "S3SseKmsKeyId");
    add_row_string(export_desc, row, "export_manifest", "ExportManifest");
    add_row_string(export_desc, row, "failure_code", "FailureCode");
    add_row_string(export_desc, row, "failure_message", "FailureMessage");
    add_row_int64(export_desc, row, "item_count", "ItemCount");
    add_row_timestamp(export_desc, row, "completed_at", "EndTime");
    return export_desc;
}

static future<std::optional<rjson::value>> existing_export_for_client_token(service::storage_proxy& proxy, service::client_state& client_state,
        std::string_view client_token, const rjson::value& request) {
    if (client_token.empty()) {
        co_return std::nullopt;
    }
    auto token_row = co_await read_client_token(proxy, client_state, empty_service_permit(), client_token);
    if (!token_row) {
        co_return std::nullopt;
    }
    auto export_arn = token_row->get<sstring>("export_arn");
    auto request_json = token_row->get<sstring>("request");
    if (!export_arn || !request_json) {
        co_return std::nullopt;
    }
    auto stored_request = rjson::parse(*request_json);
    if (!request_json_equal(stored_request, request)) {
        throw api_error("ExportConflictException", "Duplicate request detected with a different request body");
    }
    auto export_row = co_await read_export_metadata(proxy, client_state, empty_service_permit(), *export_arn);
    if (!export_row) {
        co_return std::nullopt;
    }
    co_return make_export_description(*export_arn, *export_row);
}

future<executor::request_return_type> executor::export_table_to_point_in_time(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.export_table_to_point_in_time++;

    std::string_view table_arn;
    std::string_view s3_bucket;
    try {
        table_arn = get_required_string(request, "TableArn");
        s3_bucket = get_required_string(request, "S3Bucket", "s3Bucket");
    } catch (const api_error& e) {
        co_return e;
    }

    // Validate table exists by parsing the ARN
    auto parts = parse_arn(table_arn, "TableArn", "table", "");
    try {
        (void)_proxy.data_dictionary().find_schema(parts.keyspace_name, parts.table_name);
    } catch (const data_dictionary::no_such_column_family&) {
        co_return api_error::table_not_found(
                fmt::format("Table not found: {}", table_arn));
    }

    std::string_view s3_prefix;
    try {
        s3_prefix = get_optional_string(request, "S3Prefix", "s3Prefix");
    } catch (const api_error& e) {
        co_return e;
    }

    // ExportFormat - only DYNAMODB_JSON is supported (DYNAMODB_JSON is also a default in case option is missing)
    std::string_view export_format = "DYNAMODB_JSON";
    const rjson::value* export_format_v = rjson::find(request, "ExportFormat");
    if (export_format_v) {
        if (!export_format_v->IsString()) {
            // Note: DynamoDB error message for not supported string is as follows:
            //    1 validation error detected: Value 'QWERTY' at 'exportFormat' failed to satisfy constraint: Member must satisfy enum value set: [ION, DYNAMODB_JSON]
            // for empty string:
            //    1 validation error detected: Value '' at 'exportFormat' failed to satisfy constraint: Member must satisfy enum value set: [ION, DYNAMODB_JSON]
            // Observe camel case format for field name.
            co_return api_error::validation("exportFormat parameter: failed to parse - must be a string");
        }
        export_format = rjson::to_string_view(*export_format_v);
        if (export_format != "DYNAMODB_JSON") {
            co_return api_error::validation(
                    fmt::format("exportFormat parameter: failed to parse - must be DYNAMODB_JSON."));
        }
    }

    // ExportType - only FULL_EXPORT is supported (no incremental)
    std::string_view export_type = "FULL_EXPORT";
    const rjson::value* export_type_v = rjson::find(request, "ExportType");
    if (export_type_v) {
        if (!export_type_v->IsString()) {
            co_return api_error::validation("exportType parameter: failed to parse - must be a string");
        }
        export_type = rjson::to_string_view(*export_type_v);
        if (export_type != "FULL_EXPORT") {
            co_return api_error::validation(
                    fmt::format("exportType parameter: failed to parse - must be FULL_EXPORT."));
        }
    }

    // IncrementalExportSpecification - not supported
    const rjson::value* incremental_v = rjson::find(request, "IncrementalExportSpecification");
    if (incremental_v) {
        co_return api_error::validation("incrementalExportSpecification parameter: not supported");
    }

    // ExportTime - only "now" (or close to now) is supported
    // If not specified, use current time. If specified, must be within 5 minutes of now.
    int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    int64_t export_time = now;
    const rjson::value* export_time_v = rjson::find(request, "ExportTime");
    if (export_time_v) {
        if (!export_time_v->IsNumber()) {
            co_return api_error::validation("Invalid parameter: ExportTime must be a number");
        }
        export_time = static_cast<int64_t>(export_time_v->GetDouble());
        auto diff = export_time > now ? export_time - now : now - export_time;
        if (diff > 300) {
            co_return api_error("InvalidExportTimeException",
                    fmt::format("ExportTime must be within 5 minutes of current time. "
                                "ExportTime: {}, current time: {}", export_time, now));
        }
    }

    std::string_view client_token;
    std::string_view s3_bucket_owner;
    try {
        client_token = get_optional_string(request, "ClientToken", "clientToken");
        s3_bucket_owner = get_optional_string(request, "S3BucketOwner", "s3BucketOwner");
    } catch (const api_error& e) {
        co_return e;
    }

    // S3SseAlgorithm and S3SseKmsKeyId - accepted but not used
    std::string_view s3_sse_algorithm;
    const rjson::value* s3_sse_algorithm_v = rjson::find(request, "S3SseAlgorithm");
    if (s3_sse_algorithm_v) {
        if (!s3_sse_algorithm_v->IsString()) {
            co_return api_error::validation("s3SseAlgorithm parameter: failed to parse - must be a string");
        }
        s3_sse_algorithm = rjson::to_string_view(*s3_sse_algorithm_v);
        if (s3_sse_algorithm != "AES256" && s3_sse_algorithm != "KMS") {
            co_return api_error::validation("s3SseAlgorithm parameter: failed to parse - allowed values are AES256 or KMS.");
        }
    }

    std::string_view s3_sse_kms_key_id;
    try {
        s3_sse_kms_key_id = get_optional_string(request, "S3SseKmsKeyId", "s3SseKmsKeyId");
    } catch (const api_error& e) {
        co_return e;
    }

    try {
        auto existing_export_desc = co_await existing_export_for_client_token(_proxy, client_state, client_token, request);
        if (existing_export_desc) {
            rjson::value response = rjson::empty_object();
            rjson::add(response, "ExportDescription", std::move(*existing_export_desc));
            co_return rjson::print(std::move(response));
        }
    } catch (const api_error& e) {
        co_return e;
    }

    auto export_arn = make_export_arn(table_arn, export_time);
    co_await persist_export_metadata(_proxy, std::move(permit), export_arn, table_arn, client_token, request, now, export_type);

    rjson::value export_desc = rjson::empty_object();
    rjson::add(export_desc, "ExportArn", rjson::from_string(export_arn));
    rjson::add(export_desc, "ExportFormat", rjson::from_string(export_format));
    rjson::add(export_desc, "ExportStatus", "IN_PROGRESS");
    rjson::add(export_desc, "ExportTime", rjson::value(export_time));
    rjson::add(export_desc, "ExportType", rjson::from_string(export_type));
    rjson::add(export_desc, "S3Bucket", rjson::from_string(s3_bucket));
    rjson::add(export_desc, "S3Prefix", rjson::from_string(s3_prefix));
    rjson::add(export_desc, "TableArn", rjson::from_string(table_arn));

    if (!client_token.empty()) {
        rjson::add(export_desc, "ClientToken", rjson::from_string(client_token));
    }
    if (!s3_bucket_owner.empty()) {
        rjson::add(export_desc, "S3BucketOwner", rjson::from_string(s3_bucket_owner));
    }
    if (!s3_sse_algorithm.empty()) {
        rjson::add(export_desc, "S3SseAlgorithm", rjson::from_string(s3_sse_algorithm));
    }
    if (!s3_sse_kms_key_id.empty()) {
        rjson::add(export_desc, "S3SseKmsKeyId", rjson::from_string(s3_sse_kms_key_id));
    }

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportDescription", std::move(export_desc));
    co_return rjson::print(std::move(response));
}

future<executor::request_return_type> executor::describe_export(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_export++;

    std::string_view export_arn;
    try {
        export_arn = get_required_string(request, "ExportArn");
    } catch (const api_error& e) {
        co_return e;
    }

    static constexpr std::string_view export_postfix = "/export/";
    arn_parts parts;
    try {
        parts = parse_arn(export_arn, "ExportArn", "export", export_postfix);
    } catch (const api_error& e) {
        co_return api_error::validation(e._msg);
    } catch (const std::exception& e) {
        co_return api_error::validation(fmt::format("ExportArn: Invalid export ARN `{}` - {}", export_arn, e.what()));
    }

    if (parts.postfix == "/export/") {
        co_return api_error("ExportNotFoundException", fmt::format("Export not found: {}", export_arn));
    }

    std::optional<schema_ptr> schema;
    try {
        schema = _proxy.data_dictionary().find_schema(parts.keyspace_name, parts.table_name);
    } catch (const data_dictionary::no_such_column_family&) {
    }

    if (schema) {
        maybe_audit(audit_info, audit::statement_category::QUERY, (*schema)->ks_name(), (*schema)->cf_name(), "DescribeExport", request);
    }

    auto row = co_await read_export_metadata(_proxy, client_state, std::move(permit), export_arn);
    if (!row) {
        co_return api_error("ExportNotFoundException", fmt::format("Export not found: {}", export_arn));
    }

    rjson::value response = rjson::empty_object();
    try {
        rjson::add(response, "ExportDescription", make_export_description(export_arn, *row));
    } catch (const api_error& e) {
        co_return e;
    }
    co_return rjson::print(std::move(response));
}

future<executor::request_return_type> executor::list_exports(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.list_exports++;

    static constexpr int default_max_results = 25;
    static constexpr int min_max_results = 1;
    static constexpr int max_max_results = 25;

    int max_results = default_max_results;
    const rjson::value* max_results_v = rjson::find(request, "MaxResults");
    if (max_results_v) {
        if (!max_results_v->IsInt()) {
            co_return api_error::validation("MaxResults must be an integer");
        }
        max_results = max_results_v->GetInt();
        if (max_results < min_max_results || max_results > max_max_results) {
            co_return api_error::validation("MaxResults must be greater than 0 and no greater than 25");
        }
    }

    std::string_view table_arn;
    const rjson::value* table_arn_v = rjson::find(request, "TableArn");
    if (table_arn_v) {
        if (!table_arn_v->IsString()) {
            co_return api_error::validation("tableArn parameter: failed to parse - must be a string");
        }
        table_arn = rjson::to_string_view(*table_arn_v);
        if (table_arn.empty() || table_arn.size() > 1024) {
            co_return api_error::validation("tableArn parameter: failed to parse - must be between 1 and 1024 characters");
        }
        try {
            (void)parse_arn(table_arn, "TableArn", "table", "");
        } catch (const api_error& e) {
            co_return api_error::validation(e._msg);
        } catch (const std::exception& e) {
            co_return api_error::validation(fmt::format("TableArn: Invalid table ARN `{}` - {}", table_arn, e.what()));
        }
    }

    std::string_view next_token;
    const rjson::value* next_token_v = rjson::find(request, "NextToken");
    if (next_token_v) {
        if (!next_token_v->IsString()) {
            co_return api_error::validation("NextToken must be a string");
        }
        next_token = rjson::to_string_view(*next_token_v);
    }

    maybe_audit(audit_info, audit::statement_category::QUERY, "", "", "ListExports", request);

    auto rows = co_await read_export_summaries(_proxy, client_state, std::move(permit), table_arn.empty() ? std::nullopt : std::optional<std::string_view>(table_arn));
    std::vector<std::pair<sstring, query::result_set_row>> summaries;
    summaries.reserve(rows.size());
    for (auto& row : rows) {
        auto export_arn = row.get<sstring>("export_arn");
        if (export_arn) {
            summaries.emplace_back(std::move(*export_arn), std::move(row));
        }
    }
    std::ranges::sort(summaries, [] (const auto& lhs, const auto& rhs) {
        return lhs.first > rhs.first;
    });

    rjson::value response = rjson::empty_object();
    rjson::add(response, "ExportSummaries", rjson::empty_array());
    auto& export_summaries = response["ExportSummaries"];

    int emitted = 0;
    bool has_more = false;
    std::optional<sstring> last_export_arn;
    for (const auto& [export_arn, row] : summaries) {
        if (!next_token.empty() && export_arn >= next_token) {
            continue;
        }
        if (emitted == max_results) {
            has_more = true;
            break;
        }
        rjson::push_back(export_summaries, make_export_summary(export_arn, row));
        last_export_arn = export_arn;
        ++emitted;
    }

    if (has_more && last_export_arn) {
        rjson::add(response, "NextToken", rjson::from_string(*last_export_arn));
    }

    co_return rjson::print(std::move(response));
}

}
