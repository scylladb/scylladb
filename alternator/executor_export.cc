/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file implements the Alternator export operations.

#include "alternator/executor.hh"

#include <chrono>
#include <fmt/format.h>
#include <seastar/core/coroutine.hh>

#include "data_dictionary/data_dictionary.hh"
#include "schema/schema.hh"
#include "service/storage_proxy.hh"
#include "utils/rjson.hh"

namespace alternator {

future<executor::request_return_type> executor::export_table_to_point_in_time(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.export_table_to_point_in_time++;

    // Required parameters
    const rjson::value* table_arn_v = rjson::find(request, "TableArn");
    if (!table_arn_v || !table_arn_v->IsString()) {
        co_return api_error::validation("Missing or invalid required parameter: TableArn");
    }
    std::string_view table_arn = rjson::to_string_view(*table_arn_v);
    if (table_arn.empty()) {
        co_return api_error::validation("TableArn must not be empty");
    }

    const rjson::value* s3_bucket_v = rjson::find(request, "S3Bucket");
    if (!s3_bucket_v || !s3_bucket_v->IsString()) {
        co_return api_error::validation("s3Bucket parameter: failed to parse - missing or empty");
    }
    std::string_view s3_bucket = rjson::to_string_view(*s3_bucket_v);
    if (s3_bucket.empty()) {
        co_return api_error::validation("s3Bucket parameter: failed to parse - must not be empty");
    }

    // Validate table exists by parsing the ARN
    auto parts = parse_arn(table_arn, "TableArn", "table", "");
    schema_ptr schema;
    try {
        schema = _proxy.data_dictionary().find_schema(parts.keyspace_name, parts.table_name);
    } catch (const data_dictionary::no_such_column_family&) {
        co_return api_error::table_not_found(
                fmt::format("Table not found: {}", table_arn));
    }

    // Optional parameters
    std::string_view s3_prefix;
    const rjson::value* s3_prefix_v = rjson::find(request, "S3Prefix");
    if (s3_prefix_v) {
        if (!s3_prefix_v->IsString()) {
            co_return api_error::validation("Invalid parameter: S3Prefix must be a string");
        }
        s3_prefix = rjson::to_string_view(*s3_prefix_v);
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

    // ClientToken - for idempotency
    std::string_view client_token;
    const rjson::value* client_token_v = rjson::find(request, "ClientToken");
    if (client_token_v) {
        if (!client_token_v->IsString()) {
            co_return api_error::validation("clientToken parameter: failed to parse - must be a string");
        }
        client_token = rjson::to_string_view(*client_token_v);
    }

    // S3BucketOwner - accepted but not used
    std::string_view s3_bucket_owner;
    const rjson::value* s3_bucket_owner_v = rjson::find(request, "S3BucketOwner");
    if (s3_bucket_owner_v) {
        if (!s3_bucket_owner_v->IsString()) {
            co_return api_error::validation("s3BucketOwner parameter: failed to parse - must be a string");
        }
        s3_bucket_owner = rjson::to_string_view(*s3_bucket_owner_v);
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
    const rjson::value* s3_sse_kms_key_id_v = rjson::find(request, "S3SseKmsKeyId");
    if (s3_sse_kms_key_id_v) {
        if (!s3_sse_kms_key_id_v->IsString()) {
            co_return api_error::validation("s3SseKmsKeyId parameter: failed to parse - must be a string");
        }
        s3_sse_kms_key_id = rjson::to_string_view(*s3_sse_kms_key_id_v);
    }

    // Build the ExportDescription response
    // The actual export functionality is not implemented yet - this just returns
    // an IN_PROGRESS status to indicate the export has been accepted.
    rjson::value export_desc = rjson::empty_object();
    rjson::add(export_desc, "ClientToken", rjson::from_string(client_token));
    rjson::add(export_desc, "ExportArn",
            rjson::from_string(fmt::format("arn:scylla:dynamodb:::table/{}/export/export-placeholder",
                    parts.table_name)));
    rjson::add(export_desc, "ExportFormat", rjson::from_string(export_format));
    rjson::add(export_desc, "ExportStatus", "IN_PROGRESS");
    rjson::add(export_desc, "ExportTime", rjson::value(export_time));
    rjson::add(export_desc, "ExportType", rjson::from_string(export_type));
    rjson::add(export_desc, "S3Bucket", rjson::from_string(s3_bucket));
    rjson::add(export_desc, "S3Prefix", rjson::from_string(s3_prefix));
    rjson::add(export_desc, "TableArn", rjson::from_string(table_arn));

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

}
