/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"
#include "db_clock.hh"
#include "schema/schema_fwd.hh"

namespace cql3 {
class query_processor;
}

// Access to the Alternator export to S3 metadata in the system_distributed keyspace.
namespace alternator {

// The persisted state of a single Alternator "export to S3", as needed to answer DescribeExport.
// The columns above the optional ones must all be written by the statement that creates the export
// row, so that a row is either complete or not there at all. Everything that describes a finished
// export is optional, because it is only filled in as the export runs.
struct export_row {
    sstring client_token;
    // The original ExportTableToPointInTime request, as JSON. It is the source of truth for the
    // request-derived fields of ExportDescription (S3Bucket, ExportFormat, TableArn, ...).
    sstring request;
    sstring export_status;
    ::table_id table_id;  // The exported table, as resolved when the export was accepted
    db_clock::time_point export_time;  // The point in time the data was exported from
    db_clock::time_point accepted_at;
    std::optional<sstring> export_manifest;
    std::optional<sstring> failure_code;
    std::optional<sstring> failure_message;
    std::optional<int64_t> item_count;
    std::optional<int64_t> billed_size_bytes;
    std::optional<db_clock::time_point> completed_at;
};

// Returns the export with the given ARN, or nullopt if there is no such export.
future<std::optional<export_row>> get_export(cql3::query_processor& qp, sstring export_arn);

}
