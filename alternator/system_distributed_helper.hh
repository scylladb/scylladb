/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "seastarx.hh"
#include "db_clock.hh"
#include "utils/chunked_vector.hh"
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace cql3 {
class query_processor;
}

// Helper functions for accessing export to S3 metadata in system_distributed keyspace.
// `system_distributed` keyspace contains two tables related to Alternator export to S3 functionality:
// 1) `alternator_export_to_s3_exports` - contains metadata about exports, such as export ARN, table ARN, client token, export status, timestamps, etc:
//        export_arn           utf8_type - column_kind::partition_key
//        client_token         utf8_type
//        request              utf8_type
//        export_manifest      utf8_type
//        export_status        utf8_type
//        failure_code         utf8_type
//        failure_message      utf8_type
//        item_count           long_type
//        export_id_token      utf8_type
//        accepted_at          timestamp_type
//        completed_at         timestamp_type
//        node_id              utf8_type
//        metadata_expires_at  timestamp_type
// 2) `alternator_export_to_s3_client_tokens` - used for idempotency of export creation, maps client tokens to export ARNs and contains the original request for comparison during idempotent retries:
//        client_token         utf8_type - column_kind::partition_key
//        export_arn           utf8_type
//        request              utf8_type
//        node_id              utf8_type
// All modifications to those tables must go through the helper functions - those make sure proper create-if-not-exists, update-if semantics are applied.
namespace alternator {
    struct client_row {
        sstring client_token;
        sstring export_arn;
        sstring request;
        sstring node_id;
    };

    struct export_row {
        sstring export_arn;
        sstring client_token;
        sstring request;
        sstring export_manifest;
        sstring export_status;
        sstring failure_code;
        sstring failure_message;
        int64_t item_count;
        sstring export_id_token;
        db_clock::time_point accepted_at;
        db_clock::time_point completed_at;
        sstring node_id;
        db_clock::time_point metadata_expires_at;
    };
    // Get or insert a client token row. Returns a tuple where the first element is the client_row
    // and the second element is true if the row was inserted, false if it already existed.
    future<std::tuple<client_row, bool>> get_or_insert_client_row(cql3::query_processor& db, const sstring& client_token);

    // Insert a new export metadata row (unconditional).
    // Returns true if the row was inserted, false if a row with the same export ARN already exists (in that case no update is performed).
    future<bool> insert_export(cql3::query_processor& db, const export_row& row);

    // Update export status. Requires previous export_status and node_id fields, which are used to deal with concurrent accesses.
    // Processes modifying export rows are by intention parallel. Each process is expect to keep "current" export_status and node_id values in memory
    // and use them as key when updating. If the update fails, it means other process took over and current process needs to abort.
    // Returns true if row was modified, false if not modified.
    future<bool> update_export(cql3::query_processor& db, const export_row& row, const sstring &old_export_status, const sstring &old_node_id);

    // Read a single export by ARN. Returns empty result set if not found.
    future<std::optional<export_row>> get_export(cql3::query_processor& db, sstring export_arn);

    // Read all exports. Used by ListExports.
    future<utils::chunked_vector<export_row>> get_all_exports(cql3::query_processor& db);

    // Read all client token rows. Used by garbage collection.
    future<utils::chunked_vector<client_row>> get_all_client_tokens(cql3::query_processor& db);
}