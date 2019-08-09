/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <cstdint>

#include <seastar/core/metrics_registration.hh>
#include "cql3/stats.hh"

namespace alternator {

// Object holding per-shard statistics related to Alternator.
// While this object is alive, these metrics are also registered to be
// visible by the metrics REST API, with the "alternator" prefix.
class stats {
public:
    stats();
    // Count of DynamoDB API operations by types
    struct {
        uint64_t batch_get_item = 0;
        uint64_t batch_write_item = 0;
        uint64_t create_backup = 0;
        uint64_t create_global_table = 0;
        uint64_t create_table = 0;
        uint64_t delete_backup = 0;
        uint64_t delete_item = 0;
        uint64_t delete_table = 0;
        uint64_t describe_backup = 0;
        uint64_t describe_continuous_backups = 0;
        uint64_t describe_endpoints = 0;
        uint64_t describe_global_table = 0;
        uint64_t describe_global_table_settings = 0;
        uint64_t describe_limits = 0;
        uint64_t describe_table = 0;
        uint64_t describe_time_to_live = 0;
        uint64_t get_item = 0;
        uint64_t list_backups = 0;
        uint64_t list_global_tables = 0;
        uint64_t list_tables = 0;
        uint64_t list_tags_of_resource = 0;
        uint64_t put_item = 0;
        uint64_t query = 0;
        uint64_t restore_table_from_backup = 0;
        uint64_t restore_table_to_point_in_time = 0;
        uint64_t scan = 0;
        uint64_t tag_resource = 0;
        uint64_t transact_get_items = 0;
        uint64_t transact_write_items = 0;
        uint64_t untag_resource = 0;
        uint64_t update_continuous_backups = 0;
        uint64_t update_global_table = 0;
        uint64_t update_global_table_settings = 0;
        uint64_t update_item = 0;
        uint64_t update_table = 0;
        uint64_t update_time_to_live = 0;
    } api_operations;
    // Miscellaneous event counters
    uint64_t total_operations = 0;
    uint64_t unsupported_operations = 0;
    uint64_t reads_before_write = 0;
    // CQL-derived stats
    cql3::cql_stats cql_stats;
private:
    // The metric_groups object holds this stat object's metrics registered
    // as long as the stats object is alive.
    seastar::metrics::metric_groups _metrics;
};

}
