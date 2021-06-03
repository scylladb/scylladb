/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <cstdint>

#include <seastar/core/metrics_registration.hh>
#include "seastarx.hh"
#include "utils/estimated_histogram.hh"
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
        uint64_t list_streams = 0;
        uint64_t describe_stream = 0;
        uint64_t get_shard_iterator = 0;
        uint64_t get_records = 0;

        utils::time_estimated_histogram put_item_latency;
        utils::time_estimated_histogram get_item_latency;
        utils::time_estimated_histogram delete_item_latency;
        utils::time_estimated_histogram update_item_latency;
        utils::time_estimated_histogram get_records_latency;
    } api_operations;
    // Miscellaneous event counters
    uint64_t total_operations = 0;
    uint64_t unsupported_operations = 0;
    uint64_t reads_before_write = 0;
    uint64_t write_using_lwt = 0;
    uint64_t shard_bounce_for_lwt = 0;
    uint64_t requests_blocked_memory = 0;
    uint64_t requests_shed = 0;
    // CQL-derived stats
    cql3::cql_stats cql_stats;
private:
    // The metric_groups object holds this stat object's metrics registered
    // as long as the stats object is alive.
    seastar::metrics::metric_groups _metrics;
};

}
