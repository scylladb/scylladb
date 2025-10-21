/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>

#include <seastar/core/metrics_registration.hh>
#include "utils/histogram.hh"
#include "utils/estimated_histogram.hh"
#include "cql3/stats.hh"

namespace alternator {

// Object holding per-shard statistics related to Alternator.
// While this object is alive, these metrics are also registered to be
// visible by the metrics REST API, with the "alternator" prefix.
class stats {
public:
    // Count of DynamoDB API operations by types
    struct {
        uint64_t batch_get_item = 0;
        uint64_t batch_write_item = 0;
        uint64_t batch_get_item_batch_total = 0;
        uint64_t batch_write_item_batch_total = 0;
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


        utils::timed_rate_moving_average_summary_and_histogram put_item_latency;
        utils::timed_rate_moving_average_summary_and_histogram get_item_latency;
        utils::timed_rate_moving_average_summary_and_histogram delete_item_latency;
        utils::timed_rate_moving_average_summary_and_histogram update_item_latency;
        utils::timed_rate_moving_average_summary_and_histogram batch_write_item_latency;
        utils::timed_rate_moving_average_summary_and_histogram batch_get_item_latency;
        utils::timed_rate_moving_average_summary_and_histogram get_records_latency;

        utils::estimated_histogram batch_get_item_histogram{22}; // a histogram that covers the range 1 - 100
        utils::estimated_histogram batch_write_item_histogram{22}; // a histogram that covers the range 1 - 100
    } api_operations;
    // Operation size metrics
    struct {
        // Item size statistics collected per table and aggregated per node.
        // Each histogram covers the range 0 - 446. Resolves #25143.
        // A size is the retrieved item's size.
        utils::estimated_histogram get_item_op_size_kb{30};
        // A size is the maximum of the new item's size and the old item's size.
        utils::estimated_histogram put_item_op_size_kb{30};
        // A size is the deleted item's size. If the deleted item's size is
        // unknown (i.e. read-before-write wasn't necessary and it wasn't
        // forced by a configuration option), it won't be recorded on the
        // histogram.
        utils::estimated_histogram delete_item_op_size_kb{30};
        // A size is the maximum of existing item's size and the estimated size
        // of the update. This will be changed to the maximum of the existing item's
        // size and the new item's size in a subsequent PR.
        utils::estimated_histogram update_item_op_size_kb{30};

        // A size is the sum of the sizes of all items per table. This means
        // that a single BatchGetItem / BatchWriteItem updates the histogram
        // for each table that it has items in.
        // The sizes are the retrieved items' sizes grouped per table.
        utils::estimated_histogram batch_get_item_op_size_kb{30};
        // The sizes are the the written items' sizes grouped per table.
        utils::estimated_histogram batch_write_item_op_size_kb{30};
    } operation_sizes;
    // Count of authentication and authorization failures, counted if either
    // alternator_enforce_authorization or alternator_warn_authorization are
    // set to true. If both are false, no authentication or authorization
    // checks are performed, so failures are not recognized or counted.
    // "authentication" failure means the request was not signed with a valid
    // user and key combination. "authorization" failure means the request was
    // authenticated to a valid user - but this user did not have permissions
    // to perform the operation (considering RBAC settings and the user's
    // superuser status).
    uint64_t authentication_failures = 0;
    uint64_t authorization_failures = 0;
    // Miscellaneous event counters
    uint64_t total_operations = 0;
    uint64_t unsupported_operations = 0;
    uint64_t reads_before_write = 0;
    uint64_t write_using_lwt = 0;
    uint64_t shard_bounce_for_lwt = 0;
    uint64_t requests_blocked_memory = 0;
    uint64_t requests_shed = 0;
    uint64_t rcu_half_units_total = 0;
    // wcu can results from put, update, delete and index
    // Index related will be done on top of the operation it comes with
    enum wcu_types {
        PUT_ITEM,
        UPDATE_ITEM,
        DELETE_ITEM,
        INDEX,
        NUM_TYPES
    };

    uint64_t wcu_total[NUM_TYPES] = {0};
    // CQL-derived stats
    cql3::cql_stats cql_stats;

    // Enumeration of expression types only for stats
    // if needed it can be extended e.g. per operation 
    enum expression_types {
        UPDATE_EXPRESSION,
        CONDITION_EXPRESSION,
        PROJECTION_EXPRESSION,
        NUM_EXPRESSION_TYPES
    };
    struct {
        struct {
            uint64_t hits = 0;
            uint64_t misses = 0;
        } requests[NUM_EXPRESSION_TYPES];
        uint64_t evictions = 0;
    } expression_cache;
};

struct table_stats {
    table_stats(const sstring& ks, const sstring& table);
    seastar::metrics::metric_groups _metrics;
    lw_shared_ptr<stats> _stats;
};
void register_metrics(seastar::metrics::metric_groups& metrics, const stats& stats);

inline uint64_t bytes_to_kb_ceil(uint64_t bytes) {
    return (bytes + 1023) / 1024;
}

}
