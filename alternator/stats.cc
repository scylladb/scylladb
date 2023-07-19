/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "stats.hh"
#include "utils/histogram_metrics_helper.hh"
#include <seastar/core/metrics.hh>

namespace alternator {

const char* ALTERNATOR_METRICS = "alternator";

stats::stats() : api_operations{} {
    namespace sm = seastar::metrics;
    sm::label op("op");

    _metrics.add_group("alternator", {
#define OPERATION(name, CamelCaseName) \
                sm::make_total_operations("operation", api_operations.name, \
                        sm::description("number of operations via Alternator API"), {op(CamelCaseName)}),
#define OPERATION_LATENCY(name, CamelCaseName) \
                sm::make_histogram("op_latency", \
                        sm::description("Latency histogram of an operation via Alternator API"), {op(CamelCaseName)}, [this]{return to_metrics_histogram(api_operations.name);}),
            OPERATION(batch_get_item, "BatchGetItem")
            OPERATION(batch_write_item, "BatchWriteItem")
            OPERATION(create_backup, "CreateBackup")
            OPERATION(create_global_table, "CreateGlobalTable")
            OPERATION(create_table, "CreateTable")
            OPERATION(delete_backup, "DeleteBackup")
            OPERATION(delete_item, "DeleteItem")
            OPERATION(delete_table, "DeleteTable")
            OPERATION(describe_backup, "DescribeBackup")
            OPERATION(describe_continuous_backups, "DescribeContinuousBackups")
            OPERATION(describe_endpoints, "DescribeEndpoints")
            OPERATION(describe_global_table, "DescribeGlobalTable")
            OPERATION(describe_global_table_settings, "DescribeGlobalTableSettings")
            OPERATION(describe_limits, "DescribeLimits")
            OPERATION(describe_table, "DescribeTable")
            OPERATION(describe_time_to_live, "DescribeTimeToLive")
            OPERATION(get_item, "GetItem")
            OPERATION(list_backups, "ListBackups")
            OPERATION(list_global_tables, "ListGlobalTables")
            OPERATION(list_tables, "ListTables")
            OPERATION(list_tags_of_resource, "ListTagsOfResource")
            OPERATION(put_item, "PutItem")
            OPERATION(query, "Query")
            OPERATION(restore_table_from_backup, "RestoreTableFromBackup")
            OPERATION(restore_table_to_point_in_time, "RestoreTableToPointInTime")
            OPERATION(scan, "Scan")
            OPERATION(tag_resource, "TagResource")
            OPERATION(transact_get_items, "TransactGetItems")
            OPERATION(transact_write_items, "TransactWriteItems")
            OPERATION(untag_resource, "UntagResource")
            OPERATION(update_continuous_backups, "UpdateContinuousBackups")
            OPERATION(update_global_table, "UpdateGlobalTable")
            OPERATION(update_global_table_settings, "UpdateGlobalTableSettings")
            OPERATION(update_item, "UpdateItem")
            OPERATION(update_table, "UpdateTable")
            OPERATION(update_time_to_live, "UpdateTimeToLive")
            OPERATION_LATENCY(put_item_latency, "PutItem")
            OPERATION_LATENCY(get_item_latency, "GetItem")
            OPERATION_LATENCY(delete_item_latency, "DeleteItem")
            OPERATION_LATENCY(update_item_latency, "UpdateItem")
            OPERATION(list_streams, "ListStreams")
            OPERATION(describe_stream, "DescribeStream")
            OPERATION(get_shard_iterator, "GetShardIterator")
            OPERATION(get_records, "GetRecords")
            OPERATION_LATENCY(get_records_latency, "GetRecords")
    });
    _metrics.add_group("alternator", {
            sm::make_total_operations("unsupported_operations", unsupported_operations,
                    sm::description("number of unsupported operations via Alternator API")),
            sm::make_total_operations("total_operations", total_operations,
                    sm::description("number of total operations via Alternator API")),
            sm::make_total_operations("reads_before_write", reads_before_write,
                    sm::description("number of performed read-before-write operations")),
            sm::make_total_operations("write_using_lwt", write_using_lwt,
                    sm::description("number of writes that used LWT")),
            sm::make_total_operations("shard_bounce_for_lwt", shard_bounce_for_lwt,
                    sm::description("number writes that had to be bounced from this shard because of LWT requirements")),
            sm::make_total_operations("requests_blocked_memory", requests_blocked_memory,
                    sm::description("Counts a number of requests blocked due to memory pressure.")),
            sm::make_total_operations("requests_shed", requests_shed,
                    sm::description("Counts a number of requests shed due to overload.")),
            sm::make_total_operations("filtered_rows_read_total", cql_stats.filtered_rows_read_total,
                    sm::description("number of rows read during filtering operations")),
            sm::make_total_operations("filtered_rows_matched_total", cql_stats.filtered_rows_matched_total,
                    sm::description("number of rows read and matched during filtering operations")),
            sm::make_total_operations("filtered_rows_dropped_total", [this] { return cql_stats.filtered_rows_read_total - cql_stats.filtered_rows_matched_total; },
                    sm::description("number of rows read and dropped during filtering operations")),
            // expressions cache metrics
            sm::make_total_operations("expression_cache_hits", exp_cache.hits,
                    sm::description("Counts the number of requests which used the cache.")),
            sm::make_total_operations("expression_cache_misses", exp_cache.misses,
                    sm::description("Counts the number of requests which couldn't use the cache due to miss.")),
            sm::make_total_operations("expression_cache_evictions", exp_cache.hits,
                    sm::description("Counts the number of evictions in the cache.")),
            sm::make_total_operations("expression_cache_privileged_evictions", exp_cache.hits,
                    sm::description("Counts the number of evictions in the cache for expressions which have been used more than once.")),
            sm::make_total_operations("expression_cache_unprivileged_evictions", exp_cache.hits,
                    sm::description("Counts the number of evictions in the cache for expressions which have been used only once.")),
            sm::make_gauge("expression_cache_size",
                    [] { return exp_cache_stats_updater::size(); },
                    sm::description("Number of entries in the expression cache.")),
            sm::make_gauge("expression_cache_footprint_bytes",
                    [] { return exp_cache_stats_updater::footprint(); },
                    sm::description("Size of the expression cache.")),
    });
}

// sadly this hack is needed as loading_cache expects object with static functions
thread_local stats* exp_cache_stats = nullptr;
thread_local std::function<size_t()> exp_cache_size_getter = nullptr;
thread_local std::function<size_t()> exp_cache_footprint_getter = nullptr;;

void exp_cache_stats_updater::init(stats* s, std::function<size_t()> size_getter,
                                             std::function<size_t()> footprint_getter) {
    exp_cache_stats = s;
    exp_cache_size_getter = size_getter;
    exp_cache_footprint_getter = footprint_getter;
}

size_t exp_cache_stats_updater::size() {
    return exp_cache_size_getter();
}

size_t exp_cache_stats_updater::footprint() {
    return exp_cache_footprint_getter();
}

stats& exp_cache_stats_updater::shard_stats() {
    return *exp_cache_stats;
}

}
