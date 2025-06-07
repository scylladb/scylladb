/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "stats.hh"
#include "utils/histogram_metrics_helper.hh"
#include <seastar/core/metrics.hh>
#include "utils/labels.hh"

namespace alternator {

const char* ALTERNATOR_METRICS = "alternator";
static seastar::metrics::histogram estimated_histogram_to_metrics(const utils::estimated_histogram& histogram) {
    seastar::metrics::histogram res;
    res.buckets.resize(histogram.bucket_offsets.size());
    uint64_t cumulative_count = 0;
    res.sample_count = histogram._count;
    res.sample_sum = histogram._sample_sum;
    for (size_t i = 0; i < res.buckets.size(); i++) {
        auto& v = res.buckets[i];
        v.upper_bound = histogram.bucket_offsets[i];
        cumulative_count += histogram.buckets[i];
        v.count = cumulative_count;
    }
    return res;
}

static seastar::metrics::label column_family_label("cf");
static seastar::metrics::label keyspace_label("ks");


static void register_metrics_with_optional_table(seastar::metrics::metric_groups& metrics, const stats& stats, const sstring& ks, const sstring& table) {

    // Register the
    seastar::metrics::label op("op");
    bool has_table = table.length();
    std::vector<seastar::metrics::label> aggregate_labels;
    std::vector<seastar::metrics::label_instance> labels = {alternator_label};
    if (has_table) {
        labels.push_back(column_family_label(table));
        labels.push_back(keyspace_label(ks));
        aggregate_labels.push_back(seastar::metrics::shard_label);
    }
    metrics.add_group((has_table)? "alternator_table" : "alternator", {
#define OPERATION(name, CamelCaseName) \
                seastar::metrics::make_total_operations("operation", stats.api_operations.name, \
                        seastar::metrics::description("number of operations via Alternator API"), labels)(basic_level)(op(CamelCaseName)).aggregate(aggregate_labels).set_skip_when_empty(),
#define OPERATION_LATENCY(name, CamelCaseName) \
		metrics.add_group((has_table)? "alternator_table" : "alternator", { \
                seastar::metrics::make_histogram("op_latency", \
                        seastar::metrics::description("Latency histogram of an operation via Alternator API"), labels, [&stats]{return to_metrics_histogram(stats.api_operations.name.histogram());})(op(CamelCaseName))(basic_level).aggregate({seastar::metrics::shard_label}).set_skip_when_empty()}); \
            if (!has_table) {\
            	metrics.add_group("alternator", { \
				seastar::metrics::make_summary("op_latency_summary", \
						                        seastar::metrics::description("Latency summary of an operation via Alternator API"), [&stats]{return to_metrics_summary(stats.api_operations.name.summary());})(op(CamelCaseName))(basic_level)(alternator_label).set_skip_when_empty()}); \
            }

            OPERATION(batch_get_item, "BatchGetItem")
            OPERATION(batch_write_item, "BatchWriteItem")
            OPERATION(create_backup, "CreateBackup")
            OPERATION(create_global_table, "CreateGlobalTable")
            OPERATION(delete_backup, "DeleteBackup")
            OPERATION(delete_item, "DeleteItem")
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
            OPERATION(list_streams, "ListStreams")
            OPERATION(describe_stream, "DescribeStream")
            OPERATION(get_shard_iterator, "GetShardIterator")
            OPERATION(get_records, "GetRecords")
    });
    OPERATION_LATENCY(put_item_latency, "PutItem")
    OPERATION_LATENCY(get_item_latency, "GetItem")
    OPERATION_LATENCY(delete_item_latency, "DeleteItem")
    OPERATION_LATENCY(update_item_latency, "UpdateItem")
    OPERATION_LATENCY(batch_write_item_latency, "BatchWriteItem")
    OPERATION_LATENCY(batch_get_item_latency, "BatchGetItem")
    OPERATION_LATENCY(get_records_latency, "GetRecords")
    if (!has_table) {
        // Create and delete operations are not applicable to a per-table metrics
        // only register it for the global metrics
        metrics.add_group("alternator", {
            OPERATION(create_table, "CreateTable")
            OPERATION(delete_table, "DeleteTable")

        });
    }
    metrics.add_group((has_table)? "alternator_table" : "alternator", {
            seastar::metrics::make_total_operations("unsupported_operations", stats.unsupported_operations,
                    seastar::metrics::description("number of unsupported operations via Alternator API"), labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("total_operations", stats.total_operations,
                    seastar::metrics::description("number of total operations via Alternator API"), labels)(basic_level).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("reads_before_write", stats.reads_before_write,
                    seastar::metrics::description("number of performed read-before-write operations"), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("write_using_lwt", stats.write_using_lwt,
                    seastar::metrics::description("number of writes that used LWT"), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("shard_bounce_for_lwt", stats.shard_bounce_for_lwt,
                    seastar::metrics::description("number writes that had to be bounced from this shard because of LWT requirements"), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("requests_blocked_memory", stats.requests_blocked_memory,
                    seastar::metrics::description("Counts a number of requests blocked due to memory pressure."), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("requests_shed", stats.requests_shed,
                    seastar::metrics::description("Counts a number of requests shed due to overload."), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("filtered_rows_read_total", stats.cql_stats.filtered_rows_read_total,
                    seastar::metrics::description("number of rows read during filtering operations"), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("filtered_rows_matched_total", stats.cql_stats.filtered_rows_matched_total,
                    seastar::metrics::description("number of rows read and matched during filtering operations"), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_counter("rcu_total", [&stats]{return 0.5 * stats.rcu_half_units_total;},
                    seastar::metrics::description("total number of consumed read units"), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_counter("wcu_total", stats.wcu_total[stats::wcu_types::PUT_ITEM],
                    seastar::metrics::description("total number of consumed write units"), labels)(op("PutItem")).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_counter("wcu_total", stats.wcu_total[stats::wcu_types::DELETE_ITEM],
                    seastar::metrics::description("total number of consumed write units"), labels)(op("DeleteItem")).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_counter("wcu_total", stats.wcu_total[stats::wcu_types::UPDATE_ITEM],
                    seastar::metrics::description("total number of consumed write units"), labels)(op("UpdateItem")).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_counter("wcu_total", stats.wcu_total[stats::wcu_types::INDEX],
                    seastar::metrics::description("total number of consumed write units"), labels)(op("Index")).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_total_operations("filtered_rows_dropped_total", [&stats] { return stats.cql_stats.filtered_rows_read_total - stats.cql_stats.filtered_rows_matched_total; },
                    seastar::metrics::description("number of rows read and dropped during filtering operations"), labels).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_counter("batch_item_count", seastar::metrics::description("The total number of items processed across all batches"), labels,
                    stats.api_operations.batch_write_item_batch_total)(op("BatchWriteItem")).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_counter("batch_item_count", seastar::metrics::description("The total number of items processed across all batches"), labels,
                    stats.api_operations.batch_get_item_batch_total)(op("BatchGetItem")).aggregate(aggregate_labels).set_skip_when_empty(),
            seastar::metrics::make_histogram("batch_item_count_histogram", seastar::metrics::description("Histogram of the number of items in a batch request"), labels,
                    [&stats]{ return estimated_histogram_to_metrics(stats.api_operations.batch_get_item_histogram);})(op("BatchGetItem")).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),
            seastar::metrics::make_histogram("batch_item_count_histogram", seastar::metrics::description("Histogram of the number of items in a batch request"), labels,
                    [&stats]{ return estimated_histogram_to_metrics(stats.api_operations.batch_write_item_histogram);})(op("BatchWriteItem")).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),
    });
}

void register_metrics(seastar::metrics::metric_groups& metrics, const stats& stats) {
    register_metrics_with_optional_table(metrics, stats, "", "");
}
table_stats::table_stats(const sstring& ks, const sstring& table) {
    _stats = make_lw_shared<stats>();
    register_metrics_with_optional_table(_metrics, *_stats, ks, table);
}
}
