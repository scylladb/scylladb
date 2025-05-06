/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "stats.hh"
#include "utils/histogram_metrics_helper.hh"
#include <seastar/core/metrics.hh>

namespace alternator {

const char* ALTERNATOR_METRICS = "alternator";

stats::stats() : api_operations{} {
    // Register the
    seastar::metrics::label op("op");

    _metrics.add_group("alternator", {
#define OPERATION(name, CamelCaseName) \
                seastar::metrics::make_total_operations("operation", api_operations.name, \
                        seastar::metrics::description("number of operations via Alternator API"), {op(CamelCaseName)}).set_skip_when_empty(),
#define OPERATION_LATENCY(name, CamelCaseName) \
                seastar::metrics::make_histogram("op_latency", \
                        seastar::metrics::description("Latency histogram of an operation via Alternator API"), {op(CamelCaseName)}, [this]{return to_metrics_histogram(api_operations.name.histogram());}).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(), \
				seastar::metrics::make_summary("op_latency_summary", \
						                        seastar::metrics::description("Latency summary of an operation via Alternator API"), [this]{return to_metrics_summary(api_operations.name.summary());})(op(CamelCaseName)).set_skip_when_empty(),
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
            OPERATION_LATENCY(batch_write_item_latency, "BatchWriteItem")
            OPERATION_LATENCY(batch_get_item_latency, "BatchGetItem")
            OPERATION(list_streams, "ListStreams")
            OPERATION(describe_stream, "DescribeStream")
            OPERATION(get_shard_iterator, "GetShardIterator")
            OPERATION(get_records, "GetRecords")
            OPERATION_LATENCY(get_records_latency, "GetRecords")
    });
    _metrics.add_group("alternator", {
            seastar::metrics::make_total_operations("unsupported_operations", unsupported_operations,
                    seastar::metrics::description("number of unsupported operations via Alternator API")),
            seastar::metrics::make_total_operations("total_operations", total_operations,
                    seastar::metrics::description("number of total operations via Alternator API")),
            seastar::metrics::make_total_operations("reads_before_write", reads_before_write,
                    seastar::metrics::description("number of performed read-before-write operations")),
            seastar::metrics::make_total_operations("write_using_lwt", write_using_lwt,
                    seastar::metrics::description("number of writes that used LWT")),
            seastar::metrics::make_total_operations("shard_bounce_for_lwt", shard_bounce_for_lwt,
                    seastar::metrics::description("number writes that had to be bounced from this shard because of LWT requirements")),
            seastar::metrics::make_total_operations("requests_blocked_memory", requests_blocked_memory,
                    seastar::metrics::description("Counts a number of requests blocked due to memory pressure.")),
            seastar::metrics::make_total_operations("requests_shed", requests_shed,
                    seastar::metrics::description("Counts a number of requests shed due to overload.")),
            seastar::metrics::make_total_operations("filtered_rows_read_total", cql_stats.filtered_rows_read_total,
                    seastar::metrics::description("number of rows read during filtering operations")),
            seastar::metrics::make_total_operations("filtered_rows_matched_total", cql_stats.filtered_rows_matched_total,
                    seastar::metrics::description("number of rows read and matched during filtering operations")),
<<<<<<< HEAD
            seastar::metrics::make_counter("rcu_total", rcu_total,
                    seastar::metrics::description("total number of consumed read units, counted as half units")).set_skip_when_empty(),
||||||| parent of 5ae11746fa (Alternator: Change the WCU/RCU to use units)
            seastar::metrics::make_counter("rcu_total", rcu_total,
                    seastar::metrics::description("total number of consumed read units, counted as half units"))(alternator_label).set_skip_when_empty(),
=======
            seastar::metrics::make_counter("rcu_total", [this]{return 0.5 * rcu_half_units_total;},
                    seastar::metrics::description("total number of consumed read units"))(alternator_label).set_skip_when_empty(),
>>>>>>> 5ae11746fa (Alternator: Change the WCU/RCU to use units)
            seastar::metrics::make_counter("wcu_total", wcu_total[wcu_types::PUT_ITEM],
<<<<<<< HEAD
                    seastar::metrics::description("total number of consumed write units, counted as half units"),{op("PutItem")}).set_skip_when_empty(),
||||||| parent of 5ae11746fa (Alternator: Change the WCU/RCU to use units)
                    seastar::metrics::description("total number of consumed write units, counted as half units"),{op("PutItem")})(alternator_label).set_skip_when_empty(),
=======
                    seastar::metrics::description("total number of consumed write units"),{op("PutItem")})(alternator_label).set_skip_when_empty(),
>>>>>>> 5ae11746fa (Alternator: Change the WCU/RCU to use units)
            seastar::metrics::make_counter("wcu_total", wcu_total[wcu_types::DELETE_ITEM],
<<<<<<< HEAD
                    seastar::metrics::description("total number of consumed write units, counted as half units"),{op("DeleteItem")}).set_skip_when_empty(),
||||||| parent of 5ae11746fa (Alternator: Change the WCU/RCU to use units)
                    seastar::metrics::description("total number of consumed write units, counted as half units"),{op("DeleteItem")})(alternator_label).set_skip_when_empty(),
=======
                    seastar::metrics::description("total number of consumed write units"),{op("DeleteItem")})(alternator_label).set_skip_when_empty(),
>>>>>>> 5ae11746fa (Alternator: Change the WCU/RCU to use units)
            seastar::metrics::make_counter("wcu_total", wcu_total[wcu_types::UPDATE_ITEM],
<<<<<<< HEAD
                    seastar::metrics::description("total number of consumed write units, counted as half units"),{op("UpdateItem")}).set_skip_when_empty(),
||||||| parent of 5ae11746fa (Alternator: Change the WCU/RCU to use units)
                    seastar::metrics::description("total number of consumed write units, counted as half units"),{op("UpdateItem")})(alternator_label).set_skip_when_empty(),
=======
                    seastar::metrics::description("total number of consumed write units"),{op("UpdateItem")})(alternator_label).set_skip_when_empty(),
>>>>>>> 5ae11746fa (Alternator: Change the WCU/RCU to use units)
            seastar::metrics::make_counter("wcu_total", wcu_total[wcu_types::INDEX],
<<<<<<< HEAD
                    seastar::metrics::description("total number of consumed write units, counted as half units"),{op("Index")}).set_skip_when_empty(),
||||||| parent of 5ae11746fa (Alternator: Change the WCU/RCU to use units)
                    seastar::metrics::description("total number of consumed write units, counted as half units"),{op("Index")})(alternator_label).set_skip_when_empty(),
=======
                    seastar::metrics::description("total number of consumed write units"),{op("Index")})(alternator_label).set_skip_when_empty(),
>>>>>>> 5ae11746fa (Alternator: Change the WCU/RCU to use units)
            seastar::metrics::make_total_operations("filtered_rows_dropped_total", [this] { return cql_stats.filtered_rows_read_total - cql_stats.filtered_rows_matched_total; },
                    seastar::metrics::description("number of rows read and dropped during filtering operations")),
            seastar::metrics::make_counter("batch_item_count", seastar::metrics::description("The total number of items processed across all batches"),{op("BatchWriteItem")},
                    api_operations.batch_write_item_batch_total).set_skip_when_empty(),
            seastar::metrics::make_counter("batch_item_count", seastar::metrics::description("The total number of items processed across all batches"),{op("BatchGetItem")},
                    api_operations.batch_get_item_batch_total).set_skip_when_empty(),
    });
}


}
