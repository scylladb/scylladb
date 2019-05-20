/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "stats.hh"

#include <seastar/core/metrics.hh>

namespace alternator {

const char* ALTERNATOR_METRICS = "alternator";

stats::stats() : api_operations{} {
    // Register the
    _metrics.add_group("alternator_operation", {
#define OPERATION(name, CamelCaseName) \
                seastar::metrics::make_total_operations(#name, api_operations.name, \
                        seastar::metrics::description("number of " CamelCaseName " operations via Alternator API")),
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
    });
    _metrics.add_group("alternator", {
            seastar::metrics::make_total_operations("unsupported_operations", unsupported_operations,
                    seastar::metrics::description("number of unsupported operations via Alternator API")),
            seastar::metrics::make_total_operations("total_operations", total_operations,
                    seastar::metrics::description("number of total operations via Alternator API")),
    });
}


}
