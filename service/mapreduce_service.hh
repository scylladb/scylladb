/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "locator/abstract_replication_strategy.hh"
#include "locator/token_metadata.hh"
#include "message/messaging_service_fwd.hh"
#include "query/query-request.hh"
#include "replica/database_fwd.hh"

namespace tracing {
class trace_state_ptr;
class trace_info;
}

namespace service {

class storage_proxy;
class retrying_dispatcher;

// `mapreduce_service` is a sharded service responsible for distributing and
// executing aggregation requests across a cluster.
//
// To use this service, one needs to express its aggregation query using
// `query::mapreduce_request` struct, and call the `dispatch` method with the
// previously mentioned struct acting as an argument. What will happen after
// calling it, is as follows:
//   1. `dispatch` splits aggregation query into sub-queries. The caller of
//      this method is named a super-coordinator.
//   2. Sub-queries are distributed across some group of coordinators.
//   3. Each coordinator forwards received sub-query to all its shards.
//   3. Each shard executes received sub-query and filters query partition
//      ranges so that they are contained in the set of partitions owned by
//      that shard.
//   4. Each coordinator merges results produced by its shards and sends merged
//      result to super-coordinator.
//   5. `dispatch` merges results from all coordinators and returns merged
//      result.
//
// Splitting query into sub-queries is implemented separately for vnodes
// and for tablets.
//
// The splitting algorithm for vnodes works as follows:
//   a. Partition ranges of the original query are split into a sequence of
//      vnodes.
//   b. Each vnode in the sequence is added to a set associated with some
//      endpoint that holds this vnode (this step can be though of as grouping
//      vnodes that are held by the same nodes together).
//   a. For each vnode set created in the previous point, replace partition
//      ranges of the original query with a partition ranges represented
//      by the vnode set. This replacement will create a sub-query whose
//      recipient is the endpoint that holds all vnodes in the set.
//
// The splitting algorithm for tablets is more dynamic, and unlike
// the algorithm for vnodes, it doesn't block topology changes (i.e. ERM) for
// the whole duration of the query execution:
//   a. Prepare a set of exclusive `partition_ranges`, where each range
//      represents one tablet. This set is called `ranges_left`, because it
//      contains ranges that still need processing.
//   b. Loop until `ranges_left` is empty:
//      I.  Create `tablet_replica` -> `ranges` mapping for the current ERM
//          and `ranges_left`. Store this mapping and the number
//          representing current ERM version as `ranges_per_replica`.
//      II. In parallel, for each tablet_replica, iterate through
//          ranges_per_tablet_replica. Select independently up to two ranges
//          that are still existing in ranges_left. Remove each range
//          selected for processing from ranges_left. Before each iteration,
//          verify that ERM version has not changed. If it has,
//          return to Step I.
//
// Query splitting (vnodes) example (3 node cluster with num_tokens set to 3):
//   Original query: mapreduce_request{
//       reduction_types=[reduction_type{count}],
//       cmd=read_command{contents omitted},
//       pr={(-inf, +inf)},
//       cl=ONE,
//       timeout(ms)=4864752279
//   }
//
//   Token ring:
//
//      start_token          | end_token            | endpoint
//     ----------------------+----------------------+-----------
//      -7656156436523256816 | -6273657286650174294 | 127.0.0.2
//      -6273657286650174294 |  -885518633547994880 | 127.0.0.2
//       -885518633547994880 |  -881470678946355457 | 127.0.0.1
//       -881470678946355457 |  -589668175639820781 | 127.0.0.2
//       -589668175639820781 |  1403899953968875783 | 127.0.0.3
//       1403899953968875783 |  6175622851574774197 | 127.0.0.3
//       6175622851574774197 |  7046230184729046062 | 127.0.0.3
//       7046230184729046062 |  7090132112022535426 | 127.0.0.1
//       7090132112022535426 | -7656156436523256816 | 127.0.0.1
//
//   Created sub-queries:
//
//       mapreduce_request{
//           reduction_types=[reduction_type{count}],
//           cmd=read_command{contents omitted},
//           pr={
//               (-inf, {-7656156436523256816, end}],
//               ({-885518633547994880, end}, {-881470678946355457, end}],
//               ({7046230184729046062, end}, {7090132112022535426, end}],
//               ({7090132112022535426, end}, +inf)
//           },
//           cl=ONE,
//           timeout(ms)=4865767688
//       } for 127.0.0.1
//
//       mapreduce_request{
//           reduction_types=[reduction_type{count}],
//           cmd=read_command{contents omitted},
//           pr={
//               ({-7656156436523256816, end}, {-6273657286650174294, end}],
//               ({-6273657286650174294, end}, {-885518633547994880, end}],
//               ({-881470678946355457, end}, {-589668175639820781, end}]
//           },
//           cl=ONE,
//           timeout(ms)=4865767688
//       } for 127.0.0.2
//
//       mapreduce_request{
//           reduction_types=[reduction_type{count}],
//           cmd=read_command{contents omitted},
//           pr={
//               ({-589668175639820781, end}, {1403899953968875783, end}],
//               ({1403899953968875783, end}, {6175622851574774197, end}],
//               ({6175622851574774197, end}, {7046230184729046062, end}]
//           },
//           cl=ONE,
//           timeout(ms)=4865767688
//       } for 127.0.0.3
//
class mapreduce_service : public seastar::peering_sharded_service<mapreduce_service> {
    netw::messaging_service& _messaging;
    service::storage_proxy& _proxy;
    distributed<replica::database>& _db;
    abort_source _abort_outgoing_tasks;

    struct stats {
        uint64_t requests_dispatched_to_other_nodes = 0;
        uint64_t requests_dispatched_to_own_shards = 0;
        uint64_t requests_executed = 0;
    } _stats;
    seastar::metrics::metric_groups _metrics;

    optimized_optional<abort_source::subscription> _early_abort_subscription;
    bool _shutdown = false;

public:
    mapreduce_service(netw::messaging_service& ms, service::storage_proxy& p, distributed<replica::database> &db,
        abort_source& as)
        : _messaging(ms)
        , _proxy(p)
        , _db(db)
        , _early_abort_subscription(as.subscribe([this] () noexcept {
            _shutdown = true;
            _abort_outgoing_tasks.request_abort();
        }))
    {
        register_metrics();
        init_messaging_service();
    }

    future<> stop();

    // Splits given `mapreduce_request` and distributes execution of resulting
    // subrequests across a cluster.
    future<query::mapreduce_result> dispatch(query::mapreduce_request req, tracing::trace_state_ptr tr_state);

private:
    future<> dispatch_range_and_reduce(const locator::effective_replication_map_ptr& erm, retrying_dispatcher& dispatcher, query::mapreduce_request const& req, query::mapreduce_request&& req_with_modified_pr, locator::host_id addr, query::mapreduce_result& result_, tracing::trace_state_ptr tr_state);
    future<> dispatch_to_vnodes(schema_ptr schema, replica::column_family& cf, query::mapreduce_request& req, query::mapreduce_result& result, tracing::trace_state_ptr tr_state);
    future<> dispatch_to_tablets(schema_ptr schema, replica::column_family& cf, query::mapreduce_request& req, query::mapreduce_result& result, tracing::trace_state_ptr tr_state);

    // Used to distribute given `mapreduce_request` across shards.
    future<query::mapreduce_result> dispatch_to_shards(query::mapreduce_request req, std::optional<tracing::trace_info> tr_info);
    // Used to execute a `mapreduce_request` on a shard.
    future<query::mapreduce_result> execute_on_this_shard(query::mapreduce_request req, std::optional<tracing::trace_info> tr_info);

    void register_metrics();
    void init_messaging_service();
    future<> uninit_messaging_service();

    friend class retrying_dispatcher;
    friend class mapreduce_tablet_algorithm;
};

} // namespace service
