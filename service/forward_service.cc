/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "service/forward_service.hh"

#include <boost/range/algorithm/remove_if.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>

#include "cql_serialization_format.hh"
#include "db/consistency_level.hh"
#include "dht/i_partitioner.hh"
#include "dht/sharder.hh"
#include "gms/gossiper.hh"
#include "idl/forward_request.dist.hh"
#include "locator/abstract_replication_strategy.hh"
#include "log.hh"
#include "message/messaging_service.hh"
#include "query-request.hh"
#include "query_ranges_to_vnodes.hh"
#include "replica/database.hh"
#include "schema.hh"
#include "schema_registry.hh"
#include "service/pager/query_pagers.hh"
#include "tracing/trace_state.hh"
#include "tracing/tracing.hh"
#include "utils/fb_utilities.hh"

#include "cql3/column_identifier.hh"
#include "cql3/cql_config.hh"
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/selection/selectable-expr.hh"
#include "cql3/selection/selectable.hh"
#include "cql3/selection/selection.hh"

namespace service {

static constexpr int DEFAULT_INTERNAL_PAGING_SIZE = 10000;
static logging::logger flogger("forward_service");

static const dht::token& end_token(const dht::partition_range& r) {
    static const dht::token max_token = dht::maximum_token();
    return r.end() ? r.end()->value().token() : max_token;
}

static inet_address_vector_replica_set get_live_endpoints(replica::keyspace& ks, const dht::token& token) {
    auto erm = ks.get_effective_replication_map();
    auto eps = erm->get_natural_endpoints_without_node_being_replaced(token);
    auto itend = boost::range::remove_if(
        eps,
        std::not1(std::bind1st(std::mem_fn(&gms::gossiper::is_alive), &gms::get_local_gossiper()))
    );
    eps.erase(itend, eps.end());
    return eps;
}

static void retain_local_endpoints(inet_address_vector_replica_set& eps) {
    auto itend = boost::range::remove_if(
        eps,
        [](gms::inet_address ep) {
            return !db::is_local(ep);
        }
    );
    eps.erase(itend, eps.end());
}

// Given an initial partition range vector, iterate through ranges owned by
// current shard.
class partition_ranges_owned_by_this_shard {
    schema_ptr _s;
    // _partition_ranges will contain a list of partition ranges that are known
    // to be owned by this node. We'll further need to split each such range to
    // the pieces owned by the current shard, using _intersecter.
    const dht::partition_range_vector _partition_ranges;
    size_t _range_idx;
    std::optional<dht::ring_position_range_sharder> _intersecter;
public:
    partition_ranges_owned_by_this_shard(schema_ptr s, dht::partition_range_vector v)
        :  _s(s)
        , _partition_ranges(v)
        , _range_idx(0)
    {}

    // Return the next partition_range owned by this shard, or nullopt when the
    // iteration ends.
    std::optional<dht::partition_range> next(const schema& s) {
        // We may need three or more iterations in the following loop if a
        // vnode doesn't intersect with the given shard at all (such a small
        // vnode is unlikely, but possible). The loop cannot be infinite
        // because each iteration of the loop advances _range_idx.
        for (;;) {
            if (_intersecter) {
                // Filter out ranges that are not owned by this shard.
                while (auto ret = _intersecter->next(s)) {
                    if (ret->shard == this_shard_id()) {
                        return {ret->ring_range};
                    }
                }

                // Done with this range, go to next one.
                ++_range_idx;
                _intersecter = std::nullopt;
            }

            if (_range_idx == _partition_ranges.size()) {
                return std::nullopt;
            }

            _intersecter.emplace(_s->get_sharder(), std::move(_partition_ranges[_range_idx]));
        }
    }
};

// `retrying_dispatcher` is a class that dispatches forward_requests to other
// nodes. In case of a failure, local retries are available - request being
// retried is executed on the super-coordinator.
class retrying_dispatcher {
    forward_service& _forwarder;
    tracing::trace_state_ptr _tr_state;
    std::optional<tracing::trace_info> _tr_info;
public:
    retrying_dispatcher(forward_service& forwarder, tracing::trace_state_ptr tr_state)
        : _forwarder(forwarder),
        _tr_state(tr_state),
        _tr_info(tracing::make_trace_info(tr_state))
    {}

    future<query::forward_result> dispatch_to_node(netw::msg_addr id, query::forward_request req) {
        if (!utils::fb_utilities::is_me(id.addr)) {
            _forwarder._stats.requests_dispatched_to_other_nodes += 1;

            // Try to send this forward_request to another node.
            // In case of a failure, do a retry locally.
            try {
                co_return co_await ser::forward_request_rpc_verbs::send_forward_request(
                    &_forwarder._messaging, id, req, _tr_info
                );
            } catch(rpc::closed_error& e) {
                flogger.warn("retrying forward_request={} on a super-coordinator after failing to send it to {} ({})", req, id, e.what());
                tracing::trace(_tr_state, "retrying forward_request={} on a super-coordinator after failing to send it to {} ({})", req, id, e.what());
            }
        }

        co_return co_await _forwarder.dispatch_to_shards(req, _tr_info);
    }
};

static dht::partition_range_vector retain_ranges_owned_by_this_shard(
    schema_ptr schema,
    dht::partition_range_vector pr
) {
    partition_ranges_owned_by_this_shard owned_iter(schema, std::move(pr));
    dht::partition_range_vector res;
    while (std::optional<dht::partition_range> p = owned_iter.next(*schema)) {
        res.push_back(*p);
    }

    return res;
}


locator::token_metadata_ptr forward_service::get_token_metadata_ptr() const noexcept {
    return _shared_token_metadata.get();
}

future<> forward_service::stop() {
    return uninit_messaging_service();
}

// Due to `cql3::selection::selection` not being serializable, it cannot be
// stored in `forward_request`. It has to mocked on the receiving node,
// based on requested reduction types.
static shared_ptr<cql3::selection::selection> mock_selection(
    const std::vector<query::forward_request::reduction_type>& reduction_types,
    schema_ptr schema,
    replica::database& db
) {
    std::vector<shared_ptr<cql3::selection::raw_selector>> raw_selectors;

    auto mock_singular_selection = [&] (const query::forward_request::reduction_type& type) {
        switch (type) {
            case query::forward_request::reduction_type::count: {
                auto selectable = cql3::selection::make_count_rows_function_expression();
                auto column_identifier = make_shared<cql3::column_identifier>("count", false);
                return make_shared<cql3::selection::raw_selector>(selectable, column_identifier);
            }
        }
    };

    for (auto const& type : reduction_types) {
        raw_selectors.emplace_back(mock_singular_selection(type));
    }

    return cql3::selection::selection::from_selectors(db.as_data_dictionary(), schema, std::move(raw_selectors));
}

future<query::forward_result> forward_service::dispatch_to_shards(
    query::forward_request req,
    std::optional<tracing::trace_info> tr_info
) {
    _stats.requests_dispatched_to_own_shards += 1;

    auto result = co_await container().map_reduce0(
        [req, tr_info] (auto& fs) {
            return fs.execute_on_this_shard(req, tr_info);
        },
        std::optional<query::forward_result>(),
        [&req] (std::optional<query::forward_result> partial, query::forward_result mapped) -> std::optional<query::forward_result>{
            if (partial) {
                mapped.merge(*partial, req.reduction_types);
            }
            return {mapped};
        }
    );

    co_return *result;
}

// This function executes forward_request on a shard.
// It retains partition ranges owned by this shard from requested partition
// ranges vector, so that only owned ones are queried.
future<query::forward_result> forward_service::execute_on_this_shard(
    query::forward_request req,
    std::optional<tracing::trace_info> tr_info
) {
    tracing::trace_state_ptr tr_state;
    if (tr_info) {
        tr_state = tracing::tracing::get_local_tracing_instance().create_session(*tr_info);
        tracing::begin(tr_state);
    }

    tracing::trace(tr_state, "Executing forward_request");
    _stats.requests_executed += 1;

    schema_ptr schema = local_schema_registry().get(req.cmd.schema_version);

    auto timeout = req.timeout;
    auto now = gc_clock::now();

    auto selection = mock_selection(req.reduction_types, schema, _db.local());
    auto query_state = make_lw_shared<service::query_state>(
        client_state::for_internal_calls(),
        tr_state,
        empty_service_permit() // FIXME: it probably shouldn't be empty.
    );
    auto query_options = make_lw_shared<cql3::query_options>(
        cql3::default_cql_config,
        req.cl,
        std::optional<std::vector<sstring_view>>(), // Represents empty names.
        std::vector<cql3::raw_value>(), // Represents empty values.
        true, // Skip metadata.
        cql3::query_options::specific_options::DEFAULT,
        cql_serialization_format::latest()
    );
    auto pager = service::pager::query_pagers::pager(
        _proxy,
        schema,
        selection,
        *query_state,
        *query_options,
        make_lw_shared<query::read_command>(std::move(req.cmd)),
        retain_ranges_owned_by_this_shard(schema, std::move(req.pr)),
        nullptr // No filtering restrictions
    );
    auto rs_builder = cql3::selection::result_set_builder(
        *selection,
        now,
        cql_serialization_format::latest(),
        std::vector<size_t>() // Represents empty GROUP BY indices.
    );

    // Execute query.
    while (!pager->is_exhausted()) {
        co_await pager->fetch_page(rs_builder, DEFAULT_INTERNAL_PAGING_SIZE, now, timeout);
    }

    co_return co_await rs_builder.with_thread_if_needed([&rs_builder, reduction_types = std::move(req.reduction_types), tr_state = std::move(tr_state)] {
        auto rs = rs_builder.build();
        auto& rows = rs->rows();
        if (rows.size() != 1) {
            flogger.error("aggregation result row count != 1");
            throw std::runtime_error("aggregation result row count != 1");
        }
        if (rows[0].size() != reduction_types.size()) {
            flogger.error("aggregation result column count does not match requested column count");
            throw std::runtime_error("aggregation result column count does not match requested column count");
        }
        query::forward_result res = { .query_results = rows[0] };

        query::forward_result::printer res_printer{
            .types = reduction_types,
            .res = res
        };
        tracing::trace(tr_state, "On shard execution result is {}", res_printer);

        return res;
    });
}

void forward_service::init_messaging_service() {
    ser::forward_request_rpc_verbs::register_forward_request(
        &_messaging,
        [this](query::forward_request req, std::optional<tracing::trace_info> tr_info) -> future<query::forward_result> {
            return dispatch_to_shards(req, tr_info);
        }
    );
}

future<> forward_service::uninit_messaging_service() {
    return ser::forward_request_rpc_verbs::unregister(&_messaging);
}

future<query::forward_result> forward_service::dispatch(query::forward_request req_, tracing::trace_state_ptr tr_state_) {
    query::forward_request req = std::move(req_);
    tracing::trace_state_ptr tr_state = std::move(tr_state_);

    schema_ptr schema = local_schema_registry().get(req.cmd.schema_version);
    replica::keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    // next_vnode is used to iterate through all vnodes produced by
    // query_ranges_to_vnodes_generator.
    auto next_vnode = [
        generator = query_ranges_to_vnodes_generator(get_token_metadata_ptr(), schema, req.pr)
    ] () mutable -> std::optional<dht::partition_range> {
        if (auto vnode = generator(1); !vnode.empty()) {
            return vnode[0];
        }
        return {};
    };

    // Group vnodes by assigned endpoint.
    std::map<netw::messaging_service::msg_addr, dht::partition_range_vector> vnodes_per_addr;
    while (std::optional<dht::partition_range> vnode = next_vnode()) {
        inet_address_vector_replica_set live_endpoints = get_live_endpoints(ks, end_token(*vnode));
        // Do not choose an endpoint outside the current datacenter if a request has a local consistency
        if (db::is_datacenter_local(req.cl)) {
            retain_local_endpoints(live_endpoints);
        }

        if (live_endpoints.empty()) {
            throw std::runtime_error("No live endpoint available");
        }

        auto endpoint_addr = netw::messaging_service::msg_addr{*live_endpoints.begin(), 0};
        vnodes_per_addr[endpoint_addr].push_back(*vnode);
    }

    tracing::trace(tr_state, "Dispatching forward_request to {} endpoints", vnodes_per_addr.size());

    retrying_dispatcher dispatcher(*this, tr_state);

    std::optional<query::forward_result> result;
    // Forward request to each endpoint and merge results.
    co_await parallel_for_each(vnodes_per_addr.begin(), vnodes_per_addr.end(),
        [this, &req, &result, &tr_state, &dispatcher] (std::pair<netw::messaging_service::msg_addr, dht::partition_range_vector> vnodes_with_addr) -> future<> {
            netw::messaging_service::msg_addr addr = vnodes_with_addr.first;
            std::optional<query::forward_result>& result_ = result;
            tracing::trace_state_ptr& tr_state_ = tr_state;

            query::forward_request req_with_modified_pr = req;
            req_with_modified_pr.pr = std::move(vnodes_with_addr.second);

            tracing::trace(tr_state_, "Sending forward_request to {}", addr);
            flogger.debug("dispatching forward_request={} to address={}", req_with_modified_pr, addr);

            query::forward_result partial_result = co_await dispatcher.dispatch_to_node(
                addr,
                req_with_modified_pr
            );

            query::forward_result::printer partial_result_printer{
                .types = req_with_modified_pr.reduction_types,
                .res = partial_result
            };
            tracing::trace(tr_state_, "Received forward_result={} from {}", partial_result_printer, addr);
            flogger.debug("received forward_result={} from {}", partial_result_printer, addr);

            if (result_) {
                result_->merge(partial_result, req_with_modified_pr.reduction_types);
            } else {
                result_ = partial_result;
            }
        }
    );

    query::forward_result::printer result_printer{
        .types = req.reduction_types,
        .res = *result
    };
    tracing::trace(tr_state, "Merged result is {}", result_printer);
    flogger.debug("merged result is {}", result_printer);

    co_return *result;
}

void forward_service::register_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("forward_service", {
        sm::make_total_operations("requests_dispatched_to_other_nodes", _stats.requests_dispatched_to_other_nodes,
             sm::description("how many forward requests were dispatched to other nodes"), {}),
        sm::make_total_operations("requests_dispatched_to_own_shards", _stats.requests_dispatched_to_own_shards,
             sm::description("how many forward requests were dispatched to local shards"), {}),
        sm::make_total_operations("requests_executed", _stats.requests_executed,
             sm::description("how many forward requests were executed"), {}),
    });
}

} // namespace service
