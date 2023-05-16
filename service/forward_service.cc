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
#include <stdexcept>

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
#include "seastar/core/do_with.hh"
#include "seastar/core/future.hh"
#include "seastar/core/on_internal_error.hh"
#include "seastar/core/when_all.hh"
#include "service/pager/query_pagers.hh"
#include "tracing/trace_state.hh"
#include "tracing/tracing.hh"
#include "types.hh"
#include "utils/fb_utilities.hh"
#include "service/storage_proxy.hh"

#include "cql3/functions/aggregate_function.hh"
#include "cql3/column_identifier.hh"
#include "cql3/cql_config.hh"
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/selection/selectable-expr.hh"
#include "cql3/selection/selectable.hh"
#include "cql3/selection/selection.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"

namespace service {

static constexpr int DEFAULT_INTERNAL_PAGING_SIZE = 10000;
static logging::logger flogger("forward_service");

static std::vector<::shared_ptr<db::functions::aggregate_function>> get_functions(const query::forward_request& request);

class forward_aggregates {
private:
    std::vector<::shared_ptr<db::functions::aggregate_function>> _funcs;
    std::vector<std::unique_ptr<db::functions::aggregate_function::aggregate>> _aggrs;

public:
    forward_aggregates(const query::forward_request& request);
    void merge(query::forward_result& result, query::forward_result&& other);
    void finalize(query::forward_result& result);

    template<typename Func>
    auto with_thread_if_needed(Func&& func) const {
        if (requires_thread()) {
            return async(std::move(func));
        } else {
            return futurize_invoke(std::move(func));
        }
    }

    bool requires_thread() const {
        return std::any_of(_funcs.cbegin(), _funcs.cend(), [](const ::shared_ptr<db::functions::aggregate_function>& f) {
            return f->requires_thread();
        });
    }
};

forward_aggregates::forward_aggregates(const query::forward_request& request) {
    _funcs = get_functions(request);
    std::vector<std::unique_ptr<db::functions::aggregate_function::aggregate>> aggrs;

    for (auto& func: _funcs) {
        aggrs.push_back(func->new_aggregate());
    }
    _aggrs = std::move(aggrs);
}

void forward_aggregates::merge(query::forward_result &result, query::forward_result&& other) {
    if (result.query_results.empty()) {
        result.query_results = std::move(other.query_results);
        return;
    } else if (other.query_results.empty()) {
        return;
    }

    if (result.query_results.size() != other.query_results.size() || result.query_results.size() != _aggrs.size()) {
        on_internal_error(
            flogger,
            format("forward_aggregates::merge(): operation cannot be completed due to invalid argument sizes. "
                    "this.aggrs.size(): {} "
                    "result.query_result.size(): {} "
                    "other.query_results.size(): {} ",
                    _aggrs.size(), result.query_results.size(), other.query_results.size())
        );
    }

    for (size_t i = 0; i < _aggrs.size(); i++) {
        _aggrs[i]->set_accumulator(result.query_results[i]);
        _aggrs[i]->reduce(std::move(other.query_results[i]));
        result.query_results[i] = _aggrs[i]->get_accumulator();
    }
}

void forward_aggregates::finalize(query::forward_result &result) {
    if (result.query_results.empty()) {
        // An empty result means that we didn't send the aggregation request
        // to any node. I.e., it was a query that matched no partition, such
        // as "WHERE p IN ()". We need to build a fake result with the result
        // of empty aggregation.
        for (size_t i = 0; i < _aggrs.size(); i++) {
            result.query_results.push_back(_aggrs[i]->compute());
        }
        return;
    }
    if (result.query_results.size() != _aggrs.size()) {
        on_internal_error(
            flogger,
            format("forward_aggregates::finalize(): operation cannot be completed due to invalid argument sizes. "
                    "this.aggrs.size(): {} "
                    "result.query_result.size(): {} ",
                    _aggrs.size(), result.query_results.size())
        );
    }

    for (size_t i = 0; i < _aggrs.size(); i++) {
        _aggrs[i]->set_accumulator(result.query_results[i]);
        result.query_results[i] = _aggrs[i]->compute();
    }
}

static std::vector<::shared_ptr<db::functions::aggregate_function>> get_functions(const query::forward_request& request) {
    
    schema_ptr schema = local_schema_registry().get(request.cmd.schema_version);
    std::vector<::shared_ptr<db::functions::aggregate_function>> aggrs;

    auto name_as_type = [&] (const sstring& name) -> data_type {
        auto t = schema->get_column_definition(to_bytes(name))->type->underlying_type();

        if (t->is_counter()) {
            return long_type;
        }
        return t;
    };

    for (size_t i = 0; i < request.reduction_types.size(); i++) {
        ::shared_ptr<db::functions::aggregate_function> aggr;

        if (!request.aggregation_infos) {
            if (request.reduction_types[i] == query::forward_request::reduction_type::aggregate) {
                throw std::runtime_error("No aggregation info for reduction type aggregation.");
            }

            auto name = db::functions::function_name::native_function("countRows");
            auto func = cql3::functions::functions::find(name, {});
            aggr = dynamic_pointer_cast<db::functions::aggregate_function>(func);
            if (!aggr) {
                throw std::runtime_error("Count function not found.");
            }
        } else {
            auto& info = request.aggregation_infos.value()[i];
            auto types = boost::copy_range<std::vector<data_type>>(info.column_names | boost::adaptors::transformed(name_as_type));
            
            auto func = cql3::functions::functions::mock_get(info.name, types);
            if (!func) {
                throw std::runtime_error(format("Cannot mock aggregate function {}", info.name));    
            }

            aggr = dynamic_pointer_cast<db::functions::aggregate_function>(func);
            if (!aggr) {
                throw std::runtime_error(format("Aggregate function {} not found.", info.name));
            }
        }
        aggrs.emplace_back(aggr);
    }
    
    return aggrs;
}

static const dht::token& end_token(const dht::partition_range& r) {
    static const dht::token max_token = dht::maximum_token();
    return r.end() ? r.end()->value().token() : max_token;
}

static void retain_local_endpoints(const locator::topology& topo, inet_address_vector_replica_set& eps) {
    auto itend = boost::range::remove_if(eps, std::not_fn(topo.get_local_dc_filter()));
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
        if (utils::fb_utilities::is_me(id.addr)) {
            return _forwarder.dispatch_to_shards(req, _tr_info);
        }

        _forwarder._stats.requests_dispatched_to_other_nodes += 1;

        // Check for a shutdown request before sending a forward_request to
        // another node. During the drain process, the messaging service is shut
        // down early (but not earlier than the forward_service::shutdown
        // invocation), so by performing this check, we can prevent hanging on
        // the RPC call.
        if (_forwarder._shutdown) {
            return make_exception_future<query::forward_result>(std::runtime_error("forward_service is shutting down"));
        }

        // Try to send this forward_request to another node.
        return do_with(id, req, [this] (netw::msg_addr& id, query::forward_request& req) -> future<query::forward_result> {
            return ser::forward_request_rpc_verbs::send_forward_request(
                &_forwarder._messaging, id, req, _tr_info
            ).handle_exception_type([this, &req, &id] (rpc::closed_error& e) -> future<query::forward_result> {
                if (_forwarder._shutdown) {
                    // Do not retry if shutting down.
                    return make_exception_future<query::forward_result>(e);
                }

                // In case of forwarding failure, retry using super-coordinator as a coordinator
                flogger.warn("retrying forward_request={} on a super-coordinator after failing to send it to {} ({})", req, id, e.what());
                tracing::trace(_tr_state, "retrying forward_request={} on a super-coordinator after failing to send it to {} ({})", req, id, e.what());

                return _forwarder.dispatch_to_shards(req, _tr_info);
            });
        });
    }
};

locator::token_metadata_ptr forward_service::get_token_metadata_ptr() const noexcept {
    return _shared_token_metadata.get();
}

future<> forward_service::shutdown() {
    _shutdown = true;
    return make_ready_future<>();
}

future<> forward_service::stop() {
    return uninit_messaging_service();
}

// Due to `cql3::selection::selection` not being serializable, it cannot be
// stored in `forward_request`. It has to mocked on the receiving node,
// based on requested reduction types.
static shared_ptr<cql3::selection::selection> mock_selection(
    query::forward_request& request,
    schema_ptr schema,
    replica::database& db
) {
    std::vector<shared_ptr<cql3::selection::raw_selector>> raw_selectors;

    auto functions = get_functions(request);

    auto mock_singular_selection = [&] (
        const ::shared_ptr<db::functions::aggregate_function>& aggr_function,
        const query::forward_request::reduction_type& reduction, 
        const std::optional<query::forward_request::aggregation_info>& info
    ) {
        auto name_as_expression = [] (const sstring& name) -> cql3::expr::expression {
            return cql3::expr::unresolved_identifier {
                make_shared<cql3::column_identifier_raw>(name, true)
            };
        };

        if (reduction == query::forward_request::reduction_type::count) {
            auto selectable = cql3::selection::make_count_rows_function_expression();
            auto column_identifier = make_shared<cql3::column_identifier>("count", false);
            return make_shared<cql3::selection::raw_selector>(selectable, column_identifier);
        }

        if (!info) {
            on_internal_error(flogger, "No aggregation info for reduction type aggregation.");
        }

        auto reducible_aggr = aggr_function->reducible_aggregate_function();
        auto arg_exprs =boost::copy_range<std::vector<cql3::expr::expression>>(info->column_names | boost::adaptors::transformed(name_as_expression));
        auto fc_expr = cql3::expr::function_call{reducible_aggr, arg_exprs};
        auto column_identifier = make_shared<cql3::column_identifier>(info->name.name, false);
        return make_shared<cql3::selection::raw_selector>(fc_expr, column_identifier);
    };

    for (size_t i = 0; i < request.reduction_types.size(); i++) {
        auto info = (request.aggregation_infos) ? std::optional(request.aggregation_infos->at(i)) : std::nullopt;
        raw_selectors.emplace_back(mock_singular_selection(functions[i], request.reduction_types[i], info));
    }

    return cql3::selection::selection::from_selectors(db.as_data_dictionary(), schema, std::move(raw_selectors));
}

future<query::forward_result> forward_service::dispatch_to_shards(
    query::forward_request req,
    std::optional<tracing::trace_info> tr_info
) {
    _stats.requests_dispatched_to_own_shards += 1;
    std::optional<query::forward_result> result;
    std::vector<future<query::forward_result>> futures;

    for (const auto& s : smp::all_cpus()) {
        futures.push_back(container().invoke_on(s, [req, tr_info] (auto& fs) {
            return fs.execute_on_this_shard(req, tr_info);
        }));
    }
    auto results = co_await when_all_succeed(futures.begin(), futures.end());

    forward_aggregates aggrs(req);
    co_return co_await aggrs.with_thread_if_needed([&aggrs, req, results = std::move(results), result = std::move(result)] () mutable {
        for (auto&& r : results) {
            if (result) {
                aggrs.merge(*result, std::move(r));
            }
            else {
                result = r;
            }
        }

        flogger.debug("on node execution result is {}", seastar::value_of([&req, &result] {
            return query::forward_result::printer {
                .functions = get_functions(req),
                .res = *result
            };})
        );

        return *result;
    });
}

static lowres_clock::time_point compute_timeout(const query::forward_request& req) {
    lowres_system_clock::duration time_left = req.timeout - lowres_system_clock::now();
    lowres_clock::time_point timeout_point = lowres_clock::now() + time_left;

    return timeout_point;
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

    auto timeout = compute_timeout(req);
    auto now = gc_clock::now();

    auto selection = mock_selection(req, schema, _db.local());
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
        cql3::query_options::specific_options::DEFAULT
    );

    auto rs_builder = cql3::selection::result_set_builder(
        *selection,
        now,
        std::vector<size_t>() // Represents empty GROUP BY indices.
    );

    // We serve up to 256 ranges at a time to avoid allocating a huge vector for ranges
    static constexpr size_t max_ranges = 256;
    dht::partition_range_vector ranges_owned_by_this_shard;
    ranges_owned_by_this_shard.reserve(std::min(max_ranges, req.pr.size()));
    partition_ranges_owned_by_this_shard owned_iter(schema, std::move(req.pr));

    std::optional<dht::partition_range> current_range;
    do {
        while ((current_range = owned_iter.next(*schema))) {
            ranges_owned_by_this_shard.push_back(std::move(*current_range));
            if (ranges_owned_by_this_shard.size() >= max_ranges) {
                break;
            }
        }
        if (ranges_owned_by_this_shard.empty()) {
            break;
        }
        flogger.trace("Forwarding to {} ranges owned by this shard", ranges_owned_by_this_shard.size());

        auto pager = service::pager::query_pagers::pager(
            _proxy,
            schema,
            selection,
            *query_state,
            *query_options,
            make_lw_shared<query::read_command>(req.cmd),
            std::move(ranges_owned_by_this_shard),
            nullptr // No filtering restrictions
        );

        // Execute query.
        while (!pager->is_exhausted()) {
            // It is necessary to check for a shutdown request before each
            // fetch_page operation. During the drain process, the messaging
            // service is shut down early (but not earlier than the
            // forward_service::shutdown invocation), so by performing this
            // check, we can prevent hanging on the RPC call (which can be made
            // during fetching a page).
            if (_shutdown) {
                throw std::runtime_error("forward_service is shutting down");
            }

            co_await pager->fetch_page(rs_builder, DEFAULT_INTERNAL_PAGING_SIZE, now, timeout);
        }

        ranges_owned_by_this_shard.clear();
    } while (current_range);

    co_return co_await rs_builder.with_thread_if_needed([&req, &rs_builder, reductions = req.reduction_types, tr_state = std::move(tr_state)] {
        auto rs = rs_builder.build();
        auto& rows = rs->rows();
        if (rows.size() != 1) {
            flogger.error("aggregation result row count != 1");
            throw std::runtime_error("aggregation result row count != 1");
        }
        if (rows[0].size() != reductions.size()) {
            flogger.error("aggregation result column count does not match requested column count");
            throw std::runtime_error("aggregation result column count does not match requested column count");
        }
        query::forward_result res = { .query_results = rows[0] };

        auto printer = seastar::value_of([&req, &res] {
            return query::forward_result::printer {
                .functions = get_functions(req),
                .res = res
            };
        });
        tracing::trace(tr_state, "On shard execution result is {}", printer);
        flogger.debug("on shard execution result is {}", printer);

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

future<query::forward_result> forward_service::dispatch(query::forward_request req, tracing::trace_state_ptr tr_state) {
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
    const auto& topo = get_token_metadata_ptr()->get_topology();
    while (std::optional<dht::partition_range> vnode = next_vnode()) {
        auto erm = ks.get_effective_replication_map();
        inet_address_vector_replica_set live_endpoints = _proxy.get_live_endpoints(*erm, end_token(*vnode));
        // Do not choose an endpoint outside the current datacenter if a request has a local consistency
        if (db::is_datacenter_local(req.cl)) {
            retain_local_endpoints(topo, live_endpoints);
        }

        if (live_endpoints.empty()) {
            throw std::runtime_error("No live endpoint available");
        }

        auto endpoint_addr = netw::messaging_service::msg_addr{*live_endpoints.begin(), 0};
        vnodes_per_addr[endpoint_addr].push_back(*vnode);
    }

    tracing::trace(tr_state, "Dispatching forward_request to {} endpoints", vnodes_per_addr.size());
    flogger.debug("dispatching forward_request to {} endpoints", vnodes_per_addr.size());

    retrying_dispatcher dispatcher(*this, tr_state);
    query::forward_result result;
    
    return do_with(std::move(dispatcher), std::move(result), std::move(vnodes_per_addr), std::move(req), std::move(tr_state),
        [] (
            retrying_dispatcher& dispatcher,
            query::forward_result& result,
            std::map<netw::messaging_service::msg_addr, dht::partition_range_vector>& vnodes_per_addr,
            query::forward_request& req,
            tracing::trace_state_ptr& tr_state
        )-> future<query::forward_result> {
            return parallel_for_each(vnodes_per_addr.begin(), vnodes_per_addr.end(),
                [&req, &result, &tr_state, &dispatcher] (
                    std::pair<netw::messaging_service::msg_addr, dht::partition_range_vector> vnodes_with_addr
                ) {
                    netw::messaging_service::msg_addr addr = vnodes_with_addr.first;
                    query::forward_result& result_ = result;
                    tracing::trace_state_ptr& tr_state_ = tr_state;
                    retrying_dispatcher& dispatcher_ = dispatcher;

                    query::forward_request req_with_modified_pr = req;
                    req_with_modified_pr.pr = std::move(vnodes_with_addr.second);

                    tracing::trace(tr_state_, "Sending forward_request to {}", addr);
                    flogger.debug("dispatching forward_request={} to address={}", req_with_modified_pr, addr);

                    return dispatcher_.dispatch_to_node(addr, req_with_modified_pr).then(
                        [&req, addr = std::move(addr), &result_, tr_state_ = std::move(tr_state_)] (
                            query::forward_result partial_result
                        ) mutable {
                            auto partial_printer = seastar::value_of([&req, &partial_result] { 
                                return query::forward_result::printer {
                                    .functions = get_functions(req),
                                    .res = partial_result
                                };
                            });
                            tracing::trace(tr_state_, "Received forward_result={} from {}", partial_printer, addr);
                            flogger.debug("received forward_result={} from {}", partial_printer, addr);
                            
                            return do_with(forward_aggregates(req), [&result_, partial_result = std::move(partial_result)] (forward_aggregates& aggrs) mutable {
                                return aggrs.with_thread_if_needed([&result_, &aggrs, partial_result = std::move(partial_result)] () mutable {
                                    aggrs.merge(result_, std::move(partial_result));
                                });
                            });
                    });       
                }
            ).then(
                [&result, &req, &tr_state] () -> future<query::forward_result> {
                    forward_aggregates aggrs(req);
                    const bool requires_thread = aggrs.requires_thread();

                    auto merge_result = [&result, &req, &tr_state, aggrs = std::move(aggrs)] () mutable {
                        auto printer = seastar::value_of([&req, &result] {
                            return query::forward_result::printer {
                                .functions = get_functions(req),
                                .res = result
                            };
                        });
                        tracing::trace(tr_state, "Merged result is {}", printer);
                        flogger.debug("merged result is {}", printer);

                        aggrs.finalize(result);
                        return result;
                    };
                    if (requires_thread) {
                        return seastar::async(std::move(merge_result));
                    } else {
                        return make_ready_future<query::forward_result>(merge_result());
                    }
                }
            );
        }
    );
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
