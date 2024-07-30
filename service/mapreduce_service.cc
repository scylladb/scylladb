/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "service/mapreduce_service.hh"

#include <boost/range/algorithm/remove_if.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>
#include <stdexcept>

#include "db/consistency_level.hh"
#include "dht/sharder.hh"
#include "gms/gossiper.hh"
#include "idl/mapreduce_request.dist.hh"
#include "locator/abstract_replication_strategy.hh"
#include "log.hh"
#include "message/messaging_service.hh"
#include "query-request.hh"
#include "query_ranges_to_vnodes.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "schema/schema_registry.hh"
#include <seastar/core/future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/when_all.hh>
#include "service/pager/query_pagers.hh"
#include "tracing/trace_state.hh"
#include "tracing/tracing.hh"
#include "types/types.hh"
#include "service/storage_proxy.hh"

#include "cql3/column_identifier.hh"
#include "cql3/cql_config.hh"
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/selection/selection.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/aggregate_fcts.hh"
#include "cql3/expr/expr-utils.hh"

namespace service {

static constexpr int DEFAULT_INTERNAL_PAGING_SIZE = 10000;
static logging::logger flogger("forward_service"); // not "mapreduce", for compatibility with dtest

static std::vector<::shared_ptr<db::functions::aggregate_function>> get_functions(const query::mapreduce_request& request);

class mapreduce_aggregates {
private:
    std::vector<::shared_ptr<db::functions::aggregate_function>> _funcs;
    std::vector<db::functions::stateless_aggregate_function> _aggrs;
public:
    mapreduce_aggregates(const query::mapreduce_request& request);
    void merge(query::mapreduce_result& result, query::mapreduce_result&& other);
    void finalize(query::mapreduce_result& result);

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

mapreduce_aggregates::mapreduce_aggregates(const query::mapreduce_request& request) {
    _funcs = get_functions(request);
    std::vector<db::functions::stateless_aggregate_function> aggrs;

    for (auto& func: _funcs) {
        aggrs.push_back(func->get_aggregate());
    }
    _aggrs = std::move(aggrs);
}

void mapreduce_aggregates::merge(query::mapreduce_result &result, query::mapreduce_result&& other) {
    if (result.query_results.empty()) {
        result.query_results = std::move(other.query_results);
        return;
    } else if (other.query_results.empty()) {
        return;
    }

    if (result.query_results.size() != other.query_results.size() || result.query_results.size() != _aggrs.size()) {
        on_internal_error(
            flogger,
            format("mapreduce_aggregates::merge(): operation cannot be completed due to invalid argument sizes. "
                    "this.aggrs.size(): {} "
                    "result.query_result.size(): {} "
                    "other.query_results.size(): {} ",
                    _aggrs.size(), result.query_results.size(), other.query_results.size())
        );
    }

    for (size_t i = 0; i < _aggrs.size(); i++) {
        result.query_results[i] = _aggrs[i].state_reduction_function->execute(std::vector({std::move(result.query_results[i]), std::move(other.query_results[i])}));
    }
}

void mapreduce_aggregates::finalize(query::mapreduce_result &result) {
    if (result.query_results.empty()) {
        // An empty result means that we didn't send the aggregation request
        // to any node. I.e., it was a query that matched no partition, such
        // as "WHERE p IN ()". We need to build a fake result with the result
        // of empty aggregation.
        for (size_t i = 0; i < _aggrs.size(); i++) {
            result.query_results.push_back(_aggrs[i].state_to_result_function
                    ? _aggrs[i].state_to_result_function->execute(std::vector({_aggrs[i].initial_state}))
                    : _aggrs[i].initial_state);
        }
        return;
    }
    if (result.query_results.size() != _aggrs.size()) {
        on_internal_error(
            flogger,
            format("mapreduce_aggregates::finalize(): operation cannot be completed due to invalid argument sizes. "
                    "this.aggrs.size(): {} "
                    "result.query_result.size(): {} ",
                    _aggrs.size(), result.query_results.size())
        );
    }

    for (size_t i = 0; i < _aggrs.size(); i++) {
        result.query_results[i] = _aggrs[i].state_to_result_function
            ? _aggrs[i].state_to_result_function->execute(std::vector({std::move(result.query_results[i])}))
            : result.query_results[i];
    }
}

static std::vector<::shared_ptr<db::functions::aggregate_function>> get_functions(const query::mapreduce_request& request) {
    
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
            if (request.reduction_types[i] == query::mapreduce_request::reduction_type::aggregate) {
                throw std::runtime_error("No aggregation info for reduction type aggregation.");
            }

            auto name = db::functions::function_name::native_function("countRows");
            auto func = cql3::functions::instance().find(name, {});
            aggr = dynamic_pointer_cast<db::functions::aggregate_function>(func);
            if (!aggr) {
                throw std::runtime_error("Count function not found.");
            }
        } else {
            auto& info = request.aggregation_infos.value()[i];
            auto types = boost::copy_range<std::vector<data_type>>(info.column_names | boost::adaptors::transformed(name_as_type));
            
            auto func = cql3::functions::instance().mock_get(info.name, types);
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
    locator::effective_replication_map_ptr _erm;
public:
    partition_ranges_owned_by_this_shard(schema_ptr s, dht::partition_range_vector v)
        :  _s(s)
        , _partition_ranges(v)
        , _range_idx(0)
        , _erm(_s->table().get_effective_replication_map())
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

            _intersecter.emplace(_erm->get_sharder(*_s), std::move(_partition_ranges[_range_idx]));
        }
    }
};

// `retrying_dispatcher` is a class that dispatches mapreduce_requests to other
// nodes. In case of a failure, local retries are available - request being
// retried is executed on the super-coordinator.
class retrying_dispatcher {
    mapreduce_service& _mapreducer;
    tracing::trace_state_ptr _tr_state;
    std::optional<tracing::trace_info> _tr_info;
public:
    retrying_dispatcher(mapreduce_service& mapreducer, tracing::trace_state_ptr tr_state)
        : _mapreducer(mapreducer),
        _tr_state(tr_state),
        _tr_info(tracing::make_trace_info(tr_state))
    {}

    future<query::mapreduce_result> dispatch_to_node(netw::msg_addr id, query::mapreduce_request req) {
        auto my_address = _mapreducer._messaging.broadcast_address();
        if (id.addr == my_address) {
            co_return co_await _mapreducer.dispatch_to_shards(req, _tr_info);
        }

        _mapreducer._stats.requests_dispatched_to_other_nodes += 1;

        // Check for a shutdown request before sending a mapreduce_request to
        // another node. During the drain process, the messaging service is shut
        // down early (but not earlier than the mapreduce_service::shutdown
        // invocation), so by performing this check, we can prevent hanging on
        // the RPC call.
        if (_mapreducer._shutdown) {
            throw std::runtime_error("mapreduce_service is shutting down");
        }

        // Try to send this mapreduce_request to another node.
        try {
            co_return co_await ser::mapreduce_request_rpc_verbs::send_mapreduce_request(
                &_mapreducer._messaging, id, req, _tr_info
            );
        } catch (rpc::closed_error& e) {
            if (_mapreducer._shutdown) {
                // Do not retry if shutting down.
                throw;
            }
            // In case of mapreduce failure, retry using super-coordinator as a coordinator
            flogger.warn("retrying mapreduce_request={} on a super-coordinator after failing to send it to {} ({})", req, id, e.what());
            tracing::trace(_tr_state, "retrying mapreduce_request={} on a super-coordinator after failing to send it to {} ({})", req, id, e.what());
            // Fall through since we cannot co_await in a catch block.
        }
        co_return co_await _mapreducer.dispatch_to_shards(req, _tr_info);
    }
};

locator::token_metadata_ptr mapreduce_service::get_token_metadata_ptr() const noexcept {
    return _shared_token_metadata.get();
}

future<> mapreduce_service::stop() {
    return uninit_messaging_service();
}

// Due to `cql3::selection::selection` not being serializable, it cannot be
// stored in `mapreduce_request`. It has to mocked on the receiving node,
// based on requested reduction types.
static shared_ptr<cql3::selection::selection> mock_selection(
    query::mapreduce_request& request,
    schema_ptr schema,
    replica::database& db
) {
    std::vector<cql3::selection::prepared_selector> prepared_selectors;

    auto functions = get_functions(request);

    auto mock_singular_selection = [&] (
        const ::shared_ptr<db::functions::aggregate_function>& aggr_function,
        const query::mapreduce_request::reduction_type& reduction,
        const std::optional<query::mapreduce_request::aggregation_info>& info
    ) {
        auto name_as_expression = [] (const sstring& name) -> cql3::expr::expression {
            constexpr bool keep_case = true;
            return cql3::expr::unresolved_identifier {
                make_shared<cql3::column_identifier_raw>(name, keep_case)
            };
        };

        if (reduction == query::mapreduce_request::reduction_type::count) {
            auto count_expr = cql3::expr::function_call{
                .func = cql3::functions::aggregate_fcts::make_count_rows_function(),
                .args = {},
            };
            auto column_identifier = make_shared<cql3::column_identifier>("count", false);
            return cql3::selection::prepared_selector{std::move(count_expr), column_identifier};
        }

        if (!info) {
            on_internal_error(flogger, "No aggregation info for reduction type aggregation.");
        }

        auto reducible_aggr = aggr_function->reducible_aggregate_function();
        auto arg_exprs =boost::copy_range<std::vector<cql3::expr::expression>>(info->column_names | boost::adaptors::transformed(name_as_expression));
        auto fc_expr = cql3::expr::function_call{reducible_aggr, arg_exprs};
        auto column_identifier = make_shared<cql3::column_identifier>(info->name.name, false);
        auto prepared_expr = cql3::expr::prepare_expression(fc_expr, db.as_data_dictionary(), "", schema.get(), nullptr);
        return cql3::selection::prepared_selector{std::move(prepared_expr), column_identifier};
    };

    for (size_t i = 0; i < request.reduction_types.size(); i++) {
        auto info = (request.aggregation_infos) ? std::optional(request.aggregation_infos->at(i)) : std::nullopt;
        prepared_selectors.emplace_back(mock_singular_selection(functions[i], request.reduction_types[i], info));
    }

    return cql3::selection::selection::from_selectors(db.as_data_dictionary(), schema, schema->ks_name(), std::move(prepared_selectors));
}

future<query::mapreduce_result> mapreduce_service::dispatch_to_shards(
    query::mapreduce_request req,
    std::optional<tracing::trace_info> tr_info
) {
    _stats.requests_dispatched_to_own_shards += 1;
    std::optional<query::mapreduce_result> result;
    std::vector<future<query::mapreduce_result>> futures;

    for (const auto& s : smp::all_cpus()) {
        futures.push_back(container().invoke_on(s, [req, tr_info] (auto& fs) {
            return fs.execute_on_this_shard(req, tr_info);
        }));
    }
    auto results = co_await when_all_succeed(futures.begin(), futures.end());

    mapreduce_aggregates aggrs(req);
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
            return query::mapreduce_result::printer {
                .functions = get_functions(req),
                .res = *result
            };})
        );

        return *result;
    });
}

static lowres_clock::time_point compute_timeout(const query::mapreduce_request& req) {
    lowres_system_clock::duration time_left = req.timeout - lowres_system_clock::now();
    lowres_clock::time_point timeout_point = lowres_clock::now() + time_left;

    return timeout_point;
}

// This function executes mapreduce_request on a shard.
// It retains partition ranges owned by this shard from requested partition
// ranges vector, so that only owned ones are queried.
future<query::mapreduce_result> mapreduce_service::execute_on_this_shard(
    query::mapreduce_request req,
    std::optional<tracing::trace_info> tr_info
) {
    tracing::trace_state_ptr tr_state;
    if (tr_info) {
        tr_state = tracing::tracing::get_local_tracing_instance().create_session(*tr_info);
        tracing::begin(tr_state);
    }

    tracing::trace(tr_state, "Executing mapreduce_request");
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
            // mapreduce_service::shutdown invocation), so by performing this
            // check, we can prevent hanging on the RPC call (which can be made
            // during fetching a page).
            if (_shutdown) {
                throw std::runtime_error("mapreduce_service is shutting down");
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
        query::mapreduce_result res = { .query_results = boost::copy_range<std::vector<bytes_opt>>(rows[0] | boost::adaptors::transformed([] (const managed_bytes_opt& x) { return to_bytes_opt(x); })) };

        auto printer = seastar::value_of([&req, &res] {
            return query::mapreduce_result::printer {
                .functions = get_functions(req),
                .res = res
            };
        });
        tracing::trace(tr_state, "On shard execution result is {}", printer);
        flogger.debug("on shard execution result is {}", printer);

        return res;
    });
}

void mapreduce_service::init_messaging_service() {
    ser::mapreduce_request_rpc_verbs::register_mapreduce_request(
        &_messaging,
        [this](query::mapreduce_request req, std::optional<tracing::trace_info> tr_info) -> future<query::mapreduce_result> {
            return dispatch_to_shards(req, tr_info);
        }
    );
}

future<> mapreduce_service::uninit_messaging_service() {
    return ser::mapreduce_request_rpc_verbs::unregister(&_messaging);
}

future<query::mapreduce_result> mapreduce_service::dispatch(query::mapreduce_request req, tracing::trace_state_ptr tr_state) {
    schema_ptr schema = local_schema_registry().get(req.cmd.schema_version);
    replica::table& cf = _db.local().find_column_family(schema);
    auto erm = cf.get_effective_replication_map();
    // next_vnode is used to iterate through all vnodes produced by
    // query_ranges_to_vnodes_generator.
    auto next_vnode = [
        generator = query_ranges_to_vnodes_generator(erm->make_splitter(), schema, req.pr)
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
        inet_address_vector_replica_set live_endpoints = _proxy.get_live_endpoints(*erm, end_token(*vnode));
        // Do not choose an endpoint outside the current datacenter if a request has a local consistency
        if (db::is_datacenter_local(req.cl)) {
            retain_local_endpoints(topo, live_endpoints);
        }

        if (live_endpoints.empty()) {
            throw std::runtime_error("No live endpoint available");
        }

        auto endpoint_addr = netw::messaging_service::msg_addr{*live_endpoints.begin(), 0};
        vnodes_per_addr[endpoint_addr].push_back(std::move(*vnode));
        // can potentially stall e.g. with a large tablet count.
        co_await coroutine::maybe_yield();
    }

    tracing::trace(tr_state, "Dispatching mapreduce_request to {} endpoints", vnodes_per_addr.size());
    flogger.debug("dispatching mapreduce_request to {} endpoints", vnodes_per_addr.size());

    retrying_dispatcher dispatcher(*this, tr_state);
    query::mapreduce_result result;

    co_await coroutine::parallel_for_each(vnodes_per_addr.begin(), vnodes_per_addr.end(),
            [&] (std::pair<const netw::messaging_service::msg_addr, dht::partition_range_vector>& vnodes_with_addr) -> future<> {
        netw::messaging_service::msg_addr addr = vnodes_with_addr.first;
        query::mapreduce_result& result_ = result;
        tracing::trace_state_ptr& tr_state_ = tr_state;
        retrying_dispatcher& dispatcher_ = dispatcher;

        query::mapreduce_request req_with_modified_pr = req;
        req_with_modified_pr.pr = std::move(vnodes_with_addr.second);

        tracing::trace(tr_state_, "Sending mapreduce_request to {}", addr);
        flogger.debug("dispatching mapreduce_request={} to address={}", req_with_modified_pr, addr);

        query::mapreduce_result partial_result = co_await dispatcher_.dispatch_to_node(addr, std::move(req_with_modified_pr));
        auto partial_printer = seastar::value_of([&req, &partial_result] {
            return query::mapreduce_result::printer {
                .functions = get_functions(req),
                .res = partial_result
            };
        });
        tracing::trace(tr_state_, "Received mapreduce_result={} from {}", partial_printer, addr);
        flogger.debug("received mapreduce_result={} from {}", partial_printer, addr);

        auto aggrs = mapreduce_aggregates(req);
        co_return co_await aggrs.with_thread_if_needed([&result_, &aggrs, partial_result = std::move(partial_result)] () mutable {
            aggrs.merge(result_, std::move(partial_result));
        });
    });

    mapreduce_aggregates aggrs(req);
    const bool requires_thread = aggrs.requires_thread();

    auto merge_result = [&result, &req, &tr_state, aggrs = std::move(aggrs)] () mutable {
        auto printer = seastar::value_of([&req, &result] {
            return query::mapreduce_result::printer {
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
        co_return co_await seastar::async(std::move(merge_result));
    } else {
        co_return merge_result();
    }
}

void mapreduce_service::register_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("mapreduce_service", {
        sm::make_total_operations("requests_dispatched_to_other_nodes", _stats.requests_dispatched_to_other_nodes,
             sm::description("how many mapreduce requests were dispatched to other nodes"), {}),
        sm::make_total_operations("requests_dispatched_to_own_shards", _stats.requests_dispatched_to_own_shards,
             sm::description("how many mapreduce requests were dispatched to local shards"), {}),
        sm::make_total_operations("requests_executed", _stats.requests_executed,
             sm::description("how many mapreduce requests were executed"), {}),
    });
}

} // namespace service
