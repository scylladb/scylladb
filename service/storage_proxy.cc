/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <random>
#include <fmt/ranges.h>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include "partition_range_compat.hh"
#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "storage_proxy.hh"
#include "unimplemented.hh"
#include "mutation/mutation.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/async_utils.hh"
#include "query_result_merger.hh"
#include <seastar/core/do_with.hh>
#include "message/messaging_service.hh"
#include "gms/gossiper.hh"
#include <seastar/core/future-util.hh>
#include "db/read_repair_decision.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "db/hints/manager.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/algorithm/cxx11/none_of.hpp>
#include <boost/algorithm/cxx11/partition_copy.hpp>
#include <boost/range/algorithm/count_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/empty.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/combine.hpp>
#include <boost/range/algorithm/transform.hpp>
#include <boost/range/algorithm/partition.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/outcome/result.hpp>
#include "utils/assert.hh"
#include "utils/latency.hh"
#include "schema/schema.hh"
#include "query_ranges_to_vnodes.hh"
#include "schema/schema_registry.hh"
#include <seastar/util/lazy.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/execution_stage.hh>
#include "db/timeout_clock.hh"
#include "multishard_mutation_query.hh"
#include "replica/database.hh"
#include "db/consistency_level_validations.hh"
#include "cdc/log.hh"
#include "cdc/stats.hh"
#include "cdc/cdc_options.hh"
#include "utils/histogram_metrics_helper.hh"
#include "service/paxos/prepare_summary.hh"
#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "service/paxos/proposal.hh"
#include "locator/token_metadata.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/all.hh>
#include "locator/abstract_replication_strategy.hh"
#include "service/paxos/cas_request.hh"
#include "mutation/mutation_partition_view.hh"
#include "service/paxos/paxos_state.hh"
#include "gms/feature_service.hh"
#include "db/virtual_table.hh"
#include "mutation/canonical_mutation.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "idl/storage_proxy.dist.hh"
#include "utils/result_combinators.hh"
#include "utils/result_loop.hh"
#include "utils/result_try.hh"
#include "utils/error_injection.hh"
#include "utils/exceptions.hh"
#include "utils/tuple_utils.hh"
#include "utils/rpc_utils.hh"
#include "utils/to_string.hh"
#include "replica/exceptions.hh"
#include "db/operation_type.hh"
#include "locator/util.hh"

namespace bi = boost::intrusive;

template<typename T = void>
using result = service::storage_proxy::result<T>;

namespace service {

static logging::logger slogger("storage_proxy");
static logging::logger qlogger("query_result");
static logging::logger mlogger("mutation_data");

namespace storage_proxy_stats {
static const sstring COORDINATOR_STATS_CATEGORY("storage_proxy_coordinator");
static const sstring REPLICA_STATS_CATEGORY("storage_proxy_replica");
static const seastar::metrics::label op_type_label("op_type");
static const seastar::metrics::label scheduling_group_label("scheduling_group_name");
static const seastar::metrics::label rejected_by_coordinator_label("rejected_by_coordinator");

seastar::metrics::label_instance make_scheduling_group_label(const scheduling_group& sg) {
    return scheduling_group_label(sg.name());
}

seastar::metrics::label_instance current_scheduling_group_label() {
    return make_scheduling_group_label(current_scheduling_group());
}

}

template<typename ResultType>
static future<ResultType> encode_replica_exception_for_rpc(gms::feature_service& features, std::exception_ptr eptr) {
    if (features.typed_errors_in_read_rpc) {
        if (auto ex = replica::try_encode_replica_exception(eptr); ex) {
            if constexpr (std::is_same_v<ResultType, replica::exception_variant>) {
                return make_ready_future<ResultType>(std::move(ex));
            } else {
                ResultType encoded_ex = utils::make_default_rpc_tuple<ResultType>();
                std::get<replica::exception_variant>(encoded_ex) = std::move(ex);
                return make_ready_future<ResultType>(std::move(encoded_ex));
            }
        }
    }
    return make_exception_future<ResultType>(std::move(eptr));
}

template<utils::Tuple ResultTuple, utils::Tuple SourceTuple>
static future<ResultTuple> add_replica_exception_to_query_result(gms::feature_service& features, future<SourceTuple>&& f) {
    if (!f.failed()) {
        return make_ready_future<ResultTuple>(utils::tuple_insert<ResultTuple>(f.get(), replica::exception_variant{}));
    }
    return encode_replica_exception_for_rpc<ResultTuple>(features, f.get_exception());
}

gms::inet_address storage_proxy::my_address() const noexcept {
    return _shared_token_metadata.get()->get_topology().my_address();
}

bool storage_proxy::is_me(gms::inet_address addr) const noexcept {
    return local_db().get_token_metadata().get_topology().is_me(addr);
}

bool storage_proxy::only_me(const inet_address_vector_replica_set& replicas) const noexcept {
    return replicas.size() == 1 && is_me(replicas[0]);
}

enum class storage_proxy_remote_read_verb {
    read_data,
    read_mutation_data,
    read_digest
};

}

template <> struct fmt::formatter<service::storage_proxy_remote_read_verb> : fmt::formatter<string_view> {
    auto format(service::storage_proxy_remote_read_verb verb, fmt::format_context& ctx) const {
        std::string_view name;
        using enum service::storage_proxy_remote_read_verb;
        switch (verb) {
        case read_data:
            name = "read_data";
            break;
        case read_mutation_data:
            name = "read_mutation_data";
            break;
        case read_digest:
            name = "read_digest";
            break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

namespace service {

// This class handles all communication with other nodes in `storage_proxy`:
// sending and receiving RPCs, checking the state of other nodes (e.g. by accessing gossiper state), fetching schema.
//
// The object is uniquely owned by `storage_proxy`, its lifetime is bounded by the lifetime of `storage_proxy`.
//
// The presence of this object indicates that `storage_proxy` is able to perform remote queries.
// Without it only local queries are available.
class storage_proxy::remote {
    storage_proxy& _sp;
    netw::messaging_service& _ms;
    const gms::gossiper& _gossiper;
    migration_manager& _mm;
    sharded<db::system_keyspace>& _sys_ks;

    netw::connection_drop_slot_t _connection_dropped;
    netw::connection_drop_registration_t _condrop_registration;

    bool _stopped{false};

public:
    remote(storage_proxy& sp, netw::messaging_service& ms, gms::gossiper& g, migration_manager& mm, sharded<db::system_keyspace>& sys_ks)
        : _sp(sp), _ms(ms), _gossiper(g), _mm(mm), _sys_ks(sys_ks)
        , _connection_dropped(std::bind_front(&remote::connection_dropped, this))
        , _condrop_registration(_ms.when_connection_drops(_connection_dropped))
    {
        ser::storage_proxy_rpc_verbs::register_counter_mutation(&_ms, std::bind_front(&remote::handle_counter_mutation, this));
        ser::storage_proxy_rpc_verbs::register_mutation(&_ms, std::bind_front(&remote::receive_mutation_handler, this, _sp._write_smp_service_group));
        ser::storage_proxy_rpc_verbs::register_hint_mutation(&_ms, std::bind_front(&remote::receive_hint_mutation_handler, this));
        ser::storage_proxy_rpc_verbs::register_paxos_learn(&_ms, std::bind_front(&remote::handle_paxos_learn, this));
        ser::storage_proxy_rpc_verbs::register_mutation_done(&_ms, std::bind_front(&remote::handle_mutation_done, this));
        ser::storage_proxy_rpc_verbs::register_mutation_failed(&_ms, std::bind_front(&remote::handle_mutation_failed, this));
        ser::storage_proxy_rpc_verbs::register_read_data(&_ms, std::bind_front(&remote::handle_read_data, this));
        ser::storage_proxy_rpc_verbs::register_read_mutation_data(&_ms, std::bind_front(&remote::handle_read_mutation_data, this));
        ser::storage_proxy_rpc_verbs::register_read_digest(&_ms, std::bind_front(&remote::handle_read_digest, this));
        ser::storage_proxy_rpc_verbs::register_truncate(&_ms, std::bind_front(&remote::handle_truncate, this));
        // Register PAXOS verb handlers
        ser::storage_proxy_rpc_verbs::register_paxos_prepare(&_ms, std::bind_front(&remote::handle_paxos_prepare, this));
        ser::storage_proxy_rpc_verbs::register_paxos_accept(&_ms, std::bind_front(&remote::handle_paxos_accept, this));
        ser::storage_proxy_rpc_verbs::register_paxos_prune(&_ms, std::bind_front(&remote::handle_paxos_prune, this));
    }

    ~remote() {
        SCYLLA_ASSERT(_stopped);
    }

    // Must call before destroying the `remote` object.
    future<> stop() {
        co_await ser::storage_proxy_rpc_verbs::unregister(&_ms);
        _stopped = true;
    }

    const gms::gossiper& gossiper() const {
        return _gossiper;
    }

    bool is_alive(const gms::inet_address& ep) const {
        return _gossiper.is_alive(ep);
    }

    db::system_keyspace& system_keyspace() {
        return _sys_ks.local();
    }

    // Note: none of the `send_*` functions use `remote` after yielding - by the first yield,
    // control is delegated to another service (messaging_service). Thus unfinished `send`s
    // do not make it unsafe to destroy the `remote` object.
    //
    // Running handlers prevent the object from being destroyed,
    // assuming `stop()` is called before destruction.

    future<> send_mutation(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, const std::optional<tracing::trace_info>& trace_info,
            const frozen_mutation& m, const inet_address_vector_replica_set& forward, gms::inet_address reply_to, unsigned shard,
            storage_proxy::response_id_type response_id, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) {
        return ser::storage_proxy_rpc_verbs::send_mutation(
                &_ms, std::move(addr), timeout,
                m, forward, std::move(reply_to), shard,
                response_id, trace_info, rate_limit_info, fence);
    }

    future<> send_hint_mutation(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state,
            const frozen_mutation& m, const inet_address_vector_replica_set& forward, gms::inet_address reply_to, unsigned shard,
            storage_proxy::response_id_type response_id, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) {
        tracing::trace(tr_state, "Sending a hint to /{}", addr.addr);
        return ser::storage_proxy_rpc_verbs::send_hint_mutation(
                &_ms, std::move(addr), timeout,
                m, forward, std::move(reply_to), shard,
                response_id, tracing::make_trace_info(tr_state), fence);
    }

    future<> send_counter_mutation(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state,
            std::vector<frozen_mutation> fms, db::consistency_level cl, fencing_token fence) {
        tracing::trace(tr_state, "Enqueuing counter update to {}", addr);
        auto&& opt_exception = co_await ser::storage_proxy_rpc_verbs::send_counter_mutation(
            &_ms, std::move(addr), timeout,
            std::move(fms), cl, tracing::make_trace_info(tr_state), fence);
        if (opt_exception.has_value() && *opt_exception) {
            co_await coroutine::return_exception_ptr((*opt_exception).into_exception_ptr());
        }
    }

    future<> send_mutation_done(
            netw::msg_addr addr, tracing::trace_state_ptr tr_state,
            unsigned shard, uint64_t response_id, db::view::update_backlog backlog) {
        tracing::trace(tr_state, "Sending mutation_done to /{}", addr.addr);
        return ser::storage_proxy_rpc_verbs::send_mutation_done(
                &_ms, std::move(addr),
                shard, response_id, std::move(backlog));
    }

    future<> send_mutation_failed(
            netw::msg_addr addr, tracing::trace_state_ptr tr_state,
            unsigned shard, uint64_t response_id, size_t num_failed, db::view::update_backlog backlog, replica::exception_variant exception) {
        tracing::trace(tr_state, "Sending mutation_failure with {} failures to /{}", num_failed, addr.addr);
        return ser::storage_proxy_rpc_verbs::send_mutation_failed(
                &_ms, std::move(addr),
                shard, response_id, num_failed, std::move(backlog), std::move(exception));
    }

    future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>
    send_read_mutation_data(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state,
            const query::read_command& cmd, const dht::partition_range& pr,
            fencing_token fence) {
        tracing::trace(tr_state, "read_mutation_data: sending a message to /{}", addr.addr);
        auto&& [result, hit_rate, opt_exception] = co_await ser::storage_proxy_rpc_verbs::send_read_mutation_data(&_ms, addr, timeout, cmd, pr, fence);
        if (opt_exception.has_value() && *opt_exception) {
            co_await coroutine::return_exception_ptr((*opt_exception).into_exception_ptr());
        }

        tracing::trace(tr_state, "read_mutation_data: got response from /{}", addr.addr);
        co_return rpc::tuple{make_foreign(::make_lw_shared<reconcilable_result>(std::move(result))), hit_rate.value_or(cache_temperature::invalid())};
    }

    future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>>
    send_read_data(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state,
            const query::read_command& cmd, const dht::partition_range& pr,
            query::digest_algorithm digest_algo, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) {
        tracing::trace(tr_state, "read_data: sending a message to /{}", addr.addr);
        auto&& [result, hit_rate, opt_exception] =
            co_await ser::storage_proxy_rpc_verbs::send_read_data(&_ms, addr, timeout, cmd, pr, digest_algo, rate_limit_info, fence);
        if (opt_exception.has_value() && *opt_exception) {
            co_await coroutine::return_exception_ptr((*opt_exception).into_exception_ptr());
        }

        tracing::trace(tr_state, "read_data: got response from /{}", addr.addr);
        co_return rpc::tuple{make_foreign(::make_lw_shared<query::result>(std::move(result))), hit_rate.value_or(cache_temperature::invalid())};
    }

    future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature, std::optional<full_position>>>
    send_read_digest(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state,
            const query::read_command& cmd, const dht::partition_range& pr,
            query::digest_algorithm digest_algo, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) {
        tracing::trace(tr_state, "read_digest: sending a message to /{}", addr.addr);
        auto&& [d, t, hit_rate, opt_exception, opt_last_pos] =
            co_await ser::storage_proxy_rpc_verbs::send_read_digest(&_ms, addr, timeout, cmd, pr, digest_algo, rate_limit_info, fence);
        if (opt_exception.has_value() && *opt_exception) {
            co_await coroutine::return_exception_ptr((*opt_exception).into_exception_ptr());
        }

        tracing::trace(tr_state, "read_digest: got response from /{}", addr.addr);
        co_return rpc::tuple{d, t ? t.value() : api::missing_timestamp, hit_rate.value_or(cache_temperature::invalid()), opt_last_pos ? std::move(*opt_last_pos) : std::nullopt};
    }

    future<> send_truncate(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout,
            sstring ks_name, sstring cf_name) {
        return ser::storage_proxy_rpc_verbs::send_truncate(&_ms, std::move(addr), timeout, std::move(ks_name), std::move(cf_name));
    }

    future<service::paxos::prepare_response> send_paxos_prepare(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state,
            const query::read_command& cmd, const partition_key& key, utils::UUID ballot, bool only_digest, query::digest_algorithm da) {
        tracing::trace(tr_state, "prepare_ballot: sending prepare {} to {}", ballot, addr.addr);
        return ser::storage_proxy_rpc_verbs::send_paxos_prepare(
                &_ms, addr, timeout, cmd, key, ballot, only_digest, da, tracing::make_trace_info(tr_state));
    }

    future<bool> send_paxos_accept(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state,
            const service::paxos::proposal& proposal) {
        tracing::trace(tr_state, "accept_proposal: send accept {} to {}", proposal, addr.addr);
        return ser::storage_proxy_rpc_verbs::send_paxos_accept(&_ms, std::move(addr), timeout, proposal, tracing::make_trace_info(tr_state));
    }

    future<> send_paxos_learn(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, const std::optional<tracing::trace_info>& trace_info,
            const service::paxos::proposal& decision, const inet_address_vector_replica_set& forward,
            gms::inet_address reply_to, unsigned shard, uint64_t response_id) {
        return ser::storage_proxy_rpc_verbs::send_paxos_learn(
                &_ms, addr, timeout, decision, forward, reply_to, shard, response_id, trace_info);
    }

    future<> send_paxos_prune(
            netw::msg_addr addr, storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state,
            table_schema_version schema_id, const partition_key& key, utils::UUID ballot) {
        return ser::storage_proxy_rpc_verbs::send_paxos_prune(&_ms, addr, timeout, schema_id, key, ballot, tracing::make_trace_info(tr_state));
    }

    future<> send_truncate_blocking(sstring keyspace, sstring cfname, std::optional<std::chrono::milliseconds> timeout_in_ms) {
        if (!_gossiper.get_unreachable_token_owners().empty()) {
            slogger.info("Cannot perform truncate, some hosts are down");
            // Since the truncate operation is so aggressive and is typically only
            // invoked by an admin, for simplicity we require that all nodes are up
            // to perform the operation.
            auto live_members = _gossiper.get_live_members().size();

            co_await coroutine::return_exception(exceptions::unavailable_exception(db::consistency_level::ALL,
                    live_members + _gossiper.get_unreachable_members().size(),
                    live_members));
        }

        auto all_endpoints = _gossiper.get_live_token_owners();
        auto timeout = clock_type::now() + timeout_in_ms.value_or(std::chrono::milliseconds(_sp._db.local().get_config().truncate_request_timeout_in_ms()));

        slogger.trace("Enqueuing truncate messages to hosts {}", all_endpoints);

        try {
            co_await coroutine::parallel_for_each(all_endpoints, [&] (auto ep) {
                return send_truncate(netw::messaging_service::msg_addr{ep, 0}, timeout, keyspace, cfname);
            });
           } catch (rpc::timeout_error& e) {
               slogger.trace("Truncation of {} timed out: {}", cfname, e.what());
               throw;
           } catch (...) {
               throw;
           }
    }

private:
    future<schema_ptr> get_schema_for_read(table_schema_version v, netw::msg_addr from, clock_type::time_point timeout) {
        abort_on_expiry aoe(timeout);
        co_return co_await _mm.get_schema_for_read(std::move(v), std::move(from), _ms, aoe.abort_source());
    }

    future<schema_ptr> get_schema_for_write(table_schema_version v, netw::msg_addr from, clock_type::time_point timeout) {
        abort_on_expiry aoe(timeout);
        co_return co_await _mm.get_schema_for_write(std::move(v), std::move(from), _ms, aoe.abort_source());
    }

    future<replica::exception_variant> handle_counter_mutation(
            const rpc::client_info& cinfo, rpc::opt_time_point t,
            std::vector<frozen_mutation> fms, db::consistency_level cl, std::optional<tracing::trace_info> trace_info,
            rpc::optional<service::fencing_token> fence_opt) {
        auto src_addr = netw::messaging_service::get_source(cinfo);

        tracing::trace_state_ptr trace_state_ptr;
        if (trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "Message received from /{}", src_addr.addr);
        }

        // FIXME: this is also held while mutations are send to replicas which is not needed. They are send inside mutate_counters_on_leader
        // called below. The way to fix it is to move the entry to the phased barrier into the function, but the barrier needs to be entered
        // before fencing so the fencing should be moves as well, but and we do fencing here because was want to avoid fetching schema for
        // fenced writes.
        auto op = _sp.start_write();

        const auto fence = fence_opt.value_or(fencing_token{});
        if (auto stale = _sp.apply_fence(fence, src_addr.addr)) {
            co_return co_await encode_replica_exception_for_rpc<replica::exception_variant>(_sp.features(),
                make_exception_ptr(std::move(*stale)));
        }

        std::vector<frozen_mutation_and_schema> mutations;
        auto timeout = *t;
        co_await coroutine::parallel_for_each(std::move(fms), [&] (frozen_mutation& fm) {
            // Note: not a coroutine, since get_schema_for_write() rarely blocks.
            // FIXME: optimise for cases when all fms are in the same schema
            auto schema_version = fm.schema_version();
            return get_schema_for_write(schema_version, std::move(src_addr), timeout).then([&] (schema_ptr s) mutable {
                mutations.emplace_back(frozen_mutation_and_schema { std::move(fm), std::move(s) });
            });
        });
        auto& sp = _sp;
        co_await sp.mutate_counters_on_leader(std::move(mutations), cl, timeout, std::move(trace_state_ptr), /* FIXME: rpc should also pass a permit down to callbacks */ empty_service_permit());
        if (auto stale = _sp.apply_fence(fence, src_addr.addr)) {
            co_return co_await encode_replica_exception_for_rpc<replica::exception_variant>(_sp.features(),
                make_exception_ptr(std::move(*stale)));
        }
        co_return replica::exception_variant{};
    }

    future<rpc::no_wait_type> handle_write(
            netw::messaging_service::msg_addr src_addr, rpc::opt_time_point t,
            auto schema_version, auto in, const inet_address_vector_replica_set& forward, gms::inet_address reply_to,
            unsigned shard, storage_proxy::response_id_type response_id, const std::optional<tracing::trace_info>& trace_info,
            fencing_token fence, auto&& apply_fn1, auto&& forward_fn1) {
        auto apply_fn = std::move(apply_fn1);
        auto forward_fn = std::move(forward_fn1);

        tracing::trace_state_ptr trace_state_ptr;

        if (trace_info) {
            const tracing::trace_info& tr_info = *trace_info;
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(tr_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "Message received from /{}", src_addr.addr);
        }

        auto trace_done = defer([&] {
            tracing::trace(trace_state_ptr, "Mutation handling is done");
        });

        storage_proxy::clock_type::time_point timeout;
        if (!t) {
            auto timeout_in_ms = _sp._db.local().get_config().write_request_timeout_in_ms();
            timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
        } else {
            timeout = *t;
        }

        struct errors_info {
            size_t count = 0;
            replica::exception_variant local;
        };

        const auto& m = in;
        shared_ptr<storage_proxy> p = _sp.shared_from_this();
        errors_info errors;
        ++p->get_stats().received_mutations;
        p->get_stats().forwarded_mutations += forward.size();

        if (auto stale = _sp.apply_fence(fence, src_addr.addr)) {
            errors.count += (forward.size() + 1);
            errors.local = std::move(*stale);
        } else {
            co_await coroutine::all(
                [&] () -> future<> {
                    try {
                        auto op = _sp.start_write();
                        // FIXME: get_schema_for_write() doesn't timeout
                        schema_ptr s = co_await get_schema_for_write(schema_version, netw::messaging_service::msg_addr{reply_to, shard}, timeout);
                        // Note: blocks due to execution_stage in replica::database::apply()
                        co_await apply_fn(p, trace_state_ptr, std::move(s), m, timeout, fence);
                        // We wait for send_mutation_done to complete, otherwise, if reply_to is busy, we will accumulate
                        // lots of unsent responses, which can OOM our shard.
                        //
                        // Usually we will return immediately, since this work only involves appending data to the connection
                        // send buffer.
                        auto f = co_await coroutine::as_future(send_mutation_done(netw::messaging_service::msg_addr{reply_to, shard}, trace_state_ptr,
                                shard, response_id, p->get_view_update_backlog()));
                        f.ignore_ready_future();
                    } catch (...) {
                        std::exception_ptr eptr = std::current_exception();
                        errors.count++;
                        errors.local = replica::try_encode_replica_exception(eptr);
                        seastar::log_level l = seastar::log_level::warn;
                        if (is_timeout_exception(eptr)
                                || std::holds_alternative<replica::rate_limit_exception>(errors.local.reason)
                                || std::holds_alternative<abort_requested_exception>(errors.local.reason)) {
                            // ignore timeouts, abort requests and rate limit exceptions so that logs are not flooded.
                            // database's total_writes_timedout or total_writes_rate_limited counter was incremented.
                            l = seastar::log_level::debug;
                        }
                        slogger.log(l, "Failed to apply mutation from {}#{}: {}", reply_to, shard, eptr);
                    }
                },
                [&] {
                    // Note: not a coroutine, since often nothing needs to be forwarded and this returns a ready future
                    return parallel_for_each(forward.begin(), forward.end(), [&] (gms::inet_address forward) {
                        // Note: not a coroutine, since forward_fn() typically returns a ready future
                        tracing::trace(trace_state_ptr, "Forwarding a mutation to /{}", forward);
                        return forward_fn(p, netw::messaging_service::msg_addr{forward, 0}, timeout, m, reply_to, shard, response_id,
                                            tracing::make_trace_info(trace_state_ptr), fence)
                                .then_wrapped([&] (future<> f) {
                            if (f.failed()) {
                                ++p->get_stats().forwarding_errors;
                                errors.count++;
                            };
                            f.ignore_ready_future();
                        });
                    });
                }
            );
        }
        // ignore results, since we'll be returning them via MUTATION_DONE/MUTATION_FAILURE verbs
        if (errors.count) {
            auto f = co_await coroutine::as_future(send_mutation_failed(
                    netw::messaging_service::msg_addr{reply_to, shard},
                    trace_state_ptr,
                    shard,
                    response_id,
                    errors.count,
                    p->get_view_update_backlog(),
                    std::move(errors.local)));
            f.ignore_ready_future();
        }
        co_return netw::messaging_service::no_wait();
    }

    future<rpc::no_wait_type> receive_mutation_handler(
            smp_service_group smp_grp, const rpc::client_info& cinfo, rpc::opt_time_point t,
            frozen_mutation in, inet_address_vector_replica_set forward, gms::inet_address reply_to,
            unsigned shard, storage_proxy::response_id_type response_id,
            rpc::optional<std::optional<tracing::trace_info>> trace_info,
            rpc::optional<db::per_partition_rate_limit::info> rate_limit_info_opt,
            rpc::optional<fencing_token> fence) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        auto rate_limit_info = rate_limit_info_opt.value_or(std::monostate());

        auto schema_version = in.schema_version();
        return handle_write(src_addr, t, schema_version, std::move(in), forward, reply_to, shard, response_id,
                trace_info ? *trace_info : std::nullopt,
                fence.value_or(fencing_token{}),
                /* apply_fn */ [smp_grp, rate_limit_info, src_ip = src_addr.addr] (shared_ptr<storage_proxy>& p, tracing::trace_state_ptr tr_state, schema_ptr s, const frozen_mutation& m,
                        clock_type::time_point timeout, fencing_token fence) {
                    return p->apply_fence(p->mutate_locally(std::move(s), m, std::move(tr_state), db::commitlog::force_sync::no, timeout, smp_grp, rate_limit_info), fence, src_ip);
                },
                /* forward_fn */ [this, rate_limit_info] (shared_ptr<storage_proxy>& p, netw::messaging_service::msg_addr addr, clock_type::time_point timeout, const frozen_mutation& m,
                        gms::inet_address reply_to, unsigned shard, response_id_type response_id,
                        const std::optional<tracing::trace_info>& trace_info, fencing_token fence) {
                    return send_mutation(addr, timeout, trace_info, m, {}, reply_to, shard, response_id, rate_limit_info, fence);
                });
    }

    future<rpc::no_wait_type> receive_hint_mutation_handler(
            const rpc::client_info& cinfo, rpc::opt_time_point t,
            frozen_mutation in, inet_address_vector_replica_set forward, gms::inet_address reply_to,
            unsigned shard, storage_proxy::response_id_type response_id,
            rpc::optional<std::optional<tracing::trace_info>> trace_info,
            rpc::optional<fencing_token> fence) {
        ++_sp.get_stats().received_hints_total;
        _sp.get_stats().received_hints_bytes_total += in.representation().size();

        return receive_mutation_handler(_sp._hints_write_smp_service_group, cinfo, t, std::move(in),
            std::move(forward), std::move(reply_to), shard, response_id, std::move(trace_info),
            std::monostate(), fence);
    }

    future<rpc::no_wait_type> handle_paxos_learn(
            const rpc::client_info& cinfo, rpc::opt_time_point t,
            paxos::proposal decision, inet_address_vector_replica_set forward, gms::inet_address reply_to, unsigned shard,
            storage_proxy::response_id_type response_id, std::optional<tracing::trace_info> trace_info) {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);

        auto schema_version = decision.update.schema_version();
        return handle_write(src_addr, t, schema_version, std::move(decision), forward, reply_to, shard,
                response_id, trace_info,
                fencing_token{},
               /* apply_fn */ [this] (shared_ptr<storage_proxy>& p, tracing::trace_state_ptr tr_state, schema_ptr s,
                       const paxos::proposal& decision, clock_type::time_point timeout, fencing_token) {
                     return paxos::paxos_state::learn(*p, _sys_ks.local(), std::move(s), decision, timeout, tr_state);
              },
              /* forward_fn */ [this] (shared_ptr<storage_proxy>&, netw::messaging_service::msg_addr addr, clock_type::time_point timeout, const paxos::proposal& m,
                      gms::inet_address reply_to, unsigned shard, response_id_type response_id,
                      const std::optional<tracing::trace_info>& trace_info, fencing_token) {
                    return send_paxos_learn(addr, timeout, trace_info, m, {}, reply_to, shard, response_id);
              });
    }

    future<rpc::no_wait_type> handle_mutation_done(
            const rpc::client_info& cinfo,
            unsigned shard, storage_proxy::response_id_type response_id, rpc::optional<db::view::update_backlog> backlog) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        _sp.get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return _sp.container().invoke_on(shard, _sp._write_ack_smp_service_group,
                [from, response_id, backlog = std::move(backlog)] (storage_proxy& sp) mutable {
            sp.got_response(response_id, from, std::move(backlog));
            return netw::messaging_service::no_wait();
        });
    }

    future<rpc::no_wait_type> handle_mutation_failed(
            const rpc::client_info& cinfo,
            unsigned shard, storage_proxy::response_id_type response_id, size_t num_failed,
            rpc::optional<db::view::update_backlog> backlog, rpc::optional<replica::exception_variant> exception) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        _sp.get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return _sp.container().invoke_on(shard, _sp._write_ack_smp_service_group,
                [from, response_id, num_failed, backlog = std::move(backlog), exception = std::move(exception)] (storage_proxy& sp) mutable {
            error err = error::FAILURE;
            std::optional<sstring> msg;
            if (exception) {
                err = std::visit([&] <typename Ex> (Ex& e) {
                    if constexpr (std::is_same_v<Ex, replica::rate_limit_exception>) {
                        return error::RATE_LIMIT;
                    } else if constexpr (std::is_same_v<Ex, replica::unknown_exception> || std::is_same_v<Ex, replica::no_exception>) {
                        return error::FAILURE;
                    } else if constexpr(std::is_same_v<Ex, replica::stale_topology_exception>) {
                        msg = e.what();
                        return error::FAILURE;
                    } else if constexpr (std::is_same_v<Ex, replica::abort_requested_exception>) {
                        msg = e.what();
                        return error::FAILURE;
                    }
                }, exception->reason);
            }
            sp.got_failure_response(response_id, from, num_failed, std::move(backlog), err, std::move(msg));
            return netw::messaging_service::no_wait();
        });
    }

    using read_verb = storage_proxy_remote_read_verb;

    template<typename Result, read_verb verb>
    future<Result> handle_read(const rpc::client_info& cinfo, rpc::opt_time_point t,
        query::read_command cmd1, ::compat::wrapping_partition_range pr,
        rpc::optional<query::digest_algorithm> oda,
        rpc::optional<db::per_partition_rate_limit::info> rate_limit_info_opt,
        rpc::optional<service::fencing_token> fence_opt)
    {
        tracing::trace_state_ptr trace_state_ptr;
        auto src_addr = netw::messaging_service::get_source(cinfo);
        if (cmd1.trace_info) {
            trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*cmd1.trace_info);
            tracing::begin(trace_state_ptr);
            tracing::trace(trace_state_ptr, "{}: message received from /{}", verb, src_addr.addr);
        }
        auto rate_limit_info = rate_limit_info_opt.value_or(std::monostate());
        if (!cmd1.max_result_size) {
            if constexpr (verb == read_verb::read_data) {
                auto& cfg = _sp.local_db().get_config();
                cmd1.max_result_size.emplace(cfg.max_memory_for_unlimited_query_soft_limit(), cfg.max_memory_for_unlimited_query_hard_limit());
            } else {
                cmd1.max_result_size.emplace(cinfo.retrieve_auxiliary<uint64_t>("max_result_size"));
            }
        }
        shared_ptr<storage_proxy> p = _sp.shared_from_this();
        auto cmd = make_lw_shared<query::read_command>(std::move(cmd1));
        auto src_ip = src_addr.addr;
        auto timeout = t ? *t : db::no_timeout;

        // Pull a schema from the coordinator. There are two cases:
        // 1. A mixed nodes cluster, cmd->schema_version is a table schema, a table schema is pulled
        // 2. All new nodes cluster, cmd->schema_version is a reversed table schema, a reversed table schema is pulled.
        //    Note that in this case, table schema is also pulled if outdated or missing.
        auto f_s = co_await coroutine::as_future(get_schema_for_read(cmd->schema_version, std::move(src_addr), timeout));
        if (f_s.failed()) {
            co_return co_await encode_replica_exception_for_rpc<Result>(p->features(), f_s.get_exception());
        }
        schema_ptr s = f_s.get();

        // Detect whether a transformation from legacy reverse format into native reverse
        // format is necessary before executing the read_command. That happens when the
        // native_reverse_queries feature is turned off. A custer has mixed nodes.
        bool format_reverse_required = false;

        // Check if we have a reversed query
        if (cmd->slice.is_reversed()) {
            // Verify whether read_command is provided in legacy or native reversed format by comparing
            // the schema version provided by read_command with the table schema replica holds. Note that
            // with the get_schema_for_read call above, table_schema is up-to-date.
            auto table_schema = p->local_db().find_schema(cmd->cf_id);
            // Therefore there are only two options:
            // 1. versions are the same -> legacy format
            // 2. versions are different -> schema version is equal to reversed table schema version -> native format
            format_reverse_required = cmd->schema_version == table_schema->version();
        }
        if (format_reverse_required) {
            cmd = reversed(std::move(cmd));
            s = s->get_reversed();
        }

        co_await utils::get_local_injector().inject("storage_proxy::handle_read", [s] (auto& handler) -> future<> {
            const auto cf_name = handler.get("cf_name");
            SCYLLA_ASSERT(cf_name);
            if (s->cf_name() != cf_name) {
                co_return;
            }
            slogger.info("storage_proxy::handle_read injection hit");
            co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{1});
            slogger.info("storage_proxy::handle_read injection done");
        });

        auto pr2 = ::compat::unwrap(std::move(pr), *s);
        auto do_query = [&]() {
            if constexpr (verb == read_verb::read_data) {
                if (pr2.second) {
                    // this function assumes singular queries but doesn't validate
                    throw std::runtime_error("READ_DATA called with wrapping range");
                }
                auto erm = s->table().get_effective_replication_map();
                p->get_stats().replica_data_reads++;
                if (!oda) {
                    throw std::runtime_error("READ_DATA called without digest algorithm");
                }
                auto da = oda.value();
                query::result_options opts;
                opts.digest_algo = da;
                opts.request = da == query::digest_algorithm::none ? query::result_request::only_result : query::result_request::result_and_digest;
                return p->query_result_local(erm, std::move(s), cmd, std::move(pr2.first), opts, trace_state_ptr, timeout, rate_limit_info);
            } else if constexpr (verb == read_verb::read_mutation_data) {
                p->get_stats().replica_mutation_data_reads++;
                auto f = p->query_mutations_locally(std::move(s), std::move(cmd), pr2, timeout, trace_state_ptr);
                if (format_reverse_required) {
                    f = f.then([](auto result_ht) {
                        auto&& [result, hit_rate] = result_ht;
                        return reversed(std::move(result)).then([hit_rate=std::move(hit_rate)](auto result) mutable {
                            return rpc::tuple{std::move(result), std::move(hit_rate)};
                        });
                    });
                }
                return f;
            } else if constexpr (verb == read_verb::read_digest) {
                if (pr2.second) {
                    // this function assumes singular queries but doesn't validate
                    throw std::runtime_error("READ_DIGEST called with wrapping range");
                }
                auto erm = s->table().get_effective_replication_map();
                p->get_stats().replica_digest_reads++;
                if (!oda) {
                    throw std::runtime_error("READ_DIGEST called without digest algorithm");
                }
                auto da = oda.value();
                return p->query_result_local_digest(erm, std::move(s), cmd, std::move(pr2.first), trace_state_ptr, timeout, da, rate_limit_info);
            } else {
                static_assert(verb == static_cast<read_verb>(-1), "Unsupported verb");
            }
        };

        const auto fence = fence_opt.value_or(fencing_token{});

        if (auto stale = _sp.apply_fence(fence, src_ip)) {
            co_return co_await encode_replica_exception_for_rpc<Result>(p->features(), std::make_exception_ptr(std::move(*stale)));
        }

        auto f = co_await coroutine::as_future(do_query());
        tracing::trace(trace_state_ptr, "{} handling is done, sending a response to /{}", verb, src_ip);

        if (auto stale = _sp.apply_fence(fence, src_ip)) {
            co_return co_await encode_replica_exception_for_rpc<Result>(p->features(), std::make_exception_ptr(std::move(*stale)));
        }

        co_return co_await add_replica_exception_to_query_result<Result>(p->features(), std::move(f));
    }

    using read_data_result_t = rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature, replica::exception_variant>;
    future<read_data_result_t> handle_read_data(
            const rpc::client_info& cinfo, rpc::opt_time_point t,
            query::read_command cmd1, ::compat::wrapping_partition_range pr,
            rpc::optional<query::digest_algorithm> oda,
            rpc::optional<db::per_partition_rate_limit::info> rate_limit_info_opt,
            rpc::optional<service::fencing_token> fence) {
        return handle_read<read_data_result_t, read_verb::read_data>(cinfo, t, std::move(cmd1),
            std::move(pr), oda, rate_limit_info_opt, fence);
    }

    using read_mutation_data_result_t = rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature, replica::exception_variant>;
    future<read_mutation_data_result_t> handle_read_mutation_data(
            const rpc::client_info& cinfo, rpc::opt_time_point t,
            query::read_command cmd1, ::compat::wrapping_partition_range pr,
            rpc::optional<service::fencing_token> fence) {
        return handle_read<read_mutation_data_result_t, read_verb::read_mutation_data>(cinfo, t, std::move(cmd1),
            std::move(pr), std::nullopt, std::nullopt, fence);
    }

    using read_digest_result_t = rpc::tuple<query::result_digest, long, cache_temperature, replica::exception_variant, std::optional<full_position>>;
    future<read_digest_result_t> handle_read_digest(
            const rpc::client_info& cinfo, rpc::opt_time_point t,
            query::read_command cmd1, ::compat::wrapping_partition_range pr,
            rpc::optional<query::digest_algorithm> oda,
            rpc::optional<db::per_partition_rate_limit::info> rate_limit_info_opt,
            rpc::optional<service::fencing_token> fence) {
        return handle_read<read_digest_result_t, read_verb::read_digest>(cinfo, t, std::move(cmd1),
            std::move(pr), oda, rate_limit_info_opt, fence);
    }

    future<> handle_truncate(rpc::opt_time_point timeout, sstring ksname, sstring cfname) {
        return replica::database::truncate_table_on_all_shards(_sp._db, _sys_ks, ksname, cfname);
    }

    future<foreign_ptr<std::unique_ptr<service::paxos::prepare_response>>>
    handle_paxos_prepare(
            const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            query::read_command cmd, partition_key key, utils::UUID ballot,
            bool only_digest, query::digest_algorithm da, std::optional<tracing::trace_info> trace_info) {
        auto src_addr = netw::messaging_service::get_source(cinfo);
        auto src_ip = src_addr.addr;
        tracing::trace_state_ptr tr_state;
        if (trace_info) {
            tr_state = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(tr_state);
            tracing::trace(tr_state, "paxos_prepare: message received from /{} ballot {}", src_ip, ballot);
        }
        if (!cmd.max_result_size) {
            cmd.max_result_size.emplace(cinfo.retrieve_auxiliary<uint64_t>("max_result_size"));
        }

        return get_schema_for_read(cmd.schema_version, src_addr, *timeout).then([&sp = _sp, &sys_ks = _sys_ks, cmd = std::move(cmd), key = std::move(key), ballot,
                         only_digest, da, timeout, tr_state = std::move(tr_state), src_ip] (schema_ptr schema) mutable {
            dht::token token = dht::get_token(*schema, key);
            unsigned shard = schema->table().shard_for_reads(token);
            bool local = shard == this_shard_id();
            sp.get_stats().replica_cross_shard_ops += !local;
            return sp.container().invoke_on(shard, sp._write_smp_service_group, [gs = global_schema_ptr(schema), gt = tracing::global_trace_state_ptr(std::move(tr_state)),
                                     cmd = make_lw_shared<query::read_command>(std::move(cmd)), key = std::move(key),
                                     ballot, only_digest, da, timeout, src_ip, &sys_ks] (storage_proxy& sp) {
                tracing::trace_state_ptr tr_state = gt;
                return paxos::paxos_state::prepare(sp, sys_ks.local(), tr_state, gs, *cmd, key, ballot, only_digest, da, *timeout).then([src_ip, tr_state] (paxos::prepare_response r) {
                    tracing::trace(tr_state, "paxos_prepare: handling is done, sending a response to /{}", src_ip);
                    return make_foreign(std::make_unique<paxos::prepare_response>(std::move(r)));
                });
            });
        });
    }

    future<bool> handle_paxos_accept(
            const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            paxos::proposal proposal, std::optional<tracing::trace_info> trace_info) {
        auto src_addr = netw::messaging_service::get_source(cinfo);
        auto src_ip = src_addr.addr;
        tracing::trace_state_ptr tr_state;
        if (trace_info) {
            tr_state = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(tr_state);
            tracing::trace(tr_state, "paxos_accept: message received from /{} ballot {}", src_ip, proposal);
        }
        auto handling_done = defer([tr_state, src_ip] {
            if (tr_state) {
                tracing::trace(tr_state, "paxos_accept: handling is done, sending a response to /{}", src_ip);
            }
        });
        auto schema = co_await get_schema_for_read(proposal.update.schema_version(), src_addr, *timeout);
        dht::token token = proposal.update.decorated_key(*schema).token();
        unsigned shard = schema->table().shard_for_reads(token);
        bool local = shard == this_shard_id();
        _sp.get_stats().replica_cross_shard_ops += !local;
        co_return co_await _sp.container().invoke_on(shard, _sp._write_smp_service_group, coroutine::lambda([gs = global_schema_ptr(schema), gt = tracing::global_trace_state_ptr(tr_state),
                                   proposal = std::move(proposal), timeout, token, this] (storage_proxy& sp) {
            return paxos::paxos_state::accept(sp, _sys_ks.local(), gt, gs, token, proposal, *timeout);
        }));
    }

    future<rpc::no_wait_type> handle_paxos_prune(
            const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            table_schema_version schema_id, partition_key key, utils::UUID ballot, std::optional<tracing::trace_info> trace_info) {
        static thread_local uint16_t pruning = 0;
        static constexpr uint16_t pruning_limit = 1000; // since PRUNE verb is one way replica side has its own queue limit
        auto src_addr = netw::messaging_service::get_source(cinfo);
        auto src_ip = src_addr.addr;
        tracing::trace_state_ptr tr_state;
        if (trace_info) {
            tr_state = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
            tracing::begin(tr_state);
            tracing::trace(tr_state, "paxos_prune: message received from /{} ballot {}", src_ip, ballot);
        }

        if (pruning >= pruning_limit) {
            _sp.get_stats().cas_replica_dropped_prune++;
            tracing::trace(tr_state, "paxos_prune: do not prune due to overload", src_ip);
            return make_ready_future<seastar::rpc::no_wait_type>(netw::messaging_service::no_wait());
        }

        pruning++;
        auto d = defer([] { pruning--; });
        return get_schema_for_read(schema_id, src_addr, *timeout).then([&sp = _sp, &sys_ks = _sys_ks, key = std::move(key), ballot,
                         timeout, tr_state = std::move(tr_state), src_ip, d = std::move(d)] (schema_ptr schema) mutable {
            dht::token token = dht::get_token(*schema, key);
            unsigned shard = schema->table().shard_for_reads(token);
            bool local = shard == this_shard_id();
            sp.get_stats().replica_cross_shard_ops += !local;
            return smp::submit_to(shard, sp._write_smp_service_group, [gs = global_schema_ptr(schema), gt = tracing::global_trace_state_ptr(std::move(tr_state)),
                                     key = std::move(key), ballot, timeout, src_ip, d = std::move(d), &sys_ks] () {
                tracing::trace_state_ptr tr_state = gt;
                return paxos::paxos_state::prune(sys_ks.local(), gs, key, ballot,  *timeout, tr_state).then([src_ip, tr_state] () {
                    tracing::trace(tr_state, "paxos_prune: handling is done, sending a response to /{}", src_ip);
                    return netw::messaging_service::no_wait();
                });
            });
        });
    }

    void connection_dropped(gms::inet_address addr) {
        slogger.debug("Drop hit rate info for {} because of disconnect", addr);
        for (auto&& cf : _sp._db.local().get_non_system_column_families()) {
            cf->drop_hit_rate(addr);
        }
    }
};

using namespace exceptions;

static inline
query::digest_algorithm digest_algorithm(service::storage_proxy& proxy) {
    return query::digest_algorithm::xxHash;
}

static inline
const dht::token& end_token(const dht::partition_range& r) {
    static const dht::token max_token = dht::maximum_token();
    return r.end() ? r.end()->value().token() : max_token;
}

unsigned storage_proxy::cas_shard(const schema& s, dht::token token) {
    return s.table().shard_for_reads(token);
}

static uint32_t random_variable_for_rate_limit() {
    static thread_local std::default_random_engine re{std::random_device{}()};
    static thread_local std::uniform_int_distribution<uint32_t> dist(0, 0xFFFFFFFF);
    return dist(re);
}

static result<db::per_partition_rate_limit::info> choose_rate_limit_info(
        locator::effective_replication_map_ptr erm,
        replica::database& db,
        bool coordinator_in_replica_set,
        db::operation_type op_type,
        const schema_ptr& s,
        const dht::token& token,
        tracing::trace_state_ptr tr_state) {

    db::per_partition_rate_limit::account_and_enforce enforce_info{
        .random_variable = random_variable_for_rate_limit(),
    };
    // It's fine to use shard_for_reads() because in case of no migration this is the
    // shard used by all requests. During migration, it is the shard used for request routing
    // by drivers during most of the migration. It changes after streaming, in which case we'll
    // fall back to throttling on replica side, which is suboptimal but acceptable.
    if (coordinator_in_replica_set && erm->shard_for_reads(*s, token) == this_shard_id()) {
        auto& cf = db.find_column_family(s);
        auto decision = db.account_coordinator_operation_to_rate_limit(cf, token, enforce_info, op_type);
        if (decision) {
            if (*decision == db::rate_limiter::can_proceed::yes) {
                // The coordinator has decided to accept the operation.
                // Tell other replicas only to account, but not reject
                slogger.trace("Per-partition rate limiting: coordinator accepted");
                tracing::trace(tr_state, "Per-partition rate limiting: coordinator accepted");
                return db::per_partition_rate_limit::account_only{};
            } else {
                // The coordinator has decided to reject, abort the operation
                slogger.trace("Per-partition rate limiting: coordinator rejected");
                tracing::trace(tr_state, "Per-partition rate limiting: coordinator rejected");
                return coordinator_exception_container(exceptions::rate_limit_exception(s->ks_name(), s->cf_name(), op_type, true));
            }
        }
    }

    // The coordinator is not a replica. The decision whether to accept
    // or reject is left for replicas.
    slogger.trace("Per-partition rate limiting: replicas will decide");
    tracing::trace(tr_state, "Per-partition rate limiting: replicas will decide");
    return enforce_info;
}

static inline db::per_partition_rate_limit::info adjust_rate_limit_for_local_operation(
        const db::per_partition_rate_limit::info& info) {
    if (std::holds_alternative<db::per_partition_rate_limit::account_only>(info)) {
        // In this case, the coordinator has already accounted the operation,
        // so don't do it again on this shard
        return std::monostate();
    }
    return info;
}

class mutation_holder {
protected:
    size_t _size = 0;
    schema_ptr _schema;
public:
    virtual ~mutation_holder() {}
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, locator::effective_replication_map_ptr ermptr,
            tracing::trace_state_ptr tr_state) = 0;
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) = 0;
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, const inet_address_vector_replica_set& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) = 0;
    virtual bool is_shared() = 0;
    size_t size() const {
        return _size;
    }
    const schema_ptr& schema() {
        return _schema;
    }
    // called only when all replicas replied
    virtual void release_mutation() = 0;
    // called when reply is received
    // allows mutation holder to have its own accounting
    virtual void reply(gms::inet_address ep) {};
};

// different mutation for each destination (for read repairs)
class per_destination_mutation : public mutation_holder {
    std::unordered_map<gms::inet_address, lw_shared_ptr<const frozen_mutation>> _mutations;
    dht::token _token;
public:
    per_destination_mutation(const std::unordered_map<gms::inet_address, std::optional<mutation>>& mutations) {
        for (auto&& m : mutations) {
            lw_shared_ptr<const frozen_mutation> fm;
            if (m.second) {
                _schema = m.second.value().schema();
                _token = m.second.value().token();
                fm = make_lw_shared<const frozen_mutation>(freeze(m.second.value()));
                _size += fm->representation().size();
            }
            _mutations.emplace(m.first, std::move(fm));
        }
    }
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, locator::effective_replication_map_ptr ermptr,
            tracing::trace_state_ptr tr_state) override {
        auto m = _mutations[ep];
        if (m) {
            const auto hid = ermptr->get_token_metadata().get_host_id(ep);
            return hm.store_hint(hid, ep, _schema, std::move(m), tr_state);
        } else {
            return false;
        }
    }
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) override {
        const auto my_ip = sp.my_address();
        auto m = _mutations[my_ip];
        if (m) {
            tracing::trace(tr_state, "Executing a mutation locally");
            return sp.apply_fence(sp.mutate_locally(_schema, *m, std::move(tr_state), db::commitlog::force_sync::no, timeout, rate_limit_info), fence, my_ip);
        }
        return make_ready_future<>();
    }
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, const inet_address_vector_replica_set& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info, fencing_token fence) override {
        auto m = _mutations[ep];
        if (m) {
            tracing::trace(tr_state, "Sending a mutation to /{}", ep);
            return sp.remote().send_mutation(netw::messaging_service::msg_addr{ep, 0}, timeout, tracing::make_trace_info(tr_state),
                    *m, forward, sp.my_address(), this_shard_id(),
                    response_id, rate_limit_info, fence);
        }
        sp.got_response(response_id, ep, std::nullopt);
        return make_ready_future<>();
    }
    virtual bool is_shared() override {
        return false;
    }
    virtual void release_mutation() override {
        for (auto&& m : _mutations) {
            if (m.second) {
                m.second.release();
            }
        }
    }
    dht::token& token() {
        return _token;
    }
};

// same mutation for each destination
class shared_mutation : public mutation_holder {
protected:
    lw_shared_ptr<const frozen_mutation> _mutation;
public:
    explicit shared_mutation(frozen_mutation_and_schema&& fm_a_s)
            : _mutation(make_lw_shared<const frozen_mutation>(std::move(fm_a_s.fm))) {
        _size = _mutation->representation().size();
        _schema = std::move(fm_a_s.s);
    }
    explicit shared_mutation(const mutation& m) : shared_mutation(frozen_mutation_and_schema{freeze(m), m.schema()}) {
    }
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, locator::effective_replication_map_ptr ermptr,
            tracing::trace_state_ptr tr_state) override {
        const auto hid = ermptr->get_token_metadata().get_host_id(ep);
        return hm.store_hint(hid, ep, _schema, _mutation, tr_state);
    }
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) override {
        tracing::trace(tr_state, "Executing a mutation locally");
        return sp.apply_fence(sp.mutate_locally(_schema, *_mutation, std::move(tr_state), db::commitlog::force_sync::no, timeout, rate_limit_info), fence, sp.my_address());
    }
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, const inet_address_vector_replica_set& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) override {
        tracing::trace(tr_state, "Sending a mutation to /{}", ep);
        return sp.remote().send_mutation(netw::messaging_service::msg_addr{ep, 0}, timeout, tracing::make_trace_info(tr_state),
                *_mutation, forward, sp.my_address(), this_shard_id(),
                response_id, rate_limit_info, fence);
    }
    virtual bool is_shared() override {
        return true;
    }
    virtual void release_mutation() override {
        _mutation.release();
    }
};

// shared mutation, but gets sent as a hint
class hint_mutation : public shared_mutation {
public:
    using shared_mutation::shared_mutation;
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, locator::effective_replication_map_ptr,
            tracing::trace_state_ptr tr_state) override {
        throw std::runtime_error("Attempted to store a hint for a hint");
    }
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token fence) override {
        // A hint will be sent to all relevant endpoints when the endpoint it was originally intended for
        // becomes unavailable - this might include the current node
        return sp.apply_fence(sp.mutate_hint(_schema, *_mutation, std::move(tr_state), timeout), fence, sp.my_address());
    }
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, const inet_address_vector_replica_set& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info, fencing_token fence) override {
        return sp.remote().send_hint_mutation(
                netw::messaging_service::msg_addr{ep, 0}, timeout, tr_state,
                *_mutation, forward, sp.my_address(), this_shard_id(), response_id, rate_limit_info, fence);
    }
};

// A Paxos (AKA Compare And Swap, CAS) protocol involves multiple roundtrips between the coordinator
// and endpoint participants. Some endpoints may be unavailable or slow, and this does not stop the
// protocol progress. paxos_response_handler stores the shared state of the storage proxy associated
// with all the futures associated with a Paxos protocol step (prepare, accept, learn), including
// those outstanding by the time the step ends.
//
class paxos_response_handler : public enable_shared_from_this<paxos_response_handler> {
private:
    shared_ptr<storage_proxy> _proxy;
    locator::effective_replication_map_ptr _effective_replication_map_ptr;
    // The schema for the table the operation works upon.
    schema_ptr _schema;
    // Read command used by this CAS request.
    lw_shared_ptr<query::read_command> _cmd;
    // SERIAL or LOCAL SERIAL - influences what endpoints become Paxos protocol participants,
    // as well as Paxos quorum size. Is either set explicitly in the query or derived from
    // the value set by SERIAL CONSISTENCY [SERIAL|LOCAL SERIAL] control statement.
    db::consistency_level _cl_for_paxos;
    // QUORUM, LOCAL_QUORUM, etc - defines how many replicas to wait for in LEARN step.
    // Is either set explicitly or derived from the consistency level set in keyspace options.
    db::consistency_level _cl_for_learn;
    // Live endpoints, as per get_paxos_participants()
    inet_address_vector_replica_set _live_endpoints;
    // How many endpoints need to respond favourably for the protocol to progress to the next step.
    size_t _required_participants;
    // A deadline when the entire CAS operation timeout expires, derived from write_request_timeout_in_ms
    storage_proxy::clock_type::time_point _timeout;
    // A deadline when the CAS operation gives up due to contention, derived from cas_contention_timeout_in_ms
    storage_proxy::clock_type::time_point _cas_timeout;
    // The key this request is working on.
    dht::decorated_key _key;
    // service permit from admission control
    service_permit _permit;
    // how many replicas replied to learn
    uint64_t _learned = 0;

    // Unique request id generator.
    static thread_local uint64_t next_id;

    // Unique request id for logging purposes.
    const uint64_t _id = next_id++;

    // max pruning operations to run in parallel
    static constexpr uint16_t pruning_limit = 1000;

public:
    tracing::trace_state_ptr tr_state;

public:
    paxos_response_handler(shared_ptr<storage_proxy> proxy_arg, tracing::trace_state_ptr tr_state_arg,
        service_permit permit_arg,
        dht::decorated_key key_arg, schema_ptr schema_arg, lw_shared_ptr<query::read_command> cmd_arg,
        db::consistency_level cl_for_paxos_arg, db::consistency_level cl_for_learn_arg,
        storage_proxy::clock_type::time_point timeout_arg, storage_proxy::clock_type::time_point cas_timeout_arg);

    ~paxos_response_handler();

    // Result of PREPARE step, i.e. begin_and_repair_paxos().
    struct ballot_and_data {
        // Accepted ballot.
        utils::UUID ballot;
        // Current value of the requested key or none.
        foreign_ptr<lw_shared_ptr<query::result>> data;
    };

    // Steps of the Paxos protocol
    future<ballot_and_data> begin_and_repair_paxos(client_state& cs, unsigned& contentions, bool is_write);
    future<paxos::prepare_summary> prepare_ballot(utils::UUID ballot);
    future<bool> accept_proposal(lw_shared_ptr<paxos::proposal> proposal, bool timeout_if_partially_accepted = true);
    future<> learn_decision(lw_shared_ptr<paxos::proposal> proposal, bool allow_hints = false);
    void prune(utils::UUID ballot);
    uint64_t id() const {
        return _id;
    }
    size_t block_for() const {
        return _required_participants;
    }
    schema_ptr schema() const {
        return _schema;
    }
    const partition_key& key() const {
        return _key.key();
    }
    void set_cl_for_learn(db::consistency_level cl) {
        _cl_for_learn = cl;
    }
    // this is called with an id of a replica that replied to learn request
    // and returns true when quorum of such requests are accumulated
    bool learned(gms::inet_address ep);

    const locator::effective_replication_map_ptr& get_effective_replication_map() const noexcept {
        return _effective_replication_map_ptr;
    }
};

thread_local uint64_t paxos_response_handler::next_id = 0;

class cas_mutation : public mutation_holder {
    lw_shared_ptr<paxos::proposal> _proposal;
    shared_ptr<paxos_response_handler> _handler;
public:
    explicit cas_mutation(lw_shared_ptr<paxos::proposal> proposal, schema_ptr s, shared_ptr<paxos_response_handler> handler)
            : _proposal(std::move(proposal)), _handler(std::move(handler)) {
        _size = _proposal->update.representation().size();
        _schema = std::move(s);
    }
    virtual bool store_hint(db::hints::manager& hm, gms::inet_address ep, locator::effective_replication_map_ptr,
            tracing::trace_state_ptr tr_state) override {
        return false; // CAS does not save hints yet
    }
    virtual future<> apply_locally(storage_proxy& sp, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info,
            fencing_token) override {
        tracing::trace(tr_state, "Executing a learn locally");
        // TODO: Enforce per partition rate limiting in paxos
        return paxos::paxos_state::learn(sp, sp.remote().system_keyspace(), _schema, *_proposal, timeout, tr_state);
    }
    virtual future<> apply_remotely(storage_proxy& sp, gms::inet_address ep, const inet_address_vector_replica_set& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state, db::per_partition_rate_limit::info rate_limit_info, fencing_token) override {
        tracing::trace(tr_state, "Sending a learn to /{}", ep);
        // TODO: Enforce per partition rate limiting in paxos
        return sp.remote().send_paxos_learn(
                netw::messaging_service::msg_addr{ep, 0}, timeout, tracing::make_trace_info(tr_state),
                *_proposal, forward, sp.my_address(), this_shard_id(), response_id);
    }
    virtual bool is_shared() override {
        return true;
    }
    virtual void release_mutation() override {
        _proposal.release();
    }
    virtual void reply(gms::inet_address ep) override {
        // The handler will be set for "learn", but not for PAXOS repair
        // since repair may not include all replicas
        if (_handler) {
            if (_handler->learned(ep)) {
                // It's OK to start PRUNE while LEARN is still in progress: LEARN
                // doesn't read any data from system.paxos, and PRUNE tombstone
                // will cover LEARNed value even if it arrives out of order.
                _handler->prune(_proposal->ballot);
            }
        }
    };
};

class abstract_write_response_handler : public seastar::enable_shared_from_this<abstract_write_response_handler>, public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
protected:
    using error = storage_proxy::error;
    storage_proxy::response_id_type _id;
    promise<result<>> _ready; // available when cl is achieved
    shared_ptr<storage_proxy> _proxy;
    locator::effective_replication_map_ptr _effective_replication_map_ptr;
    tracing::trace_state_ptr _trace_state;
    db::consistency_level _cl;
    size_t _total_block_for = 0;
    db::write_type _type;
    std::unique_ptr<mutation_holder> _mutation_holder;
    inet_address_vector_replica_set _targets; // who we sent this mutation to
    // added dead_endpoints as a member here as well. This to be able to carry the info across
    // calls in helper methods in a convenient way. Since we hope this will be empty most of the time
    // it should not be a huge burden. (flw)
    inet_address_vector_topology_change _dead_endpoints;
    size_t _cl_acks = 0;
    bool _cl_achieved = false;
    bool _throttled = false;
    error _error = error::NONE;
    std::optional<sstring> _message;
    size_t _failed = 0; // only failures that may impact consistency
    size_t _all_failures = 0; // total amount of failures
    size_t _total_endpoints = 0;
    storage_proxy::write_stats& _stats;
    lw_shared_ptr<cdc::operation_result_tracker> _cdc_operation_result_tracker;
    timer<storage_proxy::clock_type> _expire_timer;
    service_permit _permit; // holds admission permit until operation completes
    db::per_partition_rate_limit::info _rate_limit_info;

protected:
    virtual bool waited_for(gms::inet_address from) = 0;
    void signal(gms::inet_address from) {
        _mutation_holder->reply(from);
        if (waited_for(from)) {
            signal();
        }
    }

public:
    abstract_write_response_handler(shared_ptr<storage_proxy> p,
            locator::effective_replication_map_ptr erm,
            db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets, tracing::trace_state_ptr trace_state,
            storage_proxy::write_stats& stats, service_permit permit, db::per_partition_rate_limit::info rate_limit_info, size_t pending_endpoints = 0,
            inet_address_vector_topology_change dead_endpoints = {}, is_cancellable cancellable = is_cancellable::no)
            : _id(p->get_next_response_id()), _proxy(std::move(p))
            , _effective_replication_map_ptr(std::move(erm))
            , _trace_state(trace_state), _cl(cl), _type(type), _mutation_holder(std::move(mh)), _targets(std::move(targets)),
              _dead_endpoints(std::move(dead_endpoints)), _stats(stats), _expire_timer([this] { timeout_cb(); }), _permit(std::move(permit)),
              _rate_limit_info(rate_limit_info) {
        // original comment from cassandra:
        // during bootstrap, include pending endpoints in the count
        // or we may fail the consistency level guarantees (see #833, #8058)
        _total_block_for = db::block_for(*_effective_replication_map_ptr, _cl) + pending_endpoints;
        ++_stats.writes;

        if (cancellable) {
            register_cancellable();
        }
    }
    virtual ~abstract_write_response_handler() {
        --_stats.writes;
        if (_cl_achieved) {
            if (_throttled) {
                _ready.set_value(bo::success());
            } else {
                _stats.background_writes--;
                _proxy->_global_stats.background_write_bytes -= _mutation_holder->size();
                _proxy->unthrottle();
            }
        } else {
            if (_error == error::TIMEOUT) {
                _ready.set_value(mutation_write_timeout_exception(get_schema()->ks_name(), get_schema()->cf_name(), _cl, _cl_acks, _total_block_for, _type));
            } else if (_error == error::FAILURE) {
                if (!_message) {
                    _ready.set_exception(mutation_write_failure_exception(get_schema()->ks_name(), get_schema()->cf_name(), _cl, _cl_acks, _failed, _total_block_for, _type));
                } else {
                    _ready.set_exception(mutation_write_failure_exception(*_message, _cl, _cl_acks, _failed, _total_block_for, _type));
                }
            } else if (_error == error::RATE_LIMIT) {
                _ready.set_value(exceptions::rate_limit_exception(get_schema()->ks_name(), get_schema()->cf_name(), db::operation_type::write, false));
            }
            if (_cdc_operation_result_tracker) {
                _cdc_operation_result_tracker->on_mutation_failed();
            }
        }

        update_cancellable_live_iterators();
    }
    bool is_counter() const {
        return _type == db::write_type::COUNTER;
    }

    bool is_view() const noexcept {
        return _type == db::write_type::VIEW;
    }

    void set_cdc_operation_result_tracker(lw_shared_ptr<cdc::operation_result_tracker> tracker) {
        _cdc_operation_result_tracker = std::move(tracker);
    }

    // While delayed, a request is not throttled.
    void unthrottle() {
        _stats.background_writes++;
        _proxy->_global_stats.background_write_bytes += _mutation_holder->size();
        _throttled = false;
        _ready.set_value(bo::success());
    }
    void signal(size_t nr = 1) {
        _cl_acks += nr;
        if (!_cl_achieved && _cl_acks >= _total_block_for) {
             _cl_achieved = true;
            delay(get_trace_state(), [] (abstract_write_response_handler* self) {
                if (self->_proxy->need_throttle_writes()) {
                    self->_throttled = true;
                    self->_proxy->_throttled_writes.push_back(self->_id);
                    ++self->_stats.throttled_writes;
                } else {
                    self->unthrottle();
                }
            });
        }
    }

    bool failure(gms::inet_address from, size_t count, error err, std::optional<sstring> msg) {
        if (waited_for(from)) {
            _failed += count;
            if (_total_block_for + _failed > _total_endpoints) {
                _error = err;
                _message = std::move(msg);
                delay(get_trace_state(), [] (abstract_write_response_handler*) { });
                return true;
            }
        }
        return false;
    }

    virtual bool failure(gms::inet_address from, size_t count, error err) {
        return failure(std::move(from), count, std::move(err), {});
    }

    void on_timeout() {
        if (_cl_achieved) {
            slogger.trace("Write is not acknowledged by {} replicas after achieving CL", get_targets());
        }
        _error = error::TIMEOUT;
        // We don't delay request completion after a timeout, but its possible we are currently delaying.
    }
    // return true on last ack
    bool response(gms::inet_address from) {
        auto it = boost::find(_targets, from);
        if (it != _targets.end()) {
            signal(from);
            using std::swap;
            swap(*it, _targets.back());
            _targets.pop_back();
        } else {
            slogger.warn("Receive outdated write ack from {}", from);
        }
        return _targets.size() == 0;
    }
    // return true if handler is no longer needed because
    // CL cannot be reached
    bool failure_response(gms::inet_address from, size_t count, error err, std::optional<sstring> msg) {
        if (boost::find(_targets, from) == _targets.end()) {
            // There is a little change we can get outdated reply
            // if the coordinator was restarted after sending a request and
            // getting reply back. The chance is low though since initial
            // request id is initialized to server starting time
            slogger.warn("Receive outdated write failure from {}", from);
            return false;
        }
        _all_failures += count;
        // we should not fail CL=ANY requests since they may succeed after
        // writing hints
        return _cl != db::consistency_level::ANY && failure(from, count, err, std::move(msg));
    }
    void check_for_early_completion() {
        if (_all_failures == _targets.size()) {
            // leftover targets are all reported error, so nothing to wait for any longer
            timeout_cb();
        }
    }
    void no_targets() {
        // We don't have any live targets and we should complete the handler now.
        // Either we already stored sufficient hints to achieve CL and the handler
        // is completed successfully (see hint_to_dead_endpoints), or we don't achieve
        // CL because we didn't store sufficient hints and we don't have live targets,
        // so the handler is completed with error.
        if (!_cl_achieved) {
            _error = error::FAILURE;
        }
        _proxy->remove_response_handler(_id);
    }
    void expire_at(storage_proxy::clock_type::time_point timeout) {
        _expire_timer.arm(timeout);
    }
    void on_released() {
        _expire_timer.cancel();
        if (_targets.size() == 0) {
            _mutation_holder->release_mutation();
        }
    }
    void timeout_cb() {
        if (_cl_achieved || _cl == db::consistency_level::ANY) {
            // we are here because either cl was achieved, but targets left in the handler are not
            // responding, so a hint should be written for them, or cl == any in which case
            // hints are counted towards consistency, so we need to write hints and count how much was written
            auto hints = _proxy->hint_to_dead_endpoints(_mutation_holder, get_targets(), _effective_replication_map_ptr,
                    _type, get_trace_state());
            signal(hints);
            if (_cl == db::consistency_level::ANY && hints) {
                slogger.trace("Wrote hint to satisfy CL.ANY after no replicas acknowledged the write");
            }
            if (_cl_achieved) { // For CL=ANY this can still be false
                for (auto&& ep : get_targets()) {
                    ++stats().background_replica_writes_failed.get_ep_stat(_effective_replication_map_ptr->get_topology(), ep);
                }
                stats().background_writes_failed += int(!_targets.empty());
            }
        }

        on_timeout();
        _proxy->remove_response_handler(_id);
    }
    db::view::update_backlog max_backlog() {
        return boost::accumulate(
                get_targets() | boost::adaptors::transformed([this] (gms::inet_address ep) {
                    return _proxy->get_backlog_of(ep);
                }),
                db::view::update_backlog::no_backlog(),
                [] (const db::view::update_backlog& lhs, const db::view::update_backlog& rhs) {
                    return std::max(lhs, rhs);
                });
    }
    // Calculates how much to delay completing the request. The delay adds to the request's inherent latency.
    template<typename Func>
    void delay(tracing::trace_state_ptr trace, Func&& on_resume) {
        auto backlog = max_backlog();
        auto delay = db::view::calculate_view_update_throttling_delay(backlog, _expire_timer.get_timeout());
        stats().last_mv_flow_control_delay = delay;
        stats().mv_flow_control_delay += delay.count();
        if (delay.count() == 0) {
            tracing::trace(trace, "Delay decision due to throttling: do not delay, resuming now");
            on_resume(this);
        } else {
            ++stats().throttled_base_writes;
            ++stats().total_throttled_base_writes;
            tracing::trace(trace, "Delaying user write due to view update backlog {}/{} by {}us",
                          backlog.get_current_bytes(), backlog.get_max_bytes(), delay.count());
            // Waited on indirectly.
            (void)sleep_abortable<seastar::steady_clock_type>(delay).finally([self = shared_from_this(), on_resume = std::forward<Func>(on_resume)] {
                --self->stats().throttled_base_writes;
                on_resume(self.get());
            }).handle_exception_type([] (const seastar::sleep_aborted& ignored) { });
        }
    }
    future<result<>> wait() {
        return _ready.get_future();
    }
    const inet_address_vector_replica_set& get_targets() const {
        return _targets;
    }
    const inet_address_vector_topology_change& get_dead_endpoints() const {
        return _dead_endpoints;
    }
    bool store_hint(db::hints::manager& hm, gms::inet_address ep, locator::effective_replication_map_ptr ermptr,
            tracing::trace_state_ptr tr_state) {
        return _mutation_holder->store_hint(hm, ep, std::move(ermptr), tr_state);
    }
    future<> apply_locally(storage_proxy::clock_type::time_point timeout, tracing::trace_state_ptr tr_state) {
        auto op = _proxy->start_write();
        return _mutation_holder->apply_locally(*_proxy, timeout, std::move(tr_state),
            _rate_limit_info,
            storage_proxy::get_fence(*_effective_replication_map_ptr));
    }
    future<> apply_remotely(gms::inet_address ep, const inet_address_vector_replica_set& forward,
            storage_proxy::response_id_type response_id, storage_proxy::clock_type::time_point timeout,
            tracing::trace_state_ptr tr_state) {
        return _mutation_holder->apply_remotely(*_proxy, ep, forward,
            response_id, timeout, std::move(tr_state), _rate_limit_info,
            storage_proxy::get_fence(*_effective_replication_map_ptr));
    }
    const schema_ptr& get_schema() const {
        return _mutation_holder->schema();
    }
    size_t get_mutation_size() const {
        return _mutation_holder->size();
    }
    storage_proxy::response_id_type id() const {
      return _id;
    }
    bool read_repair_write() {
        return !_mutation_holder->is_shared();
    }
    const tracing::trace_state_ptr& get_trace_state() const {
        return _trace_state;
    }
    storage_proxy::write_stats& stats() {
        return _stats;
    }
    friend storage_proxy;

private:
    void register_cancellable();
    // Called on destruction
    void update_cancellable_live_iterators();
};

class datacenter_write_response_handler : public abstract_write_response_handler {
    bool waited_for(gms::inet_address from) override {
        const auto& topo = _effective_replication_map_ptr->get_topology();
        return topo.is_me(from) || (topo.get_datacenter(from) == topo.get_datacenter());
    }

public:
    datacenter_write_response_handler(shared_ptr<storage_proxy> p,
            locator::effective_replication_map_ptr ermp,
            db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets,
            const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state,
            storage_proxy::write_stats& stats, service_permit permit, db::per_partition_rate_limit::info rate_limit_info) :
                abstract_write_response_handler(p, ermp, cl, type, std::move(mh), // can't move ermp, it's used below
                        std::move(targets), std::move(tr_state), stats, std::move(permit), rate_limit_info,
                        ermp->get_topology().count_local_endpoints(pending_endpoints), std::move(dead_endpoints)) {
        _total_endpoints = _effective_replication_map_ptr->get_topology().count_local_endpoints(_targets);
    }
};

class write_response_handler : public abstract_write_response_handler {
    bool waited_for(gms::inet_address from) override {
        return true;
    }
public:
    write_response_handler(shared_ptr<storage_proxy> p,
            locator::effective_replication_map_ptr ermp,
            db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets,
            const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state,
            storage_proxy::write_stats& stats, service_permit permit, db::per_partition_rate_limit::info rate_limit_info, is_cancellable cancellable) :
                abstract_write_response_handler(std::move(p), std::move(ermp), cl, type, std::move(mh),
                        std::move(targets), std::move(tr_state), stats, std::move(permit), rate_limit_info, pending_endpoints.size(), std::move(dead_endpoints), cancellable) {
        _total_endpoints = _targets.size();
    }
};

// This list contains `abstract_write_response_handler`s which were constructed as `cancellable`.
// When a `cancellable` handler is constructed, it adds itself to the list (see `register_cancellable`).
// We use the list to cancel handlers - as if the write timed out - on certain events, such as when
// we shutdown a node so that shutdown is not blocked.
// We don't add normal data path writes to the list, only background work such as hints and view updates.
class storage_proxy::cancellable_write_handlers_list : public bi::list<abstract_write_response_handler, bi::base_hook<abstract_write_response_handler>, bi::constant_time_size<false>> {
    // _live_iterators holds all iterators that point into the bi:list in the base class of this object.
    // If we remove a abstract_write_response_handler from the list, and an iterator happens to point
    // into it, we advance the iterator so it doesn't point at a removed object. See #4912.
    std::vector<iterator*> _live_iterators;
public:
    cancellable_write_handlers_list() {
        _live_iterators.reserve(10); // We only expect 1.
    }
    void register_live_iterator(iterator* itp) noexcept { // We don't tolerate failure, so abort instead
        _live_iterators.push_back(itp);
    }
    void unregister_live_iterator(iterator* itp) {
        _live_iterators.erase(boost::remove(_live_iterators, itp), _live_iterators.end());
    }
    void update_live_iterators(abstract_write_response_handler* handler) {
        // handler is being removed from the b::list, so if any live iterator points at it,
        // move it to the next object (this requires that the list is traversed in the forward
        // direction).
        for (auto& itp : _live_iterators) {
            if (&**itp == handler) {
                ++*itp;
            }
        }
    }
    class iterator_guard {
        cancellable_write_handlers_list& _handlers;
        iterator* _itp;
    public:
        iterator_guard(cancellable_write_handlers_list& handlers, iterator& it) : _handlers(handlers), _itp(&it) {
            _handlers.register_live_iterator(_itp);
        }
        ~iterator_guard() {
            _handlers.unregister_live_iterator(_itp);
        }
    };
};

void abstract_write_response_handler::register_cancellable() {
    _proxy->_cancellable_write_handlers_list->push_back(*this);
}


void abstract_write_response_handler::update_cancellable_live_iterators() {
    if (is_linked()) {
        _proxy->_cancellable_write_handlers_list->update_live_iterators(this);
    }
}

class datacenter_sync_write_response_handler : public abstract_write_response_handler {
    struct dc_info {
        size_t acks;
        size_t total_block_for;
        size_t total_endpoints;
        size_t failures;
    };
    std::unordered_map<sstring, dc_info> _dc_responses;
    bool waited_for(gms::inet_address from) override {
        auto& topology = _effective_replication_map_ptr->get_topology();
        sstring data_center = topology.get_datacenter(from);
        auto dc_resp = _dc_responses.find(data_center);

        if (dc_resp->second.acks < dc_resp->second.total_block_for) {
            ++dc_resp->second.acks;
            return true;
        }
        return false;
    }
public:
    datacenter_sync_write_response_handler(shared_ptr<storage_proxy> p, locator::effective_replication_map_ptr ermp, db::consistency_level cl, db::write_type type,
            std::unique_ptr<mutation_holder> mh, inet_address_vector_replica_set targets, const inet_address_vector_topology_change& pending_endpoints,
            inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state, storage_proxy::write_stats& stats, service_permit permit,
            db::per_partition_rate_limit::info rate_limit_info) :
        abstract_write_response_handler(std::move(p), std::move(ermp), cl, type, std::move(mh), targets, std::move(tr_state), stats, std::move(permit), rate_limit_info, 0, dead_endpoints) {
        auto* erm = _effective_replication_map_ptr.get();
        auto& topology = erm->get_topology();

        for (auto& target : targets) {
            auto dc = topology.get_datacenter(target);

            if (!_dc_responses.contains(dc)) {
                auto pending_for_dc = boost::range::count_if(pending_endpoints, [&topology, &dc] (const gms::inet_address& ep){
                    return topology.get_datacenter(ep) == dc;
                });
                size_t total_endpoints_for_dc = boost::range::count_if(targets, [&topology, &dc] (const gms::inet_address& ep){
                    return topology.get_datacenter(ep) == dc;
                });
                _dc_responses.emplace(dc, dc_info{0, db::local_quorum_for(*erm, dc) + pending_for_dc, total_endpoints_for_dc, 0});
                _total_block_for += pending_for_dc;
            }
        }
    }
    bool failure(gms::inet_address from, size_t count, error err) override {
        auto& topology = _effective_replication_map_ptr->get_topology();
        const sstring& dc = topology.get_datacenter(from);
        auto dc_resp = _dc_responses.find(dc);

        dc_resp->second.failures += count;
        _failed += count;
        if (dc_resp->second.total_block_for + dc_resp->second.failures > dc_resp->second.total_endpoints) {
            _error = err;
            return true;
        }
        return false;
    }
};

static future<> sleep_approx_50ms() {
    static thread_local std::default_random_engine re{std::random_device{}()};
    static thread_local std::uniform_int_distribution<> dist(0, 100);
    return seastar::sleep(std::chrono::milliseconds(dist(re)));
}

paxos_response_handler::paxos_response_handler(shared_ptr<storage_proxy> proxy_arg, tracing::trace_state_ptr tr_state_arg,
        service_permit permit_arg,
        dht::decorated_key key_arg, schema_ptr schema_arg, lw_shared_ptr<query::read_command> cmd_arg,
        db::consistency_level cl_for_paxos_arg, db::consistency_level cl_for_learn_arg,
        storage_proxy::clock_type::time_point timeout_arg, storage_proxy::clock_type::time_point cas_timeout_arg)
        : _proxy(proxy_arg)
        , _schema(std::move(schema_arg))
        , _cmd(cmd_arg)
        , _cl_for_paxos(cl_for_paxos_arg)
        , _cl_for_learn(cl_for_learn_arg)
        , _timeout(timeout_arg)
        , _cas_timeout(cas_timeout_arg)
        , _key(std::move(key_arg))
        , _permit(std::move(permit_arg))
        , tr_state(tr_state_arg) {
    auto ks_name = _schema->ks_name();
    replica::table& table = _proxy->_db.local().find_column_family(_schema->id());
    _effective_replication_map_ptr = table.get_effective_replication_map();
    storage_proxy::paxos_participants pp = _proxy->get_paxos_participants(ks_name, *_effective_replication_map_ptr, _key.token(), _cl_for_paxos);
    _live_endpoints = std::move(pp.endpoints);
    _required_participants = pp.required_participants;
    tracing::trace(tr_state, "Create paxos_response_handler for token {} with live: {} and required participants: {}",
            _key.token(), _live_endpoints, _required_participants);
    _proxy->get_stats().cas_foreground++;
    _proxy->get_stats().cas_total_running++;
    _proxy->get_stats().cas_total_operations++;
}

paxos_response_handler::~paxos_response_handler() {
    _proxy->get_stats().cas_total_running--;
}

/**
 * Begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies.
 *
 * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
 * nodes have seen the most recent commit. Otherwise, return null.
 */
future<paxos_response_handler::ballot_and_data>
paxos_response_handler::begin_and_repair_paxos(client_state& cs, unsigned& contentions, bool is_write) {
    api::timestamp_type min_timestamp_micros_to_use = 0;
    auto _ = shared_from_this(); // hold the handler until co-routine ends

    while(true) {
        if (storage_proxy::clock_type::now() > _cas_timeout) {
            co_await coroutine::return_exception(
                mutation_write_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl_for_paxos, 0, _required_participants, db::write_type::CAS)
            );
        }

        // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is
        // globally unique), but if we've got a prepare rejected already we also want to make sure
        // we pick a timestamp that has a chance to be promised, i.e. one that is greater that the
        // most recently known in progress (#5667). Lastly, we don't want to use a timestamp that is
        // older than the last one assigned by ClientState or operations may appear out-of-order
        // (#7801).
        api::timestamp_type ballot_micros = cs.get_timestamp_for_paxos(min_timestamp_micros_to_use);
        // Note that ballotMicros is not guaranteed to be unique if two proposal are being handled
        // concurrently by the same coordinator. But we still need ballots to be unique for each
        // proposal so we have to use getRandomTimeUUIDFromMicros.
        utils::UUID ballot = utils::UUID_gen::get_random_time_UUID_from_micros(std::chrono::microseconds{ballot_micros});

        paxos::paxos_state::logger.debug("CAS[{}] Preparing {}", _id, ballot);
        tracing::trace(tr_state, "Preparing {}", ballot);

        paxos::prepare_summary summary = co_await prepare_ballot(ballot);

        if (!summary.promised) {
            paxos::paxos_state::logger.debug("CAS[{}] Some replicas have already promised a higher ballot than ours; aborting", _id);
            tracing::trace(tr_state, "Some replicas have already promised a higher ballot than ours; aborting");
            contentions++;
            co_await sleep_approx_50ms();
            continue;
        }

        min_timestamp_micros_to_use = utils::UUID_gen::micros_timestamp(summary.most_recent_promised_ballot) + 1;

        std::optional<paxos::proposal> in_progress = std::move(summary.most_recent_proposal);

        // If we have an in-progress accepted ballot greater than the most recent commit
        // we know, then it's an in-progress round that needs to be completed, so do it.
        if (in_progress &&
            (!summary.most_recent_commit || (summary.most_recent_commit && in_progress->ballot.timestamp() > summary.most_recent_commit->ballot.timestamp()))) {
            paxos::paxos_state::logger.debug("CAS[{}] Finishing incomplete paxos round {}", _id, *in_progress);
            tracing::trace(tr_state, "Finishing incomplete paxos round {}", *in_progress);
            if (is_write) {
                ++_proxy->get_stats().cas_write_unfinished_commit;
            } else {
                ++_proxy->get_stats().cas_read_unfinished_commit;
            }

            auto refreshed_in_progress = make_lw_shared<paxos::proposal>(ballot, std::move(in_progress->update));

            bool is_accepted = co_await accept_proposal(refreshed_in_progress, false);

            if (is_accepted) {
                try {
                    co_await learn_decision(std::move(refreshed_in_progress), false);
                    continue;
                } catch (mutation_write_timeout_exception& e) {
                    e.type = db::write_type::CAS;
                    // we're still doing preparation for the paxos rounds, so we want to use the CAS (see CASSANDRA-8672)
                    co_return coroutine::exception(std::make_exception_ptr(e));
                }
            } else {
                paxos::paxos_state::logger.debug("CAS[{}] Some replicas have already promised a higher ballot than ours; aborting", _id);
                tracing::trace(tr_state, "Some replicas have already promised a higher ballot than ours; aborting");
                // sleep a random amount to give the other proposer a chance to finish
                contentions++;
                co_await sleep_approx_50ms();
                continue;
            }
            SCYLLA_ASSERT(true); // no fall through
        }

        // To be able to propose our value on a new round, we need a quorum of replica to have learn
        // the previous one. Why is explained at:
        // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
        // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may
        // just be a timing issue, but may also mean we lost messages), we pro-actively "repair"
        // those nodes, and retry.
        auto now_in_sec = utils::UUID_gen::unix_timestamp_in_sec(ballot);

        inet_address_vector_replica_set missing_mrc = summary.replicas_missing_most_recent_commit(_schema, now_in_sec);
        if (missing_mrc.size() > 0) {
            paxos::paxos_state::logger.debug("CAS[{}] Repairing replicas that missed the most recent commit", _id);
            tracing::trace(tr_state, "Repairing replicas that missed the most recent commit");
            std::array<std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token, inet_address_vector_replica_set>, 1>
                m{std::make_tuple(make_lw_shared<paxos::proposal>(std::move(*summary.most_recent_commit)), _schema, shared_from_this(), _key.token(), std::move(missing_mrc))};
            // create_write_response_handler is overloaded for paxos::proposal and will
            // create cas_mutation holder, which consequently will ensure paxos::learn is
            // used.
            auto f = _proxy->mutate_internal(std::move(m), db::consistency_level::ANY, false, tr_state, _permit, _timeout)
                    .then(utils::result_into_future<result<>>);

            // TODO: provided commits did not invalidate the prepare we just did above (which they
            // didn't), we could just wait for all the missing most recent commits to
            // acknowledge this decision and then move on with proposing our value.
            try {
                co_await std::move(f);
            } catch(...) {
                paxos::paxos_state::logger.debug("CAS[{}] Failure during commit repair {}", _id, std::current_exception());
                continue;
            }
        }
        co_return ballot_and_data{ballot, std::move(summary.data)};
    }
}

template<class T> struct dependent_false : std::false_type {};

// This function implement prepare stage of Paxos protocol and collects metadata needed to repair
// previously unfinished round (if there was one).
future<paxos::prepare_summary> paxos_response_handler::prepare_ballot(utils::UUID ballot) {
    struct {
        size_t errors = 0;
        // Whether the value of the requested key received from participating replicas match.
        bool digests_match = true;
        // Digest corresponding to the value of the requested key received from participating replicas.
        std::optional<query::result_digest> digest;
        // the promise can be set before all replies are received at which point
        // the optional will be disengaged so further replies are ignored
        std::optional<promise<paxos::prepare_summary>> p = promise<paxos::prepare_summary>();
        void set_value(paxos::prepare_summary&& s) {
            p->set_value(std::move(s));
            p.reset();
        }
        void set_exception(std::exception_ptr&& e) {
            p->set_exception(std::move(e));
            p.reset();
        }
    } request_tracker;

    auto f = request_tracker.p->get_future();

    // We may continue collecting prepare responses in the background after the reply is ready
    (void)do_with(paxos::prepare_summary(_live_endpoints.size()), std::move(request_tracker), shared_from_this(),
            [this, ballot] (paxos::prepare_summary& summary, auto& request_tracker, shared_ptr<paxos_response_handler>& prh) mutable -> future<> {
        paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: sending ballot {} to {}", _id, ballot, _live_endpoints);
        auto handle_one_msg = [this, &summary, ballot, &request_tracker] (gms::inet_address peer) mutable -> future<> {
            paxos::prepare_response response;
            try {
                // To generate less network traffic, only the closest replica (first one in the list of participants)
                // sends query result content while other replicas send digests needed to check consistency.
                bool only_digest = peer != _live_endpoints[0];
                auto da = digest_algorithm(*_proxy);
                const auto& topo = _effective_replication_map_ptr->get_topology();
                if (topo.is_me(peer)) {
                    tracing::trace(tr_state, "prepare_ballot: prepare {} locally", ballot);
                    response = co_await paxos::paxos_state::prepare(*_proxy, _proxy->remote().system_keyspace(), tr_state, _schema, *_cmd, _key.key(), ballot, only_digest, da, _timeout);
                } else {
                    response = co_await _proxy->remote().send_paxos_prepare(netw::msg_addr(peer), _timeout, tr_state, *_cmd, _key.key(), ballot, only_digest, da);
                }
            } catch (...) {
                if (request_tracker.p) {
                    auto ex = std::current_exception();
                    if (is_timeout_exception(ex)) {
                        paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: timeout while sending ballot {} to {}", _id,
                                    ballot, peer);
                        auto e = std::make_exception_ptr(mutation_write_timeout_exception(_schema->ks_name(), _schema->cf_name(),
                                    _cl_for_paxos, summary.committed_ballots_by_replica.size(),  _required_participants,
                                    db::write_type::CAS));
                        request_tracker.set_exception(std::move(e));
                    } else {
                        request_tracker.errors++;
                        paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: fail to send ballot {} to {}: {}", _id,
                                ballot, peer, ex);
                        if (_required_participants + request_tracker.errors > _live_endpoints.size()) {
                            auto e = std::make_exception_ptr(mutation_write_failure_exception(_schema->ks_name(),
                                        _schema->cf_name(), _cl_for_paxos, summary.committed_ballots_by_replica.size(),
                                        request_tracker.errors, _required_participants, db::write_type::CAS));
                            request_tracker.set_exception(std::move(e));
                        }
                    }
                }
                co_return;
            }

            if (!request_tracker.p) {
                co_return;
            }

            auto on_prepare_response = [&] (auto&& response) {
                using T = std::decay_t<decltype(response)>;
                if constexpr (std::is_same_v<T, utils::UUID>) {
                    tracing::trace(tr_state, "prepare_ballot: got more up to date ballot {} from /{}", response, peer);
                    paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: got more up to date ballot {} from {}", _id, response, peer);
                    // We got an UUID that prevented our proposal from succeeding
                    summary.update_most_recent_promised_ballot(response);
                    summary.promised = false;
                    request_tracker.set_value(std::move(summary));
                    return;
                } else if constexpr (std::is_same_v<T, paxos::promise>) {
                    utils::UUID mrc_ballot = utils::UUID_gen::min_time_UUID();

                    paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: got a response {} from {}", _id, response, peer);
                    tracing::trace(tr_state, "prepare_ballot: got a response {} from /{}", response, peer);

                    // Find the newest learned value among all replicas that answered.
                    // It will be used to "repair" replicas that did not learn this value yet.
                    if (response.most_recent_commit) {
                        mrc_ballot = response.most_recent_commit->ballot;

                        if (!summary.most_recent_commit ||
                            summary.most_recent_commit->ballot.timestamp() < mrc_ballot.timestamp()) {
                            summary.most_recent_commit = std::move(response.most_recent_commit);
                        }
                    }

                    // cannot throw since the memory was reserved ahead
                    summary.committed_ballots_by_replica.emplace(peer, mrc_ballot);

                    if (response.accepted_proposal) {
                        summary.update_most_recent_promised_ballot(response.accepted_proposal->ballot);

                        // If some response has an accepted proposal, then we should replay the proposal with the highest ballot.
                        // So find the highest accepted proposal here.
                        if (!summary.most_recent_proposal || response.accepted_proposal > summary.most_recent_proposal) {
                            summary.most_recent_proposal = std::move(response.accepted_proposal);
                        }
                    }

                    // Check if the query result attached to the promise matches query results received from other participants.
                    if (request_tracker.digests_match) {
                        if (response.data_or_digest) {
                            foreign_ptr<lw_shared_ptr<query::result>> data;
                            if (std::holds_alternative<foreign_ptr<lw_shared_ptr<query::result>>>(*response.data_or_digest)) {
                                data = std::move(std::get<foreign_ptr<lw_shared_ptr<query::result>>>(*response.data_or_digest));
                            }
                            auto& digest = data ? data->digest() : std::get<query::result_digest>(*response.data_or_digest);
                            if (request_tracker.digest) {
                                if (*request_tracker.digest != digest) {
                                    request_tracker.digests_match = false;
                                }
                            } else {
                                request_tracker.digest = digest;
                            }
                            if (request_tracker.digests_match && !summary.data && data) {
                                summary.data = std::move(data);
                            }
                        } else {
                            request_tracker.digests_match = false;
                        }
                        if (!request_tracker.digests_match) {
                            request_tracker.digest.reset();
                            summary.data.reset();
                        }
                    }

                    if (summary.committed_ballots_by_replica.size() == _required_participants) { // got all replies
                        tracing::trace(tr_state, "prepare_ballot: got enough replies to proceed");
                        paxos::paxos_state::logger.trace("CAS[{}] prepare_ballot: got enough replies to proceed", _id);
                        request_tracker.set_value(std::move(summary));
                    }
                } else {
                    static_assert(dependent_false<T>::value, "unexpected type!");
                }
            };
            std::visit(on_prepare_response, std::move(response));
        };
        co_return co_await coroutine::parallel_for_each(_live_endpoints, handle_one_msg);
    });

    return f;
}

// This function implements accept stage of the Paxos protocol.
future<bool> paxos_response_handler::accept_proposal(lw_shared_ptr<paxos::proposal> proposal, bool timeout_if_partially_accepted) {
    struct {
        // the promise can be set before all replies are received at which point
        // the optional will be disengaged so further replies are ignored
        std::optional<promise<bool>> p = promise<bool>();
        size_t accepts = 0;
        size_t rejects = 0;
        size_t errors = 0;

        size_t all_replies() const {
            return accepts + rejects + errors;
        }
        size_t non_accept_replies() const {
            return rejects + errors;
        }
        size_t non_error_replies() const {
            return accepts + rejects;
        }
        void set_value(bool v) {
            p->set_value(v);
            p.reset();
        }
        void set_exception(std::exception_ptr&& e) {
            p->set_exception(std::move(e));
            p.reset();
        }
    } request_tracker;

    auto f = request_tracker.p->get_future();

    // We may continue collecting propose responses in the background after the reply is ready
    (void)do_with(std::move(request_tracker), shared_from_this(), [this, timeout_if_partially_accepted, proposal = std::move(proposal)]
                           (auto& request_tracker, shared_ptr<paxos_response_handler>& prh) -> future<> {
        paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: sending commit {} to {}", _id, *proposal, _live_endpoints);
        auto handle_one_msg = [this, &request_tracker, timeout_if_partially_accepted, proposal = std::move(proposal)] (gms::inet_address peer) mutable -> future<> {
            bool is_timeout = false;
            std::optional<bool> accepted;
            const auto& topo = _effective_replication_map_ptr->get_topology();

            try {
                if (topo.is_me(peer)) {
                    tracing::trace(tr_state, "accept_proposal: accept {} locally", *proposal);
                    accepted = co_await paxos::paxos_state::accept(*_proxy, _proxy->remote().system_keyspace(), tr_state, _schema, proposal->update.decorated_key(*_schema).token(), *proposal, _timeout);
                } else {
                    accepted = co_await _proxy->remote().send_paxos_accept(netw::msg_addr(peer), _timeout, tr_state, *proposal);
                }
            } catch(...) {
                if (request_tracker.p) {
                    auto ex = std::current_exception();
                    if (is_timeout_exception(ex)) {
                        paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: timeout while sending proposal {} to {}",
                                _id, *proposal, peer);
                        is_timeout = true;
                    } else {
                        paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: failure while sending proposal {} to {}: {}", _id,
                                *proposal, peer, ex);
                        request_tracker.errors++;
                    }
                }
            }

            if (!request_tracker.p) {
                // Ignore the response since a completion was already signaled.
                co_return;
            }

            if (accepted) {
                tracing::trace(tr_state, "accept_proposal: got \"{}\" from /{}", *accepted ? "accepted" : "rejected", peer);
                paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: got \"{}\" from {}", _id,
                        accepted ? "accepted" : "rejected", peer);

                *accepted ? request_tracker.accepts++ : request_tracker.rejects++;
            }

            /**
            * The code has two modes of operation, controlled by the timeout_if_partially_accepted parameter.
            *
            * In timeout_if_partially_accepted is false, we will return a failure as soon as a majority of nodes reject
            * the proposal. This is used when replaying a proposal from an earlier leader.
            *
            * Otherwise, we wait for either all replicas to respond or until we achieve
            * the desired quorum. We continue to wait for all replicas even after we know we cannot succeed
            * because we need to know if no node at all has accepted our proposal or if at least one has.
            * In the former case, a proposer is guaranteed no-one will replay its value; in the
            * latter we don't, so we must timeout in case another leader replays it before we
            * can; see CASSANDRA-6013.
            */
            if (request_tracker.accepts == _required_participants) {
                tracing::trace(tr_state, "accept_proposal: got enough accepts to proceed");
                paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: got enough accepts to proceed", _id);
                request_tracker.set_value(true);
            } else if (is_timeout) {
                auto e = std::make_exception_ptr(mutation_write_timeout_exception(_schema->ks_name(), _schema->cf_name(),
                            _cl_for_paxos, request_tracker.non_error_replies(), _required_participants, db::write_type::CAS));
                request_tracker.set_exception(std::move(e));
            } else if (_required_participants + request_tracker.errors > _live_endpoints.size()) {
                // We got one too many errors. The quorum is no longer reachable. We can fail here
                // timeout_if_partially_accepted or not because failing is always safe - a client cannot
                // assume that the value was not committed.
                auto e = std::make_exception_ptr(mutation_write_failure_exception(_schema->ks_name(),
                            _schema->cf_name(), _cl_for_paxos, request_tracker.non_error_replies(),
                            request_tracker.errors, _required_participants, db::write_type::CAS));
                request_tracker.set_exception(std::move(e));
            } else if (_required_participants + request_tracker.non_accept_replies()  > _live_endpoints.size() && !timeout_if_partially_accepted) {
                // In case there is no need to reply with a timeout if at least one node is accepted
                // we can fail the request as soon is we know a quorum is unreachable.
                tracing::trace(tr_state, "accept_proposal: got enough rejects to proceed");
                paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: got enough rejects to proceed", _id);
                request_tracker.set_value(false);
            } else if (request_tracker.all_replies() == _live_endpoints.size()) { // wait for all replies
                if (request_tracker.accepts == 0 && request_tracker.errors == 0) {
                    tracing::trace(tr_state, "accept_proposal: proposal is fully rejected");
                    paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: proposal is fully rejected", _id);
                    // Return false if fully refused. Consider errors as accepts here since it
                    // is not possible to know for sure.
                    request_tracker.set_value(false);
                } else {
                    // We got some rejects, but not all, and there were errors. So we can't know for
                    // sure that the proposal is fully rejected, and it is obviously not
                    // accepted, either.
                    paxos::paxos_state::logger.trace("CAS[{}] accept_proposal: proposal is partially rejected", _id);
                    tracing::trace(tr_state, "accept_proposal: proposal is partially rejected");
                    _proxy->get_stats().cas_write_timeout_due_to_uncertainty++;
                    // TODO: we report write timeout exception to be compatible with Cassandra,
                    // which uses write_timeout_exception to signal any "unknown" state.
                    // To be changed in scope of work on https://issues.apache.org/jira/browse/CASSANDRA-15350
                    auto e = std::make_exception_ptr(mutation_write_timeout_exception(_schema->ks_name(),
                                _schema->cf_name(), _cl_for_paxos, request_tracker.accepts, _required_participants,
                                db::write_type::CAS));
                    request_tracker.set_exception(std::move(e));
                }
            } // wait for more replies
        };
        co_return co_await coroutine::parallel_for_each(_live_endpoints, handle_one_msg);
    }); // do_with

    return f;
}

} // namespace service

// debug output in mutate_internal needs this
template <>
struct fmt::formatter<service::paxos_response_handler> : fmt::formatter<string_view> {
    auto format(const service::paxos_response_handler& h, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "paxos_response_handler{{{}}}", h.id());
    }
};

namespace service {

// This function implements learning stage of Paxos protocol
future<> paxos_response_handler::learn_decision(lw_shared_ptr<paxos::proposal> decision, bool allow_hints) {
    tracing::trace(tr_state, "learn_decision: committing {} with cl={}", *decision, _cl_for_learn);
    paxos::paxos_state::logger.trace("CAS[{}] learn_decision: committing {} with cl={}", _id, *decision, _cl_for_learn);
    // FIXME: allow_hints is ignored. Consider if we should follow it and remove if not.
    // Right now we do not store hints for when committing decisions.

    // `mutate_internal` behaves differently when its template parameter is a range of mutations and when it's
    // a range of (decision, schema, token)-tuples. Both code paths diverge on `create_write_response_handler`.
    // We use the first path for CDC mutations (if present) and the latter for "paxos mutations".
    // Attempts to send both kinds of mutations in one shot caused an infinite loop.
    future<> f_cdc = make_ready_future<>();
    if (_schema->cdc_options().enabled()) {
        auto update_mut = decision->update.unfreeze(_schema);
        const auto base_tbl_id = update_mut.column_family_id();
        std::vector<mutation> update_mut_vec{std::move(update_mut)};

        auto cdc = _proxy->get_cdc_service();
        if (cdc && cdc->needs_cdc_augmentation(update_mut_vec)) {
            auto cdc_shared = cdc->shared_from_this(); // keep CDC service alive
            auto [mutations, tracker] = co_await cdc->augment_mutation_call(_timeout, std::move(update_mut_vec), tr_state, _cl_for_learn);
            // Pick only the CDC ("augmenting") mutations
            std::erase_if(mutations, [base_tbl_id = std::move(base_tbl_id)] (const mutation& v) {
                return v.schema()->id() == base_tbl_id;
            });
            if (!mutations.empty()) {
                f_cdc = _proxy->mutate_internal(std::move(mutations), _cl_for_learn, false, tr_state, _permit, _timeout, std::move(tracker))
                        .then(utils::result_into_future<result<>>);
            }
        }
    }

    // Path for the "base" mutations
    std::array<std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token>, 1> m{std::make_tuple(std::move(decision), _schema, shared_from_this(), _key.token())};
    future<> f_lwt = _proxy->mutate_internal(std::move(m), _cl_for_learn, false, tr_state, _permit, _timeout)
            .then(utils::result_into_future<result<>>);

    co_await when_all_succeed(std::move(f_cdc), std::move(f_lwt)).discard_result();
}

void paxos_response_handler::prune(utils::UUID ballot) {
    if ( _proxy->get_stats().cas_now_pruning >= pruning_limit) {
        _proxy->get_stats().cas_coordinator_dropped_prune++;
        return;
    }
     _proxy->get_stats().cas_now_pruning++;
    _proxy->get_stats().cas_prune++;
    auto erm = _effective_replication_map_ptr;
    auto my_address = _proxy->my_address();
    // running in the background, but the amount of the bg job is limited by pruning_limit
    // it is waited by holding shared pointer to storage_proxy which guaranties
    // that storage_proxy::stop() will wait for this to complete
    (void)parallel_for_each(_live_endpoints, [this, ballot, erm, my_address] (gms::inet_address peer) mutable {
        if (peer == my_address) {
            tracing::trace(tr_state, "prune: prune {} locally", ballot);
            return paxos::paxos_state::prune(_proxy->remote().system_keyspace(), _schema, _key.key(), ballot, _timeout, tr_state);
        } else {
            tracing::trace(tr_state, "prune: send prune of {} to {}", ballot, peer);
            return _proxy->remote().send_paxos_prune(netw::msg_addr(peer), _timeout, tr_state, _schema->version(), _key.key(), ballot);
        }
    }).then_wrapped([this, h = shared_from_this()] (future<> f) {
        h->_proxy->get_stats().cas_now_pruning--;
        try {
            f.get();
        } catch (rpc::closed_error&) {
            // ignore errors due to closed connection
            tracing::trace(tr_state, "prune failed: connection closed");
        } catch (const mutation_write_timeout_exception& ex) {
            tracing::trace(tr_state, "prune failed: write timeout; received {:d} of {:d} required replies", ex.received, ex.block_for);
            paxos::paxos_state::logger.debug("CAS[{}] prune: failed {}", h->_id, std::current_exception());
        } catch (...) {
            tracing::trace(tr_state, "prune failed: {}", std::current_exception());
            paxos::paxos_state::logger.error("CAS[{}] prune: failed {}", h->_id, std::current_exception());
        }
    });
}

bool paxos_response_handler::learned(gms::inet_address ep) {
    if (_learned < _required_participants) {
        if (boost::range::find(_live_endpoints, ep) != _live_endpoints.end()) {
            _learned++;
            return _learned == _required_participants;
        }
    }
    return false;
}

static inet_address_vector_replica_set
replica_ids_to_endpoints(const locator::token_metadata& tm, const std::vector<locator::host_id>& replica_ids) {
    inet_address_vector_replica_set endpoints;
    endpoints.reserve(replica_ids.size());

    for (const auto& replica_id : replica_ids) {
        if (auto endpoint_opt = tm.get_endpoint_for_host_id_if_known(replica_id)) {
            endpoints.push_back(*endpoint_opt);
        }
    }

    return endpoints;
}

static std::vector<locator::host_id>
endpoints_to_replica_ids(const locator::token_metadata& tm, const inet_address_vector_replica_set& endpoints) {
    std::vector<locator::host_id> replica_ids;
    replica_ids.reserve(endpoints.size());

    for (const auto& endpoint : endpoints) {
        if (auto replica_id_opt = tm.get_host_id_if_known(endpoint)) {
            replica_ids.push_back(*replica_id_opt);
        }
    }

    return replica_ids;
}

query::max_result_size storage_proxy::get_max_result_size(const query::partition_slice& slice) const {
    if (_features.separate_page_size_and_safety_limit) {
        return _db.local().get_query_max_result_size();
    }
    // FIXME: Remove the code below once SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT
    //        cluster feature is released for more than 2 years and can be
    //        retired.
    if (!slice.options.contains<query::partition_slice::option::allow_short_read>() || slice.is_reversed()) {
        return _db.local().get_query_max_result_size().without_page_limit();
    } else {
        return query::max_result_size(query::result_memory_limiter::maximum_result_size);
    }
}

query::tombstone_limit storage_proxy::get_tombstone_limit() const {
    auto& db = _db.local();
    if (!db.is_internal_query() && _features.empty_replica_pages) {
        return query::tombstone_limit(db.get_config().query_tombstone_page_limit());
    }
    return query::tombstone_limit::max;
}

bool storage_proxy::need_throttle_writes() const {
    return get_global_stats().background_write_bytes > _background_write_throttle_threahsold || get_global_stats().queued_write_bytes > 6*1024*1024;
}

void storage_proxy::unthrottle() {
   // Here, we garbage-collect (from _throttled_writes) the response IDs which are no longer
   // relevant, because their handlers are gone.
   //
   // need_throttle_writes() may remain true for an indefinite amount of time, so without this piece of code,
   // _throttled_writes might also grow without any limit. We saw this happen in a throughput test once.
   //
   // Note that we only remove the irrelevant entries which are in front of the list.
   // We don't touch the middle of the list, so an irrelevant ID will still remain in the list if there is some
   // earlier ID which is still relevant. But since writes should have some reasonable finite timeout,
   // we assume that it's not a problem.
   //
   while (!_throttled_writes.empty() && !_response_handlers.contains(_throttled_writes.front())) {
       _throttled_writes.pop_front();
   }

   while(!need_throttle_writes() && !_throttled_writes.empty()) {
       auto id = _throttled_writes.front();
       _throttled_writes.pop_front();
       auto it = _response_handlers.find(id);
       if (it != _response_handlers.end()) {
           it->second->unthrottle();
       }
   }
}

storage_proxy::response_id_type storage_proxy::register_response_handler(shared_ptr<abstract_write_response_handler>&& h) {
    auto id = h->id();
    auto e = _response_handlers.emplace(id, std::move(h));
    SCYLLA_ASSERT(e.second);
    return id;
}

void storage_proxy::remove_response_handler(storage_proxy::response_id_type id) {
    auto entry = _response_handlers.find(id);
    SCYLLA_ASSERT(entry != _response_handlers.end());
    remove_response_handler_entry(std::move(entry));
}

void storage_proxy::remove_response_handler_entry(response_handlers_map::iterator entry) {
    entry->second->on_released();
    _response_handlers.erase(std::move(entry));
}

void storage_proxy::got_response(storage_proxy::response_id_type id, gms::inet_address from, std::optional<db::view::update_backlog> backlog) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        tracing::trace(it->second->get_trace_state(), "Got a response from /{}", from);
        if (it->second->response(from)) {
            remove_response_handler_entry(std::move(it)); // last one, remove entry. Will cancel expiration timer too.
        } else {
            it->second->check_for_early_completion();
        }
    }
    maybe_update_view_backlog_of(std::move(from), std::move(backlog));
}

void storage_proxy::got_failure_response(storage_proxy::response_id_type id, gms::inet_address from, size_t count, std::optional<db::view::update_backlog> backlog, error err, std::optional<sstring> msg) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        tracing::trace(it->second->get_trace_state(), "Got {} failures from /{}", count, from);
        if (it->second->failure_response(from, count, err, std::move(msg))) {
            remove_response_handler_entry(std::move(it));
        } else {
            it->second->check_for_early_completion();
        }
    }
    maybe_update_view_backlog_of(std::move(from), std::move(backlog));
}

void storage_proxy::maybe_update_view_backlog_of(gms::inet_address replica, std::optional<db::view::update_backlog> backlog) {
    if (backlog) {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        _view_update_backlogs.insert_or_assign(replica, view_update_backlog_timestamped{*backlog, now});
    }
}

void storage_proxy::update_view_update_backlog() {
    _max_view_update_backlog.add(get_db().local().get_view_update_backlog());
}

db::view::update_backlog storage_proxy::get_view_update_backlog() {
    return _max_view_update_backlog.fetch();
}

future<std::optional<db::view::update_backlog>> storage_proxy::get_view_update_backlog_if_changed() {
    if (this_shard_id() != 0) {
        on_internal_error(slogger, format("getting view update backlog for gossip on a non-gossip shard {}", this_shard_id()));
    }
    return _max_view_update_backlog.fetch_if_changed();
}

db::view::update_backlog storage_proxy::get_backlog_of(gms::inet_address ep) const {
    auto it = _view_update_backlogs.find(ep);
    if (it == _view_update_backlogs.end()) {
        return db::view::update_backlog::no_backlog();
    }
    return it->second.backlog;
}

future<result<>> storage_proxy::response_wait(storage_proxy::response_id_type id, clock_type::time_point timeout) {
    auto& handler = _response_handlers.find(id)->second;
    handler->expire_at(timeout);
    return handler->wait();
}

::shared_ptr<abstract_write_response_handler>& storage_proxy::get_write_response_handler(storage_proxy::response_id_type id) {
        return _response_handlers.find(id)->second;
}

result<storage_proxy::response_id_type> storage_proxy::create_write_response_handler(locator::effective_replication_map_ptr ermp,
                             db::consistency_level cl, db::write_type type, std::unique_ptr<mutation_holder> m,
                             inet_address_vector_replica_set targets, const inet_address_vector_topology_change& pending_endpoints, inet_address_vector_topology_change dead_endpoints, tracing::trace_state_ptr tr_state,
                             storage_proxy::write_stats& stats, service_permit permit, db::per_partition_rate_limit::info rate_limit_info, is_cancellable cancellable)
{
    shared_ptr<abstract_write_response_handler> h;
    auto& rs = ermp->get_replication_strategy();

    if (db::is_datacenter_local(cl)) {
        h = ::make_shared<datacenter_write_response_handler>(shared_from_this(), std::move(ermp), cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats, std::move(permit), rate_limit_info);
    } else if (cl == db::consistency_level::EACH_QUORUM && rs.get_type() == locator::replication_strategy_type::network_topology){
        h = ::make_shared<datacenter_sync_write_response_handler>(shared_from_this(), std::move(ermp), cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats, std::move(permit), rate_limit_info);
    } else {
        h = ::make_shared<write_response_handler>(shared_from_this(), std::move(ermp), cl, type, std::move(m), std::move(targets), pending_endpoints, std::move(dead_endpoints), std::move(tr_state), stats, std::move(permit), rate_limit_info, cancellable);
    }
    return bo::success(register_response_handler(std::move(h)));
}

seastar::metrics::label storage_proxy_stats::split_stats::datacenter_label("datacenter");

storage_proxy_stats::split_stats::split_stats(const sstring& category, const sstring& short_description_prefix, const sstring& long_description_prefix, const sstring& op_type, bool auto_register_metrics)
        : _short_description_prefix(short_description_prefix)
        , _long_description_prefix(long_description_prefix)
        , _category(category)
        , _op_type(op_type)
        , _auto_register_metrics(auto_register_metrics)
        , _sg(current_scheduling_group()) { }

storage_proxy_stats::write_stats::write_stats()
: writes_attempts(COORDINATOR_STATS_CATEGORY, "total_write_attempts", "total number of write requests", "mutation_data")
, writes_errors(COORDINATOR_STATS_CATEGORY, "write_errors", "number of write requests that failed", "mutation_data")
, background_replica_writes_failed(COORDINATOR_STATS_CATEGORY, "background_replica_writes_failed", "number of replica writes that timed out or failed after CL was reached", "mutation_data")
, read_repair_write_attempts(COORDINATOR_STATS_CATEGORY, "read_repair_write_attempts", "number of write operations in a read repair context", "mutation_data") { }

storage_proxy_stats::write_stats::write_stats(const sstring& category, bool auto_register_stats)
        : writes_attempts(category, "total_write_attempts", "total number of write requests", "mutation_data", auto_register_stats)
        , writes_errors(category, "write_errors", "number of write requests that failed", "mutation_data", auto_register_stats)
        , background_replica_writes_failed(category, "background_replica_writes_failed", "number of replica writes that timed out or failed after CL was reached", "mutation_data", auto_register_stats)
        , read_repair_write_attempts(category, "read_repair_write_attempts", "number of write operations in a read repair context", "mutation_data", auto_register_stats) { }

void storage_proxy_stats::write_stats::register_split_metrics_local() {
    writes_attempts.register_metrics_local();
    writes_errors.register_metrics_local();
    background_replica_writes_failed.register_metrics_local();
    read_repair_write_attempts.register_metrics_local();
}

void storage_proxy_stats::write_stats::register_stats() {
    namespace sm = seastar::metrics;
    auto new_metrics = sm::metric_groups();
    new_metrics.add_group(COORDINATOR_STATS_CATEGORY, {
            sm::make_summary("write_latency_summary", sm::description("Write latency summary"), [this] {return to_metrics_summary(write.summary());})(storage_proxy_stats::current_scheduling_group_label()).set_skip_when_empty(),
            sm::make_histogram("write_latency", sm::description("The general write latency histogram"),
                    {storage_proxy_stats::current_scheduling_group_label()},
                    [this]{return to_metrics_histogram(write.histogram());}).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),

            sm::make_queue_length("foreground_writes", [this] { return writes - background_writes; },
                           sm::description("number of currently pending foreground write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_queue_length("background_writes", background_writes,
                           sm::description("number of currently pending background write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_queue_length("current_throttled_base_writes", throttled_base_writes,
                           sm::description("number of currently throttled base replica write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_counter("throttled_base_writes_total", total_throttled_base_writes,
                           sm::description("number of throttled base replica write requests, a throttled write is one whose response was delayed, see mv_flow_control_delay_total"),
                           {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

            sm::make_gauge("last_mv_flow_control_delay", [this] { return std::chrono::duration<float>(last_mv_flow_control_delay).count(); },
                                          sm::description("delay (in seconds) added for MV flow control in the last request"),
                                          {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_counter("mv_flow_control_delay_total", [this] { return mv_flow_control_delay; },
                                          sm::description("total delay (in microseconds) added for MV flow control, to delay the response sent to finished writes, divide this by throttled_base_writes_total to find the average delay"),
                                          {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

            sm::make_total_operations("throttled_writes", throttled_writes,
                                      sm::description("number of throttled write requests"),
                                      {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

            sm::make_total_operations("write_timeouts", [this]{return write_timeouts.count();},
                           sm::description("number of write request failed due to a timeout"),
                           {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

            sm::make_total_operations("write_unavailable", [this]{return write_unavailables.count();},
                           sm::description("number write requests failed due to an \"unavailable\" error"),
                           {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

            sm::make_total_operations("write_rate_limited", [this]{return write_rate_limited_by_replicas.count();},
                           sm::description("number of write requests which were rejected by replicas because rate limit for the partition was reached."),
                           {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::rejected_by_coordinator_label(false)}).set_skip_when_empty(),

            sm::make_total_operations("write_rate_limited", [this]{return write_rate_limited_by_coordinator.count();},
                           sm::description("number of write requests which were rejected directly on the coordinator because rate limit for the partition was reached."),
                           {storage_proxy_stats::current_scheduling_group_label(),storage_proxy_stats::rejected_by_coordinator_label(true)}).set_skip_when_empty(),

            sm::make_total_operations("background_writes_failed", background_writes_failed,
                           sm::description("number of write requests that failed after CL was reached"),
                           {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

            sm::make_total_operations("writes_coordinator_outside_replica_set", writes_coordinator_outside_replica_set,
                    sm::description("number of CQL write requests which arrived to a non-replica and had to be forwarded to a replica"),
                    {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

            sm::make_total_operations("reads_coordinator_outside_replica_set", reads_coordinator_outside_replica_set,
                    sm::description("number of CQL read requests which arrived to a non-replica and had to be forwarded to a replica"),
                    {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

            sm::make_total_operations("writes_failed_due_to_too_many_in_flight_hints", writes_failed_due_to_too_many_in_flight_hints,
                    sm::description("number of CQL write requests which failed because the hinted handoff mechanism is overloaded "
                    "and cannot store any more in-flight hints"),
                    {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),
        });
    _metrics = std::exchange(new_metrics, {});
}

storage_proxy_stats::stats::stats()
        : write_stats()
        , data_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of data read requests", "data")
        , data_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of data read requests that completed", "data")
        , data_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of data read requests that failed", "data")
        , digest_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of digest read requests", "digest")
        , digest_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of digest read requests that completed", "digest")
        , digest_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of digest read requests that failed", "digest")
        , mutation_data_read_attempts(COORDINATOR_STATS_CATEGORY, "reads", "number of mutation data read requests", "mutation_data")
        , mutation_data_read_completed(COORDINATOR_STATS_CATEGORY, "completed_reads", "number of mutation data read requests that completed", "mutation_data")
        , mutation_data_read_errors(COORDINATOR_STATS_CATEGORY, "read_errors", "number of mutation data read requests that failed", "mutation_data") { }

void storage_proxy_stats::stats::register_split_metrics_local() {
    write_stats::register_split_metrics_local();

    data_read_attempts.register_metrics_local();
    data_read_completed.register_metrics_local();
    data_read_errors.register_metrics_local();
    digest_read_attempts.register_metrics_local();
    digest_read_completed.register_metrics_local();
    mutation_data_read_attempts.register_metrics_local();
    mutation_data_read_completed.register_metrics_local();
    mutation_data_read_errors.register_metrics_local();
}

void storage_proxy_stats::stats::register_stats() {
    namespace sm = seastar::metrics;
    write_stats::register_stats();
    auto new_metrics = sm::metric_groups();
    new_metrics.add_group(COORDINATOR_STATS_CATEGORY, {
        sm::make_summary("read_latency_summary", sm::description("Read latency summary"), [this] {return to_metrics_summary(read.summary());})(storage_proxy_stats::current_scheduling_group_label()).set_skip_when_empty(),
        sm::make_histogram("read_latency", sm::description("The general read latency histogram"),
                {storage_proxy_stats::current_scheduling_group_label()},
                [this]{ return to_metrics_histogram(read.histogram());}).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),

        sm::make_queue_length("foreground_reads", foreground_reads,
                sm::description("number of currently pending foreground read requests"),
                {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_queue_length("background_reads", [this] { return reads - foreground_reads; },
                       sm::description("number of currently pending background read requests"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("read_retries", read_retries,
                       sm::description("number of read retry attempts"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("canceled_read_repairs", global_read_repairs_canceled_due_to_concurrent_write,
                       sm::description("number of global read repairs canceled due to a concurrent write"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("foreground_read_repairs", read_repair_repaired_blocking,
                      sm::description("number of foreground read repairs"),
                      {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("background_read_repairs", read_repair_repaired_background,
                       sm::description("number of background read repairs"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("read_timeouts", [this]{return read_timeouts.count(); },
                       sm::description("number of read request failed due to a timeout"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("read_unavailable", [this]{return read_unavailables.count(); },
                       sm::description("number read requests failed due to an \"unavailable\" error"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("read_rate_limited", [this]{return read_rate_limited_by_replicas.count(); },
                       sm::description("number of read requests which were rejected by replicas because rate limit for the partition was reached."),
                       {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::rejected_by_coordinator_label(false)}).set_skip_when_empty(),

        sm::make_total_operations("read_rate_limited", [this]{return read_rate_limited_by_coordinator.count(); },
                       sm::description("number of read requests which were rejected directly on the coordinator because rate limit for the partition was reached."),
                       {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::rejected_by_coordinator_label(true)}).set_skip_when_empty(),

        sm::make_total_operations("range_timeouts", [this]{return range_slice_timeouts.count(); },
                       sm::description("number of range read operations failed due to a timeout"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("range_unavailable", [this]{return range_slice_unavailables.count(); },
                       sm::description("number of range read operations failed due to an \"unavailable\" error"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("speculative_digest_reads", speculative_digest_reads,
                       sm::description("number of speculative digest read requests that were sent"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("speculative_data_reads", speculative_data_reads,
                       sm::description("number of speculative data read requests that were sent"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_summary("cas_read_latency_summary", sm::description("CAS read latency summary"), [this] {return to_metrics_summary(cas_read.summary());})(storage_proxy_stats::current_scheduling_group_label()).set_skip_when_empty(),
        sm::make_summary("cas_write_latency_summary", sm::description("CAS write latency summary"), [this] {return to_metrics_summary(cas_write.summary());})(storage_proxy_stats::current_scheduling_group_label()).set_skip_when_empty(),

        sm::make_histogram("cas_read_latency", sm::description("Transactional read latency histogram"),
                {storage_proxy_stats::current_scheduling_group_label()},
                [this]{ return to_metrics_histogram(cas_read.histogram());}).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),

        sm::make_histogram("cas_write_latency", sm::description("Transactional write latency histogram"),
                {storage_proxy_stats::current_scheduling_group_label()},
                [this]{return to_metrics_histogram(cas_write.histogram());}).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),

        sm::make_total_operations("cas_write_timeouts", [this]{return cas_write_timeouts.count(); },
                       sm::description("number of transactional write request failed due to a timeout"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_write_unavailable", [this]{return cas_write_unavailables.count(); },
                       sm::description("number of transactional write requests failed due to an \"unavailable\" error"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_read_timeouts", [this]{return cas_read_timeouts.count(); },
                       sm::description("number of transactional read request failed due to a timeout"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_read_unavailable", [this]{return cas_read_unavailables.count(); },
                       sm::description("number of transactional read requests failed due to an \"unavailable\" error"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),


        sm::make_total_operations("cas_read_unfinished_commit", cas_read_unfinished_commit,
                       sm::description("number of transaction commit attempts that occurred on read"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_write_unfinished_commit", cas_write_unfinished_commit,
                       sm::description("number of transaction commit attempts that occurred on write"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_write_condition_not_met", cas_write_condition_not_met,
                       sm::description("number of transaction preconditions that did not match current values"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_write_timeout_due_to_uncertainty", cas_write_timeout_due_to_uncertainty,
                       sm::description("how many times write timeout was reported because of uncertainty in the result"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_failed_read_round_optimization", cas_failed_read_round_optimization,
                       sm::description("CAS read rounds issued only if previous value is missing on some replica"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_histogram("cas_read_contention", sm::description("how many contended reads were encountered"),
                       {storage_proxy_stats::current_scheduling_group_label()},
                       [this]{ return cas_read_contention.get_histogram(1, 8);}).set_skip_when_empty(),

        sm::make_histogram("cas_write_contention", sm::description("how many contended writes were encountered"),
                       {storage_proxy_stats::current_scheduling_group_label()},
                       [this]{ return cas_write_contention.get_histogram(1, 8);}).set_skip_when_empty(),

        sm::make_total_operations("cas_prune", cas_prune,
                       sm::description("how many times paxos prune was done after successful cas operation"),
                       {storage_proxy_stats::current_scheduling_group_label()}),

        sm::make_total_operations("cas_dropped_prune", cas_coordinator_dropped_prune,
                       sm::description("how many times a coordinator did not perform prune after cas"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_total_operations", cas_total_operations,
                       sm::description("number of total paxos operations executed (reads and writes)"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_gauge("cas_foreground", cas_foreground,
                        sm::description("how many paxos operations that did not yet produce a result are running"),
                        {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_gauge("cas_background", [this] { return cas_total_running - cas_foreground; },
                        sm::description("how many paxos operations are still running after a result was already returned"),
                        {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),
    });

    new_metrics.add_group(REPLICA_STATS_CATEGORY, {
        sm::make_total_operations("received_counter_updates", received_counter_updates,
                       sm::description("number of counter updates received by this node acting as an update leader"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("received_mutations", received_mutations,
                       sm::description("number of mutations received by a replica Node"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("forwarded_mutations", forwarded_mutations,
                       sm::description("number of mutations forwarded to other replica Nodes"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("forwarding_errors", forwarding_errors,
                       sm::description("number of errors during forwarding mutations to other replica Nodes"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("reads", replica_data_reads,
                       sm::description("number of remote data read requests this Node received"),
                       {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::op_type_label("data")}).set_skip_when_empty(),

        sm::make_total_operations("reads", replica_mutation_data_reads,
                       sm::description("number of remote mutation data read requests this Node received"),
                       {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::op_type_label("mutation_data")}).set_skip_when_empty(),

        sm::make_total_operations("reads", replica_digest_reads,
                       sm::description("number of remote digest read requests this Node received"),
                       {storage_proxy_stats::current_scheduling_group_label(), storage_proxy_stats::op_type_label("digest")}).set_skip_when_empty(),

        sm::make_total_operations("cross_shard_ops", replica_cross_shard_ops,
                       sm::description("number of operations that crossed a shard boundary"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_total_operations("cas_dropped_prune", cas_replica_dropped_prune,
                       sm::description("how many times a coordinator did not perform prune after cas"),
                       {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_counter("received_hints_total", received_hints_total,
                        sm::description("number of hints and MV hints received by this node"),
                        {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),

        sm::make_counter("received_hints_bytes_total", received_hints_bytes_total,
                        sm::description("total size of hints and MV hints received by this node"),
                        {storage_proxy_stats::current_scheduling_group_label()}).set_skip_when_empty(),
    });
    _metrics = std::exchange(new_metrics, {});
}

inline uint64_t& storage_proxy_stats::split_stats::get_ep_stat(const locator::topology& topo, gms::inet_address ep) noexcept {
    if (topo.is_me(ep)) {
        return _local.val;
    }

    try {
        sstring dc = topo.get_datacenter(ep);
        if (_auto_register_metrics) {
            register_metrics_for(dc, ep);
        }
        return _dc_stats[dc].val;
    } catch (...) {
        static thread_local uint64_t dummy_stat;
        slogger.error("Failed to obtain stats ({}), fall-back to dummy", std::current_exception());
        return dummy_stat;
    }
}

void storage_proxy_stats::split_stats::register_metrics_local() {
    namespace sm = seastar::metrics;
    auto new_metrics = sm::metric_groups();
    new_metrics.add_group(_category, {
        sm::make_counter(_short_description_prefix + sstring("_local_node"), [this] { return _local.val; },
                       sm::description(_long_description_prefix + "on a local Node"), {storage_proxy_stats::make_scheduling_group_label(_sg), op_type_label(_op_type)}).set_skip_when_empty()
    });
    _metrics = std::exchange(new_metrics, {});
}

void storage_proxy_stats::split_stats::register_metrics_for(sstring dc, gms::inet_address ep) {
    namespace sm = seastar::metrics;

    // if this is the first time we see an endpoint from this DC - add a
    // corresponding collectd metric
    if (auto [ignored, added] = _dc_stats.try_emplace(dc); added) {
        _metrics.add_group(_category, {
            sm::make_counter(_short_description_prefix + sstring("_remote_node"), [this, dc] { return _dc_stats[dc].val; },
                            sm::description(seastar::format("{} when communicating with external Nodes in another DC", _long_description_prefix)), {storage_proxy_stats::make_scheduling_group_label(_sg), datacenter_label(dc), op_type_label(_op_type)})
                                .set_skip_when_empty()
        });
    }
}

void storage_proxy_stats::global_write_stats::register_stats() {
    namespace sm = seastar::metrics;
    auto new_metrics = sm::metric_groups();
    new_metrics.add_group(COORDINATOR_STATS_CATEGORY, {
            sm::make_current_bytes("queued_write_bytes", queued_write_bytes,
                           sm::description("number of bytes in pending write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),

            sm::make_current_bytes("background_write_bytes", background_write_bytes,
                           sm::description("number of bytes in pending background write requests"),
                           {storage_proxy_stats::current_scheduling_group_label()}),
        });
    _metrics = std::exchange(new_metrics, {});
}

void storage_proxy_stats::global_stats::register_stats() {
    global_write_stats::register_stats();
}

// A helper structure for differentiating hints from mutations in overload resolution
struct hint_wrapper {
    mutation mut;
};

struct read_repair_mutation {
    std::unordered_map<gms::inet_address, std::optional<mutation>> value;
    locator::effective_replication_map_ptr ermp;
};

}

template <> struct fmt::formatter<service::hint_wrapper> : fmt::formatter<string_view> {
    auto format(const service::hint_wrapper& h, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "hint_wrapper{{{}}}", h.mut);
    }
};

template <>
struct fmt::formatter<service::read_repair_mutation> : fmt::formatter<string_view> {
    auto format(const service::read_repair_mutation& m, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", m.value);
    }
};

namespace service {

using namespace std::literals::chrono_literals;

storage_proxy::~storage_proxy() {
    SCYLLA_ASSERT(!_remote);
}

storage_proxy::storage_proxy(distributed<replica::database>& db, storage_proxy::config cfg, db::view::node_update_backlog& max_view_update_backlog,
        scheduling_group_key stats_key, gms::feature_service& feat, const locator::shared_token_metadata& stm, locator::effective_replication_map_factory& erm_factory)
    : _db(db)
    , _shared_token_metadata(stm)
    , _erm_factory(erm_factory)
    , _read_smp_service_group(cfg.read_smp_service_group)
    , _write_smp_service_group(cfg.write_smp_service_group)
    , _write_mv_smp_service_group(cfg.write_mv_smp_service_group)
    , _hints_write_smp_service_group(cfg.hints_write_smp_service_group)
    , _write_ack_smp_service_group(cfg.write_ack_smp_service_group)
    , _next_response_id(std::chrono::system_clock::now().time_since_epoch()/1ms)
    , _hints_resource_manager(*this, cfg.available_memory / 10, _db.local().get_config().max_hinted_handoff_concurrency)
    , _hints_manager(*this, _db.local().get_config().hints_directory(), cfg.hinted_handoff_enabled, _db.local().get_config().max_hint_window_in_ms(), _hints_resource_manager, _db)
    , _hints_directory_initializer(std::move(cfg.hints_directory_initializer))
    , _hints_for_views_manager(*this, _db.local().get_config().view_hints_directory(), {}, _db.local().get_config().max_hint_window_in_ms(), _hints_resource_manager, _db)
    , _stats_key(stats_key)
    , _features(feat)
    , _background_write_throttle_threahsold(cfg.available_memory / 10)
    , _mutate_stage{"storage_proxy_mutate", &storage_proxy::do_mutate}
    , _max_view_update_backlog(max_view_update_backlog)
    , _cancellable_write_handlers_list(std::make_unique<cancellable_write_handlers_list>()) {
    namespace sm = seastar::metrics;
    _metrics.add_group(storage_proxy_stats::COORDINATOR_STATS_CATEGORY, {
        sm::make_queue_length("current_throttled_writes", [this] { return _throttled_writes.size(); },
                       sm::description("number of currently throttled write requests")),
    });
    _metrics.add_group(storage_proxy_stats::REPLICA_STATS_CATEGORY, {
        sm::make_current_bytes("view_update_backlog", [this] { return _max_view_update_backlog.fetch_shard(this_shard_id()).get_current_bytes(); },
                       sm::description("Tracks the size of scylla_database_view_update_backlog and is used instead of that one to calculate the "
                                        "max backlog across all shards, which is then used by other nodes to calculate appropriate throttling delays "
                                        "if it grows too large. If it's notably different from scylla_database_view_update_backlog, it means "
                                        "that we're currently processing a write that generated a large number of view updates.")),
    });
    slogger.trace("hinted DCs: {}", cfg.hinted_handoff_enabled.to_configuration_string());
    _hints_manager.register_metrics("hints_manager");
    _hints_for_views_manager.register_metrics("hints_for_views_manager");
}

struct storage_proxy::remote& storage_proxy::remote() {
    return const_cast<struct remote&>(const_cast<const storage_proxy*>(this)->remote());
}

const struct storage_proxy::remote& storage_proxy::remote() const {
    if (_remote) {
        return *_remote;
    }

    // This error should not appear because the user should not be able to send queries
    // before `remote` is initialized, and user queries should be drained before `remote`
    // is destroyed; Scylla code should take care not to perform cluster queries outside
    // the lifetime of `remote` (it can still perform queries to local tables during
    // the entire lifetime of `storage_proxy`, which is larger than `remote`).
    //
    // If there's a bug though, fail the query.
    //
    // In the future we may want to introduce a 'recovery mode' in which Scylla starts
    // without contacting the cluster and allows the user to perform local queries (say,
    // to system tables), then this code path would be expected to happen if the user
    // tries a remote query in this recovery mode, in which case we should change it
    // from `on_internal_error` to a regular exception.
    on_internal_error(slogger,
        "attempted to perform remote query when `storage_proxy::remote` is unavailable");
}

const data_dictionary::database
storage_proxy::data_dictionary() const {
    return _db.local().as_data_dictionary();
}

storage_proxy::unique_response_handler::unique_response_handler(storage_proxy& p_, response_id_type id_) : id(id_), p(p_) {}
storage_proxy::unique_response_handler::unique_response_handler(unique_response_handler&& x) noexcept : id(x.id), p(x.p) { x.id = 0; };

storage_proxy::unique_response_handler&
storage_proxy::unique_response_handler::operator=(unique_response_handler&& x) noexcept {
    // this->p must equal x.p
    id = std::exchange(x.id, 0);
    return *this;
}

storage_proxy::unique_response_handler::~unique_response_handler() {
    if (id) {
        p.remove_response_handler(id);
    }
}
storage_proxy::response_id_type storage_proxy::unique_response_handler::release() {
    auto r = id;
    id = 0;
    return r;
}

// Invokes "apply" on every shard that is responsible for the given token, according to sharder::shard_for_writes
// Caller must keep the effective_replication_map alive around the apply operation.
template <typename Applier>
requires std::invocable<Applier, shard_id>
future<> apply_on_shards(const locator::effective_replication_map_ptr& erm, const schema& s, dht::token tok, Applier&& apply) {
    auto shards = erm->get_sharder(s).shard_for_writes(tok);
    if (shards.empty()) {
        return make_exception_future<>(std::runtime_error(format("No local shards for token {} of {}.{}", tok, s.ks_name(), s.cf_name())));
    }
    if (shards.size() == 1) [[likely]] {
        return apply(shards[0]);
    }
    return seastar::parallel_for_each(shards, std::move(apply));
}

future<>
storage_proxy::mutate_locally(const mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout, smp_service_group smp_grp, db::per_partition_rate_limit::info rate_limit_info) {
    auto erm = _db.local().find_column_family(m.schema()).get_effective_replication_map();
    auto apply = [this, erm, &m, tr_state, sync, timeout, smp_grp, rate_limit_info] (shard_id shard) {
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        auto shard_rate_limit = rate_limit_info;
        if (shard == this_shard_id()) {
            shard_rate_limit = adjust_rate_limit_for_local_operation(shard_rate_limit);
        }
        return _db.invoke_on(shard, {smp_grp, timeout},
                [s = global_schema_ptr(m.schema()),
                 m = freeze(m),
                 gtr = tracing::global_trace_state_ptr(std::move(tr_state)),
                 erm,
                 timeout,
                 sync,
                 shard_rate_limit] (replica::database& db) mutable -> future<> {
            return db.apply(s, m, gtr.get(), sync, timeout, shard_rate_limit);
        });
    };
    return apply_on_shards(erm, *m.schema(), m.token(), std::move(apply));
}

future<>
storage_proxy::mutate_locally(const schema_ptr& s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout,
        smp_service_group smp_grp, db::per_partition_rate_limit::info rate_limit_info) {
    auto erm = _db.local().find_column_family(s).get_effective_replication_map();
    auto apply = [this, erm, s, &m, tr_state, sync, timeout, smp_grp, rate_limit_info] (shard_id shard) {
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        auto shard_rate_limit = rate_limit_info;
        if (shard == this_shard_id()) {
            shard_rate_limit = adjust_rate_limit_for_local_operation(shard_rate_limit);
        }
        return _db.invoke_on(shard, {smp_grp, timeout},
                [&m, erm, gs = global_schema_ptr(s), gtr = tracing::global_trace_state_ptr(std::move(tr_state)), timeout, sync, shard_rate_limit] (replica::database& db) mutable -> future<> {
            return db.apply(gs, m, gtr.get(), sync, timeout, shard_rate_limit);
        });
    };
    return apply_on_shards(erm, *s, m.token(*s), std::move(apply));
}

future<>
storage_proxy::mutate_locally(std::vector<mutation> mutations, tracing::trace_state_ptr tr_state, clock_type::time_point timeout, smp_service_group smp_grp, db::per_partition_rate_limit::info rate_limit_info) {
    co_await coroutine::parallel_for_each(mutations, [&] (const mutation& m) mutable {
            return mutate_locally(m, tr_state, db::commitlog::force_sync::no, timeout, smp_grp, rate_limit_info);
    });
}

future<>
storage_proxy::mutate_locally(std::vector<mutation> mutation, tracing::trace_state_ptr tr_state, clock_type::time_point timeout, db::per_partition_rate_limit::info rate_limit_info) {
        return mutate_locally(std::move(mutation), tr_state, timeout, _write_smp_service_group, rate_limit_info);
}

future<>
storage_proxy::mutate_locally(std::vector<frozen_mutation_and_schema> mutations, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, clock_type::time_point timeout, db::per_partition_rate_limit::info rate_limit_info) {
    co_await coroutine::parallel_for_each(mutations, [&] (const frozen_mutation_and_schema& x) {
        return mutate_locally(x.s, x.fm, tr_state, sync, timeout, rate_limit_info);
    });
}

future<>
storage_proxy::mutate_hint(const schema_ptr& s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, clock_type::time_point timeout) {
    auto erm = _db.local().find_column_family(s).get_effective_replication_map();
    auto apply = [&, erm] (unsigned shard) {
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return _db.invoke_on(shard, {_hints_write_smp_service_group, timeout}, [&m, gs = global_schema_ptr(s), tr_state, timeout, erm] (replica::database& db) mutable -> future<> {
            return db.apply_hint(gs, m, tr_state, timeout);
        });
    };
    return apply_on_shards(erm, *s, m.token(*s), std::move(apply));
}

std::optional<replica::stale_topology_exception>
storage_proxy::apply_fence(fencing_token token, gms::inet_address caller_address) const noexcept {
    const auto fence_version = _shared_token_metadata.get_fence_version();
    if (!token || token.topology_version >= fence_version) {
        return std::nullopt;
    }
    static thread_local logger::rate_limit rate_limit(std::chrono::seconds(1));
    slogger.log(log_level::warn, rate_limit,
        "Stale topology detected, request has been fenced out, "
        "local fence version {}, request topology version {}, caller address {}",
        fence_version, token.topology_version, caller_address);
    return replica::stale_topology_exception(token.topology_version, fence_version);
}

template <typename T>
future<T> storage_proxy::apply_fence(future<T> future, fencing_token fence, gms::inet_address caller_address) const {
    if (!fence) {
        return std::move(future);
    }
    return future.then_wrapped([this, fence, caller_address](seastar::future<T>&& f) {
        if (f.failed()) {
            return std::move(f);
        }
        auto stale = apply_fence(fence, caller_address);
        return stale ? make_exception_future<T>(std::move(*stale)) : std::move(f);
    });
}

fencing_token storage_proxy::get_fence(const locator::effective_replication_map& erm) {
    // Writes to and reads from local tables don't have to be fenced. Moreover, they shouldn't, as they
    // can cause errors on the bootstrapping node. A specific scenario:
    // 1. A write request on a bootstrapping node is initiated. The write handler captures the
    // effective replication map. Let's suppose its version is 5.
    // 2. The handler is yielded on some co_await.
    // 3. The topology coordinator is progressing, and it doesn't wait for pending requests on this
    // node because it's not in the list of normal nodes.
    // 4. The topology coordinator increments the fencing version to 6 and writes it to the RAFT table.
    // 5. The bootstrapping node processes this Raft log entry. The fence version is updated to 6.
    // 6. The initial write handler wakes up. It sees that 6 > 5 and fails.
    // This scenario is only possible for local writes. Non-local writes never happen on the
    // bootstrapping node because it hasn't been published to clients yet.
    return erm.get_replication_strategy().get_type() == locator::replication_strategy_type::local ?
            fencing_token{} : fencing_token{erm.get_token_metadata().get_version()};
}

future<>
storage_proxy::mutate_counters_on_leader(std::vector<frozen_mutation_and_schema> mutations, db::consistency_level cl, clock_type::time_point timeout,
                                         tracing::trace_state_ptr trace_state, service_permit permit) {
    get_stats().received_counter_updates += mutations.size();
    {
        auto& update_ms = mutations;
        co_await coroutine::parallel_for_each(update_ms, [&] (frozen_mutation_and_schema& fm_a_s) -> future<> {
            co_await mutate_counter_on_leader_and_replicate(fm_a_s.s, std::move(fm_a_s.fm), cl, timeout, trace_state, permit);
        });
    }
}

future<>
storage_proxy::mutate_counter_on_leader_and_replicate(const schema_ptr& s, frozen_mutation fm, db::consistency_level cl, clock_type::time_point timeout,
                                                      tracing::trace_state_ptr trace_state, service_permit permit) {
    auto erm = _db.local().find_column_family(s).get_effective_replication_map();
    // FIXME: This does not handle intra-node tablet migration properly.
    // Refs https://github.com/scylladb/scylladb/issues/18180
    auto shard = erm->get_sharder(*s).shard_for_reads(fm.token(*s));
    bool local = shard == this_shard_id();
    get_stats().replica_cross_shard_ops += !local;
    return _db.invoke_on(shard, {_write_smp_service_group, timeout}, [&proxy = container(), gs = global_schema_ptr(s), fm = std::move(fm), cl, timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state)), permit = std::move(permit), local] (replica::database& db) {
        auto trace_state = gt.get();
        auto p = local ? std::move(permit) : /* FIXME: either obtain a real permit on this shard or hold original one across shard */ empty_service_permit();
        return db.apply_counter_update(gs, fm, timeout, trace_state).then([&proxy, cl, timeout, trace_state, p = std::move(p)] (mutation m) mutable {
            return proxy.local().replicate_counter_from_leader(std::move(m), cl, std::move(trace_state), timeout, std::move(p));
        });
    });
}

result<storage_proxy::response_id_type>
storage_proxy::create_write_response_handler_helper(schema_ptr s, const dht::token& token, std::unique_ptr<mutation_holder> mh,
        db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, is_cancellable cancellable) {
    replica::table& table = _db.local().find_column_family(s->id());
    auto erm = table.get_effective_replication_map();
    inet_address_vector_replica_set natural_endpoints = erm->get_natural_endpoints_without_node_being_replaced(token);
    inet_address_vector_topology_change pending_endpoints = erm->get_pending_endpoints(token);

    slogger.trace("creating write handler for token: {} natural: {} pending: {}", token, natural_endpoints, pending_endpoints);
    tracing::trace(tr_state, "Creating write handler for token: {} natural: {} pending: {}", token, natural_endpoints ,pending_endpoints);

    const bool coordinator_in_replica_set = std::find(natural_endpoints.begin(), natural_endpoints.end(),
            my_address()) != natural_endpoints.end();

    // Check if this node, which is serving as a coordinator for
    // the mutation, is also a replica for the partition being
    // changed. Mutations sent by drivers unaware of token
    // distribution create a lot of network noise and thus should be
    // accounted in the metrics.
    if (!coordinator_in_replica_set) {
        get_stats().writes_coordinator_outside_replica_set++;
    }

    // filter out natural_endpoints from pending_endpoints if the latter is not yet updated during node join
    auto itend = boost::range::remove_if(pending_endpoints, [&natural_endpoints] (gms::inet_address& p) {
        return boost::range::find(natural_endpoints, p) != natural_endpoints.end();
    });
    pending_endpoints.erase(itend, pending_endpoints.end());

    auto all = boost::range::join(natural_endpoints, pending_endpoints);
    auto all_hids = all | boost::adaptors::transformed([&erm] (const gms::inet_address& ep) {
        const auto& tm = erm->get_token_metadata();
        const auto maybe_host_id = tm.get_host_id_if_known(ep);
        if (maybe_host_id) {
            return *maybe_host_id;
        }
        // We need this additional check because even after removing the mapping IP-host ID corresponding
        // to this node from `locator::token_metadata` while decommissioning, we still perform mutations
        // targeting the local node.
        if (tm.get_topology().is_me(ep)) {
            return tm.get_topology().my_host_id();
        }
        on_internal_error(slogger, seastar::format("No mapping for {} in the passed effective replication map", ep));
    });

    if (cannot_hint(all_hids, type)) {
        get_stats().writes_failed_due_to_too_many_in_flight_hints++;
        // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
        // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
        // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
        // a small number of nodes causing problems, so we should avoid shutting down writes completely to
        // healthy nodes.  Any node with no hintsInProgress is considered healthy.
        return boost::outcome_v2::failure(overloaded_exception(_hints_manager.size_of_hints_in_progress()));
    }

    if (should_reject_due_to_view_backlog(all, s)) {
        // A base replica won't be able to send its view updates because there's too many of them.
        // To avoid any base or view inconsistencies, the request should be retried when the replica
        // isn't overloaded
        return boost::outcome_v2::failure(overloaded_exception("View update backlog is too high on node containing the base replica"));
    }
    // filter live endpoints from dead ones
    inet_address_vector_replica_set live_endpoints;
    inet_address_vector_topology_change dead_endpoints;
    live_endpoints.reserve(all.size());
    dead_endpoints.reserve(all.size());
    std::partition_copy(all.begin(), all.end(), std::back_inserter(live_endpoints),
            std::back_inserter(dead_endpoints), std::bind_front(&storage_proxy::is_alive, this));

    db::per_partition_rate_limit::info rate_limit_info;
    if (allow_limit && _db.local().can_apply_per_partition_rate_limit(*s, db::operation_type::write)) {
        auto r_rate_limit_info = choose_rate_limit_info(erm, _db.local(), coordinator_in_replica_set, db::operation_type::write, s, token, tr_state);
        if (!r_rate_limit_info) {
            return std::move(r_rate_limit_info).as_failure();
        }
        rate_limit_info = r_rate_limit_info.value();
    } else {
        slogger.trace("Operation is not rate limited");
    }

    slogger.trace("creating write handler with live: {} dead: {}", live_endpoints, dead_endpoints);
    tracing::trace(tr_state, "Creating write handler with live: {} dead: {}", live_endpoints, dead_endpoints);

    db::assure_sufficient_live_nodes(cl, *erm, live_endpoints, pending_endpoints);

    return create_write_response_handler(std::move(erm), cl, type, std::move(mh), std::move(live_endpoints), pending_endpoints,
            std::move(dead_endpoints), std::move(tr_state), get_stats(), std::move(permit), rate_limit_info, cancellable);
}

/**
 * Helper for create_write_response_handler, shared across mutate/mutate_atomically.
 * Both methods do roughly the same thing, with the latter intermixing batch log ops
 * in the logic.
 * Since ordering is (maybe?) significant, we need to carry some info across from here
 * to the hint method below (dead nodes).
 */
result<storage_proxy::response_id_type>
storage_proxy::create_write_response_handler(const mutation& m, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit) {
    return create_write_response_handler_helper(m.schema(), m.token(), std::make_unique<shared_mutation>(m), cl, type, tr_state,
            std::move(permit), allow_limit, is_cancellable::no);
}

result<storage_proxy::response_id_type>
storage_proxy::create_write_response_handler(const hint_wrapper& h, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit) {
    return create_write_response_handler_helper(h.mut.schema(), h.mut.token(), std::make_unique<hint_mutation>(h.mut), cl, type, tr_state,
            std::move(permit), allow_limit, is_cancellable::yes);
}

result<storage_proxy::response_id_type>
storage_proxy::create_write_response_handler(const read_repair_mutation& mut, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit) {
    inet_address_vector_replica_set endpoints;
    const auto& m = mut.value;
    endpoints.reserve(m.size());
    boost::copy(m | boost::adaptors::map_keys, std::inserter(endpoints, endpoints.begin()));
    auto mh = std::make_unique<per_destination_mutation>(m);

    slogger.trace("creating write handler for read repair token: {} endpoint: {}", mh->token(), endpoints);
    tracing::trace(tr_state, "Creating write handler for read repair token: {} endpoint: {}", mh->token(), endpoints);

    // No rate limiting for read repair
    return create_write_response_handler(std::move(mut.ermp), cl, type, std::move(mh), std::move(endpoints), inet_address_vector_topology_change(), inet_address_vector_topology_change(), std::move(tr_state), get_stats(), std::move(permit), std::monostate(), is_cancellable::no);
}

result<storage_proxy::response_id_type>
storage_proxy::create_write_response_handler(const std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token>& meta,
        db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit) {
    auto& [commit, s, h, t] = meta;

    return create_write_response_handler_helper(s, t, std::make_unique<cas_mutation>(std::move(commit), s, std::move(h)), cl,
            db::write_type::CAS, tr_state, std::move(permit), allow_limit, is_cancellable::no);
}

result<storage_proxy::response_id_type>
storage_proxy::create_write_response_handler(const std::tuple<lw_shared_ptr<paxos::proposal>, schema_ptr, shared_ptr<paxos_response_handler>, dht::token, inet_address_vector_replica_set>& meta,
        db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit) {
    auto& [commit, s, paxos_handler, token, endpoints] = meta;

    if (should_reject_due_to_view_backlog(endpoints, s)) {
        // A base replica won't be able to send its view updates because there's too many of them.
        // To avoid any base or view inconsistencies, the request should be retried when the replica
        // isn't overloaded
        return boost::outcome_v2::failure(overloaded_exception("View update backlog is too high on node containing the base replica"));
    }
    slogger.trace("creating write handler for paxos repair token: {} endpoint: {}", token, endpoints);
    tracing::trace(tr_state, "Creating write handler for paxos repair token: {} endpoint: {}", token, endpoints);

    auto ermp = paxos_handler->get_effective_replication_map();

    // No rate limiting for paxos (yet)
    return create_write_response_handler(std::move(ermp), cl, db::write_type::CAS, std::make_unique<cas_mutation>(std::move(commit), s, nullptr), std::move(endpoints),
                    inet_address_vector_topology_change(), inet_address_vector_topology_change(), std::move(tr_state), get_stats(), std::move(permit), std::monostate(), is_cancellable::no);
}

// After sending the write to replicas(targets), the replicas might generate send view updates. If the number of view updates
// that the replica is already sending is too high, the replica should not send any more view updates, and consequently,
// we should not send the write to the replica.
// This method returns false in usual conditions. In the rare case where the write generates view updates but we have filled
// the view update backlog over update_backlog::admission_control_threshold, it returns true to signal that the write
// should not be admitted at all.
template<typename Range>
bool storage_proxy::should_reject_due_to_view_backlog(const Range& targets, const schema_ptr& s) const {
    if (s->table().views().empty()) {
        return false;
    }
    return !std::ranges::all_of(targets, [this] (const gms::inet_address& ep) {
        return get_backlog_of(ep).relative_size() < 1;
    });
}

void storage_proxy::register_cdc_operation_result_tracker(const storage_proxy::unique_response_handler_vector& ids, lw_shared_ptr<cdc::operation_result_tracker> tracker) {
    if (!tracker) {
        return;
    }

    for (auto& id : ids) {
        auto& h = get_write_response_handler(id.id);
        if (h->get_schema()->cdc_options().enabled()) {
            h->set_cdc_operation_result_tracker(tracker);
        }
    }
}

void
storage_proxy::hint_to_dead_endpoints(response_id_type id, db::consistency_level cl) {
    auto& h = *get_write_response_handler(id);
    size_t hints = hint_to_dead_endpoints(h._mutation_holder, h.get_dead_endpoints(), h._effective_replication_map_ptr,
            h._type, h.get_trace_state());

    if (cl == db::consistency_level::ANY) {
        // for cl==ANY hints are counted towards consistency
        h.signal(hints);
    }
}

template<typename Range, typename CreateWriteHandler>
future<result<storage_proxy::unique_response_handler_vector>> storage_proxy::mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, service_permit permit, CreateWriteHandler create_handler) {
    // apply is used to convert exceptions to exceptional future
    return futurize_invoke([this] (Range&& mutations, db::consistency_level cl, db::write_type type, service_permit permit, CreateWriteHandler create_handler) {
        unique_response_handler_vector ids;
        ids.reserve(std::distance(std::begin(mutations), std::end(mutations)));
        for (auto&& m : mutations) {
            auto r_handler = create_handler(m, cl, type, permit);
            if (!r_handler) {
                return make_ready_future<result<unique_response_handler_vector>>(std::move(r_handler).as_failure());
            }
            ids.emplace_back(*this, std::move(r_handler).value());
        }
        return make_ready_future<result<unique_response_handler_vector>>(std::move(ids));
    }, std::forward<Range>(mutations), cl, type, std::move(permit), std::move(create_handler));
}

template<typename Range>
future<result<storage_proxy::unique_response_handler_vector>> storage_proxy::mutate_prepare(Range&& mutations, db::consistency_level cl, db::write_type type, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit) {
    return mutate_prepare<>(std::forward<Range>(mutations), cl, type, std::move(permit), [this, tr_state = std::move(tr_state), allow_limit] (const typename std::decay_t<Range>::value_type& m, db::consistency_level cl, db::write_type type, service_permit permit) mutable {
        return create_write_response_handler(m, cl, type, tr_state, std::move(permit), allow_limit);
    });
}

future<result<>> storage_proxy::mutate_begin(unique_response_handler_vector ids, db::consistency_level cl,
                                     tracing::trace_state_ptr trace_state, std::optional<clock_type::time_point> timeout_opt) {
    return utils::result_parallel_for_each<result<>>(ids, [this, cl, timeout_opt] (unique_response_handler& protected_response) {
        auto response_id = protected_response.id;
        // This function, mutate_begin(), is called after a preemption point
        // so it's possible that other code besides our caller just ran. In
        // particular, Scylla may have noticed that a remote node went down,
        // called storage_proxy::on_down(), and removed some of the ongoing
        // handlers, including this id. If this happens, we need to ignore
        // this id - not try to look it up or start a send.
        if (!_response_handlers.contains(response_id)) {
            protected_response.release(); // Don't try to remove this id again
            // Requests that time-out normally below after response_wait()
            // result in an exception (see ~abstract_write_response_handler())
            // However, here we no longer have the handler or its information
            // to put in the exception. The exception is not needed for
            // correctness (e.g., hints are written by timeout_cb(), not
            // because of an exception here).
            slogger.debug("unstarted write cancelled for id {}", response_id);
            return make_ready_future<result<>>(bo::success());
        }
        // it is better to send first and hint afterwards to reduce latency
        // but request may complete before hint_to_dead_endpoints() is called and
        // response_id handler will be removed, so we will have to do hint with separate
        // frozen_mutation copy, or manage handler live time differently.
        hint_to_dead_endpoints(response_id, cl);

        auto timeout = timeout_opt.value_or(clock_type::now() + std::chrono::milliseconds(_db.local().get_config().write_request_timeout_in_ms()));
        // call before send_to_live_endpoints() for the same reason as above
        auto f = response_wait(response_id, timeout);
        send_to_live_endpoints(protected_response.release(), timeout); // response is now running and it will either complete or timeout
        return f;
    });
}

// this function should be called with a future that holds result of mutation attempt (usually
// future returned by mutate_begin()). The future should be ready when function is called.
future<result<>> storage_proxy::mutate_end(future<result<>> mutate_result, utils::latency_counter lc, write_stats& stats, tracing::trace_state_ptr trace_state) {
    SCYLLA_ASSERT(mutate_result.available());
    stats.write.mark(lc.stop().latency());

    return utils::result_futurize_try([&] {
        auto&& res = mutate_result.get();
        if (res) {
            tracing::trace(trace_state, "Mutation successfully completed");
        }
        return std::move(res);
    },  utils::result_catch<replica::no_such_keyspace>([&] (const auto& ex, auto&& handle) {
        tracing::trace(trace_state, "Mutation failed: write to non existing keyspace: {}", ex.what());
        slogger.trace("Write to non existing keyspace: {}", ex.what());
        return handle.into_future();
    }), utils::result_catch<mutation_write_timeout_exception>([&] (const auto& ex, auto&& handle) {
        tracing::trace(trace_state, "Mutation failed: write timeout; received {:d} of {:d} required replies", ex.received, ex.block_for);
        slogger.debug("Write timeout; received {} of {} required replies", ex.received, ex.block_for);
        stats.write_timeouts.mark();
        return handle.into_future();
    }), utils::result_catch<exceptions::unavailable_exception>([&] (const auto& ex, auto&& handle) {
        tracing::trace(trace_state, "Mutation failed: unavailable");
        stats.write_unavailables.mark();
        slogger.trace("Unavailable");
        return handle.into_future();
    }), utils::result_catch<exceptions::rate_limit_exception>([&] (const auto& ex, auto&& handle) {
        tracing::trace(trace_state, "Mutation failed: rate limit exceeded");
        if (ex.rejected_by_coordinator) {
            stats.write_rate_limited_by_coordinator.mark();
        } else {
            stats.write_rate_limited_by_replicas.mark();
        }
        slogger.trace("Rate limit exceeded");
        return handle.into_future();
    }), utils::result_catch<overloaded_exception>([&] (const auto& ex, auto&& handle) {
        tracing::trace(trace_state, "Mutation failed: overloaded");
        stats.write_unavailables.mark();
        slogger.trace("Overloaded");
        return handle.into_future();
    }), utils::result_catch_dots([&] (auto&& handle) {
        tracing::trace(trace_state, "Mutation failed: unknown reason");
        return handle.into_future();
    }));
}

gms::inet_address storage_proxy::find_leader_for_counter_update(const mutation& m, const locator::effective_replication_map& erm, db::consistency_level cl) {
    auto live_endpoints = get_live_endpoints(erm, m.token());

    if (live_endpoints.empty()) {
        throw exceptions::unavailable_exception(cl, block_for(erm, cl), 0);
    }

    const auto my_address = this->my_address();
    // Early return if coordinator can become the leader (so one extra internode message can be
    // avoided). With token-aware drivers this is the expected case, so we are doing it ASAP.
    if (boost::algorithm::any_of_equal(live_endpoints, my_address)) {
        return my_address;
    }

    const auto local_endpoints = boost::copy_range<inet_address_vector_replica_set>(live_endpoints
        | boost::adaptors::filtered(erm.get_topology().get_local_dc_filter()));

    if (local_endpoints.empty()) {
        // FIXME: O(n log n) to get maximum
        erm.get_topology().sort_by_proximity(my_address, live_endpoints);
        return live_endpoints[0];
    } else {
        static thread_local std::default_random_engine re{std::random_device{}()};
        std::uniform_int_distribution<> dist(0, local_endpoints.size() - 1);
        return local_endpoints[dist(re)];
    }
}

template<typename Range>
future<> storage_proxy::mutate_counters(Range&& mutations, db::consistency_level cl, tracing::trace_state_ptr tr_state, service_permit permit, clock_type::time_point timeout) {
    if (boost::empty(mutations)) {
        co_return;
    }

    slogger.trace("mutate_counters cl={}", cl);
    mlogger.trace("counter mutations={}", mutations);


    // Choose a leader for each mutation
    std::unordered_map<gms::inet_address, std::vector<frozen_mutation_and_schema>> leaders;

    // The interface doesn't preclude multiple keyspaces (and thus effective_replication_maps),
    // so we need a container for them. std::set<> will result in the fewest allocations if there is just one.
    std::set<locator::effective_replication_map_ptr> erms;

    const auto fence = fencing_token{_shared_token_metadata.get()->get_version()};
    for (auto& m : mutations) {
        auto& table = _db.local().find_column_family(m.schema()->id());
        auto erm = table.get_effective_replication_map();
        erms.insert(erm);
        auto leader = find_leader_for_counter_update(m, *erm, cl);
        leaders[leader].emplace_back(frozen_mutation_and_schema { freeze(m), m.schema() });
        // FIXME: check if CL can be reached
    }

    // Forward mutations to the leaders chosen for them
    auto my_address = this->my_address();
    co_await coroutine::parallel_for_each(leaders, [this, cl, timeout, tr_state = std::move(tr_state), permit = std::move(permit), my_address, fence] (auto& endpoint_and_mutations) -> future<> {
      auto first_schema = endpoint_and_mutations.second[0].s;

      try {
        auto endpoint = endpoint_and_mutations.first;

        if (endpoint == my_address) {
            // FIXME: this is also held while mutations are send to replicas which is not needed
            // because mutate_counters_on_leader do so internally. The way to fix it is to move the
            // entry to the phased barrier into the function, but the function is called from the RPC
            // handler as well and there it is more complicated. See FIXME there.
            auto op = start_write();
            co_await apply_fence(this->mutate_counters_on_leader(std::move(endpoint_and_mutations.second), cl, timeout, tr_state, permit), fence, my_address);
        } else {
            auto& mutations = endpoint_and_mutations.second;
            auto fms = boost::copy_range<std::vector<frozen_mutation>>(mutations | boost::adaptors::transformed([] (auto& m) {
                return std::move(m.fm);
            }));

            // Coordinator is preferred as the leader - if it's not selected we can assume
            // that the query was non-token-aware and bump relevant metric.
            get_stats().writes_coordinator_outside_replica_set += fms.size();

            co_await remote().send_counter_mutation(
                    netw::messaging_service::msg_addr{ endpoint_and_mutations.first, 0 }, timeout, tr_state,
                    std::move(fms), cl, fence);
        }
      } catch (...) {
        // The leader receives a vector of mutations and processes them together,
        // so if there is a timeout we don't really know which one is to "blame"
        // and what to put in ks and cf fields of write timeout exception.
        // Let's just use the schema of the first mutation in a vector.
        auto s = first_schema;
        auto exp = std::current_exception();
        {
            // Would be better to use the effective replication map we started with, but:
            // - this is wrong anyway since we picked a random schema
            // - we only use this to calculate some information for the error message
            // - the topology coordinator should prevent incompatible changes while requests
            //   (like this one) are in flight
            auto& table = _db.local().find_column_family(s->id());
            auto erm = table.get_effective_replication_map();
            try {
                std::rethrow_exception(std::move(exp));
            } catch (rpc::timeout_error&) {
                throw mutation_write_timeout_exception(s->ks_name(), s->cf_name(), cl, 0, db::block_for(*erm, cl), db::write_type::COUNTER);
            } catch (timed_out_error&) {
                throw mutation_write_timeout_exception(s->ks_name(), s->cf_name(), cl, 0, db::block_for(*erm, cl), db::write_type::COUNTER);
            } catch (rpc::closed_error&) {
                throw mutation_write_failure_exception(s->ks_name(), s->cf_name(), cl, 0, 1, db::block_for(*erm, cl), db::write_type::COUNTER);
            } catch (replica::stale_topology_exception& e) {
                throw mutation_write_failure_exception(e.what(), cl, 0, 1, db::block_for(*erm, cl), db::write_type::COUNTER);
            }
        }
      }
    });
}

storage_proxy::paxos_participants
storage_proxy::get_paxos_participants(const sstring& ks_name, const locator::effective_replication_map& erm, const dht::token &token, db::consistency_level cl_for_paxos) {
    inet_address_vector_replica_set natural_endpoints = erm.get_natural_endpoints_without_node_being_replaced(token);
    inet_address_vector_topology_change pending_endpoints = erm.get_pending_endpoints(token);

    if (cl_for_paxos == db::consistency_level::LOCAL_SERIAL) {
        auto local_dc_filter = erm.get_topology().get_local_dc_filter();
        auto itend = boost::range::remove_if(natural_endpoints, std::not_fn(std::cref(local_dc_filter)));
        natural_endpoints.erase(itend, natural_endpoints.end());
        itend = boost::range::remove_if(pending_endpoints, std::not_fn(std::cref(local_dc_filter)));
        pending_endpoints.erase(itend, pending_endpoints.end());
    }

    // filter out natural_endpoints from pending_endpoints if the latter is not yet updated during node join
    // should never happen, but better to be safe
    auto itend = boost::range::remove_if(pending_endpoints, [&natural_endpoints] (gms::inet_address& p) {
        return boost::range::find(natural_endpoints, p) != natural_endpoints.end();
    });
    pending_endpoints.erase(itend, pending_endpoints.end());

    const size_t participants = pending_endpoints.size() + natural_endpoints.size();
    const size_t quorum_size = natural_endpoints.size() / 2 + 1;
    const size_t required_participants = quorum_size + pending_endpoints.size();

    inet_address_vector_replica_set live_endpoints;
    live_endpoints.reserve(participants);

    boost::copy(boost::range::join(natural_endpoints, pending_endpoints) |
            boost::adaptors::filtered(std::bind_front(&storage_proxy::is_alive, this)), std::back_inserter(live_endpoints));

    if (live_endpoints.size() < required_participants) {
        throw exceptions::unavailable_exception(cl_for_paxos, required_participants, live_endpoints.size());
    }

    // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
    // Note that we fake an impossible number of required nodes in the unavailable exception
    // to nail home the point that it's an impossible operation no matter how many nodes are live.
    if (pending_endpoints.size() > 1) {
        throw exceptions::unavailable_exception(fmt::format(
                "Cannot perform LWT operation as there is more than one ({}) pending range movement", pending_endpoints.size()),
                cl_for_paxos, participants + 1, live_endpoints.size());
    }

    // Apart from the ballot, paxos_state::prepare() also sends the current value of the requested key.
    // If the values received from different replicas match, we skip a separate query stage thus saving
    // one network round trip. To generate less traffic, only closest replicas send data, others send
    // digests that are used to check consistency. For this optimization to work, we need to sort the
    // list of participants by proximity to this instance.
    sort_endpoints_by_proximity(erm.get_topology(), live_endpoints);

    return paxos_participants{std::move(live_endpoints), required_participants};
}


/**
 * Use this method to have these Mutations applied
 * across all replicas. This method will take care
 * of the possibility of a replica being down and hint
 * the data across to some other replica.
 *
 * @param mutations the mutations to be applied across the replicas
 * @param consistency_level the consistency level for the operation
 * @param tr_state trace state handle
 */
future<> storage_proxy::mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters) {
    return mutate_result(std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), allow_limit, raw_counters)
            .then(utils::result_into_future<result<>>);
}

future<result<>> storage_proxy::mutate_result(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters) {
    if (_cdc && _cdc->needs_cdc_augmentation(mutations)) {
        return _cdc->augment_mutation_call(timeout, std::move(mutations), tr_state, cl).then([this, cl, timeout, tr_state, permit = std::move(permit), raw_counters, cdc = _cdc->shared_from_this(), allow_limit](std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>&& t) mutable {
            auto mutations = std::move(std::get<0>(t));
            auto tracker = std::move(std::get<1>(t));
            return _mutate_stage(this, std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), raw_counters, allow_limit, std::move(tracker));
        });
    }
    return _mutate_stage(this, std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), raw_counters, allow_limit, nullptr);
}

future<result<>> storage_proxy::do_mutate(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit, bool raw_counters, db::allow_per_partition_rate_limit allow_limit, lw_shared_ptr<cdc::operation_result_tracker> cdc_tracker) {
    auto mid = raw_counters ? mutations.begin() : boost::range::partition(mutations, [] (auto&& m) {
        return m.schema()->is_counter();
    });
    return seastar::when_all_succeed(
        mutate_counters(boost::make_iterator_range(mutations.begin(), mid), cl, tr_state, permit, timeout),
        mutate_internal(boost::make_iterator_range(mid, mutations.end()), cl, false, tr_state, permit, timeout, std::move(cdc_tracker), allow_limit)
    ).then([] (std::tuple<result<>> res) {
        // For now, only mutate_internal returns a result<>
        return std::get<0>(std::move(res));
    });
}

future<> storage_proxy::replicate_counter_from_leader(mutation m, db::consistency_level cl, tracing::trace_state_ptr tr_state,
                                                      clock_type::time_point timeout, service_permit permit) {
    // FIXME: do not send the mutation to itself, it has already been applied (it is not incorrect to do so, though)
    return mutate_internal(std::array<mutation, 1>{std::move(m)}, cl, true, std::move(tr_state), std::move(permit), timeout)
            .then(utils::result_into_future<result<>>);
}

/*
 * Range template parameter can either be range of 'mutation' or a range of 'std::unordered_map<gms::inet_address, mutation>'.
 * create_write_response_handler() has specialization for both types. The one for the former uses keyspace to figure out
 * endpoints to send mutation to, the one for the late uses endpoints that are used as keys for the map.
 */
template<typename Range>
future<result<>>
storage_proxy::mutate_internal(Range mutations, db::consistency_level cl, bool counters, tracing::trace_state_ptr tr_state, service_permit permit,
                               std::optional<clock_type::time_point> timeout_opt, lw_shared_ptr<cdc::operation_result_tracker> cdc_tracker,
                               db::allow_per_partition_rate_limit allow_limit) {
    if (boost::empty(mutations)) {
        return make_ready_future<result<>>(bo::success());
    }

    slogger.trace("mutate cl={}", cl);
    mlogger.trace("mutations={}", mutations);

    // If counters is set it means that we are replicating counter shards. There
    // is no need for special handling anymore, since the leader has already
    // done its job, but we need to return correct db::write_type in case of
    // a timeout so that client doesn't attempt to retry the request.
    auto type = counters ? db::write_type::COUNTER
                         : (std::next(std::begin(mutations)) == std::end(mutations) ? db::write_type::SIMPLE : db::write_type::UNLOGGED_BATCH);
    utils::latency_counter lc;
    lc.start();

    return mutate_prepare(mutations, cl, type, tr_state, std::move(permit), allow_limit).then(utils::result_wrap([this, cl, timeout_opt, tracker = std::move(cdc_tracker),
            tr_state] (storage_proxy::unique_response_handler_vector ids) mutable {
        register_cdc_operation_result_tracker(ids, tracker);
        return mutate_begin(std::move(ids), cl, tr_state, timeout_opt);
    })).then_wrapped([this, p = shared_from_this(), lc, tr_state] (future<result<>> f) mutable {
        return p->mutate_end(std::move(f), lc, get_stats(), std::move(tr_state));
    });
}

future<result<>>
storage_proxy::mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl,
    clock_type::time_point timeout,
    bool should_mutate_atomically, tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters) {
    warn(unimplemented::cause::TRIGGERS);
    if (should_mutate_atomically) {
        SCYLLA_ASSERT(!raw_counters);
        return mutate_atomically_result(std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit));
    }
    return mutate_result(std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit), allow_limit, raw_counters);
}

/**
 * See mutate. Adds additional steps before and after writing a batch.
 * Before writing the batch (but after doing availability check against the FD for the row replicas):
 *      write the entire batch to a batchlog elsewhere in the cluster.
 * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
 *
 * @param mutations the Mutations to be applied across the replicas
 * @param consistency_level the consistency level for the operation
 */
future<>
storage_proxy::mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit) {
    return mutate_atomically_result(std::move(mutations), cl, timeout, std::move(tr_state), std::move(permit))
            .then(utils::result_into_future<result<>>);
}

static inet_address_vector_replica_set endpoint_filter(
        const noncopyable_function<bool(const gms::inet_address&)>& is_alive, gms::inet_address my_address,
        const sstring& local_rack, const std::unordered_map<sstring, std::unordered_set<gms::inet_address>>& endpoints) {
    // special case for single-node data centers
    if (endpoints.size() == 1 && endpoints.begin()->second.size() == 1) {
        return boost::copy_range<inet_address_vector_replica_set>(endpoints.begin()->second);
    }

    // strip out dead endpoints and localhost
    std::unordered_multimap<sstring, gms::inet_address> validated;

    auto is_valid = [&is_alive, my_address] (gms::inet_address input) {
        return input != my_address && is_alive(input);
    };

    for (auto& e : endpoints) {
        for (auto& a : e.second) {
            if (is_valid(a)) {
                validated.emplace(e.first, a);
            }
        }
    }

    typedef inet_address_vector_replica_set return_type;

    if (validated.size() <= 2) {
        return boost::copy_range<return_type>(validated | boost::adaptors::map_values);
    }

    if (validated.size() - validated.count(local_rack) >= 2) {
        // we have enough endpoints in other racks
        validated.erase(local_rack);
    }

    if (validated.bucket_count() == 1) {
        // we have only 1 `other` rack
        auto res = validated | boost::adaptors::map_values;
        if (validated.size() > 2) {
            return boost::copy_range<return_type>(
                    boost::copy_range<std::vector<gms::inet_address>>(res)
                            | boost::adaptors::sliced(0, 2));
        }
        return boost::copy_range<return_type>(res);
    }

    // randomize which racks we pick from if more than 2 remaining

    std::vector<sstring> racks = boost::copy_range<std::vector<sstring>>(validated | boost::adaptors::map_keys);

    static thread_local std::default_random_engine rnd_engine{std::random_device{}()};

    if (validated.bucket_count() > 2) {
        std::shuffle(racks.begin(), racks.end(), rnd_engine);
        racks.resize(2);
    }

    inet_address_vector_replica_set result;

    // grab a random member of up to two racks
    for (auto& rack : racks) {
        auto cpy = boost::copy_range<std::vector<gms::inet_address>>(validated.equal_range(rack) | boost::adaptors::map_values);
        std::uniform_int_distribution<size_t> rdist(0, cpy.size() - 1);
        result.emplace_back(cpy[rdist(rnd_engine)]);
    }

    return result;
}

future<result<>>
storage_proxy::mutate_atomically_result(std::vector<mutation> mutations, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit) {
    utils::latency_counter lc;
    lc.start();

    class context {
        storage_proxy& _p;
        schema_ptr _schema;
        locator::effective_replication_map_ptr _ermp;
        std::vector<mutation> _mutations;
        lw_shared_ptr<cdc::operation_result_tracker> _cdc_tracker;
        db::consistency_level _cl;
        clock_type::time_point _timeout;
        tracing::trace_state_ptr _trace_state;
        storage_proxy::stats& _stats;
        service_permit _permit;

        const utils::UUID _batch_uuid;
        const inet_address_vector_replica_set _batchlog_endpoints;

    public:
        context(storage_proxy & p, std::vector<mutation>&& mutations, lw_shared_ptr<cdc::operation_result_tracker>&& cdc_tracker, db::consistency_level cl, clock_type::time_point timeout, tracing::trace_state_ptr tr_state, service_permit permit)
                : _p(p)
                , _schema(_p.local_db().find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG))
                , _ermp(_p.local_db().find_column_family(_schema->id()).get_effective_replication_map())
                , _mutations(std::move(mutations))
                , _cdc_tracker(std::move(cdc_tracker))
                , _cl(cl)
                , _timeout(timeout)
                , _trace_state(std::move(tr_state))
                , _stats(p.get_stats())
                , _permit(std::move(permit))
                , _batch_uuid(utils::UUID_gen::get_time_UUID())
                , _batchlog_endpoints(
                        [this]() -> inet_address_vector_replica_set {
                            auto local_addr = _p.my_address();
                            auto& topology = _ermp->get_topology();
                            auto local_dc = topology.get_datacenter();
                            auto local_token_owners = _ermp->get_token_metadata().get_datacenter_racks_token_owners_ips().at(local_dc);
                            auto local_rack = topology.get_rack();
                            auto chosen_endpoints = endpoint_filter(std::bind_front(&storage_proxy::is_alive, &_p), local_addr,
                                                                    local_rack, local_token_owners);

                            if (chosen_endpoints.empty()) {
                                if (_cl == db::consistency_level::ANY) {
                                    return {local_addr};
                                }
                                throw exceptions::unavailable_exception(db::consistency_level::ONE, 1, 0);
                            }
                            return chosen_endpoints;
                        }()) {
                tracing::trace(_trace_state, "Created a batch context");
                tracing::set_batchlog_endpoints(_trace_state, _batchlog_endpoints);
        }

        future<result<>> send_batchlog_mutation(mutation m, db::consistency_level cl = db::consistency_level::ONE) {
            return _p.mutate_prepare<>(std::array<mutation, 1>{std::move(m)}, cl, db::write_type::BATCH_LOG, _permit, [this] (const mutation& m, db::consistency_level cl, db::write_type type, service_permit permit) {
                return _p.create_write_response_handler(_ermp, cl, type, std::make_unique<shared_mutation>(m), _batchlog_endpoints, {}, {}, _trace_state, _stats, std::move(permit), std::monostate(), is_cancellable::no);
            }).then(utils::result_wrap([this, cl] (unique_response_handler_vector ids) {
                _p.register_cdc_operation_result_tracker(ids, _cdc_tracker);
                return _p.mutate_begin(std::move(ids), cl, _trace_state, _timeout);
            }));
        }
        future<result<>> sync_write_to_batchlog() {
            auto m = _p.do_get_batchlog_mutation_for(_schema, _mutations, _batch_uuid, netw::messaging_service::current_version, db_clock::now());
            tracing::trace(_trace_state, "Sending a batchlog write mutation");
            return send_batchlog_mutation(std::move(m));
        };
        future<> async_remove_from_batchlog() {
            // delete batch
            auto key = partition_key::from_exploded(*_schema, {uuid_type->decompose(_batch_uuid)});
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            mutation m(_schema, key);
            m.partition().apply_delete(*_schema, clustering_key_prefix::make_empty(), tombstone(now, gc_clock::now()));

            tracing::trace(_trace_state, "Sending a batchlog remove mutation");
            return send_batchlog_mutation(std::move(m), db::consistency_level::ANY).then_wrapped([] (future<result<>> f) {
                auto print_exception = [] (const auto& ex) {
                    slogger.error("Failed to remove mutations from batchlog: {}", ex);
                };
                if (f.failed()) {
                    print_exception(f.get_exception());
                } else if (result<> res = f.get(); !res) {
                    print_exception(res.assume_error());
                }
            });
        };

        future<result<>> run() {
            return _p.mutate_prepare(_mutations, _cl, db::write_type::BATCH, _trace_state, _permit, db::allow_per_partition_rate_limit::no).then(utils::result_wrap([this] (unique_response_handler_vector ids) {
                return sync_write_to_batchlog().then(utils::result_wrap([this, ids = std::move(ids)] () mutable {
                    tracing::trace(_trace_state, "Sending batch mutations");
                    _p.register_cdc_operation_result_tracker(ids, _cdc_tracker);
                    return _p.mutate_begin(std::move(ids), _cl, _trace_state, _timeout);
                })).then(utils::result_wrap([this] {
                    return utils::then_ok_result<result<>>(async_remove_from_batchlog());
                }));
            }));
        }
    };

    auto mk_ctxt = [this, tr_state, timeout, permit = std::move(permit), cl] (std::vector<mutation> mutations, lw_shared_ptr<cdc::operation_result_tracker> tracker) mutable {
      try {
          return make_ready_future<lw_shared_ptr<context>>(make_lw_shared<context>(*this, std::move(mutations), std::move(tracker), cl, timeout, std::move(tr_state), std::move(permit)));
      } catch(...) {
          return make_exception_future<lw_shared_ptr<context>>(std::current_exception());
      }
    };
    auto cleanup = [p = shared_from_this(), lc, tr_state] (future<result<>> f) mutable {
        return p->mutate_end(std::move(f), lc, p->get_stats(), std::move(tr_state));
    };

    if (_cdc && _cdc->needs_cdc_augmentation(mutations)) {
        return _cdc->augment_mutation_call(timeout, std::move(mutations), std::move(tr_state), cl).then([mk_ctxt = std::move(mk_ctxt), cleanup = std::move(cleanup), cdc = _cdc->shared_from_this()](std::tuple<std::vector<mutation>, lw_shared_ptr<cdc::operation_result_tracker>>&& t) mutable {
            auto mutations = std::move(std::get<0>(t));
            auto tracker = std::move(std::get<1>(t));
            return std::move(mk_ctxt)(std::move(mutations), std::move(tracker)).then([] (lw_shared_ptr<context> ctxt) {
                return ctxt->run().finally([ctxt]{});
            }).then_wrapped(std::move(cleanup));
        });
    }

    return mk_ctxt(std::move(mutations), nullptr).then([] (lw_shared_ptr<context> ctxt) {
        return ctxt->run().finally([ctxt]{});
    }).then_wrapped(std::move(cleanup));
}

mutation storage_proxy::get_batchlog_mutation_for(const std::vector<mutation>& mutations, const utils::UUID& id, int32_t version, db_clock::time_point now) {
    auto schema = local_db().find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG);
    return do_get_batchlog_mutation_for(std::move(schema), mutations, id, version, now);
}

mutation storage_proxy::do_get_batchlog_mutation_for(schema_ptr schema, const std::vector<mutation>& mutations, const utils::UUID& id, int32_t version, db_clock::time_point now) {
    auto key = partition_key::from_singular(*schema, id);
    auto timestamp = api::new_timestamp();
    auto data = [&mutations] {
        std::vector<canonical_mutation> fm(mutations.begin(), mutations.end());
        bytes_ostream out;
        for (auto& m : fm) {
            ser::serialize(out, m);
        }
        return to_bytes(out.linearize());
    }();

    mutation m(schema, key);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("version"), version, timestamp);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("written_at"), now, timestamp);
    m.set_cell(clustering_key_prefix::make_empty(), to_bytes("data"), data_value(std::move(data)), timestamp);

    return m;
}

template<typename Range>
bool storage_proxy::cannot_hint(const Range& targets, db::write_type type) const {
    // if hints are disabled we "can always hint" since there's going to be no hint generated in this case
    return hints_enabled(type) &&
            _hints_manager.started() && // If the manager hasn't started yet, no mutation will be performed to another node.
                                        // No hint will need to be stored.
            boost::algorithm::any_of(targets, std::bind(&db::hints::manager::too_many_in_flight_hints_for, &_hints_manager, std::placeholders::_1));
}

future<> storage_proxy::send_to_endpoint(
        std::unique_ptr<mutation_holder> m,
        locator::effective_replication_map_ptr ermp,
        gms::inet_address target,
        inet_address_vector_topology_change pending_endpoints,
        db::write_type type,
        tracing::trace_state_ptr tr_state,
        write_stats& stats,
        allow_hints allow_hints,
        is_cancellable cancellable) {
    utils::latency_counter lc;
    lc.start();

    std::optional<clock_type::time_point> timeout;
    db::consistency_level cl = allow_hints ? db::consistency_level::ANY : db::consistency_level::ONE;
    if (type == db::write_type::VIEW) {
        // View updates have a near-infinite timeout to avoid incurring the extra work of writing hints
        // and to apply backpressure.
        timeout = clock_type::now() + 5min;
    }
    return mutate_prepare(std::array{std::move(m)}, cl, type, /* does view building should hold a real permit */ empty_service_permit(),
            [this, tr_state, erm = std::move(ermp), target = std::array{target}, pending_endpoints = std::move(pending_endpoints), &stats, cancellable] (
                std::unique_ptr<mutation_holder>& m,
                db::consistency_level cl,
                db::write_type type, service_permit permit) mutable {
        inet_address_vector_replica_set targets;
        targets.reserve(pending_endpoints.size() + 1);
        inet_address_vector_topology_change dead_endpoints;
        boost::algorithm::partition_copy(
                boost::range::join(pending_endpoints, target),
                std::inserter(targets, targets.begin()),
                std::back_inserter(dead_endpoints),
                std::bind_front(&storage_proxy::is_alive, this));
        slogger.trace("Creating write handler with live: {}; dead: {}", targets, dead_endpoints);
        db::assure_sufficient_live_nodes(cl, *erm, targets, pending_endpoints);
        return create_write_response_handler(
            std::move(erm),
            cl,
            type,
            std::move(m),
            std::move(targets),
            pending_endpoints,
            std::move(dead_endpoints),
            tr_state,
            stats,
            std::move(permit),
            std::monostate(), // TODO: Pass the correct enforcement type
            cancellable);
    }).then(utils::result_wrap([this, cl, tr_state = std::move(tr_state), timeout = std::move(timeout)] (unique_response_handler_vector ids) mutable {
        return mutate_begin(std::move(ids), cl, std::move(tr_state), std::move(timeout));
    })).then_wrapped([p = shared_from_this(), lc, &stats] (future<result<>> f) {
        return p->mutate_end(std::move(f), lc, stats, nullptr).then(utils::result_into_future<result<>>);
    });
}

future<> storage_proxy::send_to_endpoint(
        frozen_mutation_and_schema fm_a_s,
        locator::effective_replication_map_ptr ermp,
        gms::inet_address target,
        inet_address_vector_topology_change pending_endpoints,
        db::write_type type,
        tracing::trace_state_ptr tr_state,
        allow_hints allow_hints,
        is_cancellable cancellable) {
    return send_to_endpoint(
            std::make_unique<shared_mutation>(std::move(fm_a_s)),
            std::move(ermp),
            std::move(target),
            std::move(pending_endpoints),
            type,
            std::move(tr_state),
            get_stats(),
            allow_hints,
            cancellable);
}

future<> storage_proxy::send_to_endpoint(
        frozen_mutation_and_schema fm_a_s,
        locator::effective_replication_map_ptr ermp,
        gms::inet_address target,
        inet_address_vector_topology_change pending_endpoints,
        db::write_type type,
        tracing::trace_state_ptr tr_state,
        write_stats& stats,
        allow_hints allow_hints,
        is_cancellable cancellable) {
    return send_to_endpoint(
            std::make_unique<shared_mutation>(std::move(fm_a_s)),
            std::move(ermp),
            std::move(target),
            std::move(pending_endpoints),
            type,
            std::move(tr_state),
            stats,
            allow_hints,
            cancellable);
}

future<> storage_proxy::send_hint_to_endpoint(frozen_mutation_and_schema fm_a_s, locator::effective_replication_map_ptr ermp, gms::inet_address target) {
    return send_to_endpoint(
            std::make_unique<hint_mutation>(std::move(fm_a_s)),
            std::move(ermp),
            std::move(target),
            { },
            db::write_type::SIMPLE,
            tracing::trace_state_ptr(),
            get_stats(),
            allow_hints::no,
            is_cancellable::yes);
}

future<> storage_proxy::send_hint_to_all_replicas(frozen_mutation_and_schema fm_a_s) {
    std::array<hint_wrapper, 1> ms{hint_wrapper { fm_a_s.fm.unfreeze(fm_a_s.s) }};
    return mutate_internal(std::move(ms), db::consistency_level::ALL, false, nullptr, empty_service_permit())
            .then(utils::result_into_future<result<>>);
}

/**
 * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
 * is not available.
 *
 * Note about hints:
 *
 * | Hinted Handoff | Consist. Level |
 * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
 * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
 * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
 * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
 *
 * @throws OverloadedException if the hints cannot be written/enqueued
 */
 // returned future is ready when sent is complete, not when mutation is executed on all (or any) targets!
void storage_proxy::send_to_live_endpoints(storage_proxy::response_id_type response_id, clock_type::time_point timeout)
{
    // extra-datacenter replicas, grouped by dc
    std::unordered_map<sstring, inet_address_vector_replica_set> dc_groups;
    std::vector<std::pair<const sstring, inet_address_vector_replica_set>> local;
    local.reserve(3);

    auto handler_ptr = get_write_response_handler(response_id);
    auto& stats = handler_ptr->stats();
    auto& handler = *handler_ptr;
    auto& global_stats = handler._proxy->_global_stats;

    if (handler.get_targets().size() == 0) {
        // Usually we remove the response handler when receiving responses from all targets.
        // Here we don't have any live targets to get responses from, so we should complete
        // the write response handler immediately. Otherwise, it will remain active
        // until it timeouts.
        handler.no_targets();
        return;
    }

    if (handler.get_targets().size() != 1 || !is_me(handler.get_targets()[0])) {
        auto& topology = handler_ptr->_effective_replication_map_ptr->get_topology();
        auto local_dc = topology.get_datacenter();

        for(auto dest: handler.get_targets()) {
            auto node = topology.find_node(dest);
            if (!node) {
                // The caller is supposed to pick target nodes from the topology
                // contained in the effective_replication_map that is kept in the handler.
                // If the e_r_m is not in sync with the topology used to pick the targets
                // endpoints may be missing here and we better off returning an error
                // (or aborting in testing) rather than segfaulting here
                // (See https://github.com/scylladb/scylladb/issues/15138)
                on_internal_error(slogger, fmt::format("Node {} was not found in topology", dest));
            }
            const auto& dc = node->dc_rack().dc;
            // read repair writes do not go through coordinator since mutations are per destination
            if (handler.read_repair_write() || dc == local_dc) {
                local.emplace_back("", inet_address_vector_replica_set({dest}));
            } else {
                dc_groups[dc].push_back(dest);
            }
        }
    } else {
        // There is only one target replica and it is me
        local.emplace_back("", handler.get_targets());
    }

    auto all = boost::range::join(local, dc_groups);
    auto my_address = this->my_address();

    // lambda for applying mutation locally
    auto lmutate = [handler_ptr, response_id, this, my_address, timeout] () mutable {
        return handler_ptr->apply_locally(timeout, handler_ptr->get_trace_state())
                .then([response_id, this, my_address, h = std::move(handler_ptr), p = shared_from_this()] {
            // make mutation alive until it is processed locally, otherwise it
            // may disappear if write timeouts before this future is ready
            got_response(response_id, my_address, get_view_update_backlog());
        });
    };

    // lambda for applying mutation remotely
    auto rmutate = [this, handler_ptr, timeout, response_id, &global_stats] (gms::inet_address coordinator, const inet_address_vector_replica_set& forward) {
        auto msize = handler_ptr->get_mutation_size(); // can overestimate for repair writes
        global_stats.queued_write_bytes += msize;

        return handler_ptr->apply_remotely(coordinator, forward, response_id, timeout, handler_ptr->get_trace_state())
                .finally([this, p = shared_from_this(), h = std::move(handler_ptr), msize, &global_stats] {
            global_stats.queued_write_bytes -= msize;
            unthrottle();
        });
    };

    // OK, now send and/or apply locally
    for (typename decltype(dc_groups)::value_type& dc_targets : all) {
        auto& forward = dc_targets.second;
        // last one in forward list is a coordinator
        auto coordinator = forward.back();
        forward.pop_back();

        size_t forward_size = forward.size();
        future<> f = make_ready_future<>();


        if (handler.is_counter() && coordinator == my_address) {
            got_response(response_id, coordinator, std::nullopt);
        } else {
            if (!handler.read_repair_write()) {
                ++stats.writes_attempts.get_ep_stat(handler_ptr->_effective_replication_map_ptr->get_topology(), coordinator);
            } else {
                ++stats.read_repair_write_attempts.get_ep_stat(handler_ptr->_effective_replication_map_ptr->get_topology(), coordinator);
            }

            if (coordinator == my_address) {
                f = futurize_invoke(lmutate);
            } else {
                f = futurize_invoke(rmutate, coordinator, forward);
            }
        }

        // Waited on indirectly.
        (void)f.handle_exception([response_id, forward_size, coordinator, handler_ptr, p = shared_from_this(), &stats] (std::exception_ptr eptr) {
            ++stats.writes_errors.get_ep_stat(handler_ptr->_effective_replication_map_ptr->get_topology(), coordinator);
            error err = error::FAILURE;
            std::optional<sstring> msg;
            if (try_catch<replica::rate_limit_exception>(eptr)) {
                // There might be a lot of those, so ignore
                err = error::RATE_LIMIT;
            } else if (const auto* stale = try_catch<replica::stale_topology_exception>(eptr)) {
                msg = stale->what();
            } else if (try_catch<rpc::closed_error>(eptr)) {
                // ignore, disconnect will be logged by gossiper
            } else if (try_catch<seastar::gate_closed_exception>(eptr)) {
                // may happen during shutdown, ignore it
            } else if (try_catch<timed_out_error>(eptr)) {
                // from lmutate(). Ignore so that logs are not flooded
                // database total_writes_timedout counter was incremented.
                // It needs to be recorded that the timeout occurred locally though.
                err = error::TIMEOUT;
            } else if (auto* e = try_catch<db::virtual_table_update_exception>(eptr)) {
                msg = e->grab_cause();
            } else {
                slogger.error("exception during mutation write to {}: {}", coordinator, eptr);
            }
            p->got_failure_response(response_id, coordinator, forward_size + 1, std::nullopt, err, std::move(msg));
        });
    }
}

// returns number of hints stored
template<typename Range>
size_t storage_proxy::hint_to_dead_endpoints(std::unique_ptr<mutation_holder>& mh, const Range& targets,
        locator::effective_replication_map_ptr ermptr, db::write_type type, tracing::trace_state_ptr tr_state) noexcept
{
    if (hints_enabled(type)) {
        db::hints::manager& hints_manager = hints_manager_for(type);
        return boost::count_if(targets, [&mh, ermptr, tr_state = std::move(tr_state), &hints_manager] (gms::inet_address target) mutable -> bool {
            return mh->store_hint(hints_manager, target, ermptr, tr_state);
        });
    } else {
        return 0;
    }
}

future<result<>> storage_proxy::schedule_repair(locator::effective_replication_map_ptr ermp, std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::optional<mutation>>> diffs, db::consistency_level cl, tracing::trace_state_ptr trace_state,
                                        service_permit permit) {
    if (diffs.empty()) {
        return make_ready_future<result<>>(bo::success());
    }
    return mutate_internal(diffs | boost::adaptors::map_values | boost::adaptors::transformed([ermp] (auto& v) { return read_repair_mutation{std::move(v), ermp}; }), cl, false, std::move(trace_state), std::move(permit));
}

class abstract_read_resolver {
protected:
    enum class error_kind : uint8_t {
        FAILURE,
        DISCONNECT,
        RATE_LIMIT,
    };
    db::consistency_level _cl;
    size_t _targets_count;
    promise<result<>> _done_promise; // all target responded
    bool _request_failed = false; // will be true if request fails or timeouts
    timer<storage_proxy::clock_type> _timeout;
    schema_ptr _schema;
    size_t _failed = 0;

    virtual void on_failure(exceptions::coordinator_exception_container&& ex) = 0;
    virtual void on_timeout() = 0;
    virtual size_t response_count() const = 0;
    void fail_request(exceptions::coordinator_exception_container&& ex) {
        _request_failed = true;
        // The exception container was created on the same shard,
        // so it should be cheap to clone and not throw
        _done_promise.set_value(ex.clone());
        _timeout.cancel();
        on_failure(std::move(ex));
    }
public:
    abstract_read_resolver(schema_ptr schema, db::consistency_level cl, size_t target_count, storage_proxy::clock_type::time_point timeout)
        : _cl(cl)
        , _targets_count(target_count)
        , _schema(std::move(schema))
    {
        _timeout.set_callback([this] {
            on_timeout();
        });
        _timeout.arm(timeout);
    }
    virtual ~abstract_read_resolver() {};
    virtual void on_error(gms::inet_address ep, error_kind kind) = 0;
    future<result<>> done() {
        return _done_promise.get_future();
    }
    void error(gms::inet_address ep, std::exception_ptr eptr) {
        sstring why;
        error_kind kind = error_kind::FAILURE;
        if (try_catch<replica::rate_limit_exception>(eptr)) {
            // There might be a lot of those, so ignore
            kind = error_kind::RATE_LIMIT;
        } else if (try_catch<rpc::closed_error>(eptr)) {
            // do not report connection closed exception, gossiper does that
            kind = error_kind::DISCONNECT;
        } else if (try_catch<rpc::timeout_error>(eptr)) {
            // do not report timeouts, the whole operation will timeout and be reported
            return; // also do not report timeout as replica failure for the same reason
        } else if (try_catch<semaphore_timed_out>(eptr)) {
            // do not report timeouts, the whole operation will timeout and be reported
            return; // also do not report timeout as replica failure for the same reason
        } else if (try_catch<timed_out_error>(eptr)) {
            // do not report timeouts, the whole operation will timeout and be reported
            return; // also do not report timeout as replica failure for the same reason
        } else if (try_catch<abort_requested_exception>(eptr)) {
            // do not report aborts, they are triggered by shutdown or timeouts
        } else if (try_catch<gate_closed_exception>(eptr)) {
            // do not report gate_closed errors, they are triggered by shutdown (See #8995)
        } else if (auto ex = try_catch<rpc::remote_verb_error>(eptr)) {
            // Log remote read error with lower severity.
            // If it is really severe it we be handled on the host that sent
            // it.
            slogger.warn("Exception when communicating with {}, to read from {}.{}: {}", ep, _schema->ks_name(), _schema->cf_name(), ex->what());
        } else {
            slogger.error("Exception when communicating with {}, to read from {}.{}: {}", ep, _schema->ks_name(), _schema->cf_name(), eptr);
        }

        if (!_request_failed) { // request may fail only once.
            on_error(ep, kind);
        }
    }
};

struct digest_read_result {
    foreign_ptr<lw_shared_ptr<query::result>> result;
    bool digests_match;
};

class digest_read_resolver : public abstract_read_resolver {
    struct digest_and_last_pos {
        query::result_digest digest;
        std::optional<full_position> last_pos;

        digest_and_last_pos(query::result_digest digest, std::optional<full_position> last_pos)
            : digest(std::move(digest)), last_pos(std::move(last_pos))
        { }
    };
private:
    shared_ptr<storage_proxy> _proxy;
    locator::effective_replication_map_ptr _effective_replication_map_ptr;
    size_t _block_for;
    size_t _cl_responses = 0;
    promise<result<digest_read_result>> _cl_promise; // cl is reached
    bool _cl_reported = false;
    foreign_ptr<lw_shared_ptr<query::result>> _data_result;
    utils::small_vector<digest_and_last_pos, 3> _digest_results;
    api::timestamp_type _last_modified = api::missing_timestamp;
    size_t _target_count_for_cl; // _target_count_for_cl < _targets_count if CL=LOCAL and RRD.GLOBAL

    void on_timeout() override {
        fail_request(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, _cl_responses, _block_for, _data_result));
    }
    void on_failure(exceptions::coordinator_exception_container&& ex) override {
        if (!_cl_reported) {
            _cl_promise.set_value(std::move(ex));
        }
        // we will not need them any more
        _data_result = foreign_ptr<lw_shared_ptr<query::result>>();
        _digest_results.clear();
    }
public:
    digest_read_resolver(shared_ptr<storage_proxy> proxy,
            locator::effective_replication_map_ptr ermp,
            schema_ptr schema, db::consistency_level cl, size_t block_for, size_t target_count_for_cl, storage_proxy::clock_type::time_point timeout)
        : abstract_read_resolver(std::move(schema), cl, 0, timeout)
        , _proxy(std::move(proxy))
        , _effective_replication_map_ptr(std::move(ermp))
        , _block_for(block_for)
        , _target_count_for_cl(target_count_for_cl)
    {}
    virtual size_t response_count() const override {
        return _digest_results.size();
    }
    void add_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<query::result>> result) {
        if (!_request_failed) {
            // if only one target was queried digest_check() will be skipped so we can also skip digest calculation
            _digest_results.emplace_back(_targets_count == 1 ? query::result_digest() : *result->digest(), result->last_position());
            _last_modified = std::max(_last_modified, result->last_modified());
            if (!_data_result) {
                _data_result = std::move(result);
            }
            got_response(from);
        }
    }
    void add_digest(gms::inet_address from, query::result_digest digest, api::timestamp_type last_modified, std::optional<full_position> last_pos) {
        if (!_request_failed) {
            _digest_results.emplace_back(std::move(digest), std::move(last_pos));
            _last_modified = std::max(_last_modified, last_modified);
            got_response(from);
        }
    }
    bool digests_match() const {
        SCYLLA_ASSERT(response_count());
        if (response_count() == 1) {
            return true;
        }
        auto& first = *_digest_results.begin();
        return std::find_if(_digest_results.begin() + 1, _digest_results.end(), [&first] (const digest_and_last_pos& digest) { return digest.digest != first.digest; }) == _digest_results.end();
    }
    const std::optional<full_position>& min_position() const {
        return std::min_element(_digest_results.begin(), _digest_results.end(), [this] (const digest_and_last_pos& a, const digest_and_last_pos& b) {
            // last_pos can be disengaged when there are not results whatsoever
            if (!a.last_pos || !b.last_pos) {
                return bool(a.last_pos) < bool(b.last_pos);
            }
            return full_position::cmp(*_schema, *a.last_pos, *b.last_pos) < 0;
        })->last_pos;
    }
private:
    bool waiting_for(gms::inet_address ep) {
        const auto& topo = _effective_replication_map_ptr->get_topology();
        return db::is_datacenter_local(_cl) ? topo.is_me(ep) || (topo.get_datacenter(ep) == topo.get_datacenter()) : true;
    }
    void got_response(gms::inet_address ep) {
        if (!_cl_reported) {
            if (waiting_for(ep)) {
                _cl_responses++;
            }
            if (_cl_responses >= _block_for && _data_result) {
                _cl_reported = true;
                _cl_promise.set_value(digest_read_result{std::move(_data_result), digests_match()});
            }
        }
        if (is_completed()) {
            _timeout.cancel();
            _done_promise.set_value(bo::success());
        }
    }
    void on_error(gms::inet_address ep, error_kind kind) override {
        if (waiting_for(ep)) {
            _failed++;
        }
        if (kind == error_kind::DISCONNECT && _block_for == _target_count_for_cl) {
            // if the error is because of a connection disconnect and there is no targets to speculate
            // wait for timeout in hope that the client will issue speculative read
            // FIXME: resolver should have access to all replicas and try another one in this case
            return;
        }
        if (_block_for + _failed > _target_count_for_cl) {
            switch (kind) {
            case error_kind::RATE_LIMIT:
                fail_request(exceptions::rate_limit_exception(_schema->ks_name(), _schema->cf_name(), db::operation_type::read, false));
                break;
            case error_kind::DISCONNECT:
            case error_kind::FAILURE:
                fail_request(read_failure_exception(_schema->ks_name(), _schema->cf_name(), _cl, _cl_responses, _failed, _block_for, _data_result));
                break;
            }
        }
    }

public:
    future<result<digest_read_result>> has_cl() {
        return _cl_promise.get_future();
    }
    bool has_data() {
        return _data_result;
    }
    void add_wait_targets(size_t targets_count) {
        _targets_count += targets_count;
    }
    bool is_completed() {
        return response_count() == _targets_count;
    }
    api::timestamp_type last_modified() const {
        return _last_modified;
    }
};

class data_read_resolver : public abstract_read_resolver {
    struct reply {
        gms::inet_address from;
        foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
        bool reached_end = false;
        reply(gms::inet_address from_, foreign_ptr<lw_shared_ptr<reconcilable_result>> result_) : from(std::move(from_)), result(std::move(result_)) {}
    };
    struct version {
        gms::inet_address from;
        std::optional<partition> par;
        bool reached_end;
        bool reached_partition_end;
        version(gms::inet_address from_, std::optional<partition> par_, bool reached_end, bool reached_partition_end)
                : from(std::move(from_)), par(std::move(par_)), reached_end(reached_end), reached_partition_end(reached_partition_end) {}
    };
    struct mutation_and_live_row_count {
        mutation mut;
        uint64_t live_row_count;
    };

    struct primary_key {
        dht::decorated_key partition;
        std::optional<clustering_key> clustering;

        class less_compare_clustering {
            bool _is_reversed;
            clustering_key::less_compare _ck_cmp;
        public:
            less_compare_clustering(const schema& s, bool is_reversed)
                : _is_reversed(is_reversed), _ck_cmp(s) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                if (!b.clustering) {
                    return false;
                }
                if (!a.clustering) {
                    return true;
                }
                if (_is_reversed) {
                    return _ck_cmp(*b.clustering, *a.clustering);
                } else {
                    return _ck_cmp(*a.clustering, *b.clustering);
                }
            }
        };

        class less_compare {
            const schema& _schema;
            less_compare_clustering _ck_cmp;
        public:
            less_compare(const schema& s, bool is_reversed)
                : _schema(s), _ck_cmp(s, is_reversed) { }

            bool operator()(const primary_key& a, const primary_key& b) const {
                auto pk_result = a.partition.tri_compare(_schema, b.partition);
                if (pk_result != 0) {
                    return pk_result < 0;
                }
                return _ck_cmp(a, b);
            }
        };
    };

    uint64_t _total_live_count = 0;
    uint64_t _max_live_count = 0;
    uint32_t _short_read_diff = 0;
    uint64_t _max_per_partition_live_count = 0;
    uint32_t _partition_count = 0;
    uint32_t _live_partition_count = 0;
    bool _increase_per_partition_limit = false;
    bool _all_reached_end = true;
    query::short_read _is_short_read;
    std::vector<reply> _data_results;
    std::unordered_map<dht::token, std::unordered_map<gms::inet_address, std::optional<mutation>>> _diffs;
private:
    void on_timeout() override {
        fail_request(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, response_count(), _targets_count, response_count() != 0));
    }
    void on_failure(exceptions::coordinator_exception_container&& ex) override {
        // we will not need them any more
        _data_results.clear();
    }

    virtual size_t response_count() const override {
        return _data_results.size();
    }

    void register_live_count(const std::vector<version>& replica_versions, uint64_t reconciled_live_rows, uint64_t limit) {
        bool any_not_at_end = boost::algorithm::any_of(replica_versions, [] (const version& v) {
            return !v.reached_partition_end;
        });
        if (any_not_at_end && reconciled_live_rows < limit && limit - reconciled_live_rows > _short_read_diff) {
            _short_read_diff = limit - reconciled_live_rows;
            _max_per_partition_live_count = reconciled_live_rows;
        }
    }
    void find_short_partitions(const std::vector<mutation_and_live_row_count>& rp, const std::vector<std::vector<version>>& versions,
                               uint64_t per_partition_limit, uint64_t row_limit, uint32_t partition_limit) {
        // Go through the partitions that weren't limited by the total row limit
        // and check whether we got enough rows to satisfy per-partition row
        // limit.
        auto partitions_left = partition_limit;
        auto rows_left = row_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | boost::adaptors::reversed) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                register_live_count(*pv, row_count, per_partition_limit);
            } else {
                break;
            }
            ++pv;
        }
    }

    static primary_key get_last_row(const schema& s, const partition& p, bool is_reversed) {
        return {p.mut().decorated_key(s), is_reversed ? p.mut().partition().first_row_key() : p.mut().partition().last_row_key()  };
    }

    // Returns the highest row sent by the specified replica, according to the schema and the direction of
    // the query.
    // versions is a table where rows are partitions in descending order and the columns identify the partition
    // sent by a particular replica.
    static primary_key get_last_row(const schema& s, bool is_reversed, const std::vector<std::vector<version>>& versions, uint32_t replica) {
        const partition* last_partition = nullptr;
        // Versions are in the reversed order.
        for (auto&& pv : versions) {
            const std::optional<partition>& p = pv[replica].par;
            if (p) {
                last_partition = &p.value();
                break;
            }
        }
        SCYLLA_ASSERT(last_partition);
        return get_last_row(s, *last_partition, is_reversed);
    }

    static primary_key get_last_reconciled_row(const schema& s, const mutation_and_live_row_count& m_a_rc, const query::read_command& cmd, uint64_t limit, bool is_reversed) {
        const auto& m = m_a_rc.mut;
        auto mp = mutation_partition(s, m.partition());
        auto&& ranges = cmd.slice.row_ranges(s, m.key());
        bool always_return_static_content = cmd.slice.options.contains<query::partition_slice::option::always_return_static_content>();
        mp.compact_for_query(s, m.decorated_key(), cmd.timestamp, ranges, always_return_static_content, limit);
        return primary_key{m.decorated_key(), get_last_reconciled_row(s, mp, is_reversed)};
    }

    static primary_key get_last_reconciled_row(const schema& s, const mutation_and_live_row_count& m_a_rc, bool is_reversed) {
        const auto& m = m_a_rc.mut;
        return primary_key{m.decorated_key(), get_last_reconciled_row(s, m.partition(), is_reversed)};
    }

    static std::optional<clustering_key> get_last_reconciled_row(const schema& s, const mutation_partition& mp, bool is_reversed) {
        std::optional<clustering_key> ck;
        if (!mp.clustered_rows().empty()) {
            if (is_reversed) {
                ck = mp.clustered_rows().begin()->key();
            } else {
                ck = mp.clustered_rows().rbegin()->key();
            }
        }
        return ck;
    }

    static bool got_incomplete_information_in_partition(const schema& s, const primary_key& last_reconciled_row, const std::vector<version>& versions, bool is_reversed) {
        primary_key::less_compare_clustering ck_cmp(s, is_reversed);
        for (auto&& v : versions) {
            if (!v.par || v.reached_partition_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, *v.par, is_reversed);
            if (ck_cmp(replica_last_row, last_reconciled_row)) {
                return true;
            }
        }
        return false;
    }

    bool got_incomplete_information_across_partitions(const schema& s, const query::read_command& cmd,
                                                      const primary_key& last_reconciled_row, std::vector<mutation_and_live_row_count>& rp,
                                                      const std::vector<std::vector<version>>& versions, bool is_reversed) {
        bool short_reads_allowed = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        bool always_return_static_content = cmd.slice.options.contains<query::partition_slice::option::always_return_static_content>();
        primary_key::less_compare cmp(s, is_reversed);
        std::optional<primary_key> shortest_read;
        auto num_replicas = versions[0].size();
        for (uint32_t i = 0; i < num_replicas; ++i) {
            if (versions.front()[i].reached_end) {
                continue;
            }
            auto replica_last_row = get_last_row(s, is_reversed, versions, i);
            if (cmp(replica_last_row, last_reconciled_row)) {
                if (short_reads_allowed) {
                    if (!shortest_read || cmp(replica_last_row, *shortest_read)) {
                        shortest_read = std::move(replica_last_row);
                    }
                } else {
                    return true;
                }
            }
        }

        // Short reads are allowed, trim the reconciled result.
        if (shortest_read) {
            _is_short_read = query::short_read::yes;

            // Prepare to remove all partitions past shortest_read
            auto it = rp.begin();
            for (; it != rp.end() && shortest_read->partition.less_compare(s, it->mut.decorated_key()); ++it) { }

            // Remove all clustering rows past shortest_read
            if (it != rp.end() && it->mut.decorated_key().equal(s, shortest_read->partition)) {
                if (!shortest_read->clustering) {
                    ++it;
                } else {
                    std::vector<query::clustering_range> ranges;
                    ranges.emplace_back(is_reversed ? query::clustering_range::make_starting_with(std::move(*shortest_read->clustering))
                                                    : query::clustering_range::make_ending_with(std::move(*shortest_read->clustering)));
                    it->live_row_count = it->mut.partition().compact_for_query(s, it->mut.decorated_key(), cmd.timestamp, ranges, always_return_static_content,
                            query::partition_max_rows);
                }
            }

            // Actually remove all partitions past shortest_read
            rp.erase(rp.begin(), it);

            // Update total live count and live partition count
            _live_partition_count = 0;
            _total_live_count = boost::accumulate(rp, uint64_t(0), [this] (uint64_t lc, const mutation_and_live_row_count& m_a_rc) {
                _live_partition_count += !!m_a_rc.live_row_count;
                return lc + m_a_rc.live_row_count;
            });
        }

        return false;
    }

    bool got_incomplete_information(const schema& s, const query::read_command& cmd, uint64_t original_row_limit, uint64_t original_per_partition_limit,
                            uint64_t original_partition_limit, std::vector<mutation_and_live_row_count>& rp, const std::vector<std::vector<version>>& versions) {
        // We need to check whether the reconciled result contains all information from all available
        // replicas. It is possible that some of the nodes have returned less rows (because the limit
        // was set and they had some tombstones missing) than the others. In such cases we cannot just
        // merge all results and return that to the client as the replicas that returned less row
        // may have newer data for the rows they did not send than any other node in the cluster.
        //
        // This function is responsible for detecting whether such problem may happen. We get partition
        // and clustering keys of the last row that is going to be returned to the client and check if
        // it is in range of rows returned by each replicas that returned as many rows as they were
        // asked for (if a replica returned less rows it means it returned everything it has).
        auto is_reversed = cmd.slice.is_reversed();

        auto rows_left = original_row_limit;
        auto partitions_left = original_partition_limit;
        auto pv = versions.rbegin();
        for (auto&& m_a_rc : rp | boost::adaptors::reversed) {
            auto row_count = m_a_rc.live_row_count;
            if (row_count < rows_left && partitions_left > !!row_count) {
                rows_left -= row_count;
                partitions_left -= !!row_count;
                if (original_per_partition_limit < query:: max_rows_if_set) {
                    auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, original_per_partition_limit, is_reversed);
                    if (got_incomplete_information_in_partition(s, last_row, *pv, is_reversed)) {
                        _increase_per_partition_limit = true;
                        return true;
                    }
                }
            } else {
                auto&& last_row = get_last_reconciled_row(s, m_a_rc, cmd, rows_left, is_reversed);
                return got_incomplete_information_across_partitions(s, cmd, last_row, rp, versions, is_reversed);
            }
            ++pv;
        }
        if (rp.empty()) {
            return false;
        }
        auto&& last_row = get_last_reconciled_row(s, *rp.begin(), is_reversed);
        return got_incomplete_information_across_partitions(s, cmd, last_row, rp, versions, is_reversed);
    }
public:
    data_read_resolver(schema_ptr schema, db::consistency_level cl, size_t targets_count, storage_proxy::clock_type::time_point timeout) : abstract_read_resolver(std::move(schema), cl, targets_count, timeout) {
        _data_results.reserve(targets_count);
    }
    void add_mutate_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<reconcilable_result>> result) {
        if (!_request_failed) {
            _max_live_count = std::max(result->row_count(), _max_live_count);
            _data_results.emplace_back(std::move(from), std::move(result));
            if (_data_results.size() == _targets_count) {
                _timeout.cancel();
                _done_promise.set_value(bo::success());
            }
        }
    }
    void on_error(gms::inet_address ep, error_kind kind) override {
        switch (kind) {
        case error_kind::RATE_LIMIT:
            fail_request(exceptions::rate_limit_exception(_schema->ks_name(), _schema->cf_name(), db::operation_type::read, false));
            break;
        case error_kind::DISCONNECT:
        case error_kind::FAILURE:
            fail_request(read_failure_exception(_schema->ks_name(), _schema->cf_name(), _cl, response_count(), 1, _targets_count, response_count() != 0));
            break;
        }
    }
    uint32_t max_live_count() const {
        return _max_live_count;
    }
    bool any_partition_short_read() const {
        return _short_read_diff > 0;
    }
    bool increase_per_partition_limit() const {
        return _increase_per_partition_limit;
    }
    uint32_t max_per_partition_live_count() const {
        return _max_per_partition_live_count;
    }
    uint32_t partition_count() const {
        return _partition_count;
    }
    uint32_t live_partition_count() const {
        return _live_partition_count;
    }
    bool all_reached_end() const {
        return _all_reached_end;
    }
    future<std::optional<reconcilable_result>> resolve(schema_ptr schema, const query::read_command& cmd, uint64_t original_row_limit, uint64_t original_per_partition_limit,
            uint32_t original_partition_limit) {
        SCYLLA_ASSERT(_data_results.size());

        if (_data_results.size() == 1) {
            // if there is a result only from one node there is nothing to reconcile
            // should happen only for range reads since single key reads will not
            // try to reconcile for CL=ONE
            auto& p = _data_results[0].result;
            co_return reconcilable_result(p->row_count(), p->partitions(), p->is_short_read());
        }

        const auto& s = *schema;

        // return true if lh > rh
        auto cmp = [&s](reply& lh, reply& rh) {
            if (lh.result->partitions().size() == 0) {
                return false; // reply with empty partition array goes to the end of the sorted array
            } else if (rh.result->partitions().size() == 0) {
                return true;
            } else {
                auto lhk = lh.result->partitions().back().mut().key();
                auto rhk = rh.result->partitions().back().mut().key();
                return lhk.ring_order_tri_compare(s, rhk) > 0;
            }
        };

        // this array will have an entry for each partition which will hold all available versions
        std::vector<std::vector<version>> versions;
        versions.reserve(_data_results.front().result->partitions().size());

        for (auto& r : _data_results) {
            _is_short_read = _is_short_read || r.result->is_short_read();
            r.reached_end = !r.result->is_short_read() && r.result->row_count() < cmd.get_row_limit()
                            && (cmd.partition_limit == query::max_partitions
                                || boost::range::count_if(r.result->partitions(), [] (const partition& p) {
                                    return p.row_count();
                                }) < cmd.partition_limit);
            _all_reached_end = _all_reached_end && r.reached_end;
        }

        do {
            // after this sort reply with largest key is at the beginning
            boost::sort(_data_results, cmp);
            if (_data_results.front().result->partitions().empty()) {
                break; // if top of the heap is empty all others are empty too
            }
            const auto& max_key = _data_results.front().result->partitions().back().mut().key();
            versions.emplace_back();
            std::vector<version>& v = versions.back();
            v.reserve(_targets_count);
            for (reply& r : _data_results) {
                auto pit = r.result->partitions().rbegin();
                if (pit != r.result->partitions().rend() && pit->mut().key().legacy_equal(s, max_key)) {
                    bool reached_partition_end = pit->row_count() < cmd.slice.partition_row_limit();
                    v.emplace_back(r.from, std::move(*pit), r.reached_end, reached_partition_end);
                    r.result->partitions().pop_back();
                } else {
                    // put empty partition for destination without result
                    v.emplace_back(r.from, std::optional<partition>(), r.reached_end, true);
                }
            }

            boost::sort(v, [] (const version& x, const version& y) {
                return x.from < y.from;
            });
        } while(true);

        std::vector<mutation_and_live_row_count> reconciled_partitions;
        reconciled_partitions.reserve(versions.size());

        // reconcile all versions
        for (std::vector<version>& v : versions) {
            auto it = boost::range::find_if(v, [] (auto&& ver) {
                    return bool(ver.par);
            });
            auto m = mutation(schema, it->par->mut().key());
            for (const version& ver : v) {
                if (ver.par) {
                    mutation_application_stats app_stats;
                    m.partition().apply(*schema, ver.par->mut().partition(), *schema, app_stats);
                    co_await coroutine::maybe_yield();
                }
            }
            auto live_row_count = m.live_row_count();
            _total_live_count += live_row_count;
            _live_partition_count += !!live_row_count;
            reconciled_partitions.emplace_back(mutation_and_live_row_count{ std::move(m), live_row_count });
            co_await coroutine::maybe_yield();
        }
        _partition_count = reconciled_partitions.size();

        bool has_diff = false;

        // calculate differences
        for (auto z : boost::combine(versions, reconciled_partitions)) {
            const mutation& m = z.get<1>().mut;
            for (const version& v : z.get<0>()) {
                auto diff = v.par
                          ? m.partition().difference(*schema, (co_await unfreeze_gently(v.par->mut(), schema)).partition())
                          : mutation_partition(*schema, m.partition());
                std::optional<mutation> mdiff;
                if (!diff.empty()) {
                    has_diff = true;
                    mdiff = mutation(schema, m.decorated_key(), std::move(diff));
                }
                if (auto [it, added] = _diffs[m.token()].try_emplace(v.from, std::move(mdiff)); !added) {
                    // should not really happen, but lets try to deal with it
                    if (mdiff) {
                        if (it->second) {
                            it->second.value().apply(std::move(mdiff.value()));
                        } else {
                            it->second = std::move(mdiff);
                        }
                    }
                }
                co_await coroutine::maybe_yield();
            }
        }

        if (has_diff) {
            if (got_incomplete_information(*schema, cmd, original_row_limit, original_per_partition_limit,
                                           original_partition_limit, reconciled_partitions, versions)) {
                co_return std::nullopt;
            }
            // filter out partitions with empty diffs
            for (auto it = _diffs.begin(); it != _diffs.end();) {
                if (boost::algorithm::none_of(it->second | boost::adaptors::map_values, std::mem_fn(&std::optional<mutation>::operator bool))) {
                    it = _diffs.erase(it);
                } else {
                    ++it;
                }
            }
        } else {
            _diffs.clear();
        }

        find_short_partitions(reconciled_partitions, versions, original_per_partition_limit, original_row_limit, original_partition_limit);

        bool allow_short_reads = cmd.slice.options.contains<query::partition_slice::option::allow_short_read>();
        if (allow_short_reads && _max_live_count >= original_row_limit && _total_live_count < original_row_limit && _total_live_count) {
            // We ended up with less rows than the client asked for (but at least one),
            // avoid retry and mark as short read instead.
            _is_short_read = query::short_read::yes;
        }

        // build reconcilable_result from reconciled data
        // traverse backwards since large keys are at the start
        utils::chunked_vector<partition> vec;
        vec.reserve(_partition_count);
        for (auto it = reconciled_partitions.rbegin(); it != reconciled_partitions.rend(); it++) {
            const mutation_and_live_row_count& m_a_rc = *it;
            vec.emplace_back(partition(m_a_rc.live_row_count, freeze(m_a_rc.mut)));
            co_await coroutine::maybe_yield();
        }

        co_return reconcilable_result(_total_live_count, std::move(vec), _is_short_read);
    }
    auto total_live_count() const {
        return _total_live_count;
    }
    auto get_diffs_for_repair() {
        return std::move(_diffs);
    }
};

class abstract_read_executor : public enable_shared_from_this<abstract_read_executor> {
protected:
    using targets_iterator = inet_address_vector_replica_set::iterator;
    using digest_resolver_ptr = ::shared_ptr<digest_read_resolver>;
    using data_resolver_ptr = ::shared_ptr<data_read_resolver>;
    // Clock type for measuring timeouts.
    using clock_type = storage_proxy::clock_type;
    // Clock type for measuring latencies.
    using latency_clock = utils::latency_counter::clock;

    schema_ptr _schema;
    shared_ptr<storage_proxy> _proxy;
    locator::effective_replication_map_ptr _effective_replication_map_ptr;
    lw_shared_ptr<query::read_command> _cmd;
    lw_shared_ptr<query::read_command> _retry_cmd;
    dht::partition_range _partition_range;
    db::consistency_level _cl;
    size_t _block_for;
    inet_address_vector_replica_set _targets;
    // Targets that were successfully used for a data or digest request
    inet_address_vector_replica_set _used_targets;
    promise<result<foreign_ptr<lw_shared_ptr<query::result>>>> _result_promise;
    tracing::trace_state_ptr _trace_state;
    lw_shared_ptr<replica::column_family> _cf;
    bool _foreground = true;
    service_permit _permit; // holds admission permit until operation completes
    db::per_partition_rate_limit::info _rate_limit_info;

private:
    const bool _native_reversed_queries_enabled;

    void on_read_resolved() noexcept {
        // We could have !_foreground if this is called on behalf of background reconciliation.
        _proxy->get_stats().foreground_reads -= int(_foreground);
        _foreground = false;
    }

    const locator::topology& get_topology() const noexcept {
        return _effective_replication_map_ptr->get_topology();
    }

public:
    abstract_read_executor(schema_ptr s, lw_shared_ptr<replica::column_family> cf, shared_ptr<storage_proxy> proxy,
            locator::effective_replication_map_ptr ermp,
            lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, size_t block_for,
            inet_address_vector_replica_set targets, tracing::trace_state_ptr trace_state, service_permit permit, db::per_partition_rate_limit::info rate_limit_info) :
                           _schema(std::move(s)), _proxy(std::move(proxy))
                         , _effective_replication_map_ptr(std::move(ermp))
                         , _cmd(std::move(cmd)), _partition_range(std::move(pr)), _cl(cl), _block_for(block_for), _targets(std::move(targets)), _trace_state(std::move(trace_state)),
                           _cf(std::move(cf)), _permit(std::move(permit)), _rate_limit_info(rate_limit_info),
                           _native_reversed_queries_enabled(_proxy->features().native_reverse_queries) {
        _proxy->get_stats().reads++;
        _proxy->get_stats().foreground_reads++;
    }
    virtual ~abstract_read_executor() {
        _proxy->get_stats().reads--;
        _proxy->get_stats().foreground_reads -= int(_foreground);
    }

    /// Targets that were successfully ised for data and/or digest requests.
    ///
    /// Only filled after the request is finished, call only after
    /// execute()'s future is ready.
    inet_address_vector_replica_set used_targets() const {
        return _used_targets;
    }

protected:
    future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> make_mutation_data_request(lw_shared_ptr<query::read_command> cmd, gms::inet_address ep, clock_type::time_point timeout) {
        ++_proxy->get_stats().mutation_data_read_attempts.get_ep_stat(get_topology(), ep);
        auto fence = storage_proxy::get_fence(*_effective_replication_map_ptr);
        if (_proxy->is_me(ep)) {
            tracing::trace(_trace_state, "read_mutation_data: querying locally");
            return _proxy->apply_fence(_proxy->query_mutations_locally(_schema, cmd, _partition_range, timeout, _trace_state), fence, _proxy->my_address());
        } else {
            const bool format_reverse_required = cmd->slice.is_reversed() && !_native_reversed_queries_enabled;
            cmd = format_reverse_required ? reversed(::make_lw_shared(*cmd)) : cmd;

            auto f = _proxy->remote().send_read_mutation_data(netw::messaging_service::msg_addr{ep, 0}, timeout,
                _trace_state, *cmd, _partition_range, fence);
            if (format_reverse_required) {
                f = f.then([](auto r) {
                    auto&& [result, hit_rate] = r;
                    return reversed(std::move(result)).then([hit_rate=std::move(hit_rate)](auto result) mutable {
                        return rpc::tuple{std::move(result), std::move(hit_rate)};
                    });
                });
            }
            return f;
        }
    }
    future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> make_data_request(gms::inet_address ep, clock_type::time_point timeout, bool want_digest) {
        ++_proxy->get_stats().data_read_attempts.get_ep_stat(get_topology(), ep);
        auto opts = want_digest
                  ? query::result_options{query::result_request::result_and_digest, digest_algorithm(*_proxy)}
                  : query::result_options{query::result_request::only_result, query::digest_algorithm::none};
        auto fence = storage_proxy::get_fence(*_effective_replication_map_ptr);
        if (_proxy->is_me(ep)) {
            tracing::trace(_trace_state, "read_data: querying locally");
            return _proxy->apply_fence(_proxy->query_result_local(_effective_replication_map_ptr, _schema, _cmd, _partition_range, opts, _trace_state, timeout, adjust_rate_limit_for_local_operation(_rate_limit_info)), fence, _proxy->my_address());
        } else {
            const bool format_reverse_required = _cmd->slice.is_reversed() && !_native_reversed_queries_enabled;
            auto cmd = format_reverse_required ? reversed(::make_lw_shared(*_cmd)) : _cmd;
            return _proxy->remote().send_read_data(netw::messaging_service::msg_addr{ep, 0}, timeout,
                _trace_state, *cmd, _partition_range, opts.digest_algo, _rate_limit_info,
                fence);
        }
    }
    future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature, std::optional<full_position>>> make_digest_request(gms::inet_address ep, clock_type::time_point timeout) {
        ++_proxy->get_stats().digest_read_attempts.get_ep_stat(get_topology(), ep);
        auto fence = storage_proxy::get_fence(*_effective_replication_map_ptr);
        if (_proxy->is_me(ep)) {
            tracing::trace(_trace_state, "read_digest: querying locally");
            return _proxy->apply_fence(_proxy->query_result_local_digest(_effective_replication_map_ptr, _schema, _cmd, _partition_range, _trace_state,
                        timeout, digest_algorithm(*_proxy), adjust_rate_limit_for_local_operation(_rate_limit_info)), fence, _proxy->my_address());
        } else {
            tracing::trace(_trace_state, "read_digest: sending a message to /{}", ep);
            const bool format_reverse_required = _cmd->slice.is_reversed() && !_native_reversed_queries_enabled;
            auto cmd = format_reverse_required ? reversed(::make_lw_shared(*_cmd)) : _cmd;
            return _proxy->remote().send_read_digest(netw::messaging_service::msg_addr{ep, 0}, timeout,
                _trace_state, *cmd, _partition_range, digest_algorithm(*_proxy), _rate_limit_info,
                fence);
        }
    }
    void make_mutation_data_requests(lw_shared_ptr<query::read_command> cmd, data_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout) {
        auto start = latency_clock::now();
        for (const gms::inet_address& ep : boost::make_iterator_range(begin, end)) {
            // Waited on indirectly, shared_from_this keeps `this` alive
            (void)make_mutation_data_request(cmd, ep, timeout).then_wrapped([this, resolver, ep, start, exec = shared_from_this()] (future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> f) {
                std::exception_ptr ex;
                try {
                  if (!f.failed()) {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<1>(v));
                    resolver->add_mutate_data(ep, std::get<0>(std::move(v)));
                    ++_proxy->get_stats().mutation_data_read_completed.get_ep_stat(get_topology(), ep);
                    register_request_latency(latency_clock::now() - start);
                    return;
                  } else {
                    ex = f.get_exception();
                  }
                } catch (...) {
                  ex = std::current_exception();
                }

                ++_proxy->get_stats().mutation_data_read_errors.get_ep_stat(get_topology(), ep);
                resolver->error(ep, std::move(ex));
            });
        }
    }
    void make_data_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout, bool want_digest) {
        auto start = latency_clock::now();
        for (const gms::inet_address& ep : boost::make_iterator_range(begin, end)) {
            // Waited on indirectly, shared_from_this keeps `this` alive
            (void)make_data_request(ep, timeout, want_digest).then_wrapped([this, resolver, ep, start, exec = shared_from_this()] (future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> f) {
                std::exception_ptr ex;
                try {
                  if (!f.failed()) {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<1>(v));
                    resolver->add_data(ep, std::get<0>(std::move(v)));
                    ++_proxy->get_stats().data_read_completed.get_ep_stat(get_topology(), ep);
                    _used_targets.push_back(ep);
                    register_request_latency(latency_clock::now() - start);
                    return;
                  } else {
                    ex = f.get_exception();
                  }
                } catch (...) {
                  ex = std::current_exception();
                }

                ++_proxy->get_stats().data_read_errors.get_ep_stat(get_topology(), ep);
                resolver->error(ep, std::move(ex));
            });
        }
    }
    void make_digest_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end, clock_type::time_point timeout) {
        auto start = latency_clock::now();
        for (const gms::inet_address& ep : boost::make_iterator_range(begin, end)) {
            // Waited on indirectly, shared_from_this keeps `this` alive
            (void)make_digest_request(ep, timeout).then_wrapped([this, resolver, ep, start, exec = shared_from_this()] (future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature, std::optional<full_position>>> f) {
                std::exception_ptr ex;
                try {
                  if (!f.failed()) {
                    auto v = f.get();
                    _cf->set_hit_rate(ep, std::get<2>(v));
                    resolver->add_digest(ep, std::get<0>(v), std::get<1>(v), std::get<3>(std::move(v)));
                    ++_proxy->get_stats().digest_read_completed.get_ep_stat(get_topology(), ep);
                    _used_targets.push_back(ep);
                    register_request_latency(latency_clock::now() - start);
                    return;
                  } else {
                    ex = f.get_exception();
                  }
                } catch (...) {
                  ex = std::current_exception();
                }

                ++_proxy->get_stats().digest_read_errors.get_ep_stat(get_topology(), ep);
                resolver->error(ep, std::move(ex));
            });
        }
    }
    virtual void make_requests(digest_resolver_ptr resolver, clock_type::time_point timeout) {
        resolver->add_wait_targets(_targets.size());
        auto want_digest = _targets.size() > 1;
        make_data_requests(resolver, _targets.begin(), _targets.begin() + 1, timeout, want_digest);
        make_digest_requests(resolver, _targets.begin() + 1, _targets.end(), timeout);
    }
    virtual void got_cl() {}
    uint64_t original_row_limit() const {
        return _cmd->get_row_limit();
    }
    uint64_t original_per_partition_row_limit() const {
        return _cmd->slice.partition_row_limit();
    }
    uint32_t original_partition_limit() const {
        return _cmd->partition_limit;
    }
    virtual void adjust_targets_for_reconciliation() {}
    void reconcile(db::consistency_level cl, storage_proxy::clock_type::time_point timeout, lw_shared_ptr<query::read_command> cmd) {
        adjust_targets_for_reconciliation();
        data_resolver_ptr data_resolver = ::make_shared<data_read_resolver>(_schema, cl, _targets.size(), timeout);
        auto exec = shared_from_this();

        if (_proxy->features().empty_replica_mutation_pages) {
            cmd->slice.options.set<query::partition_slice::option::allow_mutation_read_page_without_live_row>();
        }

        // Waited on indirectly.
        make_mutation_data_requests(cmd, data_resolver, _targets.begin(), _targets.end(), timeout);

        // Waited on indirectly.
        (void)data_resolver->done().then_wrapped([this, exec_ = std::move(exec), data_resolver_ = std::move(data_resolver), cmd_ = std::move(cmd), cl_ = cl, timeout_ = timeout] (future<result<>> f) mutable -> future<> {
            // move captures to coroutine stack frame
            // to prevent use after free
            auto exec = std::move(exec_);
            auto data_resolver = std::move(data_resolver_);
            auto cmd = std::move(cmd_);
            auto cl = cl_;
            auto timeout = timeout_;
            try {
                result<> res = f.get();
                if (!res) {
                    _result_promise.set_value(std::move(res).as_failure());
                    on_read_resolved();
                    co_return;
                }
                auto rr_opt = co_await data_resolver->resolve(_schema, *cmd, original_row_limit(), original_per_partition_row_limit(), original_partition_limit()); // reconciliation happens here

                // We generate a retry if at least one node reply with count live columns but after merge we have less
                // than the total number of column we are interested in (which may be < count on a retry).
                // So in particular, if no host returned count live columns, we know it's not a short read due to
                // row or partition limits being exhausted and retry is not needed.
                if (rr_opt && (rr_opt->is_short_read()
                               || data_resolver->all_reached_end()
                               || rr_opt->row_count() >= original_row_limit()
                               || data_resolver->live_partition_count() >= original_partition_limit())
                        && !data_resolver->any_partition_short_read()) {
                    tracing::trace(_trace_state, "Read stage is done for read-repair");
                    mlogger.trace("reconciled: {}", rr_opt->pretty_printer(_schema));

                    auto result = ::make_foreign(::make_lw_shared<query::result>(
                            co_await to_data_query_result(std::move(*rr_opt), _schema, _cmd->slice, _cmd->get_row_limit(), _cmd->partition_limit)));
                    qlogger.trace("reconciled: {}", result->pretty_printer(_schema, _cmd->slice));

                    // Un-reverse mutations for reversed queries. When a mutation comes from a node in mixed-node cluster
                    // it is reversed in make_mutation_data_request(). So we always deal here with reversed mutations for
                    // reversed queries. No matter what format. Forward mutations are sent to spare replicas from reversing
                    // them in the write-path.
                    auto diffs = data_resolver->get_diffs_for_repair();
                    if (_cmd->slice.is_reversed()) {
                        for (auto&& [token, diff] : diffs) {
                            for (auto&& [address, opt_mut] : diff) {
                                if (opt_mut) {
                                    opt_mut = reverse(std::move(opt_mut.value()));
                                    co_await coroutine::maybe_yield();
                                }
                            }
                        }
                    }

                    // wait for write to complete before returning result to prevent multiple concurrent read requests to
                    // trigger repair multiple times and to prevent quorum read to return an old value, even after a quorum
                    // another read had returned a newer value (but the newer value had not yet been sent to the other replicas)
                    // Waited on indirectly.
                    (void)_proxy->schedule_repair(_effective_replication_map_ptr, std::move(diffs), _cl, _trace_state, _permit).then(utils::result_wrap([this, result = std::move(result)] () mutable {
                        _result_promise.set_value(std::move(result));
                        return make_ready_future<::result<>>(bo::success());
                    })).then_wrapped([this, exec] (future<::result<>>&& f) {
                        // All errors are handled, it's OK to discard the result.
                        (void)utils::result_try([&] {
                            return f.get();
                        },  utils::result_catch<mutation_write_timeout_exception>([&] (const auto&) -> ::result<> {
                            // convert write error to read error
                            _result_promise.set_value(read_timeout_exception(_schema->ks_name(), _schema->cf_name(), _cl, _block_for - 1, _block_for, true));
                            return bo::success();
                        }), utils::result_catch_dots([&] (auto&& handle) -> ::result<> {
                            handle.forward_to_promise(_result_promise);
                            return bo::success();
                        }));
                        on_read_resolved();
                    });
                } else {
                    tracing::trace(_trace_state, "Not enough data, need a retry for read-repair");
                    _proxy->get_stats().read_retries++;
                    _retry_cmd = make_lw_shared<query::read_command>(*cmd);
                    // We asked t (= cmd->get_row_limit()) live columns and got l (=data_resolver->total_live_count) ones.
                    // From that, we can estimate that on this row, for x requested
                    // columns, only l/t end up live after reconciliation. So for next
                    // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
                    auto x = [](uint64_t t, uint64_t l) -> uint64_t {
                        using uint128_t = unsigned __int128;
                        auto ret = std::min<uint128_t>(query::max_rows, l == 0 ? t + 1 : (uint128_t) t * t / l + 1);
                        return static_cast<uint64_t>(ret);
                    };
                    auto all_partitions_x = [](uint64_t x, uint32_t partitions) -> uint64_t {
                        using uint128_t = unsigned __int128;
                        auto ret = std::min<uint128_t>(query::max_rows, (uint128_t) x * partitions);
                        return static_cast<uint64_t>(ret);
                    };
                    if (data_resolver->any_partition_short_read() || data_resolver->increase_per_partition_limit()) {
                        // The number of live rows was bounded by the per partition limit.
                        auto new_partition_limit = x(cmd->slice.partition_row_limit(), data_resolver->max_per_partition_live_count());
                        _retry_cmd->slice.set_partition_row_limit(new_partition_limit);
                        auto new_limit = all_partitions_x(new_partition_limit, data_resolver->partition_count());
                        _retry_cmd->set_row_limit(std::max(cmd->get_row_limit(), new_limit));
                    } else {
                        // The number of live rows was bounded by the total row limit or partition limit.
                        if (cmd->partition_limit != query::max_partitions) {
                            _retry_cmd->partition_limit = std::min<uint64_t>(query::max_partitions, x(cmd->partition_limit, data_resolver->live_partition_count()));
                        }
                        if (cmd->get_row_limit() != query::max_rows) {
                            _retry_cmd->set_row_limit(x(cmd->get_row_limit(), data_resolver->total_live_count()));
                        }
                    }

                    // We may be unable to send a single live row because of replicas bailing out too early.
                    // If that is the case disallow short reads so that we can make progress.
                    if (!data_resolver->total_live_count()) {
                        _retry_cmd->slice.options.remove<query::partition_slice::option::allow_short_read>();
                    }

                    slogger.trace("Retrying query with command {} (previous is {})", *_retry_cmd, *cmd);
                    reconcile(cl, timeout, _retry_cmd);
                }

            } catch (...) {
                _result_promise.set_exception(std::current_exception());
                on_read_resolved();
            }
        });
    }
    void reconcile(db::consistency_level cl, storage_proxy::clock_type::time_point timeout) {
        reconcile(cl, timeout, _cmd);
    }

public:
    future<result<foreign_ptr<lw_shared_ptr<query::result>>>> execute(storage_proxy::clock_type::time_point timeout) {
        if (_targets.empty()) {
            // We may have no targets to read from if a DC with zero replication is queried with LOCACL_QUORUM.
            // Return an empty result in this case
            return make_ready_future<result<foreign_ptr<lw_shared_ptr<query::result>>>>(make_foreign(make_lw_shared(query::result())));
        }
        digest_resolver_ptr digest_resolver = ::make_shared<digest_read_resolver>(_proxy, _effective_replication_map_ptr, _schema, _cl, _block_for,
                db::is_datacenter_local(_cl) ? _effective_replication_map_ptr->get_topology().count_local_endpoints(_targets): _targets.size(), timeout);
        auto exec = shared_from_this();

        make_requests(digest_resolver, timeout);

        // Waited on indirectly.
        (void)digest_resolver->has_cl().then_wrapped([exec, digest_resolver, timeout] (future<result<digest_read_result>> f) mutable {
            bool background_repair_check = false;
            // All errors are handled, it's OK to discard the result.
            (void)utils::result_try([&] () -> result<> {
                exec->got_cl();

                auto&& res = f.get(); // can throw
                if (!res) {
                    return std::move(res).as_failure();
                }
                auto&& [result, digests_match] = res.value();

                if (digests_match) {
                    if (exec->_proxy->features().empty_replica_pages && digest_resolver->response_count() > 1) {
                        auto& mp = digest_resolver->min_position();
                        auto& lp = result->last_position();
                        if (!mp || bool(lp) < bool(mp) || full_position::cmp(*exec->_schema, *mp, *lp) < 0) {
                            result->set_last_position(mp);
                        }
                    }
                    exec->_result_promise.set_value(std::move(result));
                    if (exec->_block_for < exec->_targets.size()) { // if there are more targets then needed for cl, check digest in background
                        background_repair_check = true;
                    }
                    exec->on_read_resolved();
                } else { // digest mismatch
                    // Do not optimize cross-dc repair if read_timestamp is missing (or just negative)
                    // We're interested in reads that happen within write_timeout of a write,
                    // and comparing a timestamp that is too far causes int overflow (#5556)
                    if (is_datacenter_local(exec->_cl) && exec->_cmd->read_timestamp >= api::timestamp_type(0)) {
                        auto write_timeout = exec->_proxy->_db.local().get_config().write_request_timeout_in_ms() * 1000;
                        auto delta = int64_t(digest_resolver->last_modified()) - int64_t(exec->_cmd->read_timestamp);
                        if (std::abs(delta) <= write_timeout) {
                            exec->_proxy->get_stats().global_read_repairs_canceled_due_to_concurrent_write++;
                            // if CL is local and non matching data is modified less than write_timeout ms ago do only local repair
                            auto local_dc_filter = exec->_effective_replication_map_ptr->get_topology().get_local_dc_filter();
                            auto i = boost::range::remove_if(exec->_targets, std::not_fn(std::cref(local_dc_filter)));
                            exec->_targets.erase(i, exec->_targets.end());
                        }
                    }
                    tracing::trace(exec->_trace_state, "digest mismatch, starting read repair");
                    exec->reconcile(exec->_cl, timeout);
                    exec->_proxy->get_stats().read_repair_repaired_blocking++;
                }
                return bo::success();
            },  utils::result_catch_dots([&] (auto&& handle) {
                handle.forward_to_promise(exec->_result_promise);
                exec->on_read_resolved();
                return bo::success();
            }));

            // Waited on indirectly.
            (void)digest_resolver->done().then(utils::result_wrap([exec, digest_resolver, timeout, background_repair_check] () mutable {
                if (background_repair_check && !digest_resolver->digests_match()) {
                    exec->_proxy->get_stats().read_repair_repaired_background++;
                    exec->_result_promise = promise<result<foreign_ptr<lw_shared_ptr<query::result>>>>();
                    exec->reconcile(exec->_cl, timeout);
                    return exec->_result_promise.get_future().then(utils::result_discard_value<result<foreign_ptr<lw_shared_ptr<query::result>>>>);
                } else {
                    return make_ready_future<result<>>(bo::success());
                }
            })).then_wrapped([] (auto&& f) {
                // ignore any failures during background repair (both failed results and exceptions)
                f.ignore_ready_future();
            });
        });

        return _result_promise.get_future();
    }

    lw_shared_ptr<replica::column_family>& get_cf() {
        return _cf;
    }

    // Maximum latency of a successful request made to a replica (over all requests that finished up to this point).
    // Example usage: gathering latency statistics for deciding on invoking speculative retries.
    std::optional<latency_clock::duration> max_request_latency() const {
        if (_max_request_latency == NO_LATENCY) {
            return std::nullopt;
        }
        return _max_request_latency;
    }

private:
    void register_request_latency(latency_clock::duration d) {
        _max_request_latency = std::max(_max_request_latency, d);
    }

    static constexpr latency_clock::duration NO_LATENCY{-1};
    latency_clock::duration _max_request_latency{NO_LATENCY};
};

class never_speculating_read_executor : public abstract_read_executor {
public:
    never_speculating_read_executor(schema_ptr s, lw_shared_ptr<replica::column_family> cf, shared_ptr<storage_proxy> proxy,
            locator::effective_replication_map_ptr ermp,
            lw_shared_ptr<query::read_command> cmd, dht::partition_range pr, db::consistency_level cl, inet_address_vector_replica_set targets, tracing::trace_state_ptr trace_state, service_permit permit,
            db::per_partition_rate_limit::info rate_limit_info) :
                                        abstract_read_executor(std::move(s), std::move(cf), std::move(proxy), std::move(ermp), std::move(cmd), std::move(pr), cl, 0, std::move(targets), std::move(trace_state), std::move(permit), rate_limit_info) {
        _block_for = _targets.size();
    }
};

// this executor always asks for one additional data reply
class always_speculating_read_executor : public abstract_read_executor {
public:
    using abstract_read_executor::abstract_read_executor;
    virtual void make_requests(digest_resolver_ptr resolver, storage_proxy::clock_type::time_point timeout) {
        resolver->add_wait_targets(_targets.size());
        // FIXME: consider disabling for CL=*ONE
        bool want_digest = true;
        make_data_requests(resolver, _targets.begin(), _targets.begin() + 2, timeout, want_digest);
        make_digest_requests(resolver, _targets.begin() + 2, _targets.end(), timeout);
    }
};

// this executor sends request to an additional replica after some time below timeout
class speculating_read_executor : public abstract_read_executor {
    timer<storage_proxy::clock_type> _speculate_timer;
public:
    using abstract_read_executor::abstract_read_executor;
    virtual void make_requests(digest_resolver_ptr resolver, storage_proxy::clock_type::time_point timeout) override {
        _speculate_timer.set_callback([this, resolver, timeout] {
            if (!resolver->is_completed()) { // at the time the callback runs request may be completed already
                resolver->add_wait_targets(1); // we send one more request so wait for it too
                // FIXME: consider disabling for CL=*ONE
                auto send_request = [&] (bool has_data) {
                    if (has_data) {
                        _proxy->get_stats().speculative_digest_reads++;
                        tracing::trace(_trace_state, "Launching speculative retry for digest");
                        make_digest_requests(resolver, _targets.end() - 1, _targets.end(), timeout);
                    } else {
                        _proxy->get_stats().speculative_data_reads++;
                        tracing::trace(_trace_state, "Launching speculative retry for data");
                        make_data_requests(resolver, _targets.end() - 1, _targets.end(), timeout, true);
                    }
                };
                send_request(resolver->has_data());
            }
        });
        auto& sr = _schema->speculative_retry();
        auto t = (sr.get_type() == speculative_retry::type::PERCENTILE) ?
            std::min(_cf->get_coordinator_read_latency_percentile(sr.get_value()), std::chrono::milliseconds(_proxy->get_db().local().get_config().read_request_timeout_in_ms()/2)) :
            std::chrono::milliseconds(unsigned(sr.get_value()));
        _speculate_timer.arm(t);

        // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
        // that the last replica in our list is "extra."
        resolver->add_wait_targets(_targets.size() - 1);
        // FIXME: consider disabling for CL=*ONE
        bool want_digest = true;
        if (_block_for < _targets.size() - 1) {
            // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
            // preferred by the snitch, we do an extra data read to start with against a replica more
            // likely to reply; better to let RR fail than the entire query.
            make_data_requests(resolver, _targets.begin(), _targets.begin() + 2, timeout, want_digest);
            make_digest_requests(resolver, _targets.begin() + 2, _targets.end(), timeout);
        } else {
            // not doing read repair; all replies are important, so it doesn't matter which nodes we
            // perform data reads against vs digest.
            make_data_requests(resolver, _targets.begin(), _targets.begin() + 1, timeout, want_digest);
            make_digest_requests(resolver, _targets.begin() + 1, _targets.end() - 1, timeout);
        }
    }
    virtual void got_cl() override {
        _speculate_timer.cancel();
    }
    virtual void adjust_targets_for_reconciliation() override {
        _targets = used_targets();
    }
};

result<::shared_ptr<abstract_read_executor>> storage_proxy::get_read_executor(lw_shared_ptr<query::read_command> cmd,
        locator::effective_replication_map_ptr erm,
        schema_ptr schema,
        dht::partition_range pr,
        db::consistency_level cl,
        db::read_repair_decision repair_decision,
        tracing::trace_state_ptr trace_state,
        const inet_address_vector_replica_set& preferred_endpoints,
        bool& is_read_non_local,
        service_permit permit) {
    const dht::token& token = pr.start()->value().token();
    speculative_retry::type retry_type = schema->speculative_retry().get_type();
    std::optional<gms::inet_address> extra_replica;

    inet_address_vector_replica_set all_replicas = get_endpoints_for_reading(schema->ks_name(), *erm, token);
    // Check for a non-local read before heat-weighted load balancing
    // reordering of endpoints happens. The local endpoint, if
    // present, is always first in the list, as get_endpoints_for_reading()
    // orders the list by proximity to the local endpoint.
    is_read_non_local |= !all_replicas.empty() && all_replicas.front() != erm->get_topology().my_address();

    auto cf = _db.local().find_column_family(schema).shared_from_this();
    inet_address_vector_replica_set target_replicas = filter_replicas_for_read(cl, *erm, all_replicas, preferred_endpoints, repair_decision,
            retry_type == speculative_retry::type::NONE ? nullptr : &extra_replica,
            _db.local().get_config().cache_hit_rate_read_balancing() ? &*cf : nullptr);

    slogger.trace("creating read executor for token {} with all: {} targets: {} rp decision: {}", token, all_replicas, target_replicas, repair_decision);
    tracing::trace(trace_state, "Creating read executor for token {} with all: {} targets: {} repair decision: {}", token, all_replicas, target_replicas, repair_decision);

    // Throw UAE early if we don't have enough replicas.
    try {
        db::assure_sufficient_live_nodes(cl, *erm, target_replicas);
    } catch (exceptions::unavailable_exception& ex) {
        slogger.debug("Read unavailable: cl={} required {} alive {}", ex.consistency, ex.required, ex.alive);
        get_stats().read_unavailables.mark();
        throw;
    }

    if (repair_decision != db::read_repair_decision::NONE) {
        get_stats().read_repair_attempts++;
    }

    size_t block_for = db::block_for(*erm, cl);
    auto p = shared_from_this();

    db::per_partition_rate_limit::info rate_limit_info;
    if (cmd->allow_limit && _db.local().can_apply_per_partition_rate_limit(*schema, db::operation_type::read)) {
        auto r_rate_limit_info = choose_rate_limit_info(erm, _db.local(), !is_read_non_local, db::operation_type::read, schema, token, trace_state);
        if (!r_rate_limit_info) {
            slogger.debug("Read was rate limited");
            get_stats().read_rate_limited_by_coordinator.mark();
            return std::move(r_rate_limit_info).as_failure();
        }
        rate_limit_info = r_rate_limit_info.value();
    } else {
        slogger.trace("Operation is not rate limited");
    }

    // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
    if (retry_type == speculative_retry::type::NONE || block_for == all_replicas.size()
            || (repair_decision == db::read_repair_decision::DC_LOCAL && is_datacenter_local(cl) && block_for == target_replicas.size())) {
        tracing::trace(trace_state, "Creating never_speculating_read_executor - speculative retry is disabled or there are no extra replicas to speculate with");
        return ::make_shared<never_speculating_read_executor>(schema, cf, p, std::move(erm), cmd, std::move(pr), cl, std::move(target_replicas), std::move(trace_state), std::move(permit), rate_limit_info);
    }

    if (target_replicas.size() == all_replicas.size()) {
        // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
        // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
        // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
        tracing::trace(trace_state, "always_speculating_read_executor (all targets)");
        return ::make_shared<always_speculating_read_executor>(schema, cf, p, std::move(erm), cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state), std::move(permit), rate_limit_info);
    }

    // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
    if (target_replicas.size() == block_for) { // If RRD.DC_LOCAL extra replica may already be present
        auto local_dc_filter = erm->get_topology().get_local_dc_filter();
        if (!extra_replica || (is_datacenter_local(cl) && !local_dc_filter(*extra_replica))) {
            slogger.trace("read executor no extra target to speculate");
            tracing::trace(trace_state, "Creating never_speculating_read_executor - there are no extra replicas to speculate with");
            return ::make_shared<never_speculating_read_executor>(schema, cf, p, std::move(erm), cmd, std::move(pr), cl, std::move(target_replicas), std::move(trace_state), std::move(permit), rate_limit_info);
        } else {
            target_replicas.push_back(*extra_replica);
            slogger.trace("creating read executor with extra target {}", *extra_replica);
            tracing::trace(trace_state, "Added extra target {} for speculative read", *extra_replica);
        }
    }

    if (retry_type == speculative_retry::type::ALWAYS) {
        tracing::trace(trace_state, "Creating always_speculating_read_executor");
        return ::make_shared<always_speculating_read_executor>(schema, cf, p, std::move(erm), cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state), std::move(permit), rate_limit_info);
    } else {// PERCENTILE or CUSTOM.
        tracing::trace(trace_state, "Creating speculating_read_executor");
        return ::make_shared<speculating_read_executor>(schema, cf, p, std::move(erm), cmd, std::move(pr), cl, block_for, std::move(target_replicas), std::move(trace_state), std::move(permit), rate_limit_info);
    }
}

future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature, std::optional<full_position>>>
storage_proxy::query_result_local_digest(locator::effective_replication_map_ptr erm, schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, tracing::trace_state_ptr trace_state, storage_proxy::clock_type::time_point timeout, query::digest_algorithm da, db::per_partition_rate_limit::info rate_limit_info) {
    return query_result_local(std::move(erm), std::move(query_schema), std::move(cmd), pr, query::result_options::only_digest(da), std::move(trace_state), timeout, rate_limit_info).then([] (rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> result_and_hit_rate) {
        auto&& [result, hit_rate] = result_and_hit_rate;
        return make_ready_future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature, std::optional<full_position>>>(rpc::tuple(*result->digest(), result->last_modified(), hit_rate, result->last_position()));
    });
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>>
storage_proxy::query_result_local(locator::effective_replication_map_ptr erm, schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr, query::result_options opts,
                                  tracing::trace_state_ptr trace_state, storage_proxy::clock_type::time_point timeout, db::per_partition_rate_limit::info rate_limit_info) {
    cmd->slice.options.set_if<query::partition_slice::option::with_digest>(opts.request != query::result_request::only_result);
    if (auto shard_opt = dht::is_single_shard(erm->get_sharder(*query_schema), *query_schema, pr)) {
        auto shard = *shard_opt;
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return _db.invoke_on(shard, _read_smp_service_group, [gs = global_schema_ptr(query_schema), prv = dht::partition_range_vector({pr}) /* FIXME: pr is copied */, cmd, opts, timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state)), rate_limit_info] (replica::database& db) mutable {
            auto trace_state = gt.get();
            tracing::trace(trace_state, "Start querying singular range {}", prv.front());
            return db.query(gs, *cmd, opts, prv, trace_state, timeout, rate_limit_info).then([trace_state](std::tuple<lw_shared_ptr<query::result>, cache_temperature>&& f_ht) {
                auto&& [f, ht] = f_ht;
                tracing::trace(trace_state, "Querying is done");
                return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>>(rpc::tuple(make_foreign(std::move(f)), ht));
            });
        });
    } else {
        // FIXME: adjust multishard_mutation_query to accept an smp_service_group and propagate it there
        tracing::trace(trace_state, "Start querying token range {}", pr);
        return query_nonsingular_data_locally(query_schema, cmd, {pr}, opts, trace_state, timeout).then(
                [trace_state = std::move(trace_state)] (rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>&& r_ht) {
            auto&& [r, ht] = r_ht;
            tracing::trace(trace_state, "Querying is done");
            return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>>(rpc::tuple(std::move(r), ht));
        });
    }
}

void storage_proxy::handle_read_error(std::variant<exceptions::coordinator_exception_container, std::exception_ptr> failure, bool range) {
    // All errors are handled, it's OK to discard the result.
    (void)utils::result_try([&] () -> result<> {
        if (auto* excont = std::get_if<0>(&failure)) {
            return bo::failure(std::move(*excont));
        } else {
            std::rethrow_exception(std::get<1>(std::move(failure)));
        }
    },  utils::result_catch<read_timeout_exception>([&] (const auto& ex) {
        slogger.debug("Read timeout: received {} of {} required replies, data {}present", ex.received, ex.block_for, ex.data_present ? "" : "not ");
        if (range) {
            get_stats().range_slice_timeouts.mark();
        } else {
            get_stats().read_timeouts.mark();
        }
        return bo::success();
    }), utils::result_catch<exceptions::rate_limit_exception>([&] (const auto& ex) {
        slogger.debug("Read was rate limited");
        if (ex.rejected_by_coordinator) {
            get_stats().read_rate_limited_by_coordinator.mark();
        } else {
            get_stats().read_rate_limited_by_replicas.mark();
        }
        return bo::success();
    }), utils::result_catch_dots([&] (auto&& handle) {
        slogger.debug("Error during read query {}", handle.as_inner());
        return bo::success();
    }));
}

future<result<storage_proxy::coordinator_query_result>>
storage_proxy::query_singular(lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        storage_proxy::coordinator_query_options query_options) {
    utils::small_vector<std::pair<::shared_ptr<abstract_read_executor>, dht::token_range>, 1> exec;
    exec.reserve(partition_ranges.size());

    schema_ptr schema = local_schema_registry().get(cmd->schema_version);

    replica::table& table = _db.local().find_column_family(schema->id());
    auto erm = table.get_effective_replication_map();

    db::read_repair_decision repair_decision = query_options.read_repair_decision
        ? *query_options.read_repair_decision : db::read_repair_decision::NONE;

    // Update reads_coordinator_outside_replica_set once per request,
    // not once per partition.
    bool is_read_non_local = false;

    const auto& tm = erm->get_token_metadata();
    for (auto&& pr: partition_ranges) {
        if (!pr.is_singular()) {
            co_await coroutine::return_exception(std::runtime_error("mixed singular and non singular range are not supported"));
        }

        auto token_range = dht::token_range::make_singular(pr.start()->value().token());
        auto it = query_options.preferred_replicas.find(token_range);
        const auto replicas = it == query_options.preferred_replicas.end()
            ? inet_address_vector_replica_set{} : replica_ids_to_endpoints(tm, it->second);

        auto r_read_executor = get_read_executor(cmd, erm, schema, std::move(pr), cl, repair_decision,
                                                 query_options.trace_state, replicas, is_read_non_local,
                                                 query_options.permit);
        if (!r_read_executor) {
            co_return std::move(r_read_executor).as_failure();
        }

        exec.emplace_back(r_read_executor.value(), std::move(token_range));
    }
    if (is_read_non_local) {
        get_stats().reads_coordinator_outside_replica_set++;
    }

    replicas_per_token_range used_replicas;

    // keeps sp alive for the co-routine lifetime
    auto p = shared_from_this();

    ::result<foreign_ptr<lw_shared_ptr<query::result>>> result = nullptr;

    // The following try..catch chain could be converted to an equivalent
    // utils::result_futurize_try, however the code would no longer be placed
    // inside a single coroutine and the number of task allocations would
    // increase (utils::result_futurize_try is not a coroutine).

    try {
        auto timeout = query_options.timeout(*this);
        auto handle_completion = [&] (std::pair<::shared_ptr<abstract_read_executor>, dht::token_range>& executor_and_token_range) {
                auto& [rex, token_range] = executor_and_token_range;
                used_replicas.emplace(std::move(token_range), endpoints_to_replica_ids(tm, rex->used_targets()));
                auto latency = rex->max_request_latency();
                if (latency) {
                    rex->get_cf()->add_coordinator_read_latency(*latency);
                }
        };

        if (exec.size() == 1) [[likely]] {
            result = co_await exec[0].first->execute(timeout);
            // Handle success here. Failure is handled just outside the try..catch.
            if (result) {
                handle_completion(exec[0]);
            }
        } else {
            auto mapper = [&] (
                    std::pair<::shared_ptr<abstract_read_executor>, dht::token_range>& executor_and_token_range) -> future<::result<foreign_ptr<lw_shared_ptr<query::result>>>> {
                auto result = co_await executor_and_token_range.first->execute(timeout);
                // Handle success here. Failure is handled (only once) just outside the try..catch.
                if (result) {
                    handle_completion(executor_and_token_range);
                }
                co_return std::move(result);
            };
            query::result_merger merger(cmd->get_row_limit(), cmd->partition_limit);
            merger.reserve(exec.size());
            result = co_await utils::result_map_reduce(exec.begin(), exec.end(), std::move(mapper), std::move(merger));
        }
    } catch(...) {
        handle_read_error(std::current_exception(), false);
        throw;
    }

    if (!result) {
        // TODO: The error could be passed by reference, avoid a clone here
        handle_read_error(result.error().clone(), false);
        co_return std::move(result).as_failure();
    }

    co_return coordinator_query_result(std::move(result).value(), std::move(used_replicas), repair_decision);
}

bool storage_proxy::is_worth_merging_for_range_query(
        const locator::topology& topo,
        inet_address_vector_replica_set& merged,
        inet_address_vector_replica_set& l1,
        inet_address_vector_replica_set& l2) const {
    auto has_remote_node = [&topo, my_dc = topo.get_datacenter()] (inet_address_vector_replica_set& l) {
        for (auto&& ep : l) {
            if (my_dc != topo.get_datacenter(ep)) {
                return true;
            }
        }
        return false;
    };

    //
    // Querying remote DC is likely to be an order of magnitude slower than
    // querying locally, so 2 queries to local nodes is likely to still be
    // faster than 1 query involving remote ones
    //

    bool merged_has_remote = has_remote_node(merged);
    return merged_has_remote
        ? (has_remote_node(l1) || has_remote_node(l2))
        : true;
}

future<result<query_partition_key_range_concurrent_result>>
storage_proxy::query_partition_key_range_concurrent(storage_proxy::clock_type::time_point timeout,
        locator::effective_replication_map_ptr erm,
        lw_shared_ptr<query::read_command> cmd,
        db::consistency_level cl,
        query_ranges_to_vnodes_generator ranges_to_vnodes,
        int concurrency_factor,
        tracing::trace_state_ptr trace_state,
        uint64_t remaining_row_count,
        uint32_t remaining_partition_count,
        replicas_per_token_range preferred_replicas,
        service_permit permit) {
    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results;
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    auto p = shared_from_this();
    auto& cf= _db.local().find_column_family(schema);
    auto pcf = _db.local().get_config().cache_hit_rate_read_balancing() ? &cf : nullptr;
    const auto& tm = erm->get_token_metadata();

    if (_features.range_scan_data_variant) {
        cmd->slice.options.set<query::partition_slice::option::range_scan_data_variant>();
    }

    const auto preferred_replicas_for_range = [&preferred_replicas, &tm] (const dht::partition_range& r) {
        auto it = preferred_replicas.find(r.transform(std::mem_fn(&dht::ring_position::token)));
        return it == preferred_replicas.end() ? inet_address_vector_replica_set{} : replica_ids_to_endpoints(tm, it->second);
    };
    const auto to_token_range = [] (const dht::partition_range& r) { return r.transform(std::mem_fn(&dht::ring_position::token)); };

    for (;;) {
        std::vector<::shared_ptr<abstract_read_executor>> exec;
        std::unordered_map<abstract_read_executor*, std::vector<dht::token_range>> ranges_per_exec;
        dht::partition_range_vector ranges = ranges_to_vnodes(concurrency_factor);
        dht::partition_range_vector::iterator i = ranges.begin();

        // query_ranges_to_vnodes_generator can return less results than requested. If the number of results
        // is small enough or there are a lot of results - concurrentcy_factor which is increased by shifting left can
        // eventually zero out resulting in an infinite recursion. This line makes sure that concurrency factor is never
        // get stuck on 0 and never increased too much if the number of results remains small.
        concurrency_factor = std::max(size_t(1), ranges.size());

        while (i != ranges.end()) {
            dht::partition_range& range = *i;
            inet_address_vector_replica_set live_endpoints = get_endpoints_for_reading(schema->ks_name(), *erm, end_token(range));
            inet_address_vector_replica_set merged_preferred_replicas = preferred_replicas_for_range(*i);
            inet_address_vector_replica_set filtered_endpoints = filter_replicas_for_read(cl, *erm, live_endpoints, merged_preferred_replicas, pcf);
            std::vector<dht::token_range> merged_ranges{to_token_range(range)};
            ++i;

            co_await coroutine::maybe_yield();

            // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
            // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
            // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
            if (!erm->get_replication_strategy().uses_tablets()) {
                while (i != ranges.end())
                {
                    const auto current_range_preferred_replicas = preferred_replicas_for_range(*i);
                    dht::partition_range& next_range = *i;
                    inet_address_vector_replica_set next_endpoints = get_endpoints_for_reading(schema->ks_name(), *erm, end_token(next_range));
                    inet_address_vector_replica_set next_filtered_endpoints = filter_replicas_for_read(cl, *erm, next_endpoints, current_range_preferred_replicas, pcf);

                    // Origin has this to say here:
                    // *  If the current range right is the min token, we should stop merging because CFS.getRangeSlice
                    // *  don't know how to deal with a wrapping range.
                    // *  Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
                    // *  the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
                    // *  wire compatibility, so It's likely easier not to bother;
                    // It obviously not apply for us(?), but lets follow origin for now
                    if (end_token(range) == dht::maximum_token()) {
                        break;
                    }

                    // Implementing a proper contiguity check is hard, because it requires
                    // is_successor(interval_bound<dht::ring_position> a, interval_bound<dht::ring_position> b)
                    // relation to be defined. It is needed for intervals for which their possibly adjacent
                    // bounds are either both exclusive or inclusive.
                    // For example: is_adjacent([a, b], [c, d]) requires checking is_successor(b, c).
                    // Defining a successor relationship for dht::ring_position is hard, because
                    // dht::ring_position can possibly contain partition key.
                    // Luckily, a full contiguity check here is not needed.
                    // Ranges that we want to merge here are formed by dividing a bigger ranges using
                    // query_ranges_to_vnodes_generator. By knowing query_ranges_to_vnodes_generator internals,
                    // it can be assumed that usually, mergeable ranges are of the form [a, b) [b, c).
                    // Therefore, for the most part, contiguity check is reduced to equality & inclusivity test.
                    // It's fine, that we don't detect contiguity of some other possibly contiguous
                    // ranges (like [a, b] [b+1, c]), because not merging contiguous ranges (as opposed
                    // to merging discontiguous ones) is not a correctness problem.
                    bool maybe_discontiguous = !next_range.start() || !(
                        range.end()->value().equal(*schema, next_range.start()->value()) ?
                        (range.end()->is_inclusive() || next_range.start()->is_inclusive()) : false
                    );
                    // Do not merge ranges that may be discontiguous with each other
                    if (maybe_discontiguous) {
                        break;
                    }

                    inet_address_vector_replica_set merged = intersection(live_endpoints, next_endpoints);
                    inet_address_vector_replica_set current_merged_preferred_replicas = intersection(merged_preferred_replicas, current_range_preferred_replicas);

                    // Check if there is enough endpoint for the merge to be possible.
                    if (!is_sufficient_live_nodes(cl, *erm, merged)) {
                        break;
                    }

                    inet_address_vector_replica_set filtered_merged = filter_replicas_for_read(cl, *erm, merged, current_merged_preferred_replicas, pcf);

                    // Estimate whether merging will be a win or not
                    if (filtered_merged.empty()
                            || !is_worth_merging_for_range_query(
                                    erm->get_topology(), filtered_merged, filtered_endpoints, next_filtered_endpoints)) {
                        break;
                    } else if (pcf) {
                        // check that merged set hit rate is not to low
                        auto find_min = [this, pcf] (const inet_address_vector_replica_set& range) {
                            if (only_me(range)) {
                                // The `min_element` call below would return the same thing, but thanks to this branch
                                // we avoid having to access `remote` - so we can perform local queries without `remote`.
                                return float(pcf->get_my_hit_rate().rate);
                            }

                            // There are nodes other than us in `range`.
                            struct {
                                const gms::gossiper& g;
                                replica::column_family* cf = nullptr;
                                float operator()(const gms::inet_address& ep) const {
                                    return float(cf->get_hit_rate(g, ep).rate);
                                }
                            } ep_to_hr{remote().gossiper(), pcf};

                            if (range.empty()) {
                                on_internal_error(slogger, "empty range passed to `find_min`");
                            }
                            return *boost::range::min_element(range | boost::adaptors::transformed(ep_to_hr));
                        };
                        auto merged = find_min(filtered_merged) * 1.2; // give merged set 20% boost
                        if (merged < find_min(filtered_endpoints) && merged < find_min(next_filtered_endpoints)) {
                            // if lowest cache hits rate of a merged set is smaller than lowest cache hit
                            // rate of un-merged sets then do not merge. The idea is that we better issue
                            // two different range reads with highest chance of hitting a cache then one read that
                            // will cause more IO on contacted nodes
                            break;
                        }
                    }

                    // If we get there, merge this range and the next one
                    range = dht::partition_range(range.start(), next_range.end());
                    live_endpoints = std::move(merged);
                    merged_preferred_replicas = std::move(current_merged_preferred_replicas);
                    filtered_endpoints = std::move(filtered_merged);
                    ++i;
                    merged_ranges.push_back(to_token_range(next_range));
                    co_await coroutine::maybe_yield();
                }
            }
            slogger.trace("creating range read executor for range {} in table {}.{} with targets {}",
                        range, schema->ks_name(), schema->cf_name(), filtered_endpoints);
            try {
                db::assure_sufficient_live_nodes(cl, *erm, filtered_endpoints);
            } catch(exceptions::unavailable_exception& ex) {
                slogger.debug("Read unavailable: cl={} required {} alive {}", ex.consistency, ex.required, ex.alive);
                get_stats().range_slice_unavailables.mark();
                throw;
            }

            exec.push_back(::make_shared<never_speculating_read_executor>(schema, cf.shared_from_this(), p, erm, cmd, std::move(range), cl, std::move(filtered_endpoints), trace_state, permit, std::monostate()));
            ranges_per_exec.emplace(exec.back().get(), std::move(merged_ranges));
        }

        query::result_merger merger(cmd->get_row_limit(), cmd->partition_limit);
        merger.reserve(exec.size());

        auto wrapped_result = co_await utils::result_map_reduce(exec.begin(), exec.end(), [timeout] (::shared_ptr<abstract_read_executor>& rex) {
            return rex->execute(timeout);
        }, std::move(merger));

        if (!wrapped_result) {
            auto error = std::move(wrapped_result).assume_error();
            p->handle_read_error(error.clone(), true);
            co_return error;
        }

        foreign_ptr<lw_shared_ptr<query::result>> result = std::move(wrapped_result).value();
        result->ensure_counts();
        remaining_row_count -= result->row_count().value();
        remaining_partition_count -= result->partition_count().value();
        results.emplace_back(std::move(result));
        if (ranges_to_vnodes.empty() || !remaining_row_count || !remaining_partition_count) {
            auto used_replicas = replicas_per_token_range();
            for (auto& e : exec) {
                // We add used replicas in separate per-vnode entries even if
                // they were merged, for two reasons:
                // 1) The list of replicas is determined for each vnode
                // separately and thus this makes lookups more convenient.
                // 2) On the next page the ranges might not be merged.
                auto replica_ids = endpoints_to_replica_ids(tm, e->used_targets());
                for (auto& r : ranges_per_exec[e.get()]) {
                    used_replicas.emplace(std::move(r), replica_ids);
                }
            }
            co_return query_partition_key_range_concurrent_result{std::move(results), std::move(used_replicas)};
        } else {
            cmd->set_row_limit(remaining_row_count);
            cmd->partition_limit = remaining_partition_count;
            concurrency_factor *= 2;
        }
    }
}

future<result<storage_proxy::coordinator_query_result>>
storage_proxy::query_partition_key_range(lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector partition_ranges,
        db::consistency_level cl,
        storage_proxy::coordinator_query_options query_options) {
    schema_ptr schema = local_schema_registry().get(cmd->schema_version);
    replica::table& table = _db.local().find_column_family(schema->id());
    auto erm = table.get_effective_replication_map();

    // when dealing with LocalStrategy and EverywhereStrategy keyspaces, we can skip the range splitting and merging
    // (which can be expensive in clusters with vnodes)
    auto merge_tokens = !erm->get_replication_strategy().natural_endpoints_depend_on_token();

    query_ranges_to_vnodes_generator ranges_to_vnodes(erm->make_splitter(), schema, std::move(partition_ranges), merge_tokens);

    int result_rows_per_range = 0;
    int concurrency_factor = 1;

    slogger.debug("Estimated result rows per range: {}; requested rows: {}, concurrent range requests: {}",
            result_rows_per_range, cmd->get_row_limit(), concurrency_factor);

    // The call to `query_partition_key_range_concurrent()` below
    // updates `cmd` directly when processing the results. Under
    // some circumstances, when the query executes without deferring,
    // this updating will happen before the lambda object is constructed
    // and hence the updates will be visible to the lambda. This will
    // result in the merger below trimming the results according to the
    // updated (decremented) limits and causing the paging logic to
    // declare the query exhausted due to the non-full page. To avoid
    // this save the original values of the limits here and pass these
    // to the lambda below.
    const auto row_limit = cmd->get_row_limit();
    const auto partition_limit = cmd->partition_limit;

    auto wrapped_result = co_await query_partition_key_range_concurrent(query_options.timeout(*this),
            std::move(erm),
            cmd,
            cl,
            std::move(ranges_to_vnodes),
            concurrency_factor,
            std::move(query_options.trace_state),
            cmd->get_row_limit(),
            cmd->partition_limit,
            std::move(query_options.preferred_replicas),
            std::move(query_options.permit));

    if (!wrapped_result) {
        co_return bo::failure(std::move(wrapped_result).assume_error());
    }

    auto query_result = std::move(wrapped_result).value();
    std::vector<foreign_ptr<lw_shared_ptr<query::result>>>& query_results = query_result.result;
    replicas_per_token_range& used_replicas = query_result.replicas;

    query::result_merger merger(row_limit, partition_limit);
    merger.reserve(query_results.size());

    for (auto&& r: query_results) {
        merger(std::move(r));
    }

    co_return coordinator_query_result(merger.get(), std::move(used_replicas));
}

future<storage_proxy::coordinator_query_result>
storage_proxy::query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options)
{
    utils::get_local_injector().inject("storage_proxy_query_failure", [] { throw std::runtime_error("Error injection: failing a query"); });
    return query_result(std::move(s), std::move(cmd), std::move(partition_ranges), cl, std::move(query_options))
            .then(utils::result_into_future<result<storage_proxy::coordinator_query_result>>);
}

future<result<storage_proxy::coordinator_query_result>>
storage_proxy::query_result(schema_ptr query_schema,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options)
{
    if (slogger.is_enabled(logging::log_level::trace) || qlogger.is_enabled(logging::log_level::trace)) {
        static thread_local int next_id = 0;
        auto query_id = next_id++;

        slogger.trace("query {}.{} cmd={}, ranges={}, id={}", query_schema->ks_name(), query_schema->cf_name(), *cmd, partition_ranges, query_id);
        return do_query(query_schema, cmd, std::move(partition_ranges), cl, std::move(query_options)).then_wrapped([query_id, cmd, query_schema] (future<result<coordinator_query_result>> f) -> result<coordinator_query_result> {
            auto rres = utils::result_try([&] {
                return f.get();
            },  utils::result_catch_dots([&] (auto&& handle) {
                slogger.trace("query id={} failed: {}", query_id, handle.as_inner());
                return handle.into_result();
            }));
            if (!rres) {
                return std::move(rres).as_failure();
            }
            auto qr = std::move(rres).value();
            auto& res = qr.query_result;
            if (res->buf().is_linearized()) {
                res->ensure_counts();
                slogger.trace("query_result id={}, size={}, rows={}, partitions={}", query_id, res->buf().size(), *res->row_count(), *res->partition_count());
            } else {
                slogger.trace("query_result id={}, size={}", query_id, res->buf().size());
            }
            qlogger.trace("id={}, {}", query_id, res->pretty_printer(query_schema, cmd->slice));
            return qr;
        });
    }

    return do_query(query_schema, cmd, std::move(partition_ranges), cl, std::move(query_options));
}

future<result<storage_proxy::coordinator_query_result>>
storage_proxy::do_query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options)
{
    static auto make_empty = [] {
        return make_ready_future<result<coordinator_query_result>>(make_foreign(make_lw_shared<query::result>()));
    };

    auto& slice = cmd->slice;
    if (partition_ranges.empty() ||
            (slice.default_row_ranges().empty() && !slice.get_specific_ranges())) {
        return make_empty();
    }

    if (db::is_serial_consistency(cl)) {
        auto f = do_query_with_paxos(std::move(s), std::move(cmd), std::move(partition_ranges), cl, std::move(query_options));
        return utils::then_ok_result<result<storage_proxy::coordinator_query_result>>(std::move(f));
    } else {
        utils::latency_counter lc;
        lc.start();
        auto p = shared_from_this();

        if (query::is_single_partition(partition_ranges[0])) { // do not support mixed partitions (yet?)
            try {
                return query_singular(cmd,
                        std::move(partition_ranges),
                        cl,
                        std::move(query_options)).finally([lc, p] () mutable {
                    p->get_stats().read.mark(lc.stop().latency());
                });
            } catch (const replica::no_such_column_family&) {
                get_stats().read.mark(lc.stop().latency());
                return make_empty();
            }
        }

        return query_partition_key_range(cmd,
                std::move(partition_ranges),
                cl,
                std::move(query_options)).finally([lc, p] () mutable {
            p->get_stats().range.mark(lc.stop().latency());
        });
    }
}

// WARNING: the function should be called on a shard that owns the key that is been read
future<storage_proxy::coordinator_query_result>
storage_proxy::do_query_with_paxos(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    dht::partition_range_vector&& partition_ranges,
    db::consistency_level cl,
    storage_proxy::coordinator_query_options query_options) {
    if (partition_ranges.size() != 1 || !query::is_single_partition(partition_ranges[0])) {
        return make_exception_future<storage_proxy::coordinator_query_result>(
                exceptions::invalid_request_exception("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time"));
    }

    if (cas_shard(*s, partition_ranges[0].start()->value().as_decorated_key().token()) != this_shard_id()) {
        return make_exception_future<storage_proxy::coordinator_query_result>(std::logic_error("storage_proxy::do_query_with_paxos called on a wrong shard"));
    }
    // All cas networking operations run with query provided timeout
    db::timeout_clock::time_point timeout = query_options.timeout(*this);
    // When to give up due to contention
    db::timeout_clock::time_point cas_timeout = db::timeout_clock::now() +
            std::chrono::milliseconds(_db.local().get_config().cas_contention_timeout_in_ms());

    struct read_cas_request : public cas_request {
        foreign_ptr<lw_shared_ptr<query::result>> res;
        std::optional<mutation> apply(foreign_ptr<lw_shared_ptr<query::result>> qr,
            const query::partition_slice& slice, api::timestamp_type ts) {
            res = std::move(qr);
            return std::nullopt;
        }
    };

    auto request = seastar::make_shared<read_cas_request>();

    return cas(std::move(s), request, cmd, std::move(partition_ranges), std::move(query_options),
            cl, db::consistency_level::ANY, timeout, cas_timeout, false).then([request] (bool is_applied) mutable {
        return make_ready_future<coordinator_query_result>(std::move(request->res));
    });
}

static lw_shared_ptr<query::read_command> read_nothing_read_command(schema_ptr schema) {
    // Note that because this read-nothing command has an empty slice,
    // storage_proxy::query() returns immediately - without any networking.
    auto partition_slice = query::partition_slice({}, {}, {}, query::partition_slice::option_set());
    return ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice,
            query::max_result_size(query::result_memory_limiter::unlimited_result_size), query::tombstone_limit::max);
}

static read_timeout_exception write_timeout_to_read(schema_ptr s, mutation_write_timeout_exception& ex) {
    return read_timeout_exception(s->ks_name(), s->cf_name(), ex.consistency, ex.received, ex.block_for, false);
}

static read_failure_exception write_failure_to_read(schema_ptr s, mutation_write_failure_exception& ex) {
    return read_failure_exception(s->ks_name(), s->cf_name(), ex.consistency, ex.received, ex.failures, ex.block_for, false);
}

static mutation_write_timeout_exception read_timeout_to_write(schema_ptr s, read_timeout_exception& ex) {
    return mutation_write_timeout_exception(s->ks_name(), s->cf_name(), ex.consistency, ex.received, ex.block_for, db::write_type::CAS);
}

static mutation_write_failure_exception read_failure_to_write(schema_ptr s, read_failure_exception& ex) {
    return mutation_write_failure_exception(s->ks_name(), s->cf_name(), ex.consistency, ex.received, ex.failures, ex.block_for, db::write_type::CAS);
}

/**
 * Apply mutations if and only if the current values in the row for the given key
 * match the provided conditions. The algorithm is "raw" Paxos: that is, Paxos
 * minus leader election -- any node in the cluster may propose changes for any row,
 * which (that is, the row) is the unit of values being proposed, not single columns.
 *
 * The Paxos cohort is only the replicas for the given key, not the entire cluster.
 * So we expect performance to be reasonable, but CAS is still intended to be used
 * "when you really need it," not for all your updates.
 *
 * There are three phases to Paxos:
 *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
 *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
 *     accepted.
 *  2. Accept: if a majority of replicas respond, the coordinator asks replicas to accept the value of the
 *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
 *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
 *     value.
 *
 * Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
 * so here is our approach:
 *  3a. The coordinator sends a commit message to all replicas with the ballot and value.
 *  3b. Because of 1-2, this will be the highest-seen commit ballot. The replicas will note that,
 *      and send it with subsequent promise replies. This allows us to discard acceptance records
 *      for successfully committed replicas, without allowing incomplete proposals to commit erroneously
 *      later on.
 *
 * Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
 * values) between the prepare and accept phases. This gives us a slightly longer window for another
 * coordinator to come along and trump our own promise with a newer one but is otherwise safe.
 *
 * NOTE: `cmd` argument can be nullptr, in which case it's guaranteed that this function would not perform
 * any reads of committed values (in case user of the function is not interested in them).
 *
 * WARNING: the function should be called on a shard that owns the key cas() operates on
 */
future<bool> storage_proxy::cas(schema_ptr schema, shared_ptr<cas_request> request, lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector partition_ranges, storage_proxy::coordinator_query_options query_options,
        db::consistency_level cl_for_paxos, db::consistency_level cl_for_learn,
        clock_type::time_point write_timeout, clock_type::time_point cas_timeout, bool write) {

    auto& table = local_db().find_column_family(schema->id());
    if (table.uses_tablets()) {
        auto msg = format("Cannot use LightWeight Transactions for table {}.{}: LWT is not yet supported with tablets", schema->ks_name(), schema->cf_name());
        co_await coroutine::return_exception(exceptions::invalid_request_exception(msg));
    }

    SCYLLA_ASSERT(partition_ranges.size() == 1);
    SCYLLA_ASSERT(query::is_single_partition(partition_ranges[0]));

    db::validate_for_cas(cl_for_paxos);
    db::validate_for_cas_learn(cl_for_learn, schema->ks_name());

    if (cas_shard(*schema, partition_ranges[0].start()->value().as_decorated_key().token()) != this_shard_id()) {
        co_await coroutine::return_exception(std::logic_error("storage_proxy::cas called on a wrong shard"));
    }

    // In case a nullptr is passed to this function (i.e. the caller isn't interested in
    // existing value) we fabricate an "empty"  read_command that does nothing,
    // i.e. appropriate calls to storage_proxy::query immediately return an
    // empty query::result object without accessing any data.
    if (!cmd) {
        cmd = read_nothing_read_command(schema);
    }

    shared_ptr<paxos_response_handler> handler;
    try {
        handler = seastar::make_shared<paxos_response_handler>(shared_from_this(),
                query_options.trace_state, query_options.permit,
                partition_ranges[0].start()->value().as_decorated_key(),
                schema, cmd, cl_for_paxos, cl_for_learn, write_timeout, cas_timeout);
    } catch (exceptions::unavailable_exception& ex) {
        write ?  get_stats().cas_write_unavailables.mark() : get_stats().cas_read_unavailables.mark();
        throw;
    }

    db::consistency_level cl = cl_for_paxos == db::consistency_level::LOCAL_SERIAL ?
        db::consistency_level::LOCAL_QUORUM : db::consistency_level::QUORUM;

    unsigned contentions = 0;

    dht::token token = partition_ranges[0].start()->value().as_decorated_key().token();
    utils::latency_counter lc;
    lc.start();

    bool condition_met;

    try {
        auto update_stats = seastar::defer ([&] {
            get_stats().cas_foreground--;
            write ? get_stats().cas_write.mark(lc.stop().latency()) : get_stats().cas_read.mark(lc.stop().latency());
            if (contentions > 0) {
                write ? get_stats().cas_write_contention.add(contentions) : get_stats().cas_read_contention.add(contentions);
            }
        });

        auto l = co_await paxos::paxos_state::get_cas_lock(token, write_timeout);

        co_await utils::get_local_injector().inject("cas_timeout_after_lock", write_timeout + std::chrono::milliseconds(100));

        while (true) {
            // Finish the previous PAXOS round, if any, and, as a side effect, compute
            // a ballot (round identifier) which is a) unique b) has good chances of being
            // recent enough.
            auto [ballot, qr] = co_await handler->begin_and_repair_paxos(query_options.cstate, contentions, write);
            // Read the current values and check they validate the conditions.
            if (qr) {
                paxos::paxos_state::logger.debug("CAS[{}]: Using prefetched values for CAS precondition",
                        handler->id());
                tracing::trace(handler->tr_state, "Using prefetched values for CAS precondition");
            } else {
                paxos::paxos_state::logger.debug("CAS[{}]: Reading existing values for CAS precondition",
                        handler->id());
                tracing::trace(handler->tr_state, "Reading existing values for CAS precondition");
                ++get_stats().cas_failed_read_round_optimization;

                auto pr = partition_ranges; // cannot move original because it can be reused during retry
                auto cqr = co_await query(schema, cmd, std::move(pr), cl, query_options);
                qr = std::move(cqr.query_result);
            }

            auto mutation = request->apply(std::move(qr), cmd->slice, utils::UUID_gen::micros_timestamp(ballot));
            condition_met = true;
            if (!mutation) {
                if (write) {
                    paxos::paxos_state::logger.debug("CAS[{}] precondition does not match current values", handler->id());
                    tracing::trace(handler->tr_state, "CAS precondition does not match current values");
                    ++get_stats().cas_write_condition_not_met;
                    condition_met = false;
                }
                // If a condition is not met we still need to complete paxos round to achieve
                // linearizability otherwise next write attempt may read different value as described
                // in https://github.com/scylladb/scylla/issues/6299
                // Let's use empty mutation as a value and proceed
                mutation.emplace(handler->schema(), handler->key());
                // since the value we are writing is dummy we may use minimal consistency level for learn
                handler->set_cl_for_learn(db::consistency_level::ANY);
            } else {
                paxos::paxos_state::logger.debug("CAS[{}] precondition is met; proposing client-requested updates for {}",
                        handler->id(), ballot);
                tracing::trace(handler->tr_state, "CAS precondition is met; proposing client-requested updates for {}", ballot);
            }

            auto proposal = make_lw_shared<paxos::proposal>(ballot, freeze(*mutation));

            bool is_accepted = co_await handler->accept_proposal(proposal);
            if (is_accepted) {
                // The majority (aka a QUORUM) has promised the coordinator to
                // accept the action associated with the computed ballot.
                // Apply the mutation.
                try {
                  co_await handler->learn_decision(std::move(proposal));
                } catch (unavailable_exception& e) {
                    // if learning stage encountered unavailablity error lets re-map it to a write error
                    // since unavailable error means that operation has never ever started which is not
                    // the case here
                    schema_ptr schema = handler->schema();
                    throw mutation_write_timeout_exception(schema->ks_name(), schema->cf_name(),
                                          e.consistency, e.alive, e.required, db::write_type::CAS);
                }
                paxos::paxos_state::logger.debug("CAS[{}] successful", handler->id());
                tracing::trace(handler->tr_state, "CAS successful");
                break;
            } else {
                paxos::paxos_state::logger.debug("CAS[{}] PAXOS proposal not accepted (pre-empted by a higher ballot)",
                        handler->id());
                tracing::trace(handler->tr_state, "PAXOS proposal not accepted (pre-empted by a higher ballot)");
                ++contentions;
                co_await sleep_approx_50ms();
            }
        }
    } catch (read_failure_exception& ex) {
        write ? throw read_failure_to_write(schema, ex) : throw;
    } catch (read_timeout_exception& ex) {
        if (write) {
            get_stats().cas_write_timeouts.mark();
            throw read_timeout_to_write(schema, ex);
        } else {
            get_stats().cas_read_timeouts.mark();
            throw;
        }
    } catch (mutation_write_failure_exception& ex) {
        write ? throw : throw write_failure_to_read(schema, ex);
    } catch (mutation_write_timeout_exception& ex) {
        if (write) {
            get_stats().cas_write_timeouts.mark();
            throw;
        } else {
            get_stats().cas_read_timeouts.mark();
            throw write_timeout_to_read(schema, ex);
        }
    } catch (exceptions::unavailable_exception& ex) {
        write ? get_stats().cas_write_unavailables.mark() :  get_stats().cas_read_unavailables.mark();
        throw;
    } catch (seastar::semaphore_timed_out& ex) {
        paxos::paxos_state::logger.trace("CAS[{}]: timeout while waiting for row lock {}", handler->id(), ex.what());
        if (write) {
            get_stats().cas_write_timeouts.mark();
            throw mutation_write_timeout_exception(schema->ks_name(), schema->cf_name(), cl_for_paxos, 0,  handler->block_for(), db::write_type::CAS);
        } else {
            get_stats().cas_read_timeouts.mark();
            throw read_timeout_exception(schema->ks_name(), schema->cf_name(), cl_for_paxos, 0,  handler->block_for(), 0);
        }
    }

    co_return condition_met;
}

inet_address_vector_replica_set storage_proxy::get_live_endpoints(const locator::effective_replication_map& erm, const dht::token& token) const {
    inet_address_vector_replica_set eps = erm.get_natural_endpoints_without_node_being_replaced(token);
    auto itend = boost::range::remove_if(eps, std::not_fn(std::bind_front(&storage_proxy::is_alive, this)));
    eps.erase(itend, eps.end());
    return eps;
}

void storage_proxy::sort_endpoints_by_proximity(const locator::topology& topo, inet_address_vector_replica_set& eps) const {
    topo.sort_by_proximity(my_address(), eps);
    // FIXME: before dynamic snitch is implement put local address (if present) at the beginning
    auto it = boost::range::find(eps, my_address());
    if (it != eps.end() && it != eps.begin()) {
        std::iter_swap(it, eps.begin());
    }
}

inet_address_vector_replica_set storage_proxy::get_endpoints_for_reading(const sstring& ks_name, const locator::effective_replication_map& erm, const dht::token& token) const {
    auto endpoints = erm.get_endpoints_for_reading(token);
    auto it = boost::range::remove_if(endpoints, std::not_fn(std::bind_front(&storage_proxy::is_alive, this)));
    endpoints.erase(it, endpoints.end());
    sort_endpoints_by_proximity(erm.get_topology(), endpoints);
    return endpoints;
}

// `live_endpoints` must already contain only replicas for this query; the function only filters out some of them.
inet_address_vector_replica_set
storage_proxy::filter_replicas_for_read(
        db::consistency_level cl,
        const locator::effective_replication_map& erm,
        inet_address_vector_replica_set live_endpoints,
        const inet_address_vector_replica_set& preferred_endpoints,
        db::read_repair_decision repair_decision,
        std::optional<gms::inet_address>* extra,
        replica::column_family* cf) const {
    if (live_endpoints.empty() || only_me(live_endpoints)) {
        // `db::filter_for_query` would return the same thing, but thanks to this branch we avoid having
        // to access `remote` - so we can perform local queries without the need of `remote`.
        return live_endpoints;
    }

    // There are nodes other than us in `live_endpoints`.
    auto& gossiper = remote().gossiper();

    return db::filter_for_query(cl, erm, std::move(live_endpoints), preferred_endpoints, repair_decision, gossiper, extra, cf);
}

inet_address_vector_replica_set
storage_proxy::filter_replicas_for_read(
        db::consistency_level cl,
        const locator::effective_replication_map& erm,
        const inet_address_vector_replica_set& live_endpoints,
        const inet_address_vector_replica_set& preferred_endpoints,
        replica::column_family* cf) const {
    return filter_replicas_for_read(cl, erm, live_endpoints, preferred_endpoints, db::read_repair_decision::NONE, nullptr, cf);
}

bool storage_proxy::is_alive(const gms::inet_address& ep) const {
    return _remote ? _remote->is_alive(ep) : is_me(ep);
}

inet_address_vector_replica_set storage_proxy::intersection(const inet_address_vector_replica_set& l1, const inet_address_vector_replica_set& l2) {
    inet_address_vector_replica_set inter;
    inter.reserve(l1.size());
    std::remove_copy_if(l1.begin(), l1.end(), std::back_inserter(inter), [&l2] (const gms::inet_address& a) {
        return std::find(l2.begin(), l2.end(), a) == l2.end();
    });
    return inter;
}

bool storage_proxy::hints_enabled(db::write_type type) const noexcept {
    return (!_hints_manager.is_disabled_for_all() && type != db::write_type::CAS) || type == db::write_type::VIEW;
}

db::hints::manager& storage_proxy::hints_manager_for(db::write_type type) {
    return type == db::write_type::VIEW ? _hints_for_views_manager : _hints_manager;
}

future<> storage_proxy::truncate_blocking(sstring keyspace, sstring cfname, std::optional<std::chrono::milliseconds> timeout_in_ms) {
    slogger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);

    if (local_db().find_keyspace(keyspace).get_replication_strategy().get_type() == locator::replication_strategy_type::local) {
        return replica::database::truncate_table_on_all_shards(_db, remote().system_keyspace().container(), keyspace, cfname);
    }

    return remote().send_truncate_blocking(std::move(keyspace), std::move(cfname), timeout_in_ms);
}

void storage_proxy::start_remote(netw::messaging_service& ms, gms::gossiper& g, migration_manager& mm, sharded<db::system_keyspace>& sys_ks) {
    _remote = std::make_unique<struct remote>(*this, ms, g, mm, sys_ks);
}

future<> storage_proxy::stop_remote() {
    co_await drain_on_shutdown();
    co_await _remote->stop();
    _remote = nullptr;
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>
storage_proxy::query_mutations_locally(schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd, const dht::partition_range& pr,
                                       storage_proxy::clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state) {
    auto& table = query_schema->table();
    auto erm = table.get_effective_replication_map();
    if (auto shard_opt = dht::is_single_shard(erm->get_sharder(*query_schema), *query_schema, pr)) {
        auto shard = *shard_opt;
        get_stats().replica_cross_shard_ops += shard != this_shard_id();
        return _db.invoke_on(shard, _read_smp_service_group, [cmd, &pr, gs=global_schema_ptr(query_schema), timeout, gt = tracing::global_trace_state_ptr(std::move(trace_state))] (replica::database& db) mutable {
            return db.query_mutations(gs, *cmd, pr, gt, timeout).then([] (std::tuple<reconcilable_result, cache_temperature> result_ht) {
                auto&& [result, ht] = result_ht;
                return make_ready_future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>(rpc::tuple(make_foreign(make_lw_shared<reconcilable_result>(std::move(result))), std::move(ht)));
            });
        });
    } else {
        return query_nonsingular_mutations_locally(std::move(query_schema), std::move(cmd), {pr}, std::move(trace_state), timeout);
    }
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>
storage_proxy::query_mutations_locally(schema_ptr query_schema, lw_shared_ptr<query::read_command> cmd, const ::compat::one_or_two_partition_ranges& pr,
                                       storage_proxy::clock_type::time_point timeout,
                                       tracing::trace_state_ptr trace_state) {
    if (!pr.second) {
        return query_mutations_locally(std::move(query_schema), std::move(cmd), pr.first, timeout, std::move(trace_state));
    } else {
        return query_nonsingular_mutations_locally(std::move(query_schema), std::move(cmd), pr, std::move(trace_state), timeout);
    }
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>>
storage_proxy::query_nonsingular_mutations_locally(schema_ptr s,
                                                   lw_shared_ptr<query::read_command> cmd,
                                                   const dht::partition_range_vector&& prs_in,
                                                   tracing::trace_state_ptr trace_state,
                                                   storage_proxy::clock_type::time_point timeout) {
    // This is a coroutine so that `cmd` and `prs` survive the call to query_muatations_on_all_shards().
    auto prs = std::move(prs_in);
    co_return co_await query_mutations_on_all_shards(_db, std::move(s), *cmd, prs, std::move(trace_state), timeout);
}

future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>>
storage_proxy::query_nonsingular_data_locally(schema_ptr s, lw_shared_ptr<query::read_command> cmd, const dht::partition_range_vector&& prs,
        query::result_options opts, tracing::trace_state_ptr trace_state, storage_proxy::clock_type::time_point timeout) {
    auto ranges = std::move(prs);
    auto local_cmd = cmd;
    rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> ret;
    if (local_cmd->slice.options.contains(query::partition_slice::option::range_scan_data_variant)) {
        ret = co_await query_data_on_all_shards(_db, std::move(s), *local_cmd, ranges, opts, std::move(trace_state), timeout);
    } else {
        auto res = co_await query_mutations_on_all_shards(_db, s, *local_cmd, ranges, std::move(trace_state), timeout);
        ret = rpc::tuple(make_foreign(make_lw_shared<query::result>(co_await to_data_query_result(std::move(*std::get<0>(res)), std::move(s), local_cmd->slice,
                local_cmd->get_row_limit(), local_cmd->partition_limit, opts))), std::get<1>(res));
    }
    co_return ret;
}

future<> storage_proxy::start_hints_manager() {
    if (!_hints_manager.is_disabled_for_all()) {
        co_await _hints_resource_manager.register_manager(_hints_manager);
    }
    co_await _hints_resource_manager.register_manager(_hints_for_views_manager);
    co_await _hints_resource_manager.start(remote().gossiper().shared_from_this());
}

void storage_proxy::allow_replaying_hints() noexcept {
    return _hints_resource_manager.allow_replaying();
}

future<> storage_proxy::change_hints_host_filter(db::hints::host_filter new_filter) {
    if (new_filter == _hints_manager.get_host_filter()) {
        co_return;
    }

    co_await _hints_directory_initializer.ensure_created_and_verified();
    co_await _hints_directory_initializer.ensure_rebalanced();
    // This function is idempotent
    co_await _hints_resource_manager.register_manager(_hints_manager);
    co_await _hints_manager.change_host_filter(std::move(new_filter));
}

const db::hints::host_filter& storage_proxy::get_hints_host_filter() const {
    return _hints_manager.get_host_filter();
}

future<db::hints::sync_point> storage_proxy::create_hint_sync_point(std::vector<gms::inet_address> target_hosts) const {
    db::hints::sync_point spoint;
    spoint.regular_per_shard_rps.resize(smp::count);
    spoint.mv_per_shard_rps.resize(smp::count);
    spoint.host_id = get_token_metadata_ptr()->get_my_id();

    // sharded::invoke_on does not have a const-method version, so we cannot use it here
    co_await smp::invoke_on_all([&sharded_sp = container(), &target_hosts, &spoint] {
        const storage_proxy& sp = sharded_sp.local();
        auto shard = this_shard_id();
        spoint.regular_per_shard_rps[shard] = sp._hints_manager.calculate_current_sync_point(target_hosts);
        spoint.mv_per_shard_rps[shard] = sp._hints_for_views_manager.calculate_current_sync_point(target_hosts);
    });
    co_return spoint;
}

future<> storage_proxy::wait_for_hint_sync_point(const db::hints::sync_point spoint, clock_type::time_point deadline) {
    const auto my_host_id = get_token_metadata_ptr()->get_my_id();
    if (spoint.host_id != my_host_id) {
        throw std::runtime_error(format("The hint sync point was created on another node, with host ID {}. This node's host ID is {}",
                spoint.host_id, my_host_id));
    }

    std::vector<abort_source> sources;
    sources.resize(smp::count);

    // If the timer is triggered, it will spawn a discarded future which triggers
    // abort sources on all shards. We need to make sure that this future
    // completes before exiting - we use a gate for that.
    seastar::gate timer_gate;
    seastar::timer<lowres_clock> t;
    t.set_callback([&timer_gate, &sources] {
        // The gate is waited on at the end of the wait_for_hint_sync_point function
        // The gate is guaranteed to be open at this point
        (void)with_gate(timer_gate, [&sources] {
            return smp::invoke_on_all([&sources] {
                unsigned shard = this_shard_id();
                if (!sources[shard].abort_requested()) {
                    sources[shard].request_abort();
                }
            });
        });
    });
    t.arm(deadline);

    bool was_aborted = false;
    unsigned original_shard = this_shard_id();
    co_await container().invoke_on_all([original_shard, &sources, &spoint, &was_aborted] (storage_proxy& sp) {
        auto wait_for = [&sources, original_shard, &was_aborted] (db::hints::manager& mgr, const std::vector<db::hints::sync_point::shard_rps>& shard_rps) {
            const unsigned shard = this_shard_id();
            return mgr.wait_for_sync_point(sources[shard], shard_rps[shard]).handle_exception([original_shard, &sources, &was_aborted] (auto eptr) {
                // Make sure other blocking operations are cancelled soon
                // by requesting an abort on all shards
                return smp::invoke_on_all([&sources] {
                    unsigned shard = this_shard_id();
                    if (!sources[shard].abort_requested()) {
                        sources[shard].request_abort();
                    }
                }).then([eptr = std::move(eptr), &was_aborted, original_shard] () mutable {
                    try {
                        std::rethrow_exception(std::move(eptr));
                    } catch (abort_requested_exception&) {
                        return smp::submit_to(original_shard, [&was_aborted] { was_aborted = true; });
                    } catch (...) {
                        return make_exception_future<>(std::current_exception());
                    }
                    return make_ready_future<>();
                });
            });
        };

        return when_all_succeed(
            wait_for(sp._hints_manager, spoint.regular_per_shard_rps),
            wait_for(sp._hints_for_views_manager, spoint.mv_per_shard_rps)
        ).discard_result();
    }).finally([&t, &timer_gate] {
        t.cancel();
        return timer_gate.close();
    });

    if (was_aborted) {
        throw timed_out_error{};
    }

    co_return;
}

void storage_proxy::on_join_cluster(const gms::inet_address& endpoint) {};

void storage_proxy::on_leave_cluster(const gms::inet_address& endpoint, const locator::host_id& hid) {
    // Discarding these futures is safe. They're awaited by db::hints::manager::stop().
    (void) _hints_manager.drain_for(hid, endpoint);
    (void) _hints_for_views_manager.drain_for(hid, endpoint);
}

void storage_proxy::on_up(const gms::inet_address& endpoint) {};

void storage_proxy::cancel_write_handlers(noncopyable_function<bool(const abstract_write_response_handler&)> filter_fun) {
    SCYLLA_ASSERT(thread::running_in_thread());
    auto it = _cancellable_write_handlers_list->begin();
    while (it != _cancellable_write_handlers_list->end()) {
        auto guard = it->shared_from_this();
        if (filter_fun(*it) && _response_handlers.contains(it->id())) {
            it->timeout_cb();
        }
        ++it;
        if (need_preempt()) {
            cancellable_write_handlers_list::iterator_guard ig{*_cancellable_write_handlers_list, it};
            seastar::thread::yield();
        }
    }
}

void storage_proxy::on_down(const gms::inet_address& endpoint) {
    return cancel_write_handlers([endpoint] (const abstract_write_response_handler& handler) {
        const auto& targets = handler.get_targets();
        return boost::find(targets, endpoint) != targets.end();
    });
};

future<> storage_proxy::drain_on_shutdown() {
    //NOTE: the thread is spawned here because there are delicate lifetime issues to consider
    // and writing them down with plain futures is error-prone.
    return async([this] {
        cancel_write_handlers([] (const abstract_write_response_handler&) { return true; });
        _hints_resource_manager.stop().get();
    });
}

future<> storage_proxy::abort_view_writes() {
    return async([this] {
        cancel_write_handlers([] (const abstract_write_response_handler& handler) { return handler.is_view(); });
    });
}

future<>
storage_proxy::stop() {
    return make_ready_future<>();
}

locator::token_metadata_ptr storage_proxy::get_token_metadata_ptr() const noexcept {
    return _shared_token_metadata.get();
}

future<std::vector<dht::token_range_endpoints>> storage_proxy::describe_ring(const sstring& keyspace, bool include_only_local_dc) const {
    return locator::describe_ring(_db.local(), _remote->gossiper(), keyspace, include_only_local_dc);
}

}
