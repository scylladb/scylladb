/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <chrono>
#include <fmt/ranges.h>

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/noncopyable_function.hh>

#include <variant>

#include "auth/service.hh"
#include "cdc/generation.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/system_keyspace.hh"
#include "dht/boot_strapper.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "locator/token_metadata.hh"
#include "locator/network_topology_strategy.hh"
#include "message/messaging_service.hh"
#include "mutation/async_utils.hh"
#include "raft/raft.hh"
#include "replica/database.hh"
#include "replica/tablet_mutation_builder.hh"
#include "replica/tablets.hh"
#include "db/view/view_builder.hh"
#include "service/qos/service_level_controller.hh"
#include "service/migration_manager.hh"
#include "service/raft/group0_voter_handler.hh"
#include "service/raft/join_node.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/tablet_allocator.hh"
#include "service/tablet_operation.hh"
#include "service/topology_state_machine.hh"
#include "topology_mutation.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "utils/stall_free.hh"
#include "utils/to_string.hh"
#include "service/endpoint_lifecycle_subscriber.hh"

#include "idl/join_node.dist.hh"
#include "idl/storage_service.dist.hh"
#include "replica/exceptions.hh"
#include "service/paxos/prepare_response.hh"
#include "idl/storage_proxy.dist.hh"

#include "service/topology_coordinator.hh"

#include <boost/range/join.hpp>

using token = dht::token;
using inet_address = gms::inet_address;

namespace service {

logging::logger rtlogger("raft_topology");

locator::host_id to_host_id(raft::server_id id) {
    return locator::host_id{id.uuid()};
}

future<> wait_for_gossiper(raft::server_id id, const gms::gossiper& g, abort_source& as) {
    const auto timeout = std::chrono::seconds{30};
    const auto deadline = lowres_clock::now() + timeout;
    while (true) {
        auto hids = g.get_node_ip(to_host_id(id));
        if (hids) {
            co_return;
        }
        if (lowres_clock::now() > deadline) {
            co_await coroutine::exception(std::make_exception_ptr(
                wait_for_ip_timeout(id, std::chrono::duration_cast<std::chrono::seconds>(timeout).count())));
        }
        static thread_local logger::rate_limit rate_limit{std::chrono::seconds(1)};
        rtlogger.log(log_level::warn, rate_limit, "cannot map {} to ip, retrying.", id);
        co_await sleep_abortable(std::chrono::milliseconds(5), as);
    }
}

namespace {
// Doesn't throw error on absence of values.
sstring get_application_state_gently(const gms::application_state_map& epmap, gms::application_state app_state) {
    const auto it = epmap.find(app_state);
    if (it == epmap.end()) {
        return sstring{};
    }
    // it's versioned_value::value(), not std::optional::value() - it does not throw
    return it->second.value();
}
} // namespace

class topology_coordinator : public endpoint_lifecycle_subscriber {
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    gms::gossiper& _gossiper;
    netw::messaging_service& _messaging;
    locator::shared_token_metadata& _shared_tm;
    db::system_keyspace& _sys_ks;
    replica::database& _db;
    service::raft_group0& _group0;
    service::topology_state_machine& _topo_sm;
    abort_source& _as;
    gms::feature_service& _feature_service;

    raft::server& _raft;
    const raft::term_t _term;
    uint64_t _last_cmd_index = 0;

    raft_topology_cmd_handler_type _raft_topology_cmd_handler;

    tablet_allocator& _tablet_allocator;

    // The reason load_stats_ptr is a shared ptr is that load balancer can yield, and we don't want it
    // to suffer lifetime issues when stats refresh fiber overrides the current stats.
    std::unordered_map<locator::host_id, locator::load_stats> _load_stats_per_node;
    serialized_action _tablet_load_stats_refresh;
    // FIXME: make frequency per table in order to reduce work in each iteration.
    //  Bigger tables will take longer to be resized. similar-sized tables can be batched into same iteration.
    static constexpr std::chrono::seconds tablet_load_stats_refresh_interval = std::chrono::seconds(60);

    std::chrono::milliseconds _ring_delay;

    gate::holder _group0_holder;

    using drop_guard_and_retake = bool_class<class retake_guard_tag>;

    // Engaged if an ongoing topology change should be rolled back. The string inside
    // will indicate a reason for the rollback.
    std::optional<sstring> _rollback;

    group0_voter_handler _voter_handler;

    const locator::token_metadata& get_token_metadata() const noexcept {
        return *_shared_tm.get();
    }

    locator::token_metadata_ptr get_token_metadata_ptr() const noexcept {
        return _shared_tm.get();
    }

    // This is a topology snapshot for a given node. It contains pointers into the topology state machine
    // that may be outdated after guard is released so the structure is meant to be destroyed together
    // with the guard
    struct node_to_work_on {
        group0_guard guard;
        const topology_state_machine::topology_type* topology;
        raft::server_id id;
        const replica_state* rs;
        std::optional<topology_request> request;
        std::optional<request_param> req_param;
    };

    // The topology coordinator takes guard before operation start, but it releases it during various
    // RPC commands that it sends to make it possible to submit new requests to the state machine while
    // the coordinator drives current topology change. It is safe to do so since only the coordinator is
    // ever allowed to change node's state, others may only create requests. To make sure the coordinator did
    // not change while the lock was released, and hence the old coordinator does not work on old state, we check
    // that the raft term is still the same after the lock is re-acquired. Throw term_changed_error if it did.

    struct term_changed_error {};

    future<group0_guard> cleanup_group0_config_if_needed(group0_guard guard) {
        auto& topo = _topo_sm._topology;
        auto rconf = _group0.group0_server().get_configuration();
        if (!rconf.is_joint()) {
            // Find nodes that 'left' but still in the config and remove them
            auto to_remove = std::ranges::to<std::vector<raft::server_id>>(
                    rconf.current
                    | std::views::transform([&] (const raft::config_member& m) { return m.addr.id; })
                    | std::views::filter([&] (const raft::server_id& id) { return topo.left_nodes.contains(id); }));
            if (!to_remove.empty()) {
                // Remove from group 0 nodes that left. They may failed to do so by themselves
                release_guard(std::move(guard));
                try {
                    rtlogger.debug("topology coordinator fiber removing {}"
                                  " from raft since they are in `left` state", to_remove);
                    co_await _group0.group0_server().modify_config({}, to_remove, &_as);
                } catch (const raft::commit_status_unknown&) {
                    rtlogger.warn("topology coordinator fiber got commit_status_unknown status"
                                  " while removing {} from raft", to_remove);
                }
                guard = co_await start_operation();
            }
        }
        co_return std::move(guard);
    }

    struct cancel_requests {
        group0_guard guard;
        std::unordered_set<raft::server_id> dead_nodes;
    };

    struct start_cleanup {
        group0_guard guard;
    };

    // Return dead nodes
    std::unordered_set<locator::host_id> get_dead_nodes() const {
        std::unordered_set<locator::host_id> dead_set;
        for (auto& n : _topo_sm._topology.normal_nodes) {
            bool alive = false;
            try {
                alive = _gossiper.is_alive(locator::host_id(n.first.uuid()));
            } catch (...) {}

            if (!alive) {
                dead_set.insert(locator::host_id(n.first.uuid()));
            }
        }
        return dead_set;
    }

    // Return dead nodes and while at it checking if there are live nodes that either need cleanup
    // or running one already
    std::unordered_set<raft::server_id> get_dead_node(bool& cleanup_running, bool& cleanup_needed) const {
        std::unordered_set<raft::server_id> dead_set;
        cleanup_needed = cleanup_running = false;
        for (auto& n : _topo_sm._topology.normal_nodes) {
            bool alive = false;
            try {
                alive = _gossiper.is_alive(locator::host_id(n.first.uuid()));
            } catch (...) {}

            if (!alive) {
                dead_set.insert(n.first);
            } else {
                cleanup_running |= (n.second.cleanup == cleanup_status::running);
                cleanup_needed |= (n.second.cleanup == cleanup_status::needed);
            }
        }
        return dead_set;
    }

    std::optional<request_param> get_request_param(raft::server_id id) const {
        return _topo_sm._topology.get_request_param(id);
    };

    // Returns:
    // guard - there is nothing to do.
    // cancel_requests - no request can be started so cancel the queue
    // start_cleanup - cleanup needs to be started
    // node_to_work_on - the node the topology coordinator should work on
    std::variant<group0_guard, cancel_requests, start_cleanup, node_to_work_on> get_next_task(group0_guard guard) {
        auto& topo = _topo_sm._topology;

        if (topo.transition_nodes.size() != 0) {
            // If there is a node that is the middle of topology operation continue with it
            return get_node_to_work_on(std::move(guard));
        }

        bool cleanup_running;
        bool cleanup_needed;
        const auto dead_nodes = get_dead_node(cleanup_running, cleanup_needed);

        if (cleanup_running || topo.requests.empty()) {
            // Ether there is no requests or there is a live node that runs cleanup. Wait for it to complete.
            return std::move(guard);
        }

        std::optional<std::pair<raft::server_id, topology_request>> next_req;

        for (auto& req : topo.requests) {
            auto enough_live_nodes = [&] {
                if (req.second == topology_request::rebuild) {
                    // For rebuild only the node itself should be alive to start it
                    // it may still fail if down node has data for the rebuild process
                    return !dead_nodes.contains(req.first);
                }
                auto exclude_nodes = get_excluded_nodes(topo, req.first, req.second);
                for (auto id : dead_nodes) {
                    if (!exclude_nodes.contains(id)) {
                        return false;
                    }
                }
                return true;
            };
            if (enough_live_nodes()) {
                if (!next_req || next_req->second > req.second) {
                    next_req = req;
                }
            }
        }

        if (!next_req) {
            // We did not find a request that has enough live node to proceed
            // Cancel all requests to let admin know that no operation can succeed
            rtlogger.warn("topology coordinator: cancel request queue because no request can proceed. Dead nodes: {}", dead_nodes);
            return cancel_requests{std::move(guard), std::move(dead_nodes)};
        }

        auto [id, req] = *next_req;

        if (cleanup_needed && (req == topology_request::remove || req == topology_request::leave)) {
            // If the highest prio request is removenode or decommission we need to start cleanup if one is needed
            return start_cleanup(std::move(guard));
        }

        return node_to_work_on(std::move(guard), &topo, id, &topo.find(id)->second, req, get_request_param(id));
    };

    node_to_work_on get_node_to_work_on(group0_guard guard) const {
        auto& topo = _topo_sm._topology;

        if (topo.transition_nodes.empty()) {
            on_internal_error(rtlogger, ::format(
                "could not find node to work on"
                " even though the state requires it (state: {})", topo.tstate));
        }

        auto e = &*topo.transition_nodes.begin();
        return node_to_work_on{std::move(guard), &topo, e->first, &e->second, std::nullopt, get_request_param(e->first)};
     };

    future<group0_guard> start_operation() {
        rtlogger.debug("obtaining group 0 guard...");
        auto guard = co_await _group0.client().start_operation(_as);
        rtlogger.debug("guard taken, prev_state_id: {}, new_state_id: {}, coordinator term: {}, current Raft term: {}",
                       guard.observed_group0_state_id(), guard.new_group0_state_id(), _term, _raft.get_current_term());

        if (_term != _raft.get_current_term()) {
            throw term_changed_error{};
        }

        co_return std::move(guard);
    }

    void release_node(std::optional<node_to_work_on> node) {
        // Leaving the scope destroys the object and releases the guard.
    }

    node_to_work_on retake_node(group0_guard guard, raft::server_id id) const {
        auto& topo = _topo_sm._topology;

        auto it = topo.find(id);
        SCYLLA_ASSERT(it);

        std::optional<topology_request> req;
        auto rit = topo.requests.find(id);
        if (rit != topo.requests.end()) {
            req = rit->second;
        }
        std::optional<request_param> req_param;
        auto pit = topo.req_param.find(id);
        if (pit != topo.req_param.end()) {
            req_param = pit->second;
        }
        return node_to_work_on{std::move(guard), &topo, id, &it->second, std::move(req), std::move(req_param)};
    }

    group0_guard take_guard(node_to_work_on&& node) {
        return std::move(node.guard);
    }

    future<> update_topology_state(
            group0_guard guard, std::vector<canonical_mutation>&& updates, const sstring& reason) {
        try {
            rtlogger.info("updating topology state: {}", reason);
            rtlogger.trace("update_topology_state mutations: {}", updates);
            topology_change change{std::move(updates)};
            group0_command g0_cmd = _group0.client().prepare_command(std::move(change), guard, reason);
            co_await _group0.client().add_entry(std::move(g0_cmd), std::move(guard), _as);
        } catch (group0_concurrent_modification&) {
            rtlogger.info("race while changing state: {}. Retrying", reason);
            throw;
        }
    };

    raft::server_id parse_replaced_node(const std::optional<request_param>& req_param) const {
        return service::topology::parse_replaced_node(req_param);
    }

    future<> exec_direct_command_helper(raft::server_id id, uint64_t cmd_index, const raft_topology_cmd& cmd) {
        rtlogger.debug("send {} command with term {} and index {} to {}",
            cmd.cmd, _term, cmd_index, id);
        auto result = _db.get_token_metadata().get_topology().is_me(to_host_id(id)) ?
                    co_await _raft_topology_cmd_handler(_term, cmd_index, cmd) :
                    co_await ser::storage_service_rpc_verbs::send_raft_topology_cmd(
                            &_messaging, to_host_id(id), id, _term, cmd_index, cmd);
        if (result.status == raft_topology_cmd_result::command_status::fail) {
            co_await coroutine::exception(std::make_exception_ptr(
                    std::runtime_error(::format("failed status returned from {}", id))));
        }
    };

    future<node_to_work_on> exec_direct_command(node_to_work_on&& node, const raft_topology_cmd& cmd) {
        auto id = node.id;
        release_node(std::move(node));
        const auto cmd_index = ++_last_cmd_index;
        co_await exec_direct_command_helper(id, cmd_index, cmd);
        co_return retake_node(co_await start_operation(), id);
    };

    future<> exec_global_command_helper(auto nodes, const raft_topology_cmd& cmd) {
        const auto cmd_index = ++_last_cmd_index;
        auto f = co_await coroutine::as_future(
                seastar::parallel_for_each(std::move(nodes), [this, &cmd, cmd_index] (raft::server_id id) {
            return exec_direct_command_helper(id, cmd_index, cmd);
        }));

        if (f.failed()) {
            co_await coroutine::return_exception(std::runtime_error(
                ::format("raft topology: exec_global_command({}) failed with {}",
                    cmd.cmd, f.get_exception())));
        }
    };

    future<group0_guard> exec_global_command(
            group0_guard guard, const raft_topology_cmd& cmd,
            const std::unordered_set<raft::server_id>& exclude_nodes,
            drop_guard_and_retake drop_and_retake = drop_guard_and_retake::yes) {
        rtlogger.info("executing global topology command {}, excluded nodes: {}", cmd.cmd, exclude_nodes);
        auto nodes = boost::range::join(_topo_sm._topology.normal_nodes, _topo_sm._topology.transition_nodes)
            | std::views::filter([&cmd, &exclude_nodes] (const std::pair<const raft::server_id, replica_state>& n) {
                // We must send barrier and barrier_and_drain to the decommissioning node
                // as it might be accepting or coordinating requests.
                bool include_decommissioning_node = n.second.state == node_state::decommissioning
                        && (cmd.cmd == raft_topology_cmd::command::barrier || cmd.cmd == raft_topology_cmd::command::barrier_and_drain);
                return !exclude_nodes.contains(n.first) && (n.second.state == node_state::normal || include_decommissioning_node);
            })
            | std::views::keys;
        if (drop_and_retake) {
            release_guard(std::move(guard));
        }
        co_await exec_global_command_helper(std::move(nodes), cmd);
        if (drop_and_retake) {
            guard = co_await start_operation();
        }
        co_return guard;
    }

    std::unordered_set<raft::server_id> get_excluded_nodes(const topology_state_machine::topology_type& topo,
                raft::server_id id, const std::optional<topology_request>& req) const {
        return topo.get_excluded_nodes(id, req);
    }

    std::unordered_set<raft::server_id> get_excluded_nodes(const node_to_work_on& node) const {
        return node.topology->get_excluded_nodes(node.id, node.request);
    }

    future<node_to_work_on> exec_global_command(node_to_work_on&& node, const raft_topology_cmd& cmd) {
        auto guard = co_await exec_global_command(std::move(node.guard), cmd, get_excluded_nodes(node), drop_guard_and_retake::yes);
        co_return retake_node(std::move(guard), node.id);
    };

    future<group0_guard> remove_from_group0(group0_guard guard, const raft::server_id& id) {
        rtlogger.info("removing node {} from group 0 configuration...", id);
        release_guard(std::move(guard));
        co_await _voter_handler.on_node_removed(id, _as);
        co_await _group0.remove_from_raft_config(id);
        rtlogger.info("node {} removed from group 0 configuration", id);
        co_return co_await start_operation();
    }

    future<> step_down_as_nonvoter() {
        // Become a nonvoter which triggers a leader stepdown.
        co_await _voter_handler.on_node_removed(_raft.id(), _as);
        if (_raft.is_leader()) {
            co_await _raft.wait_for_state_change(&_as);
        }

        // throw term_changed_error so we leave the coordinator loop instead of trying another
        // read_barrier which may fail with an (harmless, but unnecessary and annoying) error
        // telling us we're not in the configuration anymore (we'll get removed by the new
        // coordinator)
        throw term_changed_error{};
    }

    struct bootstrapping_info {
        const std::unordered_set<token>& bootstrap_tokens;
        const replica_state& rs;
    };

    // Returns data for a new CDC generation in the form of mutations for the CDC_GENERATIONS_V3 table
    // and the generation's UUID.
    //
    // If there's a bootstrapping node, its tokens should be included in the new generation.
    // Pass them and a reference to the bootstrapping node's replica_state through `binfo`.
    future<std::pair<utils::UUID, utils::chunked_vector<mutation>>> prepare_new_cdc_generation_data(
            locator::token_metadata_ptr tmptr, const group0_guard& guard, std::optional<bootstrapping_info> binfo,
            noncopyable_function<std::pair<size_t, uint8_t>(locator::host_id)> get_sharding_info_for_host_id) {
        auto get_sharding_info = [&] (dht::token end) -> std::pair<size_t, uint8_t> {
            if (binfo && binfo->bootstrap_tokens.contains(end)) {
                return {binfo->rs.shard_count, binfo->rs.ignore_msb};
            } else {
                auto ep = tmptr->get_endpoint(end);
                if (!ep) {
                    // get_sharding_info is only called for bootstrap tokens
                    // or for tokens present in token_metadata
                    on_internal_error(rtlogger, ::format(
                        "make_new_cdc_generation_data: get_sharding_info:"
                        " can't find endpoint for token {}", end));
                }

                try {
                    return get_sharding_info_for_host_id(*ep);
                } catch (...) {
                    on_internal_error(rtlogger, ::format(
                        "make_new_cdc_generation_data: get_sharding_info:"
                        " can't get sharding info for node {}, owner of token {}."
                        " Reason: {}", *ep, end, std::current_exception()));
                }
            }
        };

        auto gen_uuid = guard.new_group0_state_id();
        auto gen_desc = cdc::make_new_generation_description(
            binfo ? binfo->bootstrap_tokens : std::unordered_set<token>{}, get_sharding_info, tmptr);
        auto gen_table_schema = _db.find_schema(
            db::system_keyspace::NAME, db::system_keyspace::CDC_GENERATIONS_V3);

        const size_t max_command_size = _raft.max_command_size();
        const size_t mutation_size_threshold = max_command_size / 2;
        auto gen_mutations = co_await cdc::get_cdc_generation_mutations_v3(
            gen_table_schema, gen_uuid, gen_desc, mutation_size_threshold, guard.write_timestamp());

        co_return std::pair{gen_uuid, std::move(gen_mutations)};
    }

    // Broadcasts all mutations returned from `prepare_new_cdc_generation_data` except the last one.
    // Each mutation is sent in separate raft command. It takes `group0_guard`, and if the number of mutations
    // is greater than one, the guard is dropped, and a new one is created and returned, otherwise the old one
    // will be returned. Commands are sent in parallel and unguarded (the guard used for sending the last mutation
    // will guarantee that the term hasn't been changed). Returns the generation's UUID, guard and last mutation,
    // which will be sent with additional topology data by the caller.
    //
    // If we send the last mutation in the `write_mutation` command, we would use a total of `n + 1` commands
    // instead of `n-1 + 1` (where `n` is the number of mutations), so it's better to send it in `topology_change`
    // (we need to send it after all `write_mutations`) with some small metadata.
    //
    // With the default commitlog segment size, `mutation_size_threshold` will be 4 MB. In large clusters e.g.
    // 100 nodes, 64 shards per node, 256 vnodes cdc generation data can reach the size of 30 MB, thus
    // there will be no more than 8 commands.
    //
    // In a multi-DC cluster with 100ms latencies between DCs, this operation should take about 200ms since we
    // send the commands concurrently, but even if the commands were replicated sequentially by Raft,
    // it should take no more than 1.6s which is incomparably smaller than bootstrapping operation
    // (bootstrapping is quick if there is no data in the cluster, but usually if one has 100 nodes they
    // have tons of data, so indeed streaming/repair will take much longer (hours/days)).
    future<std::tuple<utils::UUID, group0_guard, canonical_mutation>> prepare_and_broadcast_cdc_generation_data(
            locator::token_metadata_ptr tmptr, group0_guard guard, std::optional<bootstrapping_info> binfo) {

        auto get_sharding_info_for_host_id = [&] (locator::host_id ep) -> std::pair<size_t, uint8_t> {
            auto ptr = _topo_sm._topology.find(raft::server_id{ep.uuid()});
            if (!ptr) {
                throw std::runtime_error(::format(
                        "raft topology: prepare_and_broadcast_cdc_generation_data: get_sharding_info_for_host_id:"
                        " couldn't find node {} in topology", ep));
            }

            auto& rs = ptr->second;
            return {rs.shard_count, rs.ignore_msb};
        };

        co_return co_await prepare_and_broadcast_cdc_generation_data(tmptr, std::move(guard), binfo, get_sharding_info_for_host_id);
    }

    future<std::tuple<utils::UUID, group0_guard, canonical_mutation>> prepare_and_broadcast_cdc_generation_data(
            locator::token_metadata_ptr tmptr, group0_guard guard, std::optional<bootstrapping_info> binfo,
            noncopyable_function<std::pair<size_t, uint8_t>(locator::host_id)> get_sharding_info_for_host_id) {

        auto [gen_uuid, gen_mutations] = co_await prepare_new_cdc_generation_data(tmptr, guard, binfo, std::move(get_sharding_info_for_host_id));

        if (gen_mutations.empty()) {
            on_internal_error(rtlogger, "cdc_generation_data: gen_mutations is empty");
        }

        std::vector<canonical_mutation> updates{gen_mutations.begin(), gen_mutations.end()};

        if (updates.size() > 1) {
            release_guard(std::move(guard));

            co_await parallel_for_each(updates.begin(), std::prev(updates.end()), [this, gen_uuid = gen_uuid] (canonical_mutation& m) {
                auto const reason = format(
                    "insert CDC generation data (UUID: {}), part", gen_uuid);

                rtlogger.trace("do update {} reason {}", m, reason);
                write_mutations change{{std::move(m)}};
                group0_command g0_cmd = _group0.client().prepare_command(std::move(change), reason);
                return _group0.client().add_entry_unguarded(std::move(g0_cmd), &_as);
            });

            guard = co_await start_operation();
        }

        co_return std::tuple{gen_uuid, std::move(guard), std::move(updates.back())};
    }

    // Deletes obsolete CDC generations. These are the published generations that stopped operating
    // sufficiently long ago (see comment below).
    //
    // Appends necessary mutations to `updates` and updates the `reason` string.
    future<> clean_obsolete_cdc_generations(
            const group0_guard& guard,
            std::vector<canonical_mutation>& updates,
            sstring& reason) {
        const auto& committed_gens = _topo_sm._topology.committed_cdc_generations;
        if (committed_gens.empty()) {
            co_return;
        }

        // If some node can still accept a write to a CDC generation, we cannot delete this generation.
        // A request coordinator accepts a write to one of the previous generations (the ones that stopped
        // operating before `now`) if the write's timestamp is higher than `now - 5s` where `now` is
        // provided by the coordinator's local clock. So, we can safely delete a generation if it
        // stopped operating more than 5s ago on all nodes. But since their clocks can be desynchronized,
        // we have to account for that. We assume that the max clock difference is within `ring_delay`.
        // So we delete only generations that stopped operating more than `5s + ring_delay` ago.
        auto ts_upper_bound = db_clock::now() - (cdc::get_generation_leeway() + _ring_delay);
        utils::get_local_injector().inject("clean_obsolete_cdc_generations_change_ts_ub", [&] {
            ts_upper_bound = db_clock::now();
        });

        // Theoretically, some generations might not be published within `5s + ring_delay` after creation.
        // If that happens, we reduce `ts_upper_bound` to ensure they aren't deleted.
        if (!_topo_sm._topology.unpublished_cdc_generations.empty()) {
            auto first_unpublished_ts = _topo_sm._topology.unpublished_cdc_generations.front().ts;
            if (first_unpublished_ts < ts_upper_bound) {
                ts_upper_bound = first_unpublished_ts;
            }
        }

        std::optional<std::vector<cdc::generation_id_v2>::const_iterator> first_nonobsolete_gen_it;
        for (auto it = committed_gens.begin(); it != committed_gens.end() && it->ts <= ts_upper_bound; it++) {
            if (it + 1 == committed_gens.end() || (it + 1)->ts > ts_upper_bound) {
                first_nonobsolete_gen_it = it;
            }
        }
        if (!first_nonobsolete_gen_it || *first_nonobsolete_gen_it == committed_gens.begin()) {
            co_return;
        }

        auto mut_ts = guard.write_timestamp();

        // Insert a tombstone covering all obsolete generations.
        auto s = _db.find_schema(db::system_keyspace::NAME, db::system_keyspace::CDC_GENERATIONS_V3);
        auto id_upper_bound = (*first_nonobsolete_gen_it)->id;
        mutation m(s, partition_key::from_singular(*s, cdc::CDC_GENERATIONS_V3_KEY));
        auto range = query::clustering_range::make_ending_with({
                clustering_key_prefix::from_single_value(*s, timeuuid_type->decompose(id_upper_bound)), false});
        auto bv = bound_view::from_range(range);
        m.partition().apply_delete(*s, range_tombstone{bv.first, bv.second, tombstone{mut_ts, gc_clock::now()}});
        updates.push_back(canonical_mutation(m));

        std::vector<cdc::generation_id_v2> new_committed_gens(*first_nonobsolete_gen_it, committed_gens.end());
        topology_mutation_builder builder(guard.write_timestamp());
        builder.set_committed_cdc_generations(std::move(new_committed_gens));
        updates.push_back(builder.build());

        reason += ::format("deleted data of CDC generations with time UUID lower than {}", id_upper_bound);
    }

    // If there are some unpublished CDC generations, publishes the one with the oldest timestamp
    // to user-facing description tables.
    //
    // Appends necessary mutations to `updates` and updates the `reason` string.
    future<> publish_oldest_cdc_generation(
            const group0_guard& guard,
            std::vector<canonical_mutation>& updates,
            sstring& reason) {
        const auto& unpublished_gens = _topo_sm._topology.unpublished_cdc_generations;
        if (unpublished_gens.empty()) {
            co_return;
        }

        // The generation under index 0 is the oldest because unpublished_cdc_generations are sorted by timestamp.
        auto gen_id = unpublished_gens[0];

        auto gen_data = co_await _sys_ks.read_cdc_generation(gen_id.id);

        co_await _sys_dist_ks.local().create_cdc_desc(
                gen_id.ts, gen_data, { get_token_metadata().count_normal_token_owners() });

        std::vector<cdc::generation_id_v2> new_unpublished_gens(unpublished_gens.begin() + 1, unpublished_gens.end());
        topology_mutation_builder builder(guard.write_timestamp());
        builder.set_unpublished_cdc_generations(std::move(new_unpublished_gens));
        updates.push_back(builder.build());

        reason += ::format("published CDC generation with ID {}, ", gen_id);
    }

    // The background fiber of the topology coordinator that continually publishes committed yet unpublished
    // CDC generations. Every generation is published in a separate group 0 operation.
    //
    // It also continually cleans the obsolete CDC generation data.
    future<> cdc_generation_publisher_fiber() {
        rtlogger.debug("start CDC generation publisher fiber");

        while (!_as.abort_requested()) {
            co_await utils::get_local_injector().inject("cdc_generation_publisher_fiber", [] (auto& handler) -> future<> {
                rtlogger.info("CDC generation publisher fiber sleeps after injection");
                co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{5});
                rtlogger.info("CDC generation publisher fiber finishes sleeping after injection");
            }, false);

            bool sleep = false;
            try {
                auto guard = co_await start_operation();
                std::vector<canonical_mutation> updates;
                sstring reason;

                co_await publish_oldest_cdc_generation(guard, updates, reason);

                co_await clean_obsolete_cdc_generations(guard, updates, reason);

                if (!updates.empty()) {
                    co_await update_topology_state(std::move(guard), std::move(updates), std::move(reason));
                } else {
                    release_guard(std::move(guard));
                }

                if (_topo_sm._topology.unpublished_cdc_generations.empty()) {
                    // No CDC generations to publish. Wait until one appears or the topology coordinator aborts.
                    rtlogger.debug("CDC generation publisher fiber has nothing to do. Sleeping.");
                    co_await _topo_sm.event.when([&] () {
                        return !_topo_sm._topology.unpublished_cdc_generations.empty() || _as.abort_requested();
                    });
                    rtlogger.debug("CDC generation publisher fiber wakes up");
                }
            } catch (raft::request_aborted&) {
                rtlogger.debug("CDC generation publisher fiber aborted");
            } catch (seastar::abort_requested_exception&) {
                rtlogger.debug("CDC generation publisher fiber aborted");
            } catch (group0_concurrent_modification&) {
            } catch (term_changed_error&) {
                rtlogger.debug("CDC generation publisher fiber notices term change {} -> {}", _term, _raft.get_current_term());
            } catch (...) {
                rtlogger.error("CDC generation publisher fiber got error {}", std::current_exception());
                sleep = true;
            }
            if (sleep) {
                try {
                    co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
                } catch (...) {
                    rtlogger.debug("CDC generation publisher: sleep failed: {}", std::current_exception());
                }
            }
            co_await coroutine::maybe_yield();
        }
    }

    // If a node crashes after initiating gossip and before joining group0, it becomes orphan and
    // remains in gossip. This background fiber periodically checks for such nodes and purges those
    // entries from gossiper by committing the node as left in raft.
    future<> gossiper_orphan_remover_fiber() {
        rtlogger.debug("start gossiper orphan remover fiber");
        bool do_speedup_fiber = false;
        co_await utils::get_local_injector().inject("fast_orphan_removal_fiber", [&](auto& handler) -> future<> {
            do_speedup_fiber = true;
            // While testing, orphan ip is introduced, and its presence is captured. Wait is added before remover thread so that the presence of orphan ip is
            // confirmed and the difference of gossiper ips before and after this thread application can be easily asserted.
            return handler.wait_for_message(db::timeout_clock::now() + std::chrono::minutes(5));
        });
        while (!_as.abort_requested()) {
            try {
                auto guard = co_await start_operation();
                std::vector<canonical_mutation> updates;
                int32_t timeout = 60;
                co_await utils::get_local_injector().inject("speedup_orphan_removal", [&](auto& handler) -> future<> {
                    // Removes all unjoined nodes. Just for testing purposes.
                    timeout = 0;
                    co_return;
                });
                std::string reason = ::format("Ban the orphan nodes in group0. Orphan nodes HostId/IP:");
                _gossiper.for_each_endpoint_state([&](const gms::endpoint_state& eps) -> void {
                    // Since generation is in seconds unit, converting current time to seconds eases comparison computations.
                    auto current_timestamp =
                            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
                    auto generation = eps.get_heart_beat_state().get_generation().value();
                    auto host_id = eps.get_host_id();
                    if (current_timestamp - generation > timeout && !_topo_sm._topology.contains(raft::server_id{host_id.id}) && !_gossiper.is_alive(host_id)) {
                        topology_mutation_builder builder(guard.write_timestamp());
                        // This topology mutation moves a node to left state and bans it. Hence, the value of below fields are not useful.
                        // The dummy_value used for few fields indicates the trivialness of this row entry, and is used to detect this special case.
                        static constexpr uint32_t dummy_value = 0;
                        builder.with_node(raft::server_id{host_id.id})
                                .set("datacenter", get_application_state_gently(eps.get_application_state_map(), gms::application_state::DC))
                                .set("rack", get_application_state_gently(eps.get_application_state_map(), gms::application_state::RACK))
                                .set("release_version", get_application_state_gently(eps.get_application_state_map(), gms::application_state::RELEASE_VERSION))
                                .set("num_tokens", dummy_value)
                                .set("tokens_string", sstring{})
                                .set("shard_count", dummy_value)
                                .set("ignore_msb", dummy_value)
                                .set("request_id", dummy_value)
                                .set("cleanup_status", cleanup_status::clean)
                                .set("node_state", node_state::left);
                        reason.append(::format(" {}/{},", host_id, eps.get_ip()));
                        updates.push_back({builder.build()});
                    }
                });
                if (!updates.empty()) {
                    co_await update_topology_state(std::move(guard), std::move(updates), reason);
                }

            } catch (raft::request_aborted&) {
                rtlogger.debug("gossiper orphan remover fiber aborted");
            } catch (seastar::abort_requested_exception&) {
                rtlogger.debug("gossiper orphan remover fiber aborted");
            } catch (group0_concurrent_modification&) {
            } catch (term_changed_error&) {
                rtlogger.debug("gossiper orphan remover fiber notices term change {} -> {}", _term, _raft.get_current_term());
            } catch (...) {
                rtlogger.error("gossiper orphan remover fiber got error {}", std::current_exception());
            }
            try {
                co_await seastar::sleep_abortable(do_speedup_fiber ? std::chrono::milliseconds(1) : std::chrono::seconds(10), _as);
            } catch (...) {
                rtlogger.debug("gossiper orphan remover: sleep failed: {}", std::current_exception());
            }
            co_await coroutine::maybe_yield();
        }
    }

    future<> group0_voter_refresher_fiber() {
        rtlogger.debug("start group0 members refresh fiber");

        // Skip waiting for the event in the first iteration to start the first voter refresh immediately.
        //
        // This is necessary to ensure we update the voters e.g. in case of a topology coordinator change
        // (the previous topology coordinator might have died in which case we'll need to remove it from
        // the voters).
        bool skip_wait = true;

        while (!_as.abort_requested()) {
            try {
                // only wait for the event in case we are not retrying the operation
                if (!skip_wait) {
                    co_await await_event();
                    // Introduce a brief delay to potentially batch multiple changes that occur in close succession
                    co_await seastar::sleep_abortable(std::chrono::milliseconds{100}, _as);
                }
                skip_wait = false;

                if (!_feature_service.group0_limited_voters) {
                    rtlogger.debug("group0 voters refresh fiber iteration skipped because the feature is disabled");
                    continue;
                }

                co_await _voter_handler.refresh(_as);
            } catch (raft::request_aborted&) {
                rtlogger.debug("group0 voters refresh fiber aborted");
                break; // exit the loop immediately (not waiting for the abort source to be set)
            } catch (seastar::abort_requested_exception&) {
                rtlogger.debug("group0 voters refresh fiber aborted");
                break; // exit the loop immediately (not waiting for the abort source to be set)
            } catch (group0_concurrent_modification&) {
                rtlogger.debug("group0 voters refresh fiber notices concurrent modification, retrying...");
                skip_wait = true;
            } catch (term_changed_error&) {
                rtlogger.debug("group0 voters refresh fiber notices term change {} -> {}", _term, _raft.get_current_term());
                break; // exit the loop immediately (term change means we're not the coordinator anymore)
            } catch (...) {
                rtlogger.error("group0 voters refresh fiber got error {}", std::current_exception());
            }
        }
    }

    // Precondition: there is no node request and no ongoing topology transition
    // (checked under the guard we're holding).
    future<> handle_global_request(group0_guard guard) {
        switch (_topo_sm._topology.global_request.value()) {
        case global_topology_request::new_cdc_generation: {
            rtlogger.info("new CDC generation requested");

            auto tmptr = get_token_metadata_ptr();
            auto [gen_uuid, guard_, mutation] = co_await prepare_and_broadcast_cdc_generation_data(tmptr, std::move(guard), std::nullopt);
            guard = std::move(guard_);

            topology_mutation_builder builder(guard.write_timestamp());
            // We don't delete the request now, but only after the generation is committed. If we deleted
            // the request now and received another new_cdc_generation request later, but before committing
            // the new generation, the second request would also create a new generation. Deleting requests
            // after the generation is committed prevents this from happening. The second request would have
            // no effect - it would just overwrite the first request.
            builder.set_transition_state(topology::transition_state::commit_cdc_generation)
                   .set_new_cdc_generation_data_uuid(gen_uuid);
            auto reason = ::format(
                "insert CDC generation data (UUID: {})", gen_uuid);
            co_await update_topology_state(std::move(guard), {std::move(mutation), builder.build()}, reason);
        }
        break;
        case global_topology_request::cleanup:
            co_await start_cleanup_on_dirty_nodes(std::move(guard), true);
            break;
        case global_topology_request::keyspace_rf_change: {
            rtlogger.info("keyspace_rf_change requested");
            sstring ks_name = *_topo_sm._topology.new_keyspace_rf_change_ks_name;
            std::unordered_map<sstring, sstring> saved_ks_props = *_topo_sm._topology.new_keyspace_rf_change_data;
            cql3::statements::ks_prop_defs new_ks_props{std::map<sstring, sstring>{saved_ks_props.begin(), saved_ks_props.end()}};

            auto repl_opts = new_ks_props.get_replication_options();
            repl_opts.erase(cql3::statements::ks_prop_defs::REPLICATION_STRATEGY_CLASS_KEY);
            utils::UUID req_uuid = *_topo_sm._topology.global_request_id;
            std::vector<canonical_mutation> updates;
            sstring error;
            if (_db.has_keyspace(ks_name)) {
                auto& ks = _db.find_keyspace(ks_name);
                auto tmptr = get_token_metadata_ptr();
                size_t unimportant_init_tablet_count = 2; // must be a power of 2
                locator::tablet_map new_tablet_map{unimportant_init_tablet_count};

                auto tables_with_mvs = ks.metadata()->tables();
                auto views = ks.metadata()->views();
                tables_with_mvs.insert(tables_with_mvs.end(), views.begin(), views.end());
                for (const auto& table_or_mv : tables_with_mvs) {
                    try {
                        locator::tablet_map old_tablets = tmptr->tablets().get_tablet_map(table_or_mv->id());
                        locator::replication_strategy_params params{repl_opts, old_tablets.tablet_count()};
                        auto new_strategy = locator::abstract_replication_strategy::create_replication_strategy("NetworkTopologyStrategy", params);
                        new_tablet_map = co_await new_strategy->maybe_as_tablet_aware()->reallocate_tablets(table_or_mv, tmptr, old_tablets);
                    } catch (const std::exception& e) {
                        error = e.what();
                        rtlogger.error("Couldn't process global_topology_request::keyspace_rf_change, error: {},"
                                       "desired new ks opts: {}", error, new_ks_props.get_replication_options());
                        updates.clear(); // remove all tablets mutations ...
                        break;           // ... and only create mutations deleting the global req
                    }

                    replica::tablet_mutation_builder tablet_mutation_builder(guard.write_timestamp(), table_or_mv->id());
                    co_await new_tablet_map.for_each_tablet([&](locator::tablet_id tablet_id, const locator::tablet_info& tablet_info) -> future<> {
                        auto last_token = new_tablet_map.get_last_token(tablet_id);
                        updates.emplace_back(co_await make_canonical_mutation_gently(
                                replica::tablet_mutation_builder(guard.write_timestamp(), table_or_mv->id())
                                        .set_new_replicas(last_token, tablet_info.replicas)
                                        .set_stage(last_token, locator::tablet_transition_stage::allow_write_both_read_old)
                                        .set_transition(last_token, locator::choose_rebuild_transition_kind(_feature_service))
                                        .build()
                        ));
                        co_await coroutine::maybe_yield();
                    });
                }
            } else {
                error = "Can't ALTER keyspace " + ks_name + ", keyspace doesn't exist";
            }

            updates.push_back(canonical_mutation(topology_mutation_builder(guard.write_timestamp())
                                                         .set_transition_state(topology::transition_state::tablet_migration)
                                                         .set_version(_topo_sm._topology.version + 1)
                                                         .del_global_topology_request()
                                                         .del_global_topology_request_id()
                                                         .build()));
            updates.push_back(canonical_mutation(topology_request_tracking_mutation_builder(req_uuid)
                                                         .done(error)
                                                         .build()));
            if (error.empty()) {
                const sstring strategy_name = "NetworkTopologyStrategy";
                auto ks_md = keyspace_metadata::new_keyspace(ks_name, strategy_name, repl_opts,
                                                             new_ks_props.get_initial_tablets(std::nullopt),
                                                             new_ks_props.get_durable_writes(), new_ks_props.get_storage_options());
                auto schema_muts = prepare_keyspace_update_announcement(_db, ks_md, guard.write_timestamp());
                for (auto& m: schema_muts) {
                    updates.emplace_back(m);
                }
            }

            sstring reason = seastar::format("ALTER tablets KEYSPACE called with options: {}", saved_ks_props);
            rtlogger.trace("do update {} reason {}", updates, reason);
            mixed_change change{std::move(updates)};
            group0_command g0_cmd = _group0.client().prepare_command(std::move(change), guard, reason);
            co_await utils::get_local_injector().inject("wait-before-committing-rf-change-event", utils::wait_for_message(30s));
            co_await _group0.client().add_entry(std::move(g0_cmd), std::move(guard), _as);
        }
        break;
        case global_topology_request::truncate_table: {
            rtlogger.info("TRUNCATE TABLE requested");
            std::vector<canonical_mutation> updates;
            updates.push_back(topology_mutation_builder(guard.write_timestamp())
                                .set_transition_state(topology::transition_state::truncate_table)
                                .set_session(session_id(_topo_sm._topology.global_request_id.value()))
                                .build());
            co_await update_topology_state(std::move(guard), std::move(updates), "TRUNCATE TABLE requested");
        }
        break;
        }
    }

    // Preconditions:
    // - There are no topology operations in progress
    // - `features_to_enable` represents a set of features that are currently
    //   marked as supported by all normal nodes and it is not empty
    future<> enable_features(group0_guard guard, std::set<sstring> features_to_enable) {
        if (!_topo_sm._topology.transition_nodes.empty()) {
            on_internal_error(rtlogger,
                    "topology coordinator attempted to enable features even though there is"
                    " a topology operations in progress");
        }

        if (utils::get_local_injector().enter("raft_topology_suppress_enabling_features")) {
            // Prevent enabling features while the injection is enabled.
            // The topology coordinator will detect in the next iteration
            // that there are still some cluster features to enable and will
            // reach this place again. In order not to spin in a loop, sleep
            // for a short while.
            co_await sleep(std::chrono::milliseconds(100));
            co_return;
        }

        // If we are here, then we noticed that all normal nodes support some
        // features that are not enabled yet. Perform a global barrier to make
        // sure that:
        //
        // 1. All normal nodes saw (and persisted) a view of the system.topology
        //    table that is equal to what the topology coordinator sees (or newer,
        //    but in that case updating the topology state will fail),
        // 2. None of the normal nodes is restarting at the moment and trying to
        //    downgrade (this is done by a special check in the barrier handler).
        //
        // It's sufficient to only include normal nodes because:
        //
        // - There are no transitioning nodes due to the precondition,
        // - New and left nodes are not part of group 0.
        //
        // After we get a successful confirmation from each normal node, we have
        // a guarantee that they won't attempt to revoke support for those
        // features. That's because we do not allow nodes to boot without
        // a feature that is supported by all nodes in the cluster, even if
        // the feature is not enabled yet.
        guard = co_await exec_global_command(std::move(guard),
                raft_topology_cmd{raft_topology_cmd::command::barrier},
                {_raft.id()},
                drop_guard_and_retake::no);

        topology_mutation_builder builder(guard.write_timestamp());
        builder.add_enabled_features(features_to_enable);
        auto reason = ::format("enabling features: {}", features_to_enable);
        co_await update_topology_state(std::move(guard), {builder.build()}, reason);

        rtlogger.info("enabled features: {}", features_to_enable);
    }

    future<group0_guard> global_token_metadata_barrier(group0_guard&& guard, std::unordered_set<raft::server_id> exclude_nodes = {}) {
        auto version = _topo_sm._topology.version;
        bool drain_failed = false;
        try {
            guard = co_await exec_global_command(std::move(guard), raft_topology_cmd::command::barrier_and_drain, exclude_nodes, drop_guard_and_retake::yes);
        } catch (...) {
            rtlogger.warn("drain rpc failed, proceed to fence old writes: {}", std::current_exception());
            drain_failed = true;
        }
        if (drain_failed) {
            guard = co_await start_operation();
        }
        topology_mutation_builder builder(guard.write_timestamp());
        // Other nodes are guaranteed to be drained on success only up to the version which was current
        // prior to barrier_and_drain RPCs were sent.
        builder.set_fence_version(version);
        auto reason = ::format("advance fence version to {}", version);
        co_await update_topology_state(std::move(guard), {builder.build()}, reason);
        guard = co_await start_operation();
        if (drain_failed) {
            // if drain failed need to wait for fence to be active on all nodes
            co_return co_await exec_global_command(std::move(guard), raft_topology_cmd::command::barrier, exclude_nodes, drop_guard_and_retake::yes);
        } else {
            co_return std::move(guard);
        }
    }

    future<group0_guard> global_tablet_token_metadata_barrier(group0_guard guard) {
        // FIXME: Don't require all nodes to be up, only tablet replicas.
        return global_token_metadata_barrier(std::move(guard), _topo_sm._topology.get_excluded_nodes());
    }

    // Represents a two-state state machine which changes monotonically
    // from "not executed" to "executed successfully". This state
    // machine is transient, lives only on this coordinator.
    // The transition is achieved by execution of an idempotent async
    // operation which is tracked by a future. Even though the async
    // action is idempotent, it is costly, so we want to avoid
    // re-executing it if it was already started by this coordinator,
    // that's why we track it.
    using background_action_holder = std::optional<future<>>;

    // Transient state of tablet migration which lives on this coordinator.
    // It is guaranteed to die when migration is finished.
    // Next migration of the same tablet is guaranteed to use a different instance.
    struct tablet_migration_state {
        background_action_holder streaming;
        background_action_holder rebuild_repair;
        background_action_holder cleanup;
        background_action_holder repair;
        std::unordered_map<locator::tablet_transition_stage, background_action_holder> barriers;
        // Record the repair_time returned by the repair_tablet rpc call
        db_clock::time_point repair_time;
    };

    std::unordered_map<locator::global_tablet_id, tablet_migration_state> _tablets;

    // Set to true when any action started on behalf of a background_action_holder
    // for any tablet finishes, or fails and needs to be restarted.
    bool _tablets_ready = false;

    seastar::named_gate _async_gate;

    bool action_failed(background_action_holder& holder) const {
        return holder && holder->failed();
    }

    // This function drives background_action_holder towards "executed successfully"
    // by starting the action if it is not already running or if the previous instance
    // of the action failed. If the action is already running, it does nothing.
    // Returns true iff background_action_holder reached the "executed successfully" state.
    bool advance_in_background(locator::global_tablet_id gid, background_action_holder& holder, const char* name,
                               std::function<future<>()> action) {
        if (!holder || holder->failed()) {
            if (action_failed(holder)) {
                // Prevent warnings about abandoned failed future. Logged below.
                holder->ignore_ready_future();
            }
            holder = futurize_invoke(action).then_wrapped([this, gid, name] (future<> f) {
                if (f.failed()) {
                    auto ep = f.get_exception();
                    rtlogger.warn("{} for tablet {} failed: {}", name, gid, ep);
                    return seastar::sleep_abortable(std::chrono::seconds(1), _as).then([ep] () mutable {
                        std::rethrow_exception(ep);
                    });
                }
                return f;
            }).finally([this, g = _async_gate.hold(), gid, name] () noexcept {
                rtlogger.debug("{} for tablet {} resolved.", name, gid);
                _tablets_ready = true;
                _topo_sm.event.broadcast();
            });
            return false;
        }

        if (!holder->available()) {
            rtlogger.debug("Tablet {} still doing {}", gid, name);
            return false;
        }

        return true;
    }

    future<> for_each_tablet_transition(std::function<void(const locator::tablet_map&,
                                                           schema_ptr,
                                                           locator::global_tablet_id,
                                                           const locator::tablet_transition_info&)> func) {
        auto tm = get_token_metadata_ptr();
        for (auto&& [table, tmap] : tm->tablets().all_tables()) {
            co_await coroutine::maybe_yield();
            auto s = _db.find_schema(table);
            for (auto&& [tablet, trinfo]: tmap->transitions()) {
                co_await coroutine::maybe_yield();
                auto gid = locator::global_tablet_id {table, tablet};
                func(*tmap, s, gid, trinfo);
            }
        }
    }

    bool is_excluded(raft::server_id server_id) const {
        return _topo_sm._topology.get_excluded_nodes().contains(server_id);
    }

    void generate_migration_update(std::vector<canonical_mutation>& out, const group0_guard& guard, const tablet_migration_info& mig) {
        const auto& tmap = get_token_metadata_ptr()->tablets().get_tablet_map(mig.tablet.table);
        auto last_token = tmap.get_last_token(mig.tablet.tablet);
        if (tmap.get_tablet_transition_info(mig.tablet.tablet)) {
            rtlogger.warn("Tablet already in transition, ignoring migration: {}", mig);
            return;
        }
        auto migration_task_info = mig.kind == locator::tablet_transition_kind::migration ? locator::tablet_task_info::make_migration_request()
            : locator::tablet_task_info::make_intranode_migration_request();
        migration_task_info.sched_nr++;
        migration_task_info.sched_time = db_clock::now();
        out.emplace_back(
            replica::tablet_mutation_builder(guard.write_timestamp(), mig.tablet.table)
                .set_new_replicas(last_token, locator::get_new_replicas(tmap.get_tablet_info(mig.tablet.tablet), mig))
                .set_stage(last_token, locator::tablet_transition_stage::allow_write_both_read_old)
                .set_transition(last_token, mig.kind)
                .set_migration_task_info(last_token, std::move(migration_task_info), _feature_service)
                .build());
    }

    void generate_repair_update(std::vector<canonical_mutation>& out, const group0_guard& guard, const locator::global_tablet_id& gid, db_clock::time_point sched_time) {
        auto& tmap = get_token_metadata_ptr()->tablets().get_tablet_map(gid.table);
        auto last_token = tmap.get_last_token(gid.tablet);
        if (tmap.get_tablet_transition_info(gid.tablet)) {
            rtlogger.warn("Tablet already in transition, ignoring repair: {}", gid);
            return;
        }
        auto& info = tmap.get_tablet_info(gid.tablet);
        auto repair_task_info = info.repair_task_info;
        if (!repair_task_info.is_user_repair_request()) {
            repair_task_info = locator::tablet_task_info::make_auto_repair_request();
        }
        repair_task_info.sched_nr++;
        repair_task_info.sched_time = db_clock::now();
        out.emplace_back(
            replica::tablet_mutation_builder(guard.write_timestamp(), gid.table)
                .set_new_replicas(last_token, tmap.get_tablet_info(gid.tablet).replicas)
                .set_stage(last_token, locator::tablet_transition_stage::repair)
                .set_transition(last_token, locator::tablet_transition_kind::repair)
                .set_repair_task_info(last_token, repair_task_info)
                .set_session(last_token, session_id(utils::UUID_gen::get_time_UUID()))
                .build());
    }

    void generate_resize_update(std::vector<canonical_mutation>& out, const group0_guard& guard, table_id table_id, locator::resize_decision resize_decision) {
            // FIXME: indent.
            auto s = _db.find_schema(table_id);
            const auto& tmap = get_token_metadata_ptr()->tablets().get_tablet_map(table_id);
            // Sequence number is monotonically increasing, globally. Therefore, it can be used to identify a decision.
            resize_decision.sequence_number = tmap.resize_decision().next_sequence_number();
            rtlogger.debug("Generating resize decision for table {} of type {} and sequence number {}",
                           table_id, resize_decision.type_name(), resize_decision.sequence_number);
            out.emplace_back(
                replica::tablet_mutation_builder(guard.write_timestamp(), table_id)
                    .set_resize_decision(std::move(resize_decision), _feature_service)
                    .build());
    }

    future<> generate_migration_updates(std::vector<canonical_mutation>& out, const group0_guard& guard, const migration_plan& plan) {
        if (plan.resize_plan().finalize_resize.empty() || plan.has_nodes_to_drain()) {
            // schedule tablet migration only if there are no pending resize finalisations or if the node is draining.
            for (const tablet_migration_info& mig : plan.migrations()) {
                co_await coroutine::maybe_yield();
                generate_migration_update(out, guard, mig);
            }
        }

        auto sched_time = db_clock::now();
        for (const auto& gid : plan.repair_plan().repairs()) {
            co_await coroutine::maybe_yield();
            generate_repair_update(out, guard, gid, sched_time);
        }

        for (auto [table_id, resize_decision] : plan.resize_plan().resize) {
            co_await coroutine::maybe_yield();
            generate_resize_update(out, guard, table_id, resize_decision);
        }
    }

    // When "drain" is true, we migrate tablets only as long as there are nodes to drain
    // and then change the transition state to write_both_read_old. Also, while draining,
    // we ignore pending topology requests which normally interrupt load balancing.
    // When "drain" is false, we do regular load balancing.
    future<> handle_tablet_migration(group0_guard guard, bool drain) {
        // This step acts like a pump which advances state machines of individual tablets,
        // batching barriers and group0 updates.
        // If progress cannot be made, e.g. because all transitions are streaming, we block
        // and wait for notification.

        rtlogger.debug("handle_tablet_migration()");
        std::vector<canonical_mutation> updates;
        bool needs_barrier = false;
        bool has_transitions = false;

        shared_promise barrier;
        auto fail_barrier = seastar::defer([&] {
            if (needs_barrier) {
                barrier.set_exception(seastar::broken_promise());
            }
        });

        _tablets_ready = false;
        co_await for_each_tablet_transition([&] (const locator::tablet_map& tmap,
                                                 schema_ptr s,
                                                 locator::global_tablet_id gid,
                                                 const locator::tablet_transition_info& trinfo) {
            has_transitions = true;
            auto last_token = tmap.get_last_token(gid.tablet);
            auto& tablet_state = _tablets[gid];
            table_id table = s->id();

            auto get_mutation_builder = [&] () {
                return replica::tablet_mutation_builder(guard.write_timestamp(), table);
            };

            auto transition_to = [&] (locator::tablet_transition_stage stage) {
                rtlogger.debug("Will set tablet {} stage to {}", gid, stage);
                updates.emplace_back(get_mutation_builder()
                        .set_stage(last_token, stage)
                        .build());
            };

            auto do_barrier = [&] {
                return advance_in_background(gid, tablet_state.barriers[trinfo.stage], "barrier", [&] {
                    needs_barrier = true;
                    return barrier.get_shared_future();
                });
            };

            auto transition_to_with_barrier = [&] (locator::tablet_transition_stage stage) {
                if (do_barrier()) {
                    transition_to(stage);
                }
            };

            auto check_excluded_replicas = [&] {
                    auto tsi = get_migration_streaming_info(get_token_metadata().get_topology(), tmap.get_tablet_info(gid.tablet), trinfo);
                    for (auto r : tsi.read_from) {
                        if (is_excluded(raft::server_id(r.host.uuid()))) {
                            rtlogger.debug("Aborting streaming of {} because read-from {} is marked as ignored", gid, r);
                            return true;
                        }
                    }
                    for (auto r : tsi.written_to) {
                        if (is_excluded(raft::server_id(r.host.uuid()))) {
                            rtlogger.debug("Aborting streaming of {} because written-to {} is marked as ignored", gid, r);
                            return true;
                        }
                    }
                    return false;
            };

            switch (trinfo.stage) {
                case locator::tablet_transition_stage::allow_write_both_read_old:
                    if (action_failed(tablet_state.barriers[trinfo.stage])) {
                        if (check_excluded_replicas()) {
                            transition_to_with_barrier(locator::tablet_transition_stage::cleanup_target);
                            break;
                        }
                    }
                    if (do_barrier()) {
                        rtlogger.debug("Will set tablet {} stage to {}", gid, locator::tablet_transition_stage::write_both_read_old);
                        updates.emplace_back(get_mutation_builder()
                            .set_stage(last_token, locator::tablet_transition_stage::write_both_read_old)
                            // Create session a bit earlier to avoid adding barrier
                            // to the streaming stage to create sessions on replicas.
                            .set_session(last_token, session_id(utils::UUID_gen::get_time_UUID()))
                            .build());
                    }
                    break;
                case locator::tablet_transition_stage::write_both_read_old:
                    if (action_failed(tablet_state.barriers[trinfo.stage])) {
                        if (check_excluded_replicas()) {
                            transition_to_with_barrier(locator::tablet_transition_stage::cleanup_target);
                            break;
                        }
                    }
                    if (trinfo.transition == locator::tablet_transition_kind::rebuild_v2) {
                        transition_to_with_barrier(locator::tablet_transition_stage::rebuild_repair);
                    } else {
                        transition_to_with_barrier(locator::tablet_transition_stage::streaming);
                    }
                    break;
                case locator::tablet_transition_stage::rebuild_repair: {
                    if (action_failed(tablet_state.rebuild_repair)) {
                        bool fail = utils::get_local_injector().enter("rebuild_repair_stage_fail");
                        if (fail || check_excluded_replicas()) {
                            if (do_barrier()) {
                                rtlogger.debug("Will set tablet {} stage to {}", gid, locator::tablet_transition_stage::cleanup_target);
                                updates.emplace_back(get_mutation_builder()
                                        .set_stage(last_token, locator::tablet_transition_stage::cleanup_target)
                                        .del_session(last_token)
                                        .build());
                            }
                            break;
                        }
                    }

                    if (advance_in_background(gid, tablet_state.rebuild_repair, "rebuild_repair", [&] {
                        utils::get_local_injector().inject("rebuild_repair_stage_fail",
                            [] { throw std::runtime_error("rebuild_repair failed due to error injection"); });
                        if (!trinfo.pending_replica) {
                            rtlogger.info("Skipped tablet rebuild repair of {} as no pending replica found", gid);
                            return make_ready_future<>();
                        }
                        auto tsi = get_migration_streaming_info(get_token_metadata().get_topology(), tmap.get_tablet_info(gid.tablet), trinfo);
                        if (tsi.read_from.empty()) {
                            rtlogger.info("Skipped tablet rebuild repair of {} as no tablet replica was found", gid);
                            return make_ready_future<>();
                        }
                        auto dst = locator::maybe_get_primary_replica(gid.tablet, {tsi.read_from.begin(), tsi.read_from.end()}, [] (const auto& tr) { return true; }).value().host;
                        rtlogger.info("Initiating repair phase of tablet rebuild host={} tablet={}", dst, gid);
                        return ser::storage_service_rpc_verbs::send_tablet_repair(&_messaging,
                                dst, _as, raft::server_id(dst.uuid()), gid).discard_result();
                    })) {
                        rtlogger.debug("Will set tablet {} stage to {}", gid, locator::tablet_transition_stage::streaming);
                        updates.emplace_back(get_mutation_builder()
                            .set_stage(last_token, locator::tablet_transition_stage::streaming)
                            .build());
                    }
                }
                    break;
                // The state "streaming" is needed to ensure that stale stream_tablet() RPC doesn't
                // get admitted before global_tablet_token_metadata_barrier() is finished for earlier
                // stage in case of coordinator failover.
                case locator::tablet_transition_stage::streaming: {
                    if (drain) {
                        utils::get_local_injector().inject("stream_tablet_fail_on_drain",
                                        [] { throw std::runtime_error("stream_tablet failed due to error injection"); });
                    }

                    if (action_failed(tablet_state.streaming)) {
                        bool cleanup = utils::get_local_injector().enter("stream_tablet_move_to_cleanup");
                        if (cleanup || check_excluded_replicas()) {
                            if (do_barrier()) {
                                rtlogger.debug("Will set tablet {} stage to {}", gid, locator::tablet_transition_stage::cleanup_target);
                                updates.emplace_back(get_mutation_builder()
                                        .set_stage(last_token, locator::tablet_transition_stage::cleanup_target)
                                        .del_session(last_token)
                                        .build());
                            }
                            break;
                        }
                    }

                    bool wait = utils::get_local_injector().enter("stream_tablet_wait");
                    if (!wait && advance_in_background(gid, tablet_state.streaming, "streaming", [&] {
                        utils::get_local_injector().inject("stream_tablet_move_to_cleanup",
                                        [] { throw std::runtime_error("stream_tablet failed due to error injection"); });

                        if (!trinfo.pending_replica) {
                            rtlogger.info("Skipped tablet streaming ({}) of {} as no pending replica found", trinfo.transition, gid);
                            return make_ready_future<>();
                        }
                        rtlogger.info("Initiating tablet streaming ({}) of {} to {}", trinfo.transition, gid, *trinfo.pending_replica);
                        auto dst = trinfo.pending_replica->host;
                        return ser::storage_service_rpc_verbs::send_tablet_stream_data(&_messaging,
                                   dst, _as, raft::server_id(dst.uuid()), gid);
                    })) {
                        rtlogger.debug("Will set tablet {} stage to {}", gid, locator::tablet_transition_stage::write_both_read_new);
                        updates.emplace_back(get_mutation_builder()
                            .set_stage(last_token, locator::tablet_transition_stage::write_both_read_new)
                            .del_session(last_token)
                            .build());
                    }
                }
                    break;
                case locator::tablet_transition_stage::write_both_read_new: {
                    utils::get_local_injector().inject("crash-in-tablet-write-both-read-new", [] {
                        rtlogger.info("crash-in-tablet-write-both-read-new hit, killing the node");
                        _exit(1);
                    });

                    auto next_stage = locator::tablet_transition_stage::use_new;
                    if (action_failed(tablet_state.barriers[trinfo.stage])) {
                        auto& tinfo = tmap.get_tablet_info(gid.tablet);
                        unsigned excluded_old = 0;
                        for (auto r : tinfo.replicas) {
                            if (is_excluded(raft::server_id(r.host.uuid()))) {
                                excluded_old++;
                            }
                        }
                        unsigned excluded_new = 0;
                        for (auto r : trinfo.next) {
                            if (is_excluded(raft::server_id(r.host.uuid()))) {
                                excluded_new++;
                            }
                        }
                        // Cannot revert if this is intra-node migration.
                        // We will lose data if the migrating node restarted, in which case the leaving replica
                        // doesn't contain writes anymore as sstables are attached only on the pending replica.
                        // Luckily, this we never reach this condition since excluded_new cannot be larger
                        // than excluded_old for intra-node migration.
                        if (excluded_new > excluded_old && trinfo.transition != locator::tablet_transition_kind::intranode_migration) {
                            rtlogger.debug("During {} stage of {} {} new nodes and {} old nodes were excluded", trinfo.stage, gid, excluded_new, excluded_old);
                            next_stage = locator::tablet_transition_stage::cleanup_target;
                        }
                    }
                    transition_to_with_barrier(next_stage);
                }
                    break;
                case locator::tablet_transition_stage::use_new:
                    transition_to_with_barrier(locator::tablet_transition_stage::cleanup);
                    break;
                case locator::tablet_transition_stage::cleanup: {
                    bool wait = utils::get_local_injector().enter("cleanup_tablet_wait");
                    if (!wait && advance_in_background(gid, tablet_state.cleanup, "cleanup", [&] {
                        auto maybe_dst = locator::get_leaving_replica(tmap.get_tablet_info(gid.tablet), trinfo);
                        if (!maybe_dst) {
                            rtlogger.info("Tablet cleanup of {} skipped because no replicas leaving", gid);
                            return make_ready_future<>();
                        }
                        locator::tablet_replica& dst = *maybe_dst;
                        if (is_excluded(raft::server_id(dst.host.uuid()))) {
                            rtlogger.info("Tablet cleanup of {} on {} skipped because node is excluded", gid, dst);
                            return make_ready_future<>();
                        }
                        rtlogger.info("Initiating tablet cleanup of {} on {}", gid, dst);
                        return ser::storage_service_rpc_verbs::send_tablet_cleanup(&_messaging,
                                                                                   dst.host, _as, raft::server_id(dst.host.uuid()), gid);
                    })) {
                        transition_to(locator::tablet_transition_stage::end_migration);
                    }
                }
                    break;
                case locator::tablet_transition_stage::cleanup_target:
                    if (advance_in_background(gid, tablet_state.cleanup, "cleanup_target", [&] {
                        if (!trinfo.pending_replica) {
                            rtlogger.info("Tablet cleanup of {} skipped because no replicas pending", gid);
                            return make_ready_future<>();
                        }
                        locator::tablet_replica dst = *trinfo.pending_replica;
                        if (is_excluded(raft::server_id(dst.host.uuid()))) {
                            rtlogger.info("Tablet cleanup of {} on {} skipped because node is excluded and doesn't need to revert migration", gid, dst);
                            return make_ready_future<>();
                        }
                        rtlogger.info("Initiating tablet cleanup of {} on {} to revert migration", gid, dst);
                        return ser::storage_service_rpc_verbs::send_tablet_cleanup(&_messaging,
                                                                                   dst.host, _as, raft::server_id(dst.host.uuid()), gid);
                    })) {
                        transition_to(locator::tablet_transition_stage::revert_migration);
                    }
                    break;
                case locator::tablet_transition_stage::revert_migration:
                    // Need a separate stage and a barrier after cleanup RPC to cut off stale RPCs.
                    // See do_tablet_operation() doc.
                    if (do_barrier()) {
                        _tablets.erase(gid);
                        updates.emplace_back(get_mutation_builder()
                                .del_transition(last_token)
                                .del_migration_task_info(last_token, _feature_service)
                                .build());
                    }
                    break;
                case locator::tablet_transition_stage::end_migration: {
                    // Need a separate stage and a barrier after cleanup RPC to cut off stale RPCs.
                    // See do_tablet_operation() doc.
                    bool defer_transition = utils::get_local_injector().enter("handle_tablet_migration_end_migration");
                    if (!defer_transition && do_barrier()) {
                        _tablets.erase(gid);
                        updates.emplace_back(get_mutation_builder()
                                .del_transition(last_token)
                                .set_replicas(last_token, trinfo.next)
                                .del_migration_task_info(last_token, _feature_service)
                                .build());
                    }
                }
                    break;
                case locator::tablet_transition_stage::repair: {
                    bool fail_repair = utils::get_local_injector().enter("handle_tablet_migration_repair_fail");
                    if (fail_repair || action_failed(tablet_state.repair)) {
                        if (do_barrier()) {
                            updates.emplace_back(get_mutation_builder()
                                    .set_stage(last_token, locator::tablet_transition_stage::end_repair)
                                    .del_session(last_token)
                                    .build());
                        }
                        break;
                    }
                    if (advance_in_background(gid, tablet_state.repair, "repair", [&] () -> future<> {
                        auto& tinfo = tmap.get_tablet_info(gid.tablet);
                        bool valid = tinfo.repair_task_info.is_valid();
                        if (!valid) {
                            rtlogger.info("Skipping tablet repair for tablet={} which is cancelled by user", gid);
                            co_return;
                        }
                        auto sched_time = tinfo.repair_task_info.sched_time;
                        auto tablet = gid;
                        auto hosts_filter = tinfo.repair_task_info.repair_hosts_filter;
                        auto dcs_filter = tinfo.repair_task_info.repair_dcs_filter;
                        const auto& topo = _db.get_token_metadata().get_topology();
                        locator::host_id dst;
                        if (hosts_filter.empty() && dcs_filter.empty()) {
                            auto primary = tmap.get_primary_replica(gid.tablet);
                            dst = primary.host;
                        } else {
                            auto dst_opt = tmap.maybe_get_selected_replica(gid.tablet, topo, tinfo.repair_task_info);
                            if (!dst_opt) {
                                co_return;
                            }
                            dst = dst_opt.value().host;
                        }
                        rtlogger.info("Initiating tablet repair host={} tablet={}", dst, gid);
                        auto res = co_await ser::storage_service_rpc_verbs::send_tablet_repair(&_messaging,
                                dst, _as, raft::server_id(dst.uuid()), gid);
                        auto duration = std::chrono::duration<float>(db_clock::now() - sched_time);
                        auto& tablet_state = _tablets[tablet];
                        tablet_state.repair_time = db_clock::from_time_t(gc_clock::to_time_t(res.repair_time));
                        rtlogger.info("Finished tablet repair host={} tablet={} duration={} repair_time={}",
                                dst, tablet, duration, res.repair_time);
                    })) {
                        auto& tinfo = tmap.get_tablet_info(gid.tablet);
                        bool valid = tinfo.repair_task_info.is_valid();
                        auto hosts_filter = tinfo.repair_task_info.repair_hosts_filter;
                        auto dcs_filter = tinfo.repair_task_info.repair_dcs_filter;
                        bool is_filter_off = hosts_filter.empty() && dcs_filter.empty();
                        rtlogger.debug("Will set tablet {} stage to {}", gid, locator::tablet_transition_stage::end_repair);
                        auto update = get_mutation_builder()
                                        .set_stage(last_token, locator::tablet_transition_stage::end_repair)
                                        .del_repair_task_info(last_token)
                                        .del_session(last_token);
                        // Skip update repair time in case hosts filter or dcs filter is set.
                        if (valid && is_filter_off) {
                            auto sched_time = tinfo.repair_task_info.sched_time;
                            auto time = tablet_state.repair_time;
                            rtlogger.debug("Set tablet repair time sched_time={} return_time={} set_time={}",
                                    sched_time, tablet_state.repair_time, time);
                            update.set_repair_time(last_token, time);
                        }
                        updates.emplace_back(update.build());
                    }
                }
                    break;
                case locator::tablet_transition_stage::end_repair: {
                    if (do_barrier()) {
                        _tablets.erase(gid);
                        updates.emplace_back(get_mutation_builder()
                            .del_transition(last_token)
                            .build());
                    }
                }
                    break;
            }
        });

        // In order to keep the cluster saturated, ask the load balancer for more transitions.
        // Unless there is a pending topology change operation.
        bool preempt = false;
        if (!drain) {
            // When draining, this method is invoked with an active node transition, which
            // would normally cause preemption, which we don't want here.
            auto ts = guard.write_timestamp();
            auto [new_preempt, new_guard] = should_preempt_balancing(std::move(guard));
            preempt = new_preempt;
            guard = std::move(new_guard);
            if (ts != guard.write_timestamp()) {
                // We rely on the fact that should_preempt_balancing() does not release the guard
                // so that tablet metadata reading and updates are atomic.
                on_internal_error(rtlogger, "should_preempt_balancing() retook the guard");
            }
        }

        bool has_nodes_to_drain = false;
        if (!preempt) {
            auto plan = co_await _tablet_allocator.balance_tablets(get_token_metadata_ptr(), {}, get_dead_nodes());
            has_nodes_to_drain = plan.has_nodes_to_drain();
            if (!drain || plan.has_nodes_to_drain()) {
                co_await generate_migration_updates(updates, guard, plan);
            }
        }

        // The updates have to be executed under the same guard which was used to read tablet metadata
        // to ensure that we don't reinsert tablet rows which were concurrently deleted by schema change
        // which happens outside the topology coordinator.
        bool has_updates = !updates.empty();
        if (has_updates) {
            co_await utils::get_local_injector().inject("tablet_transition_updates", utils::wait_for_message(2min));
            updates.emplace_back(
                topology_mutation_builder(guard.write_timestamp())
                    .set_version(_topo_sm._topology.version + 1)
                    .build());
            co_await update_topology_state(std::move(guard), std::move(updates), format("Tablet migration"));
        }

        if (needs_barrier) {
            // If has_updates is true then we have dropped the guard and need to re-obtain it.
            // It's fine to start an independent operation here. The barrier doesn't have to be executed
            // atomically with the read which set needs_barrier, because it's fine if the global barrier
            // works with a more recent set of nodes and it's fine if it propagates a more recent topology.
            if (!guard) {
                guard = co_await start_operation();
            }
            guard = co_await global_tablet_token_metadata_barrier(std::move(guard));
            barrier.set_value();
            fail_barrier.cancel();
            co_return;
        }

        if (has_updates) {
            co_return;
        }

        if (has_transitions) {
            // Streaming may have finished after we checked. To avoid missed notification, we need
            // to check atomically with event.wait()
            if (!_tablets_ready) {
                rtlogger.debug("Going to sleep with active tablet transitions");
                release_guard(std::move(guard));
                co_await await_event();
            }
            co_return;
        }

        if (drain) {
            if (has_nodes_to_drain) {
                // Prevent jumping to write_both_read_old with un-drained tablets.
                // This can happen when all candidate nodes are down.
                rtlogger.warn("Tablet draining stalled: No tablets migrating but there are nodes to drain");
                release_guard(std::move(guard));
                co_await sleep(3s); // Throttle retries
                co_return;
            }
            updates.emplace_back(
                topology_mutation_builder(guard.write_timestamp())
                    .set_transition_state(topology::transition_state::write_both_read_old)
                    .set_session(session_id(guard.new_group0_state_id()))
                    .set_version(_topo_sm._topology.version + 1)
                    .build());
        } else {
            updates.emplace_back(
                topology_mutation_builder(guard.write_timestamp())
                    .del_transition_state()
                    .set_version(_topo_sm._topology.version + 1)
                    .build());
        }
        co_await update_topology_state(std::move(guard), std::move(updates), "Finished tablet migration");
    }

    future<> handle_tablet_resize_finalization(group0_guard g) {
        co_await utils::get_local_injector().inject("handle_tablet_resize_finalization_wait", [] (auto& handler) -> future<> {
            rtlogger.info("handle_tablet_resize_finalization: waiting");
            co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::seconds{60});
        });

        // Executes a global barrier to guarantee that any process (e.g. repair) holding stale version
        // of token metadata will complete before we update topology.
        auto guard = co_await global_tablet_token_metadata_barrier(std::move(g));

        auto tm = get_token_metadata_ptr();
        auto plan = co_await _tablet_allocator.balance_tablets(tm, {}, get_dead_nodes());

        std::vector<canonical_mutation> updates;
        updates.reserve(plan.resize_plan().finalize_resize.size() * 2 + 1);

        for (auto& table_id : plan.resize_plan().finalize_resize) {
            auto s = _db.find_schema(table_id);
            auto new_tablet_map = co_await _tablet_allocator.resize_tablets(tm, table_id);
            updates.emplace_back(co_await replica::tablet_map_to_mutation(
                new_tablet_map,
                table_id,
                s->ks_name(),
                s->cf_name(),
                guard.write_timestamp(),
                _feature_service));

            // Clears the resize decision for a table.
            generate_resize_update(updates, guard, table_id, locator::resize_decision{});
        }

        updates.emplace_back(
            topology_mutation_builder(guard.write_timestamp())
                .del_transition_state()
                .set_version(_topo_sm._topology.version + 1)
                .build());
        co_await update_topology_state(std::move(guard), std::move(updates), format("Finished tablet split finalization"));
    }

    future<> handle_truncate_table(group0_guard guard) {
        // Execute a barrier to make sure the nodes we are performing truncate on see the session
        // and are able to create a topology_guard using the frozen_guard we are sending over RPC
        // TODO: Exclude nodes which don't contain replicas of the table we are truncating
        guard = co_await global_tablet_token_metadata_barrier(std::move(guard));

        const utils::UUID& global_request_id = _topo_sm._topology.global_request_id.value();
        std::optional<sstring> error;
        // We should perform TRUNCATE only if the session is still valid. It could be cleared if a previous truncate
        // handler performed the truncate and cleared the session, but crashed before finalizing the request
        if (_topo_sm._topology.session) {
            const auto topology_requests_entry = co_await _sys_ks.get_topology_request_entry(global_request_id, true);
            const table_id& table_id = topology_requests_entry.truncate_table_id;
            lw_shared_ptr<replica::table> table = _db.get_tables_metadata().get_table_if_exists(table_id);

            if (table) {
                const sstring& ks_name = table->schema()->ks_name();
                const sstring& cf_name = table->schema()->cf_name();

                rtlogger.info("Performing TRUNCATE TABLE for {}.{}", ks_name, cf_name);

                // Collect the IDs of the hosts with replicas, but ignore excluded nodes
                std::unordered_set<locator::host_id> replica_hosts;
                const std::unordered_set<raft::server_id> excluded_nodes = _topo_sm._topology.get_excluded_nodes();
                const locator::tablet_map& tmap = get_token_metadata_ptr()->tablets().get_tablet_map(table_id);
                co_await tmap.for_each_tablet([&] (locator::tablet_id tid, const locator::tablet_info& tinfo) {
                    for (const locator::tablet_replica& replica: tinfo.replicas) {
                        if (!excluded_nodes.contains(raft::server_id(replica.host.uuid()))) {
                            replica_hosts.insert(replica.host);
                        }
                    }
                    return make_ready_future<>();
                });

                // Release the guard to avoid blocking group0 for long periods of time while invoking RPCs
                release_guard(std::move(guard));

                co_await utils::get_local_injector().inject("truncate_table_wait", utils::wait_for_message(std::chrono::minutes(2)));

                // Check if all the nodes with replicas are alive
                for (const locator::host_id& replica_host: replica_hosts) {
                    if (!_gossiper.is_alive(replica_host)) {
                        throw std::runtime_error(::format("Cannot perform TRUNCATE on table {}.{} because host {} is down", ks_name, cf_name, replica_host));
                    }
                }

                // Send the RPC to all replicas
                const service::frozen_topology_guard frozen_guard { _topo_sm._topology.session };
                co_await coroutine::parallel_for_each(replica_hosts, [&] (const locator::host_id& host_id) -> future<> {
                    co_await ser::storage_proxy_rpc_verbs::send_truncate_with_tablets(&_messaging, host_id, ks_name, cf_name, frozen_guard);
                });
            } else {
                error = ::format("Cannot TRUNCATE table with UUID {} because it does not exist.", table_id);
            }

            // Clear the session and save the error message
            while (true) {
                if (!guard) {
                    guard = co_await start_operation();
                }

                std::vector<canonical_mutation> updates;
                updates.push_back(topology_mutation_builder(guard.write_timestamp())
                                    .del_session()
                                    .build());
                if (error) {
                    updates.push_back(topology_request_tracking_mutation_builder(global_request_id)
                                        .set("error", *error)
                                        .build());
                }

                try {
                    co_await update_topology_state(std::move(guard), std::move(updates), "Clear truncate session");
                    break;
                } catch (group0_concurrent_modification&) {
                }
            }
        }

        utils::get_local_injector().inject("truncate_crash_after_session_clear", [] {
            rtlogger.info("truncate_crash_after_session_clear hit, killing the node");
            _exit(1);
        });

        // Execute a barrier to ensure the TRUNCATE RPC can't run on any nodes after this point
        if (!guard) {
            guard = co_await start_operation();
        }
        guard = co_await global_tablet_token_metadata_barrier(std::move(guard));

        // Finalize the request
        while (true) {
            if (!guard) {
                guard = co_await start_operation();
            }
            std::vector<canonical_mutation> updates;
            updates.push_back(topology_mutation_builder(guard.write_timestamp())
                                .del_transition_state()
                                .del_global_topology_request()
                                .del_global_topology_request_id()
                                .build());
            updates.push_back(topology_request_tracking_mutation_builder(global_request_id)
                                .set("end_time", db_clock::now())
                                .set("done", true)
                                .build());

            try {
                co_await update_topology_state(std::move(guard), std::move(updates), "Truncate has completed");
                break;
            } catch (group0_concurrent_modification&) {
            }
        }
    }

    // This function must not release and reacquire the guard, callers rely
    // on the fact that the block which calls this is atomic.
    // FIXME: Don't take the ownership of the guard to make the above guarantee explicit.
    std::pair<bool, group0_guard> should_preempt_balancing(group0_guard guard) {
        auto work = get_next_task(std::move(guard));
        if (auto* node = std::get_if<node_to_work_on>(&work)) {
            return std::make_pair(true, std::move(node->guard));
        }

        if (auto* cancel = std::get_if<cancel_requests>(&work)) {
            // request queue needs to be canceled, so preempt balancing
            return std::make_pair(true, std::move(cancel->guard));
        }

        if (auto* cleanup = std::get_if<start_cleanup>(&work)) {
            // cleanup has to be started
            return std::make_pair(true, std::move(cleanup->guard));
        }

        guard = std::get<group0_guard>(std::move(work));

        if (_topo_sm._topology.global_request) {
            return std::make_pair(true, std::move(guard));
        }

        if (!_topo_sm._topology.calculate_not_yet_enabled_features().empty()) {
            return std::make_pair(true, std::move(guard));
        }

        return std::make_pair(false, std::move(guard));
    }

    void cleanup_ignored_nodes_on_left(topology_mutation_builder& builder, raft::server_id id) {
        if (_topo_sm._topology.ignored_nodes.contains(id)) {
            auto l = _topo_sm._topology.ignored_nodes;
            l.erase(id);
            builder.set_ignored_nodes(l);
        }
    }

    future<> cancel_all_requests(group0_guard guard, std::unordered_set<raft::server_id> dead_nodes) {
        std::vector<canonical_mutation> muts;
        std::vector<raft::server_id> reject_join;
        if (_topo_sm._topology.requests.empty()) {
            co_return;
        }
        auto ts = guard.write_timestamp();
        for (auto& [id, req] : _topo_sm._topology.requests) {
            topology_mutation_builder builder(ts);
            topology_request_tracking_mutation_builder rtbuilder(_topo_sm._topology.find(id)->second.request_id);
            auto node_builder = builder.with_node(id).del("topology_request");
            auto done_msg = fmt::format("Canceled. Dead nodes: {}", dead_nodes);
            rtbuilder.done(done_msg);
            if (_topo_sm._topology.global_request_id) {
                try {
                    utils::UUID uuid = utils::UUID{*_topo_sm._topology.global_request_id};
                    topology_request_tracking_mutation_builder rt_global_req_builder{uuid};
                    rt_global_req_builder.done(done_msg)
                                         .set("end_time", db_clock::now());
                    muts.emplace_back(rt_global_req_builder.build());
                } catch (...) {
                    rtlogger.warn("failed to cancel topology global request: {}", std::current_exception());
                }
            }
            switch (req) {
                case topology_request::replace:
                [[fallthrough]];
                case topology_request::join: {
                    node_builder.set("node_state", node_state::left);
                    reject_join.emplace_back(id);
                    try {
                        co_await wait_for_gossiper(id, _gossiper, _as);
                    } catch (...) {
                        rtlogger.warn("wait_for_ip failed during cancellation: {}", std::current_exception());
                    }
                }
                break;
                case topology_request::leave:
                [[fallthrough]];
                case topology_request::rebuild:
                [[fallthrough]];
                case topology_request::remove: {
                }
                break;
            }
            muts.emplace_back(builder.build());
            muts.emplace_back(rtbuilder.build());
        }

        co_await update_topology_state(std::move(guard), std::move(muts), "cancel all topology requests");

        for (auto id : reject_join) {
            try {
                co_await respond_to_joining_node(id, join_node_response_params{
                    .response = join_node_response_params::rejected{
                        .reason = "request canceled because some required nodes are dead"
                    },
                });
            } catch (...) {
                rtlogger.warn("attempt to send rejection response to {} failed: {}. "
                                "The node may hang. It's safe to shut it down manually now.",
                                id, std::current_exception());
            }
        }

    }

    // Returns `true` iff there was work to do.
    future<bool> handle_topology_transition(group0_guard guard) {
        auto tstate = _topo_sm._topology.tstate;
        if (!tstate) {
            // When adding a new source of work, make sure to update should_preempt_balancing() as well.

            auto work = get_next_task(std::move(guard));
            if (auto* node = std::get_if<node_to_work_on>(&work)) {
                co_await handle_node_transition(std::move(*node));
                co_return true;
            }

            if (auto* cancel = std::get_if<cancel_requests>(&work)) {
                co_await cancel_all_requests(std::move(cancel->guard), std::move(cancel->dead_nodes));
                co_return true;
            }

            if (auto* cleanup = std::get_if<start_cleanup>(&work)) {
                co_await start_cleanup_on_dirty_nodes(std::move(cleanup->guard), false);
                co_return true;
            }

            guard = std::get<group0_guard>(std::move(work));

            if (_topo_sm._topology.global_request) {
                co_await handle_global_request(std::move(guard));
                co_return true;
            }

            if (auto feats = _topo_sm._topology.calculate_not_yet_enabled_features(); !feats.empty()) {
                co_await enable_features(std::move(guard), std::move(feats));
                co_return true;
            }

            if (auto guard_opt = co_await maybe_migrate_system_tables(std::move(guard)); !guard_opt) {
                // The guard is consumed, it means we did migration
                co_return true;
            } else {
                guard = std::move(*guard_opt);
            }

            // If there is no other work, evaluate load and start tablet migration if there is imbalance.
            if (co_await maybe_start_tablet_migration(std::move(guard))) {
                co_return true;
            }
            co_return false;
        }

        rtlogger.info("entered `{}` transition state", *tstate);
        switch (*tstate) {
            case topology::transition_state::join_group0: {
                    auto node = get_node_to_work_on(std::move(guard));
                    if (node.rs->state == node_state::replacing) {
                        // Make sure all nodes are no longer trying to write to a node being replaced. This is important
                        // if the new node have the same IP, so that old write will not go to the new node by mistake after this point.
                        // It is important to do so before the call to finish_accepting_node() below since after this call the new node becomes
                        // a full member of the cluster and it starts loading an initial snapshot. Since snapshot loading is not atomic any queries
                        // that are done in parallel may see a partial state.
                        try {
                            node = retake_node(co_await global_token_metadata_barrier(std::move(node.guard), get_excluded_nodes(node)), node.id);
                        } catch (term_changed_error&) {
                            throw;
                        } catch (group0_concurrent_modification&) {
                            throw;
                        } catch (raft::request_aborted&) {
                            throw;
                        } catch (...) {
                            rtlogger.error("transition_state::join_group0, "
                                            "global_token_metadata_barrier failed, error {}",
                                            std::current_exception());
                            _rollback = fmt::format("global_token_metadata_barrier failed in join_group0 state {}", std::current_exception());
                            break;
                        }
                    }

                bool accepted;
                std::tie(node, accepted) = co_await finish_accepting_node(std::move(node));

                // If responding to the joining node failed, move the node to the left state and
                // stop the topology transition.
                if (!accepted) {
                    topology_mutation_builder builder(node.guard.write_timestamp());
                    topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
                    builder.del_transition_state()
                           .with_node(node.id)
                           .set("node_state", node_state::left);
                    rtbuilder.done("join is not accepted");
                    auto reason = ::format("bootstrap: failed to accept {}", node.id);
                    co_await update_topology_state(std::move(node.guard), {builder.build(), rtbuilder.build()}, reason);

                    rtlogger.info("node {} moved to left state", node.id);

                    break;
                }

                switch (node.rs->state) {
                    case node_state::bootstrapping: {
                        SCYLLA_ASSERT(!node.rs->ring);
                        auto num_tokens = std::get<join_param>(node.req_param.value()).num_tokens;
                        auto tokens_string = std::get<join_param>(node.req_param.value()).tokens_string;

                        auto guard = take_guard(std::move(node));
                        topology_mutation_builder builder(guard.write_timestamp());

                        // A node has just been accepted. If it is a zero-token node, we can
                        // instantly move its state to normal. Otherwise, we need to assign
                        // random tokens and prepare a new CDC generation.
                        if (num_tokens == 0 && tokens_string.empty()) {
                            topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
                            builder.del_transition_state()
                                   .with_node(node.id)
                                   .set("node_state", node_state::normal);
                            rtbuilder.done();
                            auto reason = ::format("bootstrap: joined a zero-token node {}", node.id);
                            co_await update_topology_state(std::move(guard), {builder.build(), rtbuilder.build()}, reason);
                            co_await _voter_handler.on_node_added(node.id, _as);
                            break;
                        }

                        auto tmptr = get_token_metadata_ptr();
                        std::unordered_set<token> bootstrap_tokens;
                        try {
                            bootstrap_tokens = dht::boot_strapper::get_bootstrap_tokens(tmptr, tokens_string, num_tokens, dht::check_token_endpoint::yes);
                        } catch (...) {
                            _rollback = fmt::format("Failed to assign tokens: {}", std::current_exception());
                            break;
                        }

                        auto [gen_uuid, guard_, mutation] = co_await prepare_and_broadcast_cdc_generation_data(
                                tmptr, std::move(guard), bootstrapping_info{bootstrap_tokens, *node.rs});

                        // Write the new CDC generation data through raft.
                        builder.set_transition_state(topology::transition_state::commit_cdc_generation)
                               .set_new_cdc_generation_data_uuid(gen_uuid)
                               .with_node(node.id)
                               .set("tokens", bootstrap_tokens);
                        auto reason = ::format(
                            "bootstrap: insert tokens and CDC generation data (UUID: {})", gen_uuid);
                        co_await update_topology_state(std::move(guard_), {std::move(mutation), builder.build()}, reason);

                        co_await utils::get_local_injector().inject("topology_coordinator_pause_after_updating_cdc_generation", utils::wait_for_message(5min));
                    }
                        break;
                    case node_state::replacing: {
                        SCYLLA_ASSERT(!node.rs->ring);
                        auto replaced_id = std::get<replace_param>(node.req_param.value()).replaced_id;
                        auto it = _topo_sm._topology.normal_nodes.find(replaced_id);
                        SCYLLA_ASSERT(it != _topo_sm._topology.normal_nodes.end());
                        SCYLLA_ASSERT(it->second.ring && it->second.state == node_state::normal);

                        topology_mutation_builder builder(node.guard.write_timestamp());

                        // If a zero-token node is replacing another zero-token node,
                        // we can instantly move the new node's state to normal.
                        if (it->second.ring->tokens.empty()) {
                            node = retake_node(co_await remove_from_group0(std::move(node.guard), replaced_id), node.id);

                            topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
                            builder.del_transition_state()
                                   .with_node(node.id)
                                   .set("node_state", node_state::normal);

                            // Move the old node to the left state.
                            topology_mutation_builder builder2(node.guard.write_timestamp());
                            cleanup_ignored_nodes_on_left(builder2, replaced_id);
                            builder2.with_node(replaced_id)
                                    .set("node_state", node_state::left);
                            rtbuilder.done();
                            co_await update_topology_state(take_guard(std::move(node)), {builder.build(), builder2.build(), rtbuilder.build()},
                                    fmt::format("replace: replaced node {} with the new zero-token node {}", replaced_id, node.id));
                            co_await _voter_handler.on_node_added(node.id, _as);
                            break;
                        }

                        builder.set_transition_state(topology::transition_state::tablet_draining)
                               .set_version(_topo_sm._topology.version + 1)
                               .with_node(node.id)
                               .set("tokens", it->second.ring->tokens);
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build()},
                                "replace: transition to tablet_draining and take ownership of the replaced node's tokens");
                    }
                        break;
                    default:
                        on_internal_error(rtlogger,
                                format("topology is in join_group0 state, but the node"
                                       " being worked on ({}) is in unexpected state '{}'; should be"
                                       " either 'bootstrapping' or 'replacing'", node.id, node.rs->state));
                }
            }
                break;
            case topology::transition_state::commit_cdc_generation: {
                // make sure all nodes know about new topology and have the new CDC generation data
                // (we require all nodes to be alive for topo change for now)
                // Note: if there was a replace or removenode going on, we'd need to put the replaced/removed
                // node into `exclude_nodes` parameter in `exec_global_command`, but CDC generations are never
                // introduced during replace/remove.
                try {
                    guard = co_await exec_global_command(std::move(guard), raft_topology_cmd::command::barrier, {_raft.id()});
                } catch (term_changed_error&) {
                    throw;
                } catch (group0_concurrent_modification&) {
                    throw;
                } catch (raft::request_aborted&) {
                    throw;
                } catch (...) {
                    rtlogger.error("transition_state::commit_cdc_generation, "
                                    "raft_topology_cmd::command::barrier failed, error {}", std::current_exception());
                    _rollback = fmt::format("Failed to commit cdc generation: {}", std::current_exception());
                    break;
                }

                // We don't need to add delay to the generation timestamp if this is the first generation.
                bool add_ts_delay = !_topo_sm._topology.committed_cdc_generations.empty();

                // Begin the race.
                // See the large FIXME below.
                auto cdc_gen_ts = cdc::new_generation_timestamp(add_ts_delay, _ring_delay);
                auto cdc_gen_uuid = _topo_sm._topology.new_cdc_generation_data_uuid;
                if (!cdc_gen_uuid) {
                    on_internal_error(rtlogger,
                        "new CDC generation data UUID missing in `commit_cdc_generation` state");
                }

                cdc::generation_id_v2 cdc_gen_id {
                    .ts = cdc_gen_ts,
                    .id = *cdc_gen_uuid,
                };

                if (!_topo_sm._topology.committed_cdc_generations.empty()) {
                    // Sanity check.
                    // This could happen if the topology coordinator's clock is broken.
                    auto last_committed_gen_id = _topo_sm._topology.committed_cdc_generations.back();
                    if (last_committed_gen_id.ts >= cdc_gen_ts) {
                        on_internal_error(rtlogger, ::format(
                            "new CDC generation has smaller timestamp than the previous one."
                            " Old generation ID: {}, new generation ID: {}", last_committed_gen_id, cdc_gen_id));
                    }
                }

                // Tell all nodes to start using the new CDC generation by updating the topology
                // with the generation's ID and timestamp.
                // At the same time move the topology change procedure to the next step.
                //
                // FIXME: as in previous implementation with gossiper and ring_delay, this assumes that all nodes
                // will learn about the new CDC generation before their clocks reach the generation's timestamp.
                // With this group 0 based implementation, it means that the command must be committed,
                // replicated and applied on all nodes before their clocks reach the generation's timestamp
                // (i.e. within 2 * ring_delay = 60 seconds by default if clocks are synchronized). If this
                // doesn't hold some coordinators might use the wrong CDC streams for some time and CDC stream
                // readers will miss some data. It's likely that Raft replication doesn't converge as quickly
                // as gossiping does.
                //
                // We could use a two-phase algorithm instead: first tell all nodes to prepare for using
                // the new generation, then tell all nodes to commit. If some nodes don't manage to prepare
                // in time, we abort the generation switch. If all nodes prepare, we commit. If a node prepares
                // but doesn't receive a commit in time, it stops coordinating CDC-enabled writes until it
                // receives a commit or abort. This solution does not have a safety problem like the one
                // above, but it has an availability problem when nodes get disconnected from group 0 majority
                // in the middle of a CDC generation switch (when they are prepared to switch but not
                // committed) - they won't coordinate CDC-enabled writes until they reconnect to the
                // majority and commit.
                topology_mutation_builder builder(guard.write_timestamp());
                builder.add_new_committed_cdc_generation(cdc_gen_id);
                if (_topo_sm._topology.global_request == global_topology_request::new_cdc_generation) {
                    builder.del_global_topology_request();
                    builder.del_transition_state();
                } else {
                    builder.set_transition_state(topology::transition_state::write_both_read_old);
                    builder.set_session(session_id(guard.new_group0_state_id()));
                    builder.set_version(_topo_sm._topology.version + 1);
                }
                auto str = ::format("committed new CDC generation, ID: {}", cdc_gen_id);
                co_await update_topology_state(std::move(guard), {builder.build()}, std::move(str));
            }
                break;
            case topology::transition_state::tablet_draining:
                co_await utils::get_local_injector().inject("suspend_decommission", utils::wait_for_message(1min));
                try {
                    co_await handle_tablet_migration(std::move(guard), true);
                } catch (term_changed_error&) {
                    throw;
                } catch (group0_concurrent_modification&) {
                    throw;
                } catch (raft::request_aborted&) {
                    throw;
                } catch (...) {
                    rtlogger.error("tablets draining failed with {}. Aborting the topology operation", std::current_exception());
                    _rollback = fmt::format("Failed to drain tablets: {}", std::current_exception());
                }
                break;
            case topology::transition_state::write_both_read_old: {
                auto node = get_node_to_work_on(std::move(guard));

                // make sure all nodes know about new topology (we require all nodes to be alive for topo change for now)
                try {
                    node = retake_node(co_await global_token_metadata_barrier(std::move(node.guard), get_excluded_nodes(node)), node.id);
                } catch (term_changed_error&) {
                    throw;
                } catch (group0_concurrent_modification&) {
                    throw;
                } catch (raft::request_aborted&) {
                    throw;
                } catch (...) {
                    rtlogger.error("transition_state::write_both_read_old, "
                                    "global_token_metadata_barrier failed, error {}",
                                    std::current_exception());
                    _rollback = fmt::format("global_token_metadata_barrier failed in write_both_read_old state {}", std::current_exception());
                    break;
                }

                if (_group0.is_member(node.id, true)) {
                    // If we remove a node, we make it a non-voter early to improve availability in some situations.
                    // There is no downside to it because the removed node is already considered dead by us.
                    //
                    // FIXME: removenode may be aborted and the already dead node can be resurrected. We should consider
                    // restoring its voter state on the recovery path.
                    if (node.rs->state == node_state::removing && !_feature_service.group0_limited_voters) {
                        co_await _voter_handler.on_node_removed(node.id, _as);
                    }
                }
                if (node.rs->state == node_state::replacing && !_feature_service.group0_limited_voters) {
                    // We make a replaced node a non-voter early, just like a removed node.
                    auto replaced_node_id = parse_replaced_node(node.req_param);
                    if (_group0.is_member(replaced_node_id, true)) {
                        co_await _voter_handler.on_node_removed(replaced_node_id, _as);
                    }
                }
                utils::get_local_injector().inject("crash_coordinator_before_stream", [] { abort(); });
                raft_topology_cmd cmd{raft_topology_cmd::command::stream_ranges};
                auto state = node.rs->state;
                try {
                    // No need to stream data when the node is zero-token.
                    if (!node.rs->ring->tokens.empty()) {
                        if (node.rs->state == node_state::removing) {
                            // tell all token owners to stream data of the removed node to new range owners
                            auto exclude_nodes = get_excluded_nodes(node);
                            auto normal_zero_token_nodes = _topo_sm._topology.get_normal_zero_token_nodes();
                            std::move(normal_zero_token_nodes.begin(), normal_zero_token_nodes.end(),
                                    std::inserter(exclude_nodes, exclude_nodes.begin()));
                            node = retake_node(
                                    co_await exec_global_command(take_guard(std::move(node)), cmd, exclude_nodes), node.id);
                        } else {
                            // Tell joining/leaving/replacing node to stream its ranges
                            node = co_await exec_direct_command(std::move(node), cmd);
                        }
                    }
                } catch (term_changed_error&) {
                    throw;
                } catch (raft::request_aborted&) {
                    throw;
                } catch (...) {
                    rtlogger.error("send_raft_topology_cmd(stream_ranges) failed with exception"
                                    " (node state is {}): {}", state, std::current_exception());
                    _rollback = fmt::format("Failed stream ranges: {}", std::current_exception());
                    break;
                }
                // Streaming completed. We can now move tokens state to topology::transition_state::write_both_read_new
                topology_mutation_builder builder(node.guard.write_timestamp());
                builder
                    .set_transition_state(topology::transition_state::write_both_read_new)
                    .del_session()
                    .set_version(_topo_sm._topology.version + 1);
                auto str = ::format("{}: streaming completed for node {}", node.rs->state, node.id);
                co_await update_topology_state(take_guard(std::move(node)), {builder.build()}, std::move(str));
            }
                break;
            case topology::transition_state::write_both_read_new: {
                while (utils::get_local_injector().enter("topology_coordinator_pause_after_streaming")) {
                    co_await sleep_abortable(std::chrono::milliseconds(10), _as);
                }
                auto node = get_node_to_work_on(std::move(guard));
                bool barrier_failed = false;
                // In this state writes goes to old and new replicas but reads start to be done from new replicas
                // Before we stop writing to old replicas we need to wait for all previous reads to complete
                try {
                    node = retake_node(co_await global_token_metadata_barrier(std::move(node.guard), get_excluded_nodes(node)), node.id);
                } catch (term_changed_error&) {
                    throw;
                } catch (group0_concurrent_modification&) {
                    throw;
                } catch (raft::request_aborted&) {
                    throw;
                } catch (...) {
                    rtlogger.error("transition_state::write_both_read_new, "
                                    "global_token_metadata_barrier failed, error {}",
                                    std::current_exception());
                    barrier_failed = true;
                }
                if (barrier_failed) {
                    // If barrier above failed it means there may be unfenced reads from old replicas.
                    // Lets wait for the ring delay for those writes to complete or fence to propagate
                    // before continuing.
                    // FIXME: nodes that cannot be reached need to be isolated either automatically or
                    // by an administrator
                    co_await sleep_abortable(_ring_delay, _as);
                    node = retake_node(co_await start_operation(), node.id);
                }
                topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
                rtbuilder.done();
                switch(node.rs->state) {
                case node_state::bootstrapping: {
                    co_await utils::get_local_injector().inject("delay_node_bootstrap", utils::wait_for_message(std::chrono::minutes(5)));
                    std::vector<canonical_mutation> muts;
                    // Since after bootstrapping a new node some nodes lost some ranges they need to cleanup
                    muts = mark_nodes_as_cleanup_needed(node, false);
                    topology_mutation_builder builder(node.guard.write_timestamp());
                    builder.del_transition_state()
                           .with_node(node.id)
                           .set("node_state", node_state::normal);
                    muts.emplace_back(builder.build());
                    muts.emplace_back(rtbuilder.build());
                    co_await update_topology_state(take_guard(std::move(node)), std::move(muts),
                                                   "bootstrap: read fence completed");
                    // Make sure the load balancer knows the capacity for the new node immediately.
                    (void)_tablet_load_stats_refresh.trigger().handle_exception([] (auto ep) {
                        rtlogger.warn("Error during tablet load stats refresh: {}", ep);
                    });
                    }
                    co_await _voter_handler.on_node_added(node.id, _as);
                    break;
                case node_state::removing: {
                    co_await utils::get_local_injector().inject("delay_node_removal", utils::wait_for_message(std::chrono::minutes(5)));
                    node = retake_node(co_await remove_from_group0(std::move(node.guard), node.id), node.id);
                }
                    [[fallthrough]];
                case node_state::decommissioning: {
                    topology_mutation_builder builder(node.guard.write_timestamp());
                    node_state next_state;
                    std::vector<canonical_mutation> muts;
                    muts.reserve(2);
                    if (node.rs->state == node_state::decommissioning) {
                        next_state = node.rs->state;
                        builder.set_transition_state(topology::transition_state::left_token_ring);
                    } else {
                        next_state = node_state::left;
                        builder.del_transition_state();
                        cleanup_ignored_nodes_on_left(builder, node.id);
                        muts.push_back(rtbuilder.build());
                        co_await db::view::view_builder::generate_mutations_on_node_left(_db, _sys_ks, node.guard.write_timestamp(), locator::host_id(node.id.uuid()), muts);
                    }
                    builder.set_version(_topo_sm._topology.version + 1)
                           .with_node(node.id)
                           .del("tokens")
                           .set("node_state", next_state);
                    auto str = ::format("{}: read fence completed", node.rs->state);
                    muts.push_back(builder.build());
                    co_await update_topology_state(take_guard(std::move(node)), std::move(muts), std::move(str));
                }
                    break;
                case node_state::replacing: {
                    auto replaced_node_id = parse_replaced_node(node.req_param);
                    node = retake_node(co_await remove_from_group0(std::move(node.guard), replaced_node_id), node.id);

                    std::vector<canonical_mutation> muts;

                    topology_mutation_builder builder1(node.guard.write_timestamp());
                    // Move new node to 'normal'
                    builder1.del_transition_state()
                            .set_version(_topo_sm._topology.version + 1)
                            .with_node(node.id)
                            .set("node_state", node_state::normal);
                    muts.push_back(builder1.build());

                    // Move old node to 'left'
                    topology_mutation_builder builder2(node.guard.write_timestamp());
                    cleanup_ignored_nodes_on_left(builder2, replaced_node_id);
                    builder2.with_node(replaced_node_id)
                            .del("tokens")
                            .set("node_state", node_state::left);
                    muts.push_back(builder2.build());

                    muts.push_back(rtbuilder.build());
                    co_await db::view::view_builder::generate_mutations_on_node_left(_db, _sys_ks, node.guard.write_timestamp(), locator::host_id(replaced_node_id.uuid()), muts);
                    co_await update_topology_state(take_guard(std::move(node)), std::move(muts),
                                                  "replace: read fence completed");
                    }
                    co_await _voter_handler.on_node_added(node.id, _as);
                    break;
                default:
                    on_fatal_internal_error(rtlogger, ::format(
                            "Ring state on node {} is write_both_read_new while the node is in state {}",
                            node.id, node.rs->state));
                }
                // Reads are fenced. We can now remove topology::transition_state and move node state to normal
            }
                break;
            case topology::transition_state::tablet_migration:
                co_await handle_tablet_migration(std::move(guard), false);
                break;
            case topology::transition_state::tablet_split_finalization:
                [[fallthrough]];
            case topology::transition_state::tablet_resize_finalization:
                co_await handle_tablet_resize_finalization(std::move(guard));
                break;
            case topology::transition_state::lock:
                release_guard(std::move(guard));
                co_await await_event();
                break;
            case topology::transition_state::left_token_ring: {
                auto node = get_node_to_work_on(std::move(guard));

                if (node.id == _raft.id()) {
                    // Someone else needs to coordinate the rest of the decommission process,
                    // because the decommissioning node is going to shut down in the middle of this state.
                    rtlogger.info("coordinator is decommissioning; giving up leadership");
                    co_await step_down_as_nonvoter();

                    // Note: if we restart after this point and become a voter
                    // and then a coordinator again, it's fine - we'll just repeat this step.
                    // (If we're in `left` state when we try to restart we won't
                    // be able to become a voter - we'll be banned from the cluster.)
                }

                bool barrier_failed = false;
                // Wait until other nodes observe the new token ring and stop sending writes to this node.
                try {
                    node = retake_node(co_await global_token_metadata_barrier(std::move(node.guard), get_excluded_nodes(node)), node.id);
                } catch (term_changed_error&) {
                    throw;
                } catch (group0_concurrent_modification&) {
                    throw;
                } catch (raft::request_aborted&) {
                    throw;
                } catch (...) {
                    rtlogger.error("transition_state::left_token_ring, "
                                    "raft_topology_cmd::command::barrier failed, error {}",
                                    std::current_exception());
                    barrier_failed = true;
                }

                if (barrier_failed) {
                    // If barrier above failed it means there may be unfinished writes to a decommissioned node.
                    // Lets wait for the ring delay for those writes to complete and new topology to propagate
                    // before continuing.
                    co_await sleep_abortable(_ring_delay, _as);
                    node = retake_node(co_await start_operation(), node.id);
                }

                // Make decommissioning node a non voter before reporting operation completion below.
                // Otherwise the decommissioned node may see the completion and exit before it is removed from
                // the config at which point the removal from the config will hang if the cluster had only two
                // nodes before the decommission.
                co_await _voter_handler.on_node_removed(node.id, _as);

                topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);

                rtbuilder.done();

                co_await update_topology_state(take_guard(std::move(node)), {rtbuilder.build()}, "report request completion in left_token_ring state");

                // Tell the node to shut down.
                // This is done to improve user experience when there are no failures.
                // In the next state (`node_state::left`), the node will be banned by the rest of the cluster,
                // so there's no guarantee that it would learn about entering that state even if it was still
                // a member of group0, hence we use a separate direct RPC in this state to shut it down.
                //
                // There is the possibility that the node will never get the message
                // and decommission will hang on that node.
                // This is fine for the rest of the cluster - we will still remove, ban the node and continue.
                auto node_id = node.id;
                bool shutdown_failed = false;
                try {
                    node = co_await exec_direct_command(std::move(node), raft_topology_cmd::command::barrier);
                } catch (...) {
                    rtlogger.warn("failed to tell node {} to shut down - it may hang."
                                 " It's safe to shut it down manually now. (Exception: {})",
                                 node.id, std::current_exception());
                    shutdown_failed = true;
                }
                if (shutdown_failed) {
                    node = retake_node(co_await start_operation(), node_id);
                }

                // Remove the node from group0 here - in general, it won't be able to leave on its own
                // because we'll ban it as soon as we tell it to shut down.
                node = retake_node(co_await remove_from_group0(std::move(node.guard), node.id), node.id);

                std::vector<canonical_mutation> muts;

                topology_mutation_builder builder(node.guard.write_timestamp());
                cleanup_ignored_nodes_on_left(builder, node.id);
                builder.del_transition_state()
                       .with_node(node.id)
                       .set("node_state", node_state::left);
                muts.push_back(builder.build());
                co_await db::view::view_builder::generate_mutations_on_node_left(_db, _sys_ks, node.guard.write_timestamp(), locator::host_id(node.id.uuid()), muts);
                auto str = node.rs->state == node_state::decommissioning
                        ? ::format("finished decommissioning node {}", node.id)
                        : ::format("finished rollback of {} after {} failure", node.id, node.rs->state);
                co_await update_topology_state(take_guard(std::move(node)), std::move(muts), std::move(str));
            }
                break;
            case topology::transition_state::rollback_to_normal: {
                auto node = get_node_to_work_on(std::move(guard));

                // The barrier waits for all double writes started during the operation to complete. It allowed to fail
                // since we will fence the requests later.
                bool barrier_failed = false;
                auto state = node.rs->state;
                try {
                    node.guard = co_await exec_global_command(std::move(node.guard),raft_topology_cmd::command::barrier_and_drain, get_excluded_nodes(node), drop_guard_and_retake::yes);
                } catch (term_changed_error&) {
                    throw;
                } catch(...) {
                    rtlogger.warn("failed to run barrier_and_drain during rollback of {} after {} failure: {}",
                            node.id, state, std::current_exception());
                    barrier_failed = true;
                }

                if (barrier_failed) {
                    node.guard = co_await start_operation();
                }

                node = retake_node(std::move(node.guard), node.id);

                topology_mutation_builder builder(node.guard.write_timestamp());
                topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
                builder.set_fence_version(_topo_sm._topology.version) // fence requests in case the drain above failed
                       .set_transition_state(topology::transition_state::tablet_migration) // in case tablet drain failed we need to complete tablet transitions
                       .with_node(node.id)
                       .set("node_state", node_state::normal);
                rtbuilder.done();

                auto str = fmt::format("complete rollback of {} to state normal after {} failure", node.id, node.rs->state);

                rtlogger.info("{}", str);
                co_await update_topology_state(std::move(node.guard), {builder.build(), rtbuilder.build()}, str);

                co_await _voter_handler.on_node_added(node.id, _as);
            }
                break;
            case topology::transition_state::truncate_table:
                co_await handle_truncate_table(std::move(guard));
                break;
        }
        co_return true;
    };

    // Called when there is no ongoing topology transition.
    // Used to start new topology transitions using node requests or perform node operations
    // that don't change the topology (like rebuild).
    future<> handle_node_transition(node_to_work_on&& node) {
        rtlogger.info("coordinator fiber found a node to work on id={} state={}", node.id, node.rs->state);

        switch (node.rs->state) {
            case node_state::none: {
                if (_topo_sm._topology.normal_nodes.empty()) {
                    rtlogger.info("skipping join node handshake for the first node in the cluster");
                } else {
                    auto validation_result = validate_joining_node(node);

                    if (utils::get_local_injector().enter("handle_node_transition_drop_expiring")) {
                        _gossiper.get_mutable_address_map().force_drop_expiring_entries();
                    }

                    // When the validation succeeded, it's important that all nodes in the
                    // cluster are aware of the IP address of the new node before we proceed to
                    // the topology::transition_state::join_group0 state, since in this state
                    // node IPs are already used to populate pending nodes in erm.
                    // This applies both to new and replacing nodes.
                    // If the wait_for_ip is unsuccessful, we should inform the new
                    // node about this failure.
                    // If the validation doesn't pass, we only need to call wait_for_ip on the current node,
                    // so that we can communicate the failure of the join request directly to
                    // the joining node.

                    {
                        std::exception_ptr wait_for_ip_error;
                        try {
                            if (holds_alternative<join_node_response_params::rejected>(validation_result)) {
                                release_guard(std::move(node.guard));
                                co_await wait_for_gossiper(node.id, _gossiper, _as);
                                node.guard = co_await start_operation();
                            } else {
                                auto exclude_nodes = get_excluded_nodes(node);
                                exclude_nodes.insert(node.id);
                                node.guard = co_await exec_global_command(std::move(node.guard),
                                    raft_topology_cmd::command::wait_for_ip,
                                    exclude_nodes);
                            }
                        } catch (term_changed_error&) {
                            throw;
                        } catch(...) {
                            wait_for_ip_error = std::current_exception();
                            rtlogger.warn("raft_topology_cmd::command::wait_for_ip failed, error {}",
                                wait_for_ip_error);
                        }
                        if (wait_for_ip_error) {
                            node.guard = co_await start_operation();
                        }
                        node = retake_node(std::move(node.guard), node.id);

                        if (wait_for_ip_error && holds_alternative<join_node_response_params::accepted>(validation_result)) {
                            validation_result = join_node_response_params::rejected {
                                .reason = ::format("wait_for_ip failed, error {}", wait_for_ip_error)
                            };
                        }
                    }

                    if (std::holds_alternative<join_node_response_params::rejected>(validation_result)) {
                        // Transition to left
                        topology_mutation_builder builder(node.guard.write_timestamp());
                        topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
                        builder.with_node(node.id)
                               .del("topology_request")
                               .set("node_state", node_state::left);
                        rtbuilder.done("Join is rejected during validation");
                        auto reason = ::format("bootstrap: node rejected");

                        co_await update_topology_state(std::move(node.guard), {builder.build(), rtbuilder.build()}, reason);

                        rtlogger.info("rejected node moved to left state {}", node.id);

                        try {
                            co_await respond_to_joining_node(node.id, join_node_response_params{
                                .response = std::move(validation_result),
                            });
                        } catch (const std::runtime_error& e) {
                            rtlogger.warn("attempt to send rejection response to {} failed. "
                                         "The node may hang. It's safe to shut it down manually now. Error: {}",
                                         node.id, e.what());
                        }

                        break;
                    }
                }
            }
            [[fallthrough]];
            case node_state::normal: {
                // if the state is none there have to be either 'join' or 'replace' request
                // if the state is normal there have to be either 'leave', 'remove' or 'rebuild' request
                topology_mutation_builder builder(node.guard.write_timestamp());
                topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
                rtbuilder.set("start_time", db_clock::now());
                switch (node.request.value()) {
                    case topology_request::join: {
                        SCYLLA_ASSERT(!node.rs->ring);
                        // Write chosen tokens through raft.
                        builder.set_transition_state(topology::transition_state::join_group0)
                               .with_node(node.id)
                               .set("node_state", node_state::bootstrapping)
                               .del("topology_request");
                        auto reason = ::format("bootstrap: accept node");
                        co_await update_topology_state(std::move(node.guard), {builder.build(), rtbuilder.build()}, reason);
                        break;
                        }
                    case topology_request::leave:
                        SCYLLA_ASSERT(node.rs->ring);
                        // start decommission and put tokens of decommissioning nodes into write_both_read_old state
                        // meaning that reads will go to the replica being decommissioned
                        // but writes will go to new owner as well
                        builder.set_transition_state(topology::transition_state::tablet_draining)
                               .set_version(_topo_sm._topology.version + 1)
                               .with_node(node.id)
                               .set("node_state", node_state::decommissioning)
                               .del("topology_request");
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build(), rtbuilder.build()},
                                                       "start decommission");
                        break;
                    case topology_request::remove: {
                        SCYLLA_ASSERT(node.rs->ring);

                        builder.set_transition_state(topology::transition_state::tablet_draining)
                               .set_version(_topo_sm._topology.version + 1)
                               .with_node(node.id)
                               .set("node_state", node_state::removing)
                               .del("topology_request");
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build(), rtbuilder.build()},
                                                       "start removenode");
                        break;
                        }
                    case topology_request::replace: {
                        SCYLLA_ASSERT(!node.rs->ring);

                        builder.set_transition_state(topology::transition_state::join_group0)
                               .with_node(node.id)
                               .set("node_state", node_state::replacing)
                               .del("topology_request");
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build(), rtbuilder.build()},
                                                       "replace: accept node");
                        break;
                        }
                    case topology_request::rebuild: {
                        topology_mutation_builder builder(node.guard.write_timestamp());
                        builder.with_node(node.id)
                               .set("node_state", node_state::rebuilding)
                               .del("topology_request");
                        co_await update_topology_state(take_guard(std::move(node)), {builder.build(), rtbuilder.build()},
                                                       "start rebuilding");
                        break;
                    }
                }
                break;
            }
            case node_state::rebuilding: {
                bool retake = false;
                auto id = node.id;
                topology_mutation_builder builder(node.guard.write_timestamp());
                topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
                try {
                    node = co_await exec_direct_command(std::move(node), raft_topology_cmd::command::stream_ranges);
                    rtbuilder.done();
                } catch (term_changed_error&) {
                    throw;
                } catch (raft::request_aborted& e) {
                    throw;
                } catch (...) {
                    rtlogger.error("send_raft_topology_cmd(stream_ranges) failed with exception"
                                    " (node state is rebuilding): {}", std::current_exception());
                    rtbuilder.done("streaming failed");
                    retake = true;
                }
                if (retake) {
                    node = retake_node(co_await start_operation(), id);
                }
                builder.del_session().with_node(node.id)
                       .set("node_state", node_state::normal)
                       .del("rebuild_option");
                co_await update_topology_state(take_guard(std::move(node)), {builder.build(), rtbuilder.build()}, "rebuilding completed");
            }
                break;
            case node_state::bootstrapping:
            case node_state::decommissioning:
            case node_state::removing:
            case node_state::replacing:
                // Should not get here
                on_fatal_internal_error(rtlogger, ::format(
                    "Found node {} in state {} but there is no ongoing topology transition",
                    node.id, node.rs->state));
            case node_state::left:
                // Should not get here
                on_fatal_internal_error(rtlogger, ::format(
                        "Topology coordinator is called for node {} in state 'left'", node.id));
                break;
        }
    };

    std::variant<join_node_response_params::accepted, join_node_response_params::rejected>
    validate_joining_node(const node_to_work_on& node) {
        if (*node.request == topology_request::replace) {
            auto replaced_id = std::get<replace_param>(node.req_param.value()).replaced_id;
            if (!_topo_sm._topology.normal_nodes.contains(replaced_id)) {
                return join_node_response_params::rejected {
                    .reason = ::format("Cannot replace node {} because it is not in the 'normal' state", replaced_id),
                };
            }
        }

        std::vector<sstring> unsupported_features;
        const auto& supported_features = node.rs->supported_features;
        std::ranges::set_difference(node.topology->enabled_features, supported_features, std::back_inserter(unsupported_features));
        if (!unsupported_features.empty()) {
            rtlogger.warn("node {} does not understand some features: {}", node.id, unsupported_features);
            return join_node_response_params::rejected{
                .reason = seastar::format("Feature check failed. The node does not support some features that are enabled by the cluster: {}",
                        unsupported_features),
            };
        }

        return join_node_response_params::accepted {};
    }

    // Tries to finish accepting the joining node by updating the cluster
    // configuration and sending the acceptance response.
    //
    // Returns the retaken node and information on whether responding to the
    // join request succeeded.
    future<std::tuple<node_to_work_on, bool>> finish_accepting_node(node_to_work_on&& node) {
        if (_topo_sm._topology.normal_nodes.empty()) {
            // This is the first node, it joins without the handshake.
            co_return std::tuple{std::move(node), true};
        }

        auto id = node.id;

        SCYLLA_ASSERT(!_topo_sm._topology.transition_nodes.empty());

        release_node(std::move(node));

        if (!_raft.get_configuration().contains(id)) {
            co_await _raft.modify_config({raft::config_member({id, {}}, {})}, {}, &_as);
        }

        auto responded = false;
        try {
            co_await respond_to_joining_node(id, join_node_response_params{
                .response = join_node_response_params::accepted{},
            });
            responded = true;
        } catch (const std::runtime_error& e) {
            rtlogger.warn("attempt to send acceptance response to {} failed. "
                         "The node may hang. It's safe to shut it down manually now. Error: {}",
                         node.id, e.what());
        }

        co_return std::tuple{retake_node(co_await start_operation(), id), responded};
    }

    future<> respond_to_joining_node(raft::server_id id, join_node_response_params&& params) {
        co_await ser::join_node_rpc_verbs::send_join_node_response(
            &_messaging, to_host_id(id), id,
            std::move(params)
        );
    }

    std::vector<canonical_mutation> mark_nodes_as_cleanup_needed(node_to_work_on& node, bool rollback) {
        auto& topo = _topo_sm._topology;
        std::vector<canonical_mutation> muts;
        muts.reserve(topo.normal_nodes.size());
        std::unordered_set<locator::host_id> dirty_nodes;

        for (auto& [_, erm] : _db.get_non_local_strategy_keyspaces_erms()) {
            const std::unordered_set<locator::host_id>& nodes = rollback ? erm->get_all_pending_nodes() : erm->get_dirty_endpoints();
            dirty_nodes.insert(nodes.begin(), nodes.end());
        }

        for (auto& n : dirty_nodes) {
            auto id = raft::server_id(n.uuid());
            // mark all nodes (except self) as cleanup needed
            if (node.id != id) {
                topology_mutation_builder builder(node.guard.write_timestamp());
                builder.with_node(id).set("cleanup_status", cleanup_status::needed);
                muts.emplace_back(builder.build());
                rtlogger.debug("mark node {} as needed for cleanup", id);
            }
        }
        return muts;
    }

    future<> start_cleanup_on_dirty_nodes(group0_guard guard, bool global_request) {
        auto& topo = _topo_sm._topology;
        std::vector<canonical_mutation> muts;
        muts.reserve(topo.normal_nodes.size() + size_t(global_request));

        if (global_request) {
            topology_mutation_builder builder(guard.write_timestamp());
            builder.del_global_topology_request();
            muts.emplace_back(builder.build());
        }
        for (auto& [id, rs] : topo.normal_nodes) {
            if (rs.cleanup == cleanup_status::needed) {
                topology_mutation_builder builder(guard.write_timestamp());
                builder.with_node(id).set("cleanup_status", cleanup_status::running);
                muts.emplace_back(builder.build());
                rtlogger.debug("mark node {} as cleanup running", id);
            }
        }
        if (!muts.empty()) {
          co_await update_topology_state(std::move(guard), std::move(muts), "Starting cleanup");
        }
    }

    // Returns the guard if no work done. Otherwise, performs a table migration and consumes the guard.
    future<std::optional<group0_guard>> maybe_migrate_system_tables(group0_guard guard);

    // Returns true if the state machine was transitioned into tablet migration path.
    future<bool> maybe_start_tablet_migration(group0_guard);

    // Returns true if the state machine was transitioned into tablet resize finalization path.
    future<bool> maybe_start_tablet_resize_finalization(group0_guard, const table_resize_plan& plan);

    future<> refresh_tablet_load_stats();
    future<> start_tablet_load_stats_refresher();

    // Precondition: the state machine upgrade state is not at upgrade_state::done.
    future<> do_upgrade_step(group0_guard);
    future<> build_coordinator_state(group0_guard);

    future<> await_event() {
        _as.check();
        co_await _topo_sm.event.when();
    }

    future<> fence_previous_coordinator();
    future<> rollback_current_topology_op(group0_guard&& guard);

    // Returns true if the coordinator should sleep after the exception.
    bool handle_topology_coordinator_error(std::exception_ptr eptr) noexcept;

public:
    topology_coordinator(
            sharded<db::system_distributed_keyspace>& sys_dist_ks, gms::gossiper& gossiper,
            netw::messaging_service& messaging, locator::shared_token_metadata& shared_tm,
            db::system_keyspace& sys_ks, replica::database& db, service::raft_group0& group0,
            service::topology_state_machine& topo_sm, abort_source& as, raft::server& raft_server,
            raft_topology_cmd_handler_type raft_topology_cmd_handler,
            tablet_allocator& tablet_allocator,
            std::chrono::milliseconds ring_delay,
            gms::feature_service& feature_service)
        : _sys_dist_ks(sys_dist_ks), _gossiper(gossiper), _messaging(messaging)
        , _shared_tm(shared_tm), _sys_ks(sys_ks), _db(db)
        , _group0(group0), _topo_sm(topo_sm), _as(as)
        , _feature_service(feature_service)
        , _raft(raft_server), _term(raft_server.get_current_term())
        , _raft_topology_cmd_handler(std::move(raft_topology_cmd_handler))
        , _tablet_allocator(tablet_allocator)
        , _tablet_load_stats_refresh([this] { return refresh_tablet_load_stats(); })
        , _ring_delay(ring_delay)
        , _group0_holder(_group0.hold_group0_gate())
        , _voter_handler(group0, topo_sm._topology, gossiper, feature_service)
        , _async_gate("topology_coordinator")
    {}

    // Returns true if the upgrade was done, returns false if upgrade was interrupted.
    future<bool> maybe_run_upgrade();
    future<> run();
    future<> stop();

    virtual void on_up(const gms::inet_address& endpoint, locator::host_id hid) { _topo_sm.event.broadcast(); };
    virtual void on_down(const gms::inet_address& endpoint, locator::host_id hid) { _topo_sm.event.broadcast(); };
};

future<std::optional<group0_guard>> topology_coordinator::maybe_migrate_system_tables(group0_guard guard) {
    // Check if we can upgrade the view_build_status table to v2, being managed by group0.
    // First we upgrade to an intermediate version v1_5 where we write to both tables, then
    // we upgrade to v2.
    const auto view_builder_version = co_await _sys_ks.get_view_builder_version();
    if (view_builder_version == db::system_keyspace::view_builder_version_t::v1 && _feature_service.view_build_status_on_group0) {
        rtlogger.info("Migrating view_builder to v1_5");
        auto tmptr = get_token_metadata_ptr();
        co_await db::view::view_builder::migrate_to_v1_5(tmptr, _sys_ks, _sys_ks.query_processor(), _group0.client(), _as, std::move(guard));
        co_return std::nullopt;
    }

    if (view_builder_version == db::system_keyspace::view_builder_version_t::v1_5) {
        if (!get_dead_nodes().empty()) {
            rtlogger.debug("Not all nodes are alive. Skipping system table migration until there are any dead nodes.");
            co_return std::move(guard);
        }

        rtlogger.info("Migrating view_builder to v2");
        // do a barrier to ensure all nodes applied the migration to v1_5 before we continue to v2
        guard = co_await exec_global_command(std::move(guard), raft_topology_cmd::command::barrier, {_raft.id()});

        auto tmptr = get_token_metadata_ptr();
        co_await db::view::view_builder::migrate_to_v2(tmptr, _sys_ks, _sys_ks.query_processor(), _group0.client(), _as, std::move(guard));
        co_return std::nullopt;
    }

    co_return std::move(guard);
}

future<bool> topology_coordinator::maybe_start_tablet_migration(group0_guard guard) {
    rtlogger.debug("Evaluating tablet balance");

    if (utils::get_local_injector().enter("tablet_load_stats_refresh_before_rebalancing")) {
        co_await _tablet_load_stats_refresh.trigger();
    }

    auto tm = get_token_metadata_ptr();
    auto plan = co_await _tablet_allocator.balance_tablets(tm, {}, get_dead_nodes());
    if (plan.empty()) {
        rtlogger.debug("Tablet load balancer did not make any plan");
        co_return false;
    }

    std::vector<canonical_mutation> updates;

    co_await generate_migration_updates(updates, guard, plan);

    // We only want to consider transitioning into tablet resize finalization path, if there's no other work
    // to be done (e.g. start migration or/and emit split decision).
    if (updates.empty()) {
        co_return co_await maybe_start_tablet_resize_finalization(std::move(guard), plan.resize_plan());
    }

    updates.emplace_back(
        topology_mutation_builder(guard.write_timestamp())
            .set_transition_state(topology::transition_state::tablet_migration)
            .set_version(_topo_sm._topology.version + 1)
            .build());

    co_await update_topology_state(std::move(guard), std::move(updates), "Starting tablet migration");
    co_return true;
}

future<bool> topology_coordinator::maybe_start_tablet_resize_finalization(group0_guard guard, const table_resize_plan& plan) {
    if (plan.finalize_resize.empty()) {
        co_return false;
    }
    if (utils::get_local_injector().enter("tablet_split_finalization_postpone")) {
        co_return false;
    }

    auto resize_finalization_transition_state = [this] {
        return _feature_service.tablet_merge ? topology::transition_state::tablet_resize_finalization : topology::transition_state::tablet_split_finalization;
    };

    std::vector<canonical_mutation> updates;

    updates.emplace_back(
        topology_mutation_builder(guard.write_timestamp())
            .set_transition_state(resize_finalization_transition_state())
            .set_version(_topo_sm._topology.version + 1)
            .build());

    co_await update_topology_state(std::move(guard), std::move(updates), "Started tablet resize finalization");
    co_return true;
}

future<> topology_coordinator::refresh_tablet_load_stats() {
    auto tm = get_token_metadata_ptr();

    locator::load_stats stats;
    static constexpr std::chrono::seconds wait_for_live_nodes_timeout{30};

    std::unordered_map<table_id, size_t> total_replicas;
    bool table_load_stats_invalid = false;

    for (auto& [dc, nodes] : tm->get_datacenter_token_owners_nodes()) {
        locator::load_stats dc_stats;
        rtlogger.debug("raft topology: Refreshing table load stats for DC {} that has {} token owners", dc, nodes.size());
        co_await coroutine::parallel_for_each(nodes, [&] (const auto& node) -> future<> {
            auto dst = node.get().host_id();
            auto dst_server = raft::server_id(dst.uuid());

            _as.check();

            // FIXME: if a node is down, completion status cannot be relied upon, but the average tablet size
            //  could still be inferred from the replicas available.

            auto timeout = netw::messaging_service::clock_type::now() + wait_for_live_nodes_timeout;

            abort_source as;
            auto request_abort = [&as] () mutable noexcept {
                as.request_abort();
            };
            auto t = timer<lowres_clock>(request_abort);
            t.arm(timeout);
            auto sub = _as.subscribe(request_abort);

            locator::load_stats node_stats;
            if (!_gossiper.is_alive(dst)) {
                if (_load_stats_per_node.contains(dst)) {
                    node_stats = _load_stats_per_node[dst];
                } else {
                    rtlogger.debug("raft topology: Unable to refresh table load on {} because it's down.", dst);
                    table_load_stats_invalid = true;
                    co_return;
                }
            } else if (_feature_service.tablet_load_stats_v2) {
                node_stats = co_await ser::storage_service_rpc_verbs::send_table_load_stats(&_messaging,
                                                                                            dst,
                                                                                            as,
                                                                                            dst_server);
            } else {
                node_stats = locator::load_stats::from_v1(
                    co_await ser::storage_service_rpc_verbs::send_table_load_stats_v1(&_messaging,
                                                                                      dst,
                                                                                      as,
                                                                                      dst_server));
            }

            _load_stats_per_node[dst] = node_stats;
            dc_stats += node_stats;
        });

        for (auto& [table_id, table_stats] : dc_stats.tables) {
            co_await coroutine::maybe_yield();

            auto& t = _db.find_column_family(table_id);
            auto& rs = t.get_effective_replication_map()->get_replication_strategy();
            if (!rs.uses_tablets()) {
                continue;
            }
            const auto* nts_ptr = dynamic_cast<const locator::network_topology_strategy*>(&rs);
            if (!nts_ptr) {
                on_internal_error(rtlogger, "Cannot convert replication_strategy that uses tablets into network_topology_strategy");
            }

            auto rf_for_this_dc = nts_ptr->get_replication_factor(dc);
            if (rf_for_this_dc <= 0) {
                continue;
            }
            total_replicas[table_id] += rf_for_this_dc;
            rtlogger.debug("raft topology: Refreshed table load stats for DC {}, table={}, RF={}, size_in_bytes={}, split_ready_seq_number={}",
                          dc, table_id, rf_for_this_dc, table_stats.size_in_bytes, table_stats.split_ready_seq_number);
        }

        stats += dc_stats;
    }

    for (auto& [table_id, table_load_stats] : stats.tables) {
        auto table_total_replicas = total_replicas.at(table_id);
        if (table_total_replicas == 0) {
            continue;
        }
        // Takes into account the RF of each DC, so we can compute the average total size
        // for a single table replica. This allows the load balancer to compute, in turn,
        // the average tablet size by dividing total size by tablet count.
        table_load_stats.size_in_bytes /= table_total_replicas;
    }

    if (table_load_stats_invalid) {
        stats.tables.clear();
    }

    rtlogger.debug("raft topology: Refreshed table load stats for all DC(s).");

    _tablet_allocator.set_load_stats(make_lw_shared<const locator::load_stats>(std::move(stats)));
}

future<> topology_coordinator::start_tablet_load_stats_refresher() {
    auto can_proceed = [this] { return !_async_gate.is_closed() && !_as.abort_requested(); };
    while (can_proceed()) {
        bool sleep = true;
        try {
            co_await _tablet_load_stats_refresh.trigger();
            _topo_sm.event.broadcast(); // wake up load balancer.
        } catch (raft::request_aborted&) {
            rtlogger.debug("raft topology: Tablet load stats refresher aborted");
            sleep = false;
        } catch (seastar::abort_requested_exception&) {
            rtlogger.debug("raft topology: Tablet load stats refresher aborted");
            sleep = false;
        } catch (...) {
            rtlogger.warn("Found error while refreshing load stats for tablets: {}, retrying...", std::current_exception());
        }
        auto refresh_interval = utils::get_local_injector().is_enabled("short_tablet_stats_refresh_interval") ?
                std::chrono::seconds(1) : tablet_load_stats_refresh_interval;
        if (sleep && can_proceed()) {
            try {
                co_await seastar::sleep_abortable(refresh_interval, _as);
            } catch (...) {
                rtlogger.debug("raft topology: Tablet load stats refresher: sleep failed: {}", std::current_exception());
            }
        }
    }
}

future<> topology_coordinator::do_upgrade_step(group0_guard guard) {
    switch (_topo_sm._topology.upgrade_state) {
    case topology::upgrade_state_type::not_upgraded:
        on_internal_error(rtlogger, std::make_exception_ptr(std::runtime_error(
                "topology_coordinator was started even though upgrade to raft topology was not requested yet")));

    case topology::upgrade_state_type::build_coordinator_state:
        utils::get_local_injector().inject("topology_coordinator_fail_to_build_state_during_upgrade", [] {
            throw std::runtime_error("failed to build topology coordinator state due to error injection");
        });
        co_await build_coordinator_state(std::move(guard));
        co_return;

    case topology::upgrade_state_type::done:
        on_internal_error(rtlogger, std::make_exception_ptr(std::runtime_error(
                "topology_coordinator::do_upgrade_step called after upgrade was completed")));
    }
}

future<> topology_coordinator::build_coordinator_state(group0_guard guard) {
    // Wait until all nodes reach use_post_raft_procedures
    rtlogger.info("waiting for all nodes to finish upgrade to raft schema");
    release_guard(std::move(guard));
    co_await _group0.wait_for_all_nodes_to_finish_upgrade(_as);

    auto tmptr = get_token_metadata_ptr();

    auto sl_version = co_await _sys_ks.get_service_levels_version();
    if (!sl_version || *sl_version < 2) {
        rtlogger.info("migrating service levels data");
        co_await qos::service_level_controller::migrate_to_v2(tmptr->get_normal_token_owners().size(), _sys_ks, _sys_ks.query_processor(), _group0.client(), _as);
    }

    auto auth_version = co_await _sys_ks.get_auth_version();
    if (auth_version < db::system_keyspace::auth_version_t::v2) {
        rtlogger.info("migrating system_auth keyspace data");
        co_await auth::migrate_to_auth_v2(_sys_ks, _group0.client(),
                [this] (abort_source&) { return start_operation();}, _as);
    }

    rtlogger.info("building initial raft topology state and CDC generation");
    guard = co_await start_operation();

    auto get_application_state = [&] (locator::host_id host_id, const gms::application_state_map& epmap, gms::application_state app_state) -> sstring {
        const auto it = epmap.find(app_state);
        if (it == epmap.end()) {
            throw std::runtime_error(format("failed to build initial raft topology state from gossip for node {}: application state {} is missing in gossip",
                    host_id, app_state));
        }
        // it's versioned_value::value(), not std::optional::value() - it does not throw
        return it->second.value();
    };

    // Create a new CDC generation
    auto get_sharding_info_for_host_id = [&] (locator::host_id host_id) -> std::pair<size_t, uint8_t> {
        const auto eptr = _gossiper.get_endpoint_state_ptr(host_id);
        if (!eptr) {
            throw std::runtime_error(format("no gossiper endpoint state for node {}", host_id));
        }
        const auto& epmap = eptr->get_application_state_map();
        const auto shard_count = std::stoi(get_application_state(host_id, epmap, gms::application_state::SHARD_COUNT));
        const auto ignore_msb = std::stoi(get_application_state(host_id, epmap, gms::application_state::IGNORE_MSB_BITS));
        return std::make_pair<size_t, uint8_t>(shard_count, ignore_msb);
    };
    auto [cdc_gen_uuid, guard_, mutation] = co_await prepare_and_broadcast_cdc_generation_data(tmptr, std::move(guard), std::nullopt, get_sharding_info_for_host_id);
    guard = std::move(guard_);

    topology_mutation_builder builder(guard.write_timestamp());

    std::set<sstring> enabled_features;

    // Build per-node state
    for (const auto& node: tmptr->get_topology().get_nodes()) {
        if (!node.get().is_member()) {
            continue;
        }

        const auto& host_id = node.get().host_id();
        const auto eptr = _gossiper.get_endpoint_state_ptr(host_id);
        if (!eptr) {
            throw std::runtime_error(format("failed to build initial raft topology state from gossip for node {} as gossip contains no data for it", host_id));
        }

        const auto& epmap = eptr->get_application_state_map();

        const auto datacenter = get_application_state(host_id, epmap, gms::application_state::DC);
        const auto rack = get_application_state(host_id, epmap, gms::application_state::RACK);
        const auto tokens_v = tmptr->get_tokens(host_id);
        const std::unordered_set<dht::token> tokens(tokens_v.begin(), tokens_v.end());
        const auto release_version = get_application_state(host_id, epmap, gms::application_state::RELEASE_VERSION);
        const auto num_tokens = tokens.size();
        const auto shard_count = get_application_state(host_id, epmap, gms::application_state::SHARD_COUNT);
        const auto ignore_msb = get_application_state(host_id, epmap, gms::application_state::IGNORE_MSB_BITS);
        const auto supported_features_s = get_application_state(host_id, epmap, gms::application_state::SUPPORTED_FEATURES);
        const auto supported_features = gms::feature_service::to_feature_set(supported_features_s);

        if (enabled_features.empty()) {
            enabled_features = supported_features;
        } else {
            std::erase_if(enabled_features, [&] (const sstring& f) { return !supported_features.contains(f); });
        }

        builder.with_node(raft::server_id(host_id.uuid()))
                .set("datacenter", datacenter)
                .set("rack", rack)
                .set("tokens", tokens)
                .set("node_state", node_state::normal)
                .set("release_version", release_version)
                .set("num_tokens", (uint32_t)num_tokens)
                .set("tokens_string", "")
                .set("shard_count", (uint32_t)std::stoi(shard_count))
                .set("ignore_msb", (uint32_t)std::stoi(ignore_msb))
                .set("cleanup_status", cleanup_status::clean)
                .set("request_id", utils::UUID())
                .set("supported_features", supported_features);

        rtlogger.debug("node {} will contain the following parameters: "
                "datacenter={}, rack={}, tokens={}, shard_count={}, ignore_msb={}, supported_features={}",
                host_id, datacenter, rack, tokens, shard_count, ignore_msb, supported_features);
    }

    // Build the static columns
    const bool add_ts_delay = true; // This is not the first generation, so add delay
    auto cdc_gen_ts = cdc::new_generation_timestamp(add_ts_delay, _ring_delay);

    const cdc::generation_id_v2 cdc_gen_id {
        .ts = cdc_gen_ts,
        .id = cdc_gen_uuid,
    };

    builder.set_version(topology::initial_version)
            .set_fence_version(topology::initial_version)
            .add_new_committed_cdc_generation(cdc_gen_id)
            .add_enabled_features(std::move(enabled_features));

    // Commit
    builder.set_upgrade_state(topology::upgrade_state_type::done);
    auto reason = "upgrade: build the initial state";
    co_await update_topology_state(std::move(guard), {std::move(mutation), builder.build()}, reason);
}

future<> topology_coordinator::fence_previous_coordinator() {
    // Write empty change to make sure that a guard taken by any previous coordinator cannot
    // be used to do a successful write any more. Otherwise the following can theoretically happen
    // while a coordinator tries to execute RPC R and move to state S.
    // 1. Leader A executes topology RPC R
    // 2. Leader A takes guard G
    // 3. Leader A calls update_topology_state(S)
    // 4. Leadership moves to B (while update_topology_state is still running)
    // 5. B executed topology RPC R again
    // 6. while the RPC is running leadership moves to A again
    // 7. A completes update_topology_state(S)
    // Topology state machine moves to state S while RPC R is still running.
    // If RPC is idempotent that should not be a problem since second one executed by B will do nothing,
    // but better to be safe and cut off previous write attempt
    while (!_as.abort_requested()) {
        try {
            auto guard = co_await start_operation();
            topology_mutation_builder builder(guard.write_timestamp());
            co_await update_topology_state(std::move(guard), {builder.build()}, fmt::format("Starting new topology coordinator {}", _group0.group0_server().id()));
            break;
        } catch (group0_concurrent_modification&) {
            // If we failed to write because of concurrent modification lets retry
            continue;
        } catch (raft::request_aborted&) {
            // Abort was requested. Break the loop
            rtlogger.debug("request to fence previous coordinator was aborted");
            break;
        } catch (...) {
            rtlogger.error("failed to fence previous coordinator {}", std::current_exception());
        }
        try {
            co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
        } catch (abort_requested_exception&) {
            // Abort was requested. Break the loop
            break;
        } catch (...) {
            rtlogger.debug("sleep failed while fencing previous coordinator: {}", std::current_exception());
        }
    }
}

future<> topology_coordinator::rollback_current_topology_op(group0_guard&& guard) {
    rtlogger.info("start rolling back topology change");
    // Look for a node which operation should be aborted
    // (there should be one since we are in the rollback)
    node_to_work_on node = get_node_to_work_on(std::move(guard));
    topology::transition_state transition_state;

    switch (node.rs->state) {
        case node_state::bootstrapping:
            [[fallthrough]];
        case node_state::replacing:
            // To rollback bootstrap of replace just move the topology to left_token_ring. The node we tried to
            // add will be removed from the group0 by the transition handler. It will also be notified to shutdown.
            transition_state = topology::transition_state::left_token_ring;
            break;
        case node_state::removing: {
            auto id = node.id;
            // The node was removed already. We need to add it back. Lets do it as non voter.
            // If it ever boots again it will make itself a voter.
            release_node(std::move(node));
            co_await _group0.group0_server().modify_config({raft::config_member{{id, {}}, false}}, {}, &_as);
            node = retake_node(co_await start_operation(), id);
        }
            [[fallthrough]];
        case node_state::decommissioning:
            // To rollback decommission or remove just move the topology to rollback_to_normal.
            transition_state = topology::transition_state::rollback_to_normal;
            break;
        default:
            on_internal_error(rtlogger, fmt::format("tried to rollback in unsupported state {}", node.rs->state));
    }

    topology_mutation_builder builder(node.guard.write_timestamp());
    topology_request_tracking_mutation_builder rtbuilder(node.rs->request_id);
    builder.set_transition_state(transition_state)
           .set_version(_topo_sm._topology.version + 1);
    rtbuilder.set("error", fmt::format("Rolled back: {}", *_rollback));

    std::vector<canonical_mutation> muts;
    // We are in the process of aborting remove or decommission which may have streamed some
    // ranges to other nodes. Cleanup is needed.
    muts = mark_nodes_as_cleanup_needed(node, true);
    muts.emplace_back(builder.build());
    muts.emplace_back(rtbuilder.build());

    std::string str = fmt::format("rollback {} after {} failure, moving transition state to {} and setting cleanup flag",
            node.id, node.rs->state, transition_state);
    rtlogger.info("{}", str);
    co_await update_topology_state(std::move(node.guard), std::move(muts), str);
}

bool topology_coordinator::handle_topology_coordinator_error(std::exception_ptr eptr) noexcept {
    try {
        std::rethrow_exception(std::move(eptr));
    } catch (raft::request_aborted&) {
        rtlogger.debug("topology change coordinator fiber aborted");
    } catch (seastar::abort_requested_exception&) {
        rtlogger.debug("topology change coordinator fiber aborted");
    } catch (raft::commit_status_unknown&) {
        rtlogger.warn("topology change coordinator fiber got commit_status_unknown");
    } catch (group0_concurrent_modification&) {
        rtlogger.info("topology change coordinator fiber got group0_concurrent_modification");
    } catch (topology_coordinator::term_changed_error&) {
        // Term changed. We may no longer be a leader
        rtlogger.debug("topology change coordinator fiber notices term change {} -> {}", _term, _raft.get_current_term());
    } catch (...) {
        rtlogger.error("topology change coordinator fiber got error {}", std::current_exception());
        return true;
    }
    return false;
}

future<bool> topology_coordinator::maybe_run_upgrade() {
    if (_topo_sm._topology.upgrade_state == topology::upgrade_state_type::done) {
        // Upgrade was already done, nothing to do
        co_return true;
    }

    rtlogger.info("topology coordinator fiber is upgrading the cluster to raft topology mode");

    auto abort = _as.subscribe([this] () noexcept {
        _topo_sm.event.broadcast();
    });

    while (!_as.abort_requested() && _topo_sm._topology.upgrade_state != topology::upgrade_state_type::done) {
        bool sleep = false;
        try {
            auto guard = co_await start_operation();
            co_await do_upgrade_step(std::move(guard));
        } catch (...) {
            sleep = handle_topology_coordinator_error(std::current_exception());
        }
        if (sleep) {
            try {
                co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                rtlogger.debug("sleep failed: {}", std::current_exception());
            }
        }
        co_await coroutine::maybe_yield();
    }

    co_return !_as.abort_requested();
}

future<> topology_coordinator::run() {
    auto abort = _as.subscribe([this] () noexcept {
        _topo_sm.event.broadcast();
    });

    co_await fence_previous_coordinator();
    auto cdc_generation_publisher = cdc_generation_publisher_fiber();
    auto tablet_load_stats_refresher = start_tablet_load_stats_refresher();
    auto gossiper_orphan_remover = gossiper_orphan_remover_fiber();
    auto group0_voter_refresher = group0_voter_refresher_fiber();

    while (!_as.abort_requested()) {
        bool sleep = false;
        try {
            co_await utils::get_local_injector().inject("topology_coordinator_pause_before_processing_backlog", utils::wait_for_message(5min));
            auto guard = co_await cleanup_group0_config_if_needed(co_await start_operation());

            if (_rollback) {
                co_await rollback_current_topology_op(std::move(guard));
                _rollback = std::nullopt;
                continue;
            }

            bool had_work = co_await handle_topology_transition(std::move(guard));

            if (!had_work) {
                // Nothing to work on. Wait for topology change event.
                rtlogger.debug("topology coordinator fiber has nothing to do. Sleeping.");
                co_await await_event();
                rtlogger.debug("topology coordinator fiber got an event");
            }
            co_await utils::get_local_injector().inject("wait-after-topology-coordinator-gets-event", utils::wait_for_message(30s));
        } catch (...) {
            sleep = handle_topology_coordinator_error(std::current_exception());
        }
        if (sleep) {
            try {
                co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                rtlogger.debug("sleep failed: {}", std::current_exception());
            }
        }
        co_await coroutine::maybe_yield();
    }

    co_await _async_gate.close();
    co_await std::move(tablet_load_stats_refresher);
    co_await _tablet_load_stats_refresh.join();
    co_await std::move(cdc_generation_publisher);
    co_await std::move(gossiper_orphan_remover);
    co_await std::move(group0_voter_refresher);
}

future<> topology_coordinator::stop() {
    // if topology_coordinator::run() is aborted either because we are not a
    // leader anymore, or we are shutting down as a leader, we have to handle
    // futures in _tablets in case any of them failed, before these failures
    // are checked by future's destructor.
    co_await coroutine::parallel_for_each(_tablets, [] (auto& tablet) -> future<> {
        auto& [gid, tablet_state] = tablet;
        rtlogger.debug("Checking tablet migration state for {}", gid);

        auto stop_background_action = [] (background_action_holder& holder, locator::global_tablet_id gid, std::function<std::string()> fmt_desc) -> future<> {
            if (holder) {
                try {
                    co_await std::move(*holder);
                } catch (...) {
                    rtlogger.warn("Tablet '{}' migration failed {}: {}",
                                    gid, fmt_desc(), std::current_exception());
                }
            }
        };

        // we should have at most only a single active barrier for each tablet,
        // but let's check all of them because we never reset these holders
        // once they are added as barriers
        for (auto& [stage, barrier]: tablet_state.barriers) {
            SCYLLA_ASSERT(barrier.has_value());
            co_await stop_background_action(barrier, gid, [stage] { return format("at stage {}", tablet_transition_stage_to_string(stage)); });
        }

        co_await stop_background_action(tablet_state.streaming, gid, [] { return "during streaming"; });
        co_await stop_background_action(tablet_state.cleanup, gid, [] { return "during cleanup"; });
        co_await stop_background_action(tablet_state.rebuild_repair, gid, [] { return "during rebuild_repair"; });
        co_await stop_background_action(tablet_state.repair, gid, [] { return "during repair"; });
    });
}

future<> run_topology_coordinator(
        seastar::sharded<db::system_distributed_keyspace>& sys_dist_ks, gms::gossiper& gossiper,
        netw::messaging_service& messaging, locator::shared_token_metadata& shared_tm,
        db::system_keyspace& sys_ks, replica::database& db, service::raft_group0& group0,
        service::topology_state_machine& topo_sm, seastar::abort_source& as, raft::server& raft,
        raft_topology_cmd_handler_type raft_topology_cmd_handler,
        tablet_allocator& tablet_allocator,
        std::chrono::milliseconds ring_delay,
        endpoint_lifecycle_notifier& lifecycle_notifier,
        gms::feature_service& feature_service) {

    topology_coordinator coordinator{
            sys_dist_ks, gossiper, messaging, shared_tm,
            sys_ks, db, group0, topo_sm, as, raft,
            std::move(raft_topology_cmd_handler),
            tablet_allocator,
            ring_delay,
            feature_service};

    std::exception_ptr ex;
    lifecycle_notifier.register_subscriber(&coordinator);
    try {
        rtlogger.info("start topology coordinator fiber");
        const bool upgrade_done = co_await coordinator.maybe_run_upgrade();
        if (upgrade_done) {
            co_await with_scheduling_group(group0.get_scheduling_group(), [&] {
                return coordinator.run();
            });
        }
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        try {
            if (raft.is_leader()) {
                rtlogger.warn("unhandled exception in topology_coordinator::run: {}; stepping down as a leader", ex);
                const auto stepdown_timeout_ticks = std::chrono::seconds(5) / raft_tick_interval;
                co_await raft.stepdown(raft::logical_clock::duration(stepdown_timeout_ticks));
            }
        } catch (...) {
            rtlogger.error("failed to step down before aborting: {}", std::current_exception());
        }
        on_fatal_internal_error(rtlogger, format("unhandled exception in topology_coordinator::run: {}", ex));
    }
    co_await lifecycle_notifier.unregister_subscriber(&coordinator);
    co_await coordinator.stop();
}

} // namespace service
