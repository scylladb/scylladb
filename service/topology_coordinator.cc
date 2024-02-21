/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

#include "auth/service.hh"
#include "cdc/generation.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/system_keyspace.hh"
#include "dht/boot_strapper.hh"
#include "gms/gossiper.hh"
#include "locator/tablets.hh"
#include "locator/token_metadata.hh"
#include "locator/network_topology_strategy.hh"
#include "message/messaging_service.hh"
#include "replica/database.hh"
#include "replica/tablet_mutation_builder.hh"
#include "replica/tablets.hh"
#include "service/raft/join_node.hh"
#include "service/raft/raft_address_map.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/tablet_allocator.hh"
#include "service/topology_state_machine.hh"
#include "topology_mutation.hh"
#include "utils/error_injection.hh"
#include "service/endpoint_lifecycle_subscriber.hh"

#include "idl/join_node.dist.hh"
#include "idl/storage_service.dist.hh"

#include "service/topology_coordinator.hh"

using token = dht::token;
using inet_address = gms::inet_address;

namespace service {

logging::logger rtlogger("raft_topology");

future<inet_address> wait_for_ip(raft::server_id id, const raft_address_map& am, abort_source& as) {
    const auto timeout = std::chrono::seconds{30};
    const auto deadline = lowres_clock::now() + timeout;
    while (true) {
        const auto ip = am.find(id);
        if (ip) {
            co_return *ip;
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

class topology_coordinator : public endpoint_lifecycle_subscriber {
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    gms::gossiper& _gossiper;
    netw::messaging_service& _messaging;
    locator::shared_token_metadata& _shared_tm;
    db::system_keyspace& _sys_ks;
    replica::database& _db;
    service::raft_group0& _group0;
    const service::raft_address_map& _address_map;
    service::topology_state_machine& _topo_sm;
    abort_source& _as;

    raft::server& _raft;
    const raft::term_t _term;
    uint64_t _last_cmd_index = 0;

    raft_topology_cmd_handler_type _raft_topology_cmd_handler;

    tablet_allocator& _tablet_allocator;

    // The reason load_stats_ptr is a shared ptr is that load balancer can yield, and we don't want it
    // to suffer lifetime issues when stats refresh fiber overrides the current stats.
    locator::load_stats_ptr _tablet_load_stats;
    // FIXME: make frequency per table in order to reduce work in each iteration.
    //  Bigger tables will take longer to be resized. similar-sized tables can be batched into same iteration.
    static constexpr std::chrono::seconds tablet_load_stats_refresh_interval = std::chrono::seconds(60);

    std::chrono::milliseconds _ring_delay;

    using drop_guard_and_retake = bool_class<class retake_guard_tag>;

    // Engaged if an ongoing topology change should be rolled back. The string inside
    // will indicate a reason for the rollback.
    std::optional<sstring> _rollback;

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
            auto to_remove = boost::copy_range<std::vector<raft::server_id>>(
                    rconf.current
                    | boost::adaptors::transformed([&] (const raft::config_member& m) { return m.addr.id; })
                    | boost::adaptors::filtered([&] (const raft::server_id& id) { return topo.left_nodes.contains(id); }));
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
                alive = _gossiper.is_alive(id2ip(locator::host_id(n.first.uuid())));
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
                alive = _gossiper.is_alive(id2ip(locator::host_id(n.first.uuid())));
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
        auto guard = co_await _group0.client().start_operation(&_as);

        if (_term != _raft.get_current_term()) {
            throw term_changed_error{};
        }

        co_return std::move(guard);
    }

    void release_node(std::optional<node_to_work_on> node) {
        // Leaving the scope destroys the object and releases the guard.
    }

    node_to_work_on retake_node(group0_guard guard, raft::server_id id) {
        auto& topo = _topo_sm._topology;

        auto it = topo.find(id);
        assert(it);

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
            co_await _group0.client().add_entry(std::move(g0_cmd), std::move(guard), &_as);
        } catch (group0_concurrent_modification&) {
            rtlogger.info("race while changing state: {}. Retrying", reason);
            throw;
        }
    };

    raft::server_id parse_replaced_node(const std::optional<request_param>& req_param) const {
        return service::topology::parse_replaced_node(req_param);
    }

    inet_address id2ip(locator::host_id id) const {
        auto ip = _address_map.find(raft::server_id(id.uuid()));
        if (!ip) {
            throw std::runtime_error(::format("no ip address mapping for {}", id));
        }
        return *ip;
    }

    future<> exec_direct_command_helper(raft::server_id id, uint64_t cmd_index, const raft_topology_cmd& cmd) {
        auto ip = _address_map.find(id);
        if (!ip) {
            rtlogger.warn("cannot send command {} with term {} and index {} "
                         "to {} because mapping to ip is not available",
                         cmd.cmd, _term, cmd_index, id);
            co_await coroutine::exception(std::make_exception_ptr(
                    std::runtime_error(::format("no ip address mapping for {}", id))));
        }
        rtlogger.debug("send {} command with term {} and index {} to {}/{}",
            cmd.cmd, _term, cmd_index, id, *ip);
        auto result = _db.get_token_metadata().get_topology().is_me(*ip) ?
                    co_await _raft_topology_cmd_handler(_term, cmd_index, cmd) :
                    co_await ser::storage_service_rpc_verbs::send_raft_topology_cmd(
                            &_messaging, netw::msg_addr{*ip}, id, _term, cmd_index, cmd);
        if (result.status == raft_topology_cmd_result::command_status::fail) {
            co_await coroutine::exception(std::make_exception_ptr(
                    std::runtime_error(::format("failed status returned from {}/{}", id, *ip))));
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
            | boost::adaptors::filtered([&cmd, &exclude_nodes] (const std::pair<const raft::server_id, replica_state>& n) {
                // We must send barrier_and_drain to the decommissioning node as it might be coordinating requests.
                bool drain_decommissioning_node = cmd.cmd == raft_topology_cmd::command::barrier_and_drain
                        && n.second.state == node_state::decommissioning;
                return !exclude_nodes.contains(n.first) && (n.second.state == node_state::normal || drain_decommissioning_node);
            })
            | boost::adaptors::map_keys;
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
        co_await _group0.remove_from_raft_config(id);
        rtlogger.info("node {} removed from group 0 configuration", id);
        co_return co_await start_operation();
    }

    future<> step_down_as_nonvoter() {
        // Become a nonvoter which triggers a leader stepdown.
        co_await _group0.become_nonvoter(_as);
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

    // Deletes obsolete CDC generations. These are the generations that stopped operating more than 24 hours ago.
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
        // stopped operating more than 5 s ago on all nodes. Since their clocks can be desynchronized,
        // we delete only generations that stopped operating more than 24 hours ago, which is a safe and
        // sufficient choice.
        auto ts_upper_bound = db_clock::now() - std::chrono::days(1);
        utils::get_local_injector().inject("clean_obsolete_cdc_generations_change_ts_ub", [&] {
            ts_upper_bound = db_clock::now();
        });

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

        reason += ::format("deleted data of CDC generations with time UUID not exceeding {}", id_upper_bound);
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
            });

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
            } catch (seastar::abort_requested_exception) {
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
        bool drain_failed = false;
        try {
            guard = co_await exec_global_command(std::move(guard), raft_topology_cmd::command::barrier_and_drain, exclude_nodes, drop_guard_and_retake::yes);
        } catch (...) {
            rtlogger.error("drain rpc failed, proceed to fence old writes: {}", std::current_exception());
            drain_failed = true;
        }
        if (drain_failed) {
            guard = co_await start_operation();
        }
        topology_mutation_builder builder(guard.write_timestamp());
        builder.set_fence_version(_topo_sm._topology.version);
        auto reason = ::format("advance fence version to {}", _topo_sm._topology.version);
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
        background_action_holder cleanup;
        std::unordered_map<locator::tablet_transition_stage, background_action_holder> barriers;
    };

    std::unordered_map<locator::global_tablet_id, tablet_migration_state> _tablets;

    // Set to true when any action started on behalf of a background_action_holder
    // for any tablet finishes, or fails and needs to be restarted.
    bool _tablets_ready = false;

    seastar::gate _async_gate;

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
            for (auto&& [tablet, trinfo]: tmap.transitions()) {
                co_await coroutine::maybe_yield();
                auto gid = locator::global_tablet_id {table, tablet};
                func(tmap, s, gid, trinfo);
            }
        }
    }

    bool is_excluded(raft::server_id server_id) const {
        return _topo_sm._topology.get_excluded_nodes().contains(server_id);
    }

    void generate_migration_update(std::vector<canonical_mutation>& out, const group0_guard& guard, const tablet_migration_info& mig) {
        auto& tmap = get_token_metadata_ptr()->tablets().get_tablet_map(mig.tablet.table);
        auto last_token = tmap.get_last_token(mig.tablet.tablet);
        if (tmap.get_tablet_transition_info(mig.tablet.tablet)) {
            rtlogger.warn("Tablet already in transition, ignoring migration: {}", mig);
            return;
        }
        out.emplace_back(
            replica::tablet_mutation_builder(guard.write_timestamp(), mig.tablet.table)
                .set_new_replicas(last_token, locator::get_new_replicas(tmap.get_tablet_info(mig.tablet.tablet), mig))
                .set_stage(last_token, locator::tablet_transition_stage::allow_write_both_read_old)
                .set_transition(last_token, mig.kind)
                .build());
    }

    future<> generate_migration_updates(std::vector<canonical_mutation>& out, const group0_guard& guard, const migration_plan& plan) {
        std::unordered_set<table_id> new_transitions;
        for (const tablet_migration_info& mig : plan.migrations()) {
            co_await coroutine::maybe_yield();
            generate_migration_update(out, guard, mig);
            new_transitions.insert(mig.tablet.table);
        }

        for (auto [table_id, resize_decision] : plan.resize_plan().resize) {
            auto s = _db.find_schema(table_id);
            auto& tmap = get_token_metadata_ptr()->tablets().get_tablet_map(table_id);
            // Sequence number is monotonically increasing, globally. Therefore, it can be used to identify a decision.
            resize_decision.sequence_number = tmap.resize_decision().next_sequence_number();
            rtlogger.debug("Generating resize decision for table {} of type {} and sequence number {}",
                           table_id, resize_decision.type_name(), resize_decision.sequence_number);
            out.emplace_back(
                replica::tablet_mutation_builder(guard.write_timestamp(), table_id)
                    .set_resize_decision(std::move(resize_decision))
                    .build());
        }

        // FIXME: Finalize split requests when exiting the tablet migration track.
        for (auto table_id : plan.resize_plan().finalize_resize) {
            auto& tmap = get_token_metadata_ptr()->tablets().get_tablet_map(table_id);
            // Only finalize split request if there's no ongoing transition or new ones for a given table.
            if (tmap.transitions().size() > 0 || new_transitions.contains(table_id)) {
                continue;
            }

            auto s = _db.find_schema(table_id);
            auto new_tablet_map = co_await _tablet_allocator.split_tablets(get_token_metadata_ptr(), table_id);
            out.emplace_back(co_await replica::tablet_map_to_mutation(
                new_tablet_map,
                table_id,
                s->ks_name(),
                s->cf_name(),
                guard.write_timestamp()));
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
                            transition_to_with_barrier(locator::tablet_transition_stage::revert_migration);
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
                    transition_to_with_barrier(locator::tablet_transition_stage::streaming);
                    break;
                // The state "streaming" is needed to ensure that stale stream_tablet() RPC doesn't
                // get admitted before global_tablet_token_metadata_barrier() is finished for earlier
                // stage in case of coordinator failover.
                case locator::tablet_transition_stage::streaming:
                    if (drain) {
                        utils::get_local_injector().inject("stream_tablet_fail_on_drain",
                                        [] { throw std::runtime_error("stream_tablet failed due to error injection"); });
                    }

                    if (action_failed(tablet_state.streaming)) {
                        if (check_excluded_replicas()) {
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

                    if (advance_in_background(gid, tablet_state.streaming, "streaming", [&] {
                        rtlogger.info("Initiating tablet streaming ({}) of {} to {}", trinfo.transition, gid, trinfo.pending_replica);
                        auto dst = trinfo.pending_replica.host;
                        return ser::storage_service_rpc_verbs::send_tablet_stream_data(&_messaging,
                                   netw::msg_addr(id2ip(dst)), _as, raft::server_id(dst.uuid()), gid);
                    })) {
                        rtlogger.debug("Will set tablet {} stage to {}", gid, locator::tablet_transition_stage::write_both_read_new);
                        updates.emplace_back(get_mutation_builder()
                            .set_stage(last_token, locator::tablet_transition_stage::write_both_read_new)
                            .del_session(last_token)
                            .build());
                    }
                    break;
                case locator::tablet_transition_stage::write_both_read_new: {
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
                        if (excluded_new > excluded_old) {
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
                case locator::tablet_transition_stage::cleanup:
                    if (advance_in_background(gid, tablet_state.cleanup, "cleanup", [&] {
                        locator::tablet_replica dst = locator::get_leaving_replica(tmap.get_tablet_info(gid.tablet), trinfo);
                        if (is_excluded(raft::server_id(dst.host.uuid()))) {
                            rtlogger.info("Tablet cleanup of {} on {} skipped because node is excluded", gid, dst);
                            return make_ready_future<>();
                        }
                        rtlogger.info("Initiating tablet cleanup of {} on {}", gid, dst);
                        return ser::storage_service_rpc_verbs::send_tablet_cleanup(&_messaging,
                                                                                   netw::msg_addr(id2ip(dst.host)), _as, raft::server_id(dst.host.uuid()), gid);
                    })) {
                        transition_to(locator::tablet_transition_stage::end_migration);
                    }
                    break;
                case locator::tablet_transition_stage::cleanup_target:
                    if (advance_in_background(gid, tablet_state.cleanup, "cleanup_target", [&] {
                        locator::tablet_replica dst = trinfo.pending_replica;
                        if (is_excluded(raft::server_id(dst.host.uuid()))) {
                            rtlogger.info("Tablet cleanup of {} on {} skipped because node is excluded and doesn't need to revert migration", gid, dst);
                            return make_ready_future<>();
                        }
                        rtlogger.info("Initiating tablet cleanup of {} on {} to revert migration", gid, dst);
                        return ser::storage_service_rpc_verbs::send_tablet_cleanup(&_messaging,
                                                                                   netw::msg_addr(id2ip(dst.host)), _as, raft::server_id(dst.host.uuid()), gid);
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
                                .build());
                    }
                    break;
                case locator::tablet_transition_stage::end_migration:
                    // Need a separate stage and a barrier after cleanup RPC to cut off stale RPCs.
                    // See do_tablet_operation() doc.
                    if (do_barrier()) {
                        _tablets.erase(gid);
                        updates.emplace_back(get_mutation_builder()
                                .del_transition(last_token)
                                .set_replicas(last_token, trinfo.next)
                                .build());
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
        if (!preempt) {
            auto plan = co_await _tablet_allocator.balance_tablets(get_token_metadata_ptr(), _tablet_load_stats, get_dead_nodes());
            if (!drain || plan.has_nodes_to_drain()) {
                co_await generate_migration_updates(updates, guard, plan);
            }
        }

        // The updates have to be executed under the same guard which was used to read tablet metadata
        // to ensure that we don't reinsert tablet rows which were concurrently deleted by schema change
        // which happens outside the topology coordinator.
        bool has_updates = !updates.empty();
        if (has_updates) {
            co_await utils::get_local_injector().inject("tablet_transition_updates", [] (auto& handler) {
                rtlogger.info("tablet_transition_updates: start");
                return handler.wait_for_message(db::timeout_clock::now() + std::chrono::minutes(2));
            });

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
            rtbuilder.done(fmt::format("Canceled. Dead nodes: {}", dead_nodes));
            switch (req) {
                case topology_request::replace:
                [[fallthrough]];
                case topology_request::join: {
                    node_builder.set("node_state", node_state::left);
                    reject_join.emplace_back(id);
                    try {
                        co_await wait_for_ip(id, _address_map, _as);
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

            // If there is no other work, evaluate load and start tablet migration if there is imbalance.
            if (co_await maybe_start_tablet_migration(std::move(guard))) {
                co_return true;
            }
            co_return false;
        }

        rtlogger.info("entered `{}` transition state", *tstate);
        switch (*tstate) {
            case topology::transition_state::join_group0: {
                auto [node, accepted] = co_await finish_accepting_node(get_node_to_work_on(std::move(guard)));

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
                        assert(!node.rs->ring);
                        auto num_tokens = std::get<join_param>(node.req_param.value()).num_tokens;
                        // A node have just been accepted and does not have tokens assigned yet
                        // Need to assign random tokens to the node
                        auto tmptr = get_token_metadata_ptr();
                        auto bootstrap_tokens = dht::boot_strapper::get_random_bootstrap_tokens(
                                tmptr, num_tokens, dht::check_token_endpoint::yes);

                        auto [gen_uuid, guard, mutation] = co_await prepare_and_broadcast_cdc_generation_data(
                                tmptr, take_guard(std::move(node)), bootstrapping_info{bootstrap_tokens, *node.rs});

                        topology_mutation_builder builder(guard.write_timestamp());

                        // Write the new CDC generation data through raft.
                        builder.set_transition_state(topology::transition_state::commit_cdc_generation)
                               .set_new_cdc_generation_data_uuid(gen_uuid)
                               .with_node(node.id)
                               .set("tokens", bootstrap_tokens);
                        auto reason = ::format(
                            "bootstrap: insert tokens and CDC generation data (UUID: {})", gen_uuid);
                        co_await update_topology_state(std::move(guard), {std::move(mutation), builder.build()}, reason);
                    }
                        break;
                    case node_state::replacing: {
                        assert(!node.rs->ring);
                        auto replaced_id = std::get<replace_param>(node.req_param.value()).replaced_id;
                        auto it = _topo_sm._topology.normal_nodes.find(replaced_id);
                        assert(it != _topo_sm._topology.normal_nodes.end());
                        assert(it->second.ring && it->second.state == node_state::normal);

                        topology_mutation_builder builder(node.guard.write_timestamp());

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
                try {
                    co_await handle_tablet_migration(std::move(guard), true);
                } catch (term_changed_error&) {
                    throw;
                } catch (group0_concurrent_modification&) {
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
                    if (node.rs->state == node_state::removing) {
                        co_await _group0.make_nonvoter(node.id, _as);
                    }

                    // If we decommission a node when the number of nodes is even, we make it a non-voter early.
                    // All majorities containing this node will remain majorities when we make this node a non-voter
                    // and remove it from the set because the required size of a majority decreases.
                    //
                    // FIXME: when a node restarts and notices it's a non-voter, it will become a voter again. If the
                    // node restarts during a decommission, and we want the decommission to continue (e.g. because it's
                    // at a finishing non-abortable step), we must ensure that the node doesn't become a voter.
                    if (node.rs->state == node_state::decommissioning
                            && raft::configuration::voter_count(_group0.group0_server().get_configuration().current) % 2 == 0) {
                        if (node.id == _raft.id()) {
                            rtlogger.info("coordinator is decommissioning and becomes a non-voter; "
                                         "giving up leadership");
                            co_await step_down_as_nonvoter();
                        } else {
                            co_await _group0.make_nonvoter(node.id, _as);
                        }
                    }
                }
                if (node.rs->state == node_state::replacing) {
                    // We make a replaced node a non-voter early, just like a removed node.
                    auto replaced_node_id = parse_replaced_node(node.req_param);
                    if (_group0.is_member(replaced_node_id, true)) {
                        co_await _group0.make_nonvoter(replaced_node_id, _as);
                    }
                }

                raft_topology_cmd cmd{raft_topology_cmd::command::stream_ranges};
                auto state = node.rs->state;
                try {
                    if (node.rs->state == node_state::removing) {
                        // tell all nodes to stream data of the removed node to new range owners
                        node = co_await exec_global_command(std::move(node), cmd);
                    } else {
                        // Tell joining/leaving/replacing node to stream its ranges
                        node = co_await exec_direct_command(std::move(node), cmd);
                    }
                } catch (term_changed_error&) {
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
                    }
                    break;
                case node_state::removing:
                    node = retake_node(co_await remove_from_group0(std::move(node.guard), node.id), node.id);
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

                    topology_mutation_builder builder1(node.guard.write_timestamp());
                    // Move new node to 'normal'
                    builder1.del_transition_state()
                            .set_version(_topo_sm._topology.version + 1)
                            .with_node(node.id)
                            .set("node_state", node_state::normal);

                    // Move old node to 'left'
                    topology_mutation_builder builder2(node.guard.write_timestamp());
                    cleanup_ignored_nodes_on_left(builder2, replaced_node_id);
                    builder2.with_node(replaced_node_id)
                            .del("tokens")
                            .set("node_state", node_state::left);
                    co_await update_topology_state(take_guard(std::move(node)), {builder1.build(), builder2.build(), rtbuilder.build()},
                                                  "replace: read fence completed");
                    }
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

                topology_mutation_builder builder(node.guard.write_timestamp());
                cleanup_ignored_nodes_on_left(builder, node.id);
                builder.del_transition_state()
                       .with_node(node.id)
                       .set("node_state", node_state::left);
                auto str = node.rs->state == node_state::decommissioning
                        ? ::format("finished decommissioning node {}", node.id)
                        : ::format("finished rollback of {} after {} failure", node.id, node.rs->state);
                co_await update_topology_state(take_guard(std::move(node)), {builder.build()}, std::move(str));
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
            }
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
                                co_await wait_for_ip(node.id, _address_map, _as);
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

                    if (auto* reject = std::get_if<join_node_response_params::rejected>(&validation_result)) {
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
                        assert(!node.rs->ring);
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
                        assert(node.rs->ring);
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
                        assert(node.rs->ring);

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
                        assert(!node.rs->ring);

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
                .reason = format("Feature check failed. The node does not support some features that are enabled by the cluster: {}",
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

        assert(!_topo_sm._topology.transition_nodes.empty());

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
        auto ip = id2ip(locator::host_id(id.uuid()));
        co_await ser::join_node_rpc_verbs::send_join_node_response(
            &_messaging, netw::msg_addr(ip), id,
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
                rtlogger.trace("mark node {} as needed cleanup", id);
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
                rtlogger.trace("mark node {} as cleanup running", id);
            }
        }
        if (!muts.empty()) {
          co_await update_topology_state(std::move(guard), std::move(muts), "Starting cleanup");
        }
    }


    // Returns true if the state machine was transitioned into tablet migration path.
    future<bool> maybe_start_tablet_migration(group0_guard);

    future<locator::load_stats> refresh_tablet_load_stats();
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
            std::chrono::milliseconds ring_delay)
        : _sys_dist_ks(sys_dist_ks), _gossiper(gossiper), _messaging(messaging)
        , _shared_tm(shared_tm), _sys_ks(sys_ks), _db(db)
        , _group0(group0), _address_map(_group0.address_map()), _topo_sm(topo_sm), _as(as)
        , _raft(raft_server), _term(raft_server.get_current_term())
        , _raft_topology_cmd_handler(std::move(raft_topology_cmd_handler))
        , _tablet_allocator(tablet_allocator)
        , _ring_delay(ring_delay)
    {}

    // Returns true if the upgrade was done, returns false if upgrade was interrupted.
    future<bool> maybe_run_upgrade();
    future<> run();

    virtual void on_join_cluster(const gms::inet_address& endpoint) {}
    virtual void on_leave_cluster(const gms::inet_address& endpoint) {};
    virtual void on_up(const gms::inet_address& endpoint) {};
    virtual void on_down(const gms::inet_address& endpoint) { _topo_sm.event.broadcast(); };
};

future<bool> topology_coordinator::maybe_start_tablet_migration(group0_guard guard) {
    rtlogger.debug("Evaluating tablet balance");

    if (utils::get_local_injector().enter("tablet_load_stats_refresh_before_rebalancing")) {
        _tablet_load_stats = make_lw_shared<const locator::load_stats>(co_await refresh_tablet_load_stats());
    }

    auto tm = get_token_metadata_ptr();
    auto plan = co_await _tablet_allocator.balance_tablets(tm, _tablet_load_stats, get_dead_nodes());
    if (plan.empty()) {
        rtlogger.debug("Tablets are balanced");
        co_return false;
    }

    std::vector<canonical_mutation> updates;

    co_await generate_migration_updates(updates, guard, plan);

    updates.emplace_back(
        topology_mutation_builder(guard.write_timestamp())
            .set_transition_state(topology::transition_state::tablet_migration)
            .set_version(_topo_sm._topology.version + 1)
            .build());

    co_await update_topology_state(std::move(guard), std::move(updates), "Starting tablet migration");
    co_return true;
}

future<locator::load_stats> topology_coordinator::refresh_tablet_load_stats() {
    auto tm = get_token_metadata_ptr();
    auto& topology = tm->get_topology();

    locator::load_stats stats;
    static constexpr std::chrono::seconds wait_for_live_nodes_timeout{30};

    std::unordered_map<table_id, size_t> total_replicas;

    for (auto& [dc, nodes] : topology.get_datacenter_nodes()) {
        locator::load_stats dc_stats;
        rtlogger.info("raft topology: Refreshing table load stats for DC {} that has {} endpoints", dc, nodes.size());
        co_await coroutine::parallel_for_each(nodes, [&] (const auto& node) -> future<> {
            auto dst = node->host_id();

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

            auto node_stats = co_await ser::storage_service_rpc_verbs::send_table_load_stats(&_messaging,
                                                                                             netw::msg_addr(id2ip(dst)),
                                                                                             as,
                                                                                             raft::server_id(dst.uuid()));

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
    rtlogger.debug("raft topology: Refreshed table load stats for all DC(s).");

    co_return std::move(stats);
}

future<> topology_coordinator::start_tablet_load_stats_refresher() {
    auto can_proceed = [this] { return !_async_gate.is_closed() && !_as.abort_requested(); };
    while (can_proceed()) {
        bool sleep = true;
        try {
            _tablet_load_stats = make_lw_shared<const locator::load_stats>(co_await refresh_tablet_load_stats());
            _topo_sm.event.broadcast(); // wake up load balancer.
        } catch (raft::request_aborted&) {
            rtlogger.debug("raft topology: Tablet load stats refresher aborted");
            sleep = false;
        } catch (seastar::abort_requested_exception) {
            rtlogger.debug("raft topology: Tablet load stats refresher aborted");
            sleep = false;
        } catch (...) {
            rtlogger.warn("Found error while refreshing load stats for tablets: {}, retrying...", std::current_exception());
        }
        if (sleep && can_proceed()) {
            try {
                co_await seastar::sleep_abortable(tablet_load_stats_refresh_interval, _as);
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

    rtlogger.info("migrating system_auth keyspace data");
    co_await auth::migrate_to_auth_v2(_sys_ks.query_processor(), _group0.client(),
            [this] (abort_source*) { return start_operation();}, _as);

    rtlogger.info("building initial raft topology state and CDC generation");
    guard = co_await start_operation();

    auto get_application_state = [&] (locator::host_id host_id, gms::inet_address ep, const gms::application_state_map& epmap, gms::application_state app_state) -> sstring {
        const auto it = epmap.find(app_state);
        if (it == epmap.end()) {
            throw std::runtime_error(format("failed to build initial raft topology state from gossip for node {}/{}: application state {} is missing in gossip", 
                    host_id, ep, app_state));
        }
        // it's versioned_value::value(), not std::optional::value() - it does not throw
        return it->second.value();
    };

    // Create a new CDC generation
    auto tmptr = get_token_metadata_ptr();
    auto get_sharding_info_for_host_id = [&] (locator::host_id host_id) -> std::pair<size_t, uint8_t> {
        const auto ep = tmptr->get_endpoint_for_host_id_if_known(host_id);
        if (!ep) {
            throw std::runtime_error(format("IP of node with ID {} is not known", host_id));
        }
        const auto eptr = _gossiper.get_endpoint_state_ptr(*ep);
        if (!eptr) {
            throw std::runtime_error(format("no gossiper endpoint state for node {}/{}", host_id, *ep));
        }
        const auto& epmap = eptr->get_application_state_map();
        const auto shard_count = std::stoi(get_application_state(host_id, *ep, epmap, gms::application_state::SHARD_COUNT));
        const auto ignore_msb = std::stoi(get_application_state(host_id, *ep, epmap, gms::application_state::IGNORE_MSB_BITS));
        return std::make_pair<size_t, uint8_t>(shard_count, ignore_msb);
    };
    auto [cdc_gen_uuid, guard_, mutation] = co_await prepare_and_broadcast_cdc_generation_data(tmptr, std::move(guard), std::nullopt, get_sharding_info_for_host_id);
    guard = std::move(guard_);

    topology_mutation_builder builder(guard.write_timestamp());

    std::set<sstring> enabled_features;

    // Build per-node state
    for (const auto& host_id: tmptr->get_all_endpoints()) {
        const auto ep = tmptr->get_endpoint_for_host_id_if_known(host_id);
        if (!ep) {
            throw std::runtime_error(format("failed to build initial raft topology state from gossip for node with ID {}, as its IP is not known", host_id));
        }
        const auto eptr = _gossiper.get_endpoint_state_ptr(*ep);
        if (!eptr) {
            throw std::runtime_error(format("failed to build initial raft topology state from gossip for node {}/{} as gossip contains no data for it", host_id, *ep));
        }

        const auto& epmap = eptr->get_application_state_map();

        const auto datacenter = get_application_state(host_id, *ep, epmap, gms::application_state::DC);
        const auto rack = get_application_state(host_id, *ep, epmap, gms::application_state::RACK);
        const auto tokens_v = tmptr->get_tokens(host_id);
        const std::unordered_set<dht::token> tokens(tokens_v.begin(), tokens_v.end());
        const auto release_version = get_application_state(host_id, *ep, epmap, gms::application_state::RELEASE_VERSION);
        const auto num_tokens = tokens.size();
        const auto shard_count = get_application_state(host_id, *ep, epmap, gms::application_state::SHARD_COUNT);
        const auto ignore_msb = get_application_state(host_id, *ep, epmap, gms::application_state::IGNORE_MSB_BITS);
        const auto supported_features_s = get_application_state(host_id, *ep, epmap, gms::application_state::SUPPORTED_FEATURES);
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

    while (!_as.abort_requested()) {
        bool sleep = false;
        try {
            co_await utils::get_local_injector().inject("topology_coordinator_pause_before_processing_backlog",
                [] (auto& handler) { return handler.wait_for_message(db::timeout_clock::now() + std::chrono::minutes(1)); });
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
    co_await std::move(cdc_generation_publisher);
}

future<> run_topology_coordinator(
        seastar::sharded<db::system_distributed_keyspace>& sys_dist_ks, gms::gossiper& gossiper,
        netw::messaging_service& messaging, locator::shared_token_metadata& shared_tm,
        db::system_keyspace& sys_ks, replica::database& db, service::raft_group0& group0,
        service::topology_state_machine& topo_sm, seastar::abort_source& as, raft::server& raft,
        raft_topology_cmd_handler_type raft_topology_cmd_handler,
        tablet_allocator& tablet_allocator,
        std::chrono::milliseconds ring_delay,
        endpoint_lifecycle_notifier& lifecycle_notifier) {

    topology_coordinator coordinator{
            sys_dist_ks, gossiper, messaging, shared_tm,
            sys_ks, db, group0, topo_sm, as, raft,
            std::move(raft_topology_cmd_handler),
            tablet_allocator,
            ring_delay};

    std::exception_ptr ex;
    lifecycle_notifier.register_subscriber(&coordinator);
    try {
        rtlogger.info("start topology coordinator fiber");
        const bool upgrade_done = co_await coordinator.maybe_run_upgrade();
        if (upgrade_done) {
            co_await coordinator.run();
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
}

} // namespace service
