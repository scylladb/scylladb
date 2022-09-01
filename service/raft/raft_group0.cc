/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0_client.hh"

#include "message/messaging_service.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "direct_failure_detector/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"

#include <seastar/core/smp.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>
#include <seastar/rpc/rpc_types.hh>

#include "idl/group0.dist.hh"

// Used to implement 'wait for any task to finish'.
//
// Pass a copy of this object to each task in a set of tasks.
// Once a task finishes, it should call `set_value` or `set_exception`.
//
// Call `get()` to wait for the result of the first task that finishes.
// Note that the results of all other tasks will be lost.
//
// There can be at most one `get()` call.
//
// Make sure that there is at least one task that reaches `set_value` or `set_exception`;
// otherwise `get()` would hang indefinitely.
template <typename T>
requires std::is_nothrow_move_constructible_v<T>
class tracker {
    struct shared {
        bool is_set{false};
        promise<T> p{};
    };

    lw_shared_ptr<shared> _shared{make_lw_shared<shared>()};

public:
    bool finished() {
        return _shared->is_set;
    }

    void set_value(T&& v) {
        if (!_shared->is_set) {
            _shared->p.set_value(std::move(v));
            _shared->is_set = true;
        }
    }

    void set_exception(std::exception_ptr ep) {
        if (!_shared->is_set) {
            _shared->p.set_exception(std::move(ep));
            _shared->is_set = true;
        }
    }

    future<T> get() {
        return _shared->p.get_future();
    }
};

namespace service {

static logging::logger group0_log("raft_group0");
static logging::logger upgrade_log("raft_group0_upgrade");

raft_group0::raft_group0(seastar::abort_source& abort_source,
        raft_group_registry& raft_gr,
        netw::messaging_service& ms,
        gms::gossiper& gs,
        cql3::query_processor& qp,
        service::migration_manager& mm,
        gms::feature_service& feat,
        raft_group0_client& client)
    : _abort_source(abort_source), _raft_gr(raft_gr), _ms(ms), _gossiper(gs), _qp(qp), _mm(mm), _feat(feat), _client(client)
    , _status_for_monitoring(_raft_gr.is_enabled() ? status_for_monitoring::normal : status_for_monitoring::disabled)
{
    init_rpc_verbs();
    register_metrics();
}

void raft_group0::init_rpc_verbs() {
    ser::group0_rpc_verbs::register_group0_peer_exchange(&_ms, [this] (const rpc::client_info&, rpc::opt_time_point, discovery::peer_list peers) {
        return peer_exchange(std::move(peers));
    });

    ser::group0_rpc_verbs::register_group0_modify_config(&_ms, [this] (const rpc::client_info&, rpc::opt_time_point,
            raft::group_id gid, std::vector<raft::config_member> add, std::vector<raft::server_id> del) {
        return _raft_gr.get_server(gid).modify_config(std::move(add), std::move(del));
    });

    ser::group0_rpc_verbs::register_get_group0_upgrade_state(&_ms,
        std::bind_front([] (raft_group0& self, const rpc::client_info&) -> future<group0_upgrade_state> {
            co_return (co_await self._client.get_group0_upgrade_state()).second;
        }, std::ref(*this)));
}

future<> raft_group0::uninit_rpc_verbs() {
    return when_all_succeed(
        ser::group0_rpc_verbs::unregister_group0_peer_exchange(&_ms),
        ser::group0_rpc_verbs::unregister_group0_modify_config(&_ms),
        ser::group0_rpc_verbs::unregister_get_group0_upgrade_state(&_ms)
    ).discard_result();
}

static future<group0_upgrade_state> send_get_group0_upgrade_state(netw::messaging_service& ms, const gms::inet_address& addr, abort_source& as) {
    auto state = co_await ser::group0_rpc_verbs::send_get_group0_upgrade_state(&ms, netw::msg_addr(addr), as);
    auto state_int = static_cast<int8_t>(state);
    if (state_int > group0_upgrade_state_last) {
        on_internal_error(upgrade_log, format(
            "send_get_group0_upgrade_state: unknown value for `group0_upgrade_state` received from node {}: {}",
            addr, state_int));
    }
    co_return state;
}

future<raft::server_address> raft_group0::load_my_addr() {
    assert(this_shard_id() == 0);

    auto id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
    if (!id) {
        on_internal_error(group0_log, "load_my_addr(): server ID for group 0 missing");
    }

    co_return raft::server_address{id, inet_addr_to_raft_addr(_gossiper.get_broadcast_address())};
}

seastar::future<raft::server_address> raft_group0::load_or_create_my_addr() {
    assert(this_shard_id() == 0);
    auto id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
    if (id == raft::server_id{}) {
        id = raft::server_id::create_random_id();
        co_await db::system_keyspace::set_raft_server_id(id.id);
    }
    co_return raft::server_address{id, inet_addr_to_raft_addr(_gossiper.get_broadcast_address())};
}

raft_server_for_group raft_group0::create_server_for_group0(raft::group_id gid, raft::server_address my_addr) {
    _raft_gr.address_map().set(my_addr);
    auto state_machine = std::make_unique<group0_state_machine>(_client, _mm, _qp.proxy());
    auto rpc = std::make_unique<raft_rpc>(*state_machine, _ms, _raft_gr.address_map(), gid, my_addr.id,
            [this] (gms::inet_address addr, raft::server_id raft_id, bool added) {
                // FIXME: we should eventually switch to UUID-based (not IP-based) node identification/communication scheme.
                // See #6403.
                auto fd_id = _gossiper.get_direct_fd_pinger().allocate_id(addr);
                if (added) {
                    group0_log.info("Added {} (address: {}) to group 0 RPC map", raft_id, addr);
                    _raft_gr.direct_fd().add_endpoint(fd_id);
                } else {
                    group0_log.info("Removed {} (address: {}) from group 0 RPC map", raft_id, addr);
                    _raft_gr.direct_fd().remove_endpoint(fd_id);
                }
            });
    // Keep a reference to a specific RPC class.
    auto& rpc_ref = *rpc;
    auto storage = std::make_unique<raft_sys_table_storage>(_qp, gid, my_addr.id);
    auto& persistence_ref = *storage;
    auto* cl = _qp.proxy().get_db().local().commitlog();
    auto config = raft::server::configuration {
        .on_background_error = [gid, this](std::exception_ptr e) {
            _raft_gr.abort_server(gid, fmt::format("background error, {}", e));
            _status_for_monitoring = status_for_monitoring::aborted;
        }
    };
    if (cl) {
        // Dividing by two is to protect against paddings that the
        // commit log can add for each mutation, as well as
        // against different commit log limits on different nodes.
        config.max_command_size = cl->max_record_size() / 2;
        config.max_log_size = 3 * config.max_command_size;
        config.snapshot_threshold_log_size = config.max_log_size / 2;
        config.snapshot_trailing_size = config.snapshot_threshold_log_size / 2;
    };
    auto server = raft::create_server(my_addr.id, std::move(rpc), std::move(state_machine),
            std::move(storage), _raft_gr.failure_detector(), config);

    // initialize the corresponding timer to tick the raft server instance
    auto ticker = std::make_unique<raft_ticker_type>([srv = server.get()] { srv->tick(); });

    return raft_server_for_group{
        .gid = std::move(gid),
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
        .persistence = persistence_ref,
    };
}

future<group0_info>
raft_group0::discover_group0(raft::server_address my_addr, const std::vector<raft::server_info>& seed_infos) {
    std::vector<raft::server_address> seeds;
    for (auto& info : seed_infos) {
        seeds.emplace_back(raft::server_id{}, info);
    }

    auto& p_discovery = _group0.emplace<persistent_discovery>(co_await persistent_discovery::make(my_addr, std::move(seeds), _qp));
    co_return co_await futurize_invoke([this, &p_discovery, my_addr = std::move(my_addr)] () mutable {
        return p_discovery.run(_ms, _shutdown_gate.hold(), _abort_source, std::move(my_addr));
    }).finally(std::bind_front([] (raft_group0& self, persistent_discovery& p_discovery) -> future<> {
        co_await p_discovery.stop();
        self._group0 = std::monostate{};
    }, std::ref(*this), std::ref(p_discovery)));
}

future<group0_info> persistent_discovery::run(
        netw::messaging_service& ms,
        gate::holder pause_shutdown,
        abort_source& as,
        raft::server_address my_addr) {
    // Send peer information to all known peers. If replies
    // discover new peers, send peer information to them as well.
    // As soon as we get a Raft Group 0 member information from
    // any peer, return it. If there is no Group 0, collect
    // replies from all peers, then, if this server has the smallest
    // id, make a new Group 0 with this server as the only member.
    // Otherwise sleep and keep pinging peers till some other node
    // creates a group and shares its group 0 id and peer address
    // with us.
    while (true) {
        auto output = co_await tick();

        if (std::holds_alternative<discovery::i_am_leader>(output)) {
            co_return group0_info{
                // Time-based ordering for groups identifiers may be
                // useful to provide linearisability between group
                // operations. Currently it's unused.
                .group0_id = raft::group_id{utils::UUID_gen::get_time_UUID()},
                .addr = my_addr
            };
        }

        if (std::holds_alternative<discovery::pause>(output)) {
            group0_log.trace("server {} pausing discovery...", my_addr.id);
            co_await seastar::sleep_abortable(std::chrono::milliseconds{1000}, as);
            continue;
        }

        ::tracker<std::optional<group0_info>> tracker;
        (void)[] (persistent_discovery& self, netw::messaging_service& ms, gate::holder pause_shutdown,
                  discovery::request_list request_list, ::tracker<std::optional<group0_info>> tracker) -> future<> {
            auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{1000};
            co_await parallel_for_each(request_list, [&] (std::pair<raft::server_address, discovery::peer_list>& req) -> future<> {
                netw::msg_addr peer(raft_addr_to_inet_addr(req.first));
                group0_log.trace("sending discovery message to {}", peer);
                try {
                    auto reply = co_await ser::group0_rpc_verbs::send_group0_peer_exchange(&ms, peer, timeout, std::move(req.second));

                    if (tracker.finished()) {
                        // Another peer was used to discover group 0 before us.
                        co_return;
                    }

                    if (auto peer_list = std::get_if<discovery::peer_list>(&reply.info)) {
                        // `tracker.finished()` is false so `run_discovery` hasn't exited yet, still safe to access `self`.
                        self.response(req.first, std::move(*peer_list));
                    } else if (auto g0_info = std::get_if<group0_info>(&reply.info)) {
                        tracker.set_value(std::move(*g0_info));
                    }
                } catch (std::exception& e) {
                    if (dynamic_cast<std::runtime_error*>(&e)) {
                        group0_log.trace("failed to send message: {}", e);
                    } else {
                        tracker.set_exception(std::current_exception());
                    }
                }
            });

            // In case we haven't discovered group 0 yet - need to run another iteration.
            tracker.set_value(std::nullopt);
        }(std::ref(*this), ms, pause_shutdown, std::move(std::get<discovery::request_list>(output)), tracker);

        if (auto g0_info = co_await tracker.get()) {
            co_return *g0_info;
        }
    }
}

future<> raft_group0::abort() {
    co_await uninit_rpc_verbs();
    co_await _shutdown_gate.close();
}

future<> raft_group0::start_server_for_group0(raft::group_id group0_id) {
    assert(group0_id != raft::group_id{});

    auto my_addr = co_await load_my_addr();

    group0_log.info("Server {} is starting group 0 with id {}", my_addr.id, group0_id);
    co_await _raft_gr.start_server_for_group(create_server_for_group0(group0_id, my_addr));
    _group0.emplace<raft::group_id>(group0_id);
}

future<> raft_group0::join_group0(std::vector<raft::server_info> seeds, bool as_voter) {
    assert(this_shard_id() == 0);
    assert(!joined_group0());

    auto group0_id = raft::group_id{co_await db::system_keyspace::get_raft_group0_id()};
    if (group0_id) {
        // Group 0 ID present means we've already joined group 0 before.
        co_return co_await start_server_for_group0(group0_id);
    }

    raft::server* server = nullptr;
    auto my_addr = co_await load_or_create_my_addr();
    group0_log.info("{} found no local group 0. Discovering...", my_addr.id);
    while (true) {
        auto g0_info = co_await discover_group0(my_addr, seeds);
        group0_log.info("server {} found group 0 with id {}, leader {}", my_addr.id, g0_info.group0_id, g0_info.addr.id);

        if (server && group0_id != g0_info.group0_id) {
            // `server` is not `nullptr` so we finished discovery in an earlier iteration and found a group 0 ID.
            // But in this iteration it's different. That shouldn't be possible.
            on_internal_error(group0_log, format(
                "The Raft discovery algorithm returned two different group IDs on subsequent runs: {} and {}."
                " Cannot proceed due to possible inconsistency problems."
                " If you're bootstrapping a fresh cluster, make sure that every node uses the same seeds configuration, then retry."
                " If this is happening after upgrade, please report a bug, then try following the manual recovery procedure.",
                group0_id, g0_info.group0_id));
            // TODO: link to the manual recovery docs
        }
        group0_id = g0_info.group0_id;

        if (server == nullptr) {
            // This is the first time discovery is run. Create and start a Raft server for group 0 on this node.
            raft::configuration initial_configuration;
            if (g0_info.addr.id == my_addr.id) {
                // We were chosen as the discovery leader.
                // We should start a new group with this node as voter.
                group0_log.info("Server {} chosen as discovery leader; bootstrapping group 0 from scratch", my_addr.id);
                initial_configuration.current.emplace(my_addr, true);
            }
            auto grp = create_server_for_group0(group0_id, my_addr);
            server = grp.server.get();
            co_await grp.persistence.bootstrap(std::move(initial_configuration));
            co_await _raft_gr.start_server_for_group(std::move(grp));
            // FIXME if we crash now or after getting added to the config but before storing group 0 ID,
            // we'll end with a bootstrapped server that possibly added some entries, but we won't remember that we have such a server
            // after we restart. Then we'll call `persistence.bootstrap` again after restart which will overwrite our snapshot, leading to
            // possibly incorrect state. One way of handling this may be changing `persistence.bootstrap` so it checks if any persistent
            // state is present, and if it is, do nothing.
        }

        assert(server);
        if (server->get_configuration().contains(my_addr.id)) {
            // True if we started a new group or completed a configuration change initiated earlier.
            group0_log.info("server {} already in group 0 (id {}) as {}", group0_id, my_addr.id,
                    server->get_configuration().can_vote(my_addr.id)? "voter" : "non-voter");
            break;
        }

        auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{1000};
        netw::msg_addr peer(raft_addr_to_inet_addr(g0_info.addr));
        try {
            // TODO: aborts?
            co_await ser::group0_rpc_verbs::send_group0_modify_config(&_ms, peer, timeout, group0_id, {{my_addr, as_voter}}, {});
            break;
        } catch (std::runtime_error& e) {
            // Retry
            group0_log.error("failed to modify config at peer {}: {}", g0_info.addr.id, e);
        }

        // Try again after a pause
        co_await seastar::sleep_abortable(std::chrono::milliseconds{1000}, _abort_source);
    }
    co_await db::system_keyspace::set_raft_group0_id(group0_id.id);
    // Allow peer_exchange() RPC to access group 0 only after group0_id is persisted.

    _group0 = group0_id;
    group0_log.info("{} joined group 0 with id {}", my_addr.id, group0_id);
}

static future<bool> wait_for_peers_to_enter_synchronize_state(
        const raft::server& group0_server, netw::messaging_service&, abort_source&, gate::holder pause_shutdown);
static future<bool> anyone_finished_upgrade(
        const raft::server& group0_server, netw::messaging_service&, abort_source&);
static future<bool> synchronize_schema(
        replica::database&, netw::messaging_service&,
        const raft::server& group0_server, service::migration_manager&,
        const noncopyable_function<future<bool>()>& can_finish_early,
        abort_source&);

future<> raft_group0::setup_group0(db::system_keyspace& sys_ks, const std::unordered_set<gms::inet_address>& initial_contact_nodes) {
    assert(this_shard_id() == 0);

    if (!_raft_gr.is_enabled()) {
        group0_log.info("setup_group0: local RAFT feature disabled, skipping group 0 setup.");
        // Note: if the local feature was enabled by every node earlier, that would enable the cluster
        // SUPPORTS_RAFT feature, and the node should then refuse to start during feature check
        // (because if the local feature is disabled, then the cluster feature - enabled in the cluster - is 'unknown' to us).
        co_return;
    }

    if (((co_await _client.get_group0_upgrade_state()).second) == group0_upgrade_state::recovery) {
        group0_log.warn("setup_group0: Raft RECOVERY mode, skipping group 0 setup.");
        co_return;
    }

    if (sys_ks.bootstrap_complete()) {
        auto group0_id = raft::group_id{co_await db::system_keyspace::get_raft_group0_id()};
        if (group0_id) {
            // Group 0 ID is present => we've already joined group 0 earlier.
            group0_log.info("setup_group0: group 0 ID present. Starting existing Raft server.");
            co_await start_server_for_group0(group0_id);
        } else {
            // Scylla has bootstrapped earlier but group 0 ID not present. This means we're upgrading.
            // Upgrade will start through a feature listener created after we enter NORMAL state.
            //
            // See `raft_group0::finish_setup_after_join`.
            upgrade_log.info(
                "setup_group0: Scylla bootstrap completed before but group 0 ID not present."
                " Internal upgrade-to-raft procedure will automatically start after every node finishes"
                " upgrading to the new Scylla version.");
        }

        co_return;
    }

    std::vector<raft::server_info> initial_contacts_as_raft_addrs;
    for (auto& addr: initial_contact_nodes) {
        if (addr != utils::fb_utilities::get_broadcast_address()) {
            initial_contacts_as_raft_addrs.push_back(inet_addr_to_raft_addr(addr));
        }
    }

    group0_log.info("setup_group0: joining group 0...");
    co_await join_group0(std::move(initial_contacts_as_raft_addrs), false /* non-voter */);
    group0_log.info("setup_group0: successfully joined group 0.");

    // Enter `synchronize` upgrade state in case the cluster we're joining has recently enabled Raft
    // and is currently in the middle of `upgrade_to_group0()`. For that procedure to finish
    // every member of group 0 (now including us) needs to enter `synchronize` state.
    co_await _client.set_group0_upgrade_state(group0_upgrade_state::synchronize);

    group0_log.info("setup_group0: ensuring that the cluster has fully upgraded to use Raft...");
    auto& group0_server = _raft_gr.group0();

    // Perform a Raft read barrier so we know the set of group 0 members and our group 0 state is up-to-date.
    co_await group0_server.read_barrier(&_abort_source);

    auto cfg = group0_server.get_configuration();
    if (!cfg.is_joint() && cfg.current.size() == 1) {
        group0_log.info("setup_group0: we're the only member of the cluster.");
    } else {
        // We're joining an existing cluster - we're not the only member.
        //
        // Wait until one of group 0 members enters `group0_upgrade_state::use_post_raft_procedures`
        // or all members enter `group0_upgrade_state::synchronize`.
        //
        // In a fully upgraded cluster this should finish immediately (if the network works well) - everyone is in `use_post_raft_procedures`.
        // In a cluster that is currently in the middle of `upgrade_to_group0`, this will cause us to wait until the procedure finishes.
        group0_log.info("setup_group0: ensuring that the cluster has fully upgraded to use Raft...");
        if (co_await wait_for_peers_to_enter_synchronize_state(group0_server, _ms, _abort_source, _shutdown_gate.hold())) {
            // Everyone entered `synchronize` state. That means we're bootstrapping in the middle of `upgrade_to_group0`.
            // We need to finish upgrade as others do.
            auto can_finish_early = std::bind_front(anyone_finished_upgrade, std::cref(group0_server), std::ref(_ms), std::ref(_abort_source));
            co_await synchronize_schema(_qp.db().real_database(), _ms, group0_server, _mm, can_finish_early, _abort_source);
        }
    }


    group0_log.info("setup_group0: the cluster is ready to use Raft. Finishing.");
    co_await _client.set_group0_upgrade_state(group0_upgrade_state::use_post_raft_procedures);
}

future<> raft_group0::finish_setup_after_join() {
    if (joined_group0()) {
        group0_log.info("finish_setup_after_join: group 0 ID present, loading server info.");
        auto my_addr = co_await load_my_addr();
        if (!_raft_gr.group0().get_configuration().can_vote(my_addr.id)) {
            group0_log.info("finish_setup_after_join: becoming a voter in the group 0 configuration...");
            // Just bootstrapped and joined as non-voter. Become a voter.
            auto pause_shutdown = _shutdown_gate.hold();
            co_await _raft_gr.group0().modify_config({{my_addr, true}}, {}, &_abort_source);
            group0_log.info("finish_setup_after_join: became a group 0 voter.");

            // No need to run `upgrade_to_group0()` since we must have bootstrapped with Raft
            // (that's the only way to join as non-voter today).
            co_return;
        }
    } else if (!_raft_gr.is_enabled()) {
        group0_log.info("finish_setup_after_join: local RAFT feature disabled, skipping.");
        co_return;
    } else {
        // We're either upgrading or in recovery mode.
    }

    // Note: even if we joined group 0 before, it doesn't necessarily mean we finished with the whole
    // upgrade procedure which has follow-up steps after joining group 0, hence we need to prepare
    // the listener below (or fire the upgrade procedure if the feature is already enabled) even if
    // we're already a member of group 0 by this point.
    // `upgrade_to_group0()` will correctly detect and handle which case we're in: whether the procedure
    // has already finished or we restarted in the middle after a crash.

    if (!_feat.supports_raft_cluster_mgmt) {
        group0_log.info("finish_setup_after_join: SUPPORTS_RAFT feature not yet enabled, scheduling upgrade to start when it is.");
    }

    // The listener may fire immediately, create a thread for that case.
    co_await seastar::async([this] {
        _raft_support_listener = _feat.supports_raft_cluster_mgmt.when_enabled([this] {
            group0_log.info("finish_setup_after_join: SUPPORTS_RAFT feature enabled. Starting internal upgrade-to-raft procedure.");
            upgrade_to_group0().get();
        });
    });
}

future<> raft_group0::leave_group0() {
    assert(this_shard_id() == 0);

    if (!_raft_gr.is_enabled()) {
        group0_log.info("leave_group0: local RAFT feature disabled, skipping.");
        co_return;
    }

    auto upgrade_state = (co_await _client.get_group0_upgrade_state()).second;
    if (upgrade_state == group0_upgrade_state::recovery) {
        group0_log.warn("leave_group0: in Raft RECOVERY mode, skipping.");
        co_return;
    }

    if (!_feat.supports_raft_cluster_mgmt) {
        // The Raft feature is not yet enabled.
        //
        // In that case we assume the cluster is in a partially-upgraded state (some nodes still use a version
        // without the raft/group 0 code enabled) and skip the leaving group 0 step.
        //
        // In theory we could be wrong: the cluster may have just upgraded but the user managed to call decommission
        // before we noticed it. Us leaving at this point could cause `upgrade_to_group0`
        // on other nodes to get stuck because it requires contacting all peers which may include us.
        //
        // For this to happen, the following conditions would have to be true:
        // 1. decommission is called immediately after upgrade, before we notice that the Raft feature is enabled
        // 2. we don't notice it during decommission - including the long streaming phase - even until we reach
        //    the `leave_group0` step which is almost at the end of the procedure
        // 3. some node did notice that the feature is enabled and started `upgrade_to_group0`, using us
        //    as one of the peers for running group 0 discovery, before we entered LEFT state
        //
        // These conditions together give a very unlikely scenario. If it does happen the user can perform
        // the manual recovery procedure for group 0.
        group0_log.warn(
            "leave_group0: Raft feature not enabled yet. Assuming that the cluster is partially upgraded"
            " and skipping the step. However, if your already finished the rolling upgrade procedure,"
            " this means we just haven't noticed it yet. The internal upgrade-to-raft procedure on other nodes"
            " may get stuck (they may try to contact us during the procedure). If that happens, manual recovery"
            " will be required. Consult the documentation for more details.");
        // TODO: link to the docs
        co_return;
    }

    if (upgrade_state != group0_upgrade_state::use_post_raft_procedures) {
        // The feature is enabled but `upgrade_to_group0` did not finish yet.
        // The upgrade procedure requires everyone to participate. In order to not block others
        // from doing their upgrades, we'll wait until we finish our procedure - then we can safely leave.
        //
        // FIXME: well, to be completely precise, it could happen that everyone enters `synchronize` state,
        // then we're the first to enter `use_post_raft_procedures` state and consider the procedure finished,
        // then we leave before others contact us to confirm that everyone entered `synchronize` and
        // they get stuck trying to contact us. To completely ensure liveness we should wait not only until
        // we enter `use_post_raft_procedures`, but until somebody else also does (then others can use that
        // node to confirm that they can also finish their procedures).
        // Extend `wait_until_upgrade_to_group0_finishes` with an extra step which RPCs a peer
        // to confirm they entered `use_post_raft_procedures`.
        group0_log.info("leave_group0: waiting until cluster fully upgrades to use Raft before proceeding...");
        co_await _client.wait_until_group0_upgraded(_abort_source);
        group0_log.info("leave_group0: cluster finished upgrade, continuing.");
    }

    // We're fully upgraded, we must have joined group 0.
    if (!joined_group0()) {
        on_internal_error(group0_log,
            "leave_group0: we're fully upgraded to use Raft but didn't join group 0. Please report a bug.");
    }

    auto my_id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
    if (!my_id) {
        on_internal_error(group0_log,
            "leave_group0: we're fully upgraded to use Raft and group 0 ID is present but Raft server ID is not."
            " Please report a bug.");
    }

    // Note: if this gets stuck due to a failure, the DB admin can abort.
    // FIXME: this gets stuck without failures if we're the leader (#10833)
    co_return co_await _raft_gr.group0().modify_config({}, {my_id}, &_abort_source);
}

future<> raft_group0::remove_from_group0(gms::inet_address node) {
    assert(this_shard_id() == 0);

    if (!_raft_gr.is_enabled()) {
        group0_log.info("remove_from_group0({}): local RAFT feature disabled, skipping.", node);
        co_return;
    }

    auto upgrade_state = (co_await _client.get_group0_upgrade_state()).second;
    if (upgrade_state == group0_upgrade_state::recovery) {
        group0_log.warn("remove_from_group0({}): in Raft RECOVERY mode, skipping.", node);
        co_return;
    }

    if (!_feat.supports_raft_cluster_mgmt) {
        // Similar situation as in `leave_group0` (read the comment there for detailed explanations).
        //
        // We assume that nobody started `upgrade_to_group0` yet so it's safe to remove nodes
        // from the cluster without `upgrade_to_group0` getting stuck due to unavailable peers.
        group0_log.warn(
            "remove_from_group0({}): Raft feature not enabled yet. Assuming that the cluster is partially upgraded"
            " and skipping the step. However, if your already finished the rolling upgrade procedure,"
            " this means we just haven't noticed it yet. The internal upgrade-to-raft procedure may get stuck"
            " (remaining nodes may try to contact the removed node during the procedure). If that happens,"
            " manual recovery will be required. Consult the documentation for more details.", node);
        // TODO: link to the docs
        co_return;
    }

    if (upgrade_state != group0_upgrade_state::use_post_raft_procedures) {
        // Similar situation as in `leave_group0`.
        // Wait until the upgrade procedure finishes before removing the node.
        //
        // Note: if we enter `use_post_raft_procedures`, it's safe to remove anyone else without blocking upgrade:
        // remaining nodes will observe that the procedure is finished by contacting us, even if they won't
        // be able to contact the removed node.
        group0_log.info("remove_from_group0({}): waiting until cluster fully upgrades to use Raft before proceeding...", node);
        co_await _client.wait_until_group0_upgraded(_abort_source);
        group0_log.info("remove_from_group0({}): cluster finished upgrade, continuing.", node);
    }

    // We're fully upgraded, we must have joined group 0.
    if (!joined_group0()) {
        on_internal_error(group0_log, format(
            "remove_from_group0({}): we're fully upgraded to use Raft but not a member of group 0."
            " Please report a bug.", node));
    }

    auto my_id = raft::server_id{co_await db::system_keyspace::get_raft_server_id()};
    if (!my_id) {
        on_internal_error(group0_log, format(
            "remove_from_group0({}): we're fully upgraded to use Raft and group 0 ID is present but Raft server ID is not."
            " Please report a bug.", node));
    }

    // Find their group 0 server's Raft ID.
    // Note: even if the removed node had the same inet_address as us, `find_replace_id` should correctly find them
    // (if they are still a member of group 0); hence we provide `my_id` to skip us in the search.
    auto their_id = _raft_gr.address_map().find_replace_id(node, my_id);
    if (!their_id) {
        // The address map is updated with the ID of every member of the configuration.
        // We could not find them in the address map. This could mean two things:
        // 1. they are not a member.
        // 2. They are a member, but we don't know about it yet; e.g. we just upgraded
        //    and joined group 0 but the leader is still pushing entires to us (including config entries)
        //    and we don't yet have the entry which contains `their_id`.
        //
        // To handle the second case we perform a read barrier now and check the address again.
        // Ignore the returned guard, we don't need it.
        group0_log.info("remove_from_group0({}): did not find them in group 0 configuration, synchronizing Raft before retrying...", node);
        co_await _raft_gr.group0().read_barrier(&_abort_source);

        their_id = _raft_gr.address_map().find_replace_id(node, my_id);
        if (!their_id) {
            group0_log.info("remove_from_group0({}): did not find them in group 0 configuration. Skipping.", node);
            co_return;
        }
    }

    group0_log.info(
        "remove_from_group0({}): found the node in group 0 configuration, Raft ID: {}. Proceeding with the remove...",
        node, *their_id);

    // TODO: add a timeout+retry mechanism? This could get stuck (and _abort_source is only called on shutdown).
    co_return co_await _raft_gr.group0().modify_config({}, {*their_id}, &_abort_source);
}

bool raft_group0::joined_group0() const {
    return std::holds_alternative<raft::group_id>(_group0);
}

future<group0_peer_exchange> raft_group0::peer_exchange(discovery::peer_list peers) {
    return std::visit([this, peers = std::move(peers)] (auto&& d) mutable -> future<group0_peer_exchange> {
        using T = std::decay_t<decltype(d)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            // Discovery not started or we're persisting the
            // leader information locally.
            co_return group0_peer_exchange{std::monostate{}};
        } else if constexpr (std::is_same_v<T, persistent_discovery>) {
            // Use discovery to produce a response
            if (auto response = co_await d.request(std::move(peers))) {
                co_return group0_peer_exchange{std::move(*response)};
            }
            // We just became a leader.
            // Eventually we'll answer with group0_info.
            co_return group0_peer_exchange{std::monostate{}};
        } else if constexpr (std::is_same_v<T, raft::group_id>) {
            // Even if in follower state, return own address: the
            // incoming RPC will then be bounced to the leader.
            co_return group0_peer_exchange{group0_info{
                .group0_id = std::get<raft::group_id>(_group0),
                .addr = _raft_gr.address_map().get_server_address(_raft_gr.group0().id())
            }};
        }
    }, _group0);
}

static constexpr auto DISCOVERY_KEY = "peers";

static mutation make_discovery_mutation(discovery::peer_set peers) {
    auto s = db::system_keyspace::discovery();
    auto ts = api::new_timestamp();
    auto raft_id_cdef = s->get_column_definition("raft_id");
    assert(raft_id_cdef);

    mutation m(s, partition_key::from_singular(*s, DISCOVERY_KEY));
    for (auto& p: peers) {
        auto& row = m.partition().clustered_row(*s, clustering_key::from_singular(*s, data_value(p.info)));
        row.apply(row_marker(ts));
        row.cells().apply(*raft_id_cdef, atomic_cell::make_live(*raft_id_cdef->type, ts, raft_id_cdef->type->decompose(p.id.id)));
    }

    return m;
}

static future<> store_discovered_peers(cql3::query_processor& qp, discovery::peer_set peers) {
    return qp.proxy().mutate_locally({make_discovery_mutation(std::move(peers))}, tracing::trace_state_ptr{});
}

static future<discovery::peer_set> load_discovered_peers(cql3::query_processor& qp) {
    static const auto load_cql = format(
            "SELECT server_info, raft_id FROM system.{} WHERE key = '{}'",
            db::system_keyspace::DISCOVERY, DISCOVERY_KEY);
    auto rs = co_await qp.execute_internal(load_cql, cql3::query_processor::cache_internal::yes);
    assert(rs);

    discovery::peer_set peers;
    for (auto& r: *rs) {
        peers.emplace(
            raft::server_id{r.get_as<utils::UUID>("raft_id")},
            r.get_as<bytes>("server_info")
        );
    }

    co_return peers;
}

future<persistent_discovery> persistent_discovery::make(raft::server_address self, peer_list seeds, cql3::query_processor& qp) {
    auto peers = co_await load_discovered_peers(qp);
    // If a peer is present both on disk and in provided list of `seeds`,
    // we take the information from disk (which may already contain the Raft ID of this peer).
    std::move(seeds.begin(), seeds.end(), std::inserter(peers, peers.end()));
    co_return persistent_discovery{std::move(self), {peers.begin(), peers.end()}, qp};
}

future<std::optional<discovery::peer_list>> persistent_discovery::request(peer_list peers) {
    for (auto& p: peers) {
        group0_log.debug("discovery: request peer: id={}, info={}", p.id, p.info);
    }

    if (_gate.is_closed()) {
        // We stopped discovery, about to destroy it.
        co_return std::nullopt;
    }
    auto holder = _gate.hold();

    auto response = _discovery.request(peers);
    co_await store_discovered_peers(_qp, _discovery.peers());

    co_return response;
}

void persistent_discovery::response(raft::server_address from, const peer_list& peers) {
    // The peers discovered here will be persisted on the next `request` or `tick`.
    for (auto& p: peers) {
        group0_log.debug("discovery: response peer: id={}, info={}", p.id, p.info);
    }
    _discovery.response(std::move(from), peers);
}

future<discovery::tick_output> persistent_discovery::tick() {
    // No need to enter `_gate`, since `stop` must be called after all calls to `tick` (and before the object is destroyed).

    auto result = _discovery.tick();
    co_await store_discovered_peers(_qp, _discovery.peers());

    co_return result;
}

future<> persistent_discovery::stop() {
    return _gate.close();
}

persistent_discovery::persistent_discovery(raft::server_address self, const peer_list& seeds, cql3::query_processor& qp)
    : _discovery{std::move(self), seeds}
    , _qp{qp}
{
    for (auto& addr: seeds) {
        group0_log.debug("discovery: seed peer: id={}, info={}", addr.id, addr.info);
    }
}

static std::vector<raft::server_info> inet_addrs_to_raft_infos(const std::vector<gms::inet_address>& addrs) {
    std::vector<raft::server_info> ret;
    std::transform(addrs.begin(), addrs.end(), std::back_inserter(ret), &inet_addr_to_raft_addr);
    return ret;
}

static std::vector<gms::inet_address> get_raft_members_inet_addrs(const raft::config_member_set& members) {
    std::vector<gms::inet_address> ret;
    for (auto& srv: members) {
        ret.push_back(raft_addr_to_inet_addr(srv.addr));
    }
    return ret;
}

static future<std::vector<raft::server_info>> get_peers_as_raft_infos(db::system_keyspace& sys_ks) {
    co_return inet_addrs_to_raft_infos(co_await sys_ks.load_peers());
}

// Given a function `fun` that takes an `abort_source&` as parameter,
// call `fun` with an internally constructed abort source which is aborted after the given time duration.
//
// The internal abort source also subscribes to the provided `abort_source& as` so the function will also react
// to top-level aborts.
//
// `abort_requested_exception` thrown by `fun` is translated to `timed_out_error` exception
// unless `as` requested abort or we didn't reach timeout yet.
template <std::invocable<abort_source&> F>
static futurize_t<std::invoke_result_t<F, abort_source&>>
with_timeout(abort_source& as, db::timeout_clock::duration d, F&& fun) {
    using future_t = futurize_t<std::invoke_result_t<F, abort_source&>>;

    // FIXME: using lambda as workaround for clang bug #50345 (miscompiling coroutine templates).
    auto impl = [] (abort_source& as, db::timeout_clock::duration d, F&& fun) -> future_t {
        abort_source timeout_src;
        auto sub = as.subscribe([&timeout_src] () noexcept { timeout_src.request_abort(); });
        if (!sub) {
            throw abort_requested_exception{};
        }

        // Using lambda here as workaround for seastar#1005
        future_t f = futurize_invoke([fun = std::move(fun)]
                (abort_source& s) mutable { return std::forward<F>(fun)(s); }, timeout_src);

        auto sleep_and_abort = [] (db::timeout_clock::duration d, abort_source& timeout_src) -> future<> {
            co_await sleep_abortable(d, timeout_src);
            if (!timeout_src.abort_requested()) {
                // We resolved before `f`. Abort the operation.
                timeout_src.request_abort();
            }
        }(d, timeout_src);

        f = co_await coroutine::as_future(std::move(f));

        if (!timeout_src.abort_requested()) {
            // `f` has already resolved, but abort the sleep.
            timeout_src.request_abort();
        }

        // Wait on the sleep as well (it should return shortly, being aborted) so we don't discard the future.
        try {
            co_await std::move(sleep_and_abort);
        } catch (const sleep_aborted&) {
            // Expected (if `f` resolved first or we were externally aborted).
        } catch (...) {
            // There should be no other exceptions, but just in case, catch and discard.
            // we want to propagate exceptions from `f`, not from sleep.
            group0_log.error("unexpected exception from sleep_and_abort", std::current_exception());
        }

        // Translate aborts caused by timeout to `timed_out_error`.
        // Top-level aborts (from `as`) are not translated.
        try {
            co_return co_await std::move(f);
        } catch (abort_requested_exception&) {
            if (as.abort_requested()) {
                // Assume the abort was caused by `as` (it may have been our timeout abort - doesn't matter)
                // and don't translate.
                throw;
            }

            if (!timeout_src.abort_requested()) {
                // Neither `as` nor `timeout_src` requested abort.
                // This must be another abort source internal to `fun`.
                // Don't translate.
                throw;
            }

            throw seastar::timed_out_error{};
        }
    };

    return impl(as, d, std::forward<F>(fun));
}

// Precondition: we joined group 0 and the server is running.
// Assumes we don't leave group 0 while running.
static future<> wait_until_every_peer_joined_group0(db::system_keyspace& sys_ks, const raft::server& group0_server, abort_source& as) {
    static constexpr auto retry_period = std::chrono::seconds{1};

    while (true) {
        // We fetch both config and peers on each iteration; we don't assume that they don't change.
        // No new node should join while the procedure is running, but nodes may leave.
        auto group0_config = group0_server.get_configuration();

        auto current_config = get_raft_members_inet_addrs(group0_config.current);
        std::sort(current_config.begin(), current_config.end());

        auto peers = co_await sys_ks.load_peers();
        std::sort(peers.begin(), peers.end());

        std::vector<gms::inet_address> missing_peers;
        std::set_difference(peers.begin(), peers.end(), current_config.begin(), current_config.end(), std::back_inserter(missing_peers));

        if (missing_peers.empty()) {
            if (!group0_config.is_joint()) {
                co_return;
            }

            upgrade_log.info("group 0 configuration is joint: {}. Sleeping for a while before retrying...", group0_config);
            co_await sleep_abortable(retry_period, as);
            continue;
        }

        upgrade_log.info(
            "group 0 configuration does not contain all peers yet."
            " Missing peers: {}. Current group 0 config: {}. Current group 0 config addresses: {}. Sleeping for a while before retrying...",
            missing_peers, group0_config, current_config);

        co_await sleep_abortable(retry_period, as);
    }
}

// Check if anyone entered `use_post_raft_procedures`.
// This is a best-effort single round-trip check; we don't retry if some nodes fail to answer.
static future<bool> anyone_finished_upgrade(
        const raft::server& group0_server, netw::messaging_service& ms, abort_source& as) {
    static constexpr auto max_concurrency = 10;
    static constexpr auto rpc_timeout = std::chrono::seconds{5};

    auto current_config = get_raft_members_inet_addrs(group0_server.get_configuration().current);
    bool finished = false;
    co_await max_concurrent_for_each(current_config, max_concurrency, [&] (const gms::inet_address& node) -> future<> {
        try {
            auto state = co_await with_timeout(as, rpc_timeout, std::bind_front(send_get_group0_upgrade_state, std::ref(ms), node));
            if (state == group0_upgrade_state::use_post_raft_procedures) {
                finished = true;
            }
        } catch (abort_requested_exception&) {
            upgrade_log.warn("anyone_finished_upgrade: abort requested during `send_get_group0_upgrade_state({})`", node);
            throw;
        } catch (...) {
            // XXX: are there possible fatal errors which should cause us to abort the entire procedure?
            upgrade_log.warn("anyone_finished_upgrade: `send_get_group0_upgrade_state({})` failed: {}", node, std::current_exception());
        }
    });
    co_return finished;
}

// Check if it's possible to reach everyone through `get_group0_upgrade_state` RPC.
static future<> check_remote_group0_upgrade_state_dry_run(
        const noncopyable_function<future<std::vector<gms::inet_address>>()>& get_peers,
        netw::messaging_service& ms, abort_source& as) {
    static constexpr auto max_retry_period = std::chrono::seconds{16};
    static constexpr auto rpc_timeout = std::chrono::seconds{5};
    static constexpr auto max_concurrency = 10;

    auto retry_period = std::chrono::seconds{1};
    while (true) {
        // Note: we strive to get a response from everyone in a 'single round-trip',
        // so we don't skip nodes which responded in earlier iterations.
        // We contact everyone in each iteration even if some of these guys already answered.
        // We fetch peers again on every attempt to handle the possibility of leaving nodes.
        auto peers = co_await get_peers();

        bool retry = false;
        co_await max_concurrent_for_each(peers, max_concurrency, [&] (const gms::inet_address& node) -> future<> {
            try {
                upgrade_log.info("check_remote_group0_upgrade_state_dry_run: `send_get_group0_upgrade_state({})`", node);
                co_await with_timeout(as, rpc_timeout, std::bind_front(send_get_group0_upgrade_state, std::ref(ms), node));
            } catch (abort_requested_exception&) {
                upgrade_log.warn("check_remote_group0_upgrade_state_dry_run: abort requested during `send_get_group0_upgrade_state({})`", node);
                throw;
            } catch (...) {
                // XXX: are there possible fatal errors which should cause us to abort the entire procedure?
                upgrade_log.warn(
                        "check_remote_group0_upgrade_state_dry_run: `send_get_group0_upgrade_state({})` failed: {}",
                        node, std::current_exception());
                retry = true;
            }
        });

        if (!retry) {
            co_return;
        }

        upgrade_log.warn("check_remote_group0_upgrade_state_dry_run: retrying in a while...");

        co_await sleep_abortable(retry_period, as);
        if (retry_period < max_retry_period) {
            retry_period *= 2;
        }
    }
}

// Wait until all members of group 0 enter `group0_upgrade_state::synchronize` or some node enters
// `group0_upgrade_state::use_post_raft_procedures` (the latter meaning upgrade is finished and we can also finish).
//
// Precondition: we're in `synchronize` state.
//
// Returns `true` if we finished because everybody entered `synchronize`.
// Returns `false` if we finished because somebody entered `use_post_raft_procedures`.
static future<bool> wait_for_peers_to_enter_synchronize_state(
        const raft::server& group0_server, netw::messaging_service& ms, abort_source& as, gate::holder pause_shutdown) {
    static constexpr auto retry_period = std::chrono::seconds{1};
    static constexpr auto rpc_timeout = std::chrono::seconds{5};
    static constexpr auto max_concurrency = 10;

    auto entered_synchronize = make_lw_shared<std::unordered_set<gms::inet_address>>();

    // This is a work-around for boost tests where RPC module is not listening so we cannot contact ourselves.
    // But really, except the (arguably broken) test code, we don't need to be treated as an edge case. All nodes are symmetric.
    // For production code this line is unnecessary.
    entered_synchronize->insert(utils::fb_utilities::get_broadcast_address());

    while (true) {
        // We fetch the config again on every attempt to handle the possibility of removing failed nodes.
        auto current_config = get_raft_members_inet_addrs(group0_server.get_configuration().current);

        ::tracker<bool> tracker;
        auto retry = make_lw_shared<bool>(false);

        auto sub = as.subscribe([tracker] () mutable noexcept {
            tracker.set_exception(std::make_exception_ptr(abort_requested_exception{}));
        });
        if (!sub) {
            upgrade_log.warn("wait_for_peers_to_enter_synchronize_state: abort requested");
            throw abort_requested_exception{};
        }

        (void) [] (netw::messaging_service& ms, abort_source& as, gate::holder pause_shutdown,
                   std::vector<gms::inet_address> current_config,
                   lw_shared_ptr<std::unordered_set<gms::inet_address>> entered_synchronize,
                   lw_shared_ptr<bool> retry, ::tracker<bool> tracker) -> future<> {
            co_await max_concurrent_for_each(current_config, max_concurrency, [&] (const gms::inet_address& node) -> future<> {
                if (entered_synchronize->contains(node)) {
                    co_return;
                }

                try {
                    auto state = co_await with_timeout(as, rpc_timeout, std::bind_front(send_get_group0_upgrade_state, std::ref(ms), node));
                    if (tracker.finished()) {
                        // A response from another node caused us to finish already.
                        co_return;
                    }

                    switch (state) {
                        case group0_upgrade_state::use_post_raft_procedures:
                            upgrade_log.info("wait_for_peers_to_enter_synchronize_state: {} confirmed that they finished upgrade.", node);
                            tracker.set_value(true);
                            break;
                        case group0_upgrade_state::synchronize:
                            entered_synchronize->insert(node);
                            break;
                        default:
                            upgrade_log.info("wait_for_peers_to_enter_synchronize_state: node {} not in synchronize state yet...", node);
                            *retry = true;
                    }
                } catch (abort_requested_exception&) {
                    upgrade_log.warn("wait_for_peers_to_enter_synchronize_state: abort requested during `send_get_group0_upgrade_state({})`", node);
                    tracker.set_exception(std::current_exception());
                } catch (...) {
                    // XXX: are there possible fatal errors which should cause us to abort the entire procedure?
                    upgrade_log.warn(
                            "wait_for_peers_to_enter_synchronize_state: `send_get_group0_upgrade_state({})` failed: {}",
                            node, std::current_exception());
                    *retry = true;
                }
            });

            tracker.set_value(false);
        }(ms, as, pause_shutdown, std::move(current_config), entered_synchronize, retry, tracker);

        auto finish_early = co_await tracker.get();
        if (finish_early) {
            co_return false;
        }

        if (!*retry) {
            co_return true;
        }

        upgrade_log.warn("wait_for_peers_to_enter_synchronize_state: retrying in a while...");

        co_await sleep_abortable(retry_period, as);
    }
}

// Returning nullopt means we finished early (`can_finish_early` returned true).
static future<std::optional<std::unordered_map<gms::inet_address, table_schema_version>>>
collect_schema_versions_from_group0_members(
        netw::messaging_service& ms, const raft::server& group0_server,
        const noncopyable_function<future<bool>()>& can_finish_early,
        abort_source& as) {
    static constexpr auto max_retry_period = std::chrono::seconds{16};
    static constexpr auto rpc_timeout = std::chrono::seconds{5};
    static constexpr auto max_concurrency = 10;

    auto retry_period = std::chrono::seconds{1};
    std::unordered_map<gms::inet_address, table_schema_version> versions;
    while (true) {
        // We fetch the config on each iteration; some nodes may leave.
        auto group0_config = group0_server.get_configuration();
        auto current_config = get_raft_members_inet_addrs(group0_config.current);

        bool failed = false;
        co_await max_concurrent_for_each(current_config, max_concurrency, [&] (const gms::inet_address& node) -> future<> {
            if (versions.contains(node)) {
                // This node was already contacted in a previous iteration.
                co_return;
            }

            try {
                upgrade_log.info("synchronize_schema: `send_schema_check({})`", node);
                versions.emplace(node,
                    co_await with_timeout(as, rpc_timeout, [&ms, addr = netw::msg_addr(node)] (abort_source& as) mutable {
                            return ms.send_schema_check(std::move(addr), as);
                        }));
            } catch (abort_requested_exception&) {
                upgrade_log.warn("synchronize_schema: abort requested during `send_schema_check({})`", node);
                throw;
            } catch (...) {
                // XXX: are there possible fatal errors which should cause us to abort the entire procedure?
                upgrade_log.warn("synchronize_schema: `send_schema_check({})` failed: {}", node, std::current_exception());
                failed = true;
            }
        });

        if (failed) {
            upgrade_log.warn("synchronize_schema: there were some failures when collecting remote schema versions.");
        } else if (group0_config.is_joint()) {
            upgrade_log.warn("synchronize_schema: group 0 configuration is joint: {}.", group0_config);
        } else {
            co_return versions;
        }

        upgrade_log.info("synchronize_schema: checking if we can finish early before retrying...");

        if (co_await can_finish_early()) {
            co_return std::nullopt;
        }

        upgrade_log.info(
                "synchronize_schema: could not finish early."
                " Sleeping for a while before retrying remote schema version collection...");
        co_await sleep_abortable(retry_period, as);
        if (retry_period < max_retry_period) {
            retry_period *= 2;
        }
    }
}

// Returning `true` means we synchronized schema.
// `false` means we finished early after calling `can_finish_early`.
//
// Postcondition for synchronizing schema (i.e. we return `true`):
// Let T0 be the point in time when this function starts.
// There is a schema version X and a point in time T > T0 such that:
//     - the local schema version at T was X,
//     - for every member of group 0 configuration there was a point in time T'
//       such that T > T' > T0 and the schema version at this member at T' was X.
//
// Assuming that merging schema mutations is an associative, commutative and idempotent
// operation, everybody pulling from everybody (or verifying they have the same mutations)
// should cause everybody to arrive at the same result.
static future<bool> synchronize_schema(
        replica::database& db, netw::messaging_service& ms,
        const raft::server& group0_server, service::migration_manager& mm,
        const noncopyable_function<future<bool>()>& can_finish_early,
        abort_source& as) {
    static constexpr auto max_retry_period = std::chrono::seconds{32};
    static constexpr auto rpc_timeout = std::chrono::seconds{5};
    static constexpr auto max_concurrency = 10;

    auto retry_period = std::chrono::seconds{1};
    bool last_pull_successful = false;
    size_t num_attempts_after_successful_pull = 0;

    while (true) {
        upgrade_log.info("synchronize_schema: collecting schema versions from group 0 members...");
        auto remote_versions = co_await collect_schema_versions_from_group0_members(ms, group0_server, can_finish_early, as);
        if (!remote_versions) {
            upgrade_log.info("synchronize_schema: finished early.");
            co_return false;
        }
        upgrade_log.info("synchronize_schema: collected remote schema versions.");

        auto my_version = db.get_version();
        upgrade_log.info("synchronize_schema: my version: {}", my_version);

        auto matched = std::erase_if(*remote_versions, [my_version] (const auto& p) { return p.second == my_version; });
        upgrade_log.info("synchronize_schema: schema mismatches: {}. {} nodes had a matching version.", *remote_versions, matched);

        if (remote_versions->empty()) {
            upgrade_log.info("synchronize_schema: finished.");
            co_return true;
        }

        // Note: if we successfully merged schema from everyone in earlier iterations, but our schema versions
        // are still not matching, that means our version is strictly more up-to-date than someone else's version.
        // In that case we could switch to a push mode instead of pull mode (we push schema mutations to them);
        // on the other hand this would further complicate the code and I assume that the regular schema synchronization
        // mechanisms (gossiping schema digests and triggering pulls on the other side) should deal with this case,
        // even though it may potentially take a bit longer than a pro-active approach. Furthermore, the other side
        // is also executing `synchronize_schema` at some point, so having this problem should be extremely unlikely.
        if (last_pull_successful) {
            if ((++num_attempts_after_successful_pull) > 3) {
                upgrade_log.error(
                        "synchronize_schema: we managed to pull schema from every other node, but our schema versions"
                        " are still different. The other side must have an outdated version and fail to pull it for some"
                        " reason. If this message keeps showing up, the internal upgrade-to-raft procedure is stuck;"
                        " try performing a rolling restart of your cluster."
                        " If that doesn't fix the problem, the system may require manual fixing of schema tables.");
            }
        }

        last_pull_successful = true;
        co_await max_concurrent_for_each(*remote_versions, max_concurrency, [&] (const auto& p) -> future<> {
            auto& [addr, _] = p;

            try {
                upgrade_log.info("synchronize_schema: `merge_schema_from({})`", addr);
                co_await mm.merge_schema_from(netw::msg_addr(addr));
            } catch (const rpc::closed_error& e) {
                upgrade_log.warn("synchronize_schema: `merge_schema_from({})` failed due to connection error: {}", addr, e);
                last_pull_successful = false;
            } catch (timed_out_error&) {
                upgrade_log.warn("synchronize_schema: `merge_schema_from({})` timed out", addr);
                last_pull_successful = false;
            } catch (abort_requested_exception&) {
                upgrade_log.warn("synchronize_schema: abort requested during `merge_schema_from({})`", addr);
                throw;
            } catch (...) {
                // We assume that every other exception type indicates a fatal error and happens because `merge_schema_from`
                // failed to apply schema mutations from a remote, which is not something we can automatically recover from.
                upgrade_log.error(
                        "synchronize_schema: fatal error in `merge_schema_from({})`: {}."
                        "\nCannot finish the upgrade procedure."
                        " Please fix your schema tables manually and try again by restarting the node.",
                        addr, std::current_exception());
                throw;
            }
        });

        if (co_await can_finish_early()) {
            upgrade_log.info("synchronize_schema: finishing early.");
            co_return false;
        }

        upgrade_log.info("synchronize_schema: sleeping for a while before collecting schema versions again...");
        co_await sleep_abortable(retry_period, as);

        if (retry_period < max_retry_period) {
            retry_period *= 2;
        }
    }
}

static auto warn_if_upgrade_takes_too_long() {
    auto as = std::make_unique<abort_source>();
    auto task = [] (abort_source& as) -> future<> {
        static constexpr auto warn_period = std::chrono::minutes{1};

        while (!as.abort_requested()) {
            try {
                co_await sleep_abortable(warn_period, as);
            } catch (const sleep_aborted&) {
                co_return;
            }

            upgrade_log.warn(
                "Raft upgrade procedure taking longer than expected. Please check if all nodes are live and the network is healthy."
                " If the upgrade procedure does not progress even though the cluster is healthy, try performing a rolling restart of the cluster."
                " If that doesn't help or some nodes are dead and irrecoverable, manual recovery may be required."
                " Consult the relevant documentation.");
            // TODO: link to the docs.
        }
    }(*as);

    return defer([task = std::move(task), as = std::move(as)] () mutable {
        // Stop in background.
        as->request_abort();
        (void)std::move(task).then([as = std::move(as)] {});
    });
}

future<> raft_group0::upgrade_to_group0() {
    assert(this_shard_id() == 0);

    // The SUPPORTS_RAFT cluster feature is enabled, so the local RAFT feature must also be enabled
    // (otherwise we wouldn't 'know' the cluster feature).
    assert(_raft_gr.is_enabled());

    auto start_state = (co_await _client.get_group0_upgrade_state()).second;
    switch (start_state) {
        case group0_upgrade_state::recovery:
            upgrade_log.info("RECOVERY mode. Not attempting upgrade.");
            co_return;
        case group0_upgrade_state::use_post_raft_procedures:
            upgrade_log.info("Already upgraded.");
            co_return;
        case group0_upgrade_state::synchronize:
            upgrade_log.warn(
                "Restarting upgrade in `synchronize` state."
                " A previous upgrade attempt must have been interrupted or failed.");
            break;
        case group0_upgrade_state::use_pre_raft_procedures:
            upgrade_log.info("starting in `use_pre_raft_procedures` state.");
    }

    (void)[] (raft_group0& self, abort_source& as, group0_upgrade_state start_state, gate::holder pause_shutdown) -> future<> {
        auto warner = warn_if_upgrade_takes_too_long();
        try {
            co_await self.do_upgrade_to_group0(start_state);
            co_await self._client.set_group0_upgrade_state(group0_upgrade_state::use_post_raft_procedures);
            upgrade_log.info("Raft upgrade finished.");
        } catch (...) {
            upgrade_log.error(
                "Raft upgrade failed: {}.\nTry restarting the node to retry upgrade."
                " If the procedure gets stuck, manual recovery may be required. Consult the relevant documentation.",
                std::current_exception());
                // TODO: link to the doc
        }
    }(std::ref(*this), std::ref(_abort_source), start_state, _shutdown_gate.hold());
}

// `start_state` is either `use_pre_raft_procedures` or `synchronize`.
future<> raft_group0::do_upgrade_to_group0(group0_upgrade_state start_state) {
    assert(this_shard_id() == 0);

    auto& sys_ks = _gossiper.get_system_keyspace().local();

    // Check if every peer knows about the upgrade procedure.
    //
    // In a world in which post-conditions and invariants are respected, the fact that `SUPPORTS_RAFT` feature
    // is enabled would guarantee this. However, the cluster features mechanism is unreliable and there are
    // scenarios where the feature gets enabled even though not everybody supports it. Attempts to fix this
    // only unmask more issues; see #11225. In general, fixing the gossiper/features subsystem is a big
    // project and we don't want this to block the Raft group 0 project (and we probably want to eventually
    // move most application states - including supported feature sets - to group 0 anyway).
    //
    // Work around that by ensuring that everybody is able to answer to the `get_group0_upgrade_state` call
    // before we proceed to the `join_group0` step, which doesn't tolerate servers leaving in the middle;
    // once a node is selected as one of the seeds of the discovery algorithm, it must answer. This 'dry run'
    // step, on the other hand, allows nodes to leave and will unblock as soon as all remaining peers are
    // ready to answer.
    upgrade_log.info("Waiting until everyone is ready to start upgrade...");
    co_await check_remote_group0_upgrade_state_dry_run(std::bind_front(&db::system_keyspace::load_peers, &sys_ks), _ms, _abort_source);

    if (!joined_group0()) {
        upgrade_log.info("Joining group 0...");
        co_await join_group0(co_await get_peers_as_raft_infos(sys_ks), true);
    } else {
        upgrade_log.info(
            "We're already a member of group 0."
            " Apparently we're restarting after a previous upgrade attempt failed.");
    }

    // After we joined, we shouldn't be removed from group 0 until the end of the procedure.
    // The implementation of `leave_group0` waits until upgrade finishes before leaving the group.
    // There is no guarantee that `remove_from_group0` from another node (that has
    // finished upgrading) won't remove us after we enter `synchronize` but before we leave it;
    // but then we're not needed for anything anymore and we can be shutdown,
    // and we won't do anything harmful to other nodes while in `synchronize`, worst case being
    // that we get stuck.

    auto& group0_server = _raft_gr.group0();

    upgrade_log.info("Waiting until every peer has joined Raft group 0...");
    co_await wait_until_every_peer_joined_group0(sys_ks, group0_server, _abort_source);
    upgrade_log.info("Every peer is a member of Raft group 0.");

    if (start_state == group0_upgrade_state::use_pre_raft_procedures) {
        // We perform a schema synchronization step before entering `synchronize` upgrade state.
        //
        // This step is not necessary for correctness: we will make sure schema is synchronized
        // after every node enters `synchronize`, where schema changes are disabled.
        //
        // However, it's good for reducing the chance that we get stuck later. If we manage to ensure that schema
        // is synchronized now, there's a high chance that after entering `synchronize` state we won't have
        // to do any additional schema pulls (only verify quickly that the schema is still in sync).
        upgrade_log.info("Waiting for schema to synchronize across all nodes in group 0...");
        auto can_finish_early = [] { return make_ready_future<bool>(false); };
        co_await synchronize_schema(_qp.db().real_database(), _ms, group0_server, _mm, can_finish_early, _abort_source);

        // Before entering `synchronize`, perform a round-trip of `get_group0_upgrade_state` RPC calls
        // to everyone as a dry run, just to check that nodes respond to this RPC.
        // Obviously we may lose connectivity immediately after this function finishes,
        // causing later steps to fail, but if network/RPC module is already broken, better to detect
        // it now than after entering `synchronize` state. And if this steps succeeds, then there's
        // a very high chance that the following steps succeed as well (we would need to be very unlucky otherwise).
        upgrade_log.info("Performing a dry run of remote `get_group0_upgrade_state` calls...");
        co_await check_remote_group0_upgrade_state_dry_run(
                [&group0_server] {
                    auto current_config = get_raft_members_inet_addrs(group0_server.get_configuration().current);
                    return make_ready_future<std::vector<gms::inet_address>>(std::move(current_config));
                }, _ms, _abort_source);

        upgrade_log.info("Entering synchronize state.");
        upgrade_log.warn("Schema changes are disabled in synchronize state."
                " If a failure makes us unable to proceed, manual recovery will be required.");
        co_await _client.set_group0_upgrade_state(group0_upgrade_state::synchronize);
    }

    upgrade_log.info("Waiting for all peers to enter synchronize state...");
    if (!(co_await wait_for_peers_to_enter_synchronize_state(group0_server, _ms, _abort_source, _shutdown_gate.hold()))) {
        upgrade_log.info("Another node already finished upgrade. We can finish early.");
        co_return;
    }

    upgrade_log.info("All peers in synchronize state. Waiting for schema to synchronize...");
    auto can_finish_early = std::bind_front(anyone_finished_upgrade, std::cref(group0_server), std::ref(_ms), std::ref(_abort_source));
    if (!(co_await synchronize_schema(_qp.db().real_database(), _ms, group0_server, _mm, can_finish_early, _abort_source))) {
        upgrade_log.info("Another node already finished upgrade. We can finish early.");
        co_return;
    }

    upgrade_log.info("Schema synchronized.");
}

void raft_group0::register_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("raft_group0", {
        sm::make_gauge("status", [this] { return static_cast<uint8_t>(_status_for_monitoring); },
            sm::description("status of the raft group, 0 - disabled, 1 - normal, 2 - aborted"))
    });
}

std::ostream& operator<<(std::ostream& os, group0_upgrade_state state) {
    switch (state) {
        case group0_upgrade_state::recovery:
            os << "recovery";
            break;
        case group0_upgrade_state::use_post_raft_procedures:
            os << "use_post_raft_procedures";
            break;
        case group0_upgrade_state::synchronize:
            os << "synchronize";
            break;
        case group0_upgrade_state::use_pre_raft_procedures:
            os << "use_pre_raft_procedures";
            break;
    }

    return os;
}

} // end of namespace service

