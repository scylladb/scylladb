/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include <iterator>
#include <source_location>

#include "service/raft/group0_fwd.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/raft/raft_address_map.hh"

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
#include "utils/error_injection.hh"

#include <seastar/core/smp.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>
#include <seastar/rpc/rpc_types.hh>
#include <stdexcept>

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

// TODO: change the links from master to stable/5.2 after 5.2 is released
const char* const raft_upgrade_doc = "https://docs.scylladb.com/master/architecture/raft.html#verifying-that-the-internal-raft-upgrade-procedure-finished-successfully";
static const auto raft_manual_recovery_doc = "https://docs.scylladb.com/master/architecture/raft.html#raft-manual-recovery-procedure";

// {{{ group0_rpc Maintain failure detector subscription whenever
// group 0 configuration changes.

class group0_rpc: public service::raft_rpc {
    direct_failure_detector::failure_detector& _direct_fd;
public:
    explicit group0_rpc(direct_failure_detector::failure_detector& direct_fd,
            raft_state_machine& sm, netw::messaging_service& ms,
            raft_address_map& address_map, shared_ptr<raft::failure_detector> raft_fd, raft::group_id gid, raft::server_id srv_id)
        : raft_rpc(sm, ms, address_map, std::move(raft_fd), gid, srv_id)
        , _direct_fd(direct_fd)
    {}

    virtual void on_configuration_change(raft::server_address_set add, raft::server_address_set del) override {
        for (const auto& addr: add) {
            // Entries explicitly managed via `rpc::on_configuration_change() should NOT be
            // expirable.
            _address_map.set_nonexpiring(addr.id);
            // Notify the direct failure detector that it should track
            // (or liveness of a specific raft server id.
            if (addr != _my_id) {
                // No need to ping self to know it's alive
                _direct_fd.add_endpoint(addr.id.id);
            }
        }
        for (const auto& addr: del) {
            // RPC 'send' may yield before resolving IP address,
            // e.g. on _shutdown_gate, so keep the deleted
            // entries in the map for a bit.
            _address_map.set_expiring(addr.id);
            _direct_fd.remove_endpoint(addr.id.id);
        }
    }
};

// }}} group0_rpc

raft_group0::raft_group0(seastar::abort_source& abort_source,
        raft_group_registry& raft_gr,
        sharded<netw::messaging_service>& ms,
        gms::gossiper& gs,
        gms::feature_service& feat,
        db::system_keyspace& sys_ks,
        raft_group0_client& client)
    : _abort_source(abort_source), _raft_gr(raft_gr), _ms(ms), _gossiper(gs),  _feat(feat), _sys_ks(sys_ks), _client(client)
    , _status_for_monitoring(status_for_monitoring::normal)
{
    register_metrics();
}

future<> raft_group0::start() {
    return smp::invoke_on_all([shard0_this=this]() {
        init_rpc_verbs(*shard0_this);
    });
}

void raft_group0::init_rpc_verbs(raft_group0& shard0_this) {
    ser::group0_rpc_verbs::register_group0_peer_exchange(&shard0_this._ms.local(),
        [&shard0_this] (const rpc::client_info&, rpc::opt_time_point, discovery::peer_list peers) {
            return smp::submit_to(0, [&shard0_this, peers = std::move(peers)]() mutable {
                return shard0_this.peer_exchange(std::move(peers));
            });
        });

    ser::group0_rpc_verbs::register_group0_modify_config(&shard0_this._ms.local(),
        [&shard0_this] (const rpc::client_info&, rpc::opt_time_point, raft::group_id gid, std::vector<raft::config_member> add, std::vector<raft::server_id> del) {
            return smp::submit_to(0, [&shard0_this, gid, add = std::move(add), del = std::move(del)]() mutable {
                return shard0_this._raft_gr.get_server(gid).modify_config(std::move(add), std::move(del), nullptr);
            });
        });

    ser::group0_rpc_verbs::register_get_group0_upgrade_state(&shard0_this._ms.local(),
        [&shard0_this] (const rpc::client_info&) -> future<group0_upgrade_state> {
            return smp::submit_to(0, [&shard0_this]() -> future<group0_upgrade_state> {
                const auto [holder, state] = co_await shard0_this._client.get_group0_upgrade_state();
                co_return state;
            });
        });
}

future<> raft_group0::uninit_rpc_verbs(netw::messaging_service& ms) {
    return when_all_succeed(
        ser::group0_rpc_verbs::unregister_group0_peer_exchange(&ms),
        ser::group0_rpc_verbs::unregister_group0_modify_config(&ms),
        ser::group0_rpc_verbs::unregister_get_group0_upgrade_state(&ms)
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

const raft::server_id& raft_group0::load_my_id() {
    return _raft_gr.get_my_raft_id();
}

raft_server_for_group raft_group0::create_server_for_group0(raft::group_id gid, raft::server_id my_id, service::storage_service& ss, cql3::query_processor& qp,
                                                            service::migration_manager& mm, bool topology_change_enabled) {
    auto state_machine = std::make_unique<group0_state_machine>(_client, mm, qp.proxy(), ss, _raft_gr.address_map(), _feat, topology_change_enabled);
    auto rpc = std::make_unique<group0_rpc>(_raft_gr.direct_fd(), *state_machine, _ms.local(), _raft_gr.address_map(), _raft_gr.failure_detector(), gid, my_id);
    // Keep a reference to a specific RPC class.
    auto& rpc_ref = *rpc;
    auto storage = std::make_unique<raft_sys_table_storage>(qp, gid, my_id);
    auto& persistence_ref = *storage;
    auto* cl = qp.proxy().get_db().local().commitlog();
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
    auto server = raft::create_server(my_id, std::move(rpc), std::move(state_machine),
            std::move(storage), _raft_gr.failure_detector(), config);

    // initialize the corresponding timer to tick the raft server instance
    auto ticker = std::make_unique<raft_ticker_type>([srv = server.get()] { srv->tick(); });
    lowres_clock::duration default_op_timeout = std::chrono::minutes(1);
    if (const auto ms = utils::get_local_injector().inject_parameter<int64_t>("group0-raft-op-timeout-in-ms"); ms) {
        default_op_timeout = std::chrono::milliseconds(*ms);
    }
    return raft_server_for_group{
        .gid = std::move(gid),
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
        .persistence = persistence_ref,
        .default_op_timeout = default_op_timeout
    };
}

future<group0_info>
raft_group0::discover_group0(const std::vector<gms::inet_address>& seeds, cql3::query_processor& qp) {
    auto my_id = load_my_id();
    discovery::peer_list peers;
    for (auto& ip: seeds) {
        if (ip != _gossiper.get_broadcast_address()) {
            peers.emplace_back(discovery_peer{raft::server_id{}, ip});
        }
    }
    discovery_peer my_addr = {my_id, _gossiper.get_broadcast_address()};

    auto& p_discovery = _group0.emplace<persistent_discovery>(co_await persistent_discovery::make(my_addr, std::move(peers), qp));
    co_return co_await futurize_invoke([this, &p_discovery, my_addr = std::move(my_addr)] () mutable {
        return p_discovery.run(_ms.local(), _shutdown_gate.hold(), _abort_source, std::move(my_addr));
    }).finally(std::bind_front([] (raft_group0& self, persistent_discovery& p_discovery) -> future<> {
        co_await p_discovery.stop();
        self._group0 = std::monostate{};
    }, std::ref(*this), std::ref(p_discovery)));
}

static constexpr auto DISCOVERY_KEY = "peers";

static future<discovery::peer_list> load_discovered_peers(cql3::query_processor& qp) {
    static const auto load_cql = format(
            "SELECT ip_addr, raft_server_id FROM system.{} WHERE key = '{}'",
            db::system_keyspace::DISCOVERY, DISCOVERY_KEY);
    auto rs = co_await qp.execute_internal(load_cql, cql3::query_processor::cache_internal::yes);
    assert(rs);

    discovery::peer_list peers;
    for (auto& r: *rs) {
        peers.push_back({
            raft::server_id{r.get_as<utils::UUID>("raft_server_id")},
            gms::inet_address{r.get_as<net::inet_address>("ip_addr")}
        });
    }

    co_return peers;
}

static mutation make_discovery_mutation(discovery::peer_list peers) {
    auto s = db::system_keyspace::discovery();
    auto ts = api::new_timestamp();
    auto raft_id_cdef = s->get_column_definition("raft_server_id");
    assert(raft_id_cdef);

    mutation m(s, partition_key::from_singular(*s, DISCOVERY_KEY));
    for (auto& p: peers) {
        auto& row = m.partition().clustered_row(*s, clustering_key::from_singular(*s, data_value(p.ip_addr)));
        row.apply(row_marker(ts));
        row.cells().apply(*raft_id_cdef, atomic_cell::make_live(*raft_id_cdef->type, ts, raft_id_cdef->type->decompose(p.id.id)));
    }

    return m;
}

static future<> store_discovered_peers(cql3::query_processor& qp, discovery::peer_list peers) {
    return qp.proxy().mutate_locally({make_discovery_mutation(std::move(peers))}, tracing::trace_state_ptr{});
}

future<group0_info> persistent_discovery::run(
        netw::messaging_service& ms,
        gate::holder pause_shutdown,
        abort_source& as,
        discovery_peer my_addr) {
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
                .id = my_addr.id,
                .ip_addr = my_addr.ip_addr
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
            co_await parallel_for_each(request_list, [&] (std::pair<discovery_peer, discovery::peer_list>& req) -> future<> {
                netw::msg_addr peer(req.first.ip_addr);
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
    co_await smp::invoke_on_all([this]() {
        return uninit_rpc_verbs(_ms.local());
    });
    co_await _shutdown_gate.close();

    _leadership_monitor_as.request_abort();
    co_await std::move(_leadership_monitor);

    co_await stop_group0();
}

future<> raft_group0::start_server_for_group0(raft::group_id group0_id, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled) {
    assert(group0_id != raft::group_id{});
    // The address map may miss our own id in case we connect
    // to an existing Raft Group 0 leader.
    auto my_id = load_my_id();
    group0_log.info("Server {} is starting group 0 with id {}", my_id, group0_id);
    auto srv_for_group0 = create_server_for_group0(group0_id, my_id, ss, qp, mm, topology_change_enabled);
    auto& persistence = srv_for_group0.persistence;
    auto& server = *srv_for_group0.server;
    co_await _raft_gr.start_server_for_group(std::move(srv_for_group0));
    _group0.emplace<raft::group_id>(group0_id);

    // Fix for scylladb/scylladb#16683:
    // If the snapshot index is 0, trigger creation of a new snapshot
    // so bootstrapping nodes will receive a snapshot transfer.
    auto snap = co_await persistence.load_snapshot_descriptor();
    if (snap.idx == raft::index_t{0}) {
        group0_log.info("Detected snapshot with index=0, id={}, triggering new snapshot", snap.id);
        bool created = co_await server.trigger_snapshot(&_abort_source);
        if (created) {
            snap = co_await persistence.load_snapshot_descriptor();
            group0_log.info("New snapshot created, index={} id={}", snap.idx, snap.id);
        } else {
            group0_log.warn("Could not create new snapshot, there are no entries applied");
        }
    }
}

future<> raft_group0::leadership_monitor_fiber() {
    try {
        auto sub = _abort_source.subscribe([&] () noexcept {
            if (!_leadership_monitor_as.abort_requested()) {
                _leadership_monitor_as.request_abort();
            }
        });

        while (true) {
            while (!group0_server().is_leader()) {
                co_await group0_server().wait_for_state_change(&_leadership_monitor_as);
            }
            group0_log.info("gaining leadership");
            co_await group0_server().wait_for_state_change(&_leadership_monitor_as);
            group0_log.info("losing leadership");
        }
    } catch (...) {
        group0_log.debug("leadership_monitor_fiber aborted with {}", std::current_exception());
    }
}

future<> raft_group0::join_group0(std::vector<gms::inet_address> seeds, shared_ptr<service::group0_handshaker> handshaker, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm,
                                  db::system_keyspace& sys_ks, bool topology_change_enabled) {
    assert(this_shard_id() == 0);
    assert(!joined_group0());

    auto group0_id = raft::group_id{co_await sys_ks.get_raft_group0_id()};
    if (group0_id) {
        // Group 0 ID present means we've already joined group 0 before.
        co_return co_await start_server_for_group0(group0_id, ss, qp, mm, topology_change_enabled);
    }

    raft::server* server = nullptr;
    auto my_id = load_my_id();
    group0_log.info("server {} found no local group 0. Discovering...", my_id);
    while (true) {
        auto g0_info = co_await discover_group0(seeds, qp);
        group0_log.info("server {} found group 0 with group id {}, leader {}", my_id, g0_info.group0_id, g0_info.id);

        if (server && group0_id != g0_info.group0_id) {
            // `server` is not `nullptr` so we finished discovery in an earlier iteration and found a group 0 ID.
            // But in this iteration it's different. That shouldn't be possible.
            on_internal_error(group0_log, format(
                "The Raft discovery algorithm returned two different group IDs on subsequent runs: {} and {}."
                " Cannot proceed due to possible inconsistency problems."
                " If you're bootstrapping a fresh cluster, make sure that every node uses the same seeds configuration, then retry."
                " If this is happening after upgrade, please report a bug, then try following the manual recovery procedure: {}",
                group0_id, g0_info.group0_id, raft_manual_recovery_doc));
        }
        group0_id = g0_info.group0_id;
        raft::server_address my_addr{my_id, {}};

        if (server == nullptr) {
            // This is the first time discovery is run. Create and start a Raft server for group 0 on this node.
            raft::configuration initial_configuration;
            bool nontrivial_snapshot = false;
            if (g0_info.id == my_id) {
                // We were chosen as the discovery leader.
                // We should start a new group with this node as voter.
                group0_log.info("Server {} chosen as discovery leader; bootstrapping group 0 from scratch", my_id);
                initial_configuration.current.emplace(my_addr, true);
                // Force snapshot transfer from us to subsequently joining servers.
                // This is important for upgrade and recovery, where the group 0 state machine
                // (schema tables in particular) is nonempty.
                // In a fresh cluster this will trigger an empty snapshot transfer which is redundant but correct.
                // See #14066.
                nontrivial_snapshot = true;
            } else {
                co_await handshaker->pre_server_start(g0_info);
            }
            // Bootstrap the initial configuration
            co_await raft_sys_table_storage(qp, group0_id, my_id)
                    .bootstrap(std::move(initial_configuration), nontrivial_snapshot);
            co_await start_server_for_group0(group0_id, ss, qp, mm, topology_change_enabled);
            server = &_raft_gr.group0();
            // FIXME if we crash now or after getting added to the config but before storing group 0 ID,
            // we'll end with a bootstrapped server that possibly added some entries, but we won't remember that we have such a server
            // after we restart. Then we'll call `persistence.bootstrap` again after restart which will overwrite our snapshot, leading to
            // possibly incorrect state. One way of handling this may be changing `persistence.bootstrap` so it checks if any persistent
            // state is present, and if it is, do nothing.
        }

        assert(server);
        if (server->get_configuration().contains(my_id)) {
            // True if we started a new group or completed a configuration change initiated earlier.
            group0_log.info("server {} already in group 0 (id {}) as {}", my_id, group0_id,
                    server->get_configuration().can_vote(my_id)? "voter" : "non-voter");
            break;
        }

        if (co_await handshaker->post_server_start(g0_info, _abort_source)) {
            break;
        }

        // Try again after a pause
        co_await seastar::sleep_abortable(std::chrono::milliseconds{1000}, _abort_source);
    }
    co_await sys_ks.set_raft_group0_id(group0_id.id);
    // Allow peer_exchange() RPC to access group 0 only after group0_id is persisted.

    _group0 = group0_id;

    co_await _gossiper.container().invoke_on_all([group0_id = group0_id.uuid()] (auto& gossiper) {
        gossiper.set_group0_id(group0_id);
        return make_ready_future<>();
    });

    group0_log.info("server {} joined group 0 with group id {}", my_id, group0_id);
}

shared_ptr<service::group0_handshaker> raft_group0::make_legacy_handshaker(bool can_vote) {
    struct legacy_handshaker : public group0_handshaker {
        service::raft_group0& _group0;
        netw::messaging_service& _ms;
        bool _can_vote;

        legacy_handshaker(service::raft_group0& group0, netw::messaging_service& ms, bool can_vote)
                : _group0(group0)
                , _ms(ms)
                , _can_vote(can_vote)
        {}

        future<> pre_server_start(const group0_info& info) override {
            // Nothing to do in this step
            co_return;
        }

        future<bool> post_server_start(const group0_info& g0_info, abort_source& as) override {
            netw::msg_addr peer(g0_info.ip_addr);
            auto timeout = db::timeout_clock::now() + std::chrono::milliseconds{1000};
            auto my_id = _group0.load_my_id();
            raft::server_address my_addr{my_id, {}};
            try {
                co_await ser::group0_rpc_verbs::send_group0_modify_config(&_ms, peer, timeout, g0_info.group0_id, {{my_addr, _can_vote}}, {});
                co_return true;
            } catch (std::runtime_error& e) {
                group0_log.warn("failed to modify config at peer {}: {}. Retrying.", g0_info.id, e);
                co_return false;
            }
        };
    };

    return make_shared<legacy_handshaker>(*this, _ms.local(), can_vote);
}

struct group0_members {
    const raft::server& _group0_server;
    const raft_address_map& _address_map;

    raft::config_member_set get_members() const {
        return _group0_server.get_configuration().current;
    }

    std::optional<gms::inet_address> get_inet_addr(const raft::config_member& member) const {
        return _address_map.find(member.addr.id);
    }

    std::vector<gms::inet_address> get_inet_addrs(seastar::compat::source_location l =
            seastar::compat::source_location::current()) const {
        const raft::config_member_set& members = _group0_server.get_configuration().current;
        std::vector<gms::inet_address> ret;
        std::vector<raft::server_id> missing;
        ret.reserve(members.size());
        for (const auto& srv: members) {
            auto addr = _address_map.find(srv.addr.id);
            if (!addr.has_value()) {
                missing.push_back(srv.addr.id);
            } else {
                ret.push_back(addr.value());
            }
        }
        if (!missing.empty()) {
            upgrade_log.info("{}: failed to resolve IP addresses of some of the cluster members ({})",
                l.function_name(), missing);
            ret.clear();
        }
        return ret;
    }

    bool is_joint() const {
        return _group0_server.get_configuration().is_joint();
    }
};

static future<bool> wait_for_peers_to_enter_synchronize_state(
        const group0_members& members0, netw::messaging_service&, abort_source&, gate::holder pause_shutdown);
static future<bool> anyone_finished_upgrade(
        const group0_members& members0, netw::messaging_service&, abort_source&);
static future<bool> synchronize_schema(
        replica::database&, netw::messaging_service&,
        const group0_members& members0, service::migration_manager&,
        const noncopyable_function<future<bool>()>& can_finish_early,
        abort_source&);

future<bool> raft_group0::use_raft() {
    assert(this_shard_id() == 0);

    if (((co_await _client.get_group0_upgrade_state()).second) == group0_upgrade_state::recovery) {
        group0_log.warn("setup_group0: Raft RECOVERY mode, skipping group 0 setup.");
        co_return false;
    }

    co_return true;
}

future<> raft_group0::setup_group0_if_exist(db::system_keyspace& sys_ks, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm) {
    if (!co_await use_raft()) {
        co_return;
    }

    if (!sys_ks.bootstrap_complete()) {
        // If bootstrap did not complete yet, there is no group 0 to setup at this point
        // -- it will be done after we start gossiping, in `setup_group0`.
        // Because of this we already want to disable schema pulls so they're done exclusively
        // through group 0 from the first moment we join the cluster.
        group0_log.info("Disabling migration_manager schema pulls because Raft is enabled and we're bootstrapping.");
        co_await mm.disable_schema_pulls();
        co_return;
    }

    auto group0_id = raft::group_id{co_await sys_ks.get_raft_group0_id()};
    if (group0_id) {
        // Group 0 ID is present => we've already joined group 0 earlier.
        group0_log.info("setup_group0: group 0 ID present. Starting existing Raft server.");
        co_await start_server_for_group0(group0_id, ss, qp, mm, false);

        // Start group 0 leadership monitor fiber.
        _leadership_monitor = leadership_monitor_fiber();

        // If we're not restarting in the middle of the Raft upgrade procedure, we must disable
        // migration_manager schema pulls.
        auto start_state = (co_await _client.get_group0_upgrade_state()).second;
        if (start_state == group0_upgrade_state::use_post_raft_procedures) {
            group0_log.info(
                "Disabling migration_manager schema pulls because Raft is fully functioning in this cluster.");
            co_await mm.disable_schema_pulls();
        } else {
            // We'll disable them once we complete the upgrade procedure.
        }
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
}

future<> raft_group0::setup_group0(
        db::system_keyspace& sys_ks, const std::unordered_set<gms::inet_address>& initial_contact_nodes, shared_ptr<group0_handshaker> handshaker,
        std::optional<replace_info> replace_info, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled) {
    if (!co_await use_raft()) {
        co_return;
    }

    if (sys_ks.bootstrap_complete()) {
        // If the node is bootstrapped the group0 server should be setup already
        co_return;
    }

    if (replace_info) {
        // Insert the replaced node's (Raft ID, IP address) pair into `raft_address_map`.
        // In general, the mapping won't be obtained through the regular gossiping route:
        // if we (the replacing node) use the replaced node's IP address, the replaced node's
        // application states are gone by this point (our application states overrode them
        // - the application states map is using the IP address as the key).
        // Even when we use a different IP, there's no guarantee the IPs were exchanged by now.
        // Instead, we obtain `replace_info` during the shadow round (which guarantees to contact
        // another node and fetch application states from it) and pass it to `setup_group0`.

        group0_log.info("Replacing a node with Raft ID: {}, IP address: {}",
                        replace_info->raft_id, replace_info->ip_addr);

        // `opt_add_entry` is shard-local, but that's fine - we only need this info on shard 0.
        _raft_gr.address_map().opt_add_entry(replace_info->raft_id, replace_info->ip_addr);
    }

    std::vector<gms::inet_address> seeds;
    for (auto& addr: initial_contact_nodes) {
        seeds.push_back(addr);
    }

    group0_log.info("setup_group0: joining group 0...");
    co_await join_group0(std::move(seeds), std::move(handshaker), ss, qp, mm, sys_ks, topology_change_enabled);
    group0_log.info("setup_group0: successfully joined group 0.");

    // Start group 0 leadership monitor fiber.
    _leadership_monitor = leadership_monitor_fiber();

    utils::get_local_injector().inject("stop_after_joining_group0", [&] {
        throw std::runtime_error{"injection: stop_after_joining_group0"};
    });

    // Enter `synchronize` upgrade state in case the cluster we're joining has recently enabled Raft
    // and is currently in the middle of `upgrade_to_group0()`. For that procedure to finish
    // every member of group 0 (now including us) needs to enter `synchronize` state.
    co_await _client.set_group0_upgrade_state(group0_upgrade_state::synchronize);

    group0_log.info("setup_group0: ensuring that the cluster has fully upgraded to use Raft...");
    auto& group0_server = _raft_gr.group0();
    group0_members members0{group0_server, _raft_gr.address_map()};

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
        group0_log.info("setup_group0: waiting for peers to synchronize state...");
        if (co_await wait_for_peers_to_enter_synchronize_state(members0, _ms.local(), _abort_source, _shutdown_gate.hold())) {
            // Everyone entered `synchronize` state. That means we're bootstrapping in the middle of `upgrade_to_group0`.
            // We need to finish upgrade as others do.
            auto can_finish_early = std::bind_front(anyone_finished_upgrade, std::cref(members0), std::ref(_ms.local()), std::ref(_abort_source));
            co_await synchronize_schema(qp.db().real_database(), _ms.local(), members0, mm, can_finish_early, _abort_source);
        }
    }


    group0_log.info("setup_group0: the cluster is ready to use Raft. Finishing.");
    co_await _client.set_group0_upgrade_state(group0_upgrade_state::use_post_raft_procedures);
}

future<> raft_group0::finish_setup_after_join(service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled) {
    if (joined_group0()) {
        group0_log.info("finish_setup_after_join: group 0 ID present, loading server info.");
        auto my_id = load_my_id();
        if (!_raft_gr.group0().get_configuration().can_vote(my_id)) {
            group0_log.info("finish_setup_after_join: becoming a voter in the group 0 configuration...");
            // Just bootstrapped and joined as non-voter. Become a voter.
            auto pause_shutdown = _shutdown_gate.hold();
            raft::server_address my_addr{my_id, {}};
            co_await _raft_gr.group0().modify_config({{my_addr, true}}, {}, &_abort_source);
            group0_log.info("finish_setup_after_join: became a group 0 voter.");

            // No need to run `upgrade_to_group0()` since we must have bootstrapped with Raft
            // (that's the only way to join as non-voter today).
            co_return;
        }
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
    co_await seastar::async([this, &ss, &qp, &mm, topology_change_enabled] {
        _raft_support_listener = _feat.supports_raft_cluster_mgmt.when_enabled([this, &ss, &qp, &mm, topology_change_enabled] {
            group0_log.info("finish_setup_after_join: SUPPORTS_RAFT feature enabled. Starting internal upgrade-to-raft procedure.");
            upgrade_to_group0(ss, qp, mm, topology_change_enabled).get();
        });
    });
}

future<> raft_group0::stop_group0() {
    if (auto* group0_id = std::get_if<raft::group_id>(&_group0)) {
        co_await _raft_gr.stop_server(*group0_id, "raft group0 is stopped");
    }
}

bool raft_group0::is_member(raft::server_id id, bool include_voters_only) {
    if (!joined_group0()) {
        on_internal_error(group0_log, "called is_member before we joined group 0");
    }

    auto cfg = _raft_gr.group0().get_configuration();
    return cfg.contains(id) && (!include_voters_only || cfg.can_vote(id));
}

future<> raft_group0::become_nonvoter(abort_source& as, std::optional<raft_timeout> timeout) {
    if (!(co_await raft_upgrade_complete())) {
        on_internal_error(group0_log, "called become_nonvoter before Raft upgrade finished");
    }

    auto my_id = load_my_id();
    group0_log.info("becoming a non-voter (my id = {})...", my_id);

    co_await make_raft_config_nonvoter({my_id}, as, timeout);
    group0_log.info("became a non-voter.", my_id);
}

future<> raft_group0::make_nonvoter(raft::server_id node, abort_source& as, std::optional<raft_timeout> timeout) {
    co_return co_await make_nonvoters({node}, as, timeout);
}

future<> raft_group0::make_nonvoters(const std::unordered_set<raft::server_id>& nodes, abort_source& as,
        std::optional<raft_timeout> timeout)
{
    if (!(co_await raft_upgrade_complete())) {
        on_internal_error(group0_log, "called make_nonvoters before Raft upgrade finished");
    }

    if (nodes.empty()) {
        co_return;
    }

    group0_log.info("making servers {} non-voters...", nodes);

    co_await make_raft_config_nonvoter(nodes, as, timeout);

    group0_log.info("servers {} are now non-voters.", nodes);
}

future<> raft_group0::leave_group0() {
    if (!(co_await raft_upgrade_complete())) {
        on_internal_error(group0_log, "called leave_group0 before Raft upgrade finished");
    }

    auto my_id = load_my_id();

    group0_log.info("leaving group 0 (my id = {})...", my_id);
    // Note: if this gets stuck due to a failure, the DB admin can abort.
    // FIXME: this gets stuck without failures if we're the leader (#10833)
    co_return co_await remove_from_raft_config(my_id);
    group0_log.info("left group 0.");
}

future<> raft_group0::remove_from_group0(raft::server_id node) {
    if (!(co_await raft_upgrade_complete())) {
        on_internal_error(group0_log, "called remove_from_group0 before Raft upgrade finished");
    }

    group0_log.info("remove_from_group0({}): removing the server from group 0 configuration...", node);

    co_await remove_from_raft_config(node);

    group0_log.info("remove_from_group0({}): finished removing from group 0 configuration.", node);
}

future<bool> raft_group0::wait_for_raft() {
    assert(this_shard_id() == 0);

    auto upgrade_state = (co_await _client.get_group0_upgrade_state()).second;
    if (upgrade_state == group0_upgrade_state::recovery) {
        group0_log.warn("In Raft RECOVERY mode.");
        co_return false;
    }

    if (!_feat.supports_raft_cluster_mgmt) {
        // The Raft feature is not yet enabled.
        //
        // In that case we assume the cluster is in a partially-upgraded state (some nodes still use a version
        // without the raft/group 0 code enabled) and skip the reconfiguration.
        //
        // In theory we could be wrong: the cluster may have just upgraded but the user managed to start
        // a topology operation before we noticed it. Removing a node at this point could cause `upgrade_to_group0`
        // on other nodes to get stuck because it requires contacting all peers which may include the removed node.
        //
        // This is unlikely and shouldn't happen if rolling upgrade is performed correctly.
        // If it does happen, there's always the possibility of performing the manual group 0 recovery procedure.
        group0_log.warn(
            "Raft feature not enabled yet. Assuming that the cluster is partially upgraded"
            " and skipping group 0 reconfiguration. However, if you already finished the rolling upgrade"
            " procedure, this means that the node just hasn't noticed it yet. The internal upgrade-to-raft procedure"
            " may get stuck. If that happens, manual recovery will be required."
            " Consult the documentation for more details: {}",
            raft_upgrade_doc);
        co_return false;
    }

    if (upgrade_state != group0_upgrade_state::use_post_raft_procedures) {
        // The feature is enabled but `upgrade_to_group0` did not finish yet.
        // The upgrade procedure requires everyone to participate. In order to not block the cluster
        // from doing the upgrades, we'll wait until we finish our procedure before doing the reconfiguration.
        //
        // Note: in theory it could happen that a node is removed/leaves the cluster immediately after
        // it finishes upgrade, causing others to get stuck (trying to contact the node that left).
        // It's unlikely and can be recovered from using the manual group 0 recovery procedure.
        group0_log.info("Waiting until cluster fully upgrades to use Raft before proceeding...");
        co_await _client.wait_until_group0_upgraded(_abort_source);
        group0_log.info("Cluster finished Raft upgrade procedure.");
    }

    // We're fully upgraded, we must have joined group 0.
    if (!joined_group0()) {
        on_internal_error(group0_log,
            "We're fully upgraded to use Raft but didn't join group 0. Please report a bug.");
    }

    group0_log.info("Performing a group 0 read barrier...");
    co_await _raft_gr.group0().read_barrier(&_abort_source);
    group0_log.info("Finished group 0 read barrier.");

    co_return true;
}

future<> raft_group0::make_raft_config_nonvoter(const std::unordered_set<raft::server_id>& ids, abort_source& as,
        std::optional<raft_timeout> timeout)
{
    static constexpr auto max_retry_period = std::chrono::seconds{1};
    auto retry_period = std::chrono::milliseconds{10};

    while (true) {
        as.check();

        std::vector<raft::config_member> add;
        add.reserve(ids.size());
        std::transform(ids.begin(), ids.end(), std::back_inserter(add),
        [] (raft::server_id id) { return raft::config_member{{id, {}}, false}; });

        try {
            co_await _raft_gr.group0_with_timeouts().modify_config(std::move(add), {}, &as, timeout);
            co_return;
        } catch (const raft::commit_status_unknown& e) {
            group0_log.info("make_raft_config_nonvoter({}): modify_config returned \"{}\", retrying", ids, e);
        }
        retry_period *= 2;
        if (retry_period > max_retry_period) {
            retry_period = max_retry_period;
        }
        co_await sleep_abortable(retry_period, as);
    }
}

future<> raft_group0::remove_from_raft_config(raft::server_id id) {
    static constexpr auto max_retry_period = std::chrono::seconds{1};
    auto retry_period = std::chrono::milliseconds{10};

    // TODO: add a timeout mechanism? This could get stuck (and _abort_source is only called on shutdown).
    while (true) {
        try {
            co_await _raft_gr.group0().modify_config({}, {id}, &_abort_source);
            break;
        } catch (const raft::commit_status_unknown& e) {
            group0_log.info("remove_from_raft_config({}): modify_config returned \"{}\", retrying", id, e);
        }
        retry_period *= 2;
        if (retry_period > max_retry_period) {
            retry_period = max_retry_period;
        }
        co_await sleep_abortable(retry_period, _abort_source);
    }
}

bool raft_group0::joined_group0() const {
    return std::holds_alternative<raft::group_id>(_group0);
}

future<bool> raft_group0::raft_upgrade_complete() const {
    auto upgrade_state = (co_await _client.get_group0_upgrade_state()).second;
    co_return upgrade_state == group0_upgrade_state::use_post_raft_procedures;
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
                // Use self as leader - modify_config() is
                // a forwarding API so we'll be able to forward
                // the request when it arrives.
                .id = _raft_gr.group0().id(),
                .ip_addr = _gossiper.get_broadcast_address(),
            }};
        }
    }, _group0);
}

future<persistent_discovery> persistent_discovery::make(discovery_peer my_addr, peer_list seeds, cql3::query_processor& qp) {
    auto peers = co_await load_discovered_peers(qp);
    // If we're restarting discovery, the peer list is loaded from
    // the discovery table and includes the seeds from
    // scylla.yaml, so ignore the 'seeds' param.
    //
    // Should we perhaps use 'seeds' instead, or use both, the
    // loaded seeds and scylla.yaml seeds?
    //
    // If a node crashes or stops during discovery, either of the
    // following two option is safe:
    // - restart the node; the discovery will resume from where it
    // stopped with the persisted seeds
    // - erase the data directory, possibly update scylla.yaml,
    // and start a new boot.
    // Updating scylla.yaml with a new set of seeds while keeping
    // the old data directory is something DBAs can potentially
    // do but their intent would be unclear at best: it is not
    // safe to ignore the old seeds, they may have learned about
    // this node already, so it's not safe to progress if they are
    // not unreachable. As long as the old seeds have to be reached,
    // adding more seeds is not very useful.
    //
    // We could check for this and throw, but since the
    // whole case is a bit made up, let's simply ignore scylla.yaml
    // seeds once we know they are persisted in the discovery table.
    if (peers.empty()) {
        peers = std::move(seeds);
    }
    // discovery::step() will automatically exclude my_addr and skip
    // duplicates in the list.
    co_return persistent_discovery{std::move(my_addr), peers, qp};
}

future<std::optional<discovery::peer_list>> persistent_discovery::request(peer_list peers) {
    for (auto& p: peers) {
        group0_log.debug("discovery: request peer: id={}, ip={}", p.id, p.ip_addr);
    }

    if (_gate.is_closed()) {
        // We stopped discovery, about to destroy it.
        co_return std::nullopt;
    }
    auto holder = _gate.hold();

    auto response = _discovery.request(peers);
    co_await store_discovered_peers(_qp, _discovery.get_peer_list());

    co_return response;
}

void persistent_discovery::response(discovery_peer from, const peer_list& peers) {
    // The peers discovered here will be persisted on the next `request` or `tick`.
    for (auto& p: peers) {
        group0_log.debug("discovery: response peer: id={}, ip={}", p.id, p.ip_addr);
    }
    _discovery.response(std::move(from), peers);
}

future<discovery::tick_output> persistent_discovery::tick() {
    // No need to enter `_gate`, since `stop` must be called after all calls to `tick` (and before the object is destroyed).

    auto result = _discovery.tick();
    co_await store_discovered_peers(_qp, _discovery.get_peer_list());

    co_return result;
}

future<> persistent_discovery::stop() {
    return _gate.close();
}

persistent_discovery::persistent_discovery(discovery_peer my_addr, const peer_list& seeds, cql3::query_processor& qp)
    : _discovery{std::move(my_addr), seeds}
    , _qp{qp}
{
    for (auto& addr: seeds) {
        group0_log.debug("discovery: seed peer: id={}, info={}", addr.id, addr.ip_addr);
    }
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
        auto sub = as.subscribe([&timeout_src] () noexcept {
            if (!timeout_src.abort_requested()) {
                timeout_src.request_abort();
            }
        });
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

// A helper class to sleep in a loop with an exponentially
// increasing retry period.
struct sleep_with_exponential_backoff {
    std::chrono::seconds _retry_period{1};
    static constexpr std::chrono::seconds _max_retry_period{16};
    future<> operator()(abort_source& as,
                        seastar::compat::source_location loc = seastar::compat::source_location::current()) {
        upgrade_log.info("{}: sleeping for {} seconds before retrying...", loc.function_name(), _retry_period);
        co_await sleep_abortable(_retry_period, as);
        _retry_period = std::min(_retry_period * 2, _max_retry_period);
    }
};


// Precondition: we joined group 0 and the server is running.
// Assumes we don't leave group 0 while running.
static future<> wait_until_every_peer_joined_group0(db::system_keyspace& sys_ks, const group0_members& members0, abort_source& as) {

    for (sleep_with_exponential_backoff sleep;; co_await sleep(as)) {
        // We fetch both config and peers on each iteration; we don't assume that they don't change.
        // No new node should join while the procedure is running, but nodes may leave.

        auto current_config = members0.get_inet_addrs();
        if (current_config.empty()) {
            // Not all addresses are known
            continue;
        }
        std::sort(current_config.begin(), current_config.end());

        auto peers = co_await sys_ks.load_peers();
        std::sort(peers.begin(), peers.end());

        std::vector<gms::inet_address> missing_peers;
        std::set_difference(peers.begin(), peers.end(), current_config.begin(), current_config.end(), std::back_inserter(missing_peers));

        if (missing_peers.empty()) {
            if (!members0.is_joint()) {
                co_return;
            }

            upgrade_log.info("group 0 configuration is joint: {}.", current_config);
            continue;
        }

        upgrade_log.info(
            "group 0 configuration does not contain all peers yet."
            " Missing peers: {}. Current group 0 config: {}.",
            missing_peers, current_config);
    }
}

// Check if anyone entered `use_post_raft_procedures`.
// This is a best-effort single round-trip check; we don't retry if some nodes fail to answer.
static future<bool> anyone_finished_upgrade(
        const group0_members& members0, netw::messaging_service& ms, abort_source& as) {
    static constexpr auto max_concurrency = 10;
    static constexpr auto rpc_timeout = std::chrono::seconds{5};

    auto current_config = members0.get_inet_addrs();
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
        const noncopyable_function<future<std::vector<gms::inet_address>>()>& get_inet_addrs,
        netw::messaging_service& ms, abort_source& as) {
    static constexpr auto rpc_timeout = std::chrono::seconds{5};
    static constexpr auto max_concurrency = 10;

    for (sleep_with_exponential_backoff sleep;; co_await sleep(as)) {
        // Note: we strive to get a response from everyone in a 'single round-trip',
        // so we don't skip nodes which responded in earlier iterations.
        // We contact everyone in each iteration even if some of these guys already answered.
        // We fetch peers again on every attempt to handle the possibility of leaving nodes.
        auto cluster_config = co_await get_inet_addrs();
        if (cluster_config.empty()) {
            continue;
        }

        bool retry = false;
        co_await max_concurrent_for_each(cluster_config, max_concurrency, [&] (const gms::inet_address& node) -> future<> {
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
    }
}

future<> raft_group0::wait_for_all_nodes_to_finish_upgrade(abort_source& as) {
    static constexpr auto rpc_timeout = std::chrono::seconds{5};
    static constexpr auto max_concurrency = 10;

    for (sleep_with_exponential_backoff sleep;; co_await sleep(as)) {
        group0_members members0{_raft_gr.group0(), _raft_gr.address_map()};
        auto current_config = members0.get_inet_addrs();
        if (current_config.empty()) {
            continue;
        }

        std::unordered_set<gms::inet_address> pending_nodes{current_config.begin(), current_config.end()};
        co_await max_concurrent_for_each(current_config, max_concurrency, [&] (const gms::inet_address& node) -> future<> {
            try {
                upgrade_log.info("wait_for_everybody_to_finish_upgrade: `send_get_group0_upgrade_state({})`", node);
                const auto upgrade_state = co_await with_timeout(as, rpc_timeout, std::bind_front(send_get_group0_upgrade_state, std::ref(_ms.local()), node));
                if (upgrade_state == group0_upgrade_state::use_post_raft_procedures) {
                    pending_nodes.erase(node);
                }
            } catch (abort_requested_exception&) {
                upgrade_log.warn("wait_for_everybody_to_finish_upgrade: abort requested during `send_get_group0_upgrade_state({})`", node);
                throw;
            } catch (...) {
                upgrade_log.warn(
                        "wait_for_everybody_to_finish_upgrade: `send_get_group0_upgrade_state({})` failed: {}",
                        node, std::current_exception());
            }
        });

        if (pending_nodes.empty()) {
            co_return;
        } else {
            upgrade_log.warn("wait_for_everybody_to_finish_upgrade: nodes {} didn't finish upgrade yet", pending_nodes);
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
        const group0_members& members0, netw::messaging_service& ms, abort_source& as, gate::holder pause_shutdown) {
    static constexpr auto rpc_timeout = std::chrono::seconds{5};
    static constexpr auto max_concurrency = 10;

    auto entered_synchronize = make_lw_shared<std::unordered_set<gms::inet_address>>();

    // This is a work-around for boost tests where RPC module is not listening so we cannot contact ourselves.
    // But really, except the (arguably broken) test code, we don't need to be treated as an edge case. All nodes are symmetric.
    // For production code this line is unnecessary.
    entered_synchronize->insert(ms.broadcast_address());

    for (sleep_with_exponential_backoff sleep;; co_await sleep(as)) {
        // We fetch the config again on every attempt to handle the possibility of removing failed nodes.
        auto current_members_set = members0.get_members();

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
                   raft::config_member_set current_members_set, group0_members members0,
                   lw_shared_ptr<std::unordered_set<gms::inet_address>> entered_synchronize,
                   lw_shared_ptr<bool> retry, ::tracker<bool> tracker) -> future<> {
            co_await max_concurrent_for_each(current_members_set, max_concurrency, [&] (const raft::config_member& member) -> future<> {
                auto node_opt = members0.get_inet_addr(member);

                if (!node_opt.has_value()) {
                    upgrade_log.warn("wait_for_peers_to_enter_synchronize_state: cannot resolve the IP of {}", member);
                    *retry = true;
                    co_return;
                }

                auto node = *node_opt;

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
        }(ms, as, pause_shutdown, std::move(current_members_set), members0, entered_synchronize, retry, tracker);

        auto finish_early = co_await tracker.get();
        if (finish_early) {
            co_return false;
        }

        if (!*retry) {
            co_return true;
        }
    }
}

// Returning nullopt means we finished early (`can_finish_early` returned true).
static future<std::optional<std::unordered_map<gms::inet_address, table_schema_version>>>
collect_schema_versions_from_group0_members(
        netw::messaging_service& ms, const group0_members& members0,
        const noncopyable_function<future<bool>()>& can_finish_early,
        abort_source& as) {
    static constexpr auto rpc_timeout = std::chrono::seconds{5};
    static constexpr auto max_concurrency = 10;

    std::unordered_map<gms::inet_address, table_schema_version> versions;
    for (sleep_with_exponential_backoff sleep;; co_await sleep(as)) {

        // We fetch the config on each iteration; some nodes may leave.
        auto current_config = members0.get_inet_addrs();
        if (current_config.empty()) {
            continue;
        }

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
        } else if (members0.is_joint()) {
            upgrade_log.warn("synchronize_schema: group 0 configuration is joint: {}.", current_config);
        } else {
            co_return versions;
        }

        upgrade_log.info("synchronize_schema: checking if we can finish early before retrying...");

        if (co_await can_finish_early()) {
            co_return std::nullopt;
        }

        upgrade_log.info("synchronize_schema: could not finish early.");
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
        const group0_members& members0,
        service::migration_manager& mm,
        const noncopyable_function<future<bool>()>& can_finish_early,
        abort_source& as) {
    static constexpr auto max_concurrency = 10;

    bool last_pull_successful = false;
    size_t num_attempts_after_successful_pull = 0;

    for (sleep_with_exponential_backoff sleep;; co_await sleep(as)) {
        upgrade_log.info("synchronize_schema: collecting schema versions from group 0 members...");
        auto remote_versions = co_await collect_schema_versions_from_group0_members(ms, members0, can_finish_early, as);
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
                " Consult the relevant documentation: {}", raft_upgrade_doc);
        }
    }(*as);

    return defer([task = std::move(task), as = std::move(as)] () mutable {
        // Stop in background.
        as->request_abort();
        (void)std::move(task).then([as = std::move(as)] {});
    });
}

future<> raft_group0::upgrade_to_group0(service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled) {
    assert(this_shard_id() == 0);

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
            break;
    }

    (void)[] (raft_group0& self, abort_source& as, group0_upgrade_state start_state, gate::holder pause_shutdown, service::storage_service& ss, cql3::query_processor& qp,
                service::migration_manager& mm, bool topology_change_enabled) -> future<> {
        auto warner = warn_if_upgrade_takes_too_long();
        try {
            co_await self.do_upgrade_to_group0(start_state, ss, qp, mm, topology_change_enabled);
            co_await self._client.set_group0_upgrade_state(group0_upgrade_state::use_post_raft_procedures);
            upgrade_log.info("Raft upgrade finished. Disabling migration_manager schema pulls.");
            co_await mm.disable_schema_pulls();
        } catch (...) {
            upgrade_log.error(
                "Raft upgrade failed: {}.\nTry restarting the node to retry upgrade."
                " If the procedure gets stuck, manual recovery may be required."
                " Consult the relevant documentation: {}", std::current_exception(), raft_upgrade_doc);
        }
    }(std::ref(*this), std::ref(_abort_source), start_state, _shutdown_gate.hold(), ss, qp, mm, topology_change_enabled);
}

// `start_state` is either `use_pre_raft_procedures` or `synchronize`.
future<> raft_group0::do_upgrade_to_group0(group0_upgrade_state start_state, service::storage_service& ss, cql3::query_processor& qp, service::migration_manager& mm, bool topology_change_enabled) {
    assert(this_shard_id() == 0);

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
    auto get_inet_addrs = [this]() -> future<std::vector<gms::inet_address>> {
        auto current_config = co_await _sys_ks.load_peers();
        current_config.push_back(_gossiper.get_broadcast_address());
        co_return current_config;
    };
    co_await check_remote_group0_upgrade_state_dry_run(get_inet_addrs, _ms.local(), _abort_source);

    if (!joined_group0()) {
        upgrade_log.info("Joining group 0...");
        auto handshaker = make_legacy_handshaker(true); // Voter
        co_await join_group0(co_await _sys_ks.load_peers(), std::move(handshaker), ss, qp, mm, _sys_ks, topology_change_enabled);
    } else {
        upgrade_log.info(
            "We're already a member of group 0."
            " Apparently we're restarting after a previous upgrade attempt failed.");
    }

    // Start group 0 leadership monitor fiber.
    _leadership_monitor = leadership_monitor_fiber();

    group0_members members0{_raft_gr.group0(), _raft_gr.address_map()};

    // After we joined, we shouldn't be removed from group 0 until the end of the procedure.
    // The implementation of `leave_group0` waits until upgrade finishes before leaving the group.
    // There is no guarantee that `remove_from_group0` from another node (that has
    // finished upgrading) won't remove us after we enter `synchronize` but before we leave it;
    // but then we're not needed for anything anymore and we can be shutdown,
    // and we won't do anything harmful to other nodes while in `synchronize`, worst case being
    // that we get stuck.

    upgrade_log.info("Waiting until every peer has joined Raft group 0...");
    co_await wait_until_every_peer_joined_group0(_sys_ks, members0, _abort_source);
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
        co_await synchronize_schema(qp.db().real_database(), _ms.local(), members0, mm, can_finish_early, _abort_source);

        // Before entering `synchronize`, perform a round-trip of `get_group0_upgrade_state` RPC calls
        // to everyone as a dry run, just to check that nodes respond to this RPC.
        // Obviously we may lose connectivity immediately after this function finishes,
        // causing later steps to fail, but if network/RPC module is already broken, better to detect
        // it now than after entering `synchronize` state. And if this steps succeeds, then there's
        // a very high chance that the following steps succeed as well (we would need to be very unlucky otherwise).
        upgrade_log.info("Performing a dry run of remote `get_group0_upgrade_state` calls...");
        co_await check_remote_group0_upgrade_state_dry_run(
                [members0] {
                    return make_ready_future<std::vector<gms::inet_address>>(members0.get_inet_addrs());
                }, _ms.local(), _abort_source);

        utils::get_local_injector().inject("group0_upgrade_before_synchronize",
            [] { throw std::runtime_error("error injection before group 0 upgrade enters synchronize"); });

        upgrade_log.info("Entering synchronize state.");
        upgrade_log.warn("Schema changes are disabled in synchronize state."
                " If a failure makes us unable to proceed, manual recovery will be required.");
        co_await _client.set_group0_upgrade_state(group0_upgrade_state::synchronize);
    }

    upgrade_log.info("Waiting for all peers to enter synchronize state...");
    if (!(co_await wait_for_peers_to_enter_synchronize_state(members0, _ms.local(), _abort_source, _shutdown_gate.hold()))) {
        upgrade_log.info("Another node already finished upgrade. We can finish early.");
        co_return;
    }

    upgrade_log.info("All peers in synchronize state. Waiting for schema to synchronize...");
    auto can_finish_early = std::bind_front(anyone_finished_upgrade, std::cref(members0), std::ref(_ms.local()), std::ref(_abort_source));
    if (!(co_await synchronize_schema(qp.db().real_database(), _ms.local(), members0, mm, can_finish_early, _abort_source))) {
        upgrade_log.info("Another node already finished upgrade. We can finish early.");
        co_return;
    }

    upgrade_log.info("Schema synchronized.");
}

void raft_group0::register_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("raft_group0", {
        sm::make_gauge("status", [this] { return static_cast<uint8_t>(_status_for_monitoring); },
            sm::description("status of the raft group, 1 - normal, 2 - aborted"))
    });
}

const raft_address_map& raft_group0::address_map() const {
    return _raft_gr.address_map();
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

