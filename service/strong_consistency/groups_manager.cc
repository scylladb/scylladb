/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "groups_manager.hh"

#include "locator/tablets.hh"
#include "raft/raft.hh"
#include "service/migration_manager.hh"
#include "service/strong_consistency/state_machine.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "gms/feature_service.hh"
#include "gms/gossiper.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_timeout.hh"
#include "service/storage_proxy.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "idl/strong_consistency/groups_manager.dist.hh"
#include "utils/error_injection.hh"
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "service/topology_guard.hh"
#include "utils/chain_abort_source.hh"
#include "utils/exponential_backoff_retry.hh"

#include <seastar/core/abort_source.hh>

namespace service::strong_consistency {

using namespace locator;

static logging::logger logger("sc_groups_manager");

// How long a single attempt at a raft configuration change may take. It only needs
// to be generous enough for a healthy quorum to commit the change; an attempt that
// runs out of time is reported by the barrier that scheduled it, which then retries.
static constexpr auto config_sync_timeout = std::chrono::seconds(30);

static raft::server_id to_server_id(host_id host_id) {
    return raft::server_id{host_id.uuid()};
};

// Precondition: The passed group_leader must be a non-trivial raft::server_id.
static std::optional<locator::tablet_replica_set> prepare_replicas_for_sc_tablet_version(locator::tablet_replica_set replicas, raft::server_id group_leader) {
    std::ranges::sort(replicas);
    const auto leader_host_id = locator::host_id{group_leader.uuid()};
    auto leader_it = std::ranges::find(replicas, leader_host_id, &tablet_replica::host);
    if (leader_it == replicas.end()) [[unlikely]] {
        on_internal_error(logger, seastar::format("Leader ({}) is not among the replicas: {}",
                leader_host_id, replicas));
    }
    std::ranges::rotate(replicas, leader_it);
    return std::make_optional(std::move(replicas));
}

class groups_manager::rpc_impl: public service::raft_rpc {
public:
    rpc_impl(raft_state_machine& sm, netw::messaging_service& ms,
             shared_ptr<raft::failure_detector> failure_detector,
             raft::group_id gid, raft::server_id my_id)
        : service::raft_rpc(sm, ms, std::move(failure_detector), gid, my_id)
    {
    }

    void on_configuration_change(raft::server_address_set add, raft::server_address_set del) override {
    }
};

raft_server::raft_server(groups_manager::raft_group_state& state, gate::holder holder)
    : _state(state)
    , _holder(std::move(holder))
{
}

// conditional_variable::wait doesn't have an overload taking an abort_source.
// This is a temporary workaround until we extend the interface.
// See: scylladb/seastar#3292.
static future<> wait_with_abort_source(condition_variable& cv, abort_source& as) {
    if (as.abort_requested()) {
        return make_exception_future<>(as.abort_requested_exception_ptr());
    }

    auto sub = as.subscribe([&cv] noexcept { cv.broadcast(); });

    return cv.wait().then([&as, sub = std::move(sub)] {
        return as.abort_requested()
            ? make_exception_future<>(as.abort_requested_exception_ptr())
            : make_ready_future();
    });
}

auto raft_server::begin_mutate(abort_source& as) -> begin_mutate_result {
    const auto leader = _state.server->current_leader();
    if (!leader) {
        return need_wait_for_leader{_state.server->wait_for_leader(&as)};
    }
    if (leader != _state.server->id()) {
        return raft::not_a_leader{leader};
    }
    const auto term = _state.server->get_current_term();
    if (!_state.leader_info || _state.leader_info->term != term) {
        // We are the leader, but the leader_info_updater fiber hasn't processed
        // the state change yet (leader_info is either empty or stale).
        //
        // We must wait for the updater to catch up. It is safe to wait on
        // leader_info_cond because the updater fiber guarantees a broadcast
        // after every state change wake-up. This ensures we will not deadlock,
        // even if the raft server state changes again (e.g., we lose leadership)
        // before the updater gets a chance to run.
        return need_wait_for_leader{wait_with_abort_source(_state.leader_info_cond, as)};
    }
    if (utils::get_local_injector().enter("sc_begin_mutate_wait_for_leader")) {
        // Test-only: emulate a leader whose leader_info never becomes available,
        // so callers wait on leader_info_cond until their own deadline fires.
        return need_wait_for_leader{wait_with_abort_source(_state.leader_info_cond, as)};
    }
    const auto new_ts = std::max(api::new_timestamp(), _state.leader_info->last_timestamp + 1);
    _state.leader_info->last_timestamp = new_ts;
    return timestamp_with_term{new_ts, term};
}

auto raft_server::begin_read(abort_source& as) -> begin_read_result {
    const auto leader = _state.server->current_leader();
    if (!leader) {
        return need_wait_for_leader{_state.server->wait_for_leader(&as)};
    }
    if (leader != _state.server->id()) {
        return raft::not_a_leader{leader};
    }
    return ok{};
}

groups_manager::groups_manager(netw::messaging_service& ms, 
        raft_group_registry& raft_gr, cql3::query_processor& qp,
        replica::database& db, service::migration_manager& mm, db::system_keyspace& sys_ks, gms::feature_service& features,
        gms::gossiper& gossiper, db::raft_commitlog_replay_buffer& raft_replay_buffer)
    : _ms(ms)
    , _raft_gr(raft_gr)
    , _qp(qp)
    , _db(db)
    , _mm(mm)
    , _sys_ks(sys_ks)
    , _features(features)
    , _gossiper(gossiper)
    , _raft_replay_buffer(raft_replay_buffer)
{
    init_messaging_service();
}

future<> groups_manager::start_raft_group(global_tablet_id tablet,
        raft::group_id group_id,
        token_metadata_ptr tm)
{
    const auto my_id = to_server_id(tm->get_my_id());
    const auto this_replica = locator::tablet_replica{
        .host = tm->get_my_id(),
        .shard = this_shard_id(),
    };


    auto* commitlog = _db.commitlog();
    SCYLLA_ASSERT(commitlog);
    auto storage = std::make_unique<raft_groups_storage>(_qp, group_id, my_id, this_shard_id(),
        *commitlog, tablet.table, _raft_replay_buffer.take_replayed_group_entries(group_id));

    auto state_machine = make_state_machine(tablet, group_id, _db, _mm, _sys_ks, *storage);

    auto& state_machine_ref = *state_machine;
    auto rpc = std::make_unique<rpc_impl>(state_machine_ref, _ms, _raft_gr.failure_detector(), group_id, my_id);
    // Keep a reference to a specific RPC class.
    auto& rpc_ref = *rpc;

    // Store the initial configuration if this is the first time we create this group
    // on this node
    const auto snapshot = co_await storage->load_snapshot_descriptor();
    if (!snapshot.id) {
        const auto& tablet_map = tm->tablets().get_tablet_map(tablet.table);
        const auto& tablet_info = tablet_map.get_tablet_info(tablet.tablet);
        const auto* trinfo = tablet_map.get_tablet_transition_info(tablet.tablet);
        const bool is_joining_replica = trinfo && !locator::contains(tablet_info.replicas, this_replica);

        if (is_joining_replica) {
            raft::configuration configuration;
            co_await storage->bootstrap(std::move(configuration), false);
        } else {
            raft::configuration configuration;
            configuration.current.reserve(tablet_info.replicas.size());
            for (const auto& r: tablet_info.replicas) {
                configuration.current.emplace(raft::server_address{to_server_id(r.host), {}},
                    raft::is_voter::yes);
            }
            co_await storage->bootstrap(std::move(configuration), false);
        }
    }

    auto& persistence_ref = *storage;
    auto config = raft::server::configuration {
        // Snapshotting is not implemented yet for strong consistency,
        // so effectively disable periodic snapshotting.
        // TODO: Revert after snapshots are implemented
        .snapshot_threshold = std::numeric_limits<size_t>::max(),
        .snapshot_threshold_log_size = 10 * 1024 * 1024, // 10MB
        .max_log_size = 20 * 1024 * 1024, // 20MB
        .enable_forwarding = false,
        .on_background_error = [tablet, group_id](std::exception_ptr e) {
            on_internal_error(logger, 
                ::format("table {}, tablet {} raft group {} background error {}", 
                    tablet.table, tablet.tablet, group_id, e));
        },
        .tag = format("sc-{}", group_id),
        // Spread initial tablet-group leadership across nodes: derive the
        // fast-bootstrap leader choice from the group id so that different
        // groups pick different replicas instead of all electing the
        // smallest-id node (which would concentrate load on one node when a
        // table starts with many tablets).
        .fast_bootstrap_seed = std::hash<raft::group_id>()(group_id)
    };
    auto server = raft::create_server(my_id, std::move(rpc), std::move(state_machine),
            std::move(storage), _raft_gr.failure_detector(), config);

    // initialize the corresponding timer to tick the raft server instance
    auto ticker = std::make_unique<raft_ticker_type>([srv = server.get()] { srv->tick(); });

    // Tests may lengthen the tick interval via error injection so that any
    // unwanted waiting on a raft tick becomes visible as a large delay.
    const auto tick_interval = utils::get_local_injector()
            .inject_parameter<int64_t>("strongly-consistent-raft-group-tick-interval-in-ms")
            .transform([](int64_t ms) { return raft_ticker_type::duration{std::chrono::milliseconds{ms}}; })
            .value_or(raft_tick_interval);

    co_await _raft_gr.start_server_for_group(raft_server_for_group {
        .gid = group_id,
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
        .persistence = persistence_ref,
        .state_machine = state_machine_ref
    }, tick_interval);
}

void groups_manager::schedule_raft_group_deletion(raft::group_id id, raft_group_state& state) {
    if (state.gate->is_closed()) {
        return;
    }
    logger.info("schedule_raft_group_deletion(): group id {}: scheduling", id);

    // Close the gate synchronously so state.gate->is_closed() flips immediately
    // and a concurrent schedule_raft_group_deletion() for the same group bails
    // out at the guard above. Closing inside the operation instead would let a
    // second deletion slip past the guard, and both would call gate::close() on
    // the same gate - the second call aborts the process.
    //
    // close() doesn't block here; the operation waits for the gate below. The
    // gate won't drain until all holders are released, but in-flight writes may
    // be stuck in add_entry awaiting a quorum that will never come (other nodes
    // already destroyed their servers). Aborting the raft server releases those
    // holders by making the stuck operations throw raft::stopped_error.
    auto gate_fut = state.gate->close();
    logger.debug("schedule_raft_group_deletion(): group id {}: gate close initiated", id);

    state.server_control_op = futurize_invoke([this, &state, id, g = state.gate, gate_fut = std::move(gate_fut)](this auto) -> future<> {
        co_await state.server_control_op.get_future();
        logger.debug("schedule_raft_group_deletion(): group id {}: starting", id);

        co_await _raft_gr.abort_server(id);
        logger.debug("schedule_raft_group_deletion(): group id {}: server aborted", id);

        co_await std::move(gate_fut);
        logger.debug("schedule_raft_group_deletion(): group id {}: gate closed", id);

        co_await std::move(state.leader_info_updater);

        // Both background fibers hold the gate, so the wait above already drained
        // them, but nothing may reference the state after it's erased below and this
        // makes that explicit. Note that deletion is never queued behind a
        // configuration change: abort_server() above is what terminates it.
        co_await state.config_sync.get_future();

        _raft_gr.destroy_server(id);
        logger.info("schedule_raft_group_deletion(): raft server for group id {} is destroyed", id);

        // We need to erase the raft group state only if we are still the last operation on it.
        // If another start arrived while we were stopping the raft server, a new gate
        // would have been assigned, and we should leave the state in the map.
        if (state.gate.get() == g.get() && _raft_groups.erase(id) != 1) {
            on_internal_error(logger, format("raft group {} is already deleted", id));
        }
    });
}

void groups_manager::schedule_raft_groups_deletion(bool all) {
    for (auto it = _raft_groups.begin(); it != _raft_groups.end(); ) {
        const auto next = std::next(it);
        auto& [group_id, group_state] = *it;
        if (all || !group_state.has_tablet) {
            schedule_raft_group_deletion(group_id, group_state);
        }
        it = next;
    }
}

future<> groups_manager::wait_for_groups_to_start(lowres_clock::time_point timeout) {
    while (!_starting_groups.empty()) {
        auto& state = _starting_groups.front();
        co_await state.server_control_op.get_future(timeout); // the state is unlinked when this completes
    }
}

future<> groups_manager::wait_for_snapshot_transfer(locator::global_tablet_id tablet, raft::group_id group_id, service::session_id session_id) {
    const auto timeout = lowres_clock::now() + std::chrono::minutes(5);

    co_await wait_for_groups_to_start(timeout);

    {
        const auto it = _raft_groups.find(group_id);
        if (it == _raft_groups.end()) {
            throw std::runtime_error(format("No raft group {} for tablet {} on this host", group_id, tablet));
        }
        co_await it->second.server_control_op.get_future(timeout);
    }

    // The group may have been deleted while we waited - the migration was rolled back
    // and this replica rolled away, or the table was dropped - so look it up again and
    // hold the gate for the rest of the operation, or state.server would dangle. There
    // is no scheduling point between the check and hold().
    const auto it = _raft_groups.find(group_id);
    if (it == _raft_groups.end() || !it->second.gate || it->second.gate->is_closed() || !it->second.server) {
        throw std::runtime_error(format("Raft group {} for tablet {} is not running on this host", group_id, tablet));
    }
    auto& state = it->second;
    auto holder = state.gate->hold();

    topology_guard g(session_id);

    co_await utils::get_local_injector().inject("sc_wait_for_snapshot_transfer", utils::wait_for_message(20min));

    abort_on_expiry aoe(timeout);
    auto sub = utils::chain_abort_source(aoe.abort_source(), g.abort_source());
    co_await state.server->read_barrier(&aoe.abort_source());
}

void groups_manager::init_messaging_service() {
    ser::groups_manager_rpc_verbs::register_wait_for_raft_groups_to_start(&_ms,
        [this] (rpc::opt_time_point timeout, raft::server_id dst_id, table_id table) -> future<> {
            if (_raft_gr.get_my_raft_id() != dst_id) {
                throw raft_destination_id_not_correct{_raft_gr.get_my_raft_id(), dst_id};
            }
            co_await _mm.get_group0_barrier().trigger();
            co_await container().invoke_on_all([timeout] (groups_manager& gm) {
                return gm.wait_for_groups_to_start(*timeout);
            });
        }
    );
    ser::groups_manager_rpc_verbs::register_wait_for_snapshot_transfer(&_ms,
        [this] (raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id group_id, utils::UUID session_id) -> future<> {
            if (_raft_gr.get_my_raft_id() != dst_id) {
                throw raft_destination_id_not_correct{_raft_gr.get_my_raft_id(), dst_id};
            }
            co_await _mm.get_group0_barrier().trigger();

            const auto dst_shard = [&]() -> shard_id {
                auto& table = _db.find_column_family(tablet.table);
                auto erm = table.get_effective_replication_map();
                const auto& tmap = erm->get_token_metadata().tablets().get_tablet_map(tablet.table);
                const auto* trinfo = tmap.get_tablet_transition_info(tablet.tablet);
                if (!trinfo || !trinfo->pending_replica) {
                    throw std::runtime_error(fmt::format("No pending replica for group {}", group_id));
                }
                if (trinfo->pending_replica->host != erm->get_token_metadata().get_my_id()) {
                    throw std::runtime_error(fmt::format("Tablet {} pending replica {} is not on this host", tablet, *trinfo->pending_replica));
                }
                return trinfo->pending_replica->shard;
            }();

            co_await container().invoke_on(dst_shard, [tablet, group_id, session_id] (groups_manager& gm) {
                return gm.wait_for_snapshot_transfer(tablet, group_id, service::session_id(session_id));
            });
        }
    );
}

future<> groups_manager::uninit_messaging_service() {
    return ser::groups_manager_rpc_verbs::unregister(&_ms);
}

future<> groups_manager::wait_for_table_raft_groups_on_all_hosts(table_id table, lowres_clock::time_point timeout) {
    auto& cf = _db.find_column_family(table);
    auto erm = cf.get_effective_replication_map();
    auto& tmap = erm->get_token_metadata().tablets().get_tablet_map(table);
    if (!tmap.has_raft_info()) {
        on_internal_error(logger, format("Table {} does not have raft info", table));
    }

    std::unordered_set<locator::host_id> hosts;
    for (const auto& tablet_info : tmap.tablets()) {
        for (const auto& replica : tablet_info.replicas) {
            hosts.insert(replica.host);
        }
        co_await coroutine::maybe_yield();
    }

    logger.debug("wait_for_table_raft_groups_on_all_hosts: waiting for raft groups to start on {} hosts", hosts.size());

    const auto my_id = erm->get_token_metadata().get_my_id();
    auto live_members = _gossiper.get_live_members();

    co_await coroutine::parallel_for_each(hosts, [&](locator::host_id host) -> future<> {
        if (host == my_id) {
            co_await container().invoke_on_all([timeout](groups_manager& gm) {
                return gm.wait_for_groups_to_start(timeout);
            });
        } else if (live_members.contains(host)) {
            auto dst = raft::server_id(host.uuid());
            try {
                co_await ser::groups_manager_rpc_verbs::send_wait_for_raft_groups_to_start(
                        &_ms, host, timeout, dst, table);
            } catch (...) {
                static thread_local logger::rate_limit rate_limit{std::chrono::seconds(5)};
                logger.log(log_level::warn, rate_limit,
                    "wait_for_table_raft_groups_on_all_hosts: failed to complete on node {}: {}",
                    host, std::current_exception());
            }
        }
    });
}

future<> groups_manager::leader_info_updater(raft_group_state& state, global_tablet_id tablet, raft::group_id gid) {
    try {
        const auto schema = _db.find_schema(tablet.table);
        const auto server_id = state.server->id();

        while (true) {
            const auto current_term = state.server->get_current_term();
            const auto current_leader = state.server->current_leader();

            if (current_leader == server_id) {
                logger.debug("leader_info_updater({}-{}): current term {}, running read_barrier()",
                    tablet, gid,
                    current_term);
                // We intentionally pass nullptr here. If the tablet is leaving this node,
                // the Raft server will be aborted and the loop will break.
                // The same will happen when the node is shutting down.
                // There's no reason to abort this operation in any other case.
                co_await state.server->read_barrier(nullptr);

                state.leader_info = leader_info {
                    .term = current_term,
                    .last_timestamp = schema->table().get_max_timestamp_for_tablet(tablet.tablet)
                };
                logger.debug("leader_info_updater({}-{}): read_barrier() completed, "
                    "new leader term {}, last_timestamp {}",
                    tablet, gid,
                    state.leader_info->term,
                    state.leader_info->last_timestamp);
            } else if (state.leader_info) {
                logger.debug("leader_info_updater({}-{}): this replica {} is no longer a leader, current leader {}",
                    tablet, gid, server_id, current_leader);
                state.leader_info = std::nullopt;
            }
            state.leader_info_cond.broadcast();

            // We intentionally pass nullptr here. If the tablet is leaving this node,
            // the Raft server will be aborted and the loop will break.
            // The same will happen when the node is shutting down.
            // There's no reason to abort this operation in any other case.
            co_await state.server->wait_for_state_change(nullptr);
        }
    } catch (const raft::request_aborted&) {
        // thrown from read_barrier() and wait_for_state_change when the tablet leaves this shard
        logger.debug("leader_info_updater({}-{}): got raft::request_aborted {}",
            tablet, gid, std::current_exception());
    } catch (const raft::stopped_error&) {
        // thrown from read_barrier() and wait_for_state_change when the tablet leaves this shard
        logger.debug("leader_info_updater({}-{}): got raft::stopped_error {}",
            tablet, gid, std::current_exception());
    } catch (const replica::no_such_column_family&) {
        // thrown from find_schema() and schema->table() when the table is dropped
        logger.debug("leader_info_updater({}-{}): got replica::no_such_column_family {}",
            tablet, gid, std::current_exception());
    } catch (...) {
        on_internal_error(logger, ::format("leader_info_updater({}-{}): unexpected exception: {}",
            tablet, gid, std::current_exception()));
    }
}

static raft::config_member_set to_voter_set(const locator::tablet_replica_set& replicas) {
    raft::config_member_set members;
    members.reserve(replicas.size());
    for (const auto& r : replicas) {
        members.emplace(raft::server_address{to_server_id(r.host), {}}, raft::is_voter::yes);
    }
    return members;
}

// The raft group configuration implied by a tablet's replica set and the stage its
// migration is in.
//
// This is the single source of truth for the group's expected membership: both the
// delta that drives a configuration change and the check that validates it are
// derived from here, so the two cannot drift apart.
static raft::config_member_set expected_raft_config(
        const locator::tablet_info& tinfo,
        const locator::tablet_transition_info* trinfo) {
    if (!trinfo) {
        return to_voter_set(tinfo.replicas);
    }

    switch (trinfo->stage) {
        case tablet_transition_stage::start_migration:
        case tablet_transition_stage::write_both_read_old_fallback_cleanup:
        case tablet_transition_stage::rebuild_repair:
        case tablet_transition_stage::repair:
        case tablet_transition_stage::end_repair:
        case tablet_transition_stage::restore:
        // The rollback path restores the old replica set.
        case tablet_transition_stage::sc_rollback:
        case tablet_transition_stage::cleanup_target:
        case tablet_transition_stage::revert_migration:
            return to_voter_set(tinfo.replicas);

        case tablet_transition_stage::sc_add_nonvoter:
        case tablet_transition_stage::sc_snapshot_transfer: {
            auto members = to_voter_set(tinfo.replicas);
            if (trinfo->pending_replica) {
                // Inserted after the voters, so that an intra-node migration, where
                // the pending replica shares a host with a current replica, doesn't
                // demote that host to a non-voter.
                members.emplace(raft::server_address{to_server_id(trinfo->pending_replica->host), {}},
                        raft::is_voter::no);
            }
            return members;
        }

        case tablet_transition_stage::sc_become_voter:
        case tablet_transition_stage::use_new:
        case tablet_transition_stage::cleanup:
        case tablet_transition_stage::end_migration:
            return to_voter_set(trinfo->next);
    }
    on_internal_error(logger, format("expected_raft_config: unknown tablet transition stage {}",
            static_cast<int>(trinfo->stage)));
}

// Is this replica expected to be a voter of the group, and can therefore attest to
// the group's configuration?
static bool is_expected_voter(const raft::config_member_set& expected, raft::server_id id) {
    const auto it = expected.find(id);
    return it != expected.end() && it->can_vote == raft::is_voter::yes;
}

// What separates a raft group's live configuration from the one its tablet's current
// migration stage implies.
struct config_sync_work {
    std::vector<raft::config_member> to_add;
    std::vector<raft::server_id> to_del;

    // A configuration change is still in progress, i.e. the group is in a joint
    // configuration. The delta can already be empty at that point, because it is
    // computed against C_new, so this has to be checked separately before declaring
    // the group converged.
    bool change_in_progress = false;

    // This replica is expected to be a voter of the group in the tablet's current
    // stage, and can therefore attest to the group's configuration.
    bool expected_voter = false;

    bool has_delta() const {
        return !to_add.empty() || !to_del.empty();
    }

    bool converged() const {
        return !has_delta() && !change_in_progress;
    }
};

// Diffs the configuration the tablet's stage implies against the group's live
// configuration.
//
// Always derived from the state current at call time and never stored: a delta that
// outlived the stage which implied it could drive the group towards a configuration
// the topology coordinator has already abandoned.
static config_sync_work assess_config_sync(const raft::server& server,
        const locator::tablet_map& tmap, locator::tablet_id tid) {
    const auto expected = expected_raft_config(tmap.get_tablet_info(tid), tmap.get_tablet_transition_info(tid));
    const auto config = server.get_configuration();

    config_sync_work work;
    work.change_in_progress = config.is_joint();
    for (const auto& member : expected) {
        const auto it = config.current.find(member.addr.id);
        if (it == config.current.end() || it->can_vote != member.can_vote) {
            work.to_add.push_back(member);
        }
    }
    for (const auto& member : config.current) {
        if (!expected.contains(member.addr.id)) {
            work.to_del.push_back(member.addr.id);
        }
    }
    work.expected_voter = is_expected_voter(expected, server.id());
    return work;
}

future<> groups_manager::run_config_sync(raft_group_state& state, global_tablet_id tablet,
        raft::group_id gid, config_sync_work work, gate::holder holder) {
    logger.debug("run_config_sync({}-{}): to_add={}, to_del={}", tablet, gid, work.to_add, work.to_del);

    // A single bounded attempt. It intentionally has no retry loop of its own: the
    // component that decides whether a configuration change is still wanted is the
    // topology coordinator, and converge_group_config() is what reports back to it.
    abort_on_expiry aoe(lowres_clock::now() + config_sync_timeout);
    try {
        co_await state.server->modify_config(std::move(work.to_add), std::move(work.to_del),
                &aoe.abort_source());
        logger.debug("run_config_sync({}-{}): applied, current config: {}",
            tablet, gid, state.server->get_configuration().current);
    } catch (const raft::stopped_error&) {
        // The group is being deleted or the node is shutting down.
        logger.debug("run_config_sync({}-{}): raft server stopped", tablet, gid);
    } catch (...) {
        // Deliberately not propagated: nobody is waiting for this attempt to succeed.
        // A configuration that stays behind is reported by converge_group_config(),
        // which knows what the current stage expects and has a deadline to report by.
        logger.info("run_config_sync({}-{}): attempt failed: {}", tablet, gid, std::current_exception());
    }
}

void groups_manager::maybe_schedule_config_sync(raft_group_state& state, global_tablet_id tablet,
        raft::group_id gid, const locator::tablet_map& tmap) {
    if (!state.config_sync.available()) {
        // An attempt is already in flight. Nothing to do: whoever needs the group to
        // converge drains it and reschedules.
        return;
    }
    if (!state.server || !state.gate || state.gate->is_closed()) {
        // The server hasn't finished starting, or the group is being deleted.
        return;
    }
    if (!state.server->is_leader()) {
        // Only the leader can change the configuration. Asking at execution time,
        // rather than deriving it from the replica's role in the migration, is what
        // lets the pending replica drive a rollback while it is still the leader,
        // without it ever trying to drive a change it cannot make.
        return;
    }
    const auto work = assess_config_sync(*state.server, tmap, tablet.tablet);
    if (!work.has_delta() || work.change_in_progress) {
        return;
    }
    state.config_sync = run_config_sync(state, tablet, gid, std::move(work), state.gate->hold());
}

future<> groups_manager::converge_group_config(global_tablet_id tablet, raft::group_id gid,
        const locator::tablet_map& tmap, lowres_clock::time_point deadline) {
    const auto expected_voter = is_expected_voter(
            expected_raft_config(tmap.get_tablet_info(tablet.tablet),
                    tmap.get_tablet_transition_info(tablet.tablet)),
            _raft_gr.get_my_raft_id());

    // Wait for a pending server start before looking at the group. Deliberately done
    // without holding the gate: group deletion runs on the same chain and waits for
    // the gate to drain, so holding it here would deadlock until the deadline.
    {
        const auto it = _raft_groups.find(gid);
        if (it == _raft_groups.end()) {
            if (!expected_voter) {
                co_return;
            }
            throw std::runtime_error(format("converge_group_config({}-{}): raft group is not running", tablet, gid));
        }
        // A replica the current stage doesn't expect to be a voter of has nothing to
        // attest to, and can only help if it happens to be the group's leader right
        // now. Checked before waiting for the start: a replica joining the group
        // bootstraps with an empty configuration and can't learn a leader until the
        // group's leader adds it, so its start doesn't complete before that happens.
        if (!expected_voter && !(it->second.server && it->second.server->is_leader())) {
            co_return;
        }
        co_await it->second.server_control_op.get_future(deadline);
    }

    // The group may have been deleted while we waited, so look it up again and take
    // the gate before touching state.server. There is no scheduling point between
    // the check and hold(), so the group cannot go away underneath us afterwards.
    const auto it = _raft_groups.find(gid);
    if (it == _raft_groups.end() || !it->second.gate || it->second.gate->is_closed() || !it->second.server) {
        throw std::runtime_error(format("converge_group_config({}-{}): raft group is not running", tablet, gid));
    }
    auto& state = it->second;
    auto holder = state.gate->hold();

    abort_on_expiry aoe(deadline);
    auto retry = exponential_backoff_retry(10ms, 1s);

    while (true) {
        // Re-driving the change is coupled to this loop rather than to the next token
        // metadata update, so a barrier retried by the coordinator on an otherwise
        // quiet cluster makes progress.
        maybe_schedule_config_sync(state, tablet, gid, tmap);

        // Drain the in-flight attempt, if any, including one started for an earlier
        // stage, before looking at the configuration: an attempt that has already been
        // proposed must not be able to land after we acknowledged convergence.
        auto drained = co_await coroutine::as_future(state.config_sync.get_future(deadline));
        // The attempt itself never reports an error, so the deadline is the only way
        // this wait can fail.
        const bool timed_out = drained.failed();
        if (timed_out) {
            drained.ignore_ready_future();
        }

        const auto work = assess_config_sync(*state.server, tmap, tablet.tablet);
        if (!timed_out && work.converged()) {
            co_return;
        }

        // Only the leader can repair the configuration. A replica the stage doesn't
        // expect to be a voter has nothing to attest to either, so it is done: a
        // member that raft never told about its own removal simply keeps failing this
        // check until the group is deleted, without holding anything up.
        const bool is_leader = state.server->is_leader();
        if (!is_leader && !work.expected_voter) {
            co_return;
        }

        if (timed_out || lowres_clock::now() >= deadline) {
            throw std::runtime_error(fmt::format("converge_group_config({}-{}): raft configuration "
                    "didn't converge before the deadline: missing to_add={}, to_del={}, "
                    "change in progress={}, current config: {}",
                    tablet, gid, work.to_add, work.to_del, work.change_in_progress,
                    state.server->get_configuration().current));
        }

        if (!is_leader) {
            // Our view of the configuration may just be behind the leader's. A read
            // barrier is a bounded way to catch up, unlike waiting in the background
            // for a notification which raft never sends to a removed member.
            try {
                co_await state.server->read_barrier(&aoe.abort_source());
            } catch (...) {
                logger.debug("converge_group_config({}-{}): read barrier failed: {}",
                    tablet, gid, std::current_exception());
            }
        }

        try {
            co_await retry.retry(aoe.abort_source());
        } catch (...) {
            // The deadline fired; report it on the next pass.
        }
    }
}

future<> groups_manager::local_topology_barrier(token_metadata_ptr tm, lowres_clock::time_point deadline) {
    if (!_features.strongly_consistent_tables) {
        co_return;
    }

    const auto this_replica = locator::tablet_replica {
        .host = tm->get_my_id(),
        .shard = this_shard_id()
    };

    const auto& tablets = tm->tablets();
    for (const auto& [table_id, _]: tablets.all_table_groups()) {
        const auto& tablet_map = tablets.get_tablet_map(table_id);
        if (!tablet_map.has_raft_info()) {
            continue;
        }
        for (const auto& tid: tablet_map.tablet_ids()) {
            if (!tablet_map.has_replica(tid, this_replica)) {
                continue;
            }
            co_await converge_group_config(global_tablet_id{table_id, tid},
                    tablet_map.get_tablet_raft_info(tid).group_id, tablet_map, deadline);
            co_await coroutine::maybe_yield();
        }
    }
}

void groups_manager::update(token_metadata_ptr new_tm) {
    if (!_features.strongly_consistent_tables) {
        return;
    }

    if (!_started) {
        _pending_tm = new_tm;
        return;
    }

    for (auto& [id, state]: _raft_groups) {
        state.has_tablet = false;
    }

    const auto this_replica = locator::tablet_replica {
        .host = new_tm->get_my_id(),
        .shard = this_shard_id()
    };
    _leader_cache.begin_sweep();
    const auto& tablets = new_tm->tablets();
    for (const auto& [table_id, _]: tablets.all_table_groups()) {
        const auto& tablet_map = tablets.get_tablet_map(table_id);
        if (!tablet_map.has_raft_info()) {
            continue;
        }
        for (const auto& tid: tablet_map.tablet_ids()) {
            const auto id = tablet_map.get_tablet_raft_info(tid).group_id;
            const auto tablet = global_tablet_id{table_id, tid};

            _leader_cache.mark_seen(id);
            if (!tablet_map.has_replica(tid, this_replica)) {
                continue;
            }
            auto& state = _raft_groups[id];
            state.has_tablet = true;

            // Don't start the raft server if it is already (started or starting) and not stopping.
            if (state.gate && !state.gate->is_closed()) {
                // Best-effort acceleration: the group's leader gets to apply the new
                // stage's configuration change as soon as the stage is published,
                // instead of waiting for the coordinator's barrier to ask for it.
                maybe_schedule_config_sync(state, tablet, id, tablet_map);
                continue;
            }

            logger.info("update(): starting raft server for tablet {}, group id {}", tablet, id);
            state.gate = make_lw_shared<gate>();
            _starting_groups.push_back(state);
            state.server_control_op = futurize_invoke([&state, this, tablet, id, new_tm](this auto) -> future<> {
                co_await state.server_control_op.get_future();
                co_await start_raft_group(tablet, id, std::move(new_tm));
                state.server = &_raft_gr.get_server(id);
                state.leader_info_updater = leader_info_updater(state, tablet, id);

                // We want to make sure the server is ready to serve requests before
                // we report it as started in wait_for_groups_to_start().
                abort_on_expiry aoe(lowres_clock::now() + std::chrono::seconds(60));
                while (true) {
                    // Use try_hold() rather than hold(): a concurrent
                    // schedule_raft_group_deletion() may have closed the gate
                    // while we were waiting for a leader below. In that case the
                    // group is being deleted, so stop trying to make it ready.
                    auto holder = state.gate->try_hold();
                    if (!holder) {
                        break;
                    }
                    auto srv = raft_server(state, std::move(*holder));
                    auto res = srv.begin_mutate(aoe.abort_source());
                    if (auto w = get_if<raft_server::need_wait_for_leader>(&res)) {
                        auto f = co_await coroutine::as_future(std::move(w->future));
                        if (f.failed()) {
                            logger.warn("update(): waiting for leader timed out for tablet {}, "
                                "group id {}: {}", tablet, id, f.get_exception());
                            break;
                        }
                    } else {
                        break;
                    }
                }

                _starting_groups.erase(_starting_groups.iterator_to(state));

                logger.info("update(): raft server for tablet {} and group id {} is started", tablet, id);
            });
        }
    }

    schedule_raft_groups_deletion(false);
    _leader_cache.end_sweep();
}

future<raft_server> groups_manager::acquire_server(table_id table_id, raft::group_id group_id, abort_source& as) {
    if (!_features.strongly_consistent_tables) {
        on_internal_error(logger, "strongly consistent tables are not enabled on this shard");
    }

    // A concurrent DROP TABLE may have already removed the table from database
    // registries and erased the raft group from _raft_groups via
    // schedule_raft_group_deletion.  The schema.table() in create_operation_ctx()
    // might not fail though in this case because someone might be holding
    // lw_shared_ptr<table>, so that the table is dropped but the table object
    // is still alive.
    //
    // Check that the table still exists. The table is removed from the
    // database (via schema_applier::commit_tables_and_views) BEFORE
    // groups_manager::update() is called (which triggers gate closure via
    // schedule_raft_group_deletion). Since there's no scheduling point
    // between the column_family_exists check and try_hold below, the gate
    // cannot be closed if the table exists.
    //
    // Node shutdown also closes gates (groups_manager::stop() closes every gate
    // regardless of table existence), but it cannot race with us either: the
    // strongly consistent coordinator, the only caller of acquire_server, is
    // destroyed before groups_manager::stop() runs.
    if (!_db.column_family_exists(table_id)) {
        return make_exception_future<raft_server>(
            replica::no_such_column_family(table_id));
    }

    const auto it = _raft_groups.find(group_id);
    if (it == _raft_groups.end()) {
        on_internal_error(logger, format("raft group {} not found", group_id));
    }
    auto& state = it->second;
    auto h = state.gate->try_hold();
    if (!h) {
        on_internal_error(logger, format("acquire_server: gate closed for group {} while table {} exists", group_id, table_id));
    }
    return state.server_control_op.get_future(as).then([&state, h = std::move(*h)] mutable {
        return raft_server(state, std::move(h));
    });
}

void groups_manager::start() {
    _started = true;

    if (!_features.strongly_consistent_tables) {
        return;
    }

    if (_pending_tm) {
        update(std::move(_pending_tm));
    }
}

future<> groups_manager::stop() {
    co_await uninit_messaging_service();

    if (!_started) {
        co_return;
    }

    logger.info("stop() enter");

    schedule_raft_groups_deletion(true);

    while (!_raft_groups.empty()) {
        co_await _raft_groups.begin()->second.server_control_op.get_future();
    }

    logger.info("stop() completed");
}

std::optional<locator::tablet_routing_info_v2> groups_manager::check_tablet_version(
        const replica::table& table,
        const dht::token& token,
        const locator::tablet_version_block block) const
{
    const auto& erm = table.get_effective_replication_map();
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(table.schema()->id());
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& raft_info = tablet_map.get_tablet_raft_info(tablet_id);
    const auto group_id = raft_info.group_id;

    auto group_it = _raft_groups.find(group_id);
    if (group_it == _raft_groups.end()) [[unlikely]] {
        return std::nullopt;
    }

    const raft_group_state& state = group_it->second;
    if (!state.server) [[unlikely]] {
        // We don't know who the leader is, so we cannot compute routing information.
        return std::nullopt;
    }

    const raft::server_id group_leader = state.server->current_leader();
    if (group_leader == raft::server_id{}) [[unlikely]] {
        // The leader hasn't been elected yet. We cannot compute the tablet version.
        return std::nullopt;
    }

    const auto& tablet_info = tablet_map.get_tablet_info(tablet_id);
    auto maybe_replicas = prepare_replicas_for_sc_tablet_version(tablet_info.replicas, group_leader);

    if (!maybe_replicas) [[unlikely]] {
        // The leader is not present in the replica set.
        return std::nullopt;
    }

    const auto hash = locator::internal::hash_replica_list(*maybe_replicas);

    if (locator::compare_tablet_version_block(hash, block)) [[likely]] {
        return std::nullopt;
    }

    const dht::token first_token = (tablet_id == tablet_map.first_tablet())
            ? dht::minimum_token()
            : tablet_map.get_last_token(locator::tablet_id(size_t(tablet_id) - 1));
    const dht::token last_token = tablet_map.get_last_token(tablet_id);

    return locator::tablet_routing_info_v2 {
        .tablet_replicas = std::move(*maybe_replicas),
        .token_range = std::make_pair(first_token, last_token),
        .hash = hash
    };
}

}
