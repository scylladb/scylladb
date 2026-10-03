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
#include "utils/on_internal_error.hh"
#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/as_future.hh>
#include "utils/chain_abort_source.hh"
#include "utils/exponential_backoff_retry.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/util/defer.hh>

namespace service::strong_consistency {

using namespace locator;

static logging::logger logger("sc_groups_manager");

// How long the tablet cleanup waits for the raft server of a group this node left to
// be torn down. Purely local: the deletion was scheduled when the stage that ended the
// membership was published, which the cleanup has already synchronized with, and no
// request holds the server by then, so all that's left is stopping it.
static constexpr auto group_teardown_timeout = std::chrono::seconds(60);

static raft::server_id to_server_id(host_id host_id) {
    return raft::server_id{host_id.uuid()};
};

// Tests may lengthen the tick interval via error injection so that any
// unwanted waiting on a raft tick becomes visible as a large delay.
static raft_ticker_type::duration get_tick_interval() {
    return utils::get_local_injector()
            .inject_parameter<int64_t>("strongly-consistent-raft-group-tick-interval-in-ms")
            .transform([](int64_t ms) { return raft_ticker_type::duration{std::chrono::milliseconds{ms}}; })
            .value_or(raft_tick_interval);
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

// Test-only: emulate a named leader outside the serving set, as when a replica a tablet
// migration demoted leads for one commit. The real window is too short to hit.
static std::optional<raft::server_id> injected_stale_leader() {
    if (utils::get_local_injector().enter("sc_report_stale_leader")) {
        return raft::server_id{utils::UUID(0, 1)};
    }
    return std::nullopt;
}

auto raft_server::begin_mutate(abort_source& as) -> begin_mutate_result {
    if (const auto stale_leader = injected_stale_leader()) {
        return raft::not_a_leader{*stale_leader};
    }
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
    if (const auto stale_leader = injected_stale_leader()) {
        return raft::not_a_leader{*stale_leader};
    }
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


    co_await utils::get_local_injector().inject("sc_start_raft_group_pause",
            utils::wait_for_message(std::chrono::minutes(1)));

    auto* commitlog = _db.commitlog();
    SCYLLA_ASSERT(commitlog);

    // A group this shard persists nothing about has no history here, so any commitlog
    // entries replay left for it belong to an earlier membership: the tablet was migrated
    // away, its raft state erased by the cleanup, and migrated back before the old segments
    // were recycled. Handing those to the group would give it a log from an incarnation it
    // knows nothing about, indices and terms included. Taken out of the buffer either way,
    // so that its bookkeeping stays right.
    const auto persisted = co_await raft_groups_storage::load_commit_idx_if_persisted(_qp, group_id, this_shard_id());
    auto replayed_data = _raft_replay_buffer.take_replayed_group_entries(group_id);
    if (!persisted && !replayed_data.entries.empty()) {
        logger.warn("start_raft_group(): tablet {}, group id {}: no persisted raft state, discarding {} "
                "replayed commitlog entries left over from an earlier membership",
                tablet, group_id, replayed_data.entries.size());
        replayed_data = replayed_data_per_group{};
    }

    auto storage = std::make_unique<raft_groups_storage>(_qp, group_id, my_id, this_shard_id(),
        *commitlog, tablet.table, std::move(replayed_data));

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

    co_await _raft_gr.start_server_for_group(raft_server_for_group {
        .gid = group_id,
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
        .persistence = persistence_ref,
        .state_machine = state_machine_ref
    }, get_tick_interval());
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

    chain_control_op(state, id, [this, &state, id, g = state.gate, gate_fut = std::move(gate_fut)] () mutable -> future<> {
        logger.debug("schedule_raft_group_deletion(): group id {}: starting", id);

        co_await _raft_gr.abort_server(id);
        logger.debug("schedule_raft_group_deletion(): group id {}: server aborted", id);

        co_await utils::get_local_injector().inject("sc_raft_group_deletion_pause",
                utils::wait_for_message(std::chrono::minutes(1)));

        co_await std::move(gate_fut);
        logger.debug("schedule_raft_group_deletion(): group id {}: gate closed", id);

        co_await std::move(state.leader_info_updater);

        _raft_gr.destroy_server(id);
        state.server = nullptr;
        logger.info("schedule_raft_group_deletion(): raft server for group id {} is destroyed", id);

        // We need to erase the raft group state only if we are still the last operation on it.
        // If another start arrived while we were stopping the raft server, a new gate
        // would have been assigned, and we should leave the state in the map.
        if (state.gate.get() == g.get() && _raft_groups.erase(id) != 1) {
            on_internal_error(logger, format("raft group {} is already deleted", id));
        }
    });
}

void groups_manager::chain_control_op(raft_group_state& state, raft::group_id id,
        noncopyable_function<future<>()> op, std::source_location loc) {
    state.server_control_op = futurize_invoke([&state, id, op = std::move(op), loc](this auto) -> future<> {
        co_await state.server_control_op.get_future();
        auto f = co_await coroutine::as_future(futurize_invoke(op));
        if (f.failed()) {
            utils::on_fatal_internal_error(format("{}({}:{}) `{}`: raft group {}: control operation failed: {:t}",
                    loc.file_name(), loc.line(), loc.column(), loc.function_name(), id, f.get_exception()));
        }
    });
}

std::optional<raft_server> groups_manager::try_acquire_server(raft_group_state& state) {
    // No preemption between the checks, see raft_group_state.
    auto h = state.gate->try_hold();
    if (!h || !state.server_control_op.available()) {
        return std::nullopt;
    }
    SCYLLA_ASSERT(state.server);
    return raft_server(state, std::move(*h));
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

future<> groups_manager::cleanup_group(global_tablet_id tablet, raft::group_id group_id) {
    if (const auto it = _raft_groups.find(group_id); it != _raft_groups.end()) {
        if (it->second.gate && !it->second.gate->is_closed()) {
            on_internal_error(logger, format("cleanup_group({}-{}): the raft group is running and not being deleted",
                    tablet, group_id));
        }
        logger.debug("cleanup_group({}-{}): waiting for the raft server to be torn down", tablet, group_id);
        auto drained = co_await coroutine::as_future(it->second.server_control_op.get_future(
                lowres_clock::now() + group_teardown_timeout));
        if (drained.failed()) {
            drained.ignore_ready_future();
            co_await coroutine::return_exception(std::runtime_error(format(
                    "cleanup_group({}-{}): the raft server was not torn down before the deadline", tablet, group_id)));
        }
    }
    co_await raft_groups_storage::erase_persisted_state(_qp, group_id, this_shard_id());
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
        [this] (rpc::opt_time_point timeout, raft::server_id dst_id, locator::global_tablet_id tablet,
                raft::group_id group_id, unsigned shard, service::session_id session, sstring stage) {
            return handle_migration_rpc("wait_for_snapshot_transfer", timeout, dst_id, tablet, group_id, shard, session,
                    std::move(stage), &groups_manager::wait_for_snapshot_transfer);
        }
    );
    ser::groups_manager_rpc_verbs::register_sync_raft_group_config(&_ms,
        [this] (rpc::opt_time_point timeout, raft::server_id dst_id, locator::global_tablet_id tablet,
                raft::group_id group_id, unsigned shard, service::session_id session, sstring stage) {
            return handle_migration_rpc("sync_raft_group_config", timeout, dst_id, tablet, group_id, shard, session,
                    std::move(stage), &groups_manager::sync_raft_group_config);
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

// The raft group configuration implied by a tablet's replica set and the stage its
// migration is in; see "Strongly-consistent tablets" in docs/dev/topology-over-raft.md.
//
// A replica leaves the group in two changes: it is demoted to a non-voter first, and
// removed by a later stage, once no request can still be inside its raft server with
// a view that lets it serve. The pending replica joins as a non-voter and is promoted
// once it has caught up.
static raft::config_member_set expected_raft_config(
        const locator::tablet_info& tinfo,
        const locator::tablet_transition_info* trinfo) {
    // The non-voter is inserted after the voters, so that an intra-node migration, where
    // the leaving and the pending replica share a host, doesn't demote that host.
    const auto config = [] (const locator::tablet_replica_set& voters,
            const std::optional<locator::tablet_replica>& non_voter = std::nullopt) {
        raft::config_member_set members;
        members.reserve(voters.size() + 1);
        for (const auto& r : voters) {
            members.emplace(raft::server_address{to_server_id(r.host), {}}, raft::is_voter::yes);
        }
        if (non_voter) {
            members.emplace(raft::server_address{to_server_id(non_voter->host), {}}, raft::is_voter::no);
        }
        return members;
    };

    if (!trinfo) {
        return config(tinfo.replicas);
    }

    switch (trinfo->stage) {
        case tablet_transition_stage::start_migration:
        case tablet_transition_stage::sc_remove_pending:
        case tablet_transition_stage::cleanup_target:
        case tablet_transition_stage::revert_migration:
            return config(tinfo.replicas);

        case tablet_transition_stage::sc_add_nonvoter:
        case tablet_transition_stage::sc_snapshot_transfer:
        case tablet_transition_stage::sc_rollback:
            return config(tinfo.replicas, trinfo->pending_replica);

        case tablet_transition_stage::sc_become_voter:
            return config(trinfo->next, get_leaving_replica(tinfo, *trinfo));

        case tablet_transition_stage::use_new:
        case tablet_transition_stage::cleanup:
        case tablet_transition_stage::end_migration:
            return config(trinfo->next);

        case tablet_transition_stage::write_both_read_old_fallback_cleanup:
        case tablet_transition_stage::rebuild_repair:
        case tablet_transition_stage::repair:
        case tablet_transition_stage::end_repair:
        case tablet_transition_stage::restore:
            break;
    }
    on_internal_error(logger, format("expected_raft_config: unexpected transition stage {} of a strongly "
            "consistent tablet", trinfo->stage));
}

// Should this node host a raft server for the tablet's group at the tablet's current
// migration stage?
//
// A replica hosts the group while it is a member of any configuration the group may
// have while a view of the stage is live. The pending replica also hosts from
// start_migration on, so that it can be added.
//
// Hosting ends only with the publish of a stage: raft never tells a removed server
// about its removal, and after a restart hosting is derived from the stage again. The
// stage that ends it is published after two things: a drain of every request that
// could still be inside the replica's server with a view that lets it serve, and the
// sync that removed the replica from the configuration. For the leaving replica that
// is cleanup, after use_new drained and removed it. For the pending one it is
// cleanup_target, after sc_remove_pending did. So no request holds the server when it
// is torn down, and the leader never replicates into a server that is gone.
static bool hosts_raft_group(const locator::tablet_info& tinfo,
        const locator::tablet_transition_info* trinfo,
        const locator::tablet_replica& replica) {
    if (!trinfo) {
        return locator::contains(tinfo.replicas, replica);
    }

    const auto is_pending = trinfo->pending_replica == replica;

    switch (trinfo->stage) {
        case tablet_transition_stage::start_migration:
        case tablet_transition_stage::sc_add_nonvoter:
        case tablet_transition_stage::sc_snapshot_transfer:
        case tablet_transition_stage::sc_become_voter:
        case tablet_transition_stage::use_new:
        case tablet_transition_stage::sc_rollback:
        case tablet_transition_stage::sc_remove_pending:
            return locator::contains(tinfo.replicas, replica) || is_pending;

        case tablet_transition_stage::cleanup:
        case tablet_transition_stage::end_migration:
            return locator::contains(trinfo->next, replica);

        case tablet_transition_stage::cleanup_target:
        case tablet_transition_stage::revert_migration:
            return locator::contains(tinfo.replicas, replica);

        case tablet_transition_stage::write_both_read_old_fallback_cleanup:
        case tablet_transition_stage::rebuild_repair:
        case tablet_transition_stage::repair:
        case tablet_transition_stage::end_repair:
        case tablet_transition_stage::restore:
            return locator::contains(tinfo.replicas, replica);
    }
    on_internal_error(logger, format("hosts_raft_group: unknown tablet transition stage {}",
            static_cast<int>(trinfo->stage)));
}

// What modify_config() has to be given to turn `current` into `expected`. A member whose
// voting status differs is added again, with the expected status.
struct config_delta {
    std::vector<raft::config_member> to_add;
    std::vector<raft::server_id> to_del;
};

static config_delta diff_config(const raft::config_member_set& expected, const raft::config_member_set& current) {
    config_delta delta;
    for (const auto& member : expected) {
        const auto it = current.find(member.addr.id);
        if (it == current.end() || it->can_vote != member.can_vote) {
            delta.to_add.push_back(member);
        }
    }
    for (const auto& member : current) {
        if (!expected.contains(member.addr.id)) {
            delta.to_del.push_back(member.addr.id);
        }
    }
    return delta;
}

future<> groups_manager::handle_migration_rpc(const char* verb, rpc::opt_time_point timeout, raft::server_id dst_id,
        global_tablet_id tablet, raft::group_id group_id, unsigned shard, service::session_id session,
        sstring stage_name, migration_rpc_method method) {
    if (_raft_gr.get_my_raft_id() != dst_id) {
        throw raft_destination_id_not_correct{_raft_gr.get_my_raft_id(), dst_id};
    }
    // The coordinator owns the budget for one attempt.
    if (!timeout) {
        on_internal_error(logger, format("{}({}-{}): no timeout", verb, tablet, group_id));
    }
    if (shard >= this_smp_shard_count()) {
        throw std::runtime_error(format("{}({}-{}): shard {} out of range", verb, tablet, group_id, shard));
    }
    const auto deadline = *timeout;
    const auto stage = locator::tablet_transition_stage_from_string(stage_name);

    // This node may not have applied the stage the RPC was sent for yet: a strongly
    // consistent migration runs no barrier at some of its stages. Catching up keeps the
    // target shard from refusing an RPC that is current.
    co_await _mm.get_group0_barrier().trigger();

    // The explicit object parameter copies the captures into the coroutine frame.
    co_await container().invoke_on(shard, [verb, tablet, group_id, session, stage, deadline, method]
            (this auto, groups_manager& gm) -> future<> {
        locator::tablet_metadata_guard guard(gm._db.find_column_family(tablet.table), tablet);

        // An RPC that arrives late was sent for an earlier stage, or for an earlier
        // migration of the tablet, which the session identifies: a strongly consistent
        // migration keeps one session from the stage after start_migration to its end.
        // Acting on the stage found here instead could run a sync's change before that
        // stage's barrier drained the requests the change puts at risk, or reach for a
        // raft server this replica no longer hosts.
        {
            const auto& tmap = guard.get_tablet_map();
            const auto* trinfo = tmap.get_tablet_transition_info(tablet.tablet);
            if (!trinfo || trinfo->session_id != session || trinfo->stage != stage) {
                const auto msg = fmt::format("{}({}-{}): sent for stage {} in session {}, but the tablet is {}",
                        verb, tablet, group_id, stage, session,
                        trinfo ? fmt::format("at stage {} in session {}", trinfo->stage, trinfo->session_id)
                               : std::string("not in transition"));
                logger.debug("{}", msg);
                throw std::runtime_error(msg);
            }

            const auto this_replica = locator::tablet_replica {
                .host = guard.get_token_metadata()->get_my_id(),
                .shard = this_shard_id()
            };
            if (!hosts_raft_group(tmap.get_tablet_info(tablet.tablet), trinfo, this_replica)) {
                // The coordinator sends these RPCs only to replicas that host the group at
                // the stage.
                on_internal_error(logger, format("{}({}-{}): replica {} doesn't host the group at stage {}",
                        verb, tablet, group_id, this_replica, trinfo->stage));
            }
        }

        // Ends the call at the coordinator's deadline or when the tablet's stage moves on,
        // whichever comes first. The raft calls of `method` are aborted by it too.
        abort_on_expiry aoe(deadline);
        auto sub = utils::chain_abort_source(aoe.abort_source(), guard.get_abort_source());
        const auto server = co_await gm.acquire_server(tablet.table, group_id, aoe.abort_source());

        // Supersedes the migration RPC still running on the group, if any.
        auto& state = server._state;
        if (state.migration_rpc_as) {
            state.migration_rpc_as->request_abort();
        }
        const auto rpc_as = make_lw_shared<abort_source>();
        state.migration_rpc_as = rpc_as;
        auto rpc_sub = utils::chain_abort_source(aoe.abort_source(), *rpc_as);
        // Runs while `server` still holds the group's gate, so `state` is alive.
        const auto uninstall = defer([&state, rpc_as] () noexcept {
            if (state.migration_rpc_as == rpc_as) {
                state.migration_rpc_as = nullptr;
            }
        });

        co_await (gm.*method)(tablet, group_id, server, guard, aoe.abort_source());
    });
}

future<> groups_manager::sync_raft_group_config(global_tablet_id tablet, raft::group_id gid, const raft_server& server,
        locator::tablet_metadata_guard& guard, abort_source& as) {
    // Taken from the guard's tablet map, which a later suspension may replace. Right for
    // the whole call: a transition's replica sets don't change, and a change of stage
    // aborts the call.
    const auto expected_config = std::invoke([&] {
        const auto& tmap = guard.get_tablet_map();
        return expected_raft_config(tmap.get_tablet_info(tablet.tablet), tmap.get_tablet_transition_info(tablet.tablet));
    });

    auto retry = exponential_backoff_retry(10ms, 1s);
    while (true) {
        const auto config = server.server().get_configuration();
        auto [to_add, to_del] = diff_config(expected_config, config.current);
        // In a joint configuration the delta against C_new is already empty, but the
        // change hasn't finished yet.
        if (to_add.empty() && to_del.empty() && !config.is_joint()) {
            break;
        }

        // Checked right before a change is proposed, with no suspension in between, as
        // modify_config() appends its entry before it first suspends: once the stage has
        // moved on on this host, this call proposes nothing more.
        if (as.abort_requested()) {
            const auto msg = fmt::format("sync_raft_group_config({}-{}): raft configuration didn't converge before "
                    "the deadline, the stage moved on, or a newer call superseded this one: missing to_add={}, "
                    "to_del={}, current config: {}",
                    tablet, gid, to_add, to_del, config);
            logger.debug("{}", msg);
            throw std::runtime_error(msg);
        }

        // Only the errors below have known causes that resolve on their own, so only
        // they are retried here. Anything else goes to the coordinator.
        try {
            if (server.server().is_leader()) {
                // A joint configuration has to resolve before another change is proposed.
                if (!config.is_joint()) {
                    if (utils::get_local_injector().enter("sc_config_sync_fail")) {
                        throw std::runtime_error("sc_config_sync_fail injection");
                    }
                    co_await server.server().modify_config(std::move(to_add), std::move(to_del), &as);
                    continue;
                }
            }
            // A follower waits for the leader's change to reach it, and checks again after
            // the backoff below. A configuration takes effect once its entry is appended,
            // so a read barrier isn't needed, and it would also wait for the state machine
            // to apply the log, however far behind that is.
        } catch (const raft::not_a_leader& e) {
            // The leadership moved while the change was in flight.
            logger.debug("sync_raft_group_config({}-{}): {:t}, retrying", tablet, gid, e);
        } catch (const raft::commit_status_unknown& e) {
            // The leadership moved, and the change may or may not have landed; the next
            // pass sees which.
            logger.debug("sync_raft_group_config({}-{}): {:t}, retrying", tablet, gid, e);
        } catch (const raft::conf_change_in_progress& e) {
            // Another change is still in flight on this server: a duplicate of this sync,
            // or an earlier change whose final entry isn't committed yet.
            logger.debug("sync_raft_group_config({}-{}): {:t}, retrying", tablet, gid, e);
        } catch (const raft::request_aborted&) {
            // The deadline passed or the stage moved on; the next pass reports it.
        }

        try {
            co_await retry.retry(as);
        } catch (...) {
            // Aborted; reported on the next pass.
        }
    }

    const auto is_expected_voter = [&expected_config] (raft::server_id id) {
        const auto it = expected_config.find(id);
        return it != expected_config.end() && it->can_vote == raft::is_voter::yes;
    };
    if (is_expected_voter(_raft_gr.get_my_raft_id())) {
        // After a demotion - of the leaving replica at sc_become_voter, of the pending
        // one at sc_rollback - this replica may still name the demoted leader. Waiting
        // for a voter here, before the coordinator publishes the next stage, keeps the
        // requests of that stage from finding a named leader outside their serving set.
        // Only the expected voters serve at the next stage, so only they wait.
        //
        // One wait is not enough: raft hands leadership over only to a voter that holds
        // the whole log, so until one does, the demoted leader keeps leading, and each of
        // its messages - same term - makes this replica name it again. Every pass ends on
        // a leader's message, and the loop ends once a voter wins an election: its higher
        // term makes this replica reject the demoted leader's messages.
        for (auto leader = server.server().current_leader(); !leader || !is_expected_voter(leader);
                leader = server.server().current_leader()) {
            logger.debug("sync_raft_group_config({}-{}): named leader {} is not a voter of {}, waiting",
                    tablet, gid, leader, expected_config);
            co_await server.server().wait_for_leader(&as, bool(leader));
        }
    }

    // Test-only: hold the migration at this stage once this replica has converged.
    co_await utils::get_local_injector().inject("sc_pause_after_config_sync", utils::wait_for_message(5min));
}

future<> groups_manager::wait_for_snapshot_transfer(global_tablet_id tablet, raft::group_id gid, const raft_server& server,
        locator::tablet_metadata_guard& guard, abort_source& as) {
    co_await utils::get_local_injector().inject("sc_wait_for_snapshot_transfer", utils::wait_for_message(20min));
    co_await server.server().read_barrier(&as);
}

void groups_manager::schedule_raft_group_start(global_tablet_id tablet, raft::group_id id, raft_group_state& state,
        token_metadata_ptr tm) {
    logger.info("update(): starting raft server for tablet {}, group id {}", tablet, id);
    state.gate = make_lw_shared<gate>();
    // Still linked if the previous start hasn't finished yet.
    if (!state.is_linked()) {
        _starting_groups.push_back(state);
    }
    chain_control_op(state, id, [this, &state, tablet, id, tm = std::move(tm), g = state.gate] () mutable -> future<> {
        co_await start_raft_group(tablet, id, std::move(tm));
        state.server = &_raft_gr.get_server(id);
        state.leader_info_updater = leader_info_updater(state, tablet, id);
        co_await wait_for_first_leader(tablet, id, state, *g);

        // If a restart is already queued behind us, the group isn't started yet; that
        // start will unlink the state.
        if (state.gate.get() == g.get()) {
            _starting_groups.erase(_starting_groups.iterator_to(state));
        }
        logger.info("update(): raft server for tablet {} and group id {} is started", tablet, id);
    });
}

future<> groups_manager::wait_for_first_leader(global_tablet_id tablet, raft::group_id id, raft_group_state& state,
        gate& g) {
    // Up to a minute, and not beyond a deletion queued for this incarnation: `g` is its
    // gate, while state.gate may already be a later incarnation's open gate. The server
    // can't be destroyed meanwhile: a deletion runs only after the start that calls this.
    abort_on_expiry aoe(lowres_clock::now() + std::chrono::seconds(60));
    while (auto holder = g.try_hold()) {
        auto srv = raft_server(state, std::move(*holder));
        auto res = srv.begin_mutate(aoe.abort_source());
        auto* w = get_if<raft_server::need_wait_for_leader>(&res);
        if (!w) {
            co_return;
        }
        auto f = co_await coroutine::as_future(std::move(w->future));
        if (f.failed()) {
            logger.warn("update(): waiting for leader timed out for tablet {}, group id {}: {}",
                    tablet, id, f.get_exception());
            co_return;
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

    const auto& tablets = new_tm->tablets();

    _leader_cache.begin_sweep();
    for (const auto& [table_id, _]: tablets.all_table_groups()) {
        const auto& tablet_map = tablets.get_tablet_map(table_id);
        if (!tablet_map.has_raft_info()) {
            continue;
        }
        for (const auto& tid: tablet_map.tablet_ids()) {
            const auto id = tablet_map.get_tablet_raft_info(tid).group_id;
            const auto tablet = global_tablet_id{table_id, tid};

            _leader_cache.mark_seen(id);

            if (!hosts_raft_group(tablet_map.get_tablet_info(tid), tablet_map.get_tablet_transition_info(tid), this_replica)) {
                // Either the tablet has no replica on this node, or a migration has
                // ended this node's membership in the group. Leaving has_tablet false
                // schedules the deletion of the raft server below.
                continue;
            }

            auto& state = _raft_groups[id];
            state.has_tablet = true;
            // Start the server, unless it is started or starting already and not being
            // deleted.
            if (!state.gate || state.gate->is_closed()) {
                schedule_raft_group_start(tablet, id, state, new_tm);
            }
        }
    }
    _leader_cache.end_sweep();

    schedule_raft_groups_deletion(false);
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
    // strongly consistent coordinator is destroyed before groups_manager::stop()
    // runs, and the RPC handlers that call this are unregistered on every shard
    // before it; see uninit_messaging_service().
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
    // Holder and future are taken atomically, so the future completes with the
    // start that created this gate, whose server the holder protects.
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

future<> groups_manager::stepdown_leaders() {
    if (!_started || !_features.strongly_consistent_tables) {
        co_return;
    }
    if (_raft_groups.size() == 0) {
        logger.debug("stepdown_leaders(): no tablet raft groups on this node");
        co_return;
    }

    const auto stepdown_timeout_ticks = std::max<int64_t>(std::chrono::seconds(5) / get_tick_interval(), 1);
    size_t transferred = 0;

    logger.info("stepdown_leaders(): transferring leadership away from this node, {} raft group(s) to consider",
        _raft_groups.size());

    co_await coroutine::parallel_for_each(_raft_groups, [&] (auto& entry) -> future<> {
        auto& [gid, state] = entry;

        // The handle also pins the entry: its erase waits for the gate.
        const auto srv = try_acquire_server(state);
        if (!srv || !srv->server().is_leader()) {
            co_return;
        }

        try {
            co_await srv->server().stepdown(raft::logical_clock::duration(stepdown_timeout_ticks));
            ++transferred;
            logger.debug("stepdown_leaders(): group id {}: leadership transferred", gid);
        } catch (...) {
            logger.info("stepdown_leaders(): group id {}: failed to transfer leadership: {}",
                gid, std::current_exception());
        }
    });

    logger.info("stepdown_leaders(): transferred leadership for {} raft group(s)",
        transferred);
}

future<> groups_manager::stop() {
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

}
