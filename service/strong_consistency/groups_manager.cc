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
#include "utils/error_injection.hh"
#include "idl/strong_consistency/groups_manager.dist.hh"
#include "utils/error_injection.hh"
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <seastar/core/abort_source.hh>
#include <seastar/core/sleep.hh>

namespace service::strong_consistency {

using namespace locator;

static logging::logger logger("sc_groups_manager");

// How often the leader refreshes stable_timestamp for the groups it leads.
static constexpr auto stable_timestamp_refresh_interval = std::chrono::seconds(60);

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
    , _stable_timestamp_tracker(_db, _ms, _qp, _raft_groups)
{
    init_messaging_service();
}

future<> groups_manager::start_raft_group(global_tablet_id tablet,
        raft::group_id group_id,
        token_metadata_ptr tm)
{
    const auto my_id = to_server_id(tm->get_my_id());


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

        raft::configuration configuration;
        configuration.current.reserve(tablet_info.replicas.size());
        for (const auto& r: tablet_info.replicas) {
            configuration.current.emplace(raft::server_address{to_server_id(r.host), {}},
                raft::is_voter::yes);
        }
        co_await storage->bootstrap(std::move(configuration), false);
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

using applied_timestamp_per_tablet = std::unordered_map<global_tablet_id, api::timestamp_type>;

static future<applied_timestamp_per_tablet> get_applied_timestamps(const replica::database& db, const std::vector<global_tablet_id>& tablets) {
    applied_timestamp_per_tablet res;
    res.reserve(tablets.size());
    for (const auto& tablet : tablets) {
        auto table = db.get_tables_metadata().get_table_if_exists(tablet.table);
        res[tablet] = table ? table->get_max_timestamp_for_tablet(tablet.tablet) : api::min_timestamp;
        co_await coroutine::maybe_yield();
    }
    co_return res;
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
    ser::groups_manager_rpc_verbs::register_get_local_applied_timestamps(&_ms,
        [this] (raft::server_id dst_id, service::fencing_token fence, std::vector<global_tablet_id> tablets) -> future<applied_timestamp_per_tablet> {
            if (_raft_gr.get_my_raft_id() != dst_id) {
                throw raft_destination_id_not_correct{_raft_gr.get_my_raft_id(), dst_id};
            }
            const auto tm = _db.get_token_metadata_ptr();
            const auto my_host = tm->get_my_id();
            if (auto stale = _qp.proxy().check_fence(fence, my_host); stale) {
                throw std::move(*stale);
            }

            applied_timestamp_per_tablet result;
            std::unordered_map<shard_id, std::vector<global_tablet_id>> by_shard;
            for (const auto& tablet : tablets) {
                co_await coroutine::maybe_yield();
                if (!tm->tablets().has_tablet_map(tablet.table)) {
                    result[tablet] = api::min_timestamp;
                    continue;
                }
                auto& tmap = tm->tablets().get_tablet_map(tablet.table);
                if (size_t(tablet.tablet) >= tmap.tablet_count()) {
                    result[tablet] = api::min_timestamp;
                    continue;
                }
                auto& info = tmap.get_tablet_info(tablet.tablet);
                if (auto replica = info.maybe_find_replica(my_host); replica) {
                    by_shard[replica->shard].push_back(tablet);
                } else {
                    result[tablet] = api::min_timestamp;
                }
            }

            co_await coroutine::parallel_for_each(by_shard, [&] (this auto, const auto& entry) -> future<> {
                const auto& [shard, local_tablets] = entry;
                auto timestamps = co_await container().invoke_on(shard, [&local_tablets] (groups_manager& gm) -> future<foreign_ptr<std::unique_ptr<applied_timestamp_per_tablet>>> {
                    co_return make_foreign(std::make_unique<applied_timestamp_per_tablet>(co_await get_applied_timestamps(gm._db, local_tablets)));
                });
                for (const auto& [tablet, ts] : *timestamps) {
                    result[tablet] = ts;
                    co_await coroutine::maybe_yield();
                }
            });
            co_return result;
        }
    );
    ser::groups_manager_rpc_verbs::register_set_stable_timestamp(&_ms,
        [this] (raft::server_id dst_id, service::fencing_token fence, applied_timestamp_per_tablet stable_timestamps) -> future<> {
            if (_raft_gr.get_my_raft_id() != dst_id) {
                throw raft_destination_id_not_correct{_raft_gr.get_my_raft_id(), dst_id};
            }
            const auto tm = _db.get_token_metadata_ptr();
            const auto my_host = tm->get_my_id();
            if (auto stale = _qp.proxy().check_fence(fence, my_host); stale) {
                throw std::move(*stale);
            }

            // Group the tablets by the shard that hosts their local replica, so we
            // hop to each shard at most once.
            std::unordered_map<shard_id, applied_timestamp_per_tablet> by_shard;
            for (const auto& [tablet, ts] : stable_timestamps) {
                co_await coroutine::maybe_yield();
                if (!tm->tablets().has_tablet_map(tablet.table)) {
                    continue;
                }
                auto& tmap = tm->tablets().get_tablet_map(tablet.table);
                if (size_t(tablet.tablet) >= tmap.tablet_count()) {
                    continue;
                }
                auto& info = tmap.get_tablet_info(tablet.tablet);
                if (auto replica = info.maybe_find_replica(my_host); replica) {
                    by_shard[replica->shard][tablet] = ts;
                }
            }

            co_await coroutine::parallel_for_each(by_shard, [&] (const auto& entry) -> future<> {
                const auto& [shard, local_timestamps] = entry;
                co_await container().invoke_on(shard, [&local_timestamps] (this auto, groups_manager& gm) -> future<> {
                    auto& tablets = gm._db.get_token_metadata().tablets();
                    for (const auto& [tablet, ts] : local_timestamps) {
                        co_await coroutine::maybe_yield();
                        auto group_id = tablets.get_tablet_map(tablet.table).get_tablet_raft_info(tablet.tablet).group_id;
                        auto git = gm._raft_groups.find(group_id);
                        if (git == gm._raft_groups.end()) {
                            continue; // Group no longer hosted here; drop the update.
                        }
                        co_await gm._stable_timestamp_tracker.advance(git->second, group_id, ts);
                    }
                });
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

future<> groups_manager::stable_timestamp_tracker::run() {
    while (!_as.abort_requested()) {
        auto interval = std::chrono::duration_cast<std::chrono::milliseconds>(stable_timestamp_refresh_interval);
        if (utils::get_local_injector().is_enabled("sc_short_stable_timestamp_refresh")) {
            interval = std::chrono::milliseconds(200);
        }
        try {
            co_await seastar::sleep_abortable(interval, _as);
        } catch (const seastar::sleep_aborted&) {
            co_return;
        }
        try {
            co_await refresh();
        } catch (...) {
            logger.warn("stable_timestamp_tracker::run: unexpected error: {}", std::current_exception());
        }
    }
}

groups_manager::stable_timestamp_tracker::stable_timestamp_tracker(replica::database& db,
        netw::messaging_service& ms, cql3::query_processor& qp,
        std::unordered_map<raft::group_id, raft_group_state>& raft_groups)
    : _db(db)
    , _ms(ms)
    , _qp(qp)
    , _raft_groups(raft_groups)
{}

void groups_manager::stable_timestamp_tracker::start() {
    // Idempotent: update() calls this on every token_metadata update because the
    // strongly_consistent_tables feature may only become enabled after
    // groups_manager::start() has run, and the fiber must be launched then.
    if (_fiber.available()) {
        _fiber = run();
    }
}

future<> groups_manager::stable_timestamp_tracker::stop() {
    _as.request_abort();
    co_await std::move(_fiber);
}

future<> groups_manager::stable_timestamp_tracker::advance(raft_group_state& state, raft::group_id group_id, api::timestamp_type candidate) {
    if (candidate <= state.stable_timestamp) {
        co_return;
    }
    state.stable_timestamp = candidate;
    // Persist the advanced value so it survives restarts and never regresses.
    co_await raft_groups_storage::store_stable_timestamp(_qp, group_id, this_shard_id(), state.stable_timestamp);
}

future<> groups_manager::stable_timestamp_tracker::refresh() {
    auto tm = _db.get_token_metadata_ptr();
    const auto my_host = _db.get_token_metadata().get_my_id();
    // Our topology version, sent with the RPCs below so a receiver can fence out
    // this request if we turn out to be lagging behind the current topology.
    const auto fence = service::fencing_token{tm->get_version()};

    std::unordered_map<host_id, std::vector<global_tablet_id>> host_tablets;
    auto sc_tables = tm->tablets().all_tables_ungrouped() | std::views::keys | std::views::filter([&tm] (auto table) { return tm->tablets().get_tablet_map(table).has_raft_info(); }) | std::ranges::to<std::vector<table_id>>();
    for (const auto& table : sc_tables) {
        _as.check();
        const auto& tmap = tm->tablets().get_tablet_map(table);
        co_await tmap.for_each_tablet([&, my_host = my_host](tablet_id tid, const tablet_info& ti) -> future<> {
            auto group_id = tmap.get_tablet_raft_info(tid).group_id;
            auto git = _raft_groups.find(group_id);
            if (git == _raft_groups.end()) {
                return make_ready_future<>(); // Skip stable_timestamp refresh.
            }

            auto& state = git->second;
            if (!state.leader_info) {
                return make_ready_future<>();  // Skip stable_timestamp refresh.
            }

            if (contains(ti.replicas, {my_host, this_shard_id()})) {
                for (const auto& replica : ti.replicas) {
                    host_tablets[replica.host].push_back(global_tablet_id{table, tid});
                }
            }
            return make_ready_future<>();
        });
    }

    std::unordered_map<host_id, applied_timestamp_per_tablet> host_timestamps;
    _as.check();
    co_await coroutine::parallel_for_each(host_tablets, [&](const auto& entry) -> future<> {
        auto& [host, tablets] = entry;
        if (host == my_host) {
            host_timestamps[my_host] = co_await get_applied_timestamps(_db, std::move(tablets));
        } else {
            auto dst = raft::server_id(host.uuid());
            try {
                abort_on_expiry aoe{lowres_clock::now() + std::chrono::seconds(5)};
                auto subscription = _as.subscribe([&aoe] () noexcept {
                    aoe.abort_source().request_abort();
                });
                host_timestamps[host] = co_await ser::groups_manager_rpc_verbs::send_get_local_applied_timestamps(&_ms, host, aoe.abort_source(), dst, fence, std::move(tablets));
            } catch (...) {
                static thread_local logger::rate_limit rate_limit{std::chrono::seconds(5)};
                logger.log(log_level::warn, rate_limit, "stable_timestamp_tracker::refresh: failed to get timestamps from {}: {}", host, std::current_exception());
            }
        }
    });

    std::unordered_map<host_id, applied_timestamp_per_tablet> stable_timestamps;
    for (const auto& table : sc_tables) {
        _as.check();
        const auto& tmap = tm->tablets().get_tablet_map(table);
        co_await tmap.for_each_tablet([&](tablet_id tid, const tablet_info& ti) -> future<> {
            auto group_id = tmap.get_tablet_raft_info(tid).group_id;
            auto git = _raft_groups.find(group_id);
            if (git == _raft_groups.end()) {
                co_return; // Skip stable_timestamp refresh.
            }

            auto& state = git->second;
            if (!state.leader_info) {
                co_return; // Skip stable_timestamp refresh.
            }

            auto replica_ts = [&](const tablet_replica& r) -> api::timestamp_type {
                auto host_it = host_timestamps.find(r.host);
                if (host_it == host_timestamps.end()) {
                    return api::min_timestamp;
                }
                auto ts_it = host_it->second.find(global_tablet_id{table, tid});
                return ts_it == host_it->second.end() ? api::min_timestamp : ts_it->second;
            };
            api::timestamp_type min_ts = std::ranges::min(ti.replicas | std::views::transform(replica_ts));
            co_await advance(state, group_id, min_ts);

            for (const auto& replica : ti.replicas) {
                if (replica.host != my_host) {
                    stable_timestamps[replica.host][global_tablet_id{table, tid}] = min_ts;
                }
            }
        });
    }

    _as.check();
    co_await coroutine::parallel_for_each(stable_timestamps, [&](const auto& entry) -> future<> {
        auto& [host, timestamps] = entry;
        auto dst = raft::server_id(host.uuid());
        try {
            abort_on_expiry aoe{lowres_clock::now() + std::chrono::seconds(5)};
            auto subscription = _as.subscribe([&aoe] () noexcept {
                aoe.abort_source().request_abort();
            });
            co_await ser::groups_manager_rpc_verbs::send_set_stable_timestamp(
                &_ms, host, aoe.abort_source(), dst, fence, std::move(timestamps));
        } catch (...) {
            static thread_local logger::rate_limit rate_limit{std::chrono::seconds(5)};
            logger.log(log_level::warn, rate_limit, "stable_timestamp_tracker::refresh: failed to scatter stable_timestamp to {}: {}", host, std::current_exception());
        }
    });
}

void groups_manager::update(token_metadata_ptr new_tm) {
    if (!_features.strongly_consistent_tables) {
        return;
    }

    if (!_started) {
        _pending_tm = new_tm;
        return;
    }

    // The strongly_consistent_tables feature may have become enabled only after
    // start() ran, so launch the periodic refresh fiber here. Idempotent.
    _stable_timestamp_tracker.start();

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
                continue;
            }

            logger.info("update(): starting raft server for tablet {}, group id {}", tablet, id);
            state.gate = make_lw_shared<gate>();
            _starting_groups.push_back(state);
            state.server_control_op = futurize_invoke([&state, this, tablet, id, new_tm](this auto) -> future<> {
                co_await state.server_control_op.get_future();
                co_await start_raft_group(tablet, id, std::move(new_tm));
                state.server = &_raft_gr.get_server(id);

                // Restore persisted stable_timestamp so the value survives restarts and never regresses.
                state.stable_timestamp = co_await raft_groups_storage::load_stable_timestamp(_qp, id, this_shard_id());

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

    _stable_timestamp_tracker.start();
}

future<> groups_manager::stop() {
    co_await uninit_messaging_service();

    if (!_started) {
        co_return;
    }

    logger.info("stop() enter");

    co_await _stable_timestamp_tracker.stop();

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
