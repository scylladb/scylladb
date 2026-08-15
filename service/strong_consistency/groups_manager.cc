/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "groups_manager.hh"

#include "locator/tablets.hh"
#include "locator/tablet_sharder.hh"
#include "raft/raft.hh"
#include "raft/server.hh"
#include "service/migration_manager.hh"
#include "service/strong_consistency/state_machine.hh"
#include "service/strong_consistency/raft_groups_storage.hh"
#include "service/strong_consistency/raft_resize_tracker.hh"
#include "gms/feature_service.hh"
#include "gms/gossiper.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_timeout.hh"
#include "service/storage_proxy.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "idl/strong_consistency/groups_manager.dist.hh"
#include "utils/error_injection.hh"
#include "utils/exceptions.hh"
#include <algorithm>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <seastar/core/abort_source.hh>

namespace service::strong_consistency {

using namespace locator;

static logging::logger logger("sc_groups_manager");

static raft::server_id to_server_id(host_id host_id) {
    return raft::server_id{host_id.uuid()};
};

// The applier queue limit a group runs with while it takes part in a resize as a child, in place
// of the default, which a held-back applier would otherwise hit.
//
// The bound counts messages, not entries or bytes, so the 20MB log limiter bites first for most
// workloads. Only many tiny writes, each committed on its own, reach this bound first.
static constexpr size_t resizing_applier_queue_max_size = 10'000;

// Applies the limit above to `server`, which may not have been created yet - the next update()
// takes care of it then.
static void apply_applier_queue_max_size(raft::server* server, bool is_child) {
    if (!server) {
        return;
    }
    server->set_applier_queue_max_size(is_child
        ? resizing_applier_queue_max_size
        : raft::server::default_applier_queue_max_size);
}

// Returns a token the given tablet owns. During a resize, returns one owned both by that tablet
// and by the tablet the given group will serve once the resize is finalized. Such a token always
// exists: the ranges of a parent and its children always overlap.
static dht::token stable_token_of_group(const tablet_map& tablet_map, tablet_id tid, raft::group_id gid) {
    if (tablet_map.get_tablet_raft_info(tid).group_id == gid) {
        return tablet_map.get_last_token(tid);
    }
    // A child of this tablet's group. It serves a part of the tablet's token range now and a
    // tablet of its own once the resize is finalized.
    //
    // The left child owns the tokens up to and including the split token, the right one the rest.
    // A merge produces a single child, which owns every token of each of its parents.
    if (tablet_map.resize_decision().is_split() && gid == tablet_map.get_split_child_gids(tid).first) {
        return tablet_map.get_split_token(tid);
    }
    return tablet_map.get_last_token(tid);
}

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

void raft_server::advance_leader_timestamp(api::timestamp_type ts) {
    if (_state.leader_info) {
        _state.leader_info->last_timestamp = std::max(_state.leader_info->last_timestamp, ts);
    }
}

std::optional<api::timestamp_type> raft_server::leader_timestamp() const {
    if (_state.leader_info && _state.leader_info->term == _state.server->get_current_term()) {
        return _state.leader_info->last_timestamp;
    }
    return std::nullopt;
}

groups_manager::groups_manager(netw::messaging_service& ms, 
        raft_group_registry& raft_gr, cql3::query_processor& qp,
        replica::database& db, service::migration_manager& mm, db::system_keyspace& sys_ks, gms::feature_service& features,
        gms::gossiper& gossiper, db::raft_commitlog_replay_buffer& raft_replay_buffer, sharded<raft_resize_tracker>& resize_tracker)
    : _ms(ms)
    , _raft_gr(raft_gr)
    , _qp(qp)
    , _db(db)
    , _mm(mm)
    , _sys_ks(sys_ks)
    , _features(features)
    , _gossiper(gossiper)
    , _raft_replay_buffer(raft_replay_buffer)
    , _resize_tracker(resize_tracker.local())
{
    init_messaging_service();
}

future<> groups_manager::start_raft_group(global_tablet_id tablet,
        raft::group_id group_id,
        token_metadata_ptr tm,
        std::optional<raft::group_id> parent_gid)
{
    const auto my_id = to_server_id(tm->get_my_id());

    // Restore the parent's persisted resize state before we create its raft server. The token
    // metadata captured here may predate an ending whose observation already erased the state, so
    // we consult the tracker too: a stale start must not re-create what a later observation erased.
    {
        const auto& tablet_map = tm->tablets().get_tablet_map(tablet.table);
        if (tablet_map.is_resizing(tablet.tablet)
                && tablet_map.get_tablet_raft_info(tablet.tablet).group_id == group_id
                && _resize_tracker.is_resizing(group_id)) {
            co_await _resize_tracker.restore_applied_markers(group_id);
        }
    }

    auto* commitlog = _db.commitlog();
    SCYLLA_ASSERT(commitlog);
    auto storage = std::make_unique<raft_groups_storage>(_qp, group_id, my_id, this_shard_id(),
        *commitlog, tablet.table, _raft_replay_buffer.take_replayed_group_entries(group_id));

    auto state_machine = make_state_machine(tablet, group_id, _db, _mm, _sys_ks, *storage, _resize_tracker);

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

    // A child group needs its leader on the node leading its parent. Fast bootstrap elects the
    // voter at rank (seed % num_voters) in ascending server id order, so we derive the seed from
    // the rank of the parent's leader, which every replica knows. If that leader is not known yet,
    // we keep the default seed and let the colocator move the leadership afterwards.
    uint64_t fast_bootstrap_seed = std::hash<raft::group_id>()(group_id);
    if (parent_gid) {
        const auto& tablet_map = tm->tablets().get_tablet_map(tablet.table);
        const auto& tablet_info = tablet_map.get_tablet_info(tablet.tablet);
        const auto parent_it = _raft_groups.find(*parent_gid);
        if (parent_it != _raft_groups.end() && parent_it->second.server) {
            const auto parent_leader = parent_it->second.server->current_leader();
            if (parent_leader != raft::server_id{}) {
                std::vector<raft::server_id> voters;
                voters.reserve(tablet_info.replicas.size());
                for (const auto& r : tablet_info.replicas) {
                    voters.push_back(to_server_id(r.host));
                }
                std::ranges::sort(voters);
                const auto it = std::ranges::find(voters, parent_leader);
                if (it != voters.end()) {
                    fast_bootstrap_seed = static_cast<uint64_t>(it - voters.begin());
                    logger.debug("start_raft_group: co-locating child {} initial leader with parent {} leader {} (rank {})",
                        group_id, *parent_gid, parent_leader, fast_bootstrap_seed);
                }
            }
        }
    }
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
        .fast_bootstrap_seed = fast_bootstrap_seed
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

    if (state.resize_colocation) {
        detach_resize_colocation(state);
    }

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

        // Drop the deleted group's own record: the whole state if this is the parent, and just
        // the mapping if it is a child. Unlike the erase from _raft_groups below, we need no check
        // for a start which superseded the deletion - a group deleted while a resize is recorded
        // is deleted by an ending it does not come back from.
        _resize_tracker.erase_group(id);

        _raft_gr.destroy_server(id);
        // The state object outlives the server whenever a start superseded this deletion, and
        // update() reads state.server on every token metadata change, so the pointer must not be
        // left dangling. The superseding start reassigns it once the new server is up.
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

    ser::groups_manager_rpc_verbs::register_process_raft_resize(&_ms,
        [this] (rpc::opt_time_point timeout, raft::server_id dst_id, locator::global_tablet_id tablet, raft::group_id parent_gid,
                std::vector<raft::group_id> new_gids, bool wait_only) -> future<bool> {
            if (_raft_gr.get_my_raft_id() != dst_id) {
                throw raft_destination_id_not_correct{_raft_gr.get_my_raft_id(), dst_id};
            }
            const auto shard = find_shard_for_tablet(tablet, parent_gid);
            if (!shard) {
                logger.debug("process_raft_resize: no local shard hosts group {} of tablet {}", parent_gid, tablet);
                co_return false;
            }
            co_return co_await container().invoke_on(*shard,
                    [timeout, tablet, parent_gid, new_gids = std::move(new_gids), wait_only] (groups_manager& gm) -> future<bool> {
                // The verb is declared with_timeout, so the caller always sets a deadline.
                abort_on_expiry aoe(*timeout);
                co_return co_await gm.handle_process_raft_resize(tablet, parent_gid, new_gids, wait_only,
                        aoe.abort_source());
            });
        }
    );
}

std::optional<shard_id> groups_manager::find_shard_for_tablet(locator::global_tablet_id tablet, raft::group_id expected_gid) const {
    if (!_db.column_family_exists(tablet.table)) {
        return std::nullopt;
    }
    auto erm = _db.find_column_family(tablet.table).get_effective_replication_map();
    const auto& tm = erm->get_token_metadata();
    const auto& tablet_map = tm.tablets().get_tablet_map(tablet.table);
    if (!tablet_map.has_raft_info()) {
        return std::nullopt;
    }
    // A tablet id is an index into the tablet map, so it only names the tablet the caller meant
    // as long as this replica sees the same map. Comparing the group id catches the case where it
    // does not - the caller retries until this replica catches up.
    if (tablet.tablet.value() >= tablet_map.tablet_count()
            || tablet_map.get_tablet_raft_info(tablet.tablet).group_id != expected_gid) {
        return std::nullopt;
    }
    return locator::get_shard_for_reads(tablet_map, tablet.tablet, tm.get_my_id());
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

future<> groups_manager::leader_info_updater(raft_group_state& state, table_id table, raft::group_id gid,
        dht::token token) {
    try {
        const auto schema = _db.find_schema(table);
        const auto server_id = state.server->id();

        while (true) {
            const auto current_term = state.server->get_current_term();
            const auto current_leader = state.server->current_leader();

            notify_leader_change(gid);

            if (current_leader == server_id) {
                logger.debug("leader_info_updater({}-{}): current term {}, running read_barrier()",
                    table, gid,
                    current_term);
                // We intentionally pass nullptr here. If the tablet is leaving this node,
                // the Raft server will be aborted and the loop will break.
                // The same will happen when the node is shutting down.
                // There's no reason to abort this operation in any other case.
                co_await state.server->read_barrier(nullptr);

                auto& cf = schema->table();
                const auto tid = cf.get_effective_replication_map()->get_token_metadata()
                        .tablets().get_tablet_map(table).get_tablet_id(token);
                const auto last_timestamp = cf.get_max_timestamp_for_tablet(tid);
                if (!last_timestamp) {
                    // This shard holds no storage for the tablet, so there is no clock here to
                    // seed the term from. The tablet is not served here any more - the storage
                    // goes when it leaves - and the group's own teardown follows. We end the fiber
                    // rather than hand out timestamps this replica cannot back.
                    logger.debug("leader_info_updater({}-{}): tablet {} is not served by this shard, stopping",
                        table, gid, tid);
                    state.leader_info = std::nullopt;
                    state.leader_info_cond.broadcast();
                    co_return;
                }
                state.leader_info = leader_info {
                    .term = current_term,
                    .last_timestamp = *last_timestamp
                };
                logger.debug("leader_info_updater({}-{}): read_barrier() completed, "
                    "new leader term {}, tablet now served {}, last_timestamp {}",
                    table, gid,
                    state.leader_info->term, tid,
                    state.leader_info->last_timestamp);
            } else if (state.leader_info) {
                logger.debug("leader_info_updater({}-{}): this replica {} is no longer a leader, current leader {}",
                    table, gid, server_id, current_leader);
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
            table, gid, std::current_exception());
    } catch (const raft::stopped_error&) {
        // thrown from read_barrier() and wait_for_state_change when the tablet leaves this shard
        logger.debug("leader_info_updater({}-{}): got raft::stopped_error {}",
            table, gid, std::current_exception());
    } catch (const replica::no_such_column_family&) {
        // thrown from find_schema() and schema->table() when the table is dropped
        logger.debug("leader_info_updater({}-{}): got replica::no_such_column_family {}",
            table, gid, std::current_exception());
    } catch (...) {
        on_internal_error(logger, ::format("leader_info_updater({}-{}): unexpected exception: {}",
            table, gid, std::current_exception()));
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
        struct tablet_group_info {
            tablet_id tid;
            raft::group_id id;
            std::optional<raft::group_id> parent_id;
        };
        std::vector<tablet_group_info> tablet_groups;
        for (const auto& tid: tablet_map.tablet_ids()) {
            const auto id = tablet_map.get_tablet_raft_info(tid).group_id;
            _leader_cache.mark_seen(id);
            if (!tablet_map.has_replica(tid, this_replica)) {
                continue;
            }
            tablet_groups.push_back(tablet_group_info{tid, id, std::nullopt});

            // The group serves a tablet of its own, so it is nobody's child any more: the resize
            // which created it was finalized. The group is not deleted, so we drop the mapping
            // here. We check this on its own rather than with the case below, because a single
            // token metadata change can carry both the finalization of one resize and the start of
            // another in which this group is the parent.
            if (_resize_tracker.get_parent_group(id)) {
                logger.debug("update(): group {} is no longer a child, dropping its mapping", id);
                _resize_tracker.erase_group(id);
            }

            // A resize this replica has observed only ever ends by the tablet map being replaced,
            // which takes the parent's tablet away and has its teardown drop the state.
            if (tablet_map.is_resizing(tid)) {
                const auto& new_gids = tablet_map.get_raft_resize_info(tid).new_gids;
                _resize_tracker.set_replacement_groups(id, new_gids);
                start_leader_colocator(_raft_groups[id], global_tablet_id{table_id, tid}, id, {new_gids.begin(), new_gids.end()});
                for (const auto new_gid : new_gids) {
                    tablet_groups.push_back(tablet_group_info{tid, new_gid, id});
                }
            }
        }

        for (const auto& [tid, id, parent_id]: tablet_groups) {
            const auto tablet = global_tablet_id{table_id, tid};

            auto& state = _raft_groups[id];
            state.has_tablet = true;

            // We set this from the tablet metadata on every token metadata change rather than
            // toggling it when the resize ends, so that a finalization and a restart both restore
            // the default without a path of their own. It has to happen before the check below,
            // which a group that is already running never gets past.
            apply_applier_queue_max_size(state.server, parent_id.has_value());

            // Don't start the raft server if it is already (started or starting) and not stopping.
            if (state.gate && !state.gate->is_closed()) {
                continue;
            }

            logger.info("update(): starting raft server for tablet {}, group id {}", tablet, id);
            state.gate = make_lw_shared<gate>();
            _starting_groups.push_back(state);
            const auto token = stable_token_of_group(tablet_map, tid, id);
            state.server_control_op = futurize_invoke([&state, this, tablet, id, new_tm, parent_id, token](this auto) -> future<> {
                co_await state.server_control_op.get_future();
                co_await start_raft_group(tablet, id, std::move(new_tm), parent_id);
                state.server = &_raft_gr.get_server(id);
                // The update() which started this group could not do it - the server did not exist
                // yet. A stale limit is corrected by the next update(), which finalizing or rolling
                // back the resize triggers.
                apply_applier_queue_max_size(state.server, parent_id.has_value());
                state.leader_info_updater = leader_info_updater(state, tablet.table, id, token);

                // We want to make sure the server is ready to serve requests before
                // we report it as started in wait_for_groups_to_start().
                //
                // A group replacing one whose resize is still under way is the exception: waiting
                // for a leader would wait for the sealing, which this very wait prevents on the
                // node driving it. Reporting the group as started right away costs nothing.
                if (!parent_id) {
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
    // schedule_raft_group_deletion), so a dropped table is reported as such rather than as a
    // group which is on its way out.
    //
    // Node shutdown also closes gates (groups_manager::stop() closes every gate
    // regardless of table existence), but it cannot race with us either: the
    // strongly consistent coordinator, the only caller of acquire_server, is
    // destroyed before groups_manager::stop() runs.
    if (!_db.column_family_exists(table_id)) {
        return make_exception_future<raft_server>(
            replica::no_such_column_family(table_id));
    }

    // The group may be gone, or on its way out, although the table is still here - a normal
    // outcome, not an invariant to assert. The deletion closes the gate first and erases the entry
    // once the server is torn down, so both shapes mean the same thing.
    const auto it = _raft_groups.find(group_id);
    if (it == _raft_groups.end()) {
        logger.debug("acquire_server: raft group {} of table {} is not served here", group_id, table_id);
        return make_exception_future<raft_server>(group_not_served());
    }
    auto& state = it->second;
    auto h = state.gate->try_hold();
    if (!h) {
        logger.debug("acquire_server: raft group {} of table {} is being deleted", group_id, table_id);
        return make_exception_future<raft_server>(group_not_served());
    }
    return state.server_control_op.get_future(as).then([&state, h = std::move(*h)] mutable {
        return raft_server(state, std::move(h));
    });
}

std::optional<raft_server> groups_manager::try_acquire_server(raft::group_id group_id) {
    const auto it = _raft_groups.find(group_id);
    if (it == _raft_groups.end()) {
        return std::nullopt;
    }
    auto& state = it->second;
    // server_control_op is not awaited here, so the server may still be starting.
    if (!state.server || !state.gate) {
        return std::nullopt;
    }
    auto h = state.gate->try_hold();
    if (!h) {
        // The group is being stopped.
        return std::nullopt;
    }
    return raft_server(state, std::move(*h));
}

bool groups_manager::should_handoff_writes(raft::group_id group_id) const {
    return _resize_tracker.should_handoff_writes(group_id);
}

std::optional<raft::group_id> groups_manager::group_for_handoff(schema_ptr s, const dht::token& token) const {
    const auto& tablet_map = s->table().get_effective_replication_map()->get_token_metadata().tablets().get_tablet_map(s->id());
    if (!tablet_map.has_raft_info()) {
        on_internal_error(logger, format("group_for_handoff: table {} does not have raft info", s->id()));
    }
    const auto tablet_id = tablet_map.get_tablet_id(token);
    if (!tablet_map.is_resizing(tablet_id)) {
        logger.debug("group_for_handoff: tablet {} of table {} is not resizing any more", tablet_id, s->id());
        return std::nullopt;
    }
    // The kind of the resize in progress is the kind of the resize decision, and
    // get_split_child_gids() only answers for a split. A merge never gets here: no merge decision
    // is emitted for a strongly consistent table, see tablet_allocator_impl's process_table().
    const auto& decision = tablet_map.resize_decision();
    if (!decision.is_split()) {
        on_internal_error(logger, format("group_for_handoff: tablet {} is not being split, its "
                "resize decision is {}", tablet_id, decision.type_name()));
    }
    // The left child owns the tokens up to and including the split token, the right one the
    // rest, as in stable_token_of_group().
    const auto [left, right] = tablet_map.get_split_child_gids(tablet_id);
    return token <= tablet_map.get_split_token(tablet_id) ? left : right;
}

void groups_manager::notify_leader_change(raft::group_id gid) {
    // A child is watched through its parent, so that a single colocation state covers a whole
    // resize.
    const auto parent_gid = _resize_tracker.get_parent_group(gid).value_or(gid);
    auto it = _raft_groups.find(parent_gid);
    if (it == _raft_groups.end() || !it->second.resize_colocation) {
        return;
    }
    auto& colocation = *it->second.resize_colocation;
    ++colocation.leader_change_seq;
    colocation.leader_changed.broadcast();
}

// Takes the parent's leader by value rather than the parent itself, so that no server this
// function does not operate on is kept alive while it runs - see leader_colocator().
future<groups_manager::colocation_status> groups_manager::colocate_leaders(
        raft::server_id parent_leader, raft::group_id parent_gid, const std::vector<raft::group_id>& new_gids) {
    auto status = colocation_status::colocated;
    for (const auto new_gid : new_gids) {
        auto child = try_acquire_server(new_gid);
        if (!child) {
            // The child isn't running here yet. Its leader_info_updater signals us as soon as
            // it starts.
            status = colocation_status::awaiting_leader_change;
            continue;
        }
        const auto child_leader = child->server().current_leader();
        if (child_leader == parent_leader) {
            continue;
        }
        if (!child_leader) {
            // An election is in progress in the child, it may still elect the co-located
            // replica on its own.
            status = colocation_status::awaiting_leader_change;
            continue;
        }
        // Only the current leader of a child can hand its leadership over. Every replica of the
        // parent watches this, so the transfer is performed by whichever of them leads the child.
        if (child_leader != child->server().id()) {
            status = colocation_status::awaiting_leader_change;
            continue;
        }

        logger.debug("colocate_leaders: group {} is led by this node while its parent {} is led by {}, "
            "transferring the leadership", new_gid, parent_gid, parent_leader);
        constexpr auto transfer_timeout = raft::logical_clock::duration(std::chrono::seconds(5) / raft_tick_interval);

        // Start the transfer while holding the child, then let go of it before waiting the whole
        // of transfer_timeout for it. Nothing below touches the server: stepdown() hands out its
        // future synchronously. If the server is destroyed while we wait, the future is broken and
        // the transfer is reported as failed like any other, which the caller retries.
        auto transfer = child->server().stepdown(transfer_timeout, parent_leader);
        child.reset();

        auto result = co_await coroutine::as_future(std::move(transfer));
        if (result.failed()) {
            auto ex = result.get_exception();
            if (try_catch<raft::no_other_voting_member>(ex)) {
                // Single-voter group: leadership cannot be transferred and the leaders cannot be
                // co-located any other way. Unreachable in practice, because with a single voter
                // the parent and the children all share it.
                on_internal_error(logger, format("colocate_leaders: cannot co-locate group {} "
                    "with parent {} leader {}: {}", new_gid, parent_gid, parent_leader, ex));
            }
            // Transient: the transfer times out if the target doesn't catch up with the log in
            // time, and fails outright if a stepdown is already in progress. Logged at debug
            // because the caller retries every 10ms for as long as it takes.
            logger.debug("colocate_leaders: leadership transfer of group {} to parent {} leader {} failed: {}",
                new_gid, parent_gid, parent_leader, ex);
            co_return colocation_status::transfer_failed;
        }
        // Don't transfer the remaining children in the same round: each transfer may take up to
        // transfer_timeout, and the leadership picture can change while it runs. The caller
        // re-checks anyway, so the next round picks the remaining ones up.
        co_return colocation_status::transfer_done;
    }
    co_return status;
}

future<> groups_manager::leader_colocator(resize_colocation_state& colocation, raft::group_id parent_gid) {
    // `colocation` is the state this fiber is stored in, and outlives it. It is passed directly
    // rather than found through the group's raft_group_state, which points at nothing, or at a
    // newer resize's state, from the moment this one is detached.
    auto& as = colocation.as;
    const auto tablet = colocation.tablet;
    try {
        // groups_manager::update() starts this fiber synchronously, in the middle of committing
        // a token metadata change. Get off its stack before touching any raft server - the
        // first round may already transfer a leadership.
        co_await yield();

        logger.debug("leader_colocator({}-{}): maintaining co-location with {}",
            tablet, parent_gid, colocation.new_gids);

        // Runs until aborted, which the parent group's deletion does - be the resize finalized,
        // the table dropped, or the tablet moved away - so the fiber never outlives its subject.
        // start_leader_colocator() detaches it too, but only to put another in its place.
        while (true) {
            // Sampled before the check at the end, so that a leadership change which happens while
            // we are checking is not slept through.
            const auto seq = colocation.leader_change_seq;

            auto parent = try_acquire_server(parent_gid);
            if (!parent) {
                // Transient while the raft server is (re)starting. It also fails permanently
                // once the group is scheduled for deletion, but that aborts `as` too.
                co_await sleep_abortable(10ms, as);
                continue;
            }

            const auto parent_leader = parent->server().current_leader();
            if (!parent_leader) {
                // Nothing to co-locate with yet; the election has to complete before the parent
                // can serve writes anyway. A follower learning about a new leader is not a raft
                // state change and does not signal leader_changed, so we wait for the election to
                // conclude instead. We start the wait while holding the parent and wait for it
                // after letting go, for the reason given below.
                auto leader_elected = parent->server().wait_for_leader(&as);
                parent.reset();
                co_await std::move(leader_elected);
                continue;
            }
            // Let go of the parent before waiting for anything: this fiber never holds a group
            // across a wait. Only the leader's id is needed below, and it is a snapshot either
            // way - the leaders can move while we work, which is why this runs in a loop.
            parent.reset();

            const auto status = co_await colocate_leaders(parent_leader, parent_gid, colocation.new_gids);
            switch (status) {
            case colocation_status::transfer_done:
                continue;
            case colocation_status::transfer_failed:
                co_await sleep_abortable(10ms, as);
                continue;
            case colocation_status::colocated:
            case colocation_status::awaiting_leader_change:
                break;
            }

            // Nothing to do until some leadership moves - a child electing us, or this node
            // losing the parent. leader_info_updater() of every group in the resize signals it.
            while (colocation.leader_change_seq == seq) {
                co_await wait_with_abort_source(colocation.leader_changed, as);
            }
        }
    } catch (...) {
        auto ex = std::current_exception();
        if (as.abort_requested() || try_catch<raft::stopped_error>(ex)) {
            // The resize is over, or the group is being stopped. A raft server this fiber drives
            // can be aborted before `as` is - the groups are torn down in no particular order - so
            // raft::stopped_error is as normal an exit here as the abort itself.
            logger.debug("leader_colocator({}-{}): stopping", tablet, parent_gid);
            co_return;
        }
        // Losing this fiber only means that the writes of this group may stall until the resize
        // completes, so don't bring the node down over it.
        logger.warn("leader_colocator({}-{}): stopped with an error: {}",
            tablet, parent_gid, ex);
    }
}

void groups_manager::detach_resize_colocation(raft_group_state& state) {
    // Only signals: the caller may be in the middle of committing a token metadata change, so
    // nothing is awaited here. The fiber is parked in the drain together with the state it is bound
    // to, which has to stay alive until it lets go of it.
    //
    // The caller must ensure that state.resize_colocation is set.
    auto colocation = std::exchange(state.resize_colocation, nullptr);
    colocation->as.request_abort();
    auto fiber = std::exchange(colocation->colocator, make_ready_future<>());
    _draining_colocators = _draining_colocators.then(
            [fiber = std::move(fiber), colocation = std::move(colocation)] () mutable {
        return std::move(fiber).finally([colocation = std::move(colocation)] {});
    });
}

void groups_manager::start_leader_colocator(raft_group_state& state, locator::global_tablet_id tablet,
        raft::group_id parent_gid, std::vector<raft::group_id> new_gids) {
    if (state.resize_colocation) {
        // Already running, which is the common case: update() runs on every token metadata change
        // and re-records every resize which is still in the tablet metadata.
        if (std::ranges::equal(state.resize_colocation->new_gids, new_gids)) {
            logger.debug("start_leader_colocator: group {} is already being replaced by {}, "
                "nothing to do", parent_gid, state.resize_colocation->new_gids);
            return;
        }
        // A colocator of an earlier resize of this parent is still installed. Not expected: the
        // ids of a tablet are generated once and never regenerated, so the same parent never gets
        // a second set. We handle it rather than leave it to the assignment below, which would
        // destroy the state the installed fiber runs on.
        logger.info("start_leader_colocator: group {} is still being replaced by {}, "
            "replacing the colocator to serve {}", parent_gid, state.resize_colocation->new_gids,
            new_gids);
        detach_resize_colocation(state);
    }
    // Built in place: the abort source and the condition variable it holds cannot be moved, so
    // the state cannot be constructed as a temporary and handed over.
    state.resize_colocation = std::make_unique<resize_colocation_state>(tablet, std::move(new_gids));
    state.resize_colocation->colocator = leader_colocator(*state.resize_colocation, parent_gid);
}

future<bool> groups_manager::handle_process_raft_resize(locator::global_tablet_id tablet, raft::group_id parent_gid,
        const std::vector<raft::group_id>& new_gids, bool wait_only, abort_source& as) {
    // Anything this verb needs may not be here yet, or may be on its way out. Every such case
    // resolves on its own, so we report failure and let the topology coordinator retry instead of
    // erroring out. The parent's raft server in particular is registered only once update() has
    // run on this shard, which a replica which has just restarted may not have done.
    auto parent = try_acquire_server(parent_gid);
    if (!parent) {
        logger.debug("process_raft_resize: the raft server of group {} is not available here", parent_gid);
        co_return false;
    }
    // update() records the resize in the same pass in which it starts the children, so a missing
    // state means this replica has not observed the new tablet metadata yet.
    if (!_resize_tracker.is_resizing(parent_gid)) {
        logger.debug("process_raft_resize: group {} is not known to be resizing here yet", parent_gid);
        co_return false;
    }
    if (wait_only) {
        if (new_gids.empty()) {
            // The caller records the replacement group ids before it drives anything, and never
            // records an empty set, so this only answers a peer which does not agree with us on
            // what it is asking for.
            logger.debug("process_raft_resize: no replacement groups given for {}", parent_gid);
            co_return false;
        }
        const auto finished = _resize_tracker.get_parent_finished_future(new_gids.front());
        if (!finished) {
            logger.debug("process_raft_resize: group {} is not known here as a child of {}",
                new_gids.front(), parent_gid);
            co_return false;
        }
        // We drop the group's gate holder for the wait and the flush below. Holding it across
        // the wait would deadlock: the teardown is what breaks the promise we wait on.
        parent.reset();
        // Reported rather than thrown, like every other way this verb fails to get what it was
        // asked for. The wait ends with an exception when the deadline passes, and when the parent
        // is torn down under us by a node shutdown or a table drop. The caller answers both by
        // retrying, or by giving the attempt up.
        if (auto f = co_await coroutine::as_future(finished->get_future(as)); f.failed()) {
            logger.debug("process_raft_resize: waiting for group {} to finish resizing failed: {}",
                parent_gid, f.get_exception());
            co_return false;
        }

        // Finalization takes the parent out of the tablet map, after which a replay discards the
        // parent's entries, so we get what it applied onto disk first. Once per resize: the
        // parent's log is final by now, so nothing more can arrive.
        if (const auto it = _raft_groups.find(parent_gid);
                it != _raft_groups.end() && it->second.seal_flushed) {
            co_return true;
        }
        if (!_db.column_family_exists(tablet.table)) {
            // Dropped while we were waiting.
            logger.debug("process_raft_resize: table {} of tablet {} no longer exists here",
                tablet.table, tablet);
            co_return false;
        }
        co_await _db.find_column_family(tablet.table).flush_tablet(tablet.tablet);
        co_await _db.find_column_family(db::system_keyspace::raft_groups()->id()).flush();
        // FIXME: take a snapshot of the parent here, to release the replay position handles of
        // the entries its applier never sees. acquire_replay_position_handles_for() takes over
        // those of command entries alone, so the configuration and dummy ones stay in
        // raft_commitlog's map until truncate_log_tail(), which only a snapshot drives. The
        // segments holding them stay dirty until the node restarts. This is also the one point
        // where such a snapshot would be sound: take_snapshot() captures no state, so its
        // descriptor asserts nothing beyond "the entries below me need not be replayed", and the
        // flushes above are what makes that true. It cannot be taken yet: truncating the log
        // leaves a replica which was down for part of the resize behind the snapshot index, and
        // catching one up needs transfer_snapshot(), which is not implemented for these groups.
        if (const auto it = _raft_groups.find(parent_gid); it != _raft_groups.end()) {
            it->second.seal_flushed = true;
        }
        co_return true;
    }

    // The children's leaders have to be co-located with the parent's before the no_op entries go
    // into them. Otherwise the read barriers their leader_info_updater() runs cannot finish, even
    // once the leaders do come together, and no handed-off write can be committed. The colocator
    // fiber is normally already maintaining this, so it usually holds on the first try.
    const auto resize_started = _resize_tracker.should_handoff_writes(parent_gid);
    if (!resize_started) {
        // An unknown parent leader is not a co-location: there would be nothing to co-locate the
        // children with, and the caller retries until the election concludes.
        const auto parent_leader = parent->server().current_leader();
        if (!parent_leader || co_await colocate_leaders(parent_leader, parent_gid, new_gids)
                != colocation_status::colocated) {
            co_return false;
        }
    }

    auto to_raft_command = [](raft_command command) {
        raft::command raft_cmd;
        ser::serialize(raft_cmd, command);
        return raft_cmd;
    };

    auto make_resize_cmd = [&to_raft_command] (resize_marker_kind kind) {
        return to_raft_command(raft_command{.change = resize_marker{.kind = kind}});
    };

    if (!resize_started) {
        // We put a no_op entry into each child so that its applier has something to block on.
        // From now on a read barrier in those groups blocks too, released when the parent applies
        // end_resize. That is what makes their linearizable reads wait for the parent's log to be
        // applied in full.
        for (const auto new_gid : new_gids) {
            auto child = try_acquire_server(new_gid);
            if (!child) {
                logger.debug("process_raft_resize: the raft server of group {}, a child of {}, is not available here",
                    new_gid, parent_gid);
                co_return false;
            }
            auto res = co_await coroutine::as_future(child->server().add_entry(
                to_raft_command(raft_command{.change = no_op{}}), raft::wait_type::committed, &as));
            if (res.failed()) {
                logger.debug("process_raft_resize: the no_op entry for group {} failed: {}", new_gid, res.get_exception());
                co_return false;
            }
        }

        // The first irreversible step of the seal: from here on the parent's writes are handed off
        // to its children, and no marker is ever cleared.
        auto res = co_await coroutine::as_future(parent->server().add_entry(
            make_resize_cmd(resize_marker_kind::start_resize), raft::wait_type::committed, &as));
        if (res.failed()) {
            logger.debug("process_raft_resize: start_resize commit for {} failed: {}", parent_gid, res.get_exception());
            co_return false;
        }
        // add_entry() only succeeds on the leader, so we are the leader and every write of this
        // group is handled here. We fast-forward the in-memory flag rather than wait for the entry
        // to be applied, which lets us commit end_resize immediately. Writes check the flag
        // synchronously with entering add_entry(), which appends in admission order, so every write
        // which observed !start_resize is appended before the end_resize marker below.
        _resize_tracker.mark_resize_phase(parent_gid, resize_marker_kind::start_resize);
    }

    if (!_resize_tracker.has_applied_end_resize(parent_gid)) {
        // Before the children take over for good, we lift their clocks above the parent's. The
        // per-write advance in coordinator::mutate() covers only handed-off writes and writes made
        // before start_resize was committed; entries added between the two markers can carry higher
        // timestamps than the child would calculate, and by this point we know all of them.
        //
        // We re-check the co-location even though the start of the resize did too: this may be a
        // retry which skipped that check.
        if (resize_started) {
            const auto parent_leader = parent->server().current_leader();
            if (!parent_leader || co_await colocate_leaders(parent_leader, parent_gid, new_gids)
                    != colocation_status::colocated) {
                co_return false;
            }
        }
        const auto parent_ts = parent->leader_timestamp();
        if (!parent_ts) {
            // This replica does not lead the parent, or its leader_info has not caught up with
            // the current term yet. Either resolves on its own - the parent's applier is never
            // held back - so let the caller retry.
            logger.debug("process_raft_resize: the leader clock of group {} is not known here yet", parent_gid);
            co_return false;
        }
        // The advance only reaches the child leaders hosted here whose leader_info is already
        // populated. Skipping the rest is safe, with one bounded exception: a read barrier which
        // captured the commit index before the no_op went in completes on its read quorum alone,
        // and can seed from stored data sitting below the parent's clock. That window is two
        // commits wide and opens only where the co-location above has just moved a child's
        // leadership here. Nothing here can tell such a barrier from a child held back in the
        // ordinary way, and waiting for either would be waiting for the seal this call is running.
        for (const auto new_gid : new_gids) {
            if (auto child = try_acquire_server(new_gid)) {
                child->advance_leader_timestamp(*parent_ts);
            }
        }
        auto res = co_await coroutine::as_future(parent->server().add_entry(
            make_resize_cmd(resize_marker_kind::end_resize), raft::wait_type::committed, &as));
        if (res.failed()) {
            logger.debug("process_raft_resize: end_resize commit for {} failed: {}", parent_gid, res.get_exception());
            co_return false;
        }
    }

    co_return true;
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

    // Joined after the raft servers are gone: the fibers were aborted when they were detached
    // and exit on their own, this only makes sure none of them outlives the manager.
    co_await std::exchange(_draining_colocators, make_ready_future<>());

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
