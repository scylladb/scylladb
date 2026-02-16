/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "groups_manager.hh"

#include "service/strong_consistency/state_machine.hh"
#include "gms/feature_service.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/storage_proxy.hh"
#include "replica/database.hh"
#include "db/config.hh"

namespace service::strong_consistency {

using namespace locator;

static logging::logger logger("sc_groups_manager");

static raft::server_id to_server_id(host_id host_id) {
    return raft::server_id{host_id.uuid()};
};

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

static void for_each_sc_tablet(const token_metadata& tm,
    noncopyable_function<void(global_tablet_id, raft::group_id)>&& func)
{
    const auto this_replica = locator::tablet_replica {
        .host = tm.get_my_id(),
        .shard = this_shard_id()
    };
    const auto& tablets = tm.tablets();
    for (const auto& [table_id, _]: tablets.all_table_groups()) {
        const auto& tablet_map = tablets.get_tablet_map(table_id);
        if (!tablet_map.has_raft_info()) {
            continue;
        }
        for (const auto& tablet_id: tablet_map.tablet_ids()) {
            if (tablet_map.has_replica(tablet_id, this_replica)) {
                const auto group_id = tablet_map.get_tablet_raft_info(tablet_id).group_id;
                func(global_tablet_id{table_id, tablet_id}, group_id);
            }
        }
    }
}

raft_server::raft_server(groups_manager::raft_group_state& state, gate::holder holder)
    : _state(state)
    , _holder(std::move(holder))
{
}

auto raft_server::begin_mutate() -> begin_mutate_result {
    const auto leader = _state.server->current_leader();
    if (!leader) {
        return need_wait_for_leader{_state.server->wait_for_leader(nullptr)};
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
        return need_wait_for_leader{_state.leader_info_cond.wait()};
    }
    const auto new_ts = std::max(api::new_timestamp(), _state.leader_info->last_timestamp + 1);
    _state.leader_info->last_timestamp = new_ts;
    return timestamp_with_term{new_ts, term};
}

groups_manager::groups_manager(netw::messaging_service& ms, 
        raft_group_registry& raft_gr, cql3::query_processor& qp,
        replica::database& db, gms::feature_service& features)
    : _ms(ms)
    , _raft_gr(raft_gr)
    , _qp(qp)
    , _db(db)
    , _features(features)
{
}

future<> groups_manager::start_raft_group(global_tablet_id tablet,
        raft::group_id group_id,
        token_metadata_ptr tm)
{
    const auto my_id = to_server_id(tm->get_my_id());

    auto state_machine = make_state_machine(tablet, group_id, _db);
    auto& state_machine_ref = *state_machine;
    auto rpc = std::make_unique<rpc_impl>(state_machine_ref, _ms, _raft_gr.failure_detector(), group_id, my_id);
    // Keep a reference to a specific RPC class.
    auto& rpc_ref = *rpc;
    auto storage = std::make_unique<raft_sys_table_storage>(_qp, group_id, my_id);

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
        .enable_forwarding = false,
        .on_background_error = [tablet, group_id](std::exception_ptr e) {
            on_internal_error(logger, 
                ::format("table {}, tablet {} raft group {} background error {}", 
                    tablet.table, tablet.tablet, group_id, e));
        }
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
    });
}

// Conditionally schedule a group removal.
//
// Exceptions:
// * The function doesn't throw any exceptions.
void groups_manager::schedule_raft_group_deletion(raft::group_id id, raft_group_state& state) {
    if (state.gate->is_closed()) {
        return;
    }
    logger.info("schedule_raft_group_deletion(): group id {}", id);
    state.server_control_op = futurize_invoke([this, &state, id, g = state.gate](this auto) -> future<> {
        // If this throws an exception, it's critical and something has
        // gone really wrong. It shouldn't happen under normal circumstances.
        co_await state.server_control_op.get_future();
        co_await g->close();
        co_await _raft_gr.abort_server(id);
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

// Schedule removals of Raft groups.
//
// Behavior based on the value of `all`:
//
// * true:  all groups will be scheduled to be removed.
// * false: if a group doesn't correspond to a tablet belonging to this
//          replica (node, shard), it will be scheduled to be removed.
//          Otherwise, a group won't be scheduled to be removed.
//
// Exceptions:
// * The function doesn't throw any exceptions.
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

future<> groups_manager::wait_for_groups_to_start() {
    while (true) {
        const auto it = std::ranges::find_if(_raft_groups, [](const auto& p) {
            auto& state = p.second;
            return !state.gate->is_closed() && !state.server_control_op.available();
        });
        if (it == _raft_groups.end()) {
            break;
        }

        const auto& [id, state] = *it;
        logger.info("waiting for group {} to start", id);
        co_await state.server_control_op.get_future();
    }
}

// Exceptions:
// * If this function throws an exception, it's critical and not expected.
//   Under normal circumstances, this shouldn't throw anything.
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
    }
}

void groups_manager::update(token_metadata_ptr new_tm) {
    if (this_shard_id() != 0 || !_features.strongly_consistent_tables) {
        return;
    }

    if (!_started) {
        _pending_tm = new_tm;
        return;
    }

    for (auto& [id, state]: _raft_groups) {
        state.has_tablet = false;
    }

    for_each_sc_tablet(*new_tm, [&](global_tablet_id tablet, raft::group_id id) {
        auto& state = _raft_groups[id];
        state.has_tablet = true;

        // Don't start the raft server if it is already (started or starting) and not stopping.
        if (state.gate && !state.gate->is_closed()) {
            return;
        }

        logger.info("update(): starting raft server for tablet {}, group id {}", tablet, id);
        state.gate = make_lw_shared<gate>();
        state.server_control_op = futurize_invoke([&state, this, tablet, id, new_tm](this auto) -> future<> {
            co_await state.server_control_op.get_future();
            co_await start_raft_group(tablet, id, std::move(new_tm));
            state.server = &_raft_gr.get_server(id);
            state.leader_info_updater = leader_info_updater(state, tablet, id);
            logger.info("update(): raft server for tablet {} and group id {} is started", tablet, id);
        });
    });

    schedule_raft_groups_deletion(false);
}

future<raft_server> groups_manager::acquire_server(raft::group_id group_id) {
    if (this_shard_id() != 0 || !_features.strongly_consistent_tables) {
        on_internal_error(logger, "strongly consistent tables are not enabled on this shard");
    }

    const auto it = _raft_groups.find(group_id);
    if (it == _raft_groups.end()) {
        on_internal_error(logger, format("raft group {} not found", group_id));
    }
    auto& state = it->second;
    return state.server_control_op.get_future().then([&state, h = state.gate->hold()] mutable {
        return raft_server(state, std::move(h));
    });
}

future<> groups_manager::start() {
    if (this_shard_id() != 0) {
        co_return;
    }

    _started = true;

    if (!_features.strongly_consistent_tables) {
        co_return;
    }

    if (_pending_tm) {
        update(std::move(_pending_tm));
        co_await wait_for_groups_to_start();
    }
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
