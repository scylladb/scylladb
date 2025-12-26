/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sc_groups_manager.hh"

#include "service/strong_consistency/sc_state_machine.hh"
#include "gms/feature_service.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/storage_proxy.hh"
#include "replica/database.hh"
#include "db/config.hh"

namespace service {

using namespace locator;

static logging::logger logger("sc_groups_manager");

static raft::server_id to_server_id(host_id host_id) {
    return raft::server_id{host_id.uuid()};
};

class sc_groups_manager::rpc_impl: public service::raft_rpc {
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

sc_raft_server::sc_raft_server(sc_groups_manager::raft_group_state& state, gate::holder holder)
    : _state(state)
    , _holder(std::move(holder))
{
}

auto sc_raft_server::begin_mutate() -> future<begin_mutate_result> {
    const auto current_term = _state.server->get_current_term();
    if (_state.leader_state && _state.leader_state->term == current_term) {
        const auto new_ts = std::max(api::new_timestamp(), _state.leader_state->last_timestamp + 1);
        _state.leader_state->last_timestamp = new_ts;
        return make_ready_future<begin_mutate_result>(_state.leader_state->term, new_ts);
    }
    if (_state.server->current_leader() && !_state.server->is_leader()) {
        return make_ready_future<begin_mutate_result>(current_term, std::nullopt);
    }
    return _state.leader_state_cond.wait().then([this]{ return begin_mutate(); });
}

sc_groups_manager::sc_groups_manager(netw::messaging_service& ms, 
        raft_group_registry& raft_gr, cql3::query_processor& qp,
        replica::database& db, gms::feature_service& features)
    : _ms(ms)
    , _raft_gr(raft_gr)
    , _qp(qp)
    , _db(db)
    , _features(features)
{
}

future<> sc_groups_manager::start_raft_group(global_tablet_id tablet,
        raft::group_id group_id,
        token_metadata_ptr tm)
{
    const auto my_id = to_server_id(tm->get_my_id());

    auto state_machine = make_sc_state_machine(tablet, group_id, _db);
    auto rpc = std::make_unique<rpc_impl>(*state_machine, _ms, _raft_gr.failure_detector(), group_id, my_id);
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

    // // initialize the corresponding timer to tick the raft server instance
    auto ticker = std::make_unique<raft_ticker_type>([srv = server.get()] { srv->tick(); });
    co_await _raft_gr.start_server_for_group(raft_server_for_group {
        .gid = group_id,
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
        .persistence = persistence_ref,
        .default_op_timeout_in_ms = _qp.proxy().get_db().local().get_config().group0_raft_op_timeout_in_ms
    });
}

void sc_groups_manager::schedule_raft_group_deletion(raft::group_id id, raft_group_state& state) {
    if (state.gate->is_closed()) {
        return;
    }
    logger.info("schedule_raft_group_deletion(): group id {}", id);
    state.server_op = when_all_succeed(state.server_op.get_future(), state.gate->close()).discard_result()
        .then([this, &state, id] {
            state.server = nullptr;
            return when_all_succeed(_raft_gr.abort_server(id), std::move(state.leader_state_fiber)).discard_result();
        })
        .then([this, id, &state, g = state.gate] {
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

void sc_groups_manager::schedule_raft_groups_deletion(bool all) {
    for (auto it = _raft_groups.begin(); it != _raft_groups.end(); ) {
        const auto next = std::next(it);
        auto& [group_id, group_state] = *it;
        if (all || !group_state.has_tablet) {
            schedule_raft_group_deletion(group_id, group_state);
        }
        it = next;
    }
}

future<> sc_groups_manager::leader_state_fiber(raft_group_state& state, global_tablet_id tablet, raft::group_id gid) {
    try {
        auto schema = _db.find_schema(tablet.table);
        while (true) {
            const auto current_term = state.server->get_current_term();
            const auto is_leader = state.server->is_leader();
            if (is_leader && (!state.leader_state || state.leader_state->term != current_term)) {
                logger.debug("leader_state_fiber({}-{}): leader state term {}, current term {}, running read_barrier()",
                    tablet, gid,
                    state.leader_state ? state.leader_state->term : raft::term_t(0),
                    current_term);
                co_await state.server->read_barrier(nullptr);
                if (state.server->get_current_term() != current_term) {
                    continue;
                }

                state.leader_state = leader_state {
                    .term = current_term,
                    .last_timestamp = schema->table().get_max_timestamp_for_tablet(tablet.tablet)
                };
                logger.debug("leader_state_fiber({}-{}): read_barrier() completed, "
                    "new leader_state term {}, last_timestamp {}",
                    tablet, gid,
                    state.leader_state->term,
                    state.leader_state->last_timestamp);

                state.leader_state_cond.broadcast();
            }

            const auto current_leader = state.server->current_leader();
            if (!is_leader && current_leader) {
                logger.debug("leader_state_fiber({}-{}): current leader {}", 
                    tablet, gid, current_leader);
                state.leader_state_cond.broadcast();
            }

            logger.debug("leader_state_fiber({}-{}): waiting for state change, term {}",
                tablet, gid, current_term);
            co_await state.server->wait_for_state_change(nullptr);
        }
    } catch (const raft::request_aborted&) {
        // thrown from read_barrier() and wait_for_state_change() when then tablet leaves this shard
    } catch (const raft::stopped_error&) {
        // thrown from read_barrier() and wait_for_state_change() when then tablet leaves this shard
    } catch (const replica::no_such_column_family&) {
        // thrown from find_schema() and schema->table() when the table is dropped
    }
}

future<> sc_groups_manager::wait_for_groups_to_start() {
    while (true) {
        const auto it = std::ranges::find_if(_raft_groups, [](const auto& p) {
            auto& state = p.second;
            return !state.server && !state.gate->is_closed();
        });
        if (it == _raft_groups.end()) {
            break;
        }

        const auto& [id, state] = *it;
        logger.info("waiting for group {} to start", id);
        co_await state.server_op.get_future();
    }
}

void sc_groups_manager::update(token_metadata_ptr new_tm) {
    if (this_shard_id() != 0) {
        return;
    }

    if (!_started || !_features.strongly_consistent_tables) {
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
        state.server_op = state.server_op.get_future()
            .then([this, tablet, id, new_tm] mutable {
                return start_raft_group(tablet, id, std::move(new_tm));
            })
            .then([this, &state, tablet, id] {
                state.server = &_raft_gr.get_server(id);
                state.leader_state_fiber = leader_state_fiber(state, tablet, id);
                logger.info("update(): raft server for tablet {} and group id {} is started", tablet, id);
            });
    });

    schedule_raft_groups_deletion(false);
}

future<sc_raft_server> sc_groups_manager::acquire_server(raft::group_id group_id) {
    if (this_shard_id() != 0 || !_features.strongly_consistent_tables) {
        on_internal_error(logger, "strongly consistent tables are not enabled on this shard");
    }

    const auto it = _raft_groups.find(group_id);
    if (it == _raft_groups.end()) {
        on_internal_error(logger, format("raft group {} not found", group_id));
    }
    auto& state = it->second;
    auto holder = state.gate->hold();
    auto f = make_ready_future<>();
    if (!state.server) {
        f = state.server_op.get_future();
    }
    return f.then([&state, h = std::move(holder)] mutable {
        return sc_raft_server(state, std::move(h));
    });
}
future<> sc_groups_manager::start() {
    if (this_shard_id() != 0) {
        co_return;
    }

    _started = true;

    if (!_features.strongly_consistent_tables) {
        _feature_listener = _features.strongly_consistent_tables.when_enabled([this] {
            if (_pending_tm) {
                update(std::move(_pending_tm));
            }
        });
        co_return;
    }

    if (_pending_tm) {
        update(std::move(_pending_tm));
        co_await wait_for_groups_to_start();
    }
}

future<> sc_groups_manager::stop() {
    if (!_started) {
        co_return;
    }

    logger.info("stop() enter");

    schedule_raft_groups_deletion(true);

    while (!_raft_groups.empty()) {
        co_await _raft_groups.begin()->second.server_op.get_future();
    }

    logger.info("stop() completed");
}
}
