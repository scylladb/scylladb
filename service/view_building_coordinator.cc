/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <exception>
#include <iterator>
#include <ranges>
#include <seastar/core/coroutine.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "schema/schema_fwd.hh"
#include "seastar/core/loop.hh"
#include "seastar/coroutine/maybe_yield.hh"
#include "service/coordinator_event_subscriber.hh"
#include "service/query_state.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/assert.hh"
#include "utils/log.hh"
#include "seastar/core/abort_source.hh"
#include "seastar/core/with_scheduling_group.hh"
#include "service/migration_manager.hh"
#include "replica/database.hh"
#include "view_info.hh"

#include "service/view_building_coordinator.hh"

logging::logger vbc_logger("vb_coordinator");

namespace service {

using view_name = db::system_keyspace_view_name;
using vbc_tasks = db::system_keyspace_vbc_tasks;
using vbc_view_tasks = vbc_tasks::mapped_type;

struct view_building_state {
    vbc_tasks build_tasks;
};

query_state& vb_coordinator_query_state() {
    using namespace std::chrono_literals;
    const auto t = 1h;
    static timeout_config tc{ t, t, t, t, t, t, t };
    static thread_local client_state cs(client_state::internal_tag{}, tc);
    static thread_local query_state qs(cs, empty_service_permit());
    return qs;
}

class view_building_coordinator : public migration_listener::only_view_notifications, public coordinator_event_subscriber {
    replica::database& _db;
    raft_group0& _group0;
    db::system_keyspace& _sys_ks;
    const topology_state_machine& _topo_sm;
    
    abort_source& _as;
    condition_variable _cond;
    std::optional<view_building_state> _vb_state;

public:
    view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, const topology_state_machine& topo_sm) 
        : _db(db)
        , _group0(group0)
        , _sys_ks(sys_ks)
        , _topo_sm(topo_sm)
        , _as(as) 
        , _vb_state(std::nullopt)
    {}

    future<> run();

    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {}
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }

    virtual void on_tablet_migration_start() override {}
    virtual void on_tablet_migration_finish() override {}
    virtual void on_tablet_migration_abort() override {}

private:
    future<group0_guard> start_operation() {
        auto guard = co_await _group0.client().start_operation(_as);
        co_return std::move(guard);
    }

    future<> await_event() {
        _as.check();
        co_await _cond.when();
        vbc_logger.debug("event awaited");
    }

    future<> initialize_coordinator_state() {
        auto tasks = co_await _sys_ks.get_view_building_coordinator_tasks();
        _vb_state = view_building_state {
            .build_tasks = std::move(tasks)
        };
    }

    future<> update_coordinator_state(group0_guard guard);

    future<> add_view(const view_name& view_name, view_building_state& state_copy, group0_batch& batch);
    future<> remove_view(const view_name& view_name, view_building_state& state_copy, group0_batch& batch);

private:
    future<std::set<view_name>> load_all_views() {
        static const sstring query = format("SELECT keyspace_name, view_name FROM {}.{}", db::schema_tables::v3::NAME, db::schema_tables::v3::VIEWS);

        std::set<view_name> views;
        co_await _sys_ks.query_processor().query_internal(query, [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            auto ks_name = row.get_as<sstring>("keyspace_name");
            auto view_name = row.get_as<sstring>("view_name");

            views.insert({ks_name, view_name});
            co_return stop_iteration::no;
        });
        co_return views;
    }

    future<std::set<view_name>> load_built_views() {
        auto built_views = co_await _sys_ks.load_built_views();
        co_return std::set<view_name>(std::make_move_iterator(built_views.begin()), std::make_move_iterator(built_views.end()));
    }
};

future<> view_building_coordinator::run() {
    auto abort = _as.subscribe([this] noexcept {
        _cond.broadcast();
    });

    co_await initialize_coordinator_state();

    while (!_as.abort_requested()) {
        vbc_logger.debug("coordinator loop iteration");
        try {
            co_await update_coordinator_state(co_await start_operation());

            // TODO
            // Do actual work, send RPCs to build a particular view's range
            co_await await_event();
        } catch (...) {
            
        }
        co_await coroutine::maybe_yield();
    }
}

future<> view_building_coordinator::update_coordinator_state(group0_guard guard) {
    SCYLLA_ASSERT(_vb_state);
    vbc_logger.debug("update_coordinator_state()");

    auto views = co_await load_all_views();
    auto built_views = co_await load_built_views();

    view_building_state state_copy = *_vb_state;
    group0_batch batch(std::move(guard));

    for (auto& view: views) {
        if (!_db.find_keyspace(view.first).uses_tablets() || built_views.contains(view)) {
            continue;
        }

        if (!_vb_state->build_tasks.contains(view)) {
            co_await add_view(view, state_copy, batch);
        }
    }

    for (auto& [view, _]: _vb_state->build_tasks) {
        if (!views.contains(view)) {
            co_await remove_view(view, state_copy, batch);
        }
    }

    if (!batch.empty()) {
        co_await std::move(batch).commit(_group0.client(), _as, std::nullopt); //TODO: specify timeout?
        _vb_state = std::move(state_copy);
    }
}

future<> view_building_coordinator::add_view(const view_name& view_name, view_building_state& state_copy, group0_batch& batch) {
    vbc_logger.info("Schedule view building for {}.{}", view_name.first, view_name.second);
    static const sstring query = format("INSERT INTO {}.{}(keyspace_name, view_name, host_id, shard, start_token, end_token) VALUES (?, ?, ?, ?, ?, ?)", db::system_keyspace::NAME, db::system_keyspace::VIEW_BUILDING_COORDINATOR_TASKS);

    std::vector<std::vector<data_value_or_unset>> rows;
    vbc_view_tasks task;
    for (auto& [id, state]: _topo_sm._topology.normal_nodes) {
        auto host_id = locator::host_id{id.uuid()};
        // TODO: This can be improved to assign only ranges which are actually owned by the specific shard
        auto min_token = dht::token::minimum();
        auto max_token = dht::token::maximum();

        auto host_rows = std::views::iota(size_t(0), state.shard_count) | std::views::transform([&] (const size_t& shard) {
            auto range = dht::token_range::make(min_token, max_token);
            task.insert({{host_id, shard}, {range}});

            return std::vector<data_value_or_unset>{
                data_value(view_name.first), data_value(view_name.second), 
                data_value(host_id.uuid()), data_value(int32_t(shard)),
                data_value(min_token.data()), data_value(max_token.data())
            };
        }) | std::ranges::to<std::vector>();
        rows.insert(rows.end(), std::make_move_iterator(host_rows.begin()), std::make_move_iterator(host_rows.end()));
        co_await coroutine::maybe_yield();
    }

    for (auto& row: rows) {
        auto muts = co_await _sys_ks.query_processor().get_mutations_internal(
            query, 
            vb_coordinator_query_state(), 
            batch.write_timestamp(), 
            row);
        batch.add_mutations(std::move(muts));
    }
    state_copy.build_tasks.insert({view_name, std::move(task)});
}

future<> view_building_coordinator::remove_view(const view_name& view_name, view_building_state& state_copy, group0_batch& batch) {
    vbc_logger.info("Remove all remaining tasks for {}.{}", view_name.first, view_name.second);
    static const sstring query = format("DELETE FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", db::system_keyspace::NAME, db::system_keyspace::VIEW_BUILDING_COORDINATOR_TASKS);

    auto mutations = co_await _sys_ks.query_processor().get_mutations_internal(
        query, 
        vb_coordinator_query_state(),
        batch.write_timestamp(),
        {view_name.first, view_name.second});
    
    batch.add_mutations(std::move(mutations));
    state_copy.build_tasks.erase(view_name);
}

future<> run_view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, const topology_state_machine& topo_sm) {
    view_building_coordinator vb_coordinator{as, db, group0, sys_ks, topo_sm};

    std::exception_ptr ex;
    db.get_notifier().register_listener(&vb_coordinator);
    try {
        co_await with_scheduling_group(group0.get_scheduling_group(), [&] {
            return vb_coordinator.run();
        });
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        on_fatal_internal_error(vbc_logger, format("unhandled exception in view_building_coordinator::run(): {}", ex));
    }

    co_await db.get_notifier().unregister_listener(&vb_coordinator);
}

}