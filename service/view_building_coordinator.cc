/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <exception>
#include <ranges>
#include <seastar/core/coroutine.hh>
#include <fmt/ranges.h>

#include "cql3/query_processor.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "dht/i_partitioner_fwd.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "message/messaging_service.hh"
#include "mutation/canonical_mutation.hh"
#include "schema/schema_fwd.hh"
#include "seastar/core/loop.hh"
#include "seastar/coroutine/maybe_yield.hh"
#include "seastar/coroutine/parallel_for_each.hh"
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
#include "idl/view.dist.hh"

#include "service/view_building_coordinator.hh"

static logging::logger vbc_logger("vb_coordinator");

namespace service {

namespace vbc {

struct vbc_state {
    vbc_tasks tasks;
    std::optional<table_id> processing_base;
};

class view_building_coordinator : public migration_listener::only_view_notifications {
    replica::database& _db;
    raft_group0& _group0;
    db::system_keyspace& _sys_ks;
    netw::messaging_service& _messaging;
    const topology_state_machine& _topo_sm;
    
    abort_source& _as;
    condition_variable _cond;
    semaphore _rpc_response_mutex = semaphore(1);
    std::map<view_building_target, future<>> _rpc_handlers;

public:
    view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, netw::messaging_service& messaging, const topology_state_machine& topo_sm) 
        : _db(db)
        , _group0(group0)
        , _sys_ks(sys_ks)
        , _messaging(messaging)
        , _topo_sm(topo_sm)
        , _as(as) 
    {}

    future<> run();
    future<> stop();

    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {}
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override { _cond.broadcast(); }

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

    future<vbc_state> load_coordinator_state() {
        auto tasks = co_await _sys_ks.get_view_building_coordinator_tasks();
        auto processing_base = co_await _sys_ks.get_vbc_processing_base();

        co_return vbc_state {
            .tasks = std::move(tasks),
            .processing_base = std::move(processing_base),
        };
    }
    

    future<std::optional<vbc_state>> update_coordinator_state();
    future<> add_view(const view_name& view_name, group0_batch& batch);
    future<> remove_view(const view_name& view_name, group0_batch& batch);

    std::set<view_name> get_views_to_add(const vbc_state& state, const std::vector<view_name>& views, const std::vector<view_name>& built);
    std::set<view_name> get_views_to_remove(const vbc_state& state, const std::vector<view_name>& views);
    
    table_id get_base_id(const view_name& view_name) {
        return _db.find_schema(view_name.first, view_name.second)->view_info()->base_id();
    }
    std::set<locator::host_id> get_active_hosts() {
        std::set<locator::host_id> hosts;
        for (auto& [target, _]: _rpc_handlers) {
            hosts.insert(target.host);
        }
        return hosts;
    }

    future<> build_view(vbc_state state);
    future<> send_task(view_building_target target, table_id base_id, dht::token_range range, std::vector<view_name> views);
    future<> mark_task_completed(view_building_target target, table_id base_id, dht::token_range range, std::vector<view_name> views);
    future<> abort_work(locator::host_id host);
};

future<> view_building_coordinator::run() {
    auto abort = _as.subscribe([this] noexcept {
        _cond.broadcast();
    });

    while (!_as.abort_requested()) {
        vbc_logger.debug("coordinator loop iteration");
        try {
            auto state_opt = co_await update_coordinator_state();
            if (!state_opt) {
                // If state_opt is nullopt, it means there was work to do and the state has changed.
                continue;
            }
            co_await build_view(std::move(*state_opt));
            co_await await_event();
        } catch (...) {
            
        }
        co_await coroutine::maybe_yield();
    }
}

future<std::optional<vbc_state>> view_building_coordinator::update_coordinator_state() {
    vbc_logger.debug("update_coordinator_state()");

    auto guard = co_await start_operation();
    group0_batch batch(std::move(guard));

    auto state = co_await load_coordinator_state();
    auto views = co_await _sys_ks.load_all_views();
    auto built_views = co_await _sys_ks.load_built_views();

    if (auto to_add = get_views_to_add(state, views, built_views); !to_add.empty()) {
        for (auto& view: views) {
            co_await add_view(view, batch);
        }
    } else if (auto to_remove = get_views_to_remove(state, views); !to_remove.empty()) {
        for (auto& view: views) {
            co_await remove_view(view, batch);

            if (state.processing_base && *state.processing_base == get_base_id(view)) {
                auto mut = co_await _sys_ks.make_vbc_delete_processing_base_mutation(batch.write_timestamp());
                batch.add_mutation(std::move(mut));
            }
        }
    } else if (!state.processing_base && !state.tasks.empty()) {
        // select base table to process
        auto& base_id = state.tasks.cbegin()->first;
        vbc_logger.info("Start building views for base table: {}", base_id);

        auto mut = co_await _sys_ks.make_vbc_processing_base_mutation(batch.write_timestamp(), base_id);
        batch.add_mutation(std::move(mut));
    }

    if (!batch.empty()) {
        co_await std::move(batch).commit(_group0.client(), _as, std::nullopt); //TODO: specify timeout?
        co_return std::nullopt;
    }
    co_return state;
}

static std::pair<std::vector<view_name>, dht::token_range> get_views_and_range_for_target(const base_tasks& base_tasks, const view_building_target& target) {
    std::vector<view_name> views;
    std::optional<dht::token_range> range;

    for (auto& [view, tasks]: base_tasks) {
        if (!range) {
            range = tasks.at(target).front();
            views.push_back(view);
        } else {
            auto& target_tasks = tasks.at(target);
            if (std::find(target_tasks.cbegin(), target_tasks.cend(), *range) != target_tasks.cend()) {
                views.push_back(view);
            }
        }
    }

    if (!range) {
        return {{}, dht::token_range()};
    }
    return {std::move(views), *range};
}

future<> view_building_coordinator::build_view(vbc_state state) {
    if (!state.processing_base) {
        vbc_logger.info("No view to process");
        co_return;
    }

    SCYLLA_ASSERT(state.tasks.contains(*state.processing_base));
    auto& base_tasks = state.tasks[*state.processing_base];

    for (auto& [id, replica_state]: _topo_sm._topology.normal_nodes) {
        locator::host_id host_id{id.uuid()};

        for (size_t shard = 0; shard < replica_state.shard_count; ++shard) {
            view_building_target target{host_id, shard};
            if (_rpc_handlers.contains(target) && !_rpc_handlers.at(target).available()) {
                continue;
            }
            
            auto [views, range] = get_views_and_range_for_target(base_tasks, target);
            if (views.empty()) {
                continue;
            }

            if (_rpc_handlers.contains(target)) {
                co_await std::move(_rpc_handlers.extract(target).mapped());
            }
            future<> rpc = send_task(target, *state.processing_base, range, std::move(views));
            _rpc_handlers.insert({target, std::move(rpc)});
        }
    }
}

future<> view_building_coordinator::send_task(view_building_target target, table_id base_id, dht::token_range range, std::vector<view_name> views) {
    vbc_logger.info("Sending view building task to node {}, shard {} (token range: {} | views: {})", target.host, target.shard, range, views);
    auto name_to_id = [this] (const view_name& view_name) -> table_id {
        return _db.find_uuid(view_name.first, view_name.second);
    };
    std::vector<table_id> views_ids = views | std::views::transform(name_to_id) | std::ranges::to<std::vector>();

    try {
        co_await ser::view_rpc_verbs::send_build_views_request(&_messaging, target.host, _as, base_id, target.shard, range, std::move(views_ids));
    } catch (...) {
        vbc_logger.warn("Building views for base: {}, range: {} on node: {}, shard: {} failed: {}", base_id, range, target.host, target.shard, std::current_exception());
        _cond.broadcast();
        co_return;
    }

    co_await mark_task_completed(target, base_id, range, std::move(views));
    _cond.broadcast();
}

future<> view_building_coordinator::mark_task_completed(view_building_target target, table_id base_id, dht::token_range range, std::vector<view_name> views) {
    auto lock = get_units(_rpc_response_mutex, 1, _as);
    auto guard = co_await _group0.client().start_operation(_as);
    auto state = co_await load_coordinator_state();

    std::vector<canonical_mutation> muts;
    auto& base_tasks = state.tasks[base_id];
    for (auto& view: views) {
        // Mark token_range as completed (remove it from vb state)
        auto mut = co_await _sys_ks.make_vbc_task_done_mutation(guard.write_timestamp(), view, target.host, target.shard, range);
        muts.emplace_back(std::move(mut));

        auto& ranges = base_tasks[view][target];
        ranges.erase(std::remove(ranges.begin(), ranges.end(), range), ranges.end());
        if (ranges.empty()) {
            base_tasks[view].erase(target);
        }
        vbc_logger.info("Token range {} (view: {}.{} | base_id: {}) was built on node {}, shard {}", range, view.first, view.second, base_id, target.host, target.shard);

        // Mark view as built if all tasks were completed
        if (base_tasks[view].empty()) {
            auto mut = co_await _sys_ks.make_tablet_view_built_mutation(guard.write_timestamp(), view);
            muts.emplace_back(std::move(mut));

            base_tasks.erase(view);
            vbc_logger.info("View {}.{} was built", view.first, view.second);
        }
    }

    // Unset currently processing base if all views were built
    if (base_tasks.empty()) {
        auto mut = co_await _sys_ks.make_vbc_delete_processing_base_mutation(guard.write_timestamp());
        muts.emplace_back(std::move(mut));
        vbc_logger.info("All views for base {} were built", base_id);
    }

    auto cmd = _group0.client().prepare_command(write_mutations{.mutations = std::move(muts)}, guard, "finished view building step");
    co_await _group0.client().add_entry(std::move(cmd), std::move(guard), _as);
}

future<> view_building_coordinator::abort_work(locator::host_id host) {
    return ser::view_rpc_verbs::send_abort_vbc_work(&_messaging, host);
}

std::set<view_name> view_building_coordinator::get_views_to_add(const vbc_state& state, const std::vector<view_name>& views, const std::vector<view_name>& built) {
    std::set<view_name> views_to_add;
    for (auto& view: views) {
        if (!_db.find_keyspace(view.first).uses_tablets() || std::find(built.begin(), built.end(), view) != built.end()) {
            continue;
        }

        auto base_id = get_base_id(view);
        if (!state.tasks.contains(base_id) || !state.tasks.at(base_id).contains(view)) {
            views_to_add.insert(view);
        }
    }
    return views_to_add;
}

std::set<view_name> view_building_coordinator::get_views_to_remove(const vbc_state& state, const std::vector<view_name>& views) {
    std::set<view_name> views_to_remove;
    for (auto& [_, view_tasks]: state.tasks) {
        for (auto& [view, _]: view_tasks) {
            if (std::find(views.begin(), views.end(), view) == views.end()) {
                views_to_remove.insert(view);
            }
        }
    }
    return views_to_remove;
}

future<> view_building_coordinator::add_view(const view_name& view_name, group0_batch& batch) {
    vbc_logger.info("Register new view: {}.{}", view_name.first, view_name.second);

    auto base_id = get_base_id(view_name);
    auto& base_cf = _db.find_column_family(base_id);
    auto erm = base_cf.get_effective_replication_map();
    auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(base_id);

    for (auto tid = std::optional(tablet_map.first_tablet()); tid; tid = tablet_map.next_tablet(*tid)) {
        const auto& tablet_info = tablet_map.get_tablet_info(*tid);
        auto range = tablet_map.get_token_range(*tid);

        for (auto& replica: tablet_info.replicas) {
            auto mut = co_await _sys_ks.make_vbc_task_mutation(batch.write_timestamp(), view_name, replica.host, replica.shard, range);
            batch.add_mutation(std::move(mut));
        }
    }
}

future<> view_building_coordinator::remove_view(const view_name& view_name, group0_batch& batch) {
    vbc_logger.info("Unregister all remaining tasks for view: {}.{}", view_name.first, view_name.second);
    
    auto muts = co_await _sys_ks.make_vbc_remove_view_tasks_mutations(batch.write_timestamp(), view_name);
    batch.add_mutations(std::move(muts));
}

future<> view_building_coordinator::stop() {
    _as.request_abort();
    co_await coroutine::parallel_for_each(get_active_hosts(), [this] (auto host) -> future<> {
        co_await abort_work(host);
    });
    co_await coroutine::parallel_for_each(std::move(_rpc_handlers), [] (auto&& rpc_call) -> future<> {
        co_await std::move(rpc_call.second);
    });
}

future<> run_view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, netw::messaging_service& messaging, const topology_state_machine& topo_sm) {
    view_building_coordinator vb_coordinator{as, db, group0, sys_ks, messaging, topo_sm};

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
    co_await vb_coordinator.stop();

    co_return;
}

}

}