/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "locator/tablets.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "service/task_manager_module.hh"
#include "tasks/task_handler.hh"
#include "tasks/virtual_task_hint.hh"
#include <seastar/coroutine/maybe_yield.hh>

namespace service {

struct status_helper {
    tasks::task_status status;
    utils::chunked_vector<locator::tablet_id> tablets;
    std::optional<locator::tablet_replica> pending_replica;
};

tasks::task_manager::task_group tablet_virtual_task::get_group() const noexcept {
    return tasks::task_manager::task_group::tablets_group;
}

static std::optional<locator::tablet_task_type> maybe_get_task_type(const locator::tablet_task_info& task_info, tasks::task_id task_id) {
    return task_info.is_valid() && task_info.tablet_task_id.uuid() == task_id.uuid() ? std::make_optional(task_info.request_type) : std::nullopt;
}

static sstring get_scope(locator::tablet_task_type task_type) {
    switch (task_type) {
        case locator::tablet_task_type::user_repair:
        case locator::tablet_task_type::split:
        case locator::tablet_task_type::merge:
            return "table";
        case locator::tablet_task_type::auto_repair:
        case locator::tablet_task_type::migration:
        case locator::tablet_task_type::intranode_migration:
            return "tablet";
        case locator::tablet_task_type::none:
            on_internal_error(tasks::tmlogger, "attempted to get the scope for none task type");
    }
}

static std::optional<tasks::task_stats> maybe_make_task_stats(const locator::tablet_task_info& task_info, schema_ptr schema) {
    if (!task_info.is_valid()) {
        return std::nullopt;
    }

    return tasks::task_stats{
        .task_id = tasks::task_id{task_info.tablet_task_id.uuid()},
        .type = locator::tablet_task_type_to_string(task_info.request_type),
        .kind = tasks::task_kind::cluster,
        .scope = get_scope(task_info.request_type),
        .state = tasks::task_manager::task_state::running,
        .keyspace = schema->ks_name(),
        .table = schema->cf_name(),
        .start_time = task_info.request_time
    };
}

static bool is_repair_task(const locator::tablet_task_type& task_type) {
    return task_type == locator::tablet_task_type::user_repair || task_type == locator::tablet_task_type::auto_repair;
}

static bool is_migration_task(const locator::tablet_task_type& task_type) {
    return task_type == locator::tablet_task_type::migration || task_type == locator::tablet_task_type::intranode_migration;
}

static bool is_resize_task(const locator::tablet_task_type& task_type) {
    return task_type == locator::tablet_task_type::split || task_type == locator::tablet_task_type::merge;
}

static bool tablet_id_provided(const locator::tablet_task_type& task_type) {
    return is_migration_task(task_type);
}

// iterator over all valid migration tasks
// returns tuples of (table_id, tablet_id, task_info)
auto migration_tasks(locator::token_metadata_ptr tmptr) {
    // migration tasks are shared for co-located tables, therefore we iterate over table groups
    // and return the base table as a representative of its group.
    auto tables = tmptr->tablets().all_table_groups()
        | std::views::transform([] (const auto& g) { return g.first; });

    auto tasks_view = tables
        | std::views::transform([&](table_id table) {
            auto& tmap = tmptr->tablets().get_tablet_map_view(table);
            return tmap.tablets()
                | std::views::filter([](const auto& pair) {
                    return pair.second.migration_task_info().is_valid();
                })
                | std::views::transform([table](const auto& pair) {
                    return std::make_tuple(table, pair.first, pair.second.migration_task_info());
                });
        })
        | std::views::join;

    return tasks_view;
}

// iterator over all valid repair tasks
// returns tuples of (table_id, tablet_id, task_info)
auto repair_tasks(locator::token_metadata_ptr tmptr) {
    // repair tasks are not shared for co-located tables. each table has its own repair task info,
    // even if it's co-located with other tables. therefore we iterate over all tables, whether base
    // tables or co-located tables.
    auto& tables = tmptr->tablets().all_tables_ungrouped();
    auto tasks_view = tables
        | std::views::transform([](const auto& table_pair) {
            table_id table = table_pair.first;
            const auto& tmap_view = table_pair.second;
            return tmap_view.tablets()
                | std::views::filter([](const auto& pair) {
                    return pair.second.repair_task_info().is_valid();
                })
                | std::views::transform([table](const auto& pair) {
                    return std::make_tuple(table, pair.first, pair.second.repair_task_info());
                });
        })
        | std::views::join;

    return tasks_view;
}

// iterator over all valid resize tasks
// returns tuples of (table_id, task_info)
auto resize_tasks(locator::token_metadata_ptr tmptr) {
    // resize tasks are shared for co-located tables, therefore we iterate over table groups.
    auto table_groups = tmptr->tablets().all_table_groups()
        | std::views::filter([&tmptr](const auto& group_pair) {
            auto& tmap_view = tmptr->tablets().get_tablet_map_view(group_pair.first);
            return tmap_view.resize_task_info().is_valid();
        })
        | std::views::transform([&tmptr](const auto& group_pair) {
            auto& tmap_view = tmptr->tablets().get_tablet_map_view(group_pair.first);
            return std::make_tuple(group_pair.first, tmap_view.resize_task_info());
        });

    return table_groups;
}

future<std::optional<tasks::virtual_task_hint>> tablet_virtual_task::contains(tasks::task_id task_id) const {
    co_await _ss._migration_manager.local().get_group0_barrier().trigger();

    auto tmptr = _ss.get_token_metadata_ptr();

    for (const auto& [table, task_info] : resize_tasks(tmptr)) {
        if (auto task_type = maybe_get_task_type(task_info, task_id); task_type.has_value()) {
            co_return tasks::virtual_task_hint{
                .table_id = table,
                .task_type = task_type.value(),
                .tablet_id = std::nullopt,
            };
        }
    }

    for (const auto& [table, tid, task_info] : migration_tasks(tmptr)) {
        if (auto task_type = maybe_get_task_type(task_info, task_id); task_type.has_value()) {
            co_return tasks::virtual_task_hint{
                .table_id = table,
                .task_type = task_type.value(),
                .tablet_id = tablet_id_provided(task_type.value()) ? std::make_optional(tid) : std::nullopt,
            };
        }
    }

    for (const auto& [table, tid, task_info] : repair_tasks(tmptr)) {
        if (auto task_type = maybe_get_task_type(task_info, task_id); task_type.has_value()) {
            co_return tasks::virtual_task_hint{
                .table_id = table,
                .task_type = task_type.value(),
                .tablet_id = tablet_id_provided(task_type.value()) ? std::make_optional(tid) : std::nullopt,
            };
        }
    }

    co_return std::nullopt;
}

future<tasks::is_abortable> tablet_virtual_task::is_abortable(tasks::virtual_task_hint hint) const {
    auto task_type = hint.get_task_type();
    return make_ready_future<tasks::is_abortable>(is_repair_task(task_type));
}

future<std::optional<tasks::task_status>> tablet_virtual_task::get_status(tasks::task_id id, tasks::virtual_task_hint hint) {
    auto res = co_await get_status_helper(id, std::move(hint));
    co_return res.transform([] (status_helper s) {
        return s.status;
    });
}

future<std::optional<tasks::task_status>> tablet_virtual_task::wait(tasks::task_id id, tasks::virtual_task_hint hint) {
    auto table = hint.get_table_id();
    auto task_type = hint.get_task_type();
    auto tablet_id_opt = tablet_id_provided(task_type) ? std::make_optional(hint.get_tablet_id()) : std::nullopt;

    size_t tablet_count = _ss.get_token_metadata().tablets().get_tablet_map(table).tablet_count();
    auto res = co_await get_status_helper(id, std::move(hint));
    if (!res) {
        co_return std::nullopt;
    }

    tasks::tmlogger.info("tablet_virtual_task: wait until tablet operation is finished");
    co_await _ss._topology_state_machine.event.wait([&] {
        auto& tmap = _ss.get_token_metadata().tablets().get_tablet_map_view(table);
        if (is_resize_task(task_type)) {    // Resize task.
            return tmap.resize_task_info().tablet_task_id.uuid() != id.uuid();
        } else if (tablet_id_opt.has_value()) {    // Migration task.
            return tmap.get_tablet_info(tablet_id_opt.value()).migration_task_info().tablet_task_id.uuid() != id.uuid();
        } else {    // Repair task.
            return std::all_of(res->tablets.begin(), res->tablets.end(), [&] (const locator::tablet_id& tablet) {
                return tmap.get_tablet_info(tablet).repair_task_info().tablet_task_id.uuid() != id.uuid();
            });
        }
    });

    res->status.state = tasks::task_manager::task_state::done; // Failed repair task is retried.
    if (is_migration_task(task_type)) {
        auto& replicas = _ss.get_token_metadata().tablets().get_tablet_map(table).get_tablet_info(tablet_id_opt.value()).replicas;
        auto migration_failed = std::all_of(replicas.begin(), replicas.end(), [&] (const auto& replica) { return res->pending_replica.has_value() && replica != res->pending_replica.value(); });
        res->status.state = migration_failed ? tasks::task_manager::task_state::failed : tasks::task_manager::task_state::done;
    } else if (is_resize_task(task_type)) {
        auto new_tablet_count = _ss.get_token_metadata().tablets().get_tablet_map(table).tablet_count();
        res->status.state = new_tablet_count == tablet_count ? tasks::task_manager::task_state::suspended : tasks::task_manager::task_state::done;
        res->status.children = task_type == locator::tablet_task_type::split ? co_await get_children(get_module(), id, std::bind_front(&gms::gossiper::is_alive, &_ss.gossiper())) : utils::chunked_vector<tasks::task_identity>{};
    } else {
        res->status.children = co_await get_children(get_module(), id, std::bind_front(&gms::gossiper::is_alive, &_ss.gossiper()));
    }
    res->status.end_time = db_clock::now(); // FIXME: Get precise end time.
    co_return res->status;
}

future<> tablet_virtual_task::abort(tasks::task_id id, tasks::virtual_task_hint hint) noexcept {
    auto table = hint.get_table_id();
    auto task_type = hint.get_task_type();
    if (!is_repair_task(task_type)) {
        on_internal_error(tasks::tmlogger, format("non-abortable task {} of type {} cannot be aborted", id, task_type));
    }
    co_await _ss.del_repair_tablet_request(table, locator::tablet_task_id{id.uuid()});
}

future<std::vector<tasks::task_stats>> tablet_virtual_task::get_stats() {
    std::vector<tasks::task_stats> res;

    auto tmptr = _ss.get_token_metadata_ptr();

    for (const auto& [table, task_info] : resize_tasks(tmptr)) {
        auto schema = _ss._db.local().get_tables_metadata().get_table(table).schema();
        auto resize_stats = maybe_make_task_stats(task_info, schema);
        if (resize_stats) {
            res.push_back(std::move(resize_stats.value()));
        }
    }

    for (const auto& [table, tablet, task_info] : migration_tasks(tmptr)) {
        auto schema = _ss._db.local().get_tables_metadata().get_table(table).schema();
        auto migration_stats = maybe_make_task_stats(task_info, schema);
        if (migration_stats) {
            res.push_back(std::move(migration_stats.value()));
        }
    }

    std::unordered_map<tasks::task_id, tasks::task_stats> user_requests;
    std::unordered_map<tasks::task_id, size_t> sched_num_sum;

    for (const auto& [table, tablet, task_info] : repair_tasks(tmptr)) {
        auto schema = _ss._db.local().get_tables_metadata().get_table(table).schema();
        auto repair_stats = maybe_make_task_stats(task_info, schema);
        if (repair_stats) {
            if (task_info.is_user_repair_request()) {
                // User requested repair may encompass more that one tablet.
                auto task_id = tasks::task_id{task_info.tablet_task_id.uuid()};
                user_requests[task_id] = std::move(repair_stats.value());
                sched_num_sum[task_id] += task_info.sched_nr;
            } else {
                res.push_back(std::move(repair_stats.value()));
            }
        }
    }

    for (auto& [id, task_stats] : user_requests) {
        task_stats.state = sched_num_sum[id] == 0 ? tasks::task_manager::task_state::created : tasks::task_manager::task_state::running;
        res.push_back(std::move(task_stats));
    }

    // FIXME: Show finished tasks.
    co_return res;
}

static void update_status(const locator::tablet_task_info& task_info, tasks::task_status& status, size_t& sched_nr) {
    sched_nr += task_info.sched_nr;
    status.type = locator::tablet_task_type_to_string(task_info.request_type);
    status.scope = get_scope(task_info.request_type);
    status.start_time = task_info.request_time;
}

future<std::optional<status_helper>> tablet_virtual_task::get_status_helper(tasks::task_id id, tasks::virtual_task_hint hint) {
    status_helper res;
    auto table = hint.get_table_id();
    auto task_type = hint.get_task_type();
    auto schema = _ss._db.local().get_tables_metadata().get_table(table).schema();
    res.status = {
        .task_id = id,
        .kind = tasks::task_kind::cluster,
        .is_abortable = co_await is_abortable(std::move(hint)),
        .keyspace = schema->ks_name(),
        .table = schema->cf_name(),
    };
    size_t sched_nr = 0;
    auto tmptr = _ss.get_token_metadata_ptr();
    auto& tmap = tmptr->tablets().get_tablet_map_view(table);
    if (is_repair_task(task_type)) {
        co_await tmap.for_each_tablet([&] (locator::tablet_id tid, const locator::tablet_info_view& info) {
            auto& task_info = info.repair_task_info();
            if (task_info.tablet_task_id.uuid() == id.uuid()) {
                update_status(task_info, res.status, sched_nr);
                res.tablets.push_back(tid);
            }
            return make_ready_future();
        });
        res.status.children = co_await get_children(get_module(), id, std::bind_front(&gms::gossiper::is_alive, &_ss.gossiper()));
    } else if (is_migration_task(task_type)) {    // Migration task.
        auto tablet_id = hint.get_tablet_id();
        res.pending_replica = tmap.get_tablet_transition_info(tablet_id)->pending_replica;
        auto& task_info = tmap.get_tablet_info(tablet_id).migration_task_info();
        if (task_info.tablet_task_id.uuid() == id.uuid()) {
            update_status(task_info, res.status, sched_nr);
            res.tablets.push_back(tablet_id);
        }
    } else {    // Resize task.
        auto& task_info = tmap.resize_task_info();
        if (task_info.tablet_task_id.uuid() == id.uuid()) {
            update_status(task_info, res.status, sched_nr);
            res.status.state = tasks::task_manager::task_state::running;
            res.status.children = task_type == locator::tablet_task_type::split ? co_await get_children(get_module(), id, std::bind_front(&gms::gossiper::is_alive, &_ss.gossiper())) : utils::chunked_vector<tasks::task_identity>{};
            co_return res;
        }
    }

    if (!res.tablets.empty()) {
        res.status.state = sched_nr == 0 ? tasks::task_manager::task_state::created : tasks::task_manager::task_state::running;
        co_return res;
    }
    // FIXME: Show finished tasks.
    co_return std::nullopt;
}

task_manager_module::task_manager_module(tasks::task_manager& tm, service::storage_service& ss) noexcept
    : tasks::task_manager::module(tm, "tablets")
    , _ss(ss)
{}

std::set<locator::host_id> task_manager_module::get_nodes() const {
    return get_task_manager().get_nodes(_ss);
}

}
