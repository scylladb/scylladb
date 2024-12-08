/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/tablets.hh"
#include "replica/database.hh"
#include "service/storage_service.hh"
#include "service/task_manager_module.hh"
#include "tasks/task_handler.hh"
#include "tasks/virtual_task_hint.hh"
#include <seastar/coroutine/maybe_yield.hh>
#include <boost/range/adaptor/transformed.hpp>

namespace service {

tasks::task_manager::task_group tablet_virtual_task::get_group() const noexcept {
    return tasks::task_manager::task_group::tablets_group;
}

future<std::optional<tasks::virtual_task_hint>> tablet_virtual_task::contains(tasks::task_id task_id) const {
    auto tables = get_table_ids();
    for (auto table : tables) {
        auto& tmap = _ss.get_token_metadata().tablets().get_tablet_map(table);
        for (const locator::tablet_info& info : tmap.tablets()) {
            if (info.repair_task_info.is_valid() && info.repair_task_info.tablet_task_id.uuid() == task_id.uuid()) {
                co_return tasks::virtual_task_hint{
                    .table_id = table,
                };
            }
            co_await coroutine::maybe_yield();
        }
    }
    co_return std::nullopt;
}

future<tasks::is_abortable> tablet_virtual_task::is_abortable(tasks::virtual_task_hint hint) const {
    return make_ready_future<tasks::is_abortable>(tasks::is_abortable::yes);
}

future<std::optional<tasks::task_status>> tablet_virtual_task::get_status(tasks::task_id id, tasks::virtual_task_hint hint) {
    utils::chunked_vector<locator::tablet_id> tablets;
    co_return co_await get_status_helper(id, tablets, std::move(hint));
}

future<std::optional<tasks::task_status>> tablet_virtual_task::wait(tasks::task_id id, tasks::virtual_task_hint hint) {
    auto table = get_table_id(hint);

    utils::chunked_vector<locator::tablet_id> tablets;
    auto status = co_await get_status_helper(id, tablets, std::move(hint));
    if (!status) {
        co_return std::nullopt;
    }

    co_await _ss._topology_state_machine.event.wait([&] {
        auto& tmap = _ss.get_token_metadata().tablets().get_tablet_map(table);
        return std::all_of(tablets.begin(), tablets.end(), [&] (const locator::tablet_id& tablet) {
            return tmap.get_tablet_info(tablet).repair_task_info.tablet_task_id.uuid() != id.uuid();
        });
    });

    status->state = tasks::task_manager::task_state::done; // Failed repair task is retried.
    status->end_time = db_clock::now(); // FIXME: Get precise end time.
    co_return status;
}

future<> tablet_virtual_task::abort(tasks::task_id id, tasks::virtual_task_hint hint) noexcept {
    auto table = get_table_id(hint);
    co_await _ss.del_repair_tablet_request(table, locator::tablet_task_id{id.uuid()});
}

future<std::vector<tasks::task_stats>> tablet_virtual_task::get_stats() {
    std::vector<tasks::task_stats> res;
    auto tables = get_table_ids();
    for (auto table : tables) {
        auto& tmap = _ss.get_token_metadata().tablets().get_tablet_map(table);
        auto schema = _ss._db.local().get_tables_metadata().get_table(table).schema();
        std::unordered_map<tasks::task_id, tasks::task_stats> user_requests;
        std::unordered_map<tasks::task_id, size_t> sched_num_sum;
        co_await tmap.for_each_tablet([&] (locator::tablet_id tid, const locator::tablet_info& info) {
            if (!info.repair_task_info.is_valid()) {
                return make_ready_future();
            }

            auto task_id = tasks::task_id{info.repair_task_info.tablet_task_id.uuid()};
            tasks::task_stats stats{
                .task_id = task_id,
                .type = locator::tablet_task_type_to_string(info.repair_task_info.request_type),
                .kind = tasks::task_kind::cluster,
                .scope = info.repair_task_info.is_user_request() ? "table" : "tablet",
                .state = tasks::task_manager::task_state::running,
                .keyspace = schema->ks_name(),
                .table = schema->cf_name()
            };
            if (info.repair_task_info.is_user_request()) {
                // User requested repair may encompass more that one tablet.
                user_requests[task_id] = std::move(stats);
                sched_num_sum[task_id] += info.repair_task_info.sched_nr;
            } else {
                res.push_back(std::move(stats));
            }
            return make_ready_future();
        });

        for (auto& [id, task_stats] : user_requests) {
            task_stats.state = sched_num_sum[id] == 0 ? tasks::task_manager::task_state::created : tasks::task_manager::task_state::running;
            res.push_back(std::move(task_stats));
        }
    }
    // FIXME: Show finished tasks.
    co_return res;
}

std::vector<table_id> tablet_virtual_task::get_table_ids() const {
    return boost::copy_range<std::vector<table_id>>(_ss.get_token_metadata().tablets().all_tables() | boost::adaptors::transformed([] (const auto& table_to_tablets) { return table_to_tablets.first; }));
}

future<std::optional<tasks::task_status>> tablet_virtual_task::get_status_helper(tasks::task_id id, utils::chunked_vector<locator::tablet_id>& tablets, tasks::virtual_task_hint hint) {
    auto table = get_table_id(hint);
    auto schema = _ss._db.local().get_tables_metadata().get_table(table).schema();
    tasks::task_status res{
        .task_id = id,
        .kind = tasks::task_kind::cluster,
        .is_abortable = co_await is_abortable(std::move(hint)),
        .keyspace = schema->ks_name(),
        .table = schema->cf_name(),
    };
    size_t sched_nr = 0;
    auto& tmap = _ss.get_token_metadata().tablets().get_tablet_map(table);
    co_await tmap.for_each_tablet([&] (locator::tablet_id tid, const locator::tablet_info& info) {
        if (info.repair_task_info.tablet_task_id.uuid() == id.uuid()) {
            sched_nr += info.repair_task_info.sched_nr;
            res.type = locator::tablet_task_type_to_string(info.repair_task_info.request_type);
            res.scope = info.repair_task_info.is_user_request() ? "table" : "tablet";
            res.start_time = info.repair_task_info.request_time;
            tablets.push_back(tid);
        }
        return make_ready_future();
    });

    if (!tablets.empty()) {
        res.state = sched_nr == 0 ? tasks::task_manager::task_state::created : tasks::task_manager::task_state::running;
        co_return res;
    }
    // FIXME: Show finished tasks.
    co_return std::nullopt;
}

table_id tablet_virtual_task::get_table_id(const tasks::virtual_task_hint& hint) const {
    if (!hint.table_id.has_value()) {
        on_internal_error(tasks::tmlogger, "tablet_virtual_task hint does not contain table_id");
    }
    return hint.table_id.value();
}

task_manager_module::task_manager_module(tasks::task_manager& tm) noexcept
    : tasks::task_manager::module(tm, "tablets")
{}

}
