/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "locator/tablets.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "repair/row_level.hh"
#include "service/task_manager_module.hh"
#include "service/topology_state_machine.hh"
#include "tasks/task_handler.hh"
#include "tasks/virtual_task_hint.hh"
#include "utils/UUID_gen.hh"
#include <seastar/coroutine/maybe_yield.hh>

namespace service {

struct status_helper {
    tasks::task_status status;
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

future<std::optional<tasks::virtual_task_hint>> tablet_virtual_task::contains(tasks::task_id task_id) const {
    co_await _ss._migration_manager.local().get_group0_barrier().trigger();

    auto tmptr = _ss.get_token_metadata_ptr();
    auto tables = get_table_ids();
    for (auto table : tables) {
        auto& tmap = tmptr->tablets().get_tablet_map(table);
        if (auto task_type = maybe_get_task_type(tmap.resize_task_info(), task_id); task_type.has_value()) {
            co_return tasks::virtual_task_hint{
                .table_id = table,
                .task_type = task_type.value(),
                .tablet_id = std::nullopt,
            };
        }
        std::optional<locator::tablet_id> tid = tmap.first_tablet();
        for (const locator::tablet_info& info : tmap.tablets()) {
            auto task_type = maybe_get_task_type(info.repair_task_info, task_id).or_else([&] () {
                return maybe_get_task_type(info.migration_task_info, task_id);
            });
            if (task_type.has_value()) {
                co_return tasks::virtual_task_hint{
                    .table_id = table,
                    .task_type = task_type.value(),
                    .tablet_id = tablet_id_provided(task_type.value()) ? std::make_optional(*tid) : std::nullopt,
                };
            }
            co_await coroutine::maybe_yield();
            tid = tmap.next_tablet(*tid);
        }
    }

    // Check if the task id is present in the repair task table
    auto progress = co_await _ss._repair.local().get_tablet_repair_task_progress(task_id);
    if (progress && progress->requested > 0) {
        co_return tasks::virtual_task_hint{
            .table_id = progress->table_uuid,
            .task_type = locator::tablet_task_type::user_repair,
            .tablet_id = std::nullopt,
        };
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

    const auto& tablets = _ss.get_token_metadata().tablets();
    size_t tablet_count = tablets.has_tablet_map(table) ? tablets.get_tablet_map(table).tablet_count() : 0;
    auto res = co_await get_status_helper(id, std::move(hint));
    if (!res) {
        co_return std::nullopt;
    }

    tasks::tmlogger.info("tablet_virtual_task: wait until tablet operation is finished");
    co_await utils::get_local_injector().inject("tablet_virtual_task_wait", utils::wait_for_message(60s));

    auto task_finished = [&] () -> future<bool> {
        auto tmptr = _ss.get_token_metadata_ptr();
        if (!tmptr->tablets().has_tablet_map(table)) {
            co_return true;
        }
        auto& tmap = tmptr->tablets().get_tablet_map(table);
        if (is_repair_task(task_type)) {
            bool running = false;
            co_await tmap.for_each_tablet([&] (locator::tablet_id, const locator::tablet_info& info) {
                if (info.repair_task_info.is_valid() && info.repair_task_info.tablet_task_id.uuid() == id.uuid()) {
                    running = true;
                }
                return make_ready_future();
            });
            co_return !running;
        }
        if (is_resize_task(task_type)) {
            co_return tmap.resize_task_info().tablet_task_id.uuid() != id.uuid();
        }
        if (is_migration_task(task_type)) {
            co_return tmap.get_tablet_info(tablet_id_opt.value()).migration_task_info.tablet_task_id.uuid() != id.uuid();
        }
        on_internal_error(tasks::tmlogger, fmt::format("tablet_virtual_task::wait: unhandled tablet task type {}", task_type));
    };

    // The repair check (task_finished) is async — for_each_tablet scans
    // every tablet — so we cannot use condition_variable::wait(Pred) which
    // requires a synchronous predicate.
    //
    // Register a waiter via event.wait() *before* running the async check
    // to avoid missing a broadcast that fires while task_finished() yields.
    // If the task is finished we discard the future (the stale waiter is
    // cleaned up on the next broadcast or broken()). Otherwise we co_await
    // the future, which is guaranteed to capture any broadcast that occurred
    // during the check. event.broken() during shutdown propagates
    // broken_condition_variable and unblocks the loop promptly.
    while (true) {
        auto f = _ss._topology_state_machine.event.wait();
        if (co_await task_finished()) {
            (void)f.handle_exception([] (auto&&) {});
            break;
        }
        co_await std::move(f);
    }

    res->status.state = tasks::task_manager::task_state::done; // Failed repair task is retried.
    if (!_ss.get_token_metadata().tablets().has_tablet_map(table)) {
        res->status.end_time = db_clock::now();
        co_return res->status;
    }
    if (is_migration_task(task_type)) {
        auto& replicas = _ss.get_token_metadata().tablets().get_tablet_map(table).get_tablet_info(tablet_id_opt.value()).replicas;
        auto migration_failed = std::all_of(replicas.begin(), replicas.end(), [&] (const auto& replica) { return res->pending_replica.has_value() && replica != res->pending_replica.value(); });
        res->status.state = migration_failed ? tasks::task_manager::task_state::failed : tasks::task_manager::task_state::done;
    } else if (is_resize_task(task_type)) {
        auto new_tablet_count = _ss.get_token_metadata().tablets().get_tablet_map(table).tablet_count();
        res->status.state = new_tablet_count == tablet_count ? tasks::task_manager::task_state::suspended : tasks::task_manager::task_state::done;
        res->status.children = task_type == locator::tablet_task_type::split ? co_await get_children(get_module(), id, _ss.get_token_metadata_ptr()) : utils::chunked_vector<tasks::task_identity>{};
    } else {
        res->status.children = co_await get_children(get_module(), id, _ss.get_token_metadata_ptr());
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
    auto tables = get_table_ids();
    for (auto table : tables) {
        auto& tmap = tmptr->tablets().get_tablet_map(table);
        auto schema = _ss._db.local().get_tables_metadata().get_table(table).schema();
        std::unordered_map<tasks::task_id, tasks::task_stats> user_requests;
        std::unordered_map<tasks::task_id, size_t> sched_num_sum;
        auto resize_stats = maybe_make_task_stats(tmap.resize_task_info(), schema);
        if (resize_stats) {
            res.push_back(std::move(resize_stats.value()));
        }
        co_await tmap.for_each_tablet([&] (locator::tablet_id tid, const locator::tablet_info& info) {
            auto repair_stats = maybe_make_task_stats(info.repair_task_info, schema);
            if (repair_stats) {
                if (info.repair_task_info.is_user_repair_request()) {
                    // User requested repair may encompass more that one tablet.
                    auto task_id = tasks::task_id{info.repair_task_info.tablet_task_id.uuid()};
                    user_requests[task_id] = std::move(repair_stats.value());
                    sched_num_sum[task_id] += info.repair_task_info.sched_nr;
                } else {
                    res.push_back(std::move(repair_stats.value()));
                }
            }

            auto migration_stats = maybe_make_task_stats(info.migration_task_info, schema);
            if (migration_stats) {
                res.push_back(std::move(migration_stats.value()));
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
    return _ss.get_token_metadata().tablets().all_table_groups() | std::views::transform([] (const auto& table_to_tablets) { return table_to_tablets.first; }) | std::ranges::to<std::vector<table_id>>();
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
    auto table_ptr = _ss._db.local().get_tables_metadata().get_table_if_exists(table);
    if (!table_ptr) {
        co_return tasks::task_status {
            .task_id = id,
            .kind = tasks::task_kind::cluster,
            .is_abortable = co_await is_abortable(std::move(hint)),
        };
    }
    auto schema = table_ptr->schema();
    res.status = {
        .task_id = id,
        .kind = tasks::task_kind::cluster,
        .is_abortable = co_await is_abortable(std::move(hint)),
        .keyspace = schema->ks_name(),
        .table = schema->cf_name(),
    };
    size_t sched_nr = 0;
    auto tmptr = _ss.get_token_metadata_ptr();
    auto& tmap = tmptr->tablets().get_tablet_map(table);
    bool repair_task_finished = false;
    bool repair_task_pending = false;
    bool no_tablets_processed = true;
    if (is_repair_task(task_type)) {
        auto progress = co_await _ss._repair.local().get_tablet_repair_task_progress(id);
        if (progress) {
            res.status.progress.completed = progress->finished;
            res.status.progress.total = progress->requested;
            res.status.progress_units = "tablets";
            if (progress->requested > 0 && progress->requested == progress->finished) {
                repair_task_finished = true;
            } if (progress->requested > 0 && progress->requested > progress->finished) {
                repair_task_pending = true;
            }
        }
        co_await tmap.for_each_tablet([&] (locator::tablet_id tid, const locator::tablet_info& info) {
            auto& task_info = info.repair_task_info;
            if (task_info.tablet_task_id.uuid() == id.uuid()) {
                update_status(task_info, res.status, sched_nr);
                no_tablets_processed = false;
            }
            return make_ready_future();
        });
        res.status.children = co_await get_children(get_module(), id, _ss.get_token_metadata_ptr());
    } else if (is_migration_task(task_type)) {    // Migration task.
        auto tablet_id = hint.get_tablet_id();
        res.pending_replica = tmap.get_tablet_transition_info(tablet_id)->pending_replica;
        auto& task_info = tmap.get_tablet_info(tablet_id).migration_task_info;
        if (task_info.tablet_task_id.uuid() == id.uuid()) {
            update_status(task_info, res.status, sched_nr);
            no_tablets_processed = false;
        }
    } else {    // Resize task.
        auto& task_info = tmap.resize_task_info();
        if (task_info.tablet_task_id.uuid() == id.uuid()) {
            update_status(task_info, res.status, sched_nr);
            res.status.state = tasks::task_manager::task_state::running;
            res.status.children = task_type == locator::tablet_task_type::split ? co_await get_children(get_module(), id, _ss.get_token_metadata_ptr()) : utils::chunked_vector<tasks::task_identity>{};
            co_return res;
        }
    }

    if (!no_tablets_processed) {
        res.status.state = sched_nr == 0 ? tasks::task_manager::task_state::created : tasks::task_manager::task_state::running;
        co_return res;
    }

    if (repair_task_pending) {
        // When repair_task_pending is true, the res.tablets will be empty iff the request is aborted by user.
        res.status.state = no_tablets_processed ? tasks::task_manager::task_state::failed : tasks::task_manager::task_state::running;
        co_return res;
    }
    if (repair_task_finished) {
        res.status.state = tasks::task_manager::task_state::done;
        co_return res;
    }

    co_return std::nullopt;
}

task_manager_module::task_manager_module(tasks::task_manager& tm, service::storage_service& ss) noexcept
    : tasks::task_manager::module(tm, "tablets")
    , _ss(ss)
{}

std::set<locator::host_id> task_manager_module::get_nodes() const {
    return get_task_manager().get_nodes(_ss);
}

namespace topo {

static tasks::task_manager::task_state get_state(const db::system_keyspace::topology_requests_entry& entry) {
    if (!entry.id) {
        return tasks::task_manager::task_state::created;
    } else if (!entry.done) {
        return tasks::task_manager::task_state::running;
    } else if (entry.error == "") {
        return tasks::task_manager::task_state::done;
    } else {
        return tasks::task_manager::task_state::failed;
    }
}

tasks::task_manager::task_group global_topology_request_virtual_task::get_group() const noexcept {
    return tasks::task_manager::task_group::global_topology_change_group;
}

future<std::optional<tasks::virtual_task_hint>> global_topology_request_virtual_task::contains(tasks::task_id task_id) const {
    if (!task_id.uuid().is_timestamp()) {
        // Task id of node ops operation is always a timestamp.
        co_return std::nullopt;
    }

    auto hint = std::make_optional<tasks::virtual_task_hint>({});
    auto entry = co_await _ss._sys_ks.local().get_topology_request_entry_opt(task_id.uuid());
    if (entry.has_value() && std::holds_alternative<service::global_topology_request>(entry->request_type) &&
            std::get<service::global_topology_request>(entry->request_type) == global_topology_request::keyspace_rf_change) {
        co_return hint;
    }
    co_return std::nullopt;
}

future<tasks::is_abortable> global_topology_request_virtual_task::is_abortable(tasks::virtual_task_hint) const {
    return make_ready_future<tasks::is_abortable>(tasks::is_abortable::yes);
}

static tasks::task_stats get_task_stats(const db::system_keyspace::topology_requests_entry& entry) {
    return tasks::task_stats{
        .task_id = tasks::task_id{entry.id},
        .type = fmt::to_string(std::get<service::global_topology_request>(entry.request_type)),
        .kind = tasks::task_kind::cluster,
        .scope = "keyspace",
        .state = get_state(entry),
        .sequence_number = 0,
        .keyspace = entry.new_keyspace_rf_change_ks_name.value_or(""),
        .table = "",
        .entity = "",
        .shard = 0,
        .start_time = entry.start_time,
        .end_time = entry.end_time,
    };
}

future<std::optional<tasks::task_status>> global_topology_request_virtual_task::get_status(tasks::task_id id, tasks::virtual_task_hint hint) {
    auto entry = co_await _ss._sys_ks.local().get_topology_request_entry_opt(id.uuid());
    if (!entry.has_value()) {
        co_return std::nullopt;
    }
    auto task_stats = get_task_stats(*entry);
    co_return tasks::task_status{
        .task_id = task_stats.task_id,
        .type = task_stats.type,
        .kind = task_stats.kind,
        .scope = task_stats.scope,
        .state = task_stats.state,
        .is_abortable = co_await is_abortable(std::move(hint)),
        .start_time = task_stats.start_time,
        .end_time = task_stats.end_time,
        .error = entry->error,
        .parent_id = tasks::task_id::create_null_id(),
        .sequence_number = task_stats.sequence_number,
        .shard = task_stats.shard,
        .keyspace = task_stats.keyspace,
        .table = task_stats.table,
        .entity = task_stats.entity,
        .progress_units = "",
        .progress = tasks::task_manager::task::progress{},
        .children = utils::chunked_vector<tasks::task_identity>{},
    };
}

future<std::optional<tasks::task_status>> global_topology_request_virtual_task::wait(tasks::task_id id, tasks::virtual_task_hint hint) {
    auto entry = co_await get_status(id, hint);
    if (!entry) {
        co_return std::nullopt;
    }

    co_await _ss.wait_for_topology_request_completion(id.uuid(), false);
    co_return co_await get_status(id, std::move(hint));
}

future<> global_topology_request_virtual_task::abort(tasks::task_id id, tasks::virtual_task_hint) noexcept {
    return _ss.abort_rf_change(id.uuid());
}

future<std::vector<tasks::task_stats>> global_topology_request_virtual_task::get_stats() {
    db::system_keyspace& sys_ks = _ss._sys_ks.local();
    co_return std::ranges::to<std::vector<tasks::task_stats>>(co_await sys_ks.get_topology_request_entries({global_topology_request::keyspace_rf_change}, db_clock::now() - get_task_manager().get_user_task_ttl())
            | std::views::transform([] (const auto& e) {
        auto& entry = e.second;
        return get_task_stats(entry);
    }));
}

task_manager_module::task_manager_module(tasks::task_manager& tm) noexcept
    : tasks::task_manager::module(tm, "global_topology_requests")
{}

}

namespace vnodes_to_tablets {

tasks::task_id migration_virtual_task::make_task_id(const sstring& keyspace) {
    // Prefix the keyspace name with a unique identifier for this task to avoid
    // collisions with other named UUIDs that may be added in the future.
    auto uuid = utils::UUID_gen::get_name_UUID("vnodes_to_tablets_migration:" + keyspace);
    return tasks::task_id{uuid};
}

std::optional<sstring> migration_virtual_task::find_keyspace_for_task_id(tasks::task_id id) const {
    auto& db = _ss._db.local();
    // Note: We could use a cache here, but it's probably an overkill.
    // Scylla supports up to 1000 keyspaces, which is a relatively small number
    // for UUID computation.
    for (const auto& ks_name : db.get_all_keyspaces()) {
        if (make_task_id(ks_name) == id) {
            return ks_name;
        }
    }
    return std::nullopt;
}

tasks::task_manager::task_group migration_virtual_task::get_group() const noexcept {
    return tasks::task_manager::task_group::vnodes_to_tablets_migration_group;
}

future<std::optional<tasks::virtual_task_hint>> migration_virtual_task::contains(tasks::task_id task_id) const {
    if (!task_id.uuid().is_name_based()) {
        // Task id of migration task is always a named UUID.
        co_return std::nullopt;
    }
    auto ks_name = find_keyspace_for_task_id(task_id);
    if (!ks_name) {
        co_return std::nullopt;
    }
    auto status = _ss.get_tablets_migration_status(*ks_name);
    if (status != storage_service::migration_status::migrating_to_tablets) {
        co_return std::nullopt;
    }
    co_return tasks::virtual_task_hint{.keyspace_name = *ks_name};
}

tasks::task_stats migration_virtual_task::make_task_stats(tasks::task_id id, const sstring& keyspace) {
    return tasks::task_stats{
        .task_id = id,
        .type = "vnodes_to_tablets_migration",
        .kind = tasks::task_kind::cluster,
        .scope = "keyspace",
        .state = tasks::task_manager::task_state::running,
        .sequence_number = 0,
        .keyspace = keyspace,
        .table = "",
        .entity = "",
        .shard = 0,
        .start_time = {},
        .end_time = {},
    };
}

tasks::task_status migration_virtual_task::make_task_status(tasks::task_id id, const sstring& keyspace,
        tasks::task_manager::task_state state,
        tasks::task_manager::task::progress progress) {
    auto stats = make_task_stats(id, keyspace);
    return tasks::task_status{
        .task_id = stats.task_id,
        .type = stats.type,
        .kind = stats.kind,
        .scope = stats.scope,
        .state = state,
        .is_abortable = tasks::is_abortable::no,
        .start_time = stats.start_time,
        .end_time = stats.end_time,
        .parent_id = tasks::task_id::create_null_id(),
        .sequence_number = stats.sequence_number,
        .shard = stats.shard,
        .keyspace = stats.keyspace,
        .table = stats.table,
        .entity = stats.entity,
        .progress_units = "nodes",
        .progress = progress,
    };
}

future<std::optional<tasks::task_status>> migration_virtual_task::get_status(tasks::task_id id, tasks::virtual_task_hint hint) {
    auto& ks_name = hint.keyspace_name;
    if (!ks_name) {
        co_return std::nullopt;
    }
    storage_service::keyspace_migration_status status;
    try {
        status = co_await _ss.get_tablets_migration_status_with_node_details(*ks_name);
    } catch (const replica::no_such_keyspace&) {
        co_return std::nullopt;
    }
    if (status.status != storage_service::migration_status::migrating_to_tablets) {
        co_return std::nullopt;
    }

    // The progress tracks the number of nodes currently using tablets.
    // During forward migration it increases; during rollback it decreases.
    double nodes_upgraded = 0;
    double total_nodes = status.nodes.size();
    for (const auto& node : status.nodes) {
        if (node.current_mode == "tablets") {
            nodes_upgraded++;
        }
    }

    auto task_state = tasks::task_manager::task_state::running;
    auto task_progress = tasks::task_manager::task::progress{nodes_upgraded, total_nodes};

    // Note: Children are left empty. Although the resharding tasks are children
    // of the migration task, using get_children() here would be pointless
    // because resharding runs during startup before start_listen() is called,
    // so the RPC fan-out to collect children from all nodes (including self)
    // would fail.
    co_return make_task_status(id, *ks_name, task_state, task_progress);
}

future<std::optional<tasks::task_status>> migration_virtual_task::wait(tasks::task_id id, tasks::virtual_task_hint hint) {
    auto& ks_name = hint.keyspace_name;
    if (!ks_name) {
        co_return std::nullopt;
    }

    tasks::tmlogger.info("migration_virtual_task: waiting for vnodes-to-tablets migration to finish: task_id={}, keyspace={}", id, *ks_name);

    storage_service::migration_status status;
    while (true) {
        try {
            status = _ss.get_tablets_migration_status(*ks_name);
        } catch (const replica::no_such_keyspace&) {
            co_return std::nullopt;
        }
        if (status != storage_service::migration_status::migrating_to_tablets) {
            break;
        }
        co_await _ss._topology_state_machine.event.wait();
    }

    bool migration_succeeded = status == storage_service::migration_status::tablets;
    auto state = migration_succeeded ? tasks::task_manager::task_state::done : tasks::task_manager::task_state::suspended;
    double total_nodes = _ss._topology_state_machine._topology.normal_nodes.size();
    auto task_progress = tasks::task_manager::task::progress{migration_succeeded ? total_nodes : 0, total_nodes};

    auto task_status = make_task_status(id, *ks_name, state, task_progress);
    task_status.end_time = db_clock::now();

    co_return task_status;
}

future<> migration_virtual_task::abort(tasks::task_id id, tasks::virtual_task_hint hint) noexcept {
    // Vnodes-to-tablets migration cannot be aborted via the task manager.
    // It requires a manual rollback procedure.
    return make_ready_future<>();
}

future<std::vector<tasks::task_stats>> migration_virtual_task::get_stats() {
    std::vector<tasks::task_stats> result;
    auto& db = _ss._db.local();
    for (const auto& ks_name : db.get_all_keyspaces()) {
        storage_service::migration_status status;
        try {
            status = _ss.get_tablets_migration_status(ks_name);
        } catch (const replica::no_such_keyspace&) {
            continue;
        }
        if (status != storage_service::migration_status::migrating_to_tablets) {
            continue;
        }
        result.push_back(make_task_stats(make_task_id(ks_name), ks_name));
    }
    co_return result;
}

task_manager_module::task_manager_module(tasks::task_manager& tm, service::storage_service& ss) noexcept
    : tasks::task_manager::module(tm, "vnodes_to_tablets_migration")
    , _ss(ss)
{}

std::set<locator::host_id> task_manager_module::get_nodes() const {
    return get_task_manager().get_nodes(_ss);
}

}

}
