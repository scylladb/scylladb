/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <stdexcept>

#include "db/view/view_building_worker.hh"
#include "db/view/view_consumer.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_group0_client.hh"
#include "schema/schema_fwd.hh"
#include "idl/view.dist.hh"
#include "sstables/sstables.hh"

static logging::logger vbw_logger("view_building_worker");

namespace db {

namespace view {

// Called in the context of a seastar::thread.
class view_building_worker::consumer : public view_consumer {
    replica::database& _db;
    view_building_worker::batch& _batch;
    lw_shared_ptr<replica::table> _base;
    dht::decorated_key _current_key;

    mutation_reader& _reader;
    reader_permit _permit;

protected:
    virtual void load_views_to_build() override {
        _views_to_build = _batch.tasks | std::views::filter([this] (const auto& task_entry) {
            return _db.column_family_exists(*task_entry.second.view_id);
        }) | std::views::transform([this] (const auto& task_entry) {
            return view_ptr(_db.find_schema(*task_entry.second.view_id));
        }) | std::views::filter([this] (const view_ptr& view) {
            return partition_key_matches(_db.as_data_dictionary(), *_reader.schema(), *view->view_info(), _current_key);
        }) | std::ranges::to<std::vector>();
    }
    virtual void check_for_built_views() override {}

    virtual bool should_stop_consuming_end_of_partition() override {
        return false;
    }

    virtual dht::decorated_key& get_current_key() override {
        return _current_key;
    }
    virtual void set_current_key(dht::decorated_key key) override {
        _current_key = std::move(key);
    }

    virtual lw_shared_ptr<replica::table> base() override {
        return _base;
    }
    virtual mutation_reader& reader() override {
        return _reader;
    }
    virtual reader_permit& permit() override {
        return _permit;
    }

public:
    consumer(replica::database& db, view_building_worker::batch& batch, lw_shared_ptr<replica::table> base, mutation_reader& reader, reader_permit permit, shared_ptr<view_update_generator> gen, gc_clock::time_point now, abort_source& as) 
            : view_consumer(std::move(gen), now, as)
            , _db(db)
            , _batch(batch)
            , _base(base)
            , _current_key(dht::minimum_token(), partition_key::make_empty())
            , _reader(reader)
            , _permit(std::move(permit)) {}

    dht::token consume_end_of_stream() {
        return _current_key.token();
    }
};

view_building_worker::view_building_worker(replica::database& db, service::raft_group0_client& group0_client, view_update_generator& vug, netw::messaging_service& ms, service::view_building::view_building_state_machine& vbsm)
        : _db(db)
        , _group0_client(group0_client)
        , _vug(vug)
        , _messaging(ms)
        , _vb_state_machine(vbsm) 
{
    init_messaging_service();
}

void view_building_worker::start_state_observer() {
    SCYLLA_ASSERT(this_shard_id() == 0);
    _view_building_state_observer = run_view_building_state_observer();
}

dht::token_range view_building_worker::get_tablet_token_range(table_id table_id, dht::token last_token) {
    auto& cf = _db.find_column_family(table_id);
    auto& tablet_map = cf.get_effective_replication_map()->get_token_metadata().tablets().get_tablet_map(table_id);
    return tablet_map.get_token_range(tablet_map.get_tablet_id(last_token));
}

future<> view_building_worker::drain() {
    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    if (this_shard_id() == 0) {
        auto state_observer = std::exchange(_view_building_state_observer, make_ready_future<>());
        co_await std::move(state_observer);
    }
    co_await _state.clear_state();
    co_await uninit_messaging_service();
}

future<> view_building_worker::stop() {
    return drain();
}

future<> view_building_worker::run_view_building_state_observer() {
    auto abort = _as.subscribe([this] () noexcept {
        _vb_state_machine.event.broadcast();
    });
    try {
        co_await _group0_client.wait_until_group0_upgraded(_as);
    } catch (abort_requested_exception&) {
        vbw_logger.debug("Got abort_requested_exception while waiting for group0 upgrade.");
        co_return;
    }

    // Initially discover staging sstables on this node 
    // and create view_building tasks if needed.
    // Try until success.
    while (!_as.abort_requested()) {
        try {
            auto guard = co_await _group0_client.start_operation(_as, service::raft_timeout{});
            // Sstables are spread across all shards
            auto new_staging_tasks = co_await container().map_reduce0([building_tasks = _vb_state_machine.building_state.tasks_state] (auto& vbw) -> future<std::vector<service::view_building::view_building_task>> {
                auto new_tasks = vbw.discover_staging_sstables(std::move(building_tasks));
                co_return new_tasks;
            }, std::vector<service::view_building::view_building_task>{}, [] (std::vector<service::view_building::view_building_task> a, std::vector<service::view_building::view_building_task>&& b) {
                a.insert(a.end(), std::make_move_iterator(b.begin()), std::make_move_iterator(b.end()));
                return a;
            });
            co_await save_view_building_tasks(std::move(guard), std::move(new_staging_tasks));
            break;
        } catch (abort_requested_exception&) {
        } catch (service::group0_concurrent_modification&) {
            vbw_logger.warn("Got group0_concurrent_modification error in save_view_building_tasks(). Retrying ...");
        } catch (...) {
            vbw_logger.warn("Error while discovering staging sstables: {}. Retrying...", std::current_exception());
        }
    }

    while (!_as.abort_requested()) {
        bool sleep = false;
        try {
            vbw_logger.trace("view_building_state_observer() iteration");
            auto read_apply_mutex_holder = co_await _group0_client.hold_read_apply_mutex(_as);
            
            co_await update_built_views();
            co_await update_building_state();
            _as.check();

            read_apply_mutex_holder.return_all();
            co_await _vb_state_machine.event.wait();
        } catch (abort_requested_exception&) {
        } catch (broken_condition_variable&) {
        } catch (...) {
            vbw_logger.warn("view_building_state_observer failed with: {}", std::current_exception());
            sleep = true;
        }

        if (sleep && !_as.abort_requested()) {
            try {
                vbw_logger.debug("Sleeping after exception.");
                co_await seastar::sleep_abortable(std::chrono::seconds(1), _as);
            } catch (...) {
                vbw_logger.warn("view_building_state_observer sleep failed: {}", std::current_exception());
            }
        }
    }
}

// Compares tablet-based views entries in `system.view_build_status_v2`(group0 table) 
// with data in `system.built_views`(local table), and updates the second table accordingly.
future<> view_building_worker::update_built_views() {
    auto id_to_name = [&] (table_id table_id) {
        auto schema = _db.find_schema(table_id);
        return std::make_pair(schema->ks_name(), schema->cf_name());
    };
    auto& sys_ks = _group0_client.sys_ks();

    std::set<std::pair<sstring, sstring>> built_views;
    for (auto& [id, statuses]: _vb_state_machine.views_state.status_map) {
        if (!_db.find_column_family(id).uses_tablets()) {
            // ignore vnode views
            continue;
        }
        if (std::ranges::all_of(statuses, [] (const auto& e) { return e.second == build_status::SUCCESS; })) {
            built_views.insert(id_to_name(id));
        }
    }

    auto local_built = co_await sys_ks.load_built_views() | std::views::filter([&] (auto& v) {
        return !_db.has_keyspace(v.first) || _db.find_keyspace(v.first).uses_tablets();
    }) | std::ranges::to<std::set>();

    // Remove dead entries
    for (auto& view: local_built) {
        if (!built_views.contains(view)) {
            co_await sys_ks.remove_built_view(view.first, view.second);
        }
    }

    // Add new entries
    for (auto& view: built_views) {
        if (!local_built.contains(view)) {
            co_await sys_ks.mark_view_as_built(view.first, view.second);
        }
    }
}

future<> view_building_worker::update_building_state() {
    co_await _state.update(*this);
    co_await _state.finish_completed_tasks();
}

bool view_building_worker::is_shard_free(shard_id shard) {
    return !std::ranges::any_of(_state.tasks_map, [&shard] (auto& task_entry) {
        return task_entry.second->replica.shard == shard && task_entry.second->state == view_building_worker::batch_state::in_progress;
    });
}

future<> view_building_worker::save_view_building_tasks(service::group0_guard guard, std::vector<service::view_building::view_building_task> tasks) {
    std::vector<canonical_mutation> cmuts;

    for (auto& task: tasks) {
        // Generate new id for the task
        task.id = utils::UUID_gen::get_time_UUID();
        auto mut = co_await _group0_client.sys_ks().make_view_building_task_mutation(guard.write_timestamp(), task);
        cmuts.emplace_back(std::move(mut));
    }

    auto cmd = _group0_client.prepare_command(service::write_mutations{std::move(cmuts)}, guard, "create view building tasks");
    co_await _group0_client.add_entry(std::move(cmd), std::move(guard), _as);
}

static locator::tablet_id get_sstable_tablet_id(const locator::tablet_map& tablet_map, const sstables::sstable& sst) {
    auto last_token = sst.get_last_decorated_key().token();
    auto tablet_id = tablet_map.get_tablet_id(last_token);

#ifdef SEASTAR_DEBUG
    // Single sstable from tablet-table should contain data for only one tablet
    auto first_token = sst.get_first_decorated_key().token();
    auto first_token_tablet_id = tablet_map.get_tablet_id(first_token);
    SCYLLA_ASSERT(tablet_id == first_token_tablet_id);
#endif

    return tablet_id;
}

static shard_id get_sstable_shard_id(const sstables::sstable& sst) {
    auto shards = sst.get_shards_for_this_sstable();
#ifdef SEASTAR_DEBUG
    // Sstable from tablet-table should belong to only one shard
    SCYLLA_ASSERT(shards.size() == 1);
#endif
    return shards[0];
}

static bool staging_task_exists(const service::view_building::building_tasks& tasks, table_id table_id, const locator::tablet_replica& replica, dht::token last_token) {
    if (!tasks.contains(table_id) || !tasks.at(table_id).contains(replica)) {
        return false;
    }
    auto& replica_tasks = tasks.at(table_id).at(replica);
    return std::ranges::any_of(replica_tasks.staging_tasks, [&last_token] (auto& t) {
        return t.second.last_token == last_token;
    });
}

// Because view building state lives only on shard0, the method needs to take copy of building tasks
// to determine whether a task for particular staging sstable exists or not.
std::vector<service::view_building::view_building_task> view_building_worker::discover_staging_sstables(service::view_building::building_tasks building_tasks) {
    std::vector<service::view_building::view_building_task> tasks_to_create;    
    auto my_host_id = _db.get_token_metadata().get_topology().my_host_id();

    _db.get_tables_metadata().for_each_table([&] (table_id table_id, lw_shared_ptr<replica::table> table) {
        if (!table->uses_tablets()) {
            return;
        }

        auto& tablet_map = _db.get_token_metadata().tablets().get_tablet_map(table_id);
        auto sstables = table->get_sstables();
        for (auto sstable: *sstables) {
            if (!sstable->requires_view_building()) {
                continue;
            }

            auto shard = get_sstable_shard_id(*sstable);
            auto tid = get_sstable_tablet_id(tablet_map, *sstable);
            auto last_token = tablet_map.get_last_token(tid);

            if (!staging_task_exists(building_tasks, table_id, {my_host_id, shard}, last_token)) {
                // For the future: we can check if the sstable needs to go through view building coordinator
                //                 or maybe it can be registered to view_update_generator directly.

                // Task id doesn't matter, it'll be created by save_view_building_tasks()
                tasks_to_create.emplace_back(utils::UUID{}, service::view_building::view_building_task::task_type::process_staging, 
                        service::view_building::view_building_task::task_state::idle, table_id, ::table_id{}, locator::tablet_replica{my_host_id, shard}, last_token);
            }
            _staging_sstables[table_id][last_token].push_back(std::move(sstable));
        }
    });
    return tasks_to_create;
}

future<> view_building_worker::register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<replica::table> table) {
    auto table_id = table->schema()->id();
    auto& tablet_map = _db.get_token_metadata().tablets().get_tablet_map(table_id);

    auto my_host_id = _db.get_token_metadata().get_topology().my_host_id();
    auto shard = get_sstable_shard_id(*sst);
    auto tid = get_sstable_tablet_id(tablet_map, *sst);
    auto last_token = tablet_map.get_last_token(tid);

    // Task id doesn't matter, it'll be created by save_view_building_tasks()
    service::view_building::view_building_task task {
        utils::UUID{}, service::view_building::view_building_task::task_type::process_staging, service::view_building::view_building_task::task_state::idle,
        table_id, ::table_id{}, {my_host_id, shard}, last_token
    };
    _staging_sstables[table_id][last_token].push_back(std::move(sst));

    co_await container().invoke_on(0, [task = std::move(task)] (auto& vbw) -> future<> {
        auto guard = co_await vbw._group0_client.start_operation(vbw._as, service::raft_timeout{});
        co_await vbw.save_view_building_tasks(std::move(guard), {std::move(task)});
    });
}

void view_building_worker::init_messaging_service() {
    ser::view_rpc_verbs::register_work_on_view_building_tasks(&_messaging, [this] (std::vector<utils::UUID> ids) -> future<std::vector<service::view_building::view_task_result>> {
        return container().invoke_on(0, [ids = std::move(ids)] (view_building_worker& vbw) -> future<std::vector<service::view_building::view_task_result>> {
            return vbw.get_tasks_results(std::move(ids));
        });
    });
}

future<> view_building_worker::uninit_messaging_service() {
    return ser::view_rpc_verbs::unregister(&_messaging);
}

future<std::vector<service::view_building::view_task_result>> view_building_worker::get_tasks_results(std::vector<utils::UUID> ids) {
    vbw_logger.debug("Got request for results of tasks: {}", ids);
    auto guard = co_await _group0_client.start_operation(_as, service::raft_timeout{});

    auto are_tasks_finished = [&] () {
        return std::ranges::any_of(ids, [this] (const utils::UUID& id) {
            return _state.finished_tasks.contains(id) || _state.failed_tasks.contains(id);
        });
    };

    auto get_results = [&] () -> std::vector<service::view_building::view_task_result> {
        std::vector<service::view_building::view_task_result> results;
        for (const auto& id: ids) {
            if (_state.finished_tasks.contains(id)) {
                results.emplace_back(service::view_building::view_task_result::command_status::success);
            } else if (_state.failed_tasks.contains(id)) {
                results.emplace_back(service::view_building::view_task_result::command_status::fail);
            } else {
                // This means that the task was aborted. Throw an error, 
                // so the coordinator will refresh its state and retry without aborted IDs.
                throw std::runtime_error(fmt::format("No status for task {}", id));
            }
        }
        return results;
    };

    if (are_tasks_finished()) {
        // If the batch is already finished, we can return the results immediately.
        vbw_logger.debug("Batch with tasks {} is already finished, returning results", ids);
        co_return get_results();
    }

    // All of the tasks should be executed in the same batch 
    // (their statuses are set to started in the same group0 operation).
    // If any ID is not present in the `tasks_map`, it means that it was aborted and we should fail this RPC call,
    // so the coordinator can retry without aborted IDs.
    // That's why we can identify the batch by random (.front()) ID from the `ids` vector.
    auto id = ids.front();
    if (!_state.tasks_map.contains(id)) {
        // If the batch is not found, it means that the task state wasn't set to STARTED
        vbw_logger.warn("Batch with task {} is not found in tasks map", id);
        throw std::runtime_error(fmt::format("Batch with task {} is not found. Make sure the task state was set to STARTED or the task could be aborted.", id));
    } else {
        // Validate that any of the IDs wasn't aborted.
        for (const auto& tid: ids) {
            if (!_state.tasks_map[id]->tasks.contains(tid)) {
                vbw_logger.warn("Task {} is not found in the batch", tid);
                throw std::runtime_error(fmt::format("Task {} is not found in the batch", tid));
            }
        }
    }

    if (_state.tasks_map[id]->state == view_building_worker::batch_state::idle) {
        vbw_logger.debug("Starting batch with tasks {}", _state.tasks_map[id]->tasks);
        if (!is_shard_free(_state.tasks_map[id]->replica.shard)) {
            throw std::runtime_error(fmt::format("Tried to start view building tasks ({}) on shard {} but the shard is busy", _state.tasks_map[id]->tasks, _state.tasks_map[id]->replica.shard, _state.tasks_map[id]->tasks));
        }
        co_await _state.tasks_map[id]->start();
    }

    service::release_guard(std::move(guard));
    while (!_as.abort_requested()) {
        auto read_apply_mutex_holder = co_await _group0_client.hold_read_apply_mutex(_as);

        if (are_tasks_finished()) {
            co_return get_results();
        }

        // Check if the batch is still alive
        if (!_state.tasks_map.contains(id)) {
            throw std::runtime_error(fmt::format("Batch with task {} is not found in tasks map anymore.", id));
        }

        read_apply_mutex_holder.return_all();
        co_await _state.tasks_map[id]->batch_done_cv.wait();
    }
    throw std::runtime_error("View building worker was aborted");
}

// Validates if the task can be executed in a batch on the same shard.
static bool validate_can_be_one_batch(const service::view_building::view_building_task& t1, const service::view_building::view_building_task& t2) {
    return t1.type == t2.type && t1.base_id == t2.base_id && t1.replica == t2.replica && t1.last_token == t2.last_token;
}

future<> view_building_worker::local_state::update(view_building_worker& vbw) {
    const auto& vb_state = vbw._vb_state_machine.building_state;

    // Check if the base table to process was changed.
    // If so, we clear the state, aborting tasks for previous base table and starting new ones for the new base table.
    if (processing_base_table != vb_state.currently_processed_base_table) {
        co_await clear_state();
        processing_base_table = vb_state.currently_processed_base_table;
        vbw_logger.info("Processing base table was changed to: {}", processing_base_table);

        if (processing_base_table) {
            // When we start to process new base table, we need to flush its currrent data, so we can build the view.
            
            co_await vbw.container().invoke_on_all([base_id = *processing_base_table] (view_building_worker& local_vbw) -> future<> {
                auto base_cf = local_vbw._db.find_column_family(base_id).shared_from_this();
                co_await when_all(base_cf->await_pending_writes(), base_cf->await_pending_streams());
                co_await flush_base(base_cf, local_vbw._as);
            });
        }
    }

    if (!processing_base_table) {
        vbw_logger.debug("No base table is selected to be processed.");
        co_return;
    }

    auto erm = vbw._db.find_column_family(*processing_base_table).get_effective_replication_map();
    auto my_host_id = erm->get_topology().my_host_id();
    auto current_tasks_for_this_host = vb_state.get_tasks_for_host(*processing_base_table, my_host_id);

    // scan view building state, collect alive and new (in STARTED state but not started by this worker) tasks
    std::unordered_map<shard_id, std::vector<service::view_building::view_building_task>> new_tasks;
    std::unordered_set<utils::UUID> alive_tasks; // save information about alive tasks to cleanup done/aborted ones
    for (auto& task: current_tasks_for_this_host) {
        auto id = task.get().id;
        alive_tasks.insert(id);
        if (tasks_map.contains(id) || finished_tasks.contains(id)) {
            continue;
        }

        if (task.get().state == service::view_building::view_building_task::task_state::started) {
            auto shard = task.get().replica.shard;
            if (new_tasks.contains(shard)) {
                if (!validate_can_be_one_batch(new_tasks[shard].front(), task)) {
                    // Currently we allow only one batch per shard at a time
                    on_internal_error(vbw_logger, fmt::format("Got not-compatible tasks for the same shard. Task: {}, other: {}", new_tasks[shard].front(), task.get()));
                }
            }
            new_tasks[shard].push_back(task.get());
        }
    }

    auto tasks_map_copy = tasks_map;

    // Clear aborted tasks from tasks_map
    for (auto it = tasks_map_copy.begin(); it != tasks_map_copy.end();) {
        if (!alive_tasks.contains(it->first)) {
            vbw_logger.debug("Aborting task {}", it->first);
            co_await it->second->abort_task(it->first);
            it = tasks_map_copy.erase(it);
        } else {
            ++it;
        }
    }

    // Create batches for new tasks
    for (const auto& [shard, shard_tasks]: new_tasks) {
        auto tasks = shard_tasks | std::views::transform([] (const service::view_building::view_building_task& t) {
            return std::make_pair(t.id, t);
        }) | std::ranges::to<std::unordered_map>();
        auto batch = seastar::make_shared<view_building_worker::batch>(vbw, tasks, shard_tasks.front().base_id, shard_tasks.front().replica);

        for (auto& [id, _]: tasks) {
            tasks_map_copy.insert({id, batch});
        }
        co_await coroutine::maybe_yield();
    }

    tasks_map = std::move(tasks_map_copy);
}

future<> view_building_worker::local_state::finish_completed_tasks() {
    for (auto it = tasks_map.begin(); it != tasks_map.end();) {
        if (it->second->state == view_building_worker::batch_state::idle) {
            ++it;
        } else if (it->second->state == view_building_worker::batch_state::in_progress) {
            vbw_logger.debug("Task {} is still in progress", it->first);
            ++it;
        } else {
            co_await it->second->work.get_future();
            if (it->second->state == view_building_worker::batch_state::finished) {
                finished_tasks.insert(it->first);
                vbw_logger.info("Task {} was completed", it->first);
            } else { // failed
                failed_tasks.insert(it->first);
                vbw_logger.warn("Task {} failed", it->first);
            }
            it->second->batch_done_cv.broadcast();
            it = tasks_map.erase(it);
        }
    }
}

future<> view_building_worker::local_state::clear_state() {
    for (auto& [_, batch]: tasks_map) {
        co_await batch->abort();
    }

    processing_base_table.reset();
    tasks_map.clear();
    finished_tasks.clear();
    failed_tasks.clear();
    vbw_logger.debug("View building worker state was cleared.");
}

view_building_worker::batch::batch(view_building_worker& vbw, std::unordered_map<utils::UUID, service::view_building::view_building_task> tasks, table_id base_id, locator::tablet_replica replica)
    : tasks(std::move(tasks))
    , base_id(base_id)
    , replica(replica)
    , _vbw(vbw) {}

future<> view_building_worker::batch::start() {
    co_await abort_sources.start();
    state = batch_state::in_progress;
    work = do_work();
}

future<> view_building_worker::batch::abort_task(utils::UUID id) {
    tasks.erase(id);
    if (tasks.empty()) {
        co_await abort();
    }
}

future<> view_building_worker::batch::abort() {
    if (abort_sources.local_is_initialized()) {
        co_await abort_sources.invoke_on_all([] (abort_source& local_as) {
            if (!local_as.abort_requested()) {
                local_as.request_abort();
            }
        });

        if (work.valid()) {
            co_await work.get_future();
        }
    }
}

future<> view_building_worker::batch::do_work() {
    // At this point we assume all tasks are validated to be executed in the same batch
    auto& task = tasks.begin()->second;
    vbw_logger.debug("Starting view building batch for tasks {}. Task type {}", tasks | std::views::keys, task.type);
    std::exception_ptr eptr;
    co_await _vbw.container().invoke_on(task.replica.shard, [this, &task, &eptr] (view_building_worker& vbw) -> future<> {
        try {
            switch (task.type) {
            case service::view_building::view_building_task::task_type::build_range:
                co_await do_build_range(vbw);
                break;
            case service::view_building::view_building_task::task_type::process_staging:
                co_await do_process_staging(vbw);
                break;
            }
        } catch (seastar::abort_requested_exception&) {
        } catch (...) {
            eptr = std::current_exception();
        }
    });

    if (eptr) {
        state = batch_state::failed;
        vbw_logger.warn("Batch with tasks {} failed with error: {}", tasks | std::views::keys, eptr);
    } else {
        state = batch_state::finished;
        vbw_logger.debug("Batch with tasks {} finished", tasks | std::views::keys);
    }
    co_await abort_sources.stop();
    _vbw._vb_state_machine.event.broadcast();
}

future<> view_building_worker::batch::do_build_range(view_building_worker& local_vbw) {
    // Run the view building in the streaming scheduling group
    // so that it doesn't impact other tasks with higher priority.
    seastar::thread_attributes attr;
    attr.sched_group = local_vbw._db.get_streaming_scheduling_group();
    return seastar::async(std::move(attr), [this, &local_vbw] {
        auto& as = abort_sources.local();
        auto get_views_ids = [this] {
            return tasks | std::views::values | std::views::transform([] (const service::view_building::view_building_task& t) {
                return t.view_id;
            });
        };
        auto task = tasks.begin()->second;
        auto base_cf = local_vbw._db.find_column_family(task.base_id).shared_from_this();
        gc_clock::time_point now = gc_clock::now();
        reader_permit permit = local_vbw._db.get_reader_concurrency_semaphore().make_tracking_only_permit(nullptr, "build_views_range", db::no_timeout, {});
        auto slice = make_partition_slice(*base_cf->schema());
        auto range = local_vbw.get_tablet_token_range(task.base_id, task.last_token);
        auto prange = dht::to_partition_range(range);

        auto reader = base_cf->get_sstable_set().make_local_shard_sstable_reader(
                base_cf->schema(), 
                permit,
                prange,
                slice,
                nullptr,
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::no);
        auto compaction_state = make_lw_shared<compact_for_query_state>(
                *reader.schema(),
                now,
                slice,
                query::max_rows,
                query::max_partitions);
        auto consumer = compact_for_query<view_building_worker::consumer>(compaction_state, view_building_worker::consumer(
                local_vbw._db,
                *this,
                base_cf,
                reader,
                permit,
                local_vbw._vug.shared_from_this(),
                now,
                as));

        as.check();
        std::exception_ptr eptr;
        try {
            vbw_logger.info("Starting range {} building for base table: {}.{}", range, base_cf->schema()->ks_name(), base_cf->schema()->cf_name());
            auto end_token = reader.consume_in_thread(std::move(consumer));
            vbw_logger.info("Built range {} for base table: {}.{}", dht::token_range(range.start(), end_token), base_cf->schema()->ks_name(), base_cf->schema()->cf_name());
        } catch (seastar::abort_requested_exception&) {
            eptr = std::current_exception();
            vbw_logger.info("Building range {} for base table {} and views {} was aborted.", range, task.base_id, get_views_ids());
        } catch (...) {
            eptr = std::current_exception();
            vbw_logger.warn("Error during processing range {} for base table {} and views {}: ", range, task.base_id, get_views_ids(), eptr);
        }
        reader.close().get();

        if (eptr) {
            // rethrow the exception, so the rpc call will fail and view building coordinator will notice it
            std::rethrow_exception(eptr);
        }
    });
}

future<> view_building_worker::batch::do_process_staging(view_building_worker& local_vbw) {
    // TODO: handle process staging sstables tasks
    co_return;
}

}

}
