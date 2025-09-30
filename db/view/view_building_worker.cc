/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <iterator>
#include <ranges>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "db/view/view_building_worker.hh"
#include "db/view/view_consumer.hh"
#include "dht/token.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_group0_client.hh"
#include "schema/schema_fwd.hh"
#include "idl/view.dist.hh"
#include "sstables/sstables.hh"
#include "utils/exponential_backoff_retry.hh"

static logging::logger vbw_logger("view_building_worker");

namespace db {

namespace view {

// Called in the context of a seastar::thread.
class view_building_worker::consumer : public view_consumer {
    replica::database& _db;
    const std::vector<table_id> _views_ids;
    lw_shared_ptr<replica::table> _base;
    dht::decorated_key _current_key;

    mutation_reader& _reader;
    reader_permit _permit;

protected:
    virtual void load_views_to_build() override {
        _views_to_build = _views_ids | std::views::filter([this] (const auto& id) {
            return _db.column_family_exists(id);
        }) | std::views::transform([this] (const auto& id) {
            return view_ptr(_db.find_schema(id));
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
    consumer(replica::database& db, std::vector<table_id> views_ids, lw_shared_ptr<replica::table> base, mutation_reader& reader, reader_permit permit, shared_ptr<view_update_generator> gen, gc_clock::time_point now, abort_source& as)
            : view_consumer(std::move(gen), now, as)
            , _db(db)
            , _views_ids(std::move(views_ids))
            , _base(base)
            , _current_key(dht::minimum_token(), partition_key::make_empty())
            , _reader(reader)
            , _permit(std::move(permit)) {}

    dht::token consume_end_of_stream() {
        return _current_key.token();
    }
};

static shard_id get_sstable_shard_id(const sstables::sstable& sst) {
    auto shards = sst.get_shards_for_this_sstable();
#ifdef SEASTAR_DEBUG
    // Sstable from tablet-table should belong to only one shard
    SCYLLA_ASSERT(shards.size() == 1);
#endif
    return shards[0];
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

view_building_worker::view_building_worker(replica::database& db, db::system_keyspace& sys_ks, service::migration_notifier& mnotifier, service::raft_group0_client& group0_client, view_update_generator& vug, netw::messaging_service& ms, view_building_state_machine& vbsm)
        : _db(db)
        , _sys_ks(sys_ks)
        , _mnotifier(mnotifier)
        , _group0_client(group0_client)
        , _vug(vug)
        , _messaging(ms)
        , _vb_state_machine(vbsm)
        , _gate("view_building_worker_gate")
{
    init_messaging_service();
}

void view_building_worker::start_background_fibers() {
    SCYLLA_ASSERT(this_shard_id() == 0);
    _staging_sstables_registrator = run_staging_sstables_registrator();
    _view_building_state_observer = run_view_building_state_observer();
    _mnotifier.register_listener(this);
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
    _staging_sstables_mutex.broken();
    _sstables_to_register_event.broken();
    if (this_shard_id() == 0) {
        auto sstable_registrator = std::exchange(_staging_sstables_registrator, make_ready_future<>());
        co_await std::move(sstable_registrator);
        auto state_observer = std::exchange(_view_building_state_observer, make_ready_future<>());
        co_await std::move(state_observer);
        co_await _mnotifier.unregister_listener(this);
    }
    co_await _state.clear_state();
    _state.state_updated_cv.broken();
    co_await uninit_messaging_service();
}

future<> view_building_worker::stop() {
    co_await drain();
    co_await _gate.close();
}

void view_building_worker::on_drop_view(const sstring& ks_name, const sstring& view_name) {
    (void)with_gate(_gate, [&, this] {
        return _sys_ks.remove_view_build_progress_across_all_shards(ks_name, view_name);
    });
}

future<> view_building_worker::register_staging_sstable_tasks(std::vector<sstables::shared_sstable> ssts, table_id table_id) {
    auto& tablet_map = _db.get_token_metadata().tablets().get_tablet_map(table_id);
    auto staging_task_infos = ssts | std::views::as_rvalue | std::views::transform([&] (sstables::shared_sstable sst) {
        auto tid = get_sstable_tablet_id(tablet_map, *sst);
        return staging_sstable_task_info {
            .table_id = table_id,
            .shard = get_sstable_shard_id(*sst),
            .last_token = tablet_map.get_last_token(tid),
            .sst_foreign_ptr = make_foreign(std::move(sst))
        };
    }) | std::ranges::to<std::vector>();

    co_await container().invoke_on(0, [staging_task_infos = std::move(staging_task_infos), table_id] (view_building_worker& local_vbw) mutable -> future<> {
        try {
            auto lock = co_await get_units(local_vbw._staging_sstables_mutex, 1, local_vbw._as);
            vbw_logger.debug("Saving {} sstables for table {} to create view building tasks", staging_task_infos.size(), table_id);
            auto& sstables_queue = local_vbw._sstables_to_register[table_id];
            sstables_queue.insert(sstables_queue.end(), std::make_move_iterator(staging_task_infos.begin()), std::make_move_iterator(staging_task_infos.end()));
            local_vbw._sstables_to_register_event.broadcast();
        } catch (semaphore_aborted&) {
            vbw_logger.warn("Semaphore was aborted while waiting to register {} sstables for table {}", staging_task_infos.size(), table_id);
        }
    });
}

future<> view_building_worker::run_staging_sstables_registrator() {
    co_await discover_existing_staging_sstables();

    while (!_as.abort_requested()) {
        try {
            auto lock = co_await get_units(_staging_sstables_mutex, 1, _as);
            co_await create_staging_sstable_tasks();
            lock.return_all();
            _as.check();
            co_await _sstables_to_register_event.when();
        } catch (semaphore_aborted&) {
            vbw_logger.warn("Got semaphore_aborted while creating staging sstable tasks");
        } catch (broken_condition_variable&) {
            vbw_logger.warn("Got broken_condition_variable while creating staging sstable tasks");
        } catch (abort_requested_exception&) {
            vbw_logger.warn("Got abort_requested_exception while creating staging sstable tasks");
        } catch (service::group0_concurrent_modification&) {
            vbw_logger.warn("Got group0_concurrent_modification while creating staging sstable tasks");
        } catch (raft::request_aborted&) {
            vbw_logger.warn("Got raft::request_aborted while creating staging sstable tasks");
        }
    }
}

future<> view_building_worker::create_staging_sstable_tasks() {
    if (_sstables_to_register.empty()) {
        co_return;
    }

    utils::chunked_vector<canonical_mutation> cmuts;

    auto guard = co_await _group0_client.start_operation(_as);
    auto my_host_id = _db.get_token_metadata().get_topology().my_host_id();
    for (auto& [table_id, sst_infos]: _sstables_to_register) {
        for (auto& sst_info: sst_infos) {
            view_building_task task {
                utils::UUID_gen::get_time_UUID(), view_building_task::task_type::process_staging, view_building_task::task_state::idle,
                table_id, ::table_id{}, {my_host_id, sst_info.shard}, sst_info.last_token
            };
            auto mut = co_await _group0_client.sys_ks().make_view_building_task_mutation(guard.write_timestamp(), task);
            cmuts.emplace_back(std::move(mut));
        }
    }

    vbw_logger.debug("Creating {} process_staging view_building_tasks", cmuts.size());
    auto cmd = _group0_client.prepare_command(service::write_mutations{std::move(cmuts)}, guard, "create view building tasks");
    co_await _group0_client.add_entry(std::move(cmd), std::move(guard), _as);

    // Move staging sstables from `_sstables_to_register` (on shard0) to `_staging_sstables` on corresponding shards.
    // Firstly reorgenize `_sstables_to_register` for easier movement.
    // This is done in separate loop after committing the group0 command, because we need to move values from `_sstables_to_register`
    // (`staging_sstable_task_info` is non-copyable because of `foreign_ptr` field).
    std::unordered_map<shard_id, std::unordered_map<table_id, std::unordered_map<dht::token, std::vector<foreign_ptr<sstables::shared_sstable>>>>> new_sstables_per_shard;
    for (auto& [table_id, sst_infos]: _sstables_to_register) {
        for (auto& sst_info: sst_infos) {
            new_sstables_per_shard[sst_info.shard][table_id][sst_info.last_token].push_back(std::move(sst_info.sst_foreign_ptr));
        }
    }

    for (auto& [shard, sstables_per_table]: new_sstables_per_shard) {
        co_await container().invoke_on(shard, [sstables_for_this_shard = std::move(sstables_per_table)] (view_building_worker& local_vbw) mutable {
            for (auto& [tid, ssts_map]: sstables_for_this_shard) {
                for (auto& [token, ssts]: ssts_map) {
                    auto unwrapped_ssts = ssts | std::views::as_rvalue | std::views::transform([] (auto&& fptr) {
                        return fptr.unwrap_on_owner_shard();
                    }) | std::ranges::to<std::vector>();
                    auto& tid_ssts = local_vbw._staging_sstables[tid][token];
                    tid_ssts.insert(tid_ssts.end(), std::make_move_iterator(unwrapped_ssts.begin()), std::make_move_iterator(unwrapped_ssts.end()));
                }
            }
        });
    }
    _sstables_to_register.clear();
}

future<> view_building_worker::discover_existing_staging_sstables() {
    auto merge_maps = [] (auto& a, auto&& b) mutable {
        for (auto& [tid, ssts]: b) {
            auto& tid_ssts = a[tid];
            tid_ssts.insert(tid_ssts.end(), std::make_move_iterator(ssts.begin()), std::make_move_iterator(ssts.end()));
        }
    };
    
    auto lock = co_await get_units(_staging_sstables_mutex, 1, _as);
    auto new_staging_tasks = co_await container().map_reduce0([building_tasks = _vb_state_machine.building_state.tasks_state] (auto& vbw) -> future<std::unordered_map<table_id, std::vector<staging_sstable_task_info>>> {
        auto new_tasks = vbw.discover_local_staging_sstables(std::move(building_tasks));
        co_return new_tasks;
    }, std::unordered_map<table_id, std::vector<staging_sstable_task_info>>{}, [&] (std::unordered_map<table_id, std::vector<staging_sstable_task_info>> a, std::unordered_map<table_id, std::vector<staging_sstable_task_info>>&& b) {
        merge_maps(a, std::move(b));
        return a;
    });

    merge_maps(_sstables_to_register, std::move(new_staging_tasks));
}

static bool staging_task_exists(const building_tasks& tasks, table_id table_id, const locator::tablet_replica& replica, dht::token last_token) {
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
std::unordered_map<table_id, std::vector<view_building_worker::staging_sstable_task_info>> view_building_worker::discover_local_staging_sstables(building_tasks building_tasks) {
    std::unordered_map<table_id, std::vector<staging_sstable_task_info>> tasks_to_create;
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
                tasks_to_create[table_id].emplace_back(table_id, shard, last_token, make_foreign(std::move(sstable)));
            } else {
                _staging_sstables[table_id][last_token].push_back(std::move(sstable));
            }
        }
    });
    return tasks_to_create;
}

future<> view_building_worker::run_view_building_state_observer() {
    auto abort = _as.subscribe([this] () noexcept {
        _vb_state_machine.event.broadcast();
    });

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
// When we observe that a view is built, we also remove entries in `system.scylla_views_builds_in_progress`.
future<> view_building_worker::update_built_views() {
    auto id_to_name = [&] (table_id table_id) {
        auto schema = _db.find_schema(table_id);
        return std::make_pair(schema->ks_name(), schema->cf_name());
    };
    auto& sys_ks = _group0_client.sys_ks();

    std::set<std::pair<sstring, sstring>> built_views;
    for (auto& [id, statuses]: _vb_state_machine.views_state.status_map) {
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
            co_await sys_ks.remove_view_build_progress_across_all_shards(view.first, view.second);
        }
    }
}

future<> view_building_worker::update_building_state() {
    co_await _state.update(*this);
    co_await _state.finish_completed_tasks();
    _state.state_updated_cv.broadcast();
}

bool view_building_worker::is_shard_free(shard_id shard) {
    return !std::ranges::any_of(_state.tasks_map, [&shard] (auto& task_entry) {
        return task_entry.second->replica.shard == shard && task_entry.second->state == view_building_worker::batch_state::in_progress;
    });
}

void view_building_worker::init_messaging_service() {
    ser::view_rpc_verbs::register_work_on_view_building_tasks(&_messaging, [this] (std::vector<utils::UUID> ids) -> future<std::vector<view_task_result>> {
        return container().invoke_on(0, [ids = std::move(ids)] (view_building_worker& vbw) mutable -> future<std::vector<view_task_result>> {
            return vbw.work_on_tasks(std::move(ids));
        });
    });
}

future<> view_building_worker::uninit_messaging_service() {
    return ser::view_rpc_verbs::unregister(&_messaging);
}

future<std::vector<view_task_result>> view_building_worker::work_on_tasks(std::vector<utils::UUID> ids) {
    vbw_logger.debug("Got request for results of tasks: {}", ids);
    auto guard = co_await _group0_client.start_operation(_as, service::raft_timeout{});
    auto processing_base_table = _state.processing_base_table;

    auto are_tasks_finished = [&] () {
        return std::ranges::all_of(ids, [this] (const utils::UUID& id) {
            return _state.finished_tasks.contains(id) || _state.aborted_tasks.contains(id);
        });
    };

    auto get_results = [&] () -> std::vector<view_task_result> {
        std::vector<view_task_result> results;
        for (const auto& id: ids) {
            if (_state.finished_tasks.contains(id)) {
                results.emplace_back(view_task_result::command_status::success);
            } else if (_state.aborted_tasks.contains(id)) {
                results.emplace_back(view_task_result::command_status::abort);
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
    while (!_state.tasks_map.contains(id) && processing_base_table == _state.processing_base_table) {
        vbw_logger.warn("Batch with task {} is not found in tasks map, waiting until worker updates its state", id);
        service::release_guard(std::move(guard));
        co_await _state.state_updated_cv.wait();
        guard = co_await _group0_client.start_operation(_as, service::raft_timeout{});
    }

    if (processing_base_table != _state.processing_base_table) {
        // If the processing base table was changed, we should fail this RPC call because the tasks were aborted.
        throw std::runtime_error(fmt::format("Processing base table was changed to {} ", _state.processing_base_table));
    }

    // Validate that any of the IDs wasn't aborted.
    for (const auto& tid: ids) {
        if (!_state.tasks_map[id]->tasks.contains(tid)) {
            vbw_logger.warn("Task {} is not found in the batch", tid);
            throw std::runtime_error(fmt::format("Task {} is not found in the batch", tid));
        }
    }

    if (_state.tasks_map[id]->state == view_building_worker::batch_state::idle) {
        vbw_logger.debug("Starting batch with tasks {}", _state.tasks_map[id]->tasks);
        if (!is_shard_free(_state.tasks_map[id]->replica.shard)) {
            throw std::runtime_error(fmt::format("Tried to start view building tasks ({}) on shard {} but the shard is busy", _state.tasks_map[id]->tasks, _state.tasks_map[id]->replica.shard, _state.tasks_map[id]->tasks));
        }
        _state.tasks_map[id]->start();
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
static bool validate_can_be_one_batch(const view_building_task& t1, const view_building_task& t2) {
    return t1.type == t2.type && t1.base_id == t2.base_id && t1.replica == t2.replica && t1.last_token == t2.last_token;
}

static std::unordered_set<table_id> get_ids_of_all_views(replica::database& db, table_id table_id) {
    return db.find_column_family(table_id).views() | std::views::transform([] (view_ptr vptr) {
        return vptr->id();
    }) | std::ranges::to<std::unordered_set>();;
}

future<> view_building_worker::local_state::flush_table(view_building_worker& vbw, table_id table_id) {
    // `table_id` should point to currently processing base table but
    // `view_building_worker::local_state::processing_base_table` may not be set to it yet, 
    // so we need to pass it directly
    co_await vbw.container().invoke_on_all([table_id] (view_building_worker& local_vbw) -> future<> {
        auto base_cf = local_vbw._db.find_column_family(table_id).shared_from_this();
        co_await when_all(base_cf->await_pending_writes(), base_cf->await_pending_streams());
        co_await flush_base(base_cf, local_vbw._as);
    });

    flushed_views = get_ids_of_all_views(vbw._db, table_id);
}

future<> view_building_worker::local_state::update(view_building_worker& vbw) {
    const auto& vb_state = vbw._vb_state_machine.building_state;

    // Check if the base table to process was changed.
    // If so, we clear the state, aborting tasks for previous base table and starting new ones for the new base table.
    if (processing_base_table != vb_state.currently_processed_base_table) {
        co_await clear_state();

        if (vb_state.currently_processed_base_table) {
            // When we start to process new base table, we need to flush its current data, so we can build the view.
            co_await flush_table(vbw, *vb_state.currently_processed_base_table);
        }

        processing_base_table = vb_state.currently_processed_base_table;
        vbw_logger.info("Processing base table was changed to: {}", processing_base_table);
    }

    if (!processing_base_table) {
        vbw_logger.debug("No base table is selected to be processed.");
        co_return;
    }

    std::vector<table_id> new_views;
    auto all_view_ids = get_ids_of_all_views(vbw._db, *processing_base_table);
    std::ranges::set_difference(all_view_ids, flushed_views, std::back_inserter(new_views));
    if (!new_views.empty()) {
        // Flush base table again in any new view was created, so the view building tasks will see up-to-date sstables.
        // Otherwise, we may lose mutations created after previous flush but before the new view was created.
        co_await flush_table(vbw, *processing_base_table);
    }

    auto erm = vbw._db.find_column_family(*processing_base_table).get_effective_replication_map();
    auto my_host_id = erm->get_topology().my_host_id();
    auto current_tasks_for_this_host = vb_state.get_tasks_for_host(*processing_base_table, my_host_id);

    // scan view building state, collect alive and new (in STARTED state but not started by this worker) tasks
    std::unordered_map<shard_id, std::vector<view_building_task>> new_tasks;
    std::unordered_set<utils::UUID> alive_tasks; // save information about alive tasks to cleanup done/aborted ones
    for (auto& task_ref: current_tasks_for_this_host) {
        auto& task = task_ref.get();
        auto id = task.id;

        if (task.state != view_building_task::task_state::aborted) {
            alive_tasks.insert(id);
        }

        if (tasks_map.contains(id) || finished_tasks.contains(id)) {
            continue;
        }
        else if (task.state == view_building_task::task_state::started) {
            auto shard = task.replica.shard;
            if (new_tasks.contains(shard) && !validate_can_be_one_batch(new_tasks[shard].front(), task)) {
                // Currently we allow only one batch per shard at a time
                on_internal_error(vbw_logger, fmt::format("Got not-compatible tasks for the same shard. Task: {}, other: {}", new_tasks[shard].front(), task));
            }
            new_tasks[shard].push_back(task);
        }
        co_await coroutine::maybe_yield();
    }

    auto tasks_map_copy = tasks_map;

    // Clear aborted tasks from tasks_map
    for (auto it = tasks_map_copy.begin(); it != tasks_map_copy.end();) {
        if (!alive_tasks.contains(it->first)) {
            vbw_logger.debug("Aborting task {}", it->first);
            aborted_tasks.insert(it->first);
            co_await it->second->abort_task(it->first);
            it = tasks_map_copy.erase(it);
        } else {
            ++it;
        }
    }

    // Create batches for new tasks
    for (const auto& [shard, shard_tasks]: new_tasks) {
        auto tasks = shard_tasks | std::views::transform([] (const view_building_task& t) {
            return std::make_pair(t.id, t);
        }) | std::ranges::to<std::unordered_map>();
        auto batch = seastar::make_shared<view_building_worker::batch>(vbw.container(), tasks, shard_tasks.front().base_id, shard_tasks.front().replica);

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
            finished_tasks.insert(it->first);
            vbw_logger.info("Task {} was completed", it->first);
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
    flushed_views.clear();
    tasks_map.clear();
    finished_tasks.clear();
    aborted_tasks.clear();
    state_updated_cv.broadcast();
    vbw_logger.debug("View building worker state was cleared.");
}

view_building_worker::batch::batch(sharded<view_building_worker>& vbw, std::unordered_map<utils::UUID, view_building_task> tasks, table_id base_id, locator::tablet_replica replica)
    : base_id(base_id)
    , replica(replica)
    , tasks(std::move(tasks))
    , _vbw(vbw) {}

void view_building_worker::batch::start() {
    if (this_shard_id() != 0) {
        on_internal_error(vbw_logger, "view_building_worker::batch should be started on shard0");
    }

    state = batch_state::in_progress;
    work = smp::submit_to(replica.shard, [this] () -> future<> {
        return do_work();
    }).finally([this] () {
        state = batch_state::finished;
        _vbw.local()._vb_state_machine.event.broadcast();
    });
}

future<> view_building_worker::batch::abort_task(utils::UUID id) {
    tasks.erase(id);
    if (tasks.empty()) {
        co_await abort();
    }
}

future<> view_building_worker::batch::abort() {
    co_await smp::submit_to(replica.shard, [this] () {
        as.request_abort();
    });

    if (work.valid()) {
        co_await work.get_future();
    }
}

future<> view_building_worker::batch::do_work() {
    if (this_shard_id() != replica.shard) {
        on_internal_error(vbw_logger, fmt::format("view_building_worker::batch::do_work() should be executed on tasks shard "));
    }

    // At this point we assume all tasks are validated to be executed in the same batch
    vbw_logger.debug("Starting view building batch for tasks {}. Task type {}", tasks | std::views::keys, tasks.begin()->second.type);

    std::exception_ptr eptr;
    exponential_backoff_retry r(1s, 5min);
    while (!as.abort_requested() && !tasks.empty()) {
        if (eptr) {
            try {
                co_await r.retry(as);
            } catch (const sleep_aborted&) {
                break;
            }
            eptr = nullptr;
        }

        auto task = tasks.begin()->second;
        auto type = task.type;
        auto base_id = task.base_id;
        auto last_token = task.last_token;
        auto maybe_views_ids = tasks | std::views::values | std::views::transform(&view_building_task::view_id) | std::ranges::to<std::vector>();

        try {
            std::vector<table_id> views_ids;
            switch (type) {
            case view_building_task::task_type::build_range:
                views_ids = maybe_views_ids | std::views::transform([] (const auto& i) { return *i; }) | std::ranges::to<std::vector>();
                co_await _vbw.local().do_build_range(base_id, views_ids, last_token, as);
                break;
            case view_building_task::task_type::process_staging:
                co_await _vbw.local().do_process_staging(base_id, last_token);
                break;
            }
        } catch (seastar::abort_requested_exception&) {
            vbw_logger.debug("Batch aborted");
        } catch (...) {
            eptr = std::current_exception();
        }

        if (eptr) {
            vbw_logger.warn("Batch with tasks {} failed with error: {}", tasks | std::views::keys, eptr);
        } else {
            vbw_logger.debug("Batch with tasks {} finished", tasks | std::views::keys);
            break;
        }
    }

    _vbw.local()._vb_state_machine.event.broadcast();
}

future<> view_building_worker::do_build_range(table_id base_id, std::vector<table_id> views_ids, dht::token last_token, abort_source& as) {
    utils::get_local_injector().inject("do_build_range_fail",
            [] { throw std::runtime_error("do_build_range failed due to error injection"); });

    // Run the view building in the streaming scheduling group
    // so that it doesn't impact other tasks with higher priority.
    seastar::thread_attributes attr;
    attr.sched_group = _db.get_streaming_scheduling_group();
    return seastar::async(std::move(attr), [this, base_id, views_ids = std::move(views_ids), last_token, &as] {
        gc_clock::time_point now = gc_clock::now();
        auto base_cf = _db.find_column_family(base_id).shared_from_this();
        reader_permit permit = _db.get_reader_concurrency_semaphore().make_tracking_only_permit(nullptr, "build_views_range", db::no_timeout, {});
        auto slice = make_partition_slice(*base_cf->schema());
        auto range = get_tablet_token_range(base_id, last_token);
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
                _db,
                views_ids,
                base_cf,
                reader,
                permit,
                _vug.shared_from_this(),
                now,
                as));

        as.check();
        for (auto& vid: views_ids) {
            if (!_views_in_progress.contains(vid)) {
                auto view = _db.find_schema(vid);
                _sys_ks.register_view_for_building(view->ks_name(), view->cf_name(), dht::minimum_token()).get();
                _views_in_progress.insert(vid);
            }
        }

        as.check();
        std::exception_ptr eptr;
        try {
            utils::get_local_injector().inject("view_building_worker_pause_build_range_task", [&] (auto& handler) -> future<> {
                bool should_wait = true;
                auto maybe_raw_token = handler.template get<int64_t>("token");
                if (maybe_raw_token) {
                    // Wait only if this range contains given token
                    should_wait = range.contains(dht::token(*maybe_raw_token), std::compare_three_way{});
                }

                if (should_wait) {
                    vbw_logger.info("do_build_range: paused, waiting for message");
                    co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes(5), &as);
                }
            }).get();
            utils::get_local_injector().inject("view_building_worker_pause_before_consume", 5min, as).get();

            vbw_logger.info("Starting range {} building for base table: {}.{}", range, base_cf->schema()->ks_name(), base_cf->schema()->cf_name());
            auto end_token = reader.consume_in_thread(std::move(consumer));
            vbw_logger.info("Built range {} for base table: {}.{}", dht::token_range(range.start(), end_token), base_cf->schema()->ks_name(), base_cf->schema()->cf_name());
        } catch (seastar::abort_requested_exception&) {
            eptr = std::current_exception();
            vbw_logger.info("Building range {} for base table {} and views {} was aborted.", range, base_id, views_ids);
        } catch (...) {
            eptr = std::current_exception();
            vbw_logger.warn("Error during processing range {} for base table {} and views {}: ", range, base_id, views_ids, eptr);
        }
        reader.close().get();

        if (eptr) {
            std::rethrow_exception(eptr);
        }
    });
}

future<> view_building_worker::do_process_staging(table_id table_id, dht::token last_token) {
    if (_staging_sstables[table_id][last_token].empty()) {
        co_return;
    }

    auto table = _db.get_tables_metadata().get_table(table_id).shared_from_this();
    auto sstables = std::exchange(_staging_sstables[table_id][last_token], {});
    co_await _vug.process_staging_sstables(std::move(table), std::move(sstables));
}

}

}
