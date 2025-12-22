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
#include "service/raft/raft_group0.hh"
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

view_building_worker::view_building_worker(replica::database& db, db::system_keyspace& sys_ks, service::migration_notifier& mnotifier, service::raft_group0& group0, view_update_generator& vug, netw::messaging_service& ms, view_building_state_machine& vbsm)
        : _db(db)
        , _sys_ks(sys_ks)
        , _mnotifier(mnotifier)
        , _group0(group0)
        , _vug(vug)
        , _messaging(ms)
        , _vb_state_machine(vbsm)
        , _gate("view_building_worker_gate")
{
    init_messaging_service();
}

future<> view_building_worker::init() {
    SCYLLA_ASSERT(this_shard_id() == 0);
    co_await discover_existing_staging_sstables();
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
    _state._mutex.broken();
    _staging_sstables_mutex.broken();
    _sstables_to_register_event.broken();
    if (this_shard_id() == 0) {
        auto sstable_registrator = std::exchange(_staging_sstables_registrator, make_ready_future<>());
        co_await std::move(sstable_registrator);
        auto state_observer = std::exchange(_view_building_state_observer, make_ready_future<>());
        co_await std::move(state_observer);
        co_await _mnotifier.unregister_listener(this);
    }
    co_await _state.clear();
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
    while (!_as.abort_requested()) {
        bool sleep = false;
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
        } catch (...) {
            vbw_logger.error("Exception while creating staging sstable tasks: {}", std::current_exception());
            sleep = true;
        }

        if (sleep) {
            vbw_logger.debug("Sleeping after exception.");
            co_await seastar::sleep_abortable(1s, _as).handle_exception([] (auto x) { return make_ready_future<>(); });
        }
    }
}

future<> view_building_worker::create_staging_sstable_tasks() {
    if (_sstables_to_register.empty()) {
        co_return;
    }

    utils::chunked_vector<canonical_mutation> cmuts;

    auto guard = co_await _group0.client().start_operation(_as);
    auto my_host_id = _db.get_token_metadata().get_topology().my_host_id();
    for (auto& [table_id, sst_infos]: _sstables_to_register) {
        for (auto& sst_info: sst_infos) {
            view_building_task task {
                utils::UUID_gen::get_time_UUID(), view_building_task::task_type::process_staging, false,
                table_id, ::table_id{}, {my_host_id, sst_info.shard}, sst_info.last_token
            };
            auto mut = co_await _group0.client().sys_ks().make_view_building_task_mutation(guard.write_timestamp(), task);
            cmuts.emplace_back(std::move(mut));
        }
    }

    vbw_logger.debug("Creating {} process_staging view_building_tasks", cmuts.size());
    auto cmd = _group0.client().prepare_command(service::write_mutations{std::move(cmuts)}, guard, "create view building tasks");
    co_await _group0.client().add_entry(std::move(cmd), std::move(guard), _as);

    // Move staging sstables from `_sstables_to_register` (on shard0) to `_staging_sstables` on corresponding shards.
    // Firstly reorgenize `_sstables_to_register` for easier movement.
    // This is done in separate loop after committing the group0 command, because we need to move values from `_sstables_to_register`
    // (`staging_sstable_task_info` is non-copyable because of `foreign_ptr` field).
    std::unordered_map<shard_id, std::unordered_map<table_id, std::vector<foreign_ptr<sstables::shared_sstable>>>> new_sstables_per_shard;
    for (auto& [table_id, sst_infos]: _sstables_to_register) {
        for (auto& sst_info: sst_infos) {
            new_sstables_per_shard[sst_info.shard][table_id].push_back(std::move(sst_info.sst_foreign_ptr));
        }
    }

    for (auto& [shard, sstables_per_table]: new_sstables_per_shard) {
        co_await container().invoke_on(shard, [sstables_for_this_shard = std::move(sstables_per_table)] (view_building_worker& local_vbw) mutable {
            for (auto& [tid, ssts]: sstables_for_this_shard) {
                auto unwrapped_ssts = ssts | std::views::as_rvalue | std::views::transform([] (auto&& fptr) {
                    return fptr.unwrap_on_owner_shard();
                }) | std::ranges::to<std::vector>();
                auto& tid_ssts = local_vbw._staging_sstables[tid];
                tid_ssts.insert(tid_ssts.end(), std::make_move_iterator(unwrapped_ssts.begin()), std::make_move_iterator(unwrapped_ssts.end()));
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

        // scylladb/scylladb#26403: Make sure to access the tablets map via the effective replication map of the table object.
        // The token metadata object pointed to by the database (`_db.get_token_metadata()`) may not contain
        // the tablets map of the currently processed table yet. After #24414 is fixed, this should not matter anymore.
        auto& tablet_map = table->get_effective_replication_map()->get_token_metadata().tablets().get_tablet_map(table_id);
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
                _staging_sstables[table_id].push_back(std::move(sstable));
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
            auto read_apply_mutex_holder = co_await _group0.client().hold_read_apply_mutex(_as);

            co_await update_built_views();
            co_await check_for_aborted_tasks();
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
    auto& sys_ks = _group0.client().sys_ks();

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

// Must be executed on shard0
future<> view_building_worker::check_for_aborted_tasks() {
    return container().invoke_on_all([building_state = _vb_state_machine.building_state] (view_building_worker& vbw) -> future<> {
        auto lock = co_await get_units(vbw._state._mutex, 1, vbw._as);
        co_await vbw._state.update_processing_base_table(vbw._db, building_state, vbw._as);
        if (!vbw._state._batch) {
            co_return;
        }

        auto my_host_id = vbw._db.get_token_metadata().get_topology().my_host_id();
        auto my_replica = locator::tablet_replica{my_host_id, this_shard_id()};
        auto it = vbw._state._batch->tasks.begin();
        while (it != vbw._state._batch->tasks.end()) {
            auto id = it->first;
            auto task_opt = building_state.get_task(it->second.base_id, my_replica, id);

            ++it; // Advance the iterator before potentially removing the entry from the map.
            if (!task_opt || task_opt->get().aborted) {
                co_await vbw._state._batch->abort_task(id);
            }
        }

        if (vbw._state._batch->tasks.empty()) {
            co_await vbw._state.clean_up_after_batch();
        }
    });
}

void view_building_worker::init_messaging_service() {
    ser::view_rpc_verbs::register_work_on_view_building_tasks(&_messaging, [this] (raft::term_t term, shard_id shard, std::vector<utils::UUID> ids) -> future<std::vector<utils::UUID>> {
        return container().invoke_on(shard, [term, ids = std::move(ids)] (auto& vbw) mutable -> future<std::vector<utils::UUID>> {
            return vbw.work_on_tasks(term, std::move(ids));
        });
    });
}

future<> view_building_worker::uninit_messaging_service() {
    return ser::view_rpc_verbs::unregister(&_messaging);
}

static std::unordered_set<table_id> get_ids_of_all_views(replica::database& db, table_id table_id) {
    return db.find_column_family(table_id).views() | std::views::transform([] (view_ptr vptr) {
        return vptr->id();
    }) | std::ranges::to<std::unordered_set>();;
}

// If `state::processing_base_table` is different that the `view_building_state::currently_processed_base_table`,
// clear the state, save and flush new base table
future<> view_building_worker::state::update_processing_base_table(replica::database& db, const view_building_state& building_state, abort_source& as) {
    if (processing_base_table != building_state.currently_processed_base_table) {
        co_await clear();
        if (building_state.currently_processed_base_table) {
            co_await flush_base_table(db, *building_state.currently_processed_base_table, as);
        }
        processing_base_table = building_state.currently_processed_base_table;
    }
}

// If `_batch` ptr points to valid object, co_await its `work` future, save completed tasks and delete the object
future<> view_building_worker::state::clean_up_after_batch() {
    if (_batch) {
        co_await std::move(_batch->work);
        for (auto& [id, _]: _batch->tasks) {
            completed_tasks.insert(id);
        }
        _batch = nullptr;
    }
}

// Flush base table, set is as currently processing base table and save which views exist at the time of flush
future<> view_building_worker::state::flush_base_table(replica::database& db, table_id base_table_id, abort_source& as) {
    auto cf = db.find_column_family(base_table_id).shared_from_this();
    co_await when_all(cf->await_pending_writes(), cf->await_pending_streams());
    co_await flush_base(cf, as);
    processing_base_table = base_table_id;
    flushed_views = get_ids_of_all_views(db, base_table_id);
}

future<> view_building_worker::state::clear() {
    if (_batch) {
        _batch->as.request_abort();
        co_await std::move(_batch->work);
        _batch = nullptr;
    }
    processing_base_table.reset();
    completed_tasks.clear();
    flushed_views.clear();
}

view_building_worker::batch::batch(sharded<view_building_worker>& vbw, std::unordered_map<utils::UUID, view_building_task> tasks, table_id base_id, locator::tablet_replica replica)
    : base_id(base_id)
    , replica(replica)
    , tasks(std::move(tasks))
    , _vbw(vbw) {}

void view_building_worker::batch::start() {
    if (this_shard_id() != replica.shard) {
        on_internal_error(vbw_logger, "view_building_worker::batch should be started on replica shard");
    }

    work = do_work().finally([this] {
        promise.set_value();
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
                query::max_partitions,
                base_cf->get_compaction_manager().get_tombstone_gc_state());
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
    if (_staging_sstables[table_id].empty()) {
        co_return;
    }

    auto table = _db.get_tables_metadata().get_table(table_id).shared_from_this();
    auto& tablet_map = table->get_effective_replication_map()->get_token_metadata().tablets().get_tablet_map(table_id);
    auto tid = tablet_map.get_tablet_id(last_token);
    auto tablet_range = tablet_map.get_token_range(tid);

    // Select sstables belonging to the tablet (identified by `last_token`)
    std::vector<sstables::shared_sstable> sstables_to_process;
    for (auto& sst: _staging_sstables[table_id]) {
        auto sst_last_token = sst->get_last_decorated_key().token();
        if (tablet_range.contains(sst_last_token, dht::token_comparator())) {
            sstables_to_process.push_back(sst);
        }
    }

    co_await _vug.process_staging_sstables(std::move(table), sstables_to_process);

    try {
        // Remove processed sstables from `_staging_sstables` map
        auto lock = co_await get_units(_staging_sstables_mutex, 1, _as);
        std::unordered_set<sstables::shared_sstable> sstables_to_remove(sstables_to_process.begin(), sstables_to_process.end());
        auto [first, last] = std::ranges::remove_if(_staging_sstables[table_id], [&] (auto& sst) {
            return sstables_to_remove.contains(sst);
        });
        _staging_sstables[table_id].erase(first, last);
    } catch (semaphore_aborted&) {
        vbw_logger.warn("Semaphore was aborted while waiting to removed processed sstables for table {}", table_id);
    }
}

void view_building_worker::load_sstables(table_id table_id, std::vector<sstables::shared_sstable> ssts) {
    std::ranges::copy_if(std::move(ssts), std::back_inserter(_staging_sstables[table_id]), [] (auto& sst) {
        return sst->state() == sstables::sstable_state::staging;
    });
}

void view_building_worker::cleanup_staging_sstables(locator::effective_replication_map_ptr erm, table_id table_id, locator::tablet_id tid) {
    auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(table_id);
    auto tablet_range = tablet_map.get_token_range(tid);

    auto [first, last] = std::ranges::remove_if(_staging_sstables[table_id], [&] (auto& sst) {
        auto sst_last_token = sst->get_last_decorated_key().token();
        return tablet_range.contains(sst_last_token, dht::token_comparator());
    });
    _staging_sstables[table_id].erase(first, last);
}

future<view_building_state> view_building_worker::get_latest_view_building_state(raft::term_t term) {
    return smp::submit_to(0, [&sharded_vbw = container(), term] () -> future<view_building_state> {
        auto& vbw = sharded_vbw.local();
        // auto guard = vbw._group0.client().start_operation(vbw._as);

        auto& raft_server = vbw._group0.group0_server();
        auto group0_holder = vbw._group0.hold_group0_gate();
        co_await raft_server.read_barrier(&vbw._as);
        if (raft_server.get_current_term() != term) {
           throw std::runtime_error(fmt::format("Invalid raft term. Got {} but current term is {}", term, raft_server.get_current_term()));
        }

        co_return vbw._vb_state_machine.building_state;
    });
}

future<std::vector<utils::UUID>> view_building_worker::work_on_tasks(raft::term_t term, std::vector<utils::UUID> ids) {
    auto collect_completed_tasks = [&] {
        std::vector<utils::UUID> completed;
        for (auto& id: ids) {
            if (_state.completed_tasks.contains(id)) {
                completed.push_back(id);
            }
        }
        return completed;
    };

    auto lock = co_await get_units(_state._mutex, 1, _as);
    // Firstly check if there is any batch that is finished but wasn't cleaned up.
    if (_state._batch && _state._batch->promise.available()) {
        co_await _state.clean_up_after_batch();
    }

    // Check if tasks were already completed.
    // If only part of the tasks were finished, return the subset and don't execute the remaining tasks.
    std::vector<utils::UUID> completed = collect_completed_tasks();
    if (!completed.empty()) {
        co_return completed;
    }
    lock.return_all();

    auto building_state = co_await get_latest_view_building_state(term);

    lock = co_await get_units(_state._mutex, 1, _as);
    co_await _state.update_processing_base_table(_db, building_state, _as);
    // If there is no running batch, create it.
    if (!_state._batch) {
        if (!_state.processing_base_table) {
            throw std::runtime_error("view_building_worker::state::processing_base_table needs to be set to work on view building");
        }

        auto my_host_id = _db.get_token_metadata().get_topology().my_host_id();
        auto my_replica = locator::tablet_replica{my_host_id, this_shard_id()};
        std::unordered_map<utils::UUID, view_building_task> tasks;
        for (auto& id: ids) {
            auto task_opt = building_state.get_task(*_state.processing_base_table, my_replica, id);
            if (!task_opt) {
                throw std::runtime_error(fmt::format("Task {} was not found for base table {} on replica {}", id, *building_state.currently_processed_base_table, my_replica));
            }
            tasks.insert({id, *task_opt});
        }
#ifdef SEASTAR_DEBUG
        {
            auto& some_task = tasks.begin()->second;
            for (auto& [_, t]: tasks) {
                SCYLLA_ASSERT(t.base_id == some_task.base_id);
                SCYLLA_ASSERT(t.last_token == some_task.last_token);
                SCYLLA_ASSERT(t.replica == some_task.replica);
                SCYLLA_ASSERT(t.type == some_task.type);
                SCYLLA_ASSERT(t.replica.shard == this_shard_id());
            }
        }
#endif

        // If any view was added after we did the initial flush, we need to do it again
        if (std::ranges::any_of(tasks | std::views::values, [&] (const view_building_task& t) {
            return t.view_id && !_state.flushed_views.contains(*t.view_id);
        })) {
            co_await _state.flush_base_table(_db, *_state.processing_base_table, _as);
        }

        // Create and start the batch
        _state._batch = std::make_unique<batch>(container(), std::move(tasks), *building_state.currently_processed_base_table, my_replica);
        _state._batch->start();
    }

    if (std::ranges::all_of(ids, [&] (auto& id) { return !_state._batch->tasks.contains(id); })) {
        throw std::runtime_error(fmt::format(
                "None of the tasks requested to work on is executed in current view building batch. Batch executes: {}, the RPC requested: {}",
                _state._batch->tasks | std::views::keys, ids));
    }
    auto batch_future = _state._batch->promise.get_shared_future();
    lock.return_all();

    co_await std::move(batch_future);

    lock = co_await get_units(_state._mutex, 1, _as);
    co_await _state.clean_up_after_batch();
    co_return collect_completed_tasks();
}

}

}
