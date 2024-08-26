/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "index/secondary_index_manager.hh"
#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/execution_stage.hh>
#include "utils/assert.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include <seastar/core/distributed.hh>
#include <functional>
#include <unordered_map>
#include <set>
#include <boost/functional/hash.hpp>
#include <boost/range/algorithm/find.hpp>
#include <optional>
#include <string.h>
#include "types/types.hh"
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include "db/commitlog/replay_position.hh"
#include "db/commitlog/commitlog_types.hh"
#include "schema/schema_fwd.hh"
#include "db/view/view.hh"
#include "db/snapshot-ctl.hh"
#include "memtable.hh"
#include "row_cache.hh"
#include "query-result.hh"
#include "compaction/compaction_strategy.hh"
#include "utils/estimated_histogram.hh"
#include <seastar/core/metrics_registration.hh>
#include "db/view/view_stats.hh"
#include "db/view/view_update_backlog.hh"
#include "db/view/row_locking.hh"
#include "utils/phased_barrier.hh"
#include "backlog_controller.hh"
#include "dirty_memory_manager.hh"
#include "reader_concurrency_semaphore.hh"
#include "db/timeout_clock.hh"
#include "querier.hh"
#include "cache_temperature.hh"
#include <unordered_set>
#include "utils/error_injection.hh"
#include "utils/updateable_value.hh"
#include "data_dictionary/user_types_metadata.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "data_dictionary/data_dictionary.hh"
#include "absl-flat_hash_map.hh"
#include "utils/cross-shard-barrier.hh"
#include "sstables/generation_type.hh"
#include "db/rate_limiter.hh"
#include "db/operation_type.hh"
#include "locator/tablets.hh"
#include "utils/serialized_action.hh"
#include "compaction/compaction_fwd.hh"
#include "compaction_group.hh"

class cell_locker;
class cell_locker_stats;
class locked_cell;
class mutation;

class compaction_manager;
class frozen_mutation;
class reconcilable_result;

namespace tracing { class trace_state_ptr; }
namespace s3 { struct endpoint_config; }

namespace lang { class manager; }

namespace service {
class storage_proxy;
class storage_service;
class migration_notifier;
class raft_group_registry;
}

namespace gms {
class feature_service;
}

namespace sstables {

enum class sstable_state;
class sstable;
class compaction_descriptor;
class compaction_completion_desc;
class storage_manager;
class sstables_manager;
class compaction_data;
class sstable_set;
class directory_semaphore;
struct sstable_files_snapshot;
struct entry_descriptor;

}

namespace ser {
template<typename T>
class serializer;
}

namespace gms {
class gossiper;
}

namespace api {
class autocompaction_toggle_guard;
}

namespace db {
class commitlog;
class config;
class extensions;
class rp_handle;
class data_listeners;
class large_data_handler;
class system_keyspace;
class table_selector;

namespace view {
class view_update_generator;
}

}

class mutation_reordered_with_truncate_exception : public std::exception {};

class column_family_test;
class table_for_tests;
class database_test;
using sstable_list = sstables::sstable_list;

extern logging::logger dblog;

namespace replica {

using shared_memtable = lw_shared_ptr<memtable>;
class global_table_ptr;

// We could just add all memtables, regardless of types, to a single list, and
// then filter them out when we read them. Here's why I have chosen not to do
// it:
//
// First, some of the methods in which a memtable is involved (like seal) are
// assume a commitlog, and go through great care of updating the replay
// position, flushing the log, etc.  We want to bypass those, and that has to
// be done either by sprikling the seal code with conditionals, or having a
// separate method for each seal.
//
// Also, if we ever want to put some of the memtables in as separate allocator
// region group to provide for extra QoS, having the classes properly wrapped
// will make that trivial: just pass a version of new_memtable() that puts it
// in a different region, while the list approach would require a lot of
// conditionals as well.
//
// If we are going to have different methods, better have different instances
// of a common class.
class memtable_list {
public:
    using seal_immediate_fn_type = std::function<future<> (flush_permit&&)>;
private:
    std::vector<shared_memtable> _memtables;
    seal_immediate_fn_type _seal_immediate_fn;
    std::function<schema_ptr()> _current_schema;
    replica::dirty_memory_manager* _dirty_memory_manager;
    memtable_table_shared_data _table_shared_data;
    std::optional<shared_future<>> _flush_coalescing;
    seastar::scheduling_group _compaction_scheduling_group;
    replica::table_stats& _table_stats;
public:
    using iterator = decltype(_memtables)::iterator;
    using const_iterator = decltype(_memtables)::const_iterator;
public:
    memtable_list(
            seal_immediate_fn_type seal_immediate_fn,
            std::function<schema_ptr()> cs,
            dirty_memory_manager* dirty_memory_manager,
            memtable_table_shared_data& table_shared_data,
            replica::table_stats& table_stats,
            seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group())
        : _memtables({})
        , _seal_immediate_fn(seal_immediate_fn)
        , _current_schema(cs)
        , _dirty_memory_manager(dirty_memory_manager)
        , _table_shared_data(table_shared_data)
        , _compaction_scheduling_group(compaction_scheduling_group)
        , _table_stats(table_stats) {
        add_memtable();
    }

    memtable_list(std::function<schema_ptr()> cs, dirty_memory_manager* dirty_memory_manager,
            memtable_table_shared_data& table_shared_data,
            replica::table_stats& table_stats,
            seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group())
        : memtable_list({}, std::move(cs), dirty_memory_manager, table_shared_data, table_stats, compaction_scheduling_group) {
    }

    bool may_flush() const noexcept {
        return bool(_seal_immediate_fn);
    }

    bool can_flush() const noexcept {
        return may_flush() && !empty();
    }

    bool empty() const noexcept {
        for (auto& m : _memtables) {
           if (!m->empty()) {
               return false;
            }
        }
        return true;
    }
    shared_memtable back() const noexcept {
        return _memtables.back();
    }

    // # 8904 - this method is akin to std::set::erase(key_type), not
    // erase(iterator). Should be tolerant against non-existing.
    void erase(const shared_memtable& element) noexcept {
        auto i = boost::range::find(_memtables, element);
        if (i != _memtables.end()) {
            _memtables.erase(i);
        }
    }

    // Synchronously swaps the active memtable with a new, empty one,
    // returning the old memtables list.
    // Exception safe.
    std::vector<replica::shared_memtable> clear_and_add();

    size_t size() const noexcept {
        return _memtables.size();
    }

    future<> seal_active_memtable(flush_permit&& permit) noexcept {
        return _seal_immediate_fn(std::move(permit));
    }

    auto begin() noexcept {
        return _memtables.begin();
    }

    auto begin() const noexcept {
        return _memtables.begin();
    }

    auto end() noexcept {
        return _memtables.end();
    }

    auto end() const noexcept {
        return _memtables.end();
    }

    memtable& active_memtable() noexcept {
        return *_memtables.back();
    }

    void add_memtable() {
        _memtables.emplace_back(new_memtable());
    }

    dirty_memory_manager_logalloc::region_group& region_group() noexcept {
        return _dirty_memory_manager->region_group();
    }
    // This is used for explicit flushes. Will queue the memtable for flushing and proceed when the
    // dirty_memory_manager allows us to. We will not seal at this time since the flush itself
    // wouldn't happen anyway. Keeping the memtable in memory will potentially increase the time it
    // spends in memory allowing for more coalescing opportunities.
    // The returned future<> resolves when any pending flushes are complete and the memtable is sealed.
    future<> flush();
private:
    lw_shared_ptr<memtable> new_memtable();
};

class distributed_loader;
class table_populator;

// The CF has a "stats" structure. But we don't want all fields here,
// since some of them are fairly complex for exporting to collectd. Also,
// that structure matches what we export via the API, so better leave it
// untouched. And we need more fields. We will summarize it in here what
// we need.
struct cf_stats {
    int64_t pending_memtables_flushes_count = 0;
    int64_t pending_memtables_flushes_bytes = 0;
    int64_t failed_memtables_flushes_count = 0;

    // number of time the clustering filter was executed
    int64_t clustering_filter_count = 0;
    // sstables considered by the filter (so dividing this by the previous one we get average sstables per read)
    int64_t sstables_checked_by_clustering_filter = 0;
    // number of times the filter passed the fast-path checks
    int64_t clustering_filter_fast_path_count = 0;
    // how many sstables survived the clustering key checks
    int64_t surviving_sstables_after_clustering_filter = 0;

    // How many view updates were dropped due to overload.
    int64_t dropped_view_updates = 0;

    // How many times view building was paused (e.g. due to node unavailability)
    int64_t view_building_paused = 0;

    // How many view updates were processed for all tables
    uint64_t total_view_updates_pushed_local = 0;
    uint64_t total_view_updates_pushed_remote = 0;
    uint64_t total_view_updates_failed_local = 0;
    uint64_t total_view_updates_failed_remote = 0;

    // How many times we build view updates only to realize it's the wrong node and drop the update
    uint64_t total_view_updates_on_wrong_node = 0;
};

class table;
using column_family = table;
struct table_stats;
using column_family_stats = table_stats;

class database_sstable_write_monitor;

extern const ssize_t new_reader_base_cost;

struct table_stats {
    /** Number of times flush has resulted in the memtable being switched out. */
    int64_t memtable_switch_count = 0;
    /** Estimated number of tasks pending for this column family */
    int64_t pending_flushes = 0;
    int64_t live_disk_space_used = 0;
    int64_t total_disk_space_used = 0;
    int64_t live_sstable_count = 0;
    /** Estimated number of compactions pending for this column family */
    int64_t pending_compactions = 0;
    int64_t memtable_partition_insertions = 0;
    int64_t memtable_partition_hits = 0;
    int64_t memtable_range_tombstone_reads = 0;
    int64_t memtable_row_tombstone_reads = 0;
    int64_t tablet_count = 0;
    mutation_application_stats memtable_app_stats;
    utils::timed_rate_moving_average_summary_and_histogram reads{256};
    utils::timed_rate_moving_average_summary_and_histogram writes{256};
    utils::timed_rate_moving_average_summary_and_histogram cas_prepare{256};
    utils::timed_rate_moving_average_summary_and_histogram cas_accept{256};
    utils::timed_rate_moving_average_summary_and_histogram cas_learn{256};
    utils::estimated_histogram estimated_sstable_per_read{35};
    utils::timed_rate_moving_average_and_histogram tombstone_scanned;
    utils::timed_rate_moving_average_and_histogram live_scanned;
    utils::estimated_histogram estimated_coordinator_read;
};

using storage_options = data_dictionary::storage_options;

// Smart table pointer that guards the table object
// while it's being accessed asynchronously
class table_holder {
    gate::holder _holder;
    lw_shared_ptr<table> _table_ptr;
public:
    explicit table_holder(table&);

    const table& operator*() const noexcept {
        return *_table_ptr;
    }

    table& operator*() noexcept {
        return *_table_ptr;
    }

    const table* operator->() const noexcept {
        return _table_ptr.operator->();
    }

    table* operator->() noexcept {
        return _table_ptr.operator->();
    }
};

class table : public enable_lw_shared_from_this<table>
            , public weakly_referencable<table> {
public:
    struct config {
        std::vector<sstring> all_datadirs;
        sstring datadir;
        bool enable_disk_writes = true;
        bool enable_disk_reads = true;
        bool enable_cache = true;
        bool enable_commitlog = true;
        bool enable_incremental_backups = false;
        utils::updateable_value<bool> compaction_enforce_min_threshold{false};
        bool enable_dangerous_direct_import_of_cassandra_counters = false;
        replica::dirty_memory_manager* dirty_memory_manager = &default_dirty_memory_manager;
        reader_concurrency_semaphore* streaming_read_concurrency_semaphore;
        reader_concurrency_semaphore* compaction_concurrency_semaphore;
        replica::cf_stats* cf_stats = nullptr;
        seastar::scheduling_group memtable_scheduling_group;
        seastar::scheduling_group memtable_to_cache_scheduling_group;
        seastar::scheduling_group compaction_scheduling_group;
        seastar::scheduling_group memory_compaction_scheduling_group;
        seastar::scheduling_group statement_scheduling_group;
        seastar::scheduling_group streaming_scheduling_group;
        bool enable_metrics_reporting = false;
        bool enable_node_aggregated_table_metrics = true;
        size_t view_update_concurrency_semaphore_limit;
        db::data_listeners* data_listeners = nullptr;
        uint32_t tombstone_warn_threshold{0};
        unsigned x_log2_compaction_groups{0};
        utils::updateable_value<bool> enable_compacting_data_for_streaming_and_repair;
        utils::updateable_value<bool> enable_tombstone_gc_for_streaming_and_repair;
    };

    using snapshot_details = db::snapshot_ctl::table_snapshot_details;
    struct cache_hit_rate {
        cache_temperature rate;
        lowres_clock::time_point last_updated;
    };
private:
    schema_ptr _schema;
    config _config;
    locator::effective_replication_map_ptr _erm;
    lw_shared_ptr<const storage_options> _storage_opts;
    memtable_table_shared_data _memtable_shared_data;
    mutable table_stats _stats;
    mutable db::view::stats _view_stats;
    mutable row_locker::stats _row_locker_stats;

    uint64_t _failed_counter_applies_to_memtable = 0;

    template<typename... Args>
    void do_apply(compaction_group& cg, db::rp_handle&&, Args&&... args);

    lw_shared_ptr<memtable_list> make_memory_only_memtable_list();
    lw_shared_ptr<memtable_list> make_memtable_list(compaction_group& cg);

    compaction_manager& _compaction_manager;
    sstables::compaction_strategy _compaction_strategy;
    // The storage_group_manager manages either a single storage_group for vnodes or per-tablet storage_group for tablets.
    // It contains and manages both the compaction_groups list and the storage_groups vector.
    std::unique_ptr<storage_group_manager> _sg_manager;
    // Compound SSTable set for all the compaction groups, which is useful for operations spanning all of them.
    lw_shared_ptr<const sstables::sstable_set> _sstables;
    // Control background fibers waiting for sstables to be deleted
    seastar::gate _sstable_deletion_gate;
    // This semaphore ensures that an operation like snapshot won't have its selected
    // sstables deleted by compaction in parallel, a race condition which could
    // easily result in failure.
    seastar::named_semaphore _sstable_deletion_sem = {1, named_semaphore_exception_factory{"sstable deletion"}};
    // Ensures that concurrent updates to sstable set will work correctly
    seastar::named_semaphore _sstable_set_mutation_sem = {1, named_semaphore_exception_factory{"sstable set mutation"}};
    mutable row_cache _cache; // Cache covers only sstables.
    // Initialized when the table is populated via update_sstables_known_generation.
    std::optional<sstables::sstable_generation_generator> _sstable_generation_generator;

    db::replay_position _highest_rp;
    db::replay_position _flush_rp;
    db::replay_position _lowest_allowed_rp;

    // Provided by the database that owns this commitlog
    db::commitlog* _commitlog;
    // The table is constructed in readonly mode - this flag is true after the constructor finishes.
    // This allows to read the table on the early stages of the node boot process,
    // when the commitlog is not yet initialized.
    // The flag is set to false in mark_ready_for_writes function, which
    // is called when the commitlog is created and the table is ready to accept writes.
    bool _readonly;
    bool _durable_writes;
    sstables::sstables_manager& _sstables_manager;
    secondary_index::secondary_index_manager _index_manager;
    bool _compaction_disabled_by_user = false;
    bool _tombstone_gc_enabled = true;
    utils::phased_barrier _flush_barrier;
    std::vector<view_ptr> _views;

    std::unique_ptr<cell_locker> _counter_cell_locks; // Memory-intensive; allocate only when needed.

    // Labels used to identify writes and reads for this table in the rate_limiter structure.
    db::rate_limiter::label _rate_limiter_label_for_writes;
    db::rate_limiter::label _rate_limiter_label_for_reads;

    void set_metrics();
    seastar::metrics::metric_groups _metrics;

    // holds average cache hit rate of all shards
    // recalculated periodically
    cache_temperature _global_cache_hit_rate = cache_temperature(0.0f);

    // holds cache hit rates per each node in a cluster
    // may not have information for some node, since it fills
    // in dynamically
    std::unordered_map<gms::inet_address, cache_hit_rate> _cluster_cache_hit_rates;

    // Operations like truncate, flush, query, etc, may depend on a column family being alive to
    // complete.  Some of them have their own gate already (like flush), used in specialized wait
    // logic. That is particularly useful if there is a particular
    // order in which we need to close those gates. For all the others operations that don't have
    // such needs, we have this generic _async_gate, which all potentially asynchronous operations
    // have to get.  It will be closed by stop().
    seastar::gate _async_gate;

    double _cached_percentile = -1;
    lowres_clock::time_point _percentile_cache_timestamp;
    std::chrono::milliseconds _percentile_cache_value;

    // Phaser used to synchronize with in-progress writes. This is useful for code that,
    // after some modification, needs to ensure that news writes will see it before
    // it can proceed, such as the view building code.
    utils::phased_barrier _pending_writes_phaser;
    // Corresponding phaser for in-progress reads.
    utils::phased_barrier _pending_reads_phaser;
    // Corresponding phaser for in-progress streams
    utils::phased_barrier _pending_streams_phaser;
    // Corresponding phaser for in-progress flushes
    utils::phased_barrier _pending_flushes_phaser;

    // This field cashes the last truncation time for the table.
    // The master resides in system.truncated table
    std::optional<db_clock::time_point> _truncated_at;

    bool _is_bootstrap_or_replace = false;
    sstables::shared_sstable make_sstable(sstables::sstable_state state);

public:
    void deregister_metrics();

    data_dictionary::table as_data_dictionary() const;

    future<> add_sstable_and_update_cache(sstables::shared_sstable sst,
                                          sstables::offstrategy offstrategy = sstables::offstrategy::no);
    future<> add_sstables_and_update_cache(const std::vector<sstables::shared_sstable>& ssts);
    future<> move_sstables_from_staging(std::vector<sstables::shared_sstable>);
    sstables::shared_sstable make_sstable();
    void set_truncation_time(db_clock::time_point truncated_at) noexcept {
        _truncated_at = truncated_at;
    }
    db_clock::time_point get_truncation_time() const;

    void notify_bootstrap_or_replace_start();

    void notify_bootstrap_or_replace_end();

    // Ensures that concurrent preemptible mutations to sstable lists will produce correct results.
    // User will hold this permit until done with all updates. As soon as it's released, another concurrent
    // attempt to update the lists will be able to proceed.
    struct sstable_list_builder {
        using permit_t = semaphore_units<seastar::named_semaphore_exception_factory>;
        permit_t permit;

        explicit sstable_list_builder(permit_t p) : permit(std::move(p)) {}
        sstable_list_builder& operator=(const sstable_list_builder&) = delete;
        sstable_list_builder(const sstable_list_builder&) = delete;

        // Builds new sstable set from existing one, with new sstables added to it and old sstables removed from it.
        future<lw_shared_ptr<sstables::sstable_set>>
        build_new_list(const sstables::sstable_set& current_sstables,
                       sstables::sstable_set new_sstable_list,
                       const std::vector<sstables::shared_sstable>& new_sstables,
                       const std::vector<sstables::shared_sstable>& old_sstables);
    };

    // Precondition: table needs tablet splitting.
    // Returns true if all storage of table is ready for splitting.
    bool all_storage_groups_split();
    future<> split_all_storage_groups();

    // Splits compaction group of a single tablet, if and only if the underlying table has
    // split request emitted by coordinator (found in tablet metadata).
    // If split is required, then the compaction group of the given tablet is guaranteed to
    // be split once it returns.
    future<> maybe_split_compaction_group_of(locator::tablet_id);
private:
    // If SSTable doesn't need split, the same input SSTable is returned as output.
    // If SSTable needs split, then output SSTables are returned and the input SSTable is deleted.
    // NOTE: it must only be used on new SSTables that weren't added to the set yet.
    future<std::vector<sstables::shared_sstable>> maybe_split_new_sstable(const sstables::shared_sstable&);

    // Called when coordinator executes tablet splitting, i.e. commit the new tablet map with
    // each tablet split into two, so this replica will remap all of its compaction groups
    // that were previously split.
    future<> handle_tablet_split_completion(size_t old_tablet_count, const locator::tablet_map& new_tmap);

    // Select a storage group from a given token.
    storage_group& storage_group_for_token(dht::token token) const noexcept;
    storage_group& storage_group_for_id(size_t i) const;

    std::unique_ptr<storage_group_manager> make_storage_group_manager();
    compaction_group* get_compaction_group(size_t id) const noexcept;
    // Select a compaction group from a given token.
    compaction_group& compaction_group_for_token(dht::token token) const noexcept;
    // Return compaction groups, present in this shard, that own a particular token range.
    utils::chunked_vector<compaction_group*> compaction_groups_for_token_range(dht::token_range tr) const;
    // Select a compaction group from a given key.
    compaction_group& compaction_group_for_key(partition_key_view key, const schema_ptr& s) const noexcept;
    // Select a compaction group from a given sstable based on its token range.
    compaction_group& compaction_group_for_sstable(const sstables::shared_sstable& sst) const noexcept;
    // Safely iterate through compaction groups, while performing async operations on them.
    future<> parallel_foreach_compaction_group(std::function<future<>(compaction_group&)> action);
    void for_each_compaction_group(std::function<void(compaction_group&)> action);
    void for_each_compaction_group(std::function<void(const compaction_group&)> action) const;
    // Unsafe reference to all storage groups. Don't use it across preemption points.
    const storage_group_map& storage_groups() const;

    // Safely iterate through SSTables, with deletion guard taken to make sure they're not
    // removed during iteration.
    // WARNING: Be careful that the action doesn't perform an operation that will itself
    // take the deletion guard, as that will cause a deadlock. For example, memtable flush
    // can wait on compaction (backpressure) which in turn takes deletion guard on completion.
    future<> safe_foreach_sstable(const sstables::sstable_set&, noncopyable_function<future<>(const sstables::shared_sstable&)> action);

    bool cache_enabled() const {
        return _config.enable_cache && _schema->caching_options().enabled();
    }
    void update_stats_for_new_sstable(const sstables::shared_sstable& sst) noexcept;
    future<> do_add_sstable_and_update_cache(compaction_group& cg, sstables::shared_sstable sst, sstables::offstrategy, bool trigger_compaction);
    future<> do_add_sstable_and_update_cache(sstables::shared_sstable sst, sstables::offstrategy offstrategy, bool trigger_compaction);
    // Helpers which add sstable on behalf of a compaction group and refreshes compound set.
    void add_sstable(compaction_group& cg, sstables::shared_sstable sstable);
    void add_maintenance_sstable(compaction_group& cg, sstables::shared_sstable sst);
    static void add_sstable_to_backlog_tracker(compaction_backlog_tracker& tracker, sstables::shared_sstable sstable);
    static void remove_sstable_from_backlog_tracker(compaction_backlog_tracker& tracker, sstables::shared_sstable sstable);
    lw_shared_ptr<memtable> new_memtable();
    future<> try_flush_memtable_to_sstable(compaction_group& cg, lw_shared_ptr<memtable> memt, sstable_write_permit&& permit);
    // Caller must keep m alive.
    future<> update_cache(compaction_group& cg, lw_shared_ptr<memtable> m, std::vector<sstables::shared_sstable> ssts);
    struct merge_comparator;

    // update the sstable generation, making sure (in calculate_generation_for_new_table)
    // that new new sstables don't overwrite this one.
    void update_sstables_known_generation(sstables::generation_type generation);

    sstables::generation_type calculate_generation_for_new_table();
private:
    void rebuild_statistics();
    void subtract_compaction_group_from_stats(const compaction_group& cg) noexcept;
private:
    mutation_source_opt _virtual_reader;
    std::optional<noncopyable_function<future<>(const frozen_mutation&)>> _virtual_writer;

    // Creates a mutation reader which covers given sstables.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // The 'range' parameter must be live as long as the reader is used.
    // Mutations returned by the reader will all have given schema.
    mutation_reader make_sstable_reader(schema_ptr schema,
                                        reader_permit permit,
                                        lw_shared_ptr<const sstables::sstable_set> sstables,
                                        const dht::partition_range& range,
                                        const query::partition_slice& slice,
                                        tracing::trace_state_ptr trace_state,
                                        streamed_mutation::forwarding fwd,
                                        mutation_reader::forwarding fwd_mr,
                                        const sstables::sstable_predicate& = sstables::default_sstable_predicate()) const;

    lw_shared_ptr<sstables::sstable_set> make_maintenance_sstable_set() const;
    lw_shared_ptr<const sstables::sstable_set> make_compound_sstable_set() const;
    // Compound sstable set must be refreshed whenever any of its managed sets are changed
    void refresh_compound_sstable_set();

    snapshot_source sstables_as_snapshot_source();
    partition_presence_checker make_partition_presence_checker(lw_shared_ptr<const sstables::sstable_set>);
    std::chrono::steady_clock::time_point _sstable_writes_disabled_at;

    dirty_memory_manager_logalloc::region_group& dirty_memory_region_group() const {
        return _config.dirty_memory_manager->region_group();
    }

    // reserve_fn will be called before any element is added to readers
    void add_memtables_to_reader_list(std::vector<mutation_reader>& readers,
             const schema_ptr& s,
             const reader_permit& permit,
             const dht::partition_range& range,
             const query::partition_slice& slice,
             const tracing::trace_state_ptr& trace_state,
             streamed_mutation::forwarding fwd,
             mutation_reader::forwarding fwd_mr,
             std::function<void(size_t)> reserve_fn) const;
public:
    sstring dir() const {
        return _config.datadir;
    }

    const storage_options& get_storage_options() const noexcept { return *_storage_opts; }
    lw_shared_ptr<const storage_options> get_storage_options_ptr() const noexcept { return _storage_opts; }
    future<> init_storage();
    future<> destroy_storage();

    seastar::gate& async_gate() { return _async_gate; }

    uint64_t failed_counter_applies_to_memtable() const {
        return _failed_counter_applies_to_memtable;
    }

    // This function should be called when this column family is ready for writes, IOW,
    // to produce SSTables. Extensive details about why this is important can be found
    // in Scylla's Github Issue #1014
    //
    // Nothing should be writing to SSTables before we have the chance to populate the
    // existing SSTables and calculate what should the next generation number be.
    //
    // However, if that happens, we want to protect against it in a way that does not
    // involve overwriting existing tables. This is one of the ways to do it: every
    // column family starts in an unwriteable state, and when it can finally be written
    // to, we mark it as writeable.
    //
    // Note that this *cannot* be a part of add_column_family. That adds a column family
    // to a db in memory only, and if anybody is about to write to a CF, that was most
    // likely already called. We need to call this explicitly when we are sure we're ready
    // to issue disk operations safely.
    void mark_ready_for_writes(db::commitlog* cl);

    // Creates a mutation reader which covers all data sources for this column family.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // Note: for data queries use query() instead.
    // The 'range' parameter must be live as long as the reader is used.
    // Mutations returned by the reader will all have given schema.
    mutation_reader make_reader_v2(schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state = nullptr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const;
    mutation_reader make_reader_v2_excluding_staging(schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state = nullptr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const;

    mutation_reader make_reader_v2(schema_ptr schema, reader_permit permit, const dht::partition_range& range = query::full_partition_range) const {
        auto& full_slice = schema->full_slice();
        return make_reader_v2(std::move(schema), std::move(permit), range, full_slice);
    }

    // The streaming mutation reader differs from the regular mutation reader in that:
    //  - Reflects all writes accepted by replica prior to creation of the
    //    reader and a _bounded_ amount of writes which arrive later.
    //  - Does not populate the cache
    // Requires ranges to be sorted and disjoint.
    // When compaction_time is engaged, the reader's output will be compacted, with the provided query time.
    // This compaction doesn't do tombstone garbage collection.
    mutation_reader make_streaming_reader(schema_ptr schema, reader_permit permit,
            const dht::partition_range_vector& ranges, gc_clock::time_point compaction_time) const;

    // Single range overload.
    mutation_reader make_streaming_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
            const query::partition_slice& slice,
            mutation_reader::forwarding fwd_mr,
            gc_clock::time_point compaction_time) const;

    mutation_reader make_streaming_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range, gc_clock::time_point compaction_time) {
        return make_streaming_reader(schema, std::move(permit), range, schema->full_slice(), mutation_reader::forwarding::no, compaction_time);
    }

    // Stream reader from the given sstables
    mutation_reader make_streaming_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
            lw_shared_ptr<sstables::sstable_set> sstables, gc_clock::time_point compaction_time) const;

    // Make a reader which reads only from the row-cache.
    // The reader doesn't populate the cache, it reads only what is in the cache
    // Supports reading only a single partition.
    // Does not support reading in reverse.
    mutation_reader make_nonpopulating_cache_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
            const query::partition_slice& slice, tracing::trace_state_ptr ts);

    sstables::shared_sstable make_streaming_sstable_for_write();
    sstables::shared_sstable make_streaming_staging_sstable();

    mutation_source as_mutation_source() const;
    mutation_source as_mutation_source_excluding_staging() const;

    // Select all memtables which contain this token and return them as mutation sources.
    // We could return memtables here, but table has no public memtable accessors so far.
    // Memtables are mutable objects, so it is best to keep it this way.
    std::vector<mutation_source> select_memtables_as_mutation_sources(dht::token) const;

    void set_virtual_reader(mutation_source virtual_reader) {
        _virtual_reader = std::move(virtual_reader);
    }

    void set_virtual_writer(noncopyable_function<future<>(const frozen_mutation&)> writer) {
        _virtual_writer.emplace(std::move(writer));
    }

    // Queries can be satisfied from multiple data sources, so they are returned
    // as temporaries.
    //
    // FIXME: in case a query is satisfied from a single memtable, avoid a copy
    using const_mutation_partition_ptr = std::unique_ptr<const mutation_partition>;
    using const_row_ptr = std::unique_ptr<const row>;
    // Allow an action to be performed on each active memtable, each of which belongs to a different compaction group.
    void for_each_active_memtable(noncopyable_function<void(memtable&)> action);
    api::timestamp_type min_memtable_timestamp() const;
    const row_cache& get_row_cache() const {
        return _cache;
    }

    row_cache& get_row_cache() {
        return _cache;
    }

    db::rate_limiter::label& get_rate_limiter_label_for_op_type(db::operation_type op_type) {
        switch (op_type) {
        case db::operation_type::write:
            return _rate_limiter_label_for_writes;
        case db::operation_type::read:
            return _rate_limiter_label_for_reads;
        }
        std::abort(); // compiler will error if we get here
    }

    db::rate_limiter::label& get_rate_limiter_label_for_writes() {
        return _rate_limiter_label_for_writes;
    }

    db::rate_limiter::label& get_rate_limiter_label_for_reads() {
        return _rate_limiter_label_for_reads;
    }

    future<std::vector<locked_cell>> lock_counter_cells(const mutation& m, db::timeout_clock::time_point timeout);

    logalloc::occupancy_stats occupancy() const;
public:
    table(schema_ptr schema, config cfg, lw_shared_ptr<const storage_options> sopts, compaction_manager& cm, sstables::sstables_manager& sm, cell_locker_stats& cl_stats, cache_tracker& row_cache_tracker, locator::effective_replication_map_ptr erm);

    table(column_family&&) = delete; // 'this' is being captured during construction
    ~table();

    table_holder hold() {
        return table_holder(*this);
    }

    const schema_ptr& schema() const { return _schema; }
    void set_schema(schema_ptr);
    db::commitlog* commitlog() const;
    const locator::effective_replication_map_ptr& get_effective_replication_map() const { return _erm; }
    future<> update_effective_replication_map(locator::effective_replication_map_ptr);
    [[gnu::always_inline]] bool uses_tablets() const;
private:
    future<> clear_inactive_reads_for_tablet(database& db, storage_group& sg);
    future<> stop_compaction_groups(storage_group& sg);
    future<> flush_compaction_groups(storage_group& sg);
    future<> cleanup_compaction_groups(database& db, db::system_keyspace& sys_ks, locator::tablet_id tid, storage_group& sg);
public:
    future<> cleanup_tablet(database&, db::system_keyspace&, locator::tablet_id);
    // For tests only.
    future<> cleanup_tablet_without_deallocation(database& db, db::system_keyspace& sys_ks, locator::tablet_id tid);
    future<const_mutation_partition_ptr> find_partition(schema_ptr, reader_permit permit, const dht::decorated_key& key) const;
    future<const_row_ptr> find_row(schema_ptr, reader_permit permit, const dht::decorated_key& partition_key, clustering_key clustering_key) const;
    shard_id shard_for_reads(dht::token t) const;
    dht::shard_replica_set shard_for_writes(dht::token t) const;
    // Applies given mutation to this column family
    // The mutation is always upgraded to current schema.
    void apply(const frozen_mutation& m, const schema_ptr& m_schema, db::rp_handle&& h = {}) {
        do_apply(compaction_group_for_key(m.key(), m_schema), std::move(h), m, m_schema);
    }
    void apply(const mutation& m, db::rp_handle&& h = {}) {
        do_apply(compaction_group_for_token(m.token()), std::move(h), m);
    }

    future<> apply(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&& h, db::timeout_clock::time_point tmo);
    future<> apply(const mutation& m, db::rp_handle&& h, db::timeout_clock::time_point tmo);

    // Returns at most "cmd.limit" rows
    // The saved_querier parameter is an input-output parameter which contains
    // the saved querier from the previous page (if there was one) and after
    // completion it contains the to-be saved querier for the next page (if
    // there is one). Pass nullptr when queriers are not saved.
    future<lw_shared_ptr<query::result>>
    query(schema_ptr,
        reader_permit permit,
        const query::read_command& cmd,
        query::result_options opts,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        query::result_memory_limiter& memory_limiter,
        db::timeout_clock::time_point timeout,
        std::optional<query::querier>* saved_querier = { });

    // Performs a query on given data source returning data in reconcilable form.
    //
    // Reads at most row_limit rows. If less rows are returned, the data source
    // didn't have more live data satisfying the query.
    //
    // Any cells which have expired according to query_time are returned as
    // deleted cells and do not count towards live data. The mutations are
    // compact, meaning that any cell which is covered by higher-level tombstone
    // is absent in the results.
    //
    // 'source' doesn't have to survive deferring.
    //
    // The saved_querier parameter is an input-output parameter which contains
    // the saved querier from the previous page (if there was one) and after
    // completion it contains the to-be saved querier for the next page (if
    // there is one). Pass nullptr when queriers are not saved.
    future<reconcilable_result>
    mutation_query(schema_ptr query_schema,
            reader_permit permit,
            const query::read_command& cmd,
            const dht::partition_range& range,
            tracing::trace_state_ptr trace_state,
            query::result_memory_accounter accounter,
            db::timeout_clock::time_point timeout,
            std::optional<query::querier>* saved_querier = { });

    void start();
    future<> stop();
    future<> flush(std::optional<db::replay_position> = {});
    future<> clear(); // discards memtable(s) without flushing them to disk.
    future<db::replay_position> discard_sstables(db_clock::time_point);

    bool can_flush() const;

    using do_flush = bool_class<struct do_flush_tag>;

    // Start a compaction of all sstables in a process known as major compaction
    // Active memtable is flushed first to guarantee that data like tombstone,
    // sitting in the memtable, will be compacted with shadowed data.
    future<> compact_all_sstables(tasks::task_info info, do_flush = do_flush::yes);

    future<bool> snapshot_exists(sstring name);

    db::replay_position set_low_replay_position_mark();

private:
    using snapshot_file_set = foreign_ptr<std::unique_ptr<std::unordered_set<sstring>>>;

    future<snapshot_file_set> take_snapshot(database& db, sstring jsondir);
    // Writes the table schema and the manifest of all files in the snapshot directory.
    future<> finalize_snapshot(database& db, sstring jsondir, std::vector<snapshot_file_set> file_sets);
    static future<> seal_snapshot(sstring jsondir, std::vector<snapshot_file_set> file_sets);
public:
    static future<> snapshot_on_all_shards(sharded<database>& sharded_db, const global_table_ptr& table_shards, sstring name);

    future<std::unordered_map<sstring, snapshot_details>> get_snapshot_details();
    static future<snapshot_details> get_snapshot_details(std::filesystem::path snapshot_dir, std::filesystem::path datadir);

    /*!
     * \brief write the schema to a 'schema.cql' file at the given directory.
     *
     * When doing a snapshot, the snapshot directory contains a 'schema.cql' file
     * with a CQL command that can be used to generate the schema.
     * The content is is similar to the result of the CQL DESCRIBE command of the table.
     *
     * When a schema has indexes, local indexes or views, those indexes and views
     * are represented by their own schemas.
     * In those cases, the method would write the relevant information for each of the schemas:
     *
     * The schema of the base table would output a file with the CREATE TABLE command
     * and the schema of the view that is used for the index would output a file with the
     * CREATE INDEX command.
     * The same is true for local index and MATERIALIZED VIEW.
     */
    future<> write_schema_as_cql(database& db, sstring dir) const;

    bool incremental_backups_enabled() const {
        return _config.enable_incremental_backups;
    }

    void set_incremental_backups(bool val) {
        _config.enable_incremental_backups = val;
    }

    bool uses_static_sharding() const {
        return !_erm || _erm->get_replication_strategy().is_vnode_based();
    }

    /*!
     * \brief get sstables by key
     * Return a set of the sstables names that contain the given
     * partition key in nodetool format
     */
    future<std::unordered_set<sstables::shared_sstable>> get_sstables_by_partition_key(const sstring& key) const;

    const sstables::sstable_set& get_sstable_set() const;
    lw_shared_ptr<const sstable_list> get_sstables() const;
    lw_shared_ptr<const sstable_list> get_sstables_including_compacted_undeleted() const;
    std::vector<sstables::shared_sstable> select_sstables(const dht::partition_range& range) const;
    size_t sstables_count() const;
    std::vector<uint64_t> sstable_count_per_level() const;
    int64_t get_unleveled_sstables() const;

    void start_compaction();
    void trigger_compaction();
    void try_trigger_compaction(compaction_group& cg) noexcept;
    // Triggers offstrategy compaction, if needed, in the background.
    void trigger_offstrategy_compaction();
    // Performs offstrategy compaction, if needed, returning
    // a future<bool> that is resolved when offstrategy_compaction completes.
    // The future value is true iff offstrategy compaction was required.
    future<bool> perform_offstrategy_compaction(tasks::task_info info);
    future<> perform_cleanup_compaction(owned_ranges_ptr sorted_owned_ranges,
                                        tasks::task_info info,
                                        do_flush = do_flush::yes);
    unsigned estimate_pending_compactions() const;

    void set_compaction_strategy(sstables::compaction_strategy_type strategy);
    const sstables::compaction_strategy& get_compaction_strategy() const {
        return _compaction_strategy;
    }

    sstables::compaction_strategy& get_compaction_strategy() {
        return _compaction_strategy;
    }

    const compaction_manager& get_compaction_manager() const noexcept {
        return _compaction_manager;
    }

    compaction_manager& get_compaction_manager() noexcept {
        return _compaction_manager;
    }

    table_stats& get_stats() const {
        return _stats;
    }

    // The tablet filter is used to not double account migrating tablets, so it's important that
    // only one of pending or leaving replica is accounted based on current migration stage.
    locator::table_load_stats table_load_stats(std::function<bool(const locator::tablet_map&, locator::global_tablet_id)> tablet_filter) const noexcept;

    const db::view::stats& get_view_stats() const {
        return _view_stats;
    }

    db::view::stats& view_stats() const noexcept {
        return _view_stats;
    }

    replica::cf_stats* cf_stats() const {
        return _config.cf_stats;
    }

    const config& get_config() const {
        return _config;
    }

    cache_temperature get_global_cache_hit_rate() const {
        return _global_cache_hit_rate;
    }

    bool durable_writes() const {
        return _durable_writes;
    }

    void set_durable_writes(bool dw) {
        _durable_writes = dw;
    }

    void set_global_cache_hit_rate(cache_temperature rate) {
        _global_cache_hit_rate = rate;
    }

    void set_hit_rate(gms::inet_address addr, cache_temperature rate);
    cache_hit_rate get_my_hit_rate() const;
    cache_hit_rate get_hit_rate(const gms::gossiper& g, gms::inet_address addr);
    void drop_hit_rate(gms::inet_address addr);

    void enable_auto_compaction();
    future<> disable_auto_compaction();

    void set_tombstone_gc_enabled(bool tombstone_gc_enabled) noexcept;

    bool tombstone_gc_enabled() const noexcept {
        return _tombstone_gc_enabled;
    }

    bool is_auto_compaction_disabled_by_user() const {
      return _compaction_disabled_by_user;
    }

    utils::phased_barrier::operation write_in_progress() {
        return _pending_writes_phaser.start();
    }

    future<> await_pending_writes() noexcept {
        return _pending_writes_phaser.advance_and_await();
    }

    size_t writes_in_progress() const {
        return _pending_writes_phaser.operations_in_progress();
    }

    utils::phased_barrier::operation read_in_progress() {
        return _pending_reads_phaser.start();
    }

    future<> await_pending_reads() noexcept {
        return _pending_reads_phaser.advance_and_await();
    }

    size_t reads_in_progress() const {
        return _pending_reads_phaser.operations_in_progress();
    }

    utils::phased_barrier::operation stream_in_progress() {
        return _pending_streams_phaser.start();
    }

    future<> await_pending_streams() noexcept {
        return _pending_streams_phaser.advance_and_await();
    }

    size_t streams_in_progress() const {
        return _pending_streams_phaser.operations_in_progress();
    }

    future<> await_pending_flushes() noexcept {
        return _pending_flushes_phaser.advance_and_await();
    }

    future<> await_pending_ops() noexcept {
        return when_all(await_pending_reads(), await_pending_writes(), await_pending_streams(), await_pending_flushes()).discard_result();
    }

    void add_or_update_view(view_ptr v);
    void remove_view(view_ptr v);
    void clear_views();
    const std::vector<view_ptr>& views() const;
    future<row_locker::lock_holder> push_view_replica_updates(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& s, const frozen_mutation& fm, db::timeout_clock::time_point timeout,
            tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem) const;
    future<row_locker::lock_holder> push_view_replica_updates(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout,
            tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem) const;
    future<row_locker::lock_holder>
    stream_view_replica_updates(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout,
            std::vector<sstables::shared_sstable>& excluded_sstables) const;

    void add_coordinator_read_latency(utils::estimated_histogram::duration latency);
    std::chrono::milliseconds get_coordinator_read_latency_percentile(double percentile);

    secondary_index::secondary_index_manager& get_index_manager() {
        return _index_manager;
    }

    const secondary_index::secondary_index_manager& get_index_manager() const noexcept {
        return _index_manager;
    }

    sstables::sstables_manager& get_sstables_manager() noexcept {
        return _sstables_manager;
    }

    const sstables::sstables_manager& get_sstables_manager() const noexcept {
        return _sstables_manager;
    }

    reader_concurrency_semaphore& streaming_read_concurrency_semaphore() {
        return *_config.streaming_read_concurrency_semaphore;
    }

    reader_concurrency_semaphore& compaction_concurrency_semaphore() {
        return *_config.compaction_concurrency_semaphore;
    }

    size_t estimate_read_memory_cost() const;

private:
    future<row_locker::lock_holder> do_push_view_replica_updates(shared_ptr<db::view::view_update_generator> gen, schema_ptr s, mutation m, db::timeout_clock::time_point timeout, mutation_source source,
            tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem, query::partition_slice::option_set custom_opts) const;
    std::vector<view_ptr> affected_views(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& base, const mutation& update) const;

    mutable row_locker _row_locker;
    future<row_locker::lock_holder> local_base_lock(
            const schema_ptr& s,
            const dht::decorated_key& pk,
            const query::clustering_row_ranges& rows,
            db::timeout_clock::time_point timeout) const;

    // One does not need to wait on this future if all we are interested in, is
    // initiating the write.  The writes initiated here will eventually
    // complete, and the seastar::gate below will make sure they are all
    // completed before we stop() this column_family.
    //
    // But it is possible to synchronously wait for the seal to complete by
    // waiting on this future. This is useful in situations where we want to
    // synchronously flush data to disk.
    //
    // The function never fails.
    // It either succeeds eventually after retrying or aborts.
    future<> seal_active_memtable(compaction_group& cg, flush_permit&&) noexcept;

    void check_valid_rp(const db::replay_position&) const;

    void recalculate_tablet_count_stats();
    int64_t calculate_tablet_count() const;
public:
    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    future<bool> for_all_partitions_slow(schema_ptr, reader_permit permit, std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const;

    friend std::ostream& operator<<(std::ostream& out, const column_family& cf);
    // Testing purposes.
    // to let test classes access calculate_generation_for_new_table
    friend class ::column_family_test;
    friend class ::table_for_tests;

    friend class distributed_loader;
    friend class table_populator;

private:
    timer<> _off_strategy_trigger;
    void do_update_off_strategy_trigger();

    compaction_group* try_get_compaction_group_with_static_sharding() const;
public:
    void update_off_strategy_trigger();
    void enable_off_strategy_trigger();

    compaction::table_state& try_get_table_state_with_static_sharding() const;
    // Safely iterate through table states, while performing async operations on them.
    future<> parallel_foreach_table_state(std::function<future<>(compaction::table_state&)> action);
    compaction::table_state& table_state_for_sstable(const sstables::shared_sstable& sst) const noexcept {
        return compaction_group_for_sstable(sst).as_table_state();
    }

    // Uncoditionally erase sst from `sstables_requiring_cleanup`
    // Returns true iff sst was found and erased.
    bool erase_sstable_cleanup_state(const sstables::shared_sstable& sst);

    // Returns true if the sstable requires cleanup.
    bool requires_cleanup(const sstables::shared_sstable& sst) const;

    // Returns true if any of the sstables requires cleanup.
    bool requires_cleanup(const sstables::sstable_set& set) const;

    // Takes snapshot of current storage state (includes memtable and sstables) from
    // all compaction groups that overlap with a given token range. The output is
    // a list of SSTables that represent the snapshot.
    future<utils::chunked_vector<sstables::sstable_files_snapshot>> take_storage_snapshot(dht::token_range tr);

    // Clones storage of a given tablet. Memtable is flushed first to guarantee that the
    // snapshot (list of sstables) will include all the data written up to the time it was taken.
    future<utils::chunked_vector<sstables::entry_descriptor>> clone_tablet_storage(locator::tablet_id tid);

    friend class compaction_group;
};

lw_shared_ptr<sstables::sstable_set> make_tablet_sstable_set(schema_ptr, const storage_group_manager& sgm, const locator::tablet_map&);

using user_types_metadata = data_dictionary::user_types_metadata;

using keyspace_metadata = data_dictionary::keyspace_metadata;

class keyspace {
public:
    struct config {
        bool enable_commitlog = true;
        bool enable_disk_reads = true;
        bool enable_disk_writes = true;
        bool enable_cache = true;
        bool enable_incremental_backups = false;
        utils::updateable_value<bool> compaction_enforce_min_threshold{false};
        bool enable_dangerous_direct_import_of_cassandra_counters = false;
        replica::dirty_memory_manager* dirty_memory_manager = &default_dirty_memory_manager;
        reader_concurrency_semaphore* streaming_read_concurrency_semaphore;
        reader_concurrency_semaphore* compaction_concurrency_semaphore;
        replica::cf_stats* cf_stats = nullptr;
        seastar::scheduling_group memtable_scheduling_group;
        seastar::scheduling_group memtable_to_cache_scheduling_group;
        seastar::scheduling_group compaction_scheduling_group;
        seastar::scheduling_group memory_compaction_scheduling_group;
        seastar::scheduling_group statement_scheduling_group;
        seastar::scheduling_group streaming_scheduling_group;
        bool enable_metrics_reporting = false;
        size_t view_update_concurrency_semaphore_limit;
    };
private:
    locator::replication_strategy_ptr _replication_strategy;
    locator::vnode_effective_replication_map_ptr _effective_replication_map;
    lw_shared_ptr<keyspace_metadata> _metadata;
    config _config;
    locator::effective_replication_map_factory& _erm_factory;

public:
    explicit keyspace(lw_shared_ptr<keyspace_metadata> metadata, config cfg, locator::effective_replication_map_factory& erm_factory);
    keyspace(const keyspace&) = delete;
    void operator=(const keyspace&) = delete;
    keyspace(keyspace&&) = default;

    future<> shutdown() noexcept;

    future<> update_from(const locator::shared_token_metadata& stm, lw_shared_ptr<keyspace_metadata>);

    future<> init_storage();

    /** Note: return by shared pointer value, since the meta data is
     * semi-volatile. I.e. we could do alter keyspace at any time, and
     * boom, it is replaced.
     */
    lw_shared_ptr<keyspace_metadata> metadata() const;
    future<> create_replication_strategy(const locator::shared_token_metadata& stm);
    void update_effective_replication_map(locator::vnode_effective_replication_map_ptr erm);

    /**
     * This should not really be return by reference, since replication
     * strategy is also volatile in that it could be replaced at "any" time.
     * However, all current uses at least are "instantateous", i.e. does not
     * carry it across a continuation. So it is sort of same for now, but
     * should eventually be refactored.
     */
    const locator::abstract_replication_strategy& get_replication_strategy() const;
    locator::replication_strategy_ptr get_replication_strategy_ptr() const {
        return _replication_strategy;
    }

    locator::vnode_effective_replication_map_ptr get_vnode_effective_replication_map() const;

    bool uses_tablets() const {
        return _replication_strategy->uses_tablets();
    }

    column_family::config make_column_family_config(const schema& s, const database& db) const;
    void add_or_update_column_family(const schema_ptr& s);
    void add_user_type(const user_type ut);
    void remove_user_type(const user_type ut);

    bool incremental_backups_enabled() const {
        return _config.enable_incremental_backups;
    }

    void set_incremental_backups(bool val) {
        _config.enable_incremental_backups = val;
    }
};

using no_such_keyspace = data_dictionary::no_such_keyspace;
using no_such_column_family = data_dictionary::no_such_column_family;

struct database_config {
    seastar::scheduling_group memtable_scheduling_group;
    seastar::scheduling_group memtable_to_cache_scheduling_group; // FIXME: merge with memtable_scheduling_group
    seastar::scheduling_group compaction_scheduling_group;
    seastar::scheduling_group memory_compaction_scheduling_group;
    seastar::scheduling_group statement_scheduling_group;
    seastar::scheduling_group streaming_scheduling_group;
    seastar::scheduling_group gossip_scheduling_group;
    seastar::scheduling_group commitlog_scheduling_group;
    seastar::scheduling_group schema_commitlog_scheduling_group;
    size_t available_memory;
    std::optional<sstables::sstable_version_types> sstables_format;
};

struct string_pair_eq {
    using is_transparent = void;
    using spair = std::pair<std::string_view, std::string_view>;
    bool operator()(spair lhs, spair rhs) const;
};

class db_user_types_storage;

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use table::shard_for_reads()/table::shard_for_writes() for data

class database : public peering_sharded_service<database> {
    friend class ::database_test;
public:
    enum class table_kind {
        system,
        user,
    };

    struct drain_progress {
        int32_t total_cfs;
        int32_t remaining_cfs;

        drain_progress& operator+=(const drain_progress& other) {
            total_cfs += other.total_cfs;
            remaining_cfs += other.remaining_cfs;
            return *this;
        }
    };

    using ks_cf_t = std::pair<sstring, sstring>;
    using ks_cf_to_uuid_t =
        flat_hash_map<ks_cf_t, table_id, utils::tuple_hash, string_pair_eq>;
    class tables_metadata {
        rwlock _cf_lock;
        std::unordered_map<table_id, lw_shared_ptr<column_family>> _column_families;
        ks_cf_to_uuid_t _ks_cf_to_uuid;
    private:
        void add_table_helper(database& db, keyspace& ks, table& cf, schema_ptr s);
        void remove_table_helper(database& db, keyspace& ks, table& cf, schema_ptr s);
    public:
        size_t size() const noexcept;

        future<> add_table(database& db, keyspace& ks, table& cf, schema_ptr s);
        future<> remove_table(database& db, table& cf) noexcept;
        table& get_table(table_id id) const;
        table_id get_table_id(const std::pair<std::string_view, std::string_view>& kscf) const;
        lw_shared_ptr<table> get_table_if_exists(table_id id) const;
        table_id get_table_id_if_exists(const std::pair<std::string_view, std::string_view>& kscf) const;
        bool contains(table_id id) const;
        bool contains(std::pair<std::string_view, std::string_view> kscf) const;
        void for_each_table(std::function<void(table_id, lw_shared_ptr<table>)> f) const;
        void for_each_table_id(std::function<void(const ks_cf_t&, table_id)> f) const;
        future<> for_each_table_gently(std::function<future<>(table_id, lw_shared_ptr<table>)> f);
        future<> parallel_for_each_table(std::function<future<>(table_id, lw_shared_ptr<table>)> f);
        const std::unordered_map<table_id, lw_shared_ptr<table>> get_column_families_copy() const;

        auto filter(std::function<bool(std::pair<table_id, lw_shared_ptr<table>>)> f) const {
            return _column_families | boost::adaptors::filtered(std::move(f));
        }
    };
private:
    replica::cf_stats _cf_stats;
    static constexpr size_t max_count_concurrent_reads{100};
    size_t max_memory_concurrent_reads() { return _dbcfg.available_memory * 0.02; }
    // Assume a queued read takes up 1kB of memory, and allow 2% of memory to be filled up with such reads.
    size_t max_inactive_queue_length() { return _dbcfg.available_memory * 0.02 / 1000; }
    // They're rather heavyweight, so limit more
    static constexpr size_t max_count_streaming_concurrent_reads{10};
    size_t max_memory_streaming_concurrent_reads() { return _dbcfg.available_memory * 0.02; }
    static constexpr size_t max_count_system_concurrent_reads{10};
    size_t max_memory_system_concurrent_reads() { return _dbcfg.available_memory * 0.02; };
    size_t max_memory_pending_view_updates() const {
        auto ret = _dbcfg.available_memory * 0.1;
        utils::get_local_injector().inject("view_update_limit", [&ret] {
            ret = 250000;
        });
        return ret;
    }

    struct db_stats {
        uint64_t total_writes = 0;
        uint64_t total_writes_failed = 0;
        uint64_t total_writes_timedout = 0;
        uint64_t total_writes_rate_limited = 0;
        uint64_t total_reads = 0;
        uint64_t total_reads_failed = 0;
        uint64_t total_reads_rate_limited = 0;

        uint64_t short_data_queries = 0;
        uint64_t short_mutation_queries = 0;

        uint64_t multishard_query_unpopped_fragments = 0;
        uint64_t multishard_query_unpopped_bytes = 0;
        uint64_t multishard_query_failed_reader_stops = 0;
        uint64_t multishard_query_failed_reader_saves = 0;
    };

    lw_shared_ptr<db_stats> _stats;
    std::shared_ptr<db_user_types_storage> _user_types;
    std::unique_ptr<cell_locker_stats> _cl_stats;

    const db::config& _cfg;

    dirty_memory_manager _system_dirty_memory_manager;
    dirty_memory_manager _dirty_memory_manager;

    database_config _dbcfg;
    backlog_controller::scheduling_group _flush_sg;
    flush_controller _memtable_controller;
    drain_progress _drain_progress {};

    reader_concurrency_semaphore _read_concurrency_sem;
    reader_concurrency_semaphore _streaming_concurrency_sem;
    reader_concurrency_semaphore _compaction_concurrency_sem;
    reader_concurrency_semaphore _system_read_concurrency_sem;

    db::timeout_semaphore _view_update_concurrency_sem{max_memory_pending_view_updates()};

    cache_tracker _row_cache_tracker;
    seastar::shared_ptr<db::view::view_update_generator> _view_update_generator;

    inheriting_concrete_execution_stage<
            future<>,
            database*,
            schema_ptr,
            const frozen_mutation&,
            tracing::trace_state_ptr,
            db::timeout_clock::time_point,
            db::commitlog_force_sync,
            db::per_partition_rate_limit::info> _apply_stage;

    flat_hash_map<sstring, keyspace> _keyspaces;
    tables_metadata _tables_metadata;
    std::unique_ptr<db::commitlog> _commitlog;
    std::unique_ptr<db::commitlog> _schema_commitlog;
    utils::updateable_value_source<table_schema_version> _version;
    uint32_t _schema_change_count = 0;
    // compaction_manager object is referenced by all column families of a database.
    compaction_manager& _compaction_manager;
    seastar::metrics::metric_groups _metrics;
    bool _enable_incremental_backups = false;
    bool _shutdown = false;
    bool _enable_autocompaction_toggle = false;
    query::querier_cache _querier_cache;

    std::unique_ptr<db::large_data_handler> _large_data_handler;
    std::unique_ptr<db::large_data_handler> _nop_large_data_handler;

    std::unique_ptr<sstables::sstables_manager> _user_sstables_manager;
    std::unique_ptr<sstables::sstables_manager> _system_sstables_manager;

    query::result_memory_limiter _result_memory_limiter;

    friend db::data_listeners;
    std::unique_ptr<db::data_listeners> _data_listeners;

    service::migration_notifier& _mnotifier;
    gms::feature_service& _feat;
    std::vector<std::any> _listeners;
    const locator::shared_token_metadata& _shared_token_metadata;
    lang::manager& _lang_manager;

    utils::cross_shard_barrier _stop_barrier;

    db::rate_limiter _rate_limiter;

    serialized_action _update_memtable_flush_static_shares_action;
    utils::observer<float> _memtable_flush_static_shares_observer;

    db_clock::time_point _all_tables_flushed_at;

public:
    data_dictionary::database as_data_dictionary() const;
    db::commitlog* commitlog_for(const schema_ptr& schema);
    std::shared_ptr<data_dictionary::user_types_storage> as_user_types_storage() const noexcept;
    const data_dictionary::user_types_storage& user_types() const noexcept;
    future<> init_commitlog();
    const gms::feature_service& features() const { return _feat; }
    future<> apply_in_memory(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&&, db::timeout_clock::time_point timeout);
    future<> apply_in_memory(const mutation& m, column_family& cf, db::rp_handle&&, db::timeout_clock::time_point timeout);

    drain_progress get_drain_progress() const noexcept {
        return _drain_progress;
    }

    future<> drain();

    void plug_system_keyspace(db::system_keyspace& sys_ks) noexcept;
    void unplug_system_keyspace() noexcept;

    void plug_view_update_generator(db::view::view_update_generator& generator) noexcept;
    void unplug_view_update_generator() noexcept;

private:
    future<> flush_non_system_column_families();
    future<> flush_system_column_families();

    using system_keyspace = bool_class<struct system_keyspace_tag>;
    future<> create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm, locator::effective_replication_map_factory& erm_factory, system_keyspace system);
    void setup_metrics();
    void setup_scylla_memory_diagnostics_producer();

    future<> do_apply(schema_ptr, const frozen_mutation&, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout, db::commitlog_force_sync sync, db::per_partition_rate_limit::info rate_limit_info);
    future<> do_apply_many(const std::vector<frozen_mutation>&, db::timeout_clock::time_point timeout);
    future<> apply_with_commitlog(column_family& cf, const mutation& m, db::timeout_clock::time_point timeout);

    future<mutation> do_apply_counter_update(column_family& cf, const frozen_mutation& fm, schema_ptr m_schema, db::timeout_clock::time_point timeout,
                                             tracing::trace_state_ptr trace_state);

    template<typename Future>
    Future update_write_metrics(Future&& f);
    void update_write_metrics_for_timed_out_write();
    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>&, locator::effective_replication_map_factory& erm_factory, system_keyspace system);
    future<> remove(table&) noexcept;
    void drop_keyspace(const sstring& name);
    future<> update_keyspace(const keyspace_metadata& tmp_ksm);
    static future<> modify_keyspace_on_all_shards(sharded<database>& sharded_db, std::function<future<>(replica::database&)> func, std::function<future<>(replica::database&)> notifier);

    future<> foreach_reader_concurrency_semaphore(std::function<future<>(reader_concurrency_semaphore&)> func);
public:
    static table_schema_version empty_version;

    query::result_memory_limiter& get_result_memory_limiter() {
        return _result_memory_limiter;
    }

    void set_enable_incremental_backups(bool val) { _enable_incremental_backups = val; }

    void enable_autocompaction_toggle() noexcept { _enable_autocompaction_toggle = true; }
    friend class api::autocompaction_toggle_guard;

    // Load the schema definitions kept in schema tables from disk and initialize in-memory schema data structures
    // (keyspace/table definitions, column mappings etc.)
    future<> parse_system_tables(distributed<service::storage_proxy>&, sharded<db::system_keyspace>&);

    database(const db::config&, database_config dbcfg, service::migration_notifier& mn, gms::feature_service& feat, const locator::shared_token_metadata& stm,
            compaction_manager& cm, sstables::storage_manager& sstm, lang::manager& langm, sstables::directory_semaphore& sst_dir_sem, const abort_source& abort,
            utils::cross_shard_barrier barrier = utils::cross_shard_barrier(utils::cross_shard_barrier::solo{}) /* for single-shard usage */);
    database(database&&) = delete;
    ~database();

    cache_tracker& row_cache_tracker() { return _row_cache_tracker; }
    future<> drop_caches() const;

    void update_version(const table_schema_version& version);

    const table_schema_version& get_version() const;
    utils::observable<table_schema_version>& observable_schema_version() const { return _version.as_observable(); }

    db::commitlog* commitlog() const {
        return _commitlog.get();
    }
    db::commitlog* schema_commitlog() const {
        return _schema_commitlog.get();
    }
    replica::cf_stats* cf_stats() {
        return &_cf_stats;
    }

    seastar::scheduling_group get_streaming_scheduling_group() const { return _dbcfg.streaming_scheduling_group; }

    compaction_manager& get_compaction_manager() {
        return _compaction_manager;
    }
    const compaction_manager& get_compaction_manager() const {
        return _compaction_manager;
    }

    const locator::shared_token_metadata& get_shared_token_metadata() const { return _shared_token_metadata; }
    const locator::token_metadata& get_token_metadata() const { return *_shared_token_metadata.get(); }

    lang::manager& lang() noexcept { return _lang_manager; }
    const lang::manager& lang() const noexcept { return _lang_manager; }

    service::migration_notifier& get_notifier() { return _mnotifier; }
    const service::migration_notifier& get_notifier() const { return _mnotifier; }

    // Setup in-memory data structures for this table (`table` and, if it doesn't exist yet, `keyspace` object).
    // Create the keyspace data directories if the keyspace wasn't created yet.
    //
    // Note: 'system table' does not necessarily mean it sits in `system` keyspace, it could also be `system_schema`;
    // in general we mean local tables created by the system (not the user).
    future<> create_local_system_table(
            schema_ptr table, bool write_in_user_memory, locator::effective_replication_map_factory&);

    void init_schema_commitlog();
    using is_new_cf = bool_class<struct is_new_cf_tag>;
    future<> add_column_family_and_make_directory(schema_ptr schema, is_new_cf is_new);

    /* throws no_such_column_family if missing */
    table_id find_uuid(std::string_view ks, std::string_view cf) const;
    table_id find_uuid(const schema_ptr&) const;

    /**
     * Creates a keyspace for a given metadata if it still doesn't exist.
     *
     * @return ready future when the operation is complete
     */
    static future<> create_keyspace_on_all_shards(sharded<database>& sharded_db, sharded<service::storage_proxy>& proxy, const keyspace_metadata& ksm);
    /* below, find_keyspace throws no_such_<type> on fail */
    keyspace& find_keyspace(std::string_view name);
    const keyspace& find_keyspace(std::string_view name) const;
    bool has_keyspace(std::string_view name) const;
    void validate_keyspace_update(keyspace_metadata& ksm);
    void validate_new_keyspace(keyspace_metadata& ksm);
    static future<> update_keyspace_on_all_shards(sharded<database>& sharded_db, const keyspace_metadata& ksm);
    static future<> drop_keyspace_on_all_shards(sharded<database>& sharded_db, const sstring& name);
    std::vector<sstring> get_non_system_keyspaces() const;
    std::vector<sstring> get_user_keyspaces() const;
    std::vector<sstring> get_all_keyspaces() const;
    std::vector<sstring> get_non_local_strategy_keyspaces() const;
    std::vector<sstring> get_non_local_vnode_based_strategy_keyspaces() const;
    std::unordered_map<sstring, locator::vnode_effective_replication_map_ptr> get_non_local_strategy_keyspaces_erms() const;
    column_family& find_column_family(std::string_view ks, std::string_view name);
    const column_family& find_column_family(std::string_view ks, std::string_view name) const;
    column_family& find_column_family(const table_id&);
    const column_family& find_column_family(const table_id&) const;
    column_family& find_column_family(const schema_ptr&);
    const column_family& find_column_family(const schema_ptr&) const;
    bool column_family_exists(const table_id& uuid) const;
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name) const;
    schema_ptr find_schema(const table_id&) const;
    bool has_schema(std::string_view ks_name, std::string_view cf_name) const;
    std::set<sstring> existing_index_names(const sstring& ks_name, const sstring& cf_to_exclude = sstring()) const;
    sstring get_available_index_name(const sstring& ks_name, const sstring& cf_name,
                                     std::optional<sstring> index_name_root) const;
    schema_ptr find_indexed_table(const sstring& ks_name, const sstring& index_name) const;
    /// Revert the system read concurrency to the normal value.
    ///
    /// When started the database uses a higher initial concurrency for system
    /// reads, to speed up startup. After startup this should be reverted to
    /// the normal concurrency.
    void revert_initial_system_read_concurrency_boost();
    future<> start();
    future<> shutdown();
    future<> stop();
    future<> close_tables(table_kind kind_to_close);

    /// Checks whether per-partition rate limit can be applied to the operation or not.
    bool can_apply_per_partition_rate_limit(const schema& s, db::operation_type op_type) const;

    /// Tries to account given operation to the rate limit when the coordinator is a replica.
    /// This function can be called ONLY when rate limiting can be applied to the operation (see `can_apply_per_partition_rate_limit`)
    /// AND the current node/shard is a replica for the given operation.
    ///
    /// nullopt -> the decision should be delegated to replicas
    /// can_proceed::no -> operation should be rejected
    /// can_proceed::yes -> operation should be accepted
    std::optional<db::rate_limiter::can_proceed> account_coordinator_operation_to_rate_limit(table& tbl, const dht::token& token,
            db::per_partition_rate_limit::account_and_enforce account_and_enforce_info,
            db::operation_type op_type);

    future<std::tuple<lw_shared_ptr<query::result>, cache_temperature>> query(schema_ptr query_schema, const query::read_command& cmd, query::result_options opts,
                                                                  const dht::partition_range_vector& ranges, tracing::trace_state_ptr trace_state,
                                                                  db::timeout_clock::time_point timeout, db::per_partition_rate_limit::info rate_limit_info = std::monostate{});
    future<std::tuple<reconcilable_result, cache_temperature>> query_mutations(schema_ptr query_schema, const query::read_command& cmd, const dht::partition_range& range,
                                                tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout);
    // Apply the mutation atomically.
    // Throws timed_out_error when timeout is reached.
    future<> apply(schema_ptr, const frozen_mutation&, tracing::trace_state_ptr tr_state, db::commitlog_force_sync sync, db::timeout_clock::time_point timeout, db::per_partition_rate_limit::info rate_limit_info = std::monostate{});
    // Apply mutations atomically.
    // On restart, either all mutations will be replayed or none of them.
    // All mutations must belong to the same commitlog domain.
    // All mutations must be owned by the current shard.
    // Mutations may be partially visible to reads during the call.
    // Mutations may be partially visible to reads until restart on exception (FIXME).
    future<> apply(const std::vector<frozen_mutation>&, db::timeout_clock::time_point timeout);
    future<> apply_hint(schema_ptr, const frozen_mutation&, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout);
    future<mutation> apply_counter_update(schema_ptr, const frozen_mutation& m, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_state);
    keyspace::config make_keyspace_config(const keyspace_metadata& ksm);
    const sstring& get_snitch_name() const;
    /*!
     * \brief clear snapshot based on a tag
     * The clear_snapshot method deletes specific or multiple snapshots
     * You can specify:
     * tag - The snapshot tag (the one that was used when creating the snapshot) if not specified
     *       All snapshot will be deleted
     * keyspace_names - a vector of keyspace names that will be deleted, if empty all keyspaces
     *                  will be deleted.
     * table_name - A name of a specific table inside the keyspace, if empty all tables will be deleted.
     */
    future<> clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, const sstring& table_name);

    using snapshot_details = db::snapshot_ctl::db_snapshot_details;
    future<std::unordered_map<sstring, snapshot_details>> get_snapshot_details();

    friend std::ostream& operator<<(std::ostream& out, const database& db);
    const flat_hash_map<sstring, keyspace>& get_keyspaces() const {
        return _keyspaces;
    }

    flat_hash_map<sstring, keyspace>& get_keyspaces() {
        return _keyspaces;
    }

    const tables_metadata& get_tables_metadata() const {
        return _tables_metadata;
    }

    tables_metadata& get_tables_metadata() {
        return _tables_metadata;
    }

    std::vector<lw_shared_ptr<column_family>> get_non_system_column_families() const;

    std::vector<view_ptr> get_views() const;

    const db::config& get_config() const {
        return _cfg;
    }
    const db::extensions& extensions() const;

    sstables::sstables_manager& get_user_sstables_manager() const noexcept {
        SCYLLA_ASSERT(_user_sstables_manager);
        return *_user_sstables_manager;
    }

    sstables::sstables_manager& get_system_sstables_manager() const noexcept {
        SCYLLA_ASSERT(_system_sstables_manager);
        return *_system_sstables_manager;
    }

    sstables::sstables_manager& get_sstables_manager(system_keyspace is_sys_ks) const noexcept {
        return is_sys_ks ? get_system_sstables_manager() : get_user_sstables_manager();
    }

    // Returns the list of ranges held by this endpoint
    // The returned list is sorted, and its elements are non overlapping and non wrap-around.
    future<dht::token_range_vector> get_keyspace_local_ranges(locator::vnode_effective_replication_map_ptr erm);

    void set_format(sstables::sstable_version_types format) noexcept;
    void set_format_by_config();

    future<> flush_all_memtables();
    future<> flush(const sstring& ks, const sstring& cf);
    // flush a table identified by the given id on all shards.
    static future<> flush_table_on_all_shards(sharded<database>& sharded_db, table_id id);
    // flush a single table in a keyspace on all shards.
    static future<> flush_table_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name, std::string_view table_name);
    // flush a list of tables in a keyspace on all shards.
    static future<> flush_tables_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name, std::vector<sstring> table_names);
    // flush all tables in a keyspace on all shards.
    static future<> flush_keyspace_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name);
    // flush all tables on this shard.
    // Note: force_new_active_segment in the commitlog, so that
    // flushing all tables will allow reclaiming of all commitlog segments
    future<> flush_all_tables();

    static future<db_clock::time_point> get_all_tables_flushed_at(sharded<database>& sharded_db);

    static future<> drop_cache_for_table_on_all_shards(sharded<database>& sharded_db, table_id id);
    static future<> drop_cache_for_keyspace_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name);

    static future<> snapshot_table_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name, sstring table_name, sstring tag, db::snapshot_ctl::snap_views, bool skip_flush);
    static future<> snapshot_tables_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name, std::vector<sstring> table_names, sstring tag, db::snapshot_ctl::snap_views, bool skip_flush);
    static future<> snapshot_keyspace_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name, sstring tag, bool skip_flush);

public:
    bool update_column_family(schema_ptr s);
private:
    future<> add_column_family(keyspace& ks, schema_ptr schema, column_family::config cfg, is_new_cf is_new);
    future<> detach_column_family(table& cf);

    struct table_truncate_state;

    static future<> truncate_table_on_all_shards(sharded<database>& db, sharded<db::system_keyspace>& sys_ks, const global_table_ptr&, std::optional<db_clock::time_point> truncated_at_opt, bool with_snapshot, std::optional<sstring> snapshot_name_opt);
    future<> truncate(db::system_keyspace& sys_ks, column_family& cf, const table_truncate_state&, db_clock::time_point truncated_at);
public:
    /** Truncates the given column family */
    // If truncated_at_opt is not given, it is set to db_clock::now right after flush/clear.
    static future<> truncate_table_on_all_shards(sharded<database>& db, sharded<db::system_keyspace>& sys_ks, sstring ks_name, sstring cf_name, std::optional<db_clock::time_point> truncated_at_opt = {}, bool with_snapshot = true, std::optional<sstring> snapshot_name_opt = {});

    // drops the table on all shards and removes the table directory if there are no snapshots
    static future<> drop_table_on_all_shards(sharded<database>& db, sharded<db::system_keyspace>& sys_ks, sstring ks_name, sstring cf_name, bool with_snapshot = true);

    const dirty_memory_manager_logalloc::region_group& dirty_memory_region_group() const {
        return _dirty_memory_manager.region_group();
    }

    db_stats& get_stats() {
        return *_stats;
    }

    void set_querier_cache_entry_ttl(std::chrono::seconds entry_ttl) {
        _querier_cache.set_entry_ttl(entry_ttl);
    }

    const query::querier_cache::stats& get_querier_cache_stats() const {
        return _querier_cache.get_stats();
    }

    query::querier_cache& get_querier_cache() {
        return _querier_cache;
    }

    db::view::update_backlog get_view_update_backlog() const {
        return {max_memory_pending_view_updates() - _view_update_concurrency_sem.current(), max_memory_pending_view_updates()};
    }

    db::data_listeners& data_listeners() const {
        return *_data_listeners;
    }

    // Get the maximum result size for a query, appropriate for the
    // query class, which is deduced from the current scheduling group.
    query::max_result_size get_query_max_result_size() const;

    // Get the reader concurrency semaphore, appropriate for the query class,
    // which is deduced from the current scheduling group.
    reader_concurrency_semaphore& get_reader_concurrency_semaphore();

    // Convenience method to obtain an admitted permit. See reader_concurrency_semaphore::obtain_permit().
    future<reader_permit> obtain_reader_permit(table& tbl, const char* const op_name, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr);
    future<reader_permit> obtain_reader_permit(schema_ptr schema, const char* const op_name, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr);

    bool is_internal_query() const;
    bool is_user_semaphore(const reader_concurrency_semaphore& semaphore) const;

    db::timeout_semaphore& view_update_sem() {
        return _view_update_concurrency_sem;
    }

    future<> clear_inactive_reads_for_tablet(table_id table, dht::token_range tablet_range);
};

// A pair of helper functions to make directory name for a table
// out of its name and uuid, and to parse the directory name back
// into name and uuid of the table
sstring format_table_directory_name(sstring name, table_id id);
std::pair<sstring, table_id> parse_table_directory_name(const sstring&);

} // namespace replica

future<> start_large_data_handler(sharded<replica::database>& db);

// Creates a streaming reader that reads from all shards.
//
// Shard readers are created via `table::make_streaming_reader()`.
// Range generator must generate disjoint, monotonically increasing ranges.
// Opt-in for compacting the output by passing `compaction_time`, see
// make_streaming_reader() for more details.
mutation_reader make_multishard_streaming_reader(distributed<replica::database>& db, schema_ptr schema, reader_permit permit,
        std::function<std::optional<dht::partition_range>()> range_generator, gc_clock::time_point compaction_time);

mutation_reader make_multishard_streaming_reader(distributed<replica::database>& db,
    schema_ptr schema, reader_permit permit, const dht::partition_range& range, gc_clock::time_point compaction_time);

bool is_internal_keyspace(std::string_view name);
