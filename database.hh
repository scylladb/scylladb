/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "locator/token_metadata.hh"
#include "index/secondary_index_manager.hh"
#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/execution_stage.hh>
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include <seastar/core/distributed.hh>
#include <functional>
#include <unordered_map>
#include <map>
#include <set>
#include <boost/functional/hash.hpp>
#include <boost/range/algorithm/find.hpp>
#include <optional>
#include <string.h>
#include "types.hh"
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include "db/commitlog/replay_position.hh"
#include <limits>
#include "schema_fwd.hh"
#include "db/view/view.hh"
#include "gms/feature.hh"
#include "memtable.hh"
#include "mutation_reader.hh"
#include "row_cache.hh"
#include "compaction_strategy.hh"
#include "utils/estimated_histogram.hh"
#include "sstables/sstable_set.hh"
#include <seastar/core/metrics_registration.hh>
#include "tracing/trace_state.hh"
#include "db/view/view_stats.hh"
#include "db/view/view_update_backlog.hh"
#include "db/view/row_locking.hh"
#include "utils/phased_barrier.hh"
#include "backlog_controller.hh"
#include "dirty_memory_manager.hh"
#include "reader_concurrency_semaphore.hh"
#include "db/timeout_clock.hh"
#include "querier.hh"
#include "mutation_query.hh"
#include "cache_temperature.hh"
#include <unordered_set>
#include "utils/disk-error-handler.hh"
#include "utils/updateable_value.hh"
#include "user_types_metadata.hh"
#include "query_class_config.hh"
#include "absl-flat_hash_map.hh"

class cell_locker;
class cell_locker_stats;
class locked_cell;
class mutation;

class frozen_mutation;
class reconcilable_result;

namespace service {
class storage_proxy;
class migration_notifier;
class migration_manager;
}

namespace netw {
class messaging_service;
}

namespace gms {
class feature_service;
}

namespace sstables {

class sstable;
class entry_descriptor;
class compaction_descriptor;
class compaction_completion_desc;
class foreign_sstable_open_info;
class sstables_manager;

}

class compaction_manager;

namespace ser {
template<typename T>
class serializer;
}

namespace db {
class commitlog;
class config;
class extensions;
class rp_handle;
class data_listeners;
class large_data_handler;

namespace system_keyspace {
future<> make(database& db);
}
}

namespace locator {

class abstract_replication_strategy;

} // namespace locator

class mutation_reordered_with_truncate_exception : public std::exception {};

using shared_memtable = lw_shared_ptr<memtable>;
class memtable_list;

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
    using seal_delayed_fn_type = std::function<future<> ()>;
private:
    std::vector<shared_memtable> _memtables;
    seal_immediate_fn_type _seal_immediate_fn;
    std::function<schema_ptr()> _current_schema;
    dirty_memory_manager* _dirty_memory_manager;
    std::optional<shared_promise<>> _flush_coalescing;
    seastar::scheduling_group _compaction_scheduling_group;
    table_stats& _table_stats;
public:
    memtable_list(
            seal_immediate_fn_type seal_immediate_fn,
            std::function<schema_ptr()> cs,
            dirty_memory_manager* dirty_memory_manager,
            table_stats& table_stats,
            seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group())
        : _memtables({})
        , _seal_immediate_fn(seal_immediate_fn)
        , _current_schema(cs)
        , _dirty_memory_manager(dirty_memory_manager)
        , _compaction_scheduling_group(compaction_scheduling_group)
        , _table_stats(table_stats) {
        add_memtable();
    }

    memtable_list(std::function<schema_ptr()> cs, dirty_memory_manager* dirty_memory_manager,
            table_stats& table_stats,
            seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group())
        : memtable_list({}, std::move(cs), dirty_memory_manager, table_stats, compaction_scheduling_group) {
    }

    bool may_flush() const {
        return bool(_seal_immediate_fn);
    }

    bool can_flush() const {
        return may_flush() && !empty();
    }

    bool empty() const {
        for (auto& m : _memtables) {
           if (!m->empty()) {
               return false;
            }
        }
        return true;
    }
    shared_memtable back() {
        return _memtables.back();
    }

    // The caller has to make sure the element exist before calling this.
    void erase(const shared_memtable& element) {
        _memtables.erase(boost::range::find(_memtables, element));
    }

    // Clears the active memtable and adds a new, empty one.
    // Exception safe.
    void clear_and_add() {
        auto mt = new_memtable();
        _memtables.clear();
        // emplace_back might throw only if _memtables was empty
        // on entry. Otherwise, we rely on clear() not to release
        // the vector capacity (See https://en.cppreference.com/w/cpp/container/vector/clear)
        // and lw_shared_ptr being nothrow move constructible.
        _memtables.emplace_back(std::move(mt));
    }

    size_t size() const {
        return _memtables.size();
    }

    future<> seal_active_memtable(flush_permit&& permit) {
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

    memtable& active_memtable() {
        return *_memtables.back();
    }

    void add_memtable() {
        _memtables.emplace_back(new_memtable());
    }

    logalloc::region_group& region_group() {
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

using sstable_list = sstables::sstable_list;

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
};

class table;
using column_family = table;
struct table_stats;
using column_family_stats = table_stats;

class database_sstable_write_monitor;

using enable_backlog_tracker = bool_class<class enable_backlog_tracker_tag>;

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
    mutation_application_stats memtable_app_stats;
    utils::timed_rate_moving_average_and_histogram reads{256};
    utils::timed_rate_moving_average_and_histogram writes{256};
    utils::timed_rate_moving_average_and_histogram cas_prepare{256};
    utils::timed_rate_moving_average_and_histogram cas_accept{256};
    utils::timed_rate_moving_average_and_histogram cas_learn{256};
    utils::time_estimated_histogram estimated_read;
    utils::time_estimated_histogram estimated_write;
    utils::time_estimated_histogram estimated_cas_prepare;
    utils::time_estimated_histogram estimated_cas_accept;
    utils::time_estimated_histogram estimated_cas_learn;
    utils::estimated_histogram estimated_sstable_per_read{35};
    utils::timed_rate_moving_average_and_histogram tombstone_scanned;
    utils::timed_rate_moving_average_and_histogram live_scanned;
    utils::estimated_histogram estimated_coordinator_read;
};

class table : public enable_lw_shared_from_this<table> {
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
        ::dirty_memory_manager* dirty_memory_manager = &default_dirty_memory_manager;
        reader_concurrency_semaphore* streaming_read_concurrency_semaphore;
        reader_concurrency_semaphore* compaction_concurrency_semaphore;
        ::cf_stats* cf_stats = nullptr;
        seastar::scheduling_group memtable_scheduling_group;
        seastar::scheduling_group memtable_to_cache_scheduling_group;
        seastar::scheduling_group compaction_scheduling_group;
        seastar::scheduling_group memory_compaction_scheduling_group;
        seastar::scheduling_group statement_scheduling_group;
        seastar::scheduling_group streaming_scheduling_group;
        bool enable_metrics_reporting = false;
        sstables::sstables_manager* sstables_manager;
        db::timeout_semaphore* view_update_concurrency_semaphore;
        size_t view_update_concurrency_semaphore_limit;
        db::data_listeners* data_listeners = nullptr;
    };
    struct no_commitlog {};

    struct snapshot_details {
        int64_t total;
        int64_t live;
    };
    struct cache_hit_rate {
        cache_temperature rate;
        lowres_clock::time_point last_updated;
    };
private:
    schema_ptr _schema;
    config _config;
    mutable table_stats _stats;
    mutable db::view::stats _view_stats;
    mutable row_locker::stats _row_locker_stats;

    uint64_t _failed_counter_applies_to_memtable = 0;

    template<typename... Args>
    void do_apply(db::rp_handle&&, Args&&... args);

    lw_shared_ptr<memtable_list> _memtables;

    lw_shared_ptr<memtable_list> make_memory_only_memtable_list();
    lw_shared_ptr<memtable_list> make_memtable_list();

    sstables::compaction_strategy _compaction_strategy;
    // SSTable set which contains all non-maintenance sstables
    lw_shared_ptr<sstables::sstable_set> _main_sstables;
    // Holds SSTables created by maintenance operations, which need reshaping before integration into the main set
    lw_shared_ptr<sstables::sstable_set> _maintenance_sstables;
    // Compound set which manages all the SSTable sets (e.g. main, etc) and allow their operations to be combined
    lw_shared_ptr<sstables::sstable_set> _sstables;
    // sstables that have been compacted (so don't look up in query) but
    // have not been deleted yet, so must not GC any tombstones in other sstables
    // that may delete data in these sstables:
    std::vector<sstables::shared_sstable> _sstables_compacted_but_not_deleted;
    // sstables that have been opened but not loaded yet, that's because refresh
    // needs to load all opened sstables atomically, and now, we open a sstable
    // in all shards at the same time, which makes it hard to store all sstables
    // we need to load later on for all shards.
    std::vector<sstables::shared_sstable> _sstables_opened_but_not_loaded;
    // sstables that should not be compacted (e.g. because they need to be used
    // to generate view updates later)
    std::unordered_map<uint64_t, sstables::shared_sstable> _sstables_staging;
    // Control background fibers waiting for sstables to be deleted
    seastar::gate _sstable_deletion_gate;
    // This semaphore ensures that an operation like snapshot won't have its selected
    // sstables deleted by compaction in parallel, a race condition which could
    // easily result in failure.
    seastar::named_semaphore _sstable_deletion_sem = {1, named_semaphore_exception_factory{"sstable deletion"}};
    // This semaphore ensures that off-strategy compaction will be serialized and also
    // protects against candidates being picked more than once.
    seastar::named_semaphore _off_strategy_sem = {1, named_semaphore_exception_factory{"off-strategy compaction"}};
    mutable row_cache _cache; // Cache covers only sstables.
    std::optional<int64_t> _sstable_generation = {};

    db::replay_position _highest_rp;
    db::replay_position _flush_rp;
    db::replay_position _lowest_allowed_rp;

    // Provided by the database that owns this commitlog
    db::commitlog* _commitlog;
    bool _durable_writes;
    compaction_manager& _compaction_manager;
    secondary_index::secondary_index_manager _index_manager;
    int _compaction_disabled = 0;
    bool _compaction_disabled_by_user = false;
    utils::phased_barrier _flush_barrier;
    std::vector<view_ptr> _views;

    std::unique_ptr<cell_locker> _counter_cell_locks; // Memory-intensive; allocate only when needed.
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
    db_clock::time_point _truncated_at = db_clock::time_point::min();

    bool _is_bootstrap_or_replace = false;
public:
    future<> add_sstable_and_update_cache(sstables::shared_sstable sst,
                                          sstables::offstrategy offstrategy = sstables::offstrategy::no);
    future<> move_sstables_from_staging(std::vector<sstables::shared_sstable>);
    sstables::shared_sstable get_staging_sstable(uint64_t generation) {
        auto it = _sstables_staging.find(generation);
        return it != _sstables_staging.end() ? it->second : nullptr;
    }
    sstables::shared_sstable make_sstable(sstring dir, int64_t generation, sstables::sstable_version_types v, sstables::sstable_format_types f,
            io_error_handler_gen error_handler_gen);
    sstables::shared_sstable make_sstable(sstring dir, int64_t generation, sstables::sstable_version_types v, sstables::sstable_format_types f);
    sstables::shared_sstable make_sstable(sstring dir);
    sstables::shared_sstable make_sstable();
    void cache_truncation_record(db_clock::time_point truncated_at) {
        _truncated_at = truncated_at;
    }
    db_clock::time_point get_truncation_record() {
        return _truncated_at;
    }

    void notify_bootstrap_or_replace_start();

    void notify_bootstrap_or_replace_end();
private:
    bool cache_enabled() const {
        return _config.enable_cache && _schema->caching_options().enabled();
    }
    void update_stats_for_new_sstable(uint64_t disk_space_used_by_sstable) noexcept;
    // Adds new sstable to the set of sstables
    // Doesn't update the cache. The cache must be synchronized in order for reads to see
    // the writes contained in this sstable.
    // Cache must be synchronized atomically with this, otherwise write atomicity may not be respected.
    // Doesn't trigger compaction.
    // Strong exception guarantees.
    lw_shared_ptr<sstables::sstable_set>
    do_add_sstable(lw_shared_ptr<sstables::sstable_set> sstables, sstables::shared_sstable sstable,
        enable_backlog_tracker backlog_tracker);
    void add_sstable(sstables::shared_sstable sstable);
    void add_maintenance_sstable(sstables::shared_sstable sst);
    static void add_sstable_to_backlog_tracker(compaction_backlog_tracker& tracker, sstables::shared_sstable sstable);
    static void remove_sstable_from_backlog_tracker(compaction_backlog_tracker& tracker, sstables::shared_sstable sstable);
    void load_sstable(sstables::shared_sstable& sstable, bool reset_level = false);
    lw_shared_ptr<memtable> new_memtable();
    future<stop_iteration> try_flush_memtable_to_sstable(lw_shared_ptr<memtable> memt, sstable_write_permit&& permit);
    // Caller must keep m alive.
    future<> update_cache(lw_shared_ptr<memtable> m, std::vector<sstables::shared_sstable> ssts);
    struct merge_comparator;

    // update the sstable generation, making sure that new new sstables don't overwrite this one.
    void update_sstables_known_generation(unsigned generation) {
        if (!_sstable_generation) {
            _sstable_generation = 1;
        }
        _sstable_generation = std::max<uint64_t>(*_sstable_generation, generation /  smp::count + 1);
    }

    uint64_t calculate_generation_for_new_table() {
        assert(_sstable_generation);
        // FIXME: better way of ensuring we don't attempt to
        // overwrite an existing table.
        return (*_sstable_generation)++ * smp::count + this_shard_id();
    }

    // inverse of calculate_generation_for_new_table(), used to determine which
    // shard a sstable should be opened at.
    static int64_t calculate_shard_from_sstable_generation(int64_t sstable_generation) {
        return sstable_generation % smp::count;
    }

    // Builds new sstable set from existing one, with new sstables added to it and old sstables removed from it.
    future<lw_shared_ptr<sstables::sstable_set>>
    build_new_sstable_list(const sstables::sstable_set& current_sstables,
                        sstables::sstable_set new_sstable_list,
                        const std::vector<sstables::shared_sstable>& new_sstables,
                        const std::vector<sstables::shared_sstable>& old_sstables);

    future<>
    update_sstable_lists_on_off_strategy_completion(const std::vector<sstables::shared_sstable>& old_maintenance_sstables,
                                                    const std::vector<sstables::shared_sstable>& new_main_sstables);

    // Rebuild sstable set, delete input sstables right away, and update row cache and statistics.
    void on_compaction_completion(sstables::compaction_completion_desc& desc);

    void rebuild_statistics();
private:
    mutation_source_opt _virtual_reader;
    // Creates a mutation reader which covers given sstables.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // The 'range' parameter must be live as long as the reader is used.
    // Mutations returned by the reader will all have given schema.
    flat_mutation_reader make_sstable_reader(schema_ptr schema,
                                        reader_permit permit,
                                        lw_shared_ptr<sstables::sstable_set> sstables,
                                        const dht::partition_range& range,
                                        const query::partition_slice& slice,
                                        const io_priority_class& pc,
                                        tracing::trace_state_ptr trace_state,
                                        streamed_mutation::forwarding fwd,
                                        mutation_reader::forwarding fwd_mr) const;

    lw_shared_ptr<sstables::sstable_set> make_maintenance_sstable_set() const;
    lw_shared_ptr<sstables::sstable_set> make_compound_sstable_set();
    // Compound sstable set must be refreshed whenever any of its managed sets are changed
    void refresh_compound_sstable_set();

    snapshot_source sstables_as_snapshot_source();
    partition_presence_checker make_partition_presence_checker(lw_shared_ptr<sstables::sstable_set>);
    std::chrono::steady_clock::time_point _sstable_writes_disabled_at;
    void do_trigger_compaction();
public:
    sstring dir() const {
        return _config.datadir;
    }

    logalloc::region_group& dirty_memory_region_group() const {
        return _config.dirty_memory_manager->region_group();
    }

    // Used for asynchronous operations that may defer and need to guarantee that the column
    // family will be alive until their termination
    template<typename Func, typename Futurator = futurize<std::result_of_t<Func()>>, typename... Args>
    typename Futurator::type run_async(Func&& func, Args&&... args) {
        return with_gate(_async_gate, [func = std::forward<Func>(func), args = std::make_tuple(std::forward<Args>(args)...)] () mutable {
            return Futurator::apply(func, std::move(args));
        });
    }

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
    void mark_ready_for_writes() {
        update_sstables_known_generation(0);
    }

    // Creates a mutation reader which covers all data sources for this column family.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // Note: for data queries use query() instead.
    // The 'range' parameter must be live as long as the reader is used.
    // Mutations returned by the reader will all have given schema.
    // If I/O needs to be issued to read anything in the specified range, the operations
    // will be scheduled under the priority class given by pc.
    flat_mutation_reader make_reader(schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc = default_priority_class(),
            tracing::trace_state_ptr trace_state = nullptr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const;
    flat_mutation_reader make_reader_excluding_sstables(schema_ptr schema,
            reader_permit permit,
            std::vector<sstables::shared_sstable>& sst,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc = default_priority_class(),
            tracing::trace_state_ptr trace_state = nullptr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const;

    flat_mutation_reader make_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range = query::full_partition_range) const {
        auto& full_slice = schema->full_slice();
        return make_reader(std::move(schema), std::move(permit), range, full_slice);
    }

    // The streaming mutation reader differs from the regular mutation reader in that:
    //  - Reflects all writes accepted by replica prior to creation of the
    //    reader and a _bounded_ amount of writes which arrive later.
    //  - Does not populate the cache
    // Requires ranges to be sorted and disjoint.
    flat_mutation_reader make_streaming_reader(schema_ptr schema,
            const dht::partition_range_vector& ranges) const;

    // Single range overload.
    flat_mutation_reader make_streaming_reader(schema_ptr schema, const dht::partition_range& range,
            const query::partition_slice& slice,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no) const;

    flat_mutation_reader make_streaming_reader(schema_ptr schema, const dht::partition_range& range) {
        return make_streaming_reader(schema, range, schema->full_slice());
    }

    // Stream reader from the given sstables
    flat_mutation_reader make_streaming_reader(schema_ptr schema, const dht::partition_range& range,
            lw_shared_ptr<sstables::sstable_set> sstables) const;

    sstables::shared_sstable make_streaming_sstable_for_write(std::optional<sstring> subdir = {});
    sstables::shared_sstable make_streaming_staging_sstable() {
        return make_streaming_sstable_for_write("staging");
    }

    mutation_source as_mutation_source() const;
    mutation_source as_mutation_source_excluding(std::vector<sstables::shared_sstable>& sst) const;

    void set_virtual_reader(mutation_source virtual_reader) {
        _virtual_reader = std::move(virtual_reader);
    }

    // Queries can be satisfied from multiple data sources, so they are returned
    // as temporaries.
    //
    // FIXME: in case a query is satisfied from a single memtable, avoid a copy
    using const_mutation_partition_ptr = std::unique_ptr<const mutation_partition>;
    using const_row_ptr = std::unique_ptr<const row>;
    memtable& active_memtable() { return _memtables->active_memtable(); }
    const row_cache& get_row_cache() const {
        return _cache;
    }

    row_cache& get_row_cache() {
        return _cache;
    }

    future<std::vector<locked_cell>> lock_counter_cells(const mutation& m, db::timeout_clock::time_point timeout);

    logalloc::occupancy_stats occupancy() const;
private:
    table(schema_ptr schema, config cfg, db::commitlog* cl, compaction_manager&, cell_locker_stats& cl_stats, cache_tracker& row_cache_tracker);
public:
    table(schema_ptr schema, config cfg, db::commitlog& cl, compaction_manager& cm, cell_locker_stats& cl_stats, cache_tracker& row_cache_tracker)
        : table(schema, std::move(cfg), &cl, cm, cl_stats, row_cache_tracker) {}
    table(schema_ptr schema, config cfg, no_commitlog, compaction_manager& cm, cell_locker_stats& cl_stats, cache_tracker& row_cache_tracker)
        : table(schema, std::move(cfg), nullptr, cm, cl_stats, row_cache_tracker) {}
    table(column_family&&) = delete; // 'this' is being captured during construction
    ~table();
    const schema_ptr& schema() const { return _schema; }
    void set_schema(schema_ptr);
    db::commitlog* commitlog() { return _commitlog; }
    future<const_mutation_partition_ptr> find_partition(schema_ptr, reader_permit permit, const dht::decorated_key& key) const;
    future<const_mutation_partition_ptr> find_partition_slow(schema_ptr, reader_permit permit, const partition_key& key) const;
    future<const_row_ptr> find_row(schema_ptr, reader_permit permit, const dht::decorated_key& partition_key, clustering_key clustering_key) const;
    // Applies given mutation to this column family
    // The mutation is always upgraded to current schema.
    void apply(const frozen_mutation& m, const schema_ptr& m_schema, db::rp_handle&& = {});
    void apply(const mutation& m, db::rp_handle&& = {});

    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(schema_ptr,
        const query::read_command& cmd,
        query::query_class_config class_config,
        query::result_options opts,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        query::result_memory_limiter& memory_limiter,
        db::timeout_clock::time_point timeout,
        query::querier_cache_context cache_ctx = { });

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
    future<reconcilable_result>
    mutation_query(schema_ptr s,
            const query::read_command& cmd,
            query::query_class_config class_config,
            const dht::partition_range& range,
            tracing::trace_state_ptr trace_state,
            query::result_memory_accounter accounter,
            db::timeout_clock::time_point timeout,
            query::querier_cache_context cache_ctx = { });

    void start();
    future<> stop();
    future<> flush(std::optional<db::replay_position> = {});
    future<> clear(); // discards memtable(s) without flushing them to disk.
    future<db::replay_position> discard_sstables(db_clock::time_point);

    bool can_flush() const;

    // FIXME: this is just an example, should be changed to something more
    // general. compact_all_sstables() starts a compaction of all sstables.
    // It doesn't flush the current memtable first. It's just a ad-hoc method,
    // not a real compaction policy.
    future<> compact_all_sstables();
    // Compact all sstables provided in the vector.
    future<> compact_sstables(sstables::compaction_descriptor descriptor);

    future<bool> snapshot_exists(sstring name);

    db::replay_position set_low_replay_position_mark();

    future<> snapshot(database& db, sstring name);
    future<std::unordered_map<sstring, snapshot_details>> get_snapshot_details();

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

    const bool incremental_backups_enabled() const {
        return _config.enable_incremental_backups;
    }

    void set_incremental_backups(bool val) {
        _config.enable_incremental_backups = val;
    }

    bool compaction_enforce_min_threshold() const {
        return _config.compaction_enforce_min_threshold || _is_bootstrap_or_replace;
    }

    unsigned min_compaction_threshold() {
        // During receiving stream operations, the less we compact the faster streaming is. For
        // bootstrap and replace thereThere are no readers so it is fine to be less aggressive with
        // compactions as long as we don't ignore them completely (this could create a problem for
        // when streaming ends)
        if (_is_bootstrap_or_replace) {
            auto target = std::min(schema()->max_compaction_threshold(), 16);
            return std::max(schema()->min_compaction_threshold(), target);
        } else {
            return schema()->min_compaction_threshold();
        }
    }

    /*!
     * \brief get sstables by key
     * Return a set of the sstables names that contain the given
     * partition key in nodetool format
     */
    future<std::unordered_set<sstring>> get_sstables_by_partition_key(const sstring& key) const;

    const sstables::sstable_set& get_sstable_set() const;
    lw_shared_ptr<const sstable_list> get_sstables() const;
    lw_shared_ptr<const sstable_list> get_sstables_including_compacted_undeleted() const;
    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const;
    std::vector<sstables::shared_sstable> select_sstables(const dht::partition_range& range) const;
    // Return all sstables but those that are off-strategy like the ones in maintenance set and staging dir.
    std::vector<sstables::shared_sstable> in_strategy_sstables() const;
    size_t sstables_count() const;
    std::vector<uint64_t> sstable_count_per_level() const;
    int64_t get_unleveled_sstables() const;

    void start_compaction();
    void trigger_compaction();
    void try_trigger_compaction() noexcept;
    future<> run_compaction(sstables::compaction_descriptor descriptor);
    void trigger_offstrategy_compaction();
    future<> run_offstrategy_compaction();
    void set_compaction_strategy(sstables::compaction_strategy_type strategy);
    const sstables::compaction_strategy& get_compaction_strategy() const {
        return _compaction_strategy;
    }

    sstables::compaction_strategy& get_compaction_strategy() {
        return _compaction_strategy;
    }

    table_stats& get_stats() const {
        return _stats;
    }

    const db::view::stats& get_view_stats() const {
        return _view_stats;
    }

    ::cf_stats* cf_stats() {
        return _config.cf_stats;
    }

    const config& get_config() const {
        return _config;
    }

    compaction_manager& get_compaction_manager() const {
        return _compaction_manager;
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
    cache_hit_rate get_hit_rate(gms::inet_address addr);
    void drop_hit_rate(gms::inet_address addr);

    future<> run_with_compaction_disabled(std::function<future<> ()> func);

    void enable_auto_compaction();
    void disable_auto_compaction();
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
    future<row_locker::lock_holder> push_view_replica_updates(const schema_ptr& s, const frozen_mutation& fm, db::timeout_clock::time_point timeout,
            tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem) const;
    future<row_locker::lock_holder> push_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout,
            tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem) const;
    future<row_locker::lock_holder>
    stream_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout,
            std::vector<sstables::shared_sstable>& excluded_sstables) const;

    void add_coordinator_read_latency(utils::estimated_histogram::duration latency);
    std::chrono::milliseconds get_coordinator_read_latency_percentile(double percentile);

    secondary_index::secondary_index_manager& get_index_manager() {
        return _index_manager;
    }

    sstables::sstables_manager& get_sstables_manager() const {
        assert(_config.sstables_manager);
        return *_config.sstables_manager;
    }

    // Reader's schema must be the same as the base schema of each of the views.
    future<> populate_views(
            std::vector<db::view::view_and_base>,
            dht::token base_token,
            flat_mutation_reader&&,
            gc_clock::time_point);

    reader_concurrency_semaphore& streaming_read_concurrency_semaphore() {
        return *_config.streaming_read_concurrency_semaphore;
    }

    reader_concurrency_semaphore& compaction_concurrency_semaphore() {
        return *_config.compaction_concurrency_semaphore;
    }

private:
    future<row_locker::lock_holder> do_push_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout, mutation_source&& source,
            tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem, const io_priority_class& io_priority, query::partition_slice::option_set custom_opts) const;
    std::vector<view_ptr> affected_views(const schema_ptr& base, const mutation& update, gc_clock::time_point now) const;
    future<> generate_and_propagate_view_updates(const schema_ptr& base,
            reader_permit permit,
            std::vector<db::view::view_and_base>&& views,
            mutation&& m,
            flat_mutation_reader_opt existings,
            tracing::trace_state_ptr tr_state,
            gc_clock::time_point now) const;

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
    future<> seal_active_memtable(flush_permit&&);

    void check_valid_rp(const db::replay_position&) const;
public:
    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    future<bool> for_all_partitions_slow(schema_ptr, reader_permit permit, std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const;

    friend std::ostream& operator<<(std::ostream& out, const column_family& cf);
    // Testing purposes.
    friend class column_family_test;

    friend class distributed_loader;

private:
    timer<> _off_strategy_trigger;
    void do_update_off_strategy_trigger();

public:
    void update_off_strategy_trigger();
    void enable_off_strategy_trigger();
};

class user_types_metadata;

class keyspace_metadata final {
    sstring _name;
    sstring _strategy_name;
    std::map<sstring, sstring> _strategy_options;
    std::unordered_map<sstring, schema_ptr> _cf_meta_data;
    bool _durable_writes;
    user_types_metadata _user_types;
public:
    keyspace_metadata(std::string_view name,
                 std::string_view strategy_name,
                 std::map<sstring, sstring> strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{});
    keyspace_metadata(std::string_view name,
                 std::string_view strategy_name,
                 std::map<sstring, sstring> strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs,
                 user_types_metadata user_types);
    static lw_shared_ptr<keyspace_metadata>
    new_keyspace(std::string_view name,
                 std::string_view strategy_name,
                 std::map<sstring, sstring> options,
                 bool durables_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{});
    void validate(const locator::shared_token_metadata& stm) const;
    const sstring& name() const {
        return _name;
    }
    const sstring& strategy_name() const {
        return _strategy_name;
    }
    const std::map<sstring, sstring>& strategy_options() const {
        return _strategy_options;
    }
    const std::unordered_map<sstring, schema_ptr>& cf_meta_data() const {
        return _cf_meta_data;
    }
    bool durable_writes() const {
        return _durable_writes;
    }
    user_types_metadata& user_types() {
        return _user_types;
    }
    const user_types_metadata& user_types() const {
        return _user_types;
    }
    void add_or_update_column_family(const schema_ptr& s) {
        _cf_meta_data[s->cf_name()] = s;
    }
    void remove_column_family(const schema_ptr& s) {
        _cf_meta_data.erase(s->cf_name());
    }
    void add_user_type(const user_type ut);
    void remove_user_type(const user_type ut);
    std::vector<schema_ptr> tables() const;
    std::vector<view_ptr> views() const;
    friend std::ostream& operator<<(std::ostream& os, const keyspace_metadata& m);
};

class keyspace {
public:
    struct config {
        std::vector<sstring> all_datadirs;
        sstring datadir;
        bool enable_commitlog = true;
        bool enable_disk_reads = true;
        bool enable_disk_writes = true;
        bool enable_cache = true;
        bool enable_incremental_backups = false;
        utils::updateable_value<bool> compaction_enforce_min_threshold{false};
        bool enable_dangerous_direct_import_of_cassandra_counters = false;
        ::dirty_memory_manager* dirty_memory_manager = &default_dirty_memory_manager;
        reader_concurrency_semaphore* streaming_read_concurrency_semaphore;
        reader_concurrency_semaphore* compaction_concurrency_semaphore;
        ::cf_stats* cf_stats = nullptr;
        seastar::scheduling_group memtable_scheduling_group;
        seastar::scheduling_group memtable_to_cache_scheduling_group;
        seastar::scheduling_group compaction_scheduling_group;
        seastar::scheduling_group memory_compaction_scheduling_group;
        seastar::scheduling_group statement_scheduling_group;
        seastar::scheduling_group streaming_scheduling_group;
        bool enable_metrics_reporting = false;
        db::timeout_semaphore* view_update_concurrency_semaphore = nullptr;
        size_t view_update_concurrency_semaphore_limit;
    };
private:
    std::unique_ptr<locator::abstract_replication_strategy> _replication_strategy;
    lw_shared_ptr<keyspace_metadata> _metadata;
    shared_promise<> _populated;
    config _config;
public:
    explicit keyspace(lw_shared_ptr<keyspace_metadata> metadata, config cfg);

    void update_from(const locator::shared_token_metadata& stm, lw_shared_ptr<keyspace_metadata>);

    /** Note: return by shared pointer value, since the meta data is
     * semi-volatile. I.e. we could do alter keyspace at any time, and
     * boom, it is replaced.
     */
    lw_shared_ptr<keyspace_metadata> metadata() const;
    void create_replication_strategy(const locator::shared_token_metadata& stm, const std::map<sstring, sstring>& options);
    /**
     * This should not really be return by reference, since replication
     * strategy is also volatile in that it could be replaced at "any" time.
     * However, all current uses at least are "instantateous", i.e. does not
     * carry it across a continuation. So it is sort of same for now, but
     * should eventually be refactored.
     */
    locator::abstract_replication_strategy& get_replication_strategy();
    const locator::abstract_replication_strategy& get_replication_strategy() const;
    column_family::config make_column_family_config(const schema& s, const database& db) const;
    future<> make_directory_for_column_family(const sstring& name, utils::UUID uuid);
    void add_or_update_column_family(const schema_ptr& s);
    void add_user_type(const user_type ut);
    void remove_user_type(const user_type ut);

    // FIXME to allow simple registration at boostrap
    void set_replication_strategy(std::unique_ptr<locator::abstract_replication_strategy> replication_strategy);

    const bool incremental_backups_enabled() const {
        return _config.enable_incremental_backups;
    }

    void set_incremental_backups(bool val) {
        _config.enable_incremental_backups = val;
    }

    const sstring& datadir() const {
        return _config.datadir;
    }

    sstring column_family_directory(const sstring& base_path, const sstring& name, utils::UUID uuid) const;
    sstring column_family_directory(const sstring& name, utils::UUID uuid) const;

    future<> ensure_populated() const;
    void mark_as_populated();
};

class no_such_keyspace : public std::runtime_error {
public:
    no_such_keyspace(std::string_view ks_name);
};

class no_such_column_family : public std::runtime_error {
public:
    no_such_column_family(const utils::UUID& uuid);
    no_such_column_family(std::string_view ks_name, std::string_view cf_name);
};


struct database_config {
    seastar::scheduling_group memtable_scheduling_group;
    seastar::scheduling_group memtable_to_cache_scheduling_group; // FIXME: merge with memtable_scheduling_group
    seastar::scheduling_group compaction_scheduling_group;
    seastar::scheduling_group memory_compaction_scheduling_group;
    seastar::scheduling_group statement_scheduling_group;
    seastar::scheduling_group streaming_scheduling_group;
    seastar::scheduling_group gossip_scheduling_group;
    size_t available_memory;
};

struct string_pair_eq {
    using is_transparent = void;
    using spair = std::pair<std::string_view, std::string_view>;
    bool operator()(spair lhs, spair rhs) const;
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
    friend class database_test;
public:
    enum class table_kind {
        system,
        user,
    };

private:
    ::cf_stats _cf_stats;
    static constexpr size_t max_count_concurrent_reads{100};
    size_t max_memory_concurrent_reads() { return _dbcfg.available_memory * 0.02; }
    // Assume a queued read takes up 10kB of memory, and allow 2% of memory to be filled up with such reads.
    size_t max_inactive_queue_length() { return _dbcfg.available_memory * 0.02 / 10000; }
    // They're rather heavyweight, so limit more
    static constexpr size_t max_count_streaming_concurrent_reads{10};
    size_t max_memory_streaming_concurrent_reads() { return _dbcfg.available_memory * 0.02; }
    static constexpr size_t max_count_system_concurrent_reads{10};
    size_t max_memory_system_concurrent_reads() { return _dbcfg.available_memory * 0.02; };
    size_t max_memory_pending_view_updates() const { return _dbcfg.available_memory * 0.1; }

    struct db_stats {
        uint64_t total_writes = 0;
        uint64_t total_writes_failed = 0;
        uint64_t total_writes_timedout = 0;
        uint64_t total_reads = 0;
        uint64_t total_reads_failed = 0;
        uint64_t sstable_read_queue_overloaded = 0;

        uint64_t short_data_queries = 0;
        uint64_t short_mutation_queries = 0;

        uint64_t multishard_query_unpopped_fragments = 0;
        uint64_t multishard_query_unpopped_bytes = 0;
        uint64_t multishard_query_failed_reader_stops = 0;
        uint64_t multishard_query_failed_reader_saves = 0;
    };

    lw_shared_ptr<db_stats> _stats;
    std::unique_ptr<cell_locker_stats> _cl_stats;

    const db::config& _cfg;

    dirty_memory_manager _system_dirty_memory_manager;
    dirty_memory_manager _dirty_memory_manager;

    database_config _dbcfg;
    flush_controller _memtable_controller;

    reader_concurrency_semaphore _read_concurrency_sem;
    reader_concurrency_semaphore _streaming_concurrency_sem;
    reader_concurrency_semaphore _compaction_concurrency_sem;
    reader_concurrency_semaphore _system_read_concurrency_sem;

    db::timeout_semaphore _view_update_concurrency_sem{max_memory_pending_view_updates()};

    cache_tracker _row_cache_tracker;

    inheriting_concrete_execution_stage<future<lw_shared_ptr<query::result>>,
        column_family*,
        schema_ptr,
        const query::read_command&,
        query::query_class_config,
        query::result_options,
        const dht::partition_range_vector&,
        tracing::trace_state_ptr,
        query::result_memory_limiter&,
        db::timeout_clock::time_point,
        query::querier_cache_context> _data_query_stage;

    inheriting_concrete_execution_stage<future<reconcilable_result>,
        table*,
        schema_ptr,
        const query::read_command&,
        query::query_class_config,
        const dht::partition_range&,
        tracing::trace_state_ptr,
        query::result_memory_accounter,
        db::timeout_clock::time_point,
        query::querier_cache_context> _mutation_query_stage;

    inheriting_concrete_execution_stage<
            future<>,
            database*,
            schema_ptr,
            const frozen_mutation&,
            tracing::trace_state_ptr,
            db::timeout_clock::time_point,
            db::commitlog::force_sync> _apply_stage;

    flat_hash_map<sstring, keyspace> _keyspaces;
    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>> _column_families;
    using ks_cf_to_uuid_t =
        flat_hash_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash, string_pair_eq>;
    ks_cf_to_uuid_t _ks_cf_to_uuid;
    std::unique_ptr<db::commitlog> _commitlog;
    utils::updateable_value_source<utils::UUID> _version;
    uint32_t _schema_change_count = 0;
    // compaction_manager object is referenced by all column families of a database.
    std::unique_ptr<compaction_manager> _compaction_manager;
    seastar::metrics::metric_groups _metrics;
    bool _enable_incremental_backups = false;
    utils::UUID _local_host_id;

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
    const locator::shared_token_metadata& _shared_token_metadata;

    sharded<semaphore>& _sst_dir_semaphore;

    bool _supports_infinite_bound_range_deletions = false;
    gms::feature::listener_registration _infinite_bound_range_deletions_reg;

    future<> init_commitlog();
public:
    const gms::feature_service& features() const { return _feat; }
    future<> apply_in_memory(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&&, db::timeout_clock::time_point timeout);
    future<> apply_in_memory(const mutation& m, column_family& cf, db::rp_handle&&, db::timeout_clock::time_point timeout);

    void set_local_id(utils::UUID uuid) noexcept { _local_host_id = std::move(uuid); }

private:
    using system_keyspace = bool_class<struct system_keyspace_tag>;
    void create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm, system_keyspace system);
    friend future<> db::system_keyspace::make(database& db);
    void setup_metrics();
    void setup_scylla_memory_diagnostics_producer();

    friend class db_apply_executor;
    future<> do_apply(schema_ptr, const frozen_mutation&, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout, db::commitlog::force_sync sync);
    future<> apply_with_commitlog(schema_ptr, column_family&, utils::UUID, const frozen_mutation&, db::timeout_clock::time_point timeout, db::commitlog::force_sync sync);
    future<> apply_with_commitlog(column_family& cf, const mutation& m, db::timeout_clock::time_point timeout);

    future<mutation> do_apply_counter_update(column_family& cf, const frozen_mutation& fm, schema_ptr m_schema, db::timeout_clock::time_point timeout,
                                             tracing::trace_state_ptr trace_state);

    template<typename Future>
    Future update_write_metrics(Future&& f);
    void update_write_metrics_for_timed_out_write();
    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>&, bool is_bootstrap, system_keyspace system);
public:
    static utils::UUID empty_version;

    query::result_memory_limiter& get_result_memory_limiter() {
        return _result_memory_limiter;
    }

    void set_enable_incremental_backups(bool val) { _enable_incremental_backups = val; }

    future<> parse_system_tables(distributed<service::storage_proxy>&, distributed<service::migration_manager>&);
    database(const db::config&, database_config dbcfg, service::migration_notifier& mn, gms::feature_service& feat, const locator::shared_token_metadata& stm, abort_source& as, sharded<semaphore>& sst_dir_sem);
    database(database&&) = delete;
    ~database();

    cache_tracker& row_cache_tracker() { return _row_cache_tracker; }
    future<> drop_caches() const;

    void update_version(const utils::UUID& version);

    const utils::UUID& get_version() const;
    utils::observable<utils::UUID>& observable_schema_version() const { return _version.as_observable(); }

    db::commitlog* commitlog() const {
        return _commitlog.get();
    }

    seastar::scheduling_group get_statement_scheduling_group() const { return _dbcfg.statement_scheduling_group; }
    seastar::scheduling_group get_streaming_scheduling_group() const { return _dbcfg.streaming_scheduling_group; }
    size_t get_available_memory() const { return _dbcfg.available_memory; }

    compaction_manager& get_compaction_manager() {
        return *_compaction_manager;
    }
    const compaction_manager& get_compaction_manager() const {
        return *_compaction_manager;
    }

    const locator::shared_token_metadata& get_shared_token_metadata() const { return _shared_token_metadata; }
    const locator::token_metadata& get_token_metadata() const { return *_shared_token_metadata.get(); }

    service::migration_notifier& get_notifier() { return _mnotifier; }
    const service::migration_notifier& get_notifier() const { return _mnotifier; }

    void add_column_family(keyspace& ks, schema_ptr schema, column_family::config cfg);
    future<> add_column_family_and_make_directory(schema_ptr schema);

    /* throws std::out_of_range if missing */
    const utils::UUID& find_uuid(std::string_view ks, std::string_view cf) const;
    const utils::UUID& find_uuid(const schema_ptr&) const;

    /**
     * Creates a keyspace for a given metadata if it still doesn't exist.
     *
     * @return ready future when the operation is complete
     */
    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>&);
    /* below, find_keyspace throws no_such_<type> on fail */
    keyspace& find_keyspace(std::string_view name);
    const keyspace& find_keyspace(std::string_view name) const;
    bool has_keyspace(std::string_view name) const;
    void validate_keyspace_update(keyspace_metadata& ksm);
    void validate_new_keyspace(keyspace_metadata& ksm);
    future<> update_keyspace(sharded<service::storage_proxy>& proxy, const sstring& name);
    void drop_keyspace(const sstring& name);
    std::vector<sstring> get_non_system_keyspaces() const;
    column_family& find_column_family(std::string_view ks, std::string_view name);
    const column_family& find_column_family(std::string_view ks, std::string_view name) const;
    column_family& find_column_family(const utils::UUID&);
    const column_family& find_column_family(const utils::UUID&) const;
    column_family& find_column_family(const schema_ptr&);
    const column_family& find_column_family(const schema_ptr&) const;
    bool column_family_exists(const utils::UUID& uuid) const;
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name) const;
    schema_ptr find_schema(const utils::UUID&) const;
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
    future<> stop();
    future<> close_tables(table_kind kind_to_close);

    future<> stop_large_data_handler();
    unsigned shard_of(const mutation& m);
    unsigned shard_of(const frozen_mutation& m);
    future<std::tuple<lw_shared_ptr<query::result>, cache_temperature>> query(schema_ptr, const query::read_command& cmd, query::result_options opts,
                                                                  const dht::partition_range_vector& ranges, tracing::trace_state_ptr trace_state,
                                                                  db::timeout_clock::time_point timeout);
    future<std::tuple<reconcilable_result, cache_temperature>> query_mutations(schema_ptr, const query::read_command& cmd, const dht::partition_range& range,
                                                tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout);
    // Apply the mutation atomically.
    // Throws timed_out_error when timeout is reached.
    future<> apply(schema_ptr, const frozen_mutation&, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, db::timeout_clock::time_point timeout);
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

    friend std::ostream& operator<<(std::ostream& out, const database& db);
    const flat_hash_map<sstring, keyspace>& get_keyspaces() const {
        return _keyspaces;
    }

    flat_hash_map<sstring, keyspace>& get_keyspaces() {
        return _keyspaces;
    }

    const std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& get_column_families() const {
        return _column_families;
    }

    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& get_column_families() {
        return _column_families;
    }

    std::vector<lw_shared_ptr<column_family>> get_non_system_column_families() const;

    std::vector<view_ptr> get_views() const;

    const ks_cf_to_uuid_t&
    get_column_families_mapping() const {
        return _ks_cf_to_uuid;
    }

    const db::config& get_config() const {
        return _cfg;
    }
    const db::extensions& extensions() const;

    db::large_data_handler* get_large_data_handler() const {
        return _large_data_handler.get();
    }

    db::large_data_handler* get_nop_large_data_handler() const {
        return _nop_large_data_handler.get();
    }

    sstables::sstables_manager& get_user_sstables_manager() const {
        assert(_user_sstables_manager);
        return *_user_sstables_manager;
    }

    sstables::sstables_manager& get_system_sstables_manager() const {
        assert(_system_sstables_manager);
        return *_system_sstables_manager;
    }

    dht::token_range_vector get_keyspace_local_ranges(sstring ks);

    void set_format(sstables::sstable_version_types format);
    void set_format_by_config();

    future<> flush_all_memtables();
    future<> flush(const sstring& ks, const sstring& cf);

    // See #937. Truncation now requires a callback to get a time stamp
    // that must be guaranteed to be the same for all shards.
    typedef std::function<future<db_clock::time_point>()> timestamp_func;

    /** Truncates the given column family */
    future<> truncate(sstring ksname, sstring cfname, timestamp_func);
    future<> truncate(const keyspace& ks, column_family& cf, timestamp_func, bool with_snapshot = true);
    future<> truncate_views(const column_family& base, db_clock::time_point truncated_at, bool should_flush);

    bool update_column_family(schema_ptr s);
    future<> drop_column_family(const sstring& ks_name, const sstring& cf_name, timestamp_func, bool with_snapshot = true);
    future<> remove(const column_family&) noexcept;

    const logalloc::region_group& dirty_memory_region_group() const {
        return _dirty_memory_manager.region_group();
    }

    std::unordered_set<sstring> get_initial_tokens();
    std::optional<gms::inet_address> get_replace_address();
    bool is_replacing();
    void register_connection_drop_notifier(netw::messaging_service& ms);

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

    friend class distributed_loader;

    db::data_listeners& data_listeners() const {
        return *_data_listeners;
    }

    bool supports_infinite_bound_range_deletions() {
        return _supports_infinite_bound_range_deletions;
    }

    // Get the maximum result size for an unlimited query, appropriate for the
    // query class, which is deduced from the current scheduling group.
    query::max_result_size get_unlimited_query_max_result_size() const;

    // Get the reader concurrency semaphore, appropriate for the query class,
    // which is deduced from the current scheduling group.
    reader_concurrency_semaphore& get_reader_concurrency_semaphore();

    sharded<semaphore>& get_sharded_sst_dir_semaphore() {
        return _sst_dir_semaphore;
    }
};

future<> start_large_data_handler(sharded<database>& db);
future<> stop_database(sharded<database>& db);

// Creates a streaming reader that reads from all shards.
//
// Shard readers are created via `table::make_streaming_reader()`.
// Range generator must generate disjoint, monotonically increasing ranges.
flat_mutation_reader make_multishard_streaming_reader(distributed<database>& db, schema_ptr schema,
        std::function<std::optional<dht::partition_range>()> range_generator);

bool is_internal_keyspace(std::string_view name);

#endif /* DATABASE_HH_ */
