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

#include "dht/i_partitioner.hh"
#include "locator/abstract_replication_strategy.hh"
#include "index/secondary_index_manager.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include <seastar/core/execution_stage.hh>
#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include "core/distributed.hh"
#include <functional>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <iosfwd>
#include <boost/functional/hash.hpp>
#include <boost/range/algorithm/find.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "compound.hh"
#include "core/future.hh"
#include "core/gate.hh"
#include "cql3/column_specification.hh"
#include "db/commitlog/replay_position.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "atomic_cell.hh"
#include "query-request.hh"
#include "keys.hh"
#include "mutation.hh"
#include "memtable.hh"
#include <list>
#include "mutation_reader.hh"
#include "row_cache.hh"
#include "compaction_strategy.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/histogram.hh"
#include "utils/estimated_histogram.hh"
#include "sstables/sstable_set.hh"
#include "sstables/progress_monitor.hh"
#include "sstables/version.hh"
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/metrics_registration.hh>
#include "tracing/trace_state.hh"
#include "db/view/view.hh"
#include "db/view/view_update_backlog.hh"
#include "db/view/row_locking.hh"
#include "lister.hh"
#include "utils/phased_barrier.hh"
#include "backlog_controller.hh"
#include "dirty_memory_manager.hh"
#include "reader_concurrency_semaphore.hh"
#include "db/timeout_clock.hh"
#include "querier.hh"
#include "mutation_query.hh"
#include "db/large_partition_handler.hh"
#include <unordered_set>

class cell_locker;
class cell_locker_stats;
class locked_cell;

class frozen_mutation;
class reconcilable_result;

namespace service {
class storage_proxy;
}

namespace netw {
class messaging_service;
}

namespace sstables {

class sstable;
class entry_descriptor;
class compaction_descriptor;
class foreign_sstable_open_info;

}

class compaction_manager;

namespace ser {
template<typename T>
class serializer;
}

namespace db {
class commitlog;
class config;
class rp_handle;

namespace system_keyspace {
void make(database& db, bool durable, bool volatile_testing_only);
}
}

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
    seal_delayed_fn_type _seal_delayed_fn;
    std::function<schema_ptr()> _current_schema;
    dirty_memory_manager* _dirty_memory_manager;
    std::experimental::optional<shared_promise<>> _flush_coalescing;
    seastar::scheduling_group _compaction_scheduling_group;
public:
    memtable_list(
            seal_immediate_fn_type seal_immediate_fn,
            seal_delayed_fn_type seal_delayed_fn,
            std::function<schema_ptr()> cs,
            dirty_memory_manager* dirty_memory_manager,
            seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group())
        : _memtables({})
        , _seal_immediate_fn(seal_immediate_fn)
        , _seal_delayed_fn(seal_delayed_fn)
        , _current_schema(cs)
        , _dirty_memory_manager(dirty_memory_manager)
        , _compaction_scheduling_group(compaction_scheduling_group) {
        add_memtable();
    }

    memtable_list(
            seal_immediate_fn_type seal_immediate_fn,
            std::function<schema_ptr()> cs,
            dirty_memory_manager* dirty_memory_manager,
            seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group())
        : memtable_list(std::move(seal_immediate_fn), {}, std::move(cs), dirty_memory_manager, compaction_scheduling_group) {
    }

    memtable_list(std::function<schema_ptr()> cs, dirty_memory_manager* dirty_memory_manager, seastar::scheduling_group compaction_scheduling_group = seastar::current_scheduling_group())
        : memtable_list({}, {}, std::move(cs), dirty_memory_manager, compaction_scheduling_group) {
    }

    bool may_flush() const {
        return bool(_seal_immediate_fn);
    }

    shared_memtable back() {
        return _memtables.back();
    }

    // The caller has to make sure the element exist before calling this.
    void erase(const shared_memtable& element) {
        _memtables.erase(boost::range::find(_memtables, element));
    }
    void clear() {
        _memtables.clear();
    }

    size_t size() const {
        return _memtables.size();
    }

    future<> seal_active_memtable_immediate(flush_permit&& permit) {
        return _seal_immediate_fn(std::move(permit));
    }

    future<> seal_active_memtable_delayed() {
        if (_seal_delayed_fn) {
            return _seal_delayed_fn();
        }
        return request_flush();
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
    future<> request_flush();
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
};

class cache_temperature {
    float hit_rate;
    explicit cache_temperature(uint8_t hr) : hit_rate(hr/255.0f) {}
public:
    uint8_t get_serialized_temperature() const {
        return hit_rate * 255;
    }
    cache_temperature() : hit_rate(0) {}
    explicit cache_temperature(float hr) : hit_rate(hr) {}
    explicit operator float() const { return hit_rate; }
    static cache_temperature invalid() { return cache_temperature(-1.0f); }
    friend struct ser::serializer<cache_temperature>;
};

class table;
using column_family = table;

class database_sstable_write_monitor;

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
        bool compaction_enforce_min_threshold = false;
        ::dirty_memory_manager* dirty_memory_manager = &default_dirty_memory_manager;
        ::dirty_memory_manager* streaming_dirty_memory_manager = &default_dirty_memory_manager;
        reader_concurrency_semaphore* read_concurrency_semaphore;
        reader_concurrency_semaphore* streaming_read_concurrency_semaphore;
        ::cf_stats* cf_stats = nullptr;
        seastar::scheduling_group memtable_scheduling_group;
        seastar::scheduling_group memtable_to_cache_scheduling_group;
        seastar::scheduling_group compaction_scheduling_group;
        seastar::scheduling_group memory_compaction_scheduling_group;
        seastar::scheduling_group statement_scheduling_group;
        seastar::scheduling_group streaming_scheduling_group;
        bool enable_metrics_reporting = false;
        db::large_partition_handler* large_partition_handler;
        db::timeout_semaphore* view_update_concurrency_semaphore;
        size_t view_update_concurrency_semaphore_limit;
    };
    struct no_commitlog {};
    struct stats {
        /** Number of times flush has resulted in the memtable being switched out. */
        int64_t memtable_switch_count = 0;
        /** Estimated number of tasks pending for this column family */
        int64_t pending_flushes = 0;
        int64_t live_disk_space_used = 0;
        int64_t total_disk_space_used = 0;
        int64_t live_sstable_count = 0;
        /** Estimated number of compactions pending for this column family */
        int64_t pending_compactions = 0;
        utils::timed_rate_moving_average_and_histogram reads{256};
        utils::timed_rate_moving_average_and_histogram writes{256};
        utils::estimated_histogram estimated_read;
        utils::estimated_histogram estimated_write;
        utils::estimated_histogram estimated_sstable_per_read{35};
        utils::timed_rate_moving_average_and_histogram tombstone_scanned;
        utils::timed_rate_moving_average_and_histogram live_scanned;
        utils::estimated_histogram estimated_coordinator_read;
    };

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
    mutable stats _stats;
    mutable db::view::stats _view_stats;
    mutable row_locker::stats _row_locker_stats;

    uint64_t _failed_counter_applies_to_memtable = 0;

    template<typename... Args>
    void do_apply(db::rp_handle&&, Args&&... args);

    lw_shared_ptr<memtable_list> _memtables;

    // In older incarnations, we simply commited the mutations to memtables.
    // However, doing that makes it harder for us to provide QoS within the
    // disk subsystem. Keeping them in separate memtables allow us to properly
    // classify those streams into its own I/O class
    //
    // We could write those directly to disk, but we still want the mutations
    // coming through the wire to go to a memtable staging area.  This has two
    // major advantages:
    //
    // first, it will allow us to properly order the partitions. They are
    // hopefuly sent in order but we can't really guarantee that without
    // sacrificing sender-side parallelism.
    //
    // second, we will be able to coalesce writes from multiple plan_id's and
    // even multiple senders, as well as automatically tapping into the dirty
    // memory throttling mechanism, guaranteeing we will not overload the
    // server.
    lw_shared_ptr<memtable_list> _streaming_memtables;
    utils::phased_barrier _streaming_flush_phaser;

    // If mutations are fragmented during streaming the sstables cannot be made
    // visible immediately after memtable flush, because that could cause
    // readers to see only a part of a partition thus violating isolation
    // guarantees.
    // Mutations that are sent in fragments are kept separately in per-streaming
    // plan memtables and the resulting sstables are not made visible until
    // the streaming is complete.
    struct monitored_sstable {
        std::unique_ptr<database_sstable_write_monitor> monitor;
        sstables::shared_sstable sstable;
    };

    struct streaming_memtable_big {
        lw_shared_ptr<memtable_list> memtables;
        std::vector<monitored_sstable> sstables;
        seastar::gate flush_in_progress;
    };
    std::unordered_map<utils::UUID, lw_shared_ptr<streaming_memtable_big>> _streaming_memtables_big;

    future<std::vector<monitored_sstable>> flush_streaming_big_mutations(utils::UUID plan_id);
    void apply_streaming_big_mutation(schema_ptr m_schema, utils::UUID plan_id, const frozen_mutation& m);
    future<> seal_active_streaming_memtable_big(streaming_memtable_big& smb, flush_permit&&);

    lw_shared_ptr<memtable_list> make_memory_only_memtable_list();
    lw_shared_ptr<memtable_list> make_memtable_list();
    lw_shared_ptr<memtable_list> make_streaming_memtable_list();
    lw_shared_ptr<memtable_list> make_streaming_memtable_big_list(streaming_memtable_big& smb);

    sstables::compaction_strategy _compaction_strategy;
    // generation -> sstable. Ordered by key so we can easily get the most recent.
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
    // sstables that are shared between several shards so we want to rewrite
    // them (split the data belonging to this shard to a separate sstable),
    // but for correct compaction we need to start the compaction only after
    // reading all sstables.
    std::unordered_map<uint64_t, sstables::shared_sstable> _sstables_need_rewrite;
    // sstables that should not be compacted (e.g. because they need to be used
    // to generate view updates later)
    std::unordered_map<uint64_t, sstables::shared_sstable> _sstables_staging;
    // Control background fibers waiting for sstables to be deleted
    seastar::gate _sstable_deletion_gate;
    // This semaphore ensures that an operation like snapshot won't have its selected
    // sstables deleted by compaction in parallel, a race condition which could
    // easily result in failure.
    seastar::semaphore _sstable_deletion_sem = {1};
    // There are situations in which we need to stop writing sstables. Flushers will take
    // the read lock, and the ones that wish to stop that process will take the write lock.
    rwlock _sstables_lock;
    mutable row_cache _cache; // Cache covers only sstables.
    std::experimental::optional<int64_t> _sstable_generation = {};

    db::replay_position _highest_rp;
    db::replay_position _lowest_allowed_rp;

    // Provided by the database that owns this commitlog
    db::commitlog* _commitlog;
    compaction_manager& _compaction_manager;
    secondary_index::secondary_index_manager _index_manager;
    int _compaction_disabled = 0;
    utils::phased_barrier _flush_barrier;
    seastar::gate _streaming_flush_gate;
    std::vector<view_ptr> _views;

    std::unique_ptr<cell_locker> _counter_cell_locks;
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
    // logic (like the streaming_flush_gate). That is particularly useful if there is a particular
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
public:
    future<> add_sstable_and_update_cache(sstables::shared_sstable sst);
    void move_sstable_from_staging_in_thread(sstables::shared_sstable sst);
    sstables::shared_sstable get_staging_sstable(uint64_t generation) {
        auto it = _sstables_staging.find(generation);
        return it != _sstables_staging.end() ? it->second : nullptr;
    }
private:
    void update_stats_for_new_sstable(uint64_t disk_space_used_by_sstable, const std::vector<unsigned>& shards_for_the_sstable) noexcept;
    // Adds new sstable to the set of sstables
    // Doesn't update the cache. The cache must be synchronized in order for reads to see
    // the writes contained in this sstable.
    // Cache must be synchronized atomically with this, otherwise write atomicity may not be respected.
    // Doesn't trigger compaction.
    // Strong exception guarantees.
    void add_sstable(sstables::shared_sstable sstable, const std::vector<unsigned>& shards_for_the_sstable);
    // returns an empty pointer if sstable doesn't belong to current shard.
    future<sstables::shared_sstable> open_sstable(sstables::foreign_sstable_open_info info, sstring dir,
        int64_t generation, sstables::sstable_version_types v, sstables::sstable_format_types f);
    void load_sstable(sstables::shared_sstable& sstable, bool reset_level = false);
    lw_shared_ptr<memtable> new_memtable();
    lw_shared_ptr<memtable> new_streaming_memtable();
    future<stop_iteration> try_flush_memtable_to_sstable(lw_shared_ptr<memtable> memt, sstable_write_permit&& permit);
    // Caller must keep m alive.
    future<> update_cache(lw_shared_ptr<memtable> m, sstables::shared_sstable sst);
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
        return (*_sstable_generation)++ * smp::count + engine().cpu_id();
    }

    // inverse of calculate_generation_for_new_table(), used to determine which
    // shard a sstable should be opened at.
    static int64_t calculate_shard_from_sstable_generation(int64_t sstable_generation) {
        return sstable_generation % smp::count;
    }

    // Rebuilds existing sstable set with new sstables added to it and old sstables removed from it.
    void rebuild_sstable_list(const std::vector<sstables::shared_sstable>& new_sstables,
        const std::vector<sstables::shared_sstable>& old_sstables);

    // Rebuilds the sstable set right away and schedule deletion of old sstables.
    void on_compaction_completion(const std::vector<sstables::shared_sstable>& new_sstables,
        const std::vector<sstables::shared_sstable>& sstables_to_remove);

    void rebuild_statistics();

    // This function replaces new sstables by their ancestors, which are sstables that needed resharding.
    void replace_ancestors_needed_rewrite(std::unordered_set<uint64_t> ancestors, std::vector<sstables::shared_sstable> new_sstables);
    void remove_ancestors_needed_rewrite(std::unordered_set<uint64_t> ancestors);
private:
    mutation_source_opt _virtual_reader;
    // Creates a mutation reader which covers given sstables.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // The 'range' parameter must be live as long as the reader is used.
    // Mutations returned by the reader will all have given schema.
    flat_mutation_reader make_sstable_reader(schema_ptr schema,
                                        lw_shared_ptr<sstables::sstable_set> sstables,
                                        const dht::partition_range& range,
                                        const query::partition_slice& slice,
                                        const io_priority_class& pc,
                                        tracing::trace_state_ptr trace_state,
                                        streamed_mutation::forwarding fwd,
                                        mutation_reader::forwarding fwd_mr) const;

    snapshot_source sstables_as_snapshot_source();
    partition_presence_checker make_partition_presence_checker(lw_shared_ptr<sstables::sstable_set>);
    std::chrono::steady_clock::time_point _sstable_writes_disabled_at;
    void do_trigger_compaction();
public:
    bool has_shared_sstables() const {
        return bool(_sstables_need_rewrite.size());
    }

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
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc = default_priority_class(),
            tracing::trace_state_ptr trace_state = nullptr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const;
    flat_mutation_reader make_reader_excluding_sstable(schema_ptr schema,
            sstables::shared_sstable sst,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc = default_priority_class(),
            tracing::trace_state_ptr trace_state = nullptr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::yes) const;

    flat_mutation_reader make_reader(schema_ptr schema, const dht::partition_range& range = query::full_partition_range) const {
        auto& full_slice = schema->full_slice();
        return make_reader(std::move(schema), range, full_slice);
    }

    // The streaming mutation reader differs from the regular mutation reader in that:
    //  - Reflects all writes accepted by replica prior to creation of the
    //    reader and a _bounded_ amount of writes which arrive later.
    //  - Does not populate the cache
    // Requires ranges to be sorted and disjoint.
    flat_mutation_reader make_streaming_reader(schema_ptr schema,
            const dht::partition_range_vector& ranges) const;

    sstables::shared_sstable make_streaming_sstable_for_write(std::optional<sstring> subdir = {});
    sstables::shared_sstable make_streaming_staging_sstable() {
        return make_streaming_sstable_for_write("staging");
    }

    mutation_source as_mutation_source() const;
    mutation_source as_mutation_source_excluding(sstables::shared_sstable sst) const;

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
    future<const_mutation_partition_ptr> find_partition(schema_ptr, const dht::decorated_key& key) const;
    future<const_mutation_partition_ptr> find_partition_slow(schema_ptr, const partition_key& key) const;
    future<const_row_ptr> find_row(schema_ptr, const dht::decorated_key& partition_key, clustering_key clustering_key) const;
    // Applies given mutation to this column family
    // The mutation is always upgraded to current schema.
    void apply(const frozen_mutation& m, const schema_ptr& m_schema, db::rp_handle&& = {});
    void apply(const mutation& m, db::rp_handle&& = {});
    void apply_streaming_mutation(schema_ptr, utils::UUID plan_id, const frozen_mutation&, bool fragmented);

    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(schema_ptr,
        const query::read_command& cmd,
        query::result_options opts,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        query::result_memory_limiter& memory_limiter,
        uint64_t max_result_size,
        db::timeout_clock::time_point timeout = db::no_timeout,
        query::querier_cache_context cache_ctx = { });

    void start();
    future<> stop();
    future<> flush();
    future<> flush_streaming_mutations(utils::UUID plan_id, dht::partition_range_vector ranges = dht::partition_range_vector{});
    future<> fail_streaming_mutations(utils::UUID plan_id);
    future<> clear(); // discards memtable(s) without flushing them to disk.
    future<db::replay_position> discard_sstables(db_clock::time_point);

    // Important warning: disabling writes will only have an effect in the current shard.
    // The other shards will keep writing tables at will. Therefore, you very likely need
    // to call this separately in all shards first, to guarantee that none of them are writing
    // new data before you can safely assume that the whole node is disabled.
    future<int64_t> disable_sstable_write();

    // SSTable writes are now allowed again, and generation is updated to new_generation if != -1
    // returns the amount of microseconds elapsed since we disabled writes.
    std::chrono::steady_clock::duration enable_sstable_write(int64_t new_generation) {
        if (new_generation != -1) {
            update_sstables_known_generation(new_generation);
        }
        _sstables_lock.write_unlock();
        return std::chrono::steady_clock::now() - _sstable_writes_disabled_at;
    }

    // Make sure the generation numbers are sequential, starting from "start".
    // Generations before "start" are left untouched.
    //
    // Return the highest generation number seen so far
    //
    // Word of warning: although this function will reshuffle anything over "start", it is
    // very dangerous to do that with live SSTables. This is meant to be used with SSTables
    // that are not yet managed by the system.
    //
    // Parameter all_generations stores the generation of all SSTables in the system, so it
    // will be easy to determine which SSTable is new.
    // An example usage would query all shards asking what is the highest SSTable number known
    // to them, and then pass that + 1 as "start".
    future<std::vector<sstables::entry_descriptor>> reshuffle_sstables(std::set<int64_t> all_generations, int64_t start);

    // FIXME: this is just an example, should be changed to something more
    // general. compact_all_sstables() starts a compaction of all sstables.
    // It doesn't flush the current memtable first. It's just a ad-hoc method,
    // not a real compaction policy.
    future<> compact_all_sstables();
    // Compact all sstables provided in the vector.
    // If cleanup is set to true, compaction_sstables will run on behalf of a cleanup job,
    // meaning that irrelevant keys will be discarded.
    future<> compact_sstables(sstables::compaction_descriptor descriptor, bool cleanup = false);
    // Performs a cleanup on each sstable of this column family, excluding
    // those ones that are irrelevant to this node or being compacted.
    // Cleanup is about discarding keys that are no longer relevant for a
    // given sstable, e.g. after node loses part of its token range because
    // of a newly added node.
    future<> cleanup_sstables(sstables::compaction_descriptor descriptor);

    future<bool> snapshot_exists(sstring name);

    db::replay_position set_low_replay_position_mark();

    future<> snapshot(sstring name);
    future<std::unordered_map<sstring, snapshot_details>> get_snapshot_details();

    const bool incremental_backups_enabled() const {
        return _config.enable_incremental_backups;
    }

    void set_incremental_backups(bool val) {
        _config.enable_incremental_backups = val;
    }

    bool compaction_enforce_min_threshold() const {
        return _config.compaction_enforce_min_threshold;
    }

    /*!
     * \brief get sstables by key
     * Return a set of the sstables names that contain the given
     * partition key in nodetool format
     */
    future<std::unordered_set<sstring>> get_sstables_by_partition_key(const sstring& key) const;

    const sstables::sstable_set& get_sstable_set() const;
    lw_shared_ptr<sstable_list> get_sstables() const;
    lw_shared_ptr<sstable_list> get_sstables_including_compacted_undeleted() const;
    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const;
    std::vector<sstables::shared_sstable> select_sstables(const dht::partition_range& range) const;
    std::vector<sstables::shared_sstable> candidates_for_compaction() const;
    std::vector<sstables::shared_sstable> sstables_need_rewrite() const;
    size_t sstables_count() const;
    std::vector<uint64_t> sstable_count_per_level() const;
    int64_t get_unleveled_sstables() const;

    void start_compaction();
    void trigger_compaction();
    void try_trigger_compaction() noexcept;
    future<> run_compaction(sstables::compaction_descriptor descriptor);
    void set_compaction_strategy(sstables::compaction_strategy_type strategy);
    const sstables::compaction_strategy& get_compaction_strategy() const {
        return _compaction_strategy;
    }

    sstables::compaction_strategy& get_compaction_strategy() {
        return _compaction_strategy;
    }

    const stats& get_stats() const {
        return _stats;
    }

    ::cf_stats* cf_stats() {
        return _config.cf_stats;
    }

    compaction_manager& get_compaction_manager() const {
        return _compaction_manager;
    }

    cache_temperature get_global_cache_hit_rate() const {
        return _global_cache_hit_rate;
    }

    void set_global_cache_hit_rate(cache_temperature rate) {
        _global_cache_hit_rate = rate;
    }

    void set_hit_rate(gms::inet_address addr, cache_temperature rate);
    cache_hit_rate get_hit_rate(gms::inet_address addr);
    void drop_hit_rate(gms::inet_address addr);

    future<> run_with_compaction_disabled(std::function<future<> ()> func);

    utils::phased_barrier::operation write_in_progress() {
        return _pending_writes_phaser.start();
    }

    future<> await_pending_writes() {
        return _pending_writes_phaser.advance_and_await();
    }

    utils::phased_barrier::operation read_in_progress() {
        return _pending_reads_phaser.start();
    }

    future<> await_pending_reads() {
        return _pending_reads_phaser.advance_and_await();
    }

    void add_or_update_view(view_ptr v);
    void remove_view(view_ptr v);
    void clear_views();
    const std::vector<view_ptr>& views() const;
    future<row_locker::lock_holder> push_view_replica_updates(const schema_ptr& s, const frozen_mutation& fm, db::timeout_clock::time_point timeout) const;
    future<row_locker::lock_holder> push_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout) const;
    future<row_locker::lock_holder> stream_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout, sstables::shared_sstable excluded_sstable) const;
    void add_coordinator_read_latency(utils::estimated_histogram::duration latency);
    std::chrono::milliseconds get_coordinator_read_latency_percentile(double percentile);

    secondary_index::secondary_index_manager& get_index_manager() {
        return _index_manager;
    }

    db::large_partition_handler* get_large_partition_handler() {
        assert(_config.large_partition_handler);
        return _config.large_partition_handler;
    }

    future<> populate_views(
            std::vector<view_ptr>,
            dht::token base_token,
            flat_mutation_reader&&);

    reader_concurrency_semaphore& read_concurrency_semaphore() {
        return *_config.read_concurrency_semaphore;
    }

private:
    future<row_locker::lock_holder> do_push_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout, mutation_source&& source) const;
    std::vector<view_ptr> affected_views(const schema_ptr& base, const mutation& update) const;
    future<> generate_and_propagate_view_updates(const schema_ptr& base,
            std::vector<view_ptr>&& views,
            mutation&& m,
            flat_mutation_reader_opt existings) const;

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

    // I am assuming here that the repair process will potentially send ranges containing
    // few mutations, definitely not enough to fill a memtable. It wants to know whether or
    // not each of those ranges individually succeeded or failed, so we need a future for
    // each.
    //
    // One of the ways to fix that, is changing the repair itself to send more mutations at
    // a single batch. But relying on that is a bad idea for two reasons:
    //
    // First, the goals of the SSTable writer and the repair sender are at odds. The SSTable
    // writer wants to write as few SSTables as possible, while the repair sender wants to
    // break down the range in pieces as small as it can and checksum them individually, so
    // it doesn't have to send a lot of mutations for no reason.
    //
    // Second, even if the repair process wants to process larger ranges at once, some ranges
    // themselves may be small. So while most ranges would be large, we would still have
    // potentially some fairly small SSTables lying around.
    //
    // The best course of action in this case is to coalesce the incoming streams write-side.
    // repair can now choose whatever strategy - small or big ranges - it wants, resting assure
    // that the incoming memtables will be coalesced together.
    future<> seal_active_streaming_memtable_immediate(flush_permit&&);

    // filter manifest.json files out
    static bool manifest_json_filter(const lister::path&, const directory_entry& entry);

    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    // Func signature: bool (const decorated_key& dk, const mutation_partition& mp)
    template <typename Func>
    future<bool> for_all_partitions(schema_ptr, Func&& func) const;
    void check_valid_rp(const db::replay_position&) const;
public:
    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    future<bool> for_all_partitions_slow(schema_ptr, std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const;

    friend std::ostream& operator<<(std::ostream& out, const column_family& cf);
    // Testing purposes.
    friend class column_family_test;

    friend class distributed_loader;
};

using sstable_reader_factory_type = std::function<flat_mutation_reader(sstables::shared_sstable&, const dht::partition_range& pr)>;

// Filters out mutation that doesn't belong to current shard.
flat_mutation_reader make_local_shard_sstable_reader(schema_ptr s,
        lw_shared_ptr<sstables::sstable_set> sstables,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        reader_resource_tracker resource_tracker,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        sstables::read_monitor_generator& monitor_generator = sstables::default_read_monitor_generator());

flat_mutation_reader make_range_sstable_reader(schema_ptr s,
        lw_shared_ptr<sstables::sstable_set> sstables,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        reader_resource_tracker resource_tracker,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        sstables::read_monitor_generator& monitor_generator = sstables::default_read_monitor_generator());

class user_types_metadata {
    std::unordered_map<bytes, user_type> _user_types;
public:
    user_type get_type(const bytes& name) const {
        return _user_types.at(name);
    }
    const std::unordered_map<bytes, user_type>& get_all_types() const {
        return _user_types;
    }
    void add_type(user_type type) {
        auto i = _user_types.find(type->_name);
        assert(i == _user_types.end() || type->is_compatible_with(*i->second));
        _user_types[type->_name] = std::move(type);
    }
    void remove_type(user_type type) {
        _user_types.erase(type->_name);
    }
    friend std::ostream& operator<<(std::ostream& os, const user_types_metadata& m);
};

class keyspace_metadata final {
    sstring _name;
    sstring _strategy_name;
    std::map<sstring, sstring> _strategy_options;
    std::unordered_map<sstring, schema_ptr> _cf_meta_data;
    bool _durable_writes;
    lw_shared_ptr<user_types_metadata> _user_types;
public:
    keyspace_metadata(sstring name,
                 sstring strategy_name,
                 std::map<sstring, sstring> strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{},
                 lw_shared_ptr<user_types_metadata> user_types = make_lw_shared<user_types_metadata>());
    static lw_shared_ptr<keyspace_metadata>
    new_keyspace(sstring name,
                 sstring strategy_name,
                 std::map<sstring, sstring> options,
                 bool durables_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{})
    {
        return ::make_lw_shared<keyspace_metadata>(name, strategy_name, options, durables_writes, cf_defs);
    }
    void validate() const;
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
    const lw_shared_ptr<user_types_metadata>& user_types() const {
        return _user_types;
    }
    void add_or_update_column_family(const schema_ptr& s) {
        _cf_meta_data[s->cf_name()] = s;
    }
    void remove_column_family(const schema_ptr& s) {
        _cf_meta_data.erase(s->cf_name());
    }
    void add_user_type(const user_type ut) {
        _user_types->add_type(ut);
    }
    void remove_user_type(const user_type ut) {
        _user_types->remove_type(ut);
    }
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
        bool compaction_enforce_min_threshold = false;
        ::dirty_memory_manager* dirty_memory_manager = &default_dirty_memory_manager;
        ::dirty_memory_manager* streaming_dirty_memory_manager = &default_dirty_memory_manager;
        reader_concurrency_semaphore* read_concurrency_semaphore;
        reader_concurrency_semaphore* streaming_read_concurrency_semaphore;
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
    config _config;
public:
    explicit keyspace(lw_shared_ptr<keyspace_metadata> metadata, config cfg)
        : _metadata(std::move(metadata))
        , _config(std::move(cfg))
    {}

    void update_from(lw_shared_ptr<keyspace_metadata>);

    /** Note: return by shared pointer value, since the meta data is
     * semi-volatile. I.e. we could do alter keyspace at any time, and
     * boom, it is replaced.
     */
    lw_shared_ptr<keyspace_metadata> metadata() const {
        return _metadata;
    }
    void create_replication_strategy(const std::map<sstring, sstring>& options);
    /**
     * This should not really be return by reference, since replication
     * strategy is also volatile in that it could be replaced at "any" time.
     * However, all current uses at least are "instantateous", i.e. does not
     * carry it across a continuation. So it is sort of same for now, but
     * should eventually be refactored.
     */
    locator::abstract_replication_strategy& get_replication_strategy();
    const locator::abstract_replication_strategy& get_replication_strategy() const;
    column_family::config make_column_family_config(const schema& s, const db::config& db_config, db::large_partition_handler* lp_handler) const;
    future<> make_directory_for_column_family(const sstring& name, utils::UUID uuid);
    void add_or_update_column_family(const schema_ptr& s) {
        _metadata->add_or_update_column_family(s);
    }
    void add_user_type(const user_type ut) {
        _metadata->add_user_type(ut);
    }
    void remove_user_type(const user_type ut) {
        _metadata->remove_user_type(ut);
    }

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
};

class no_such_keyspace : public std::runtime_error {
public:
    no_such_keyspace(const sstring& ks_name);
};

class no_such_column_family : public std::runtime_error {
public:
    no_such_column_family(const utils::UUID& uuid);
    no_such_column_family(const sstring& ks_name, const sstring& cf_name);
};


struct database_config {
    seastar::scheduling_group memtable_scheduling_group;
    seastar::scheduling_group memtable_to_cache_scheduling_group; // FIXME: merge with memtable_scheduling_group
    seastar::scheduling_group compaction_scheduling_group;
    seastar::scheduling_group memory_compaction_scheduling_group;
    seastar::scheduling_group statement_scheduling_group;
    seastar::scheduling_group streaming_scheduling_group;
    size_t available_memory;
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
private:
    ::cf_stats _cf_stats;
    static const size_t max_count_concurrent_reads{100};
    size_t max_memory_concurrent_reads() { return _dbcfg.available_memory * 0.02; }
    // Assume a queued read takes up 10kB of memory, and allow 2% of memory to be filled up with such reads.
    size_t max_inactive_queue_length() { return _dbcfg.available_memory * 0.02 / 10000; }
    // They're rather heavyweight, so limit more
    static const size_t max_count_streaming_concurrent_reads{10};
    size_t max_memory_streaming_concurrent_reads() { return _dbcfg.available_memory * 0.02; }
    static const size_t max_count_system_concurrent_reads{10};
    size_t max_memory_system_concurrent_reads() { return _dbcfg.available_memory * 0.02; };
    static constexpr size_t max_concurrent_sstable_loads() { return 3; }
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

    std::unique_ptr<db::config> _cfg;

    dirty_memory_manager _system_dirty_memory_manager;
    dirty_memory_manager _dirty_memory_manager;
    dirty_memory_manager _streaming_dirty_memory_manager;

    database_config _dbcfg;
    flush_controller _memtable_controller;

    reader_concurrency_semaphore _read_concurrency_sem;
    reader_concurrency_semaphore _streaming_concurrency_sem;
    reader_concurrency_semaphore _system_read_concurrency_sem;

    semaphore _sstable_load_concurrency_sem{max_concurrent_sstable_loads()};

    db::timeout_semaphore _view_update_concurrency_sem{max_memory_pending_view_updates()};

    cache_tracker _row_cache_tracker;

    inheriting_concrete_execution_stage<future<lw_shared_ptr<query::result>>,
        column_family*,
        schema_ptr,
        const query::read_command&,
        query::result_options,
        const dht::partition_range_vector&,
        tracing::trace_state_ptr,
        query::result_memory_limiter&,
        uint64_t,
        db::timeout_clock::time_point,
        query::querier_cache_context> _data_query_stage;

    mutation_query_stage _mutation_query_stage;

    inheriting_concrete_execution_stage<
            future<>,
            database*,
            schema_ptr,
            const frozen_mutation&,
            db::timeout_clock::time_point> _apply_stage;

    std::unordered_map<sstring, keyspace> _keyspaces;
    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>> _column_families;
    std::unordered_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash> _ks_cf_to_uuid;
    std::unique_ptr<db::commitlog> _commitlog;
    utils::UUID _version;
    // compaction_manager object is referenced by all column families of a database.
    std::unique_ptr<compaction_manager> _compaction_manager;
    seastar::metrics::metric_groups _metrics;
    bool _enable_incremental_backups = false;

    query::querier_cache _querier_cache;

    std::unique_ptr<db::large_partition_handler> _large_partition_handler;

    future<> init_commitlog();
    future<> apply_in_memory(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&&, db::timeout_clock::time_point timeout);
    future<> apply_in_memory(const mutation& m, column_family& cf, db::rp_handle&&, db::timeout_clock::time_point timeout);
private:
    // Unless you are an earlier boostraper or the database itself, you should
    // not be using this directly.  Go for the public create_keyspace instead.
    void add_keyspace(sstring name, keyspace k);
    void create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm);
    friend void db::system_keyspace::make(database& db, bool durable, bool volatile_testing_only);
    void setup_metrics();

    friend class db_apply_executor;
    future<> do_apply(schema_ptr, const frozen_mutation&, db::timeout_clock::time_point timeout);
    future<> apply_with_commitlog(schema_ptr, column_family&, utils::UUID, const frozen_mutation&, db::timeout_clock::time_point timeout);
    future<> apply_with_commitlog(column_family& cf, const mutation& m, db::timeout_clock::time_point timeout);

    query::result_memory_limiter _result_memory_limiter;

    future<mutation> do_apply_counter_update(column_family& cf, const frozen_mutation& fm, schema_ptr m_schema, db::timeout_clock::time_point timeout,
                                             tracing::trace_state_ptr trace_state);

    template<typename Future>
    Future update_write_metrics(Future&& f);
public:
    static utils::UUID empty_version;

    query::result_memory_limiter& get_result_memory_limiter() {
        return _result_memory_limiter;
    }

    void set_enable_incremental_backups(bool val) { _enable_incremental_backups = val; }

    future<> parse_system_tables(distributed<service::storage_proxy>&);
    database();
    database(const db::config&, database_config dbcfg);
    database(database&&) = delete;
    ~database();

    cache_tracker& row_cache_tracker() { return _row_cache_tracker; }

    void update_version(const utils::UUID& version);

    const utils::UUID& get_version() const;

    db::commitlog* commitlog() const {
        return _commitlog.get();
    }

    seastar::scheduling_group get_streaming_scheduling_group() const { return _dbcfg.streaming_scheduling_group; }
    size_t get_available_memory() const { return _dbcfg.available_memory; }

    compaction_manager& get_compaction_manager() {
        return *_compaction_manager;
    }
    const compaction_manager& get_compaction_manager() const {
        return *_compaction_manager;
    }

    void add_column_family(keyspace& ks, schema_ptr schema, column_family::config cfg);
    future<> add_column_family_and_make_directory(schema_ptr schema);

    /* throws std::out_of_range if missing */
    const utils::UUID& find_uuid(const sstring& ks, const sstring& cf) const;
    const utils::UUID& find_uuid(const schema_ptr&) const;

    /**
     * Creates a keyspace for a given metadata if it still doesn't exist.
     *
     * @return ready future when the operation is complete
     */
    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>&);
    /* below, find_keyspace throws no_such_<type> on fail */
    keyspace& find_keyspace(const sstring& name);
    const keyspace& find_keyspace(const sstring& name) const;
    bool has_keyspace(const sstring& name) const;
    future<> update_keyspace(const sstring& name);
    void drop_keyspace(const sstring& name);
    const auto& keyspaces() const { return _keyspaces; }
    std::vector<sstring> get_non_system_keyspaces() const;
    column_family& find_column_family(const sstring& ks, const sstring& name);
    const column_family& find_column_family(const sstring& ks, const sstring& name) const;
    column_family& find_column_family(const utils::UUID&);
    const column_family& find_column_family(const utils::UUID&) const;
    column_family& find_column_family(const schema_ptr&);
    const column_family& find_column_family(const schema_ptr&) const;
    bool column_family_exists(const utils::UUID& uuid) const;
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name) const;
    schema_ptr find_schema(const utils::UUID&) const;
    bool has_schema(const sstring& ks_name, const sstring& cf_name) const;
    std::set<sstring> existing_index_names(const sstring& ks_name, const sstring& cf_to_exclude = sstring()) const;
    sstring get_available_index_name(const sstring& ks_name, const sstring& cf_name,
                                     std::experimental::optional<sstring> index_name_root) const;
    schema_ptr find_indexed_table(const sstring& ks_name, const sstring& index_name) const;
    future<> stop();
    unsigned shard_of(const dht::token& t);
    unsigned shard_of(const mutation& m);
    unsigned shard_of(const frozen_mutation& m);
    future<lw_shared_ptr<query::result>, cache_temperature> query(schema_ptr, const query::read_command& cmd, query::result_options opts,
                                                                  const dht::partition_range_vector& ranges, tracing::trace_state_ptr trace_state,
                                                                  uint64_t max_result_size, db::timeout_clock::time_point timeout = db::no_timeout);
    future<reconcilable_result, cache_temperature> query_mutations(schema_ptr, const query::read_command& cmd, const dht::partition_range& range,
                                                query::result_memory_accounter&& accounter, tracing::trace_state_ptr trace_state,
                                                db::timeout_clock::time_point timeout = db::no_timeout);
    // Apply the mutation atomically.
    // Throws timed_out_error when timeout is reached.
    future<> apply(schema_ptr, const frozen_mutation&, db::timeout_clock::time_point timeout = db::no_timeout);
    future<> apply_streaming_mutation(schema_ptr, utils::UUID plan_id, const frozen_mutation&, bool fragmented);
    future<mutation> apply_counter_update(schema_ptr, const frozen_mutation& m, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_state);
    keyspace::config make_keyspace_config(const keyspace_metadata& ksm);
    const sstring& get_snitch_name() const;
    future<> clear_snapshot(sstring tag, std::vector<sstring> keyspace_names);

    friend std::ostream& operator<<(std::ostream& out, const database& db);
    const std::unordered_map<sstring, keyspace>& get_keyspaces() const {
        return _keyspaces;
    }

    std::unordered_map<sstring, keyspace>& get_keyspaces() {
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

    const std::unordered_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash>&
    get_column_families_mapping() const {
        return _ks_cf_to_uuid;
    }

    const db::config& get_config() const {
        return *_cfg;
    }

    db::large_partition_handler* get_large_partition_handler() {
        return _large_partition_handler.get();
    }

    future<> flush_all_memtables();

    // See #937. Truncation now requires a callback to get a time stamp
    // that must be guaranteed to be the same for all shards.
    typedef std::function<future<db_clock::time_point>()> timestamp_func;

    /** Truncates the given column family */
    future<> truncate(sstring ksname, sstring cfname, timestamp_func);
    future<> truncate(const keyspace& ks, column_family& cf, timestamp_func, bool with_snapshot = true);
    future<> truncate_views(const column_family& base, db_clock::time_point truncated_at, bool should_flush);

    bool update_column_family(schema_ptr s);
    future<> drop_column_family(const sstring& ks_name, const sstring& cf_name, timestamp_func, bool with_snapshot = true);
    void remove(const column_family&);

    const logalloc::region_group& dirty_memory_region_group() const {
        return _dirty_memory_manager.region_group();
    }

    std::unordered_set<sstring> get_initial_tokens();
    std::experimental::optional<gms::inet_address> get_replace_address();
    bool is_replacing();
    reader_concurrency_semaphore& user_read_concurrency_sem() {
        return _read_concurrency_sem;
    }
    reader_concurrency_semaphore& streaming_read_concurrency_sem() {
        return _streaming_concurrency_sem;
    }
    reader_concurrency_semaphore& system_keyspace_read_concurrency_sem() {
        return _system_read_concurrency_sem;
    }
    semaphore& sstable_load_concurrency_sem() {
        return _sstable_load_concurrency_sem;
    }
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
};

future<> update_schema_version_and_announce(distributed<service::storage_proxy>& proxy);

bool is_internal_keyspace(const sstring& name);

class distributed_loader {
public:
    static void reshard(distributed<database>& db, sstring ks_name, sstring cf_name);
    static future<> open_sstable(distributed<database>& db, sstables::entry_descriptor comps,
        std::function<future<> (column_family&, sstables::foreign_sstable_open_info)> func,
        const io_priority_class& pc = default_priority_class());
    static future<> load_new_sstables(distributed<database>& db, sstring ks, sstring cf, std::vector<sstables::entry_descriptor> new_tables);
    static future<std::vector<sstables::entry_descriptor>> flush_upload_dir(distributed<database>& db, sstring ks_name, sstring cf_name);
    static future<sstables::entry_descriptor> probe_file(distributed<database>& db, sstring sstdir, sstring fname);
    static future<> populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf);
    static future<> populate_keyspace(distributed<database>& db, sstring datadir, sstring ks_name);
    static future<> init_system_keyspace(distributed<database>& db);
    static future<> ensure_system_table_directories(distributed<database>& db);
    static future<> init_non_system_keyspaces(distributed<database>& db, distributed<service::storage_proxy>& proxy);
};

#endif /* DATABASE_HH_ */
