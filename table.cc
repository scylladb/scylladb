/*
 * Copyright (C) 2018-present ScyllaDB
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

#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/closeable.hh>

#include "database.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "service/priority_manager.hh"
#include "db/schema_tables.hh"
#include "cell_locking.hh"
#include "utils/logalloc.hh"
#include "checked-file-impl.hh"
#include "view_info.hh"
#include "db/data_listeners.hh"
#include "memtable-sstable.hh"
#include "compaction/compaction_manager.hh"
#include "sstables/sstable_directory.hh"
#include "db/system_keyspace.hh"
#include "db/query_context.hh"
#include "query-result-writer.hh"
#include "db/view/view.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include "utils/error_injection.hh"
#include "utils/histogram_metrics_helper.hh"
#include "utils/fb_utilities.hh"
#include "mutation_source_metadata.hh"
#include "gms/gossiper.hh"
#include "db/config.hh"

#include <boost/range/algorithm/remove_if.hpp>

static logging::logger tlogger("table");
static seastar::metrics::label column_family_label("cf");
static seastar::metrics::label keyspace_label("ks");


using namespace std::chrono_literals;

flat_mutation_reader
table::make_sstable_reader(schema_ptr s,
                                   reader_permit permit,
                                   lw_shared_ptr<sstables::sstable_set> sstables,
                                   const dht::partition_range& pr,
                                   const query::partition_slice& slice,
                                   const io_priority_class& pc,
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr) const {
    // CAVEAT: if make_sstable_reader() is called on a single partition
    // we want to optimize and read exactly this partition. As a
    // consequence, fast_forward_to() will *NOT* work on the result,
    // regardless of what the fwd_mr parameter says.
    if (pr.is_singular() && pr.start()->value().has_key()) {
        const dht::ring_position& pos = pr.start()->value();
        if (dht::shard_of(*s, pos.token()) != this_shard_id()) {
            return make_empty_flat_reader(s, std::move(permit)); // range doesn't belong to this shard
        }

        return sstables->create_single_key_sstable_reader(const_cast<column_family*>(this), std::move(s), std::move(permit),
                _stats.estimated_sstable_per_read, pr, slice, pc, std::move(trace_state), fwd, fwd_mr);
    } else {
        return sstables->make_local_shard_sstable_reader(std::move(s), std::move(permit), pr, slice, pc,
                std::move(trace_state), fwd, fwd_mr);
    }
}

lw_shared_ptr<sstables::sstable_set> table::make_compound_sstable_set() {
    return make_lw_shared(sstables::make_compound_sstable_set(_schema, { _main_sstables, _maintenance_sstables }));
}

lw_shared_ptr<sstables::sstable_set> table::make_maintenance_sstable_set() const {
    // Level metadata is not used because (level 0) maintenance sstables are disjoint and must be stored for efficient retrieval in the partitioned set
    bool use_level_metadata = false;
    return make_lw_shared<sstables::sstable_set>(
            sstables::make_partitioned_sstable_set(_schema, make_lw_shared<sstable_list>(sstable_list{}), use_level_metadata));
}

void table::refresh_compound_sstable_set() {
    _sstables = make_compound_sstable_set();
}

// Exposed for testing, not performance critical.
future<table::const_mutation_partition_ptr>
table::find_partition(schema_ptr s, reader_permit permit, const dht::decorated_key& key) const {
    return do_with(dht::partition_range::make_singular(key), [s = std::move(s), permit = std::move(permit), this] (auto& range) mutable {
        return with_closeable(this->make_reader(std::move(s), std::move(permit), range), [] (flat_mutation_reader& reader) {
            return read_mutation_from_flat_mutation_reader(reader).then([] (mutation_opt&& mo) -> std::unique_ptr<const mutation_partition> {
                if (!mo) {
                    return {};
                }
                return std::make_unique<const mutation_partition>(std::move(mo->partition()));
            });
        });
    });
}

future<table::const_mutation_partition_ptr>
table::find_partition_slow(schema_ptr s, reader_permit permit, const partition_key& key) const {
    return find_partition(s, std::move(permit), dht::decorate_key(*s, key));
}

future<table::const_row_ptr>
table::find_row(schema_ptr s, reader_permit permit, const dht::decorated_key& partition_key, clustering_key clustering_key) const {
    return find_partition(s, std::move(permit), partition_key).then([clustering_key = std::move(clustering_key), s] (const_mutation_partition_ptr p) {
        if (!p) {
            return make_ready_future<const_row_ptr>();
        }
        auto r = p->find_row(*s, clustering_key);
        if (r) {
            // FIXME: remove copy if only one data source
            return make_ready_future<const_row_ptr>(std::make_unique<row>(*s, column_kind::regular_column, *r));
        } else {
            return make_ready_future<const_row_ptr>();
        }
    });
}

flat_mutation_reader
table::make_reader(schema_ptr s,
                           reader_permit permit,
                           const dht::partition_range& range,
                           const query::partition_slice& slice,
                           const io_priority_class& pc,
                           tracing::trace_state_ptr trace_state,
                           streamed_mutation::forwarding fwd,
                           mutation_reader::forwarding fwd_mr) const {
    if (_virtual_reader) {
        return (*_virtual_reader).make_reader(s, std::move(permit), range, slice, pc, trace_state, fwd, fwd_mr);
    }

    std::vector<flat_mutation_reader> readers;
    readers.reserve(_memtables->size() + 1);

    // We're assuming that cache and memtables are both read atomically
    // for single-key queries, so we don't need to special case memtable
    // undergoing a move to cache. At any given point in time between
    // deferring points the sum of data in memtable and cache is coherent. If
    // single-key queries for each data source were performed across deferring
    // points, it would be possible that partitions which are ahead of the
    // memtable cursor would be placed behind the cache cursor, resulting in
    // those partitions being missing in the combined reader.
    //
    // We need to handle this in range queries though, as they are always
    // deferring. scanning_reader from memtable.cc is falling back to reading
    // the sstable when memtable is flushed. After memtable is moved to cache,
    // new readers will no longer use the old memtable, but until then
    // performance may suffer. We should fix this when we add support for
    // range queries in cache, so that scans can always be satisfied form
    // memtable and cache only, as long as data is not evicted.
    //
    // https://github.com/scylladb/scylla/issues/309
    // https://github.com/scylladb/scylla/issues/185

    for (auto&& mt : *_memtables) {
        readers.emplace_back(mt->make_flat_reader(s, permit, range, slice, pc, trace_state, fwd, fwd_mr));
    }

    if (cache_enabled() && !slice.options.contains(query::partition_slice::option::bypass_cache)) {
        readers.emplace_back(_cache.make_reader(s, permit, range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    } else {
        readers.emplace_back(make_sstable_reader(s, permit, _sstables, range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    }

    auto comb_reader = make_combined_reader(s, std::move(permit), std::move(readers), fwd, fwd_mr);
    if (_config.data_listeners && !_config.data_listeners->empty()) {
        return _config.data_listeners->on_read(s, range, slice, std::move(comb_reader));
    } else {
        return comb_reader;
    }
}

sstables::shared_sstable table::make_streaming_sstable_for_write(std::optional<sstring> subdir) {
    sstring dir = _config.datadir;
    if (subdir) {
        dir += "/" + *subdir;
    }
    auto newtab = make_sstable(dir);
    tlogger.debug("Created sstable for streaming: ks={}, cf={}, dir={}", schema()->ks_name(), schema()->cf_name(), dir);
    return newtab;
}

flat_mutation_reader
table::make_streaming_reader(schema_ptr s, reader_permit permit,
                           const dht::partition_range_vector& ranges) const {
    auto& slice = s->full_slice();
    auto& pc = service::get_local_streaming_priority();

    auto source = mutation_source([this] (schema_ptr s, reader_permit permit, const dht::partition_range& range, const query::partition_slice& slice,
                                      const io_priority_class& pc, tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd, mutation_reader::forwarding fwd_mr) {
        std::vector<flat_mutation_reader> readers;
        readers.reserve(_memtables->size() + 1);
        for (auto&& mt : *_memtables) {
            readers.emplace_back(mt->make_flat_reader(s, permit, range, slice, pc, trace_state, fwd, fwd_mr));
        }
        readers.emplace_back(make_sstable_reader(s, permit, _sstables, range, slice, pc, std::move(trace_state), fwd, fwd_mr));
        return make_combined_reader(s, std::move(permit), std::move(readers), fwd, fwd_mr);
    });

    return make_flat_multi_range_reader(s, std::move(permit), std::move(source), ranges, slice, pc, nullptr, mutation_reader::forwarding::no);
}

flat_mutation_reader table::make_streaming_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
        const query::partition_slice& slice, mutation_reader::forwarding fwd_mr) const {
    const auto& pc = service::get_local_streaming_priority();
    auto trace_state = tracing::trace_state_ptr();
    const auto fwd = streamed_mutation::forwarding::no;

    std::vector<flat_mutation_reader> readers;
    readers.reserve(_memtables->size() + 1);
    for (auto&& mt : *_memtables) {
        readers.emplace_back(mt->make_flat_reader(schema, permit, range, slice, pc, trace_state, fwd, fwd_mr));
    }
    readers.emplace_back(make_sstable_reader(schema, permit, _sstables, range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    return make_combined_reader(std::move(schema), std::move(permit), std::move(readers), fwd, fwd_mr);
}

flat_mutation_reader table::make_streaming_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
        lw_shared_ptr<sstables::sstable_set> sstables) const {
    auto& slice = schema->full_slice();
    const auto& pc = service::get_local_streaming_priority();
    auto trace_state = tracing::trace_state_ptr();
    const auto fwd = streamed_mutation::forwarding::no;
    const auto fwd_mr = mutation_reader::forwarding::no;
    return sstables->make_range_sstable_reader(std::move(schema), std::move(permit), range, slice, pc,
            std::move(trace_state), fwd, fwd_mr);
}

future<std::vector<locked_cell>> table::lock_counter_cells(const mutation& m, db::timeout_clock::time_point timeout) {
    assert(m.schema() == _counter_cell_locks->schema());
    return _counter_cell_locks->lock_cells(m.decorated_key(), partition_cells_range(m.partition()), timeout);
}

// Not performance critical. Currently used for testing only.
future<bool>
table::for_all_partitions_slow(schema_ptr s, reader_permit permit, std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const {
    struct iteration_state {
        flat_mutation_reader reader;
        std::function<bool (const dht::decorated_key&, const mutation_partition&)> func;
        bool ok = true;
        bool empty = false;
    public:
        bool done() const { return !ok || empty; }
        iteration_state(schema_ptr s, reader_permit permit, const column_family& cf,
                std::function<bool (const dht::decorated_key&, const mutation_partition&)>&& func)
            : reader(cf.make_reader(std::move(s), std::move(permit)))
            , func(std::move(func))
        { }
    };

    return do_with(iteration_state(std::move(s), std::move(permit), *this, std::move(func)), [] (iteration_state& is) {
        return do_until([&is] { return is.done(); }, [&is] {
            return read_mutation_from_flat_mutation_reader(is.reader).then([&is](mutation_opt&& mo) {
                if (!mo) {
                    is.empty = true;
                } else {
                    is.ok = is.func(mo->decorated_key(), mo->partition());
                }
            });
        }).then([&is] {
            return is.ok;
        }).finally([&is] {
            return is.reader.close();
        });
    });
}

static bool belongs_to_current_shard(const std::vector<shard_id>& shards) {
    return boost::find(shards, this_shard_id()) != shards.end();
}

static bool belongs_to_other_shard(const std::vector<shard_id>& shards) {
    return shards.size() != size_t(belongs_to_current_shard(shards));
}

sstables::shared_sstable table::make_sstable(sstring dir, int64_t generation, sstables::sstable_version_types v, sstables::sstable_format_types f,
        io_error_handler_gen error_handler_gen) {
    return get_sstables_manager().make_sstable(_schema, dir, generation, v, f, gc_clock::now(), error_handler_gen);
}

sstables::shared_sstable table::make_sstable(sstring dir, int64_t generation,
        sstables::sstable_version_types v, sstables::sstable_format_types f) {
    return get_sstables_manager().make_sstable(_schema, dir, generation, v, f);
}

sstables::shared_sstable table::make_sstable(sstring dir) {
    return make_sstable(dir, calculate_generation_for_new_table(),
                        get_sstables_manager().get_highest_supported_format(), sstables::sstable::format_types::big);
}

sstables::shared_sstable table::make_sstable() {
    return make_sstable(_config.datadir);
}

void table::notify_bootstrap_or_replace_start() {
    _is_bootstrap_or_replace = true;
}

void table::notify_bootstrap_or_replace_end() {
    _is_bootstrap_or_replace = false;
    trigger_offstrategy_compaction();
}

void table::update_stats_for_new_sstable(uint64_t disk_space_used_by_sstable) noexcept {
    _stats.live_disk_space_used += disk_space_used_by_sstable;
    _stats.total_disk_space_used += disk_space_used_by_sstable;
    _stats.live_sstable_count++;
}

inline void table::add_sstable_to_backlog_tracker(compaction_backlog_tracker& tracker, sstables::shared_sstable sstable) {
    tracker.add_sstable(std::move(sstable));
}

inline void table::remove_sstable_from_backlog_tracker(compaction_backlog_tracker& tracker, sstables::shared_sstable sstable) {
    tracker.remove_sstable(std::move(sstable));
}

lw_shared_ptr<sstables::sstable_set>
table::do_add_sstable(lw_shared_ptr<sstables::sstable_set> sstables, sstables::shared_sstable sstable,
        enable_backlog_tracker backlog_tracker) {
    if (belongs_to_other_shard(sstable->get_shards_for_this_sstable())) {
        on_internal_error(tlogger, format("Attempted to load the shared SSTable {} at table", sstable->get_filename()));
    }
    // allow in-progress reads to continue using old list
    auto new_sstables = make_lw_shared<sstables::sstable_set>(*sstables);
    new_sstables->insert(sstable);
    if (sstable->requires_view_building()) {
        _sstables_staging.emplace(sstable->generation(), sstable);
    } else if (backlog_tracker) {
        add_sstable_to_backlog_tracker(_compaction_strategy.get_backlog_tracker(), sstable);
    }
    // update sstable set last in case either updating
    // staging sstables or backlog tracker throws
    update_stats_for_new_sstable(sstable->bytes_on_disk());
    return new_sstables;
}

void table::add_sstable(sstables::shared_sstable sstable) {
    _main_sstables = do_add_sstable(_main_sstables, std::move(sstable), enable_backlog_tracker::yes);
    refresh_compound_sstable_set();
}

void table::add_maintenance_sstable(sstables::shared_sstable sst) {
    _maintenance_sstables = do_add_sstable(_maintenance_sstables, std::move(sst), enable_backlog_tracker::no);
    refresh_compound_sstable_set();
}

void table::do_update_off_strategy_trigger() {
    _off_strategy_trigger.rearm(timer<>::clock::now() +  std::chrono::minutes(5));
}

// If there are more sstables to be added to the off-strategy sstable set, call
// update_off_strategy_trigger to update the timer and delay to trigger
// off-strategy compaction. The off-strategy compaction will be triggered when
// the timer is expired.
void table::update_off_strategy_trigger() {
    if (_off_strategy_trigger.armed()) {
        do_update_off_strategy_trigger();
    }
}

// Call enable_off_strategy_trigger to enable the automatic off-strategy
// compaction trigger.
void table::enable_off_strategy_trigger() {
    do_update_off_strategy_trigger();
}

future<>
table::add_sstable_and_update_cache(sstables::shared_sstable sst, sstables::offstrategy offstrategy) {
    auto permit = co_await seastar::get_units(_sstable_set_mutation_sem, 1);
    co_return co_await get_row_cache().invalidate(row_cache::external_updater([this, sst, offstrategy] () noexcept {
        // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
        // atomically load all opened sstables into column family.
        if (!offstrategy) {
            add_sstable(sst);
            trigger_compaction();
        } else {
            add_maintenance_sstable(sst);
        }
    }), dht::partition_range::make({sst->get_first_decorated_key(), true}, {sst->get_last_decorated_key(), true}));
}

future<>
table::update_cache(lw_shared_ptr<memtable> m, std::vector<sstables::shared_sstable> ssts) {
    auto permit = co_await seastar::get_units(_sstable_set_mutation_sem, 1);
    mutation_source_opt ms_opt;
    if (ssts.size() == 1) {
        ms_opt = ssts.front()->as_mutation_source();
    } else {
        std::vector<mutation_source> sources;
        sources.reserve(ssts.size());
        for (auto& sst : ssts) {
            sources.push_back(sst->as_mutation_source());
        }
        ms_opt = make_combined_mutation_source(std::move(sources));
    }
    auto adder = row_cache::external_updater([this, m, ssts = std::move(ssts), new_ssts_ms = std::move(*ms_opt)] () mutable {
        for (auto& sst : ssts) {
            add_sstable(sst);
        }
        m->mark_flushed(std::move(new_ssts_ms));
        try_trigger_compaction();
    });
    if (cache_enabled()) {
        co_return co_await _cache.update(std::move(adder), *m);
    } else {
        co_return co_await _cache.invalidate(std::move(adder)).then([m] { return m->clear_gently(); });
    }
}

// Handles permit management only, used for situations where we don't want to inform
// the compaction manager about backlogs (i.e., tests)
class permit_monitor : public sstables::write_monitor {
    lw_shared_ptr<sstable_write_permit> _permit;
public:
    permit_monitor(lw_shared_ptr<sstable_write_permit> permit)
            : _permit(std::move(permit)) {
    }

    virtual void on_write_started(const sstables::writer_offset_tracker& t) override { }
    virtual void on_data_write_completed() override {
        // We need to start a flush before the current one finishes, otherwise
        // we'll have a period without significant disk activity when the current
        // SSTable is being sealed, the caches are being updated, etc. To do that,
        // we ensure the permit doesn't outlive this continuation.
        *_permit = sstable_write_permit::unconditional();
    }
};

// Handles all tasks related to sstable writing: permit management, compaction backlog updates, etc
class database_sstable_write_monitor : public permit_monitor, public backlog_write_progress_manager {
    sstables::shared_sstable _sst;
    sstables::compaction_strategy& _compaction_strategy;
    const sstables::writer_offset_tracker* _tracker = nullptr;
    uint64_t _progress_seen = 0;
    api::timestamp_type _maximum_timestamp;
public:
    database_sstable_write_monitor(lw_shared_ptr<sstable_write_permit> permit, sstables::shared_sstable sst,
        sstables::compaction_strategy& strategy, api::timestamp_type max_timestamp)
            : permit_monitor(std::move(permit))
            , _sst(std::move(sst))
            , _compaction_strategy(strategy)
            , _maximum_timestamp(max_timestamp)
    {}

    database_sstable_write_monitor(const database_sstable_write_monitor&) = delete;
    database_sstable_write_monitor(database_sstable_write_monitor&& x) = default;

    ~database_sstable_write_monitor() {
        // We failed to finish handling this SSTable, so we have to update the backlog_tracker
        // about it.
        if (_sst) {
            _compaction_strategy.get_backlog_tracker().revert_charges(_sst);
        }
    }

    virtual void on_write_started(const sstables::writer_offset_tracker& t) override {
        _tracker = &t;
        _compaction_strategy.get_backlog_tracker().register_partially_written_sstable(_sst, *this);
    }

    virtual void on_data_write_completed() override {
        permit_monitor::on_data_write_completed();
        _progress_seen = _tracker->offset;
        _tracker = nullptr;
    }

    virtual uint64_t written() const override {
        if (_tracker) {
            return _tracker->offset;
        }
        return _progress_seen;
    }

    api::timestamp_type maximum_timestamp() const override {
        return _maximum_timestamp;
    }

    unsigned level() const override {
        return 0;
    }
};

future<>
table::seal_active_memtable(flush_permit&& permit) {
    auto old = _memtables->back();
    tlogger.debug("Sealing active memtable of {}.{}, partitions: {}, occupancy: {}", _schema->ks_name(), _schema->cf_name(), old->partition_count(), old->occupancy());

    if (old->empty()) {
        tlogger.debug("Memtable is empty");
        return _flush_barrier.advance_and_await();
    }
    _memtables->add_memtable();
    _stats.memtable_switch_count++;
    // This will set evictable occupancy of the old memtable region to zero, so that
    // this region is considered last for flushing by dirty_memory_manager::flush_when_needed().
    // If we don't do that, the flusher may keep picking up this memtable list for flushing after
    // the permit is released even though there is not much to flush in the active memtable of this list.
    old->region().ground_evictable_occupancy();
    auto previous_flush = _flush_barrier.advance_and_await();
    auto op = _flush_barrier.start();

    auto memtable_size = old->occupancy().total_space();

    _stats.pending_flushes++;
    _config.cf_stats->pending_memtables_flushes_count++;
    _config.cf_stats->pending_memtables_flushes_bytes += memtable_size;

    return do_with(std::move(permit), [this, old] (auto& permit) {
        return repeat([this, old, &permit] () mutable {
            auto sstable_write_permit = permit.release_sstable_write_permit();
            return this->try_flush_memtable_to_sstable(old, std::move(sstable_write_permit)).then([this, &permit] (auto should_stop) mutable {
                if (should_stop) {
                    return make_ready_future<stop_iteration>(should_stop);
                }
                return sleep(10s).then([this, &permit] () mutable {
                    return std::move(permit).reacquire_sstable_write_permit().then([this, &permit] (auto new_permit) mutable {
                        permit = std::move(new_permit);
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                });
            });
        });
    }).then_wrapped([this, memtable_size, old, op = std::move(op), previous_flush = std::move(previous_flush)] (future<> f) mutable {
        _stats.pending_flushes--;
        _config.cf_stats->pending_memtables_flushes_count--;
        _config.cf_stats->pending_memtables_flushes_bytes -= memtable_size;

        if (f.failed()) {
            return f;
        }

        if (_commitlog) {
            _commitlog->discard_completed_segments(_schema->id(), old->get_and_discard_rp_set());
        }
        return previous_flush.finally([op = std::move(op)] { });
    });
    // FIXME: release commit log
    // FIXME: provide back-pressure to upper layers
}

future<stop_iteration>
table::try_flush_memtable_to_sstable(lw_shared_ptr<memtable> old, sstable_write_permit&& permit) {
  return with_scheduling_group(_config.memtable_scheduling_group, [this, old = std::move(old), permit = std::move(permit)] () mutable {
    // Note that due to our sharded architecture, it is possible that
    // in the face of a value change some shards will backup sstables
    // while others won't.
    //
    // This is, in theory, possible to mitigate through a rwlock.
    // However, this doesn't differ from the situation where all tables
    // are coming from a single shard and the toggle happens in the
    // middle of them.
    //
    // The code as is guarantees that we'll never partially backup a
    // single sstable, so that is enough of a guarantee.

    return do_with(std::vector<sstables::shared_sstable>(), [this, old, permit = make_lw_shared(std::move(permit))] (auto& newtabs) {
        auto metadata = mutation_source_metadata{};
        metadata.min_timestamp = old->get_min_timestamp();
        metadata.max_timestamp = old->get_max_timestamp();

        auto consumer = _compaction_strategy.make_interposer_consumer(metadata, [this, old, permit, &newtabs] (flat_mutation_reader reader) mutable {
            auto&& priority = service::get_local_memtable_flush_priority();
            sstables::sstable_writer_config cfg = get_sstables_manager().configure_writer("memtable");
            cfg.backup = incremental_backups_enabled();

            auto newtab = make_sstable();
            newtabs.push_back(newtab);
            tlogger.debug("Flushing to {}", newtab->get_filename());

            auto monitor = database_sstable_write_monitor(permit, newtab, _compaction_strategy,
                old->get_max_timestamp());

            return do_with(std::move(monitor), [newtab, cfg = std::move(cfg), old, reader = std::move(reader), &priority] (auto& monitor) mutable {
                // FIXME: certain writers may receive only a small subset of the partitions, so bloom filters will be
                // bigger than needed, due to overestimation. That's eventually adjusted through compaction, though.
                return write_memtable_to_sstable(std::move(reader), *old, newtab, monitor, cfg, priority);
            });
        });

        auto f = consumer(old->make_flush_reader(
                    old->schema(),
                    compaction_concurrency_semaphore().make_tracking_only_permit(old->schema().get(), "try_flush_memtable_to_sstable()", db::no_timeout),
                    service::get_local_memtable_flush_priority()));

        // Switch back to default scheduling group for post-flush actions, to avoid them being staved by the memtable flush
        // controller. Cache update does not affect the input of the memtable cpu controller, so it can be subject to
        // priority inversion.
        return with_scheduling_group(default_scheduling_group(), [this, old = std::move(old), &newtabs, f = std::move(f)] () mutable {
            return f.then([this, &newtabs, old] {
                return parallel_for_each(newtabs, [] (auto& newtab) {
                    return newtab->open_data().then([&newtab] {
                        tlogger.debug("Flushing to {} done", newtab->get_filename());
                    });
                }).then([this, old, &newtabs] () {
                    return with_scheduling_group(_config.memtable_to_cache_scheduling_group, [this, old, &newtabs] {
                        return update_cache(old, newtabs);
                    });
                }).then([this, old, &newtabs] () noexcept {
                    _memtables->erase(old);
                    tlogger.debug("Memtable for {}.{} replaced, into {} sstables", old->schema()->ks_name(), old->schema()->cf_name(), newtabs.size());
                    return stop_iteration::yes;
                });
            }).handle_exception([this, old, &newtabs] (auto e) {
                for (auto& newtab : newtabs) {
                    newtab->mark_for_deletion();
                    tlogger.error("failed to write sstable {}: {}", newtab->get_filename(), e);
                }
                _config.cf_stats->failed_memtables_flushes_count++;
                // If we failed this write we will try the write again and that will create a new flush reader
                // that will decrease dirty memory again. So we need to reset the accounting.
                old->revert_flushed_memory();
                return stop_iteration(_async_gate.is_closed());
            });
        });
    });
  });
}

void
table::start() {
    // FIXME: add option to disable automatic compaction.
    start_compaction();
}

future<>
table::stop() {
    if (_async_gate.is_closed()) {
        return make_ready_future<>();
    }
    return _async_gate.close().then([this] {
        return await_pending_ops().finally([this] {
            return _memtables->flush().finally([this] {
                return _compaction_manager.remove(this).then([this] {
                    return _sstable_deletion_gate.close().then([this] {
                        return get_row_cache().invalidate(row_cache::external_updater([this] {
                            _main_sstables = _compaction_strategy.make_sstable_set(_schema);
                            _maintenance_sstables = make_maintenance_sstable_set();
                            _sstables = make_compound_sstable_set();
                            _sstables_staging.clear();
                        })).then([this] {
                            _cache.refresh_snapshot();
                        });
                    });
                });
            });
        });
    });
}

void table::set_metrics() {
    auto cf = column_family_label(_schema->cf_name());
    auto ks = keyspace_label(_schema->ks_name());
    namespace ms = seastar::metrics;
    if (_config.enable_metrics_reporting) {
        _metrics.add_group("column_family", {
                ms::make_derive("memtable_switch", ms::description("Number of times flush has resulted in the memtable being switched out"), _stats.memtable_switch_count)(cf)(ks),
                ms::make_counter("memtable_partition_writes", [this] () { return _stats.memtable_partition_insertions + _stats.memtable_partition_hits; }, ms::description("Number of write operations performed on partitions in memtables"))(cf)(ks),
                ms::make_counter("memtable_partition_hits", _stats.memtable_partition_hits, ms::description("Number of times a write operation was issued on an existing partition in memtables"))(cf)(ks),
                ms::make_counter("memtable_row_writes", _stats.memtable_app_stats.row_writes, ms::description("Number of row writes performed in memtables"))(cf)(ks),
                ms::make_counter("memtable_row_hits", _stats.memtable_app_stats.row_hits, ms::description("Number of rows overwritten by write operations in memtables"))(cf)(ks),
                ms::make_counter("memtable_range_tombstone_reads", _stats.memtable_range_tombstone_reads, ms::description("Number of range tombstones read from memtables"))(cf)(ks),
                ms::make_counter("memtable_row_tombstone_reads", _stats.memtable_row_tombstone_reads, ms::description("Number of row tombstones read from memtables"))(cf)(ks),
                ms::make_gauge("pending_tasks", ms::description("Estimated number of tasks pending for this column family"), _stats.pending_flushes)(cf)(ks),
                ms::make_gauge("live_disk_space", ms::description("Live disk space used"), _stats.live_disk_space_used)(cf)(ks),
                ms::make_gauge("total_disk_space", ms::description("Total disk space used"), _stats.total_disk_space_used)(cf)(ks),
                ms::make_gauge("live_sstable", ms::description("Live sstable count"), _stats.live_sstable_count)(cf)(ks),
                ms::make_gauge("pending_compaction", ms::description("Estimated number of compactions pending for this column family"), _stats.pending_compactions)(cf)(ks),
                ms::make_gauge("pending_sstable_deletions",
                        ms::description("Number of tasks waiting to delete sstables from a table"),
                        [this] { return _sstable_deletion_sem.waiters(); })(cf)(ks)
        });

        // Metrics related to row locking
        auto add_row_lock_metrics = [this, ks, cf] (row_locker::single_lock_stats& stats, sstring stat_name) {
            _metrics.add_group("column_family", {
                ms::make_total_operations(format("row_lock_{}_acquisitions", stat_name), stats.lock_acquisitions, ms::description(format("Row lock acquisitions for {} lock", stat_name)))(cf)(ks),
                ms::make_queue_length(format("row_lock_{}_operations_currently_waiting_for_lock", stat_name), stats.operations_currently_waiting_for_lock, ms::description(format("Operations currently waiting for {} lock", stat_name)))(cf)(ks),
                ms::make_histogram(format("row_lock_{}_waiting_time", stat_name), ms::description(format("Histogram representing time that operations spent on waiting for {} lock", stat_name)),
                        [&stats] {return to_metrics_histogram(stats.estimated_waiting_for_lock);})(cf)(ks)
            });
        };
        add_row_lock_metrics(_row_locker_stats.exclusive_row, "exclusive_row");
        add_row_lock_metrics(_row_locker_stats.shared_row, "shared_row");
        add_row_lock_metrics(_row_locker_stats.exclusive_partition, "exclusive_partition");
        add_row_lock_metrics(_row_locker_stats.shared_partition, "shared_partition");

        // View metrics are created only for base tables, so there's no point in adding them to views (which cannot act as base tables for other views)
        if (!_schema->is_view()) {
            _view_stats.register_stats();
        }

        if (_schema->ks_name() != db::system_keyspace::NAME && _schema->ks_name() != db::schema_tables::v3::NAME && _schema->ks_name() != "system_traces") {
            _metrics.add_group("column_family", {
                    ms::make_histogram("read_latency", ms::description("Read latency histogram"), [this] {return to_metrics_histogram(_stats.estimated_read);})(cf)(ks),
                    ms::make_histogram("write_latency", ms::description("Write latency histogram"), [this] {return to_metrics_histogram(_stats.estimated_write);})(cf)(ks),
                    ms::make_histogram("cas_prepare_latency", ms::description("CAS prepare round latency histogram"), [this] {return to_metrics_histogram(_stats.estimated_cas_prepare);})(cf)(ks),
                    ms::make_histogram("cas_propose_latency", ms::description("CAS accept round latency histogram"), [this] {return to_metrics_histogram(_stats.estimated_cas_accept);})(cf)(ks),
                    ms::make_histogram("cas_commit_latency", ms::description("CAS learn round latency histogram"), [this] {return to_metrics_histogram(_stats.estimated_cas_learn);})(cf)(ks),
                    ms::make_gauge("cache_hit_rate", ms::description("Cache hit rate"), [this] {return float(_global_cache_hit_rate);})(cf)(ks)
            });
        }
    }
}

void table::rebuild_statistics() {
    // zeroing live_disk_space_used and live_sstable_count because the
    // sstable list was re-created
    _stats.live_disk_space_used = 0;
    _stats.live_sstable_count = 0;

    _sstables->for_each_sstable([this] (const sstables::shared_sstable& tab) {
        update_stats_for_new_sstable(tab->bytes_on_disk());
    });
    for (auto& tab : _sstables_compacted_but_not_deleted) {
        update_stats_for_new_sstable(tab->bytes_on_disk());
    }
}

future<lw_shared_ptr<sstables::sstable_set>>
table::sstable_list_builder::build_new_list(const sstables::sstable_set& current_sstables,
                              sstables::sstable_set new_sstable_list,
                              const std::vector<sstables::shared_sstable>& new_sstables,
                              const std::vector<sstables::shared_sstable>& old_sstables) {
    std::unordered_set<sstables::shared_sstable> s(old_sstables.begin(), old_sstables.end());

    // this might seem dangerous, but "move" here just avoids constness,
    // making the two ranges compatible when compiling with boost 1.55.
    // Noone is actually moving anything...
    for (auto all = current_sstables.all(); auto&& tab : boost::range::join(new_sstables, std::move(*all))) {
        if (!s.contains(tab)) {
            new_sstable_list.insert(tab);
        }
        co_await make_ready_future<>(); // yield if needed.
    }
    co_return make_lw_shared<sstables::sstable_set>(std::move(new_sstable_list));
}

future<>
table::update_sstable_lists_on_off_strategy_completion(const std::vector<sstables::shared_sstable>& old_maintenance_sstables,
                                                       const std::vector<sstables::shared_sstable>& new_main_sstables) {
    class sstable_lists_updater : public row_cache::external_updater_impl {
        using sstables_t = std::vector<sstables::shared_sstable>;
        table& _t;
        sstable_list_builder _builder;
        const sstables_t& _old_maintenance;
        const sstables_t& _new_main;
        lw_shared_ptr<sstables::sstable_set> _new_maintenance_list;
        lw_shared_ptr<sstables::sstable_set> _new_main_list;
    public:
        explicit sstable_lists_updater(table& t, sstable_list_builder::permit_t permit, const sstables_t& old_maintenance, const sstables_t& new_main)
                : _t(t), _builder(std::move(permit)), _old_maintenance(old_maintenance), _new_main(new_main) {
        }
        virtual future<> prepare() override {
            sstables_t empty;
            // adding new sstables, created by off-strategy operation, to main set
            _new_main_list = co_await _builder.build_new_list(*_t._main_sstables, _t._compaction_strategy.make_sstable_set(_t._schema), _new_main, empty);
            // removing old sstables, used as input by off-strategy, from the maintenance set
            _new_maintenance_list = co_await _builder.build_new_list(*_t._maintenance_sstables, std::move(*_t.make_maintenance_sstable_set()), empty, _old_maintenance);
        }
        virtual void execute() override {
            _t._main_sstables = std::move(_new_main_list);
            _t._maintenance_sstables = std::move(_new_maintenance_list);
            _t.refresh_compound_sstable_set();
        }
        static std::unique_ptr<row_cache::external_updater_impl> make(table& t, sstable_list_builder::permit_t permit, const sstables_t& old_maintenance, const sstables_t& new_main) {
            return std::make_unique<sstable_lists_updater>(t, std::move(permit), old_maintenance, new_main);
        }
    };
    auto permit = co_await seastar::get_units(_sstable_set_mutation_sem, 1);
    auto updater = row_cache::external_updater(sstable_lists_updater::make(*this, std::move(permit), old_maintenance_sstables, new_main_sstables));

    // row_cache::invalidate() is only used to synchronize sstable list updates, to prevent race conditions from occurring,
    // meaning nothing is actually invalidated.
    dht::partition_range_vector empty_ranges = {};
    co_await _cache.invalidate(std::move(updater), std::move(empty_ranges));
    _cache.refresh_snapshot();
    rebuild_statistics();
}

// Note: must run in a seastar thread
void
table::on_compaction_completion(sstables::compaction_completion_desc& desc) {
    // Build a new list of _sstables: We remove from the existing list the
    // tables we compacted (by now, there might be more sstables flushed
    // later), and we add the new tables generated by the compaction.
    // We create a new list rather than modifying it in-place, so that
    // on-going reads can continue to use the old list.
    //
    // We only remove old sstables after they are successfully deleted,
    // to avoid a new compaction from ignoring data in the old sstables
    // if the deletion fails (note deletion of shared sstables can take
    // unbounded time, because all shards must agree on the deletion).

    // make sure all old sstables belong *ONLY* to current shard before we proceed to their deletion.
    for (auto& sst : desc.old_sstables) {
        auto shards = sst->get_shards_for_this_sstable();
        if (shards.size() > 1) {
            throw std::runtime_error(format("A regular compaction for {}.{} INCORRECTLY used shared sstable {}. Only resharding work with those!",
                _schema->ks_name(), _schema->cf_name(), sst->toc_filename()));
        }
        if (!belongs_to_current_shard(shards)) {
            throw std::runtime_error(format("A regular compaction for {}.{} INCORRECTLY used sstable {} which doesn't belong to this shard!",
                _schema->ks_name(), _schema->cf_name(), sst->toc_filename()));
        }
    }

    // Precompute before so undo_compacted_but_not_deleted can be sure not to throw
    std::unordered_set<sstables::shared_sstable> s(
           desc.old_sstables.begin(), desc.old_sstables.end());
    _sstables_compacted_but_not_deleted.insert(_sstables_compacted_but_not_deleted.end(), desc.old_sstables.begin(), desc.old_sstables.end());
    // After we are done, unconditionally remove compacted sstables from _sstables_compacted_but_not_deleted,
    // or they could stay forever in the set, resulting in deleted files remaining
    // opened and disk space not being released until shutdown.
    auto undo_compacted_but_not_deleted = defer([&] {
        auto e = boost::range::remove_if(_sstables_compacted_but_not_deleted, [&] (sstables::shared_sstable sst) {
            return s.contains(sst);
        });
        _sstables_compacted_but_not_deleted.erase(e, _sstables_compacted_but_not_deleted.end());
        rebuild_statistics();
    });

    class sstable_list_updater : public row_cache::external_updater_impl {
        table& _t;
        sstable_list_builder _builder;
        const sstables::compaction_completion_desc& _desc;
        lw_shared_ptr<sstables::sstable_set> _new_sstables;
    public:
        explicit sstable_list_updater(table& t, sstable_list_builder::permit_t permit, sstables::compaction_completion_desc& d) : _t(t), _builder(std::move(permit)), _desc(d) {}
        virtual future<> prepare() override {
            _new_sstables = co_await _builder.build_new_list(*_t._main_sstables, _t._compaction_strategy.make_sstable_set(_t._schema), _desc.new_sstables, _desc.old_sstables);
        }
        virtual void execute() override {
            _t._main_sstables = std::move(_new_sstables);
            _t.refresh_compound_sstable_set();
        }
        static std::unique_ptr<row_cache::external_updater_impl> make(table& t, sstable_list_builder::permit_t permit, sstables::compaction_completion_desc& d) {
            return std::make_unique<sstable_list_updater>(t, std::move(permit), d);
        }
    };
    auto permit = seastar::get_units(_sstable_set_mutation_sem, 1).get0();
    auto updater = row_cache::external_updater(sstable_list_updater::make(*this, std::move(permit), desc));

    _cache.invalidate(std::move(updater), std::move(desc.ranges_for_cache_invalidation)).get();

    // refresh underlying data source in row cache to prevent it from holding reference
    // to sstables files that are about to be deleted.
    _cache.refresh_snapshot();

    rebuild_statistics();

    auto f = seastar::try_with_gate(_sstable_deletion_gate, [this, sstables_to_remove = desc.old_sstables] {
       return with_semaphore(_sstable_deletion_sem, 1, [sstables_to_remove = std::move(sstables_to_remove)] {
           return sstables::delete_atomically(std::move(sstables_to_remove));
       });
    });

    try {
        f.get();
    } catch (...) {
        // There is nothing more we can do here.
        // Any remaining SSTables will eventually be re-compacted and re-deleted.
        tlogger.error("Compacted SSTables deletion failed: {}. Ignored.", std::current_exception());
    }
}

future<>
table::compact_sstables(sstables::compaction_descriptor descriptor) {
    if (!descriptor.sstables.size()) {
        // if there is nothing to compact, just return.
        return make_ready_future<>();
    }

    descriptor.creator = [this] (shard_id dummy) {
            auto sst = make_sstable();
            return sst;
    };
    descriptor.replacer = [this, release_exhausted = descriptor.release_exhausted] (sstables::compaction_completion_desc desc) {
        _compaction_strategy.notify_completion(desc.old_sstables, desc.new_sstables);
        _compaction_manager.propagate_replacement(this, desc.old_sstables, desc.new_sstables);
        this->on_compaction_completion(desc);
        if (release_exhausted) {
            release_exhausted(desc.old_sstables);
        }
    };

    return sstables::compact_sstables(std::move(descriptor), *this).then([this] (auto info) {
        if (info.type != sstables::compaction_type::Compaction) {
            return make_ready_future<>();
        }
        // skip update if running without a query context, for example, when running a test case.
        if (!db::qctx) {
            return make_ready_future<>();
        }
        // FIXME: add support to merged_rows. merged_rows is a histogram that
        // shows how many sstables each row is merged from. This information
        // cannot be accessed until we make combined_reader more generic,
        // for example, by adding a reducer method.
        return db::system_keyspace::update_compaction_history(info.compaction_uuid, info.ks_name, info.cf_name, info.ended_at,
            info.start_size, info.end_size, std::unordered_map<int32_t, int64_t>{});
    });
}

// Note: We assume that the column_family does not get destroyed during compaction.
future<>
table::compact_all_sstables() {
    return _compaction_manager.submit_major_compaction(this);
}

void table::start_compaction() {
    set_compaction_strategy(_schema->compaction_strategy());
}

void table::trigger_compaction() {
    // Submitting compaction job to compaction manager.
    do_trigger_compaction(); // see below
}

void table::try_trigger_compaction() noexcept {
    try {
        trigger_compaction();
    } catch (...) {
        tlogger.error("Failed to trigger compaction: {}", std::current_exception());
    }
}

void table::do_trigger_compaction() {
    // But only submit if we're not locked out
    if (!_compaction_disabled) {
        _compaction_manager.submit(this);
    }
}

void table::trigger_offstrategy_compaction() {
    // If the user calls trigger_offstrategy_compaction() to trigger
    // off-strategy explicitly, cancel the timeout based automatic trigger.
    _off_strategy_trigger.cancel();
    _compaction_manager.submit_offstrategy(this);
}

future<> table::run_offstrategy_compaction() {
    // This procedure will reshape sstables in maintenance set until it's ready for
    // integration into main set.
    // It may require N reshape rounds before the set satisfies the strategy invariant.
    // This procedure also only updates maintenance set at the end, on success.
    // Otherwise, some overlapping could be introduced in the set after each reshape
    // round, progressively degrading read amplification until integration happens.
    // The drawback of this approach is the 2x space requirement as the old sstables
    // will only be deleted at the end. The impact of this space requirement is reduced
    // by the fact that off-strategy is serialized across all tables, meaning that the
    // actual requirement is the size of the largest table's maintenance set.

    auto sem_unit = co_await seastar::get_units(_off_strategy_sem, 1);

    tlogger.info("Starting off-strategy compaction for {}.{}, {} candidates were found",
        _schema->ks_name(), _schema->cf_name(), _maintenance_sstables->all()->size());

    const auto old_sstables = boost::copy_range<std::vector<sstables::shared_sstable>>(*_maintenance_sstables->all());
    std::vector<sstables::shared_sstable> reshape_candidates = old_sstables;
    std::vector<sstables::shared_sstable> new_unused_sstables, sstables_to_remove;

    auto cleanup_new_unused_sstables_on_failure = defer([&new_unused_sstables] {
        for (auto& sst : new_unused_sstables) {
            sst->mark_for_deletion();
        }
    });

    for (;;) {
        auto& iop = service::get_local_streaming_priority(); // run reshape in maintenance mode
        auto desc = _compaction_strategy.get_reshaping_job(reshape_candidates, _schema, iop, sstables::reshape_mode::strict);
        if (desc.sstables.empty()) {
            // at this moment reshape_candidates contains a set of sstables ready for integration into main set
            co_await update_sstable_lists_on_off_strategy_completion(old_sstables, reshape_candidates);
            break;
        }

        desc.creator = [this, &new_unused_sstables] (shard_id dummy) {
            auto sst = make_sstable();
            new_unused_sstables.push_back(sst);
            return sst;
        };
        auto input = boost::copy_range<std::unordered_set<sstables::shared_sstable>>(desc.sstables);

        auto ret = co_await sstables::compact_sstables(std::move(desc), *this);

        // update list of reshape candidates without input but with output added to it
        auto it = boost::remove_if(reshape_candidates, [&] (auto& s) { return input.contains(s); });
        reshape_candidates.erase(it, reshape_candidates.end());
        std::move(ret.new_sstables.begin(), ret.new_sstables.end(), std::back_inserter(reshape_candidates));
        std::move(input.begin(), input.end(), std::back_inserter(sstables_to_remove));
    }

    cleanup_new_unused_sstables_on_failure.cancel();
    // By marking input sstables for deletion instead, the ones which require view building will stay in the staging
    // directory until they're moved to the main dir when the time comes. Also, that allows view building to resume
    // on restart if there's a crash midway.
    for (auto& sst : sstables_to_remove) {
        sst->mark_for_deletion();
    }

    tlogger.info("Done with off-strategy compaction for {}.{}", _schema->ks_name(), _schema->cf_name());
}

void table::set_compaction_strategy(sstables::compaction_strategy_type strategy) {
    tlogger.debug("Setting compaction strategy of {}.{} to {}", _schema->ks_name(), _schema->cf_name(), sstables::compaction_strategy::name(strategy));
    auto new_cs = make_compaction_strategy(strategy, _schema->compaction_strategy_options());

    _compaction_manager.register_backlog_tracker(new_cs.get_backlog_tracker());
    auto move_read_charges = new_cs.type() == _compaction_strategy.type();
    _compaction_strategy.get_backlog_tracker().transfer_ongoing_charges(new_cs.get_backlog_tracker(), move_read_charges);

    auto new_sstables = new_cs.make_sstable_set(_schema);
    _main_sstables->for_each_sstable([&] (const sstables::shared_sstable& s) {
        add_sstable_to_backlog_tracker(new_cs.get_backlog_tracker(), s);
        new_sstables.insert(s);
    });

    if (!move_read_charges) {
        _compaction_manager.stop_tracking_ongoing_compactions(this);
    }

    // now exception safe:
    _compaction_strategy = std::move(new_cs);
    _main_sstables = std::move(new_sstables);
    refresh_compound_sstable_set();
}

size_t table::sstables_count() const {
    return _sstables->all()->size();
}

std::vector<uint64_t> table::sstable_count_per_level() const {
    std::vector<uint64_t> count_per_level;
    _sstables->for_each_sstable([&] (const sstables::shared_sstable& sst) {
        auto level = sst->get_sstable_level();

        if (level + 1 > count_per_level.size()) {
            count_per_level.resize(level + 1, 0UL);
        }
        count_per_level[level]++;
    });
    return count_per_level;
}

int64_t table::get_unleveled_sstables() const {
    // TODO: when we support leveled compaction, we should return the number of
    // SSTables in L0. If leveled compaction is enabled in this column family,
    // then we should return zero, as we currently do.
    return 0;
}

future<std::unordered_set<sstring>> table::get_sstables_by_partition_key(const sstring& key) const {
    return do_with(std::unordered_set<sstring>(), make_lw_shared<sstables::sstable_set::incremental_selector>(get_sstable_set().make_incremental_selector()),
            partition_key(partition_key::from_nodetool_style_string(_schema, key)),
            [this] (std::unordered_set<sstring>& filenames, lw_shared_ptr<sstables::sstable_set::incremental_selector>& sel, partition_key& pk) {
        return do_with(dht::decorated_key(dht::decorate_key(*_schema, pk)),
                [this, &filenames, &sel, &pk](dht::decorated_key& dk) mutable {
            const auto& sst = sel->select(dk).sstables;
            auto hk = sstables::sstable::make_hashed_key(*_schema, dk.key());

            return do_for_each(sst, [this, &filenames, &dk, hk = std::move(hk)] (std::vector<sstables::shared_sstable>::const_iterator::reference s) mutable {
                auto name = s->get_filename();
                return s->has_partition_key(hk, dk).then([name = std::move(name), &filenames] (bool contains) mutable {
                    if (contains) {
                        filenames.insert(name);
                    }
                });
            });
        }).then([&filenames] {
            return make_ready_future<std::unordered_set<sstring>>(filenames);
        });
    });
}

const sstables::sstable_set& table::get_sstable_set() const {
    // main sstables is enough for the outside world. sstables in other set like maintenance is not needed even for expiration purposes in compaction
    return *_main_sstables;
}

lw_shared_ptr<const sstable_list> table::get_sstables() const {
    return _sstables->all();
}

std::vector<sstables::shared_sstable> table::select_sstables(const dht::partition_range& range) const {
    return _sstables->select(range);
}

std::vector<sstables::shared_sstable> table::in_strategy_sstables() const {
    auto sstables = _main_sstables->all();
    return boost::copy_range<std::vector<sstables::shared_sstable>>(*sstables
            | boost::adaptors::filtered([this] (auto& sst) {
        return !_sstables_staging.contains(sst->generation());
    }));
}

// Gets the list of all sstables in the column family, including ones that are
// not used for active queries because they have already been compacted, but are
// waiting for delete_atomically() to return.
//
// As long as we haven't deleted them, compaction needs to ensure it doesn't
// garbage-collect a tombstone that covers data in an sstable that may not be
// successfully deleted.
lw_shared_ptr<const sstable_list> table::get_sstables_including_compacted_undeleted() const {
    if (_sstables_compacted_but_not_deleted.empty()) {
        return get_sstables();
    }
    auto ret = make_lw_shared<sstable_list>(*_sstables->all());
    for (auto&& s : _sstables_compacted_but_not_deleted) {
        ret->insert(s);
    }
    return ret;
}

const std::vector<sstables::shared_sstable>& table::compacted_undeleted_sstables() const {
    return _sstables_compacted_but_not_deleted;
}

lw_shared_ptr<memtable_list>
table::make_memory_only_memtable_list() {
    auto get_schema = [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(get_schema), _config.dirty_memory_manager, _stats, _config.memory_compaction_scheduling_group);
}

lw_shared_ptr<memtable_list>
table::make_memtable_list() {
    auto seal = [this] (flush_permit&& permit) {
        return seal_active_memtable(std::move(permit));
    };
    auto get_schema = [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(seal), std::move(get_schema), _config.dirty_memory_manager, _stats, _config.memory_compaction_scheduling_group);
}

table::table(schema_ptr schema, config config, db::commitlog* cl, compaction_manager& compaction_manager,
             cell_locker_stats& cl_stats, cache_tracker& row_cache_tracker)
    : _schema(std::move(schema))
    , _config(std::move(config))
    , _view_stats(format("{}_{}_view_replica_update", _schema->ks_name(), _schema->cf_name()),
                         keyspace_label(_schema->ks_name()),
                         column_family_label(_schema->cf_name())
                        )
    , _memtables(_config.enable_disk_writes ? make_memtable_list() : make_memory_only_memtable_list())
    , _compaction_strategy(make_compaction_strategy(_schema->compaction_strategy(), _schema->compaction_strategy_options()))
    , _main_sstables(make_lw_shared<sstables::sstable_set>(_compaction_strategy.make_sstable_set(_schema)))
    , _maintenance_sstables(make_maintenance_sstable_set())
    , _sstables(make_compound_sstable_set())
    , _cache(_schema, sstables_as_snapshot_source(), row_cache_tracker, is_continuous::yes)
    , _commitlog(cl)
    , _durable_writes(true)
    , _compaction_manager(compaction_manager)
    , _index_manager(*this)
    , _counter_cell_locks(_schema->is_counter() ? std::make_unique<cell_locker>(_schema, cl_stats) : nullptr)
    , _row_locker(_schema)
    , _off_strategy_trigger([this] { trigger_offstrategy_compaction(); })
{
    if (!_config.enable_disk_writes) {
        tlogger.warn("Writes disabled, column family no durable.");
    }
    set_metrics();
}

partition_presence_checker
table::make_partition_presence_checker(lw_shared_ptr<sstables::sstable_set> sstables) {
    auto sel = make_lw_shared<sstables::sstable_set::incremental_selector>(sstables->make_incremental_selector());
    return [this, sstables = std::move(sstables), sel = std::move(sel)] (const dht::decorated_key& key) {
        auto& sst = sel->select(key).sstables;
        if (sst.empty()) {
            return partition_presence_checker_result::definitely_doesnt_exist;
        }
        auto hk = sstables::sstable::make_hashed_key(*_schema, key.key());
        for (auto&& s : sst) {
            if (s->filter_has_key(hk)) {
                return partition_presence_checker_result::maybe_exists;
            }
        }
        return partition_presence_checker_result::definitely_doesnt_exist;
    };
}

snapshot_source
table::sstables_as_snapshot_source() {
    return snapshot_source([this] () {
        auto sst_set = _sstables;
        return mutation_source([this, sst_set] (schema_ptr s,
                reader_permit permit,
                const dht::partition_range& r,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding fwd_mr) {
            return make_sstable_reader(std::move(s), std::move(permit), sst_set, r, slice, pc, std::move(trace_state), fwd, fwd_mr);
        }, [this, sst_set] {
            return make_partition_presence_checker(sst_set);
        });
    });
}

// define in .cc, since sstable is forward-declared in .hh
table::~table() {
}


logalloc::occupancy_stats table::occupancy() const {
    logalloc::occupancy_stats res;
    for (auto m : *_memtables) {
        res += m->region().occupancy();
    }
    return res;
}


// Snapshots: snapshotting the files themselves is easy: if more than one CF
// happens to link an SSTable twice, all but one will fail, and we will end up
// with one copy.
//
// The problem for us, is that the snapshot procedure is supposed to leave a
// manifest file inside its directory.  So if we just call snapshot() from
// multiple shards, only the last one will succeed, writing its own SSTables to
// the manifest leaving all other shards' SSTables unaccounted for.
//
// Moreover, for things like drop table, the operation should only proceed when the
// snapshot is complete. That includes the manifest file being correctly written,
// and for this reason we need to wait for all shards to finish their snapshotting
// before we can move on.
//
// To know which files we must account for in the manifest, we will keep an
// SSTable set.  Theoretically, we could just rescan the snapshot directory and
// see what's in there. But we would need to wait for all shards to finish
// before we can do that anyway. That is the hard part, and once that is done
// keeping the files set is not really a big deal.
//
// This code assumes that all shards will be snapshotting at the same time. So
// far this is a safe assumption, but if we ever want to take snapshots from a
// group of shards only, this code will have to be updated to account for that.
struct snapshot_manager {
    std::unordered_set<sstring> files;
    named_semaphore requests = {0, named_semaphore_exception_factory{"snapshot manager requests"}};
    named_semaphore manifest_write = {0, named_semaphore_exception_factory{"snapshot manager manifest write"}};
    snapshot_manager() {}
};
static thread_local std::unordered_map<sstring, lw_shared_ptr<snapshot_manager>> pending_snapshots;

static future<>
seal_snapshot(sstring jsondir) {
    std::ostringstream ss;
    int n = 0;
    ss << "{" << std::endl << "\t\"files\" : [ ";
    for (auto&& rf: pending_snapshots.at(jsondir)->files) {
        if (n++ > 0) {
            ss << ", ";
        }
        ss << "\"" << rf << "\"";
    }
    ss << " ]" << std::endl << "}" << std::endl;

    auto json = ss.str();
    auto jsonfile = jsondir + "/manifest.json";

    tlogger.debug("Storing manifest {}", jsonfile);

    return io_check([jsondir] { return recursive_touch_directory(jsondir); }).then([jsonfile, json = std::move(json)] {
        return open_checked_file_dma(general_disk_error_handler, jsonfile, open_flags::wo | open_flags::create | open_flags::truncate).then([json](file f) {
            return make_file_output_stream(std::move(f)).then([json](output_stream<char>&& out) {
                return do_with(std::move(out), [json] (output_stream<char>& out) {
                    return out.write(json.c_str(), json.size()).then([&out] {
                       return out.flush();
                    }).then([&out] {
                       return out.close();
                    });
                });
            });
        });
    }).then([jsondir] {
        return io_check(sync_directory, std::move(jsondir));
    }).finally([jsondir] {
        pending_snapshots.erase(jsondir);
        return make_ready_future<>();
    });
}

future<> table::write_schema_as_cql(database& db, sstring dir) const {
    std::ostringstream ss;
    try {
        this->schema()->describe(db, ss);
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
    auto schema_description = ss.str();
    auto schema_file_name = dir + "/schema.cql";
    return open_checked_file_dma(general_disk_error_handler, schema_file_name, open_flags::wo | open_flags::create | open_flags::truncate).then([schema_description = std::move(schema_description)](file f) {
        return make_file_output_stream(std::move(f)).then([schema_description  = std::move(schema_description)] (output_stream<char>&& out) mutable {
            return do_with(std::move(out), [schema_description  = std::move(schema_description)] (output_stream<char>& out) {
                return out.write(schema_description.c_str(), schema_description.size()).then([&out] {
                   return out.flush();
                }).then([&out] {
                   return out.close();
                });
            });
        });
    });

}

future<> table::snapshot(database& db, sstring name, bool skip_flush) {
    auto jsondir = _config.datadir + "/snapshots/" + name;
    tlogger.debug("snapshot {}: skip_flush={}", jsondir, skip_flush);
    auto f = skip_flush ? make_ready_future<>() : flush();
    return f.then([this, &db, jsondir = std::move(jsondir)]() {
       return with_semaphore(_sstable_deletion_sem, 1, [this, &db, jsondir = std::move(jsondir)]() {
        auto tables = boost::copy_range<std::vector<sstables::shared_sstable>>(*_sstables->all());
        return do_with(std::move(tables), std::move(jsondir), [this, &db] (std::vector<sstables::shared_sstable>& tables, const sstring& jsondir) {
            return io_check([&jsondir] { return recursive_touch_directory(jsondir); }).then([this, &db, &jsondir, &tables] {
                return max_concurrent_for_each(tables, db.get_config().initial_sstable_loading_concurrency(), [&db, &jsondir] (sstables::shared_sstable sstable) {
                  return with_semaphore(db.get_sharded_sst_dir_semaphore().local(), 1, [&jsondir, sstable] {
                    return io_check([sstable, &dir = jsondir] {
                        return sstable->create_links(dir);
                    });
                  });
                });
            }).then([&jsondir, &tables] {
                return io_check(sync_directory, jsondir);
            }).finally([this, &tables, &db, &jsondir] {
                auto shard = std::hash<sstring>()(jsondir) % smp::count;
                std::unordered_set<sstring> table_names;
                for (auto& sst : tables) {
                    auto f = sst->get_filename();
                    auto rf = f.substr(sst->get_dir().size() + 1);
                    table_names.insert(std::move(rf));
                }
                return smp::submit_to(shard, [requester = this_shard_id(), &jsondir, this, &db,
                                              tables = std::move(table_names), datadir = _config.datadir] {

                    if (!pending_snapshots.contains(jsondir)) {
                        pending_snapshots.emplace(jsondir, make_lw_shared<snapshot_manager>());
                    }
                    auto snapshot = pending_snapshots.at(jsondir);
                    for (auto&& sst: tables) {
                        snapshot->files.insert(std::move(sst));
                    }

                    snapshot->requests.signal(1);
                    auto my_work = make_ready_future<>();
                    if (requester == this_shard_id()) {
                        my_work = snapshot->requests.wait(smp::count).then([&jsondir,
                                                                            &db, snapshot, this] {
                            // this_shard_id() here == requester == this_shard_id() before submit_to() above,
                            // so the db reference is still local
                            return write_schema_as_cql(db, jsondir).handle_exception([&jsondir](std::exception_ptr ptr) {
                                tlogger.error("Failed writing schema file in snapshot in {} with exception {}", jsondir, ptr);
                                return make_ready_future<>();
                            }).finally([&jsondir, snapshot] () mutable {
                                return seal_snapshot(jsondir).then([snapshot] {
                                    snapshot->manifest_write.signal(smp::count);
                                    return make_ready_future<>();
                                });
                            });
                        });
                    }
                    return my_work.then([snapshot] {
                        return snapshot->manifest_write.wait(1);
                    }).then([snapshot] {});
                });
            });
        });
       });
    });
}

future<bool> table::snapshot_exists(sstring tag) {
    sstring jsondir = _config.datadir + "/snapshots/" + tag;
    return open_checked_directory(general_disk_error_handler, std::move(jsondir)).then_wrapped([] (future<file> f) {
        try {
            f.get0();
            return make_ready_future<bool>(true);
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw;
            }
            return make_ready_future<bool>(false);
        }
    });
}

future<std::unordered_map<sstring, table::snapshot_details>> table::get_snapshot_details() {
    return seastar::async([this] {
        std::unordered_map<sstring, snapshot_details> all_snapshots;
        for (auto& datadir : _config.all_datadirs) {
            fs::path snapshots_dir = fs::path(datadir) / "snapshots";
            auto file_exists = io_check([&snapshots_dir] { return seastar::file_exists(snapshots_dir.native()); }).get0();
            if (!file_exists) {
                continue;
            }

            lister::scan_dir(snapshots_dir,  { directory_entry_type::directory }, [this, datadir, &all_snapshots] (fs::path snapshots_dir, directory_entry de) {
                auto snapshot_name = de.name;
                all_snapshots.emplace(snapshot_name, snapshot_details());
                return lister::scan_dir(snapshots_dir / fs::path(snapshot_name),  { directory_entry_type::regular }, [this, datadir, &all_snapshots, snapshot_name] (fs::path snapshot_dir, directory_entry de) {
                    return io_check(file_size, (snapshot_dir / de.name).native()).then([this, datadir, &all_snapshots, snapshot_name, snapshot_dir, name = de.name] (auto size) {
                        // The manifest is the only file expected to be in this directory not belonging to the SSTable.
                        // For it, we account the total size, but zero it for the true size calculation.
                        //
                        // All the others should just generate an exception: there is something wrong, so don't blindly
                        // add it to the size.
                        if (name != "manifest.json" && name != "schema.cql") {
                            sstables::entry_descriptor::make_descriptor(snapshot_dir.native(), name);
                            all_snapshots.at(snapshot_name).total += size;
                        } else {
                            size = 0;
                        }
                        return io_check(file_size, (fs::path(datadir) / name).native()).then_wrapped([&all_snapshots, snapshot_name, size] (auto fut) {
                            try {
                                // File exists in the main SSTable directory. Snapshots are not contributing to size
                                fut.get0();
                            } catch (std::system_error& e) {
                                if (e.code() != std::error_code(ENOENT, std::system_category())) {
                                    throw;
                                }
                                all_snapshots.at(snapshot_name).live += size;
                            }
                            return make_ready_future<>();
                        });
                    });
                });
            }).get();
        }
        return all_snapshots;
    });
}

future<> table::flush(std::optional<db::replay_position> pos) {
    if (pos && *pos < _flush_rp) {
        return make_ready_future<>();
    }
    auto op = _pending_flushes_phaser.start();
    return _memtables->flush().then([this, op = std::move(op), fp = _highest_rp] {
        _flush_rp = std::max(_flush_rp, fp);
    });
}

bool table::can_flush() const {
    return _memtables->can_flush();
}

future<> table::clear() {
    if (_commitlog) {
        for (auto& t : *_memtables) {
            _commitlog->discard_completed_segments(_schema->id(), t->get_and_discard_rp_set());
        }
    }
    _memtables->clear_and_add();
    return _cache.invalidate(row_cache::external_updater([] { /* There is no underlying mutation source */ }));
}

// NOTE: does not need to be futurized, but might eventually, depending on
// if we implement notifications, whatnot.
future<db::replay_position> table::discard_sstables(db_clock::time_point truncated_at) {
    assert(_compaction_disabled > 0);

    struct pruner {
        column_family& cf;
        db::replay_position rp;
        struct removed_sstable {
            sstables::shared_sstable sst;
            ::enable_backlog_tracker enable_backlog_tracker;
        };
        std::vector<removed_sstable> remove;

        pruner(column_family& cf)
            : cf(cf) {}

        void prune(db_clock::time_point truncated_at) {
            auto gc_trunc = to_gc_clock(truncated_at);

            auto pruned = make_lw_shared<sstables::sstable_set>(cf._compaction_strategy.make_sstable_set(cf._schema));
            auto maintenance_pruned = cf.make_maintenance_sstable_set();

            auto prune = [this, &gc_trunc] (lw_shared_ptr<sstables::sstable_set>& pruned,
                                            lw_shared_ptr<sstables::sstable_set>& pruning,
                                            ::enable_backlog_tracker enable_backlog_tracker) mutable {
                pruning->for_each_sstable([&] (const sstables::shared_sstable& p) mutable {
                    if (p->max_data_age() <= gc_trunc) {
                        rp = std::max(p->get_stats_metadata().position, rp);
                        remove.emplace_back(removed_sstable{p, enable_backlog_tracker});
                        return;
                    }
                    pruned->insert(p);
                });
            };
            prune(pruned, cf._main_sstables, enable_backlog_tracker::yes);
            prune(maintenance_pruned, cf._maintenance_sstables, enable_backlog_tracker::no);

            cf._main_sstables = std::move(pruned);
            cf._maintenance_sstables = std::move(maintenance_pruned);
            cf.refresh_compound_sstable_set();
        }
    };
    auto p = make_lw_shared<pruner>(*this);
    return _cache.invalidate(row_cache::external_updater([p, truncated_at] {
        p->prune(truncated_at);
        tlogger.debug("cleaning out row cache");
    })).then([this, p]() mutable {
        rebuild_statistics();

        return parallel_for_each(p->remove, [this](pruner::removed_sstable& r) {
            if (r.enable_backlog_tracker) {
                remove_sstable_from_backlog_tracker(_compaction_strategy.get_backlog_tracker(), r.sst);
            }
            return sstables::delete_atomically({r.sst});
        }).then([p] {
            return make_ready_future<db::replay_position>(p->rp);
        });
    });
}

void table::set_schema(schema_ptr s) {
    assert(s->is_counter() == _schema->is_counter());
    tlogger.debug("Changing schema version of {}.{} ({}) from {} to {}",
                _schema->ks_name(), _schema->cf_name(), _schema->id(), _schema->version(), s->version());

    for (auto& m : *_memtables) {
        m->set_schema(s);
    }

    _cache.set_schema(s);
    if (_counter_cell_locks) {
        _counter_cell_locks->set_schema(s);
    }
    _schema = std::move(s);

    for (auto&& v : _views) {
        v->view_info()->set_base_info(
            v->view_info()->make_base_dependent_view_info(*_schema));
    }

    set_compaction_strategy(_schema->compaction_strategy());
    trigger_compaction();
}

static std::vector<view_ptr>::iterator find_view(std::vector<view_ptr>& views, const view_ptr& v) {
    return std::find_if(views.begin(), views.end(), [&v] (auto&& e) {
        return e->id() == v->id();
    });
}

void table::add_or_update_view(view_ptr v) {
    v->view_info()->set_base_info(
        v->view_info()->make_base_dependent_view_info(*_schema));
    auto existing = find_view(_views, v);
    if (existing != _views.end()) {
        *existing = std::move(v);
    } else {
        _views.push_back(std::move(v));
    }
}

void table::remove_view(view_ptr v) {
    auto existing = find_view(_views, v);
    if (existing != _views.end()) {
        _views.erase(existing);
    }
}

void table::clear_views() {
    _views.clear();
}

const std::vector<view_ptr>& table::views() const {
    return _views;
}

std::vector<view_ptr> table::affected_views(const schema_ptr& base, const mutation& update) const {
    //FIXME: Avoid allocating a vector here; consider returning the boost iterator.
    return boost::copy_range<std::vector<view_ptr>>(_views | boost::adaptors::filtered([&, this] (auto&& view) {
        return db::view::partition_key_matches(*base, *view->view_info(), update.decorated_key());
    }));
}

static size_t memory_usage_of(const utils::chunked_vector<frozen_mutation_and_schema>& ms) {
    // Overhead of sending a view mutation, in terms of data structures used by the storage_proxy.
    constexpr size_t base_overhead_bytes = 256;
    return boost::accumulate(ms | boost::adaptors::transformed([] (const frozen_mutation_and_schema& m) {
        return m.fm.representation().size();
    }), size_t{base_overhead_bytes * ms.size()});
}

/**
 * Given some updates on the base table and the existing values for the rows affected by that update, generates the
 * mutations to be applied to the base table's views, and sends them to the paired view replicas.
 *
 * @param base the base schema at a particular version.
 * @param views the affected views which need to be updated.
 * @param updates the base table updates being applied.
 * @param existings the existing values for the rows affected by updates. This is used to decide if a view is
 * obsoleted by the update and should be removed, gather the values for columns that may not be part of the update if
 * a new view entry needs to be created, and compute the minimal updates to be applied if the view entry isn't changed
 * but has simply some updated values.
 * @return a future resolving to the mutations to apply to the views, which can be empty.
 */
future<> table::generate_and_propagate_view_updates(const schema_ptr& base,
        reader_permit permit,
        std::vector<db::view::view_and_base>&& views,
        mutation&& m,
        flat_mutation_reader_opt existings,
        tracing::trace_state_ptr tr_state,
        gc_clock::time_point now) const {
    auto base_token = m.token();
    db::view::view_update_builder builder = co_await db::view::make_view_update_builder(
            base,
            std::move(views),
            flat_mutation_reader_from_mutations(std::move(permit), {std::move(m)}),
            std::move(existings),
            now);

    std::exception_ptr err = nullptr;
    while (true) {
        utils::chunked_vector<frozen_mutation_and_schema> updates;
        try {
            updates = co_await builder.build_some();
        } catch (...) {
            err = std::current_exception();
            break;
        }
        if (updates.empty()) {
            break;
        }
        tracing::trace(tr_state, "Generated {} view update mutations", updates.size());
        auto units = seastar::consume_units(*_config.view_update_concurrency_semaphore, memory_usage_of(updates));
        try {
            co_await db::view::mutate_MV(base_token, std::move(updates), _view_stats, *_config.cf_stats, tr_state,
                std::move(units), service::allow_hints::yes, db::view::wait_for_all_updates::no);
        } catch (...) {
            // Ignore exceptions: any individual failure to propagate a view update will be reported
            // by a separate mechanism in mutate_MV() function. Moreover, we should continue trying
            // to generate updates even if some of them fail, in order to minimize the potential
            // inconsistencies caused by not being able to propagate an update
        }
    }
    co_await builder.close();
    if (err) {
        std::rethrow_exception(err);
    }
}

/**
 * Shard-local locking of clustering rows or entire partitions of the base
 * table during a Materialized-View read-modify-update:
 *
 * Consider that two concurrent base-table updates set column C, a column
 * added to a view's primary key, to two different values - V1 and V2.
 * Say that that before the updates, C's value was V0. Both updates may remove
 * from the view the old row with V0, one will add a view row with V1 and the
 * second will add a view row with V2, and we end up with two rows, with the
 * two different values, instead of just one row with the last value.
 *
 * The solution is to lock the base row which we read to ensure atomic read-
 * modify-write to the view table: Under one locked section, the row with V0
 * is deleted and a new one with V1 is created, and then under a second locked
 * section the row with V1 is deleted and a new  one with V2 is created.
 * Note that the lock is node-local (and in fact shard-local) and the locked
 * section doesn't include the view table modifications - it includes just the
 * read and the creation of the update commands - commands which will
 * eventually be sent to the view replicas.
 *
 * We need to lock a base-table row even if an update does not modify the
 * view's new key column C: Consider an update that only updates a non-key
 * column (but also in the view) D. We still need to read the current base row
 * to retrieve the view row's current key (column C), and then write the
 * modification to *that* view row. Having several such modifications in
 * parallel is fine. What is not fine is to have in parallel a modification
 * of the value of C. So basically we need a reader-writer lock (a.k.a.
 * shared-exclusive lock) on base rows:
 * 1. Updates which do not modify the view's key column take a reader lock
 *    on the base row.
 * 2. Updates which do modify the view's key column take a writer lock.
 *
 * Further complicating matters is that some operations involve multiple
 * base rows - such as a deletion of an entire partition or a range of rows.
 * In that case, we should lock the entire partition, and forbid parallel
 * work on the same partition or one of its rows. We can do this with a
 * read-writer lock on base partitions:
 * 1. Before we lock a row (as described above), we lock its partition key
 *    with the reader lock.
 * 2. When an operation involves an entire partition (or range of rows),
 *    we lock the partition key with a writer lock.
 *
 * If an operation involves only a range of rows, not an entire partition,
 * we could in theory lock only this range and not an entire partition.
 * However, we expect this case to be rare enough to not care about and we
 * currently just lock the entire partition.
 *
 * If a base table has *multiple* views, we still read the base table row
 * only once, and have to keep a lock around this read and all the view
 * updates generation. This lock needs to be the strictest of the above -
 * i.e., if a column is modified which is not part of one view's key but is
 * part of a second view's key - we should lock the base row with the
 * stricter writer lock, not a reader lock.
 */
future<row_locker::lock_holder>
table::local_base_lock(
        const schema_ptr& s,
        const dht::decorated_key& pk,
        const query::clustering_row_ranges& rows,
        db::timeout_clock::time_point timeout) const {
    // FIXME: Optimization:
    // Below we always pass "true" to the lock functions and take an exclusive
    // lock on the affected row or partition. But as explained above, if all
    // the modified columns are not key columns in *any* of the views, and
    // shared lock is enough. We should test for this case and pass false.
    // This will allow more parallelism in concurrent modifications to the
    // same row - probably not a very urgent case.
    _row_locker.upgrade(s);
    if (rows.size() == 1 && rows[0].is_singular() && rows[0].start() && !rows[0].start()->value().is_empty(*s)) {
        // A single clustering row is involved.
        return _row_locker.lock_ck(pk, rows[0].start()->value(), true, timeout, _row_locker_stats);
    } else {
        // More than a single clustering row is involved. Most commonly it's
        // the entire partition, so let's lock the entire partition. We could
        // lock less than the entire partition in more elaborate cases where
        // just a few individual rows are involved, or row ranges, but we
        // don't think this will make a practical difference.
        return _row_locker.lock_pk(pk, true, timeout, _row_locker_stats);
    }
}

/**
 * Given some updates on the base table and assuming there are no pre-existing, overlapping updates,
 * generates the mutations to be applied to the base table's views, and sends them to the paired
 * view replicas. The future resolves when the updates have been acknowledged by the repicas, i.e.,
 * propagating the view updates to the view replicas happens synchronously.
 *
 * @param views the affected views which need to be updated.
 * @param base_token The token to use to match the base replica with the paired replicas.
 * @param reader the base table updates being applied, which all correspond to the base token.
 * @return a future that resolves when the updates have been acknowledged by the view replicas
 */
future<> table::populate_views(
        std::vector<db::view::view_and_base> views,
        dht::token base_token,
        flat_mutation_reader&& reader,
        gc_clock::time_point now) {
    auto schema = reader.schema();
    db::view::view_update_builder builder = co_await db::view::make_view_update_builder(
            schema,
            std::move(views),
            std::move(reader),
            { },
            now);

    std::exception_ptr err;
    while (true) {
        try {
            auto updates = co_await builder.build_some();
            if (updates.empty()) {
                break;
            }
            size_t update_size = memory_usage_of(updates);
            size_t units_to_wait_for = std::min(_config.view_update_concurrency_semaphore_limit, update_size);
            auto units = co_await seastar::get_units(*_config.view_update_concurrency_semaphore, units_to_wait_for);
            units.adopt(seastar::consume_units(*_config.view_update_concurrency_semaphore, update_size - units_to_wait_for));
            co_await db::view::mutate_MV(base_token, std::move(updates), _view_stats, *_config.cf_stats,
                    tracing::trace_state_ptr(), std::move(units), service::allow_hints::no, db::view::wait_for_all_updates::yes);
        } catch (...) {
            if (!err) {
                err = std::current_exception();
            }
        }
    }
    co_await builder.close();
    if (err) {
        std::rethrow_exception(err);
    }
}

const ssize_t new_reader_base_cost{16 * 1024};

size_t table::estimate_read_memory_cost() const {
    return new_reader_base_cost;
}

void table::set_hit_rate(gms::inet_address addr, cache_temperature rate) {
    auto& e = _cluster_cache_hit_rates[addr];
    e.rate = rate;
    e.last_updated = lowres_clock::now();
}

table::cache_hit_rate table::get_hit_rate(gms::inet_address addr) {
    auto it = _cluster_cache_hit_rates.find(addr);
    if (utils::fb_utilities::get_broadcast_address() == addr) {
        return cache_hit_rate { _global_cache_hit_rate, lowres_clock::now()};
    }
    if (it == _cluster_cache_hit_rates.end()) {
        // no data yet, get it from the gossiper
        auto& gossiper = gms::get_local_gossiper();
        auto* eps = gossiper.get_endpoint_state_for_endpoint_ptr(addr);
        if (eps) {
            auto* state = eps->get_application_state_ptr(gms::application_state::CACHE_HITRATES);
            float f = -1.0f; // missing state means old node
            if (state) {
                sstring me = format("{}.{}", _schema->ks_name(), _schema->cf_name());
                auto i = state->value.find(me);
                if (i != sstring::npos) {
                    f = strtof(&state->value[i + me.size() + 1], nullptr);
                } else {
                    f = 0.0f; // empty state means that node has rebooted
                }
                set_hit_rate(addr, cache_temperature(f));
                return cache_hit_rate{cache_temperature(f), lowres_clock::now()};
            }
        }
        return cache_hit_rate {cache_temperature(0.0f), lowres_clock::now()};
    } else {
        return it->second;
    }
}

void table::drop_hit_rate(gms::inet_address addr) {
    _cluster_cache_hit_rates.erase(addr);
}

void
table::check_valid_rp(const db::replay_position& rp) const {
    if (rp != db::replay_position() && rp < _lowest_allowed_rp) {
        throw mutation_reordered_with_truncate_exception();
    }
}

db::replay_position table::set_low_replay_position_mark() {
    _lowest_allowed_rp = _highest_rp;
    return _lowest_allowed_rp;
}

template<typename... Args>
void table::do_apply(db::rp_handle&& h, Args&&... args) {
    if (_async_gate.is_closed()) {
        on_internal_error(tlogger, "Table async_gate is closed");
    }

    utils::latency_counter lc;
    _stats.writes.set_latency(lc);
    db::replay_position rp = h;
    check_valid_rp(rp);
    try {
        _memtables->active_memtable().apply(std::forward<Args>(args)..., std::move(h));
        _highest_rp = std::max(_highest_rp, rp);
    } catch (...) {
        _failed_counter_applies_to_memtable++;
        throw;
    }
    _stats.writes.mark(lc);
    if (lc.is_start()) {
        _stats.estimated_write.add(lc.latency());
    }
}

void
table::apply(const mutation& m, db::rp_handle&& h) {
    do_apply(std::move(h), m);
}

void
table::apply(const frozen_mutation& m, const schema_ptr& m_schema, db::rp_handle&& h) {
    do_apply(std::move(h), m, m_schema);
}

future<>
write_memtable_to_sstable(flat_mutation_reader reader,
                          memtable& mt, sstables::shared_sstable sst,
                          sstables::write_monitor& monitor,
                          sstables::sstable_writer_config& cfg,
                          const io_priority_class& pc) {
    cfg.replay_position = mt.replay_position();
    cfg.monitor = &monitor;
    cfg.origin = "memtable";
    schema_ptr s = reader.schema();
    return sst->write_components(std::move(reader), mt.partition_count(), s, cfg, mt.get_encoding_stats(), pc);
}

future<>
write_memtable_to_sstable(reader_permit permit, memtable& mt, sstables::shared_sstable sst,
                          sstables::write_monitor& monitor,
                          sstables::sstable_writer_config& cfg,
                          const io_priority_class& pc) {
    return write_memtable_to_sstable(mt.make_flush_reader(mt.schema(), std::move(permit), pc), mt, std::move(sst), monitor, cfg, pc);
}

future<>
write_memtable_to_sstable(memtable& mt, sstables::shared_sstable sst, sstables::sstable_writer_config cfg) {
    return do_with(
            permit_monitor(make_lw_shared(sstable_write_permit::unconditional())),
            std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, "write_memtable_to_sstable"),
            cfg,
            [&mt, sst] (auto& monitor, auto& semaphore, auto& cfg) {
        return write_memtable_to_sstable(semaphore->make_tracking_only_permit(mt.schema().get(), "mt_to_sst", db::no_timeout), mt, std::move(sst), monitor, cfg)
        .finally([&semaphore] {
                return semaphore->stop();
        });
    });
}


struct query_state {
    explicit query_state(schema_ptr s,
                         const query::read_command& cmd,
                         query::result_options opts,
                         const dht::partition_range_vector& ranges,
                         query::result_memory_accounter memory_accounter)
            : schema(std::move(s))
            , cmd(cmd)
            , builder(cmd.slice, opts, std::move(memory_accounter))
            , limit(cmd.get_row_limit())
            , partition_limit(cmd.partition_limit)
            , current_partition_range(ranges.begin())
            , range_end(ranges.end()){
    }
    schema_ptr schema;
    const query::read_command& cmd;
    query::result::builder builder;
    uint64_t limit;
    uint32_t partition_limit;
    bool range_empty = false;   // Avoid ubsan false-positive when moving after construction
    dht::partition_range_vector::const_iterator current_partition_range;
    dht::partition_range_vector::const_iterator range_end;
    uint64_t remaining_rows() const {
        return limit - builder.row_count();
    }
    uint32_t remaining_partitions() const {
        return partition_limit - builder.partition_count();
    }
    bool done() const {
        return !remaining_rows() || !remaining_partitions() || current_partition_range == range_end || builder.is_short_read();
    }
};

future<lw_shared_ptr<query::result>>
table::query(schema_ptr s,
        reader_permit permit,
        const query::read_command& cmd,
        query::query_class_config class_config,
        query::result_options opts,
        const dht::partition_range_vector& partition_ranges,
        tracing::trace_state_ptr trace_state,
        query::result_memory_limiter& memory_limiter,
        db::timeout_clock::time_point timeout,
        std::optional<query::data_querier>* saved_querier) {
    if (cmd.get_row_limit() == 0 || cmd.slice.partition_row_limit() == 0 || cmd.partition_limit == 0) {
        co_return make_lw_shared<query::result>();
    }

    _async_gate.enter();
    utils::latency_counter lc;
    _stats.reads.set_latency(lc);

    auto finally = defer([&] () noexcept {
        _stats.reads.mark(lc);
        if (lc.is_start()) {
            _stats.estimated_read.add(lc.latency());
        }
        _async_gate.leave();
    });

    const auto short_read_allowed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
    auto accounter = co_await (opts.request == query::result_request::only_digest
             ? memory_limiter.new_digest_read(*cmd.max_result_size, short_read_allowed) : memory_limiter.new_data_read(*cmd.max_result_size, short_read_allowed));

    query_state qs(s, cmd, opts, partition_ranges, std::move(accounter));

    std::optional<query::data_querier> querier_opt;
    if (saved_querier) {
        querier_opt = std::move(*saved_querier);
    }

    while (!qs.done()) {
        auto&& range = *qs.current_partition_range++;

        if (!querier_opt) {
            querier_opt = query::data_querier(as_mutation_source(), s, permit, range, qs.cmd.slice,
                    service::get_local_sstable_query_read_priority(), trace_state);
        }
        auto& q = *querier_opt;

        std::exception_ptr ex;
      try {
        co_await q.consume_page(query_result_builder(*s, qs.builder), qs.remaining_rows(), qs.remaining_partitions(), qs.cmd.timestamp,
                class_config.max_memory_for_unlimited_query);
      } catch (...) {
        ex = std::current_exception();
      }
        if (ex || !qs.done()) {
            co_await q.close();
            querier_opt = {};
        }
        if (ex) {
            co_return coroutine::exception(std::move(ex));
        }
    }

    if (!saved_querier || (querier_opt && !querier_opt->are_limits_reached() && !qs.builder.is_short_read())) {
        co_await querier_opt->close();
        querier_opt = {};
    }
    if (saved_querier) {
        *saved_querier = std::move(querier_opt);
    }

    co_return make_lw_shared<query::result>(qs.builder.build());
}

future<reconcilable_result>
table::mutation_query(schema_ptr s,
        reader_permit permit,
        const query::read_command& cmd,
        query::query_class_config class_config,
        const dht::partition_range& range,
        tracing::trace_state_ptr trace_state,
        query::result_memory_accounter accounter,
        db::timeout_clock::time_point timeout,
        std::optional<query::mutation_querier>* saved_querier) {
    if (cmd.get_row_limit() == 0 || cmd.slice.partition_row_limit() == 0 || cmd.partition_limit == 0) {
        co_return reconcilable_result();
    }

    std::optional<query::mutation_querier> querier_opt;
    if (saved_querier) {
        querier_opt = std::move(*saved_querier);
    }
    if (!querier_opt) {
        querier_opt = query::mutation_querier(as_mutation_source(), s, permit, range, cmd.slice,
                service::get_local_sstable_query_read_priority(), trace_state);
    }
    auto& q = *querier_opt;

    std::exception_ptr ex;
  try {
    // Un-reverse the schema sent to the coordinator, it expects the
    // legacy format.
    auto result_schema = cmd.slice.options.contains(query::partition_slice::option::reversed) ? s->make_reversed() : s;
    auto rrb = reconcilable_result_builder(*result_schema, cmd.slice, std::move(accounter));
    auto r = co_await q.consume_page(std::move(rrb), cmd.get_row_limit(), cmd.partition_limit, cmd.timestamp, class_config.max_memory_for_unlimited_query);

    if (!saved_querier || (!q.are_limits_reached() && !r.is_short_read())) {
        co_await q.close();
        querier_opt = {};
    }
    if (saved_querier) {
        *saved_querier = std::move(querier_opt);
    }

    co_return r;
  } catch (...) {
    ex = std::current_exception();
  }
    co_await q.close();
    co_return coroutine::exception(std::move(ex));
}

mutation_source
table::as_mutation_source() const {
    return mutation_source([this] (schema_ptr s,
                                   reader_permit permit,
                                   const dht::partition_range& range,
                                   const query::partition_slice& slice,
                                   const io_priority_class& pc,
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr) {
        return this->make_reader(std::move(s), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
    });
}

void table::add_coordinator_read_latency(utils::estimated_histogram::duration latency) {
    _stats.estimated_coordinator_read.add(std::chrono::duration_cast<std::chrono::microseconds>(latency).count());
}

std::chrono::milliseconds table::get_coordinator_read_latency_percentile(double percentile) {
    if (_cached_percentile != percentile || lowres_clock::now() - _percentile_cache_timestamp > 1s) {
        _percentile_cache_timestamp = lowres_clock::now();
        _cached_percentile = percentile;
        _percentile_cache_value = std::max(_stats.estimated_coordinator_read.percentile(percentile) / 1000, int64_t(1)) * 1ms;
        _stats.estimated_coordinator_read *= 0.9; // decay values a little to give new data points more weight
    }
    return _percentile_cache_value;
}

future<>
table::run_with_compaction_disabled(std::function<future<> ()> func) {
    ++_compaction_disabled;
    return _compaction_manager.remove(this).then(std::move(func)).finally([this] {
        if (--_compaction_disabled == 0) {
            // we're turning if on again, use function that does not increment
            // the counter further.
            do_trigger_compaction();
        }
    });
}

void
table::enable_auto_compaction() {
    // XXX: unmute backlog. turn table backlog back on.
    //      see table::disable_auto_compaction() notes.
    _compaction_disabled_by_user = false;
}

void
table::disable_auto_compaction() {
    // XXX: mute backlog. When we disable background compactions
    // for the table, we must also disable current backlog of the
    // table compaction strategy that contributes to the scheduling
    // group resources prioritization.
    //
    // There are 2 possibilities possible:
    // - there are no ongoing background compaction, and we can freely
    //   mute table backlog.
    // - there are compactions happening. than we must decide either
    //   we want to allow them to finish not allowing submitting new
    //   compactions tasks, or we may "suspend" them until the bg
    //   compactions will be enabled back. This is not a worst option
    //   because it will allow bg compactions to finish if there are
    //   unused resourced, it will not lose any writers/readers stats.
    //
    // Besides that:
    // - there are major compactions that additionally uses constant
    //   size backlog of shares,
    // - sstables rewrites tasks that do the same.
    // 
    // Setting NullCompactionStrategy is not an option due to the
    // following reasons:
    // - it will 0 backlog if suspending current compactions is not an
    //   option
    // - it will break computation of major compaction descriptor
    //   for new submissions
    _compaction_disabled_by_user = true;
}

flat_mutation_reader
table::make_reader_excluding_sstables(schema_ptr s,
        reader_permit permit,
        std::vector<sstables::shared_sstable>& excluded,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const {
    std::vector<flat_mutation_reader> readers;
    readers.reserve(_memtables->size() + 1);

    for (auto&& mt : *_memtables) {
        readers.emplace_back(mt->make_flat_reader(s, permit, range, slice, pc, trace_state, fwd, fwd_mr));
    }

    auto excluded_ssts = boost::copy_range<std::unordered_set<sstables::shared_sstable>>(excluded);
    auto effective_sstables = make_lw_shared(_compaction_strategy.make_sstable_set(_schema));
    _sstables->for_each_sstable([&excluded_ssts, &effective_sstables] (const sstables::shared_sstable& sst) mutable {
        if (excluded_ssts.contains(sst)) {
            return;
        }
        effective_sstables->insert(sst);
    });

    readers.emplace_back(make_sstable_reader(s, permit, std::move(effective_sstables), range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    return make_combined_reader(s, std::move(permit), std::move(readers), fwd, fwd_mr);
}

future<> table::move_sstables_from_staging(std::vector<sstables::shared_sstable> sstables) {
    return with_semaphore(_sstable_deletion_sem, 1, [this, sstables = std::move(sstables)] {
        return do_with(std::set<sstring>({dir()}), std::move(sstables), _main_sstables->all(),
                       [this] (std::set<sstring>& dirs_to_sync, std::vector<sstables::shared_sstable>& sstables, lw_shared_ptr<sstable_list>& main_sstables) {
            return do_for_each(sstables, [this, &dirs_to_sync, &main_sstables] (sstables::shared_sstable sst) {
                dirs_to_sync.emplace(sst->get_dir());
                return sst->move_to_new_dir(dir(), sst->generation(), false).then_wrapped([this, sst, &dirs_to_sync, &main_sstables] (future<> f) {
                    if (!f.failed()) {
                        _sstables_staging.erase(sst->generation());
                        // Maintenance SSTables being moved from staging shouldn't be added to tracker because they're off-strategy
                        if (main_sstables->contains(sst)) {
                            add_sstable_to_backlog_tracker(_compaction_strategy.get_backlog_tracker(), sst);
                        }
                        return make_ready_future<>();
                    } else {
                        auto ep = f.get_exception();
                        tlogger.warn("Failed to move sstable {} from staging: {}", sst->get_filename(), ep);
                        return make_exception_future<>(ep);
                    }
                });
            }).finally([&dirs_to_sync] {
                return parallel_for_each(dirs_to_sync, [] (sstring dir) {
                    return sync_directory(dir);
                });
            });
        });
    });
}

/**
 * Given an update for the base table, calculates the set of potentially affected views,
 * generates the relevant updates, and sends them to the paired view replicas.
 */
future<row_locker::lock_holder> table::push_view_replica_updates(const schema_ptr& s, const frozen_mutation& fm,
        db::timeout_clock::time_point timeout, tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem) const {
    //FIXME: Avoid unfreezing here.
    auto m = fm.unfreeze(s);
    return push_view_replica_updates(s, std::move(m), timeout, std::move(tr_state), sem);
}

future<row_locker::lock_holder> table::do_push_view_replica_updates(schema_ptr s, mutation m, db::timeout_clock::time_point timeout, mutation_source source,
        tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem, const io_priority_class& io_priority, query::partition_slice::option_set custom_opts) const {
    if (!_config.view_update_concurrency_semaphore->current()) {
        // We don't have resources to generate view updates for this write. If we reached this point, we failed to
        // throttle the client. The memory queue is already full, waiting on the semaphore would cause this node to
        // run out of memory, and generating hints would ultimately result in the disk queue being full too. We don't
        // drop the base write, which could create inconsistencies between base replicas. So we dolefully continue,
        // and note the fact we dropped a view update.
        ++_config.cf_stats->dropped_view_updates;
        co_return row_locker::lock_holder();
    }
    schema_ptr base = schema();
    m.upgrade(base);
    gc_clock::time_point now = gc_clock::now();
    utils::get_local_injector().inject("table_push_view_replica_updates_stale_time_point", [&now] {
        now -= 10s;
    });
    auto views = db::view::with_base_info_snapshot(affected_views(base, m));
    if (views.empty()) {
        co_return row_locker::lock_holder();
    }
    auto cr_ranges = co_await db::view::calculate_affected_clustering_ranges(*base, m.decorated_key(), m.partition(), views);
    if (cr_ranges.empty()) {
        tracing::trace(tr_state, "View updates do not require read-before-write");
        co_await generate_and_propagate_view_updates(base, sem.make_tracking_only_permit(s.get(), "push-view-updates-1", timeout), std::move(views), std::move(m), { }, std::move(tr_state), now);
        // In this case we are not doing a read-before-write, just a
        // write, so no lock is needed.
        co_return row_locker::lock_holder();
    }
    // We read the whole set of regular columns in case the update now causes a base row to pass
    // a view's filters, and a view happens to include columns that have no value in this update.
    // Also, one of those columns can determine the lifetime of the base row, if it has a TTL.
    auto columns = boost::copy_range<query::column_id_vector>(
            base->regular_columns() | boost::adaptors::transformed(std::mem_fn(&column_definition::id)));
    query::partition_slice::option_set opts;
    opts.set(query::partition_slice::option::send_partition_key);
    opts.set(query::partition_slice::option::send_clustering_key);
    opts.set(query::partition_slice::option::send_timestamp);
    opts.set(query::partition_slice::option::send_ttl);
    opts.add(custom_opts);
    auto slice = query::partition_slice(
            std::move(cr_ranges), { }, std::move(columns), std::move(opts), { }, cql_serialization_format::internal(), query::max_rows);
    // Take the shard-local lock on the base-table row or partition as needed.
    // We'll return this lock to the caller, which will release it after
    // writing the base-table update.
    future<row_locker::lock_holder> lockf = local_base_lock(base, m.decorated_key(), slice.default_row_ranges(), timeout);
    co_await utils::get_local_injector().inject("table_push_view_replica_updates_timeout", timeout);
    auto lock = co_await std::move(lockf);
    auto pk = dht::partition_range::make_singular(m.decorated_key());
    auto permit = sem.make_tracking_only_permit(base.get(), "push-view-updates-2", timeout);
    auto reader = source.make_reader(base, permit, pk, slice, io_priority, tr_state, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    co_await this->generate_and_propagate_view_updates(base, std::move(permit), std::move(views), std::move(m), std::move(reader), tr_state, now);
    tracing::trace(tr_state, "View updates for {}.{} were generated and propagated", base->ks_name(), base->cf_name());
    // return the local partition/row lock we have taken so it
    // remains locked until the caller is done modifying this
    // partition/row and destroys the lock object.
    co_return std::move(lock);

}

future<row_locker::lock_holder> table::push_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout,
        tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem) const {
    return do_push_view_replica_updates(s, std::move(m), timeout, as_mutation_source(),
            std::move(tr_state), sem, service::get_local_sstable_query_read_priority(), {});
}

future<row_locker::lock_holder>
table::stream_view_replica_updates(const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout,
        std::vector<sstables::shared_sstable>& excluded_sstables) const {
    return do_push_view_replica_updates(
            s,
            std::move(m),
            timeout,
            as_mutation_source_excluding(excluded_sstables),
            tracing::trace_state_ptr(),
            *_config.streaming_read_concurrency_semaphore,
            service::get_local_streaming_priority(),
            query::partition_slice::option_set::of<query::partition_slice::option::bypass_cache>());
}

mutation_source
table::as_mutation_source_excluding(std::vector<sstables::shared_sstable>& ssts) const {
    return mutation_source([this, &ssts] (schema_ptr s,
                                   reader_permit permit,
                                   const dht::partition_range& range,
                                   const query::partition_slice& slice,
                                   const io_priority_class& pc,
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr) {
        return this->make_reader_excluding_sstables(std::move(s), std::move(permit), ssts, range, slice, pc, std::move(trace_state), fwd, fwd_mr);
    });
}
