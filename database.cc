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

#include "log.hh"
#include "lister.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/system_keyspace.hh"
#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "to_string.hh"
#include "query-result-writer.hh"
#include "cql3/column_identifier.hh"
#include "core/seastar.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/execution_stage.hh>
#include <seastar/util/defer.hh>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "sstables/sstables.hh"
#include "sstables/compaction.hh"
#include "sstables/remove.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include "locator/simple_snitch.hh"
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/adaptor/map.hpp>
#include "frozen_mutation.hh"
#include "mutation_partition_applier.hh"
#include "core/do_with.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "message/messaging_service.hh"
#include "mutation_query.hh"
#include <core/fstream.hh>
#include <seastar/core/enum.hh>
#include "utils/latency.hh"
#include "schema_registry.hh"
#include "service/priority_manager.hh"
#include "cell_locking.hh"
#include "db/view/row_locking.hh"
#include "view_info.hh"
#include "memtable-sstable.hh"
#include "db/schema_tables.hh"
#include "db/query_context.hh"
#include "sstables/compaction_manager.hh"
#include "sstables/compaction_backlog_manager.hh"
#include "sstables/progress_monitor.hh"
#include "auth/common.hh"
#include "tracing/trace_keyspace_helper.hh"

#include "checked-file-impl.hh"
#include "disk-error-handler.hh"

#include "db/timeout_clock.hh"

using namespace std::chrono_literals;

logging::logger dblog("database");

namespace {

sstables::sstable::version_types get_highest_supported_format() {
    if (service::get_local_storage_service().cluster_supports_mc_sstable()) {
        return sstables::sstable::version_types::mc;
    } else if (service::get_local_storage_service().cluster_supports_la_sstable()) {
        return sstables::sstable::version_types::la;
    } else {
        return sstables::sstable::version_types::ka;
    }
}

} /* anonymous namespace */

// Handles permit management only, used for situations where we don't want to inform
// the compaction manager about backlogs (i.e., tests)
class permit_monitor : public sstables::write_monitor {
    sstable_write_permit _permit;
public:
    permit_monitor(sstable_write_permit&& permit)
            : _permit(std::move(permit)) {
    }

    virtual void on_write_started(const sstables::writer_offset_tracker& t) override { }
    virtual void on_data_write_completed() override {
        // We need to start a flush before the current one finishes, otherwise
        // we'll have a period without significant disk activity when the current
        // SSTable is being sealed, the caches are being updated, etc. To do that,
        // we ensure the permit doesn't outlive this continuation.
        _permit = sstable_write_permit::unconditional();
    }
    virtual void on_write_completed() override { }
    virtual void on_flush_completed() override { }
};

// Handles all tasks related to sstable writing: permit management, compaction backlog updates, etc
class database_sstable_write_monitor : public permit_monitor, public backlog_write_progress_manager {
    sstables::shared_sstable _sst;
    compaction_manager& _compaction_manager;
    sstables::compaction_strategy& _compaction_strategy;
    const sstables::writer_offset_tracker* _tracker = nullptr;
    uint64_t _progress_seen = 0;
    api::timestamp_type _maximum_timestamp;
public:
    database_sstable_write_monitor(sstable_write_permit&& permit, sstables::shared_sstable sst, compaction_manager& manager,
                                   sstables::compaction_strategy& strategy, api::timestamp_type max_timestamp)
            : permit_monitor(std::move(permit))
            , _sst(std::move(sst))
            , _compaction_manager(manager)
            , _compaction_strategy(strategy)
            , _maximum_timestamp(max_timestamp)
    {}

    virtual void on_write_started(const sstables::writer_offset_tracker& t) override {
        _tracker = &t;
        _compaction_strategy.get_backlog_tracker().register_partially_written_sstable(_sst, *this);
    }

    virtual void on_data_write_completed() override {
        permit_monitor::on_data_write_completed();
        _progress_seen = _tracker->offset;
        _tracker = nullptr;
    }

    void write_failed() {
        _compaction_strategy.get_backlog_tracker().revert_charges(_sst);
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

static const std::unordered_set<sstring> system_keyspaces = {
                db::system_keyspace::NAME, db::schema_tables::NAME
};

bool is_system_keyspace(const sstring& name) {
    return system_keyspaces.find(name) != system_keyspaces.end();
}

static const std::unordered_set<sstring> internal_keyspaces = {
        db::system_distributed_keyspace::NAME,
        db::system_keyspace::NAME,
        db::schema_tables::NAME,
        auth::meta::AUTH_KS,
        tracing::trace_keyspace_helper::KEYSPACE_NAME
};

bool is_internal_keyspace(const sstring& name) {
    return internal_keyspaces.find(name) != internal_keyspaces.end();
}

// Used for tests where the CF exists without a database object. We need to pass a valid
// dirty_memory manager in that case.
thread_local dirty_memory_manager default_dirty_memory_manager;

lw_shared_ptr<memtable_list>
table::make_memory_only_memtable_list() {
    auto get_schema = [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(get_schema), _config.dirty_memory_manager, _config.memory_compaction_scheduling_group);
}

lw_shared_ptr<memtable_list>
table::make_memtable_list() {
    auto seal = [this] (flush_permit&& permit) {
        return seal_active_memtable(std::move(permit));
    };
    auto get_schema = [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(seal), std::move(get_schema), _config.dirty_memory_manager, _config.memory_compaction_scheduling_group);
}

lw_shared_ptr<memtable_list>
table::make_streaming_memtable_list() {
    auto seal = [this] (flush_permit&& permit) {
        return seal_active_streaming_memtable_immediate(std::move(permit));
    };
    auto get_schema =  [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(seal), std::move(get_schema), _config.streaming_dirty_memory_manager, _config.streaming_scheduling_group);
}

lw_shared_ptr<memtable_list>
table::make_streaming_memtable_big_list(streaming_memtable_big& smb) {
    auto seal = [this, &smb] (flush_permit&& permit) {
        return seal_active_streaming_memtable_big(smb, std::move(permit));
    };
    auto get_schema =  [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(seal), std::move(get_schema), _config.streaming_dirty_memory_manager, _config.streaming_scheduling_group);
}

table::table(schema_ptr schema, config config, db::commitlog* cl, compaction_manager& compaction_manager, cell_locker_stats& cl_stats, cache_tracker& row_cache_tracker)
    : _schema(std::move(schema))
    , _config(std::move(config))
    , _view_stats(sprint("%s_%s_view_replica_update", _schema->ks_name(), _schema->cf_name()))
    , _memtables(_config.enable_disk_writes ? make_memtable_list() : make_memory_only_memtable_list())
    , _streaming_memtables(_config.enable_disk_writes ? make_streaming_memtable_list() : make_memory_only_memtable_list())
    , _compaction_strategy(make_compaction_strategy(_schema->compaction_strategy(), _schema->compaction_strategy_options()))
    , _sstables(make_lw_shared(_compaction_strategy.make_sstable_set(_schema)))
    , _cache(_schema, sstables_as_snapshot_source(), row_cache_tracker, is_continuous::yes)
    , _commitlog(cl)
    , _compaction_manager(compaction_manager)
    , _index_manager(*this)
    , _counter_cell_locks(std::make_unique<cell_locker>(_schema, cl_stats))
    , _row_locker(_schema)
{
    if (!_config.enable_disk_writes) {
        dblog.warn("Writes disabled, column family no durable.");
    }
    set_metrics();
}

partition_presence_checker
table::make_partition_presence_checker(lw_shared_ptr<sstables::sstable_set> sstables) {
    auto sel = make_lw_shared(sstables->make_incremental_selector());
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
                const dht::partition_range& r,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding fwd_mr) {
            return make_sstable_reader(std::move(s), sst_set, r, slice, pc, std::move(trace_state), fwd, fwd_mr);
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
    for (auto m : *_streaming_memtables) {
        res += m->region().occupancy();
    }
    for (auto smb : _streaming_memtables_big) {
        for (auto m : *smb.second->memtables) {
            res += m->region().occupancy();
        }
    }
    return res;
}

static
bool belongs_to_current_shard(const dht::decorated_key& dk) {
    return dht::shard_of(dk.token()) == engine().cpu_id();
}

// Stores ranges for all components of the same clustering key, index 0 referring to component
// range 0, and so on.
using ck_filter_clustering_key_components = std::vector<nonwrapping_range<bytes_view>>;
// Stores an entry for each clustering key range specified by the filter.
using ck_filter_clustering_key_ranges = std::vector<ck_filter_clustering_key_components>;

// Used to split a clustering key range into a range for each component.
// If a range in ck_filtering_all_ranges is composite, a range will be created
// for each component. If it's not composite, a single range is created.
// This split is needed to check for overlap in each component individually.
static ck_filter_clustering_key_ranges
ranges_for_clustering_key_filter(const schema_ptr& schema, const query::clustering_row_ranges& ck_filtering_all_ranges) {
    ck_filter_clustering_key_ranges ranges;

    for (auto& r : ck_filtering_all_ranges) {
        // this vector stores a range for each component of a key, only one if not composite.
        ck_filter_clustering_key_components composite_ranges;

        if (r.is_full()) {
            ranges.push_back({ nonwrapping_range<bytes_view>::make_open_ended_both_sides() });
            continue;
        }
        auto start = r.start() ? r.start()->value().components() : clustering_key_prefix::make_empty().components();
        auto end = r.end() ? r.end()->value().components() : clustering_key_prefix::make_empty().components();
        auto start_it = start.begin();
        auto end_it = end.begin();

        // This test is enough because equal bounds in nonwrapping_range are inclusive.
        auto is_singular = [&schema] (const auto& type_it, const bytes_view& b1, const bytes_view& b2) {
            if (type_it == schema->clustering_key_type()->types().end()) {
                throw std::runtime_error(sprint("clustering key filter passed more components than defined in schema of %s.%s",
                    schema->ks_name(), schema->cf_name()));
            }
            return (*type_it)->compare(b1, b2) == 0;
        };
        auto type_it = schema->clustering_key_type()->types().begin();
        composite_ranges.reserve(schema->clustering_key_size());

        // the rule is to ignore any component cn if another component ck (k < n) is not if the form [v, v].
        // If we have [v1, v1], [v2, v2], ... {vl3, vr3}, ....
        // then we generate [v1, v1], [v2, v2], ... {vl3, vr3}. Where {  = '(' or '[', etc.
        while (start_it != start.end() && end_it != end.end() && is_singular(type_it++, *start_it, *end_it)) {
            composite_ranges.push_back(nonwrapping_range<bytes_view>({{ std::move(*start_it++), true }},
                {{ std::move(*end_it++), true }}));
        }
        // handle a single non-singular tail element, if present
        if (start_it != start.end() && end_it != end.end()) {
            composite_ranges.push_back(nonwrapping_range<bytes_view>({{ std::move(*start_it), r.start()->is_inclusive() }},
                {{ std::move(*end_it), r.end()->is_inclusive() }}));
        } else if (start_it != start.end()) {
            composite_ranges.push_back(nonwrapping_range<bytes_view>({{ std::move(*start_it), r.start()->is_inclusive() }}, {}));
        } else if (end_it != end.end()) {
            composite_ranges.push_back(nonwrapping_range<bytes_view>({}, {{ std::move(*end_it), r.end()->is_inclusive() }}));
        }

        ranges.push_back(std::move(composite_ranges));
    }
    return ranges;
}

// Return true if this sstable possibly stores clustering row(s) specified by ranges.
static inline bool
contains_rows(const sstables::sstable& sst, const schema_ptr& schema, const ck_filter_clustering_key_ranges& ranges) {
    auto& clustering_key_types = schema->clustering_key_type()->types();
    auto& clustering_components_ranges = sst.clustering_components_ranges();

    if (!schema->clustering_key_size() || clustering_components_ranges.empty()) {
        return true;
    }
    return boost::algorithm::any_of(ranges, [&] (const ck_filter_clustering_key_components& range) {
        auto s = std::min(range.size(), clustering_components_ranges.size());
        return boost::algorithm::all_of(boost::irange<unsigned>(0, s), [&] (unsigned i) {
            auto& type = clustering_key_types[i];
            return range[i].is_full() || range[i].overlaps(clustering_components_ranges[i], type->as_tri_comparator());
        });
    });
}

// Filter out sstables for reader using bloom filter and sstable metadata that keeps track
// of a range for each clustering component.
static std::vector<sstables::shared_sstable>
filter_sstable_for_reader(std::vector<sstables::shared_sstable>&& sstables, column_family& cf, const schema_ptr& schema,
        const sstables::key& key, const query::partition_slice& slice) {
    auto sstable_has_not_key = [&] (const sstables::shared_sstable& sst) {
        return !sst->filter_has_key(key);
    };
    sstables.erase(boost::remove_if(sstables, sstable_has_not_key), sstables.end());

    // FIXME: Workaround for https://github.com/scylladb/scylla/issues/3552
    // and https://github.com/scylladb/scylla/issues/3553
    const bool filtering_broken = true;

    // no clustering filtering is applied if schema defines no clustering key or
    // compaction strategy thinks it will not benefit from such an optimization.
    if (filtering_broken || !schema->clustering_key_size() || !cf.get_compaction_strategy().use_clustering_key_filter()) {
         return sstables;
    }
    ::cf_stats* stats = cf.cf_stats();
    stats->clustering_filter_count++;
    stats->sstables_checked_by_clustering_filter += sstables.size();

    auto ck_filtering_all_ranges = slice.get_all_ranges();
    // fast path to include all sstables if only one full range was specified.
    // For example, this happens if query only specifies a partition key.
    if (ck_filtering_all_ranges.size() == 1 && ck_filtering_all_ranges[0].is_full()) {
        stats->clustering_filter_fast_path_count++;
        stats->surviving_sstables_after_clustering_filter += sstables.size();
        return sstables;
    }
    auto ranges = ranges_for_clustering_key_filter(schema, ck_filtering_all_ranges);
    if (ranges.empty()) {
        return {};
    }

    int64_t min_timestamp = std::numeric_limits<int64_t>::max();
    auto sstable_has_clustering_key = [&min_timestamp, &schema, &ranges] (const sstables::shared_sstable& sst) {
        if (!contains_rows(*sst, schema, ranges)) {
            return false; // ordered after sstables that contain clustering rows.
        } else {
            min_timestamp = std::min(min_timestamp, sst->get_stats_metadata().min_timestamp);
            return true;
        }
    };
    auto sstable_has_relevant_tombstone = [&min_timestamp] (const sstables::shared_sstable& sst) {
        const auto& stats = sst->get_stats_metadata();
        // re-add sstable as candidate if it contains a tombstone that may cover a row in an included sstable.
        return (stats.max_timestamp > min_timestamp && stats.estimated_tombstone_drop_time.bin.size());
    };
    auto skipped = std::partition(sstables.begin(), sstables.end(), sstable_has_clustering_key);
    auto actually_skipped = std::partition(skipped, sstables.end(), sstable_has_relevant_tombstone);
    sstables.erase(actually_skipped, sstables.end());
    stats->surviving_sstables_after_clustering_filter += sstables.size();

    return sstables;
}

// Incremental selector implementation for combined_mutation_reader that
// selects readers on-demand as the read progresses through the token
// range.
class incremental_reader_selector : public reader_selector {
    const dht::partition_range* _pr;
    lw_shared_ptr<sstables::sstable_set> _sstables;
    tracing::trace_state_ptr _trace_state;
    sstables::sstable_set::incremental_selector _selector;
    std::unordered_set<sstables::shared_sstable> _read_sstables;
    sstable_reader_factory_type _fn;

    flat_mutation_reader create_reader(sstables::shared_sstable sst) {
        tracing::trace(_trace_state, "Reading partition range {} from sstable {}", *_pr, seastar::value_of([&sst] { return sst->get_filename(); }));
        return _fn(sst, *_pr);
    }

public:
    explicit incremental_reader_selector(schema_ptr s,
            lw_shared_ptr<sstables::sstable_set> sstables,
            const dht::partition_range& pr,
            tracing::trace_state_ptr trace_state,
            sstable_reader_factory_type fn)
        : reader_selector(s, pr.start() ? pr.start()->value() : dht::ring_position_view::min())
        , _pr(&pr)
        , _sstables(std::move(sstables))
        , _trace_state(std::move(trace_state))
        , _selector(_sstables->make_incremental_selector())
        , _fn(std::move(fn)) {

        dblog.trace("incremental_reader_selector {}: created for range: {} with {} sstables",
                this,
                *_pr,
                _sstables->all()->size());
    }

    incremental_reader_selector(const incremental_reader_selector&) = delete;
    incremental_reader_selector& operator=(const incremental_reader_selector&) = delete;

    incremental_reader_selector(incremental_reader_selector&&) = delete;
    incremental_reader_selector& operator=(incremental_reader_selector&&) = delete;

    virtual std::vector<flat_mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>& pos) override {
        dblog.trace("incremental_reader_selector {}: {}({})", this, __FUNCTION__, seastar::lazy_deref(pos));

        auto readers = std::vector<flat_mutation_reader>();

        do {
            auto selection = _selector.select(_selector_position);
            _selector_position = selection.next_position;

            dblog.trace("incremental_reader_selector {}: {} sstables to consider, advancing selector to {}", this, selection.sstables.size(),
                    _selector_position);

            readers = boost::copy_range<std::vector<flat_mutation_reader>>(selection.sstables
                    | boost::adaptors::filtered([this] (auto& sst) { return _read_sstables.emplace(sst).second; })
                    | boost::adaptors::transformed([this] (auto& sst) { return this->create_reader(sst); }));
        } while (!_selector_position.is_max() && readers.empty() && (!pos || dht::ring_position_tri_compare(*_s, *pos, _selector_position) >= 0));

        dblog.trace("incremental_reader_selector {}: created {} new readers", this, readers.size());

        return readers;
    }

    virtual std::vector<flat_mutation_reader> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        _pr = &pr;

        auto pos = dht::ring_position_view::for_range_start(*_pr);
        if (dht::ring_position_tri_compare(*_s, pos, _selector_position) >= 0) {
            return create_new_readers(pos);
        }

        return {};
    }
};

static flat_mutation_reader
create_single_key_sstable_reader(column_family* cf,
                                 schema_ptr schema,
                                 lw_shared_ptr<sstables::sstable_set> sstables,
                                 utils::estimated_histogram& sstable_histogram,
                                 const dht::partition_range& pr, // must be singular
                                 const query::partition_slice& slice,
                                 const io_priority_class& pc,
                                 reader_resource_tracker resource_tracker,
                                 tracing::trace_state_ptr trace_state,
                                 streamed_mutation::forwarding fwd,
                                 mutation_reader::forwarding fwd_mr)
{
    auto key = sstables::key::from_partition_key(*schema, *pr.start()->value().key());
    auto readers = boost::copy_range<std::vector<flat_mutation_reader>>(
        filter_sstable_for_reader(sstables->select(pr), *cf, schema, key, slice)
        | boost::adaptors::transformed([&] (const sstables::shared_sstable& sstable) {
            tracing::trace(trace_state, "Reading key {} from sstable {}", pr, seastar::value_of([&sstable] { return sstable->get_filename(); }));
            return sstable->read_row_flat(schema, pr.start()->value(), slice, pc, resource_tracker, fwd);
        })
    );
    if (readers.empty()) {
        return make_empty_flat_reader(schema);
    }
    sstable_histogram.add(readers.size());
    return make_combined_reader(schema, std::move(readers), fwd, fwd_mr);
}

flat_mutation_reader
table::make_sstable_reader(schema_ptr s,
                                   lw_shared_ptr<sstables::sstable_set> sstables,
                                   const dht::partition_range& pr,
                                   const query::partition_slice& slice,
                                   const io_priority_class& pc,
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr) const {
    auto* semaphore = service::get_local_streaming_read_priority().id() == pc.id()
        ? _config.streaming_read_concurrency_semaphore
        : _config.read_concurrency_semaphore;

    // CAVEAT: if make_sstable_reader() is called on a single partition
    // we want to optimize and read exactly this partition. As a
    // consequence, fast_forward_to() will *NOT* work on the result,
    // regardless of what the fwd_mr parameter says.
    if (pr.is_singular() && pr.start()->value().has_key()) {
        const dht::ring_position& pos = pr.start()->value();
        if (dht::shard_of(pos.token()) != engine().cpu_id()) {
            return make_empty_flat_reader(s); // range doesn't belong to this shard
        }

        if (semaphore) {
            auto ms = mutation_source([semaphore, this, sstables=std::move(sstables)] (
                        schema_ptr s,
                        const dht::partition_range& pr,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        mutation_reader::forwarding fwd_mr,
                        reader_resource_tracker tracker) {
                    return create_single_key_sstable_reader(const_cast<column_family*>(this), std::move(s), std::move(sstables),
                                _stats.estimated_sstable_per_read, pr, slice, pc, tracker, std::move(trace_state), fwd, fwd_mr);
                });
            return make_restricted_flat_reader(*semaphore, std::move(ms), std::move(s), pr, slice, pc, std::move(trace_state), fwd, fwd_mr);
        } else {
            return create_single_key_sstable_reader(const_cast<column_family*>(this), std::move(s), std::move(sstables),
                        _stats.estimated_sstable_per_read, pr, slice, pc, no_resource_tracking(), std::move(trace_state), fwd, fwd_mr);
        }
    } else {
        if (semaphore) {
            auto ms = mutation_source([semaphore, sstables=std::move(sstables)] (
                        schema_ptr s,
                        const dht::partition_range& pr,
                        const query::partition_slice& slice,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        streamed_mutation::forwarding fwd,
                        mutation_reader::forwarding fwd_mr,
                        reader_resource_tracker tracker) {
                    return make_local_shard_sstable_reader(std::move(s), std::move(sstables), pr, slice, pc,
                        tracker, std::move(trace_state), fwd, fwd_mr);
                });
            return make_restricted_flat_reader(*semaphore, std::move(ms), std::move(s), pr, slice, pc, std::move(trace_state), fwd, fwd_mr);
        } else {
            return make_local_shard_sstable_reader(std::move(s), std::move(sstables), pr, slice, pc,
                no_resource_tracking(), std::move(trace_state), fwd, fwd_mr);
        }
    }
}

// Exposed for testing, not performance critical.
future<table::const_mutation_partition_ptr>
table::find_partition(schema_ptr s, const dht::decorated_key& key) const {
    return do_with(dht::partition_range::make_singular(key), [s = std::move(s), this] (auto& range) {
        return do_with(this->make_reader(s, range), [s] (flat_mutation_reader& reader) {
            return read_mutation_from_flat_mutation_reader(reader, db::no_timeout).then([] (mutation_opt&& mo) -> std::unique_ptr<const mutation_partition> {
                if (!mo) {
                    return {};
                }
                return std::make_unique<const mutation_partition>(std::move(mo->partition()));
            });
        });
    });
}

future<table::const_mutation_partition_ptr>
table::find_partition_slow(schema_ptr s, const partition_key& key) const {
    return find_partition(s, dht::global_partitioner().decorate_key(*s, key));
}

future<table::const_row_ptr>
table::find_row(schema_ptr s, const dht::decorated_key& partition_key, clustering_key clustering_key) const {
    return find_partition(s, partition_key).then([clustering_key = std::move(clustering_key), s] (const_mutation_partition_ptr p) {
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
                           const dht::partition_range& range,
                           const query::partition_slice& slice,
                           const io_priority_class& pc,
                           tracing::trace_state_ptr trace_state,
                           streamed_mutation::forwarding fwd,
                           mutation_reader::forwarding fwd_mr) const {
    if (_virtual_reader) {
        return (*_virtual_reader).make_reader(s, range, slice, pc, trace_state, fwd, fwd_mr);
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
        readers.emplace_back(mt->make_flat_reader(s, range, slice, pc, trace_state, fwd, fwd_mr));
    }

    if (_config.enable_cache) {
        readers.emplace_back(_cache.make_reader(s, range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    } else {
        readers.emplace_back(make_sstable_reader(s, _sstables, range, slice, pc, std::move(trace_state), fwd, fwd_mr));
    }

    return make_combined_reader(s, std::move(readers), fwd, fwd_mr);
}

sstables::shared_sstable table::make_streaming_sstable_for_write(std::optional<sstring> subdir) {
    sstring dir = _config.datadir;
    if (subdir) {
        dir += "/" + *subdir;
    }
    auto newtab = sstables::make_sstable(_schema,
            dir, calculate_generation_for_new_table(),
            get_highest_supported_format(),
            sstables::sstable::format_types::big);
    dblog.debug("Created sstable for streaming: ks={}, cf={}, dir={}", schema()->ks_name(), schema()->cf_name(), dir);
    return newtab;
}

flat_mutation_reader
table::make_streaming_reader(schema_ptr s,
                           const dht::partition_range_vector& ranges) const {
    auto& slice = s->full_slice();
    auto& pc = service::get_local_streaming_read_priority();

    auto source = mutation_source([this] (schema_ptr s, const dht::partition_range& range, const query::partition_slice& slice,
                                      const io_priority_class& pc, tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd, mutation_reader::forwarding fwd_mr) {
        std::vector<flat_mutation_reader> readers;
        readers.reserve(_memtables->size() + 1);
        for (auto&& mt : *_memtables) {
            readers.emplace_back(mt->make_flat_reader(s, range, slice, pc, trace_state, fwd, fwd_mr));
        }
        readers.emplace_back(make_sstable_reader(s, _sstables, range, slice, pc, std::move(trace_state), fwd, fwd_mr));
        return make_combined_reader(s, std::move(readers), fwd, fwd_mr);
    });

    return make_flat_multi_range_reader(s, std::move(source), ranges, slice, pc, nullptr, mutation_reader::forwarding::no);
}

future<std::vector<locked_cell>> table::lock_counter_cells(const mutation& m, db::timeout_clock::time_point timeout) {
    assert(m.schema() == _counter_cell_locks->schema());
    return _counter_cell_locks->lock_cells(m.decorated_key(), partition_cells_range(m.partition()), timeout);
}

// Not performance critical. Currently used for testing only.
template <typename Func>
future<bool>
table::for_all_partitions(schema_ptr s, Func&& func) const {
    static_assert(std::is_same<bool, std::result_of_t<Func(const dht::decorated_key&, const mutation_partition&)>>::value,
                  "bad Func signature");

    struct iteration_state {
        flat_mutation_reader reader;
        Func func;
        bool ok = true;
        bool empty = false;
    public:
        bool done() const { return !ok || empty; }
        iteration_state(schema_ptr s, const column_family& cf, Func&& func)
            : reader(cf.make_reader(std::move(s)))
            , func(std::move(func))
        { }
    };

    return do_with(iteration_state(std::move(s), *this, std::move(func)), [] (iteration_state& is) {
        return do_until([&is] { return is.done(); }, [&is] {
            return read_mutation_from_flat_mutation_reader(is.reader, db::no_timeout).then([&is](mutation_opt&& mo) {
                if (!mo) {
                    is.empty = true;
                } else {
                    is.ok = is.func(mo->decorated_key(), mo->partition());
                }
            });
        }).then([&is] {
            return is.ok;
        });
    });
}

future<bool>
table::for_all_partitions_slow(schema_ptr s, std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const {
    return for_all_partitions(std::move(s), std::move(func));
}

static bool belongs_to_current_shard(const std::vector<shard_id>& shards) {
    return boost::find(shards, engine().cpu_id()) != shards.end();
}

static bool belongs_to_other_shard(const std::vector<shard_id>& shards) {
    return shards.size() != size_t(belongs_to_current_shard(shards));
}

future<sstables::shared_sstable>
table::open_sstable(sstables::foreign_sstable_open_info info, sstring dir, int64_t generation,
        sstables::sstable::version_types v, sstables::sstable::format_types f) {
    auto sst = sstables::make_sstable(_schema, dir, generation, v, f);
    if (!belongs_to_current_shard(info.owners)) {
        dblog.debug("sstable {} not relevant for this shard, ignoring", sst->get_filename());
        return make_ready_future<sstables::shared_sstable>();
    }
    if (!belongs_to_other_shard(info.owners)) {
        sst->set_unshared();
    }
    return sst->load(std::move(info)).then([sst] () mutable {
        return make_ready_future<sstables::shared_sstable>(std::move(sst));
    });
}

void table::load_sstable(sstables::shared_sstable& sst, bool reset_level) {
    if (schema()->is_counter() && !sst->has_scylla_component()) {
        throw std::runtime_error("Loading non-Scylla SSTables containing counters is not supported. Use sstableloader instead.");
    }
    auto& shards = sst->get_shards_for_this_sstable();
    if (belongs_to_other_shard(shards)) {
        // If we're here, this sstable is shared by this and other
        // shard(s). Shared sstables cannot be deleted until all
        // shards compacted them, so to reduce disk space usage we
        // want to start splitting them now.
        // However, we need to delay this compaction until we read all
        // the sstables belonging to this CF, because we need all of
        // them to know which tombstones we can drop, and what
        // generation number is free.
        _sstables_need_rewrite.emplace(sst->generation(), sst);
    }
    if (reset_level) {
        // When loading a migrated sstable, set level to 0 because
        // it may overlap with existing tables in levels > 0.
        // This step is optional, because even if we didn't do this
        // scylla would detect the overlap, and bring back some of
        // the sstables to level 0.
        sst->set_sstable_level(0);
    }
    add_sstable(sst, std::move(shards));
}

void table::update_stats_for_new_sstable(uint64_t disk_space_used_by_sstable, const std::vector<unsigned>& shards_for_the_sstable) noexcept {
    assert(!shards_for_the_sstable.empty());
    if (*boost::min_element(shards_for_the_sstable) == engine().cpu_id()) {
        _stats.live_disk_space_used += disk_space_used_by_sstable;
        _stats.total_disk_space_used += disk_space_used_by_sstable;
        _stats.live_sstable_count++;
    }
}

void table::add_sstable(sstables::shared_sstable sstable, const std::vector<unsigned>& shards_for_the_sstable) {
    // allow in-progress reads to continue using old list
    auto new_sstables = make_lw_shared(*_sstables);
    new_sstables->insert(sstable);
    _sstables = std::move(new_sstables);
    update_stats_for_new_sstable(sstable->bytes_on_disk(), shards_for_the_sstable);
    if (sstable->is_staging()) {
        _sstables_staging.emplace(sstable->generation(), sstable);
    } else {
        _compaction_strategy.get_backlog_tracker().add_sstable(sstable);
    }
}

future<>
table::add_sstable_and_update_cache(sstables::shared_sstable sst) {
    return get_row_cache().invalidate([this, sst] () noexcept {
        // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
        // atomically load all opened sstables into column family.
        add_sstable(sst, {engine().cpu_id()});
        trigger_compaction();
    }, dht::partition_range::make({sst->get_first_decorated_key(), true}, {sst->get_last_decorated_key(), true}));
}

future<>
table::update_cache(lw_shared_ptr<memtable> m, sstables::shared_sstable sst) {
    auto adder = [this, m, sst] {
        auto newtab_ms = sst->as_mutation_source();
        add_sstable(sst, {engine().cpu_id()});
        m->mark_flushed(std::move(newtab_ms));
        try_trigger_compaction();
    };
    if (_config.enable_cache) {
        return _cache.update(adder, *m);
    } else {
        adder();
        return m->clear_gently();
    }
}

future<>
table::seal_active_streaming_memtable_immediate(flush_permit&& permit) {
  return with_scheduling_group(_config.streaming_scheduling_group, [this, permit = std::move(permit)] () mutable {
    auto old = _streaming_memtables->back();
    if (old->empty()) {
        return make_ready_future<>();
    }
    _streaming_memtables->add_memtable();
    _streaming_memtables->erase(old);

    dblog.debug("Sealing streaming memtable of {}.{}, partitions: {}, occupancy: {}", _schema->ks_name(), _schema->cf_name(), old->partition_count(), old->occupancy());

    auto guard = _streaming_flush_phaser.start();
    return with_gate(_streaming_flush_gate, [this, old, permit = std::move(permit)] () mutable {
        return with_lock(_sstables_lock.for_read(), [this, old, permit = std::move(permit)] () mutable {
            auto newtab = sstables::make_sstable(_schema,
                _config.datadir, calculate_generation_for_new_table(),
                get_highest_supported_format(),
                sstables::sstable::format_types::big);

            newtab->set_unshared();

            dblog.debug("Flushing to {}", newtab->get_filename());

            // This is somewhat similar to the main memtable flush, but with important differences.
            //
            // The first difference, is that we don't keep aggregate collectd statistics about this one.
            // If we ever need to, we'll keep them separate statistics, but we don't want to polute the
            // main stats about memtables with streaming memtables.
            //
            // Lastly, we don't have any commitlog RP to update, and we don't need to deal manipulate the
            // memtable list, since this memtable was not available for reading up until this point.
            auto fp = permit.release_sstable_write_permit();
            database_sstable_write_monitor monitor(std::move(fp), newtab, _compaction_manager, _compaction_strategy, old->get_max_timestamp());
            return do_with(std::move(monitor), [this, newtab, old, permit = std::move(permit)] (auto& monitor) mutable {
                auto&& priority = service::get_local_streaming_write_priority();
                return write_memtable_to_sstable(*old, newtab, monitor, get_large_partition_handler(), incremental_backups_enabled(), priority, false).then([this, newtab, old] {
                    return newtab->open_data();
                }).then([this, old, newtab] () {
                    return with_scheduling_group(_config.memtable_to_cache_scheduling_group, [this, newtab, old] {
                      auto adder = [this, newtab] {
                          add_sstable(newtab, {engine().cpu_id()});
                          try_trigger_compaction();
                          dblog.debug("Flushing to {} done", newtab->get_filename());
                      };
                      if (_config.enable_cache) {
                        return _cache.update_invalidating(adder, *old);
                      } else {
                        adder();
                        return old->clear_gently();
                      }
                    });
                }).handle_exception([old, permit = std::move(permit), &monitor, newtab] (auto ep) {
                    monitor.write_failed();
                    newtab->mark_for_deletion();
                    dblog.error("failed to write streamed sstable: {}", ep);
                    return make_exception_future<>(ep);
                });
            });
            // We will also not have any retry logic. If we fail here, we'll fail the streaming and let
            // the upper layers know. They can then apply any logic they want here.
        });
    }).finally([guard = std::move(guard)] { });
  });
}

future<> table::seal_active_streaming_memtable_big(streaming_memtable_big& smb, flush_permit&& permit) {
  return with_scheduling_group(_config.streaming_scheduling_group, [this, &smb, permit = std::move(permit)] () mutable {
    auto old = smb.memtables->back();
    if (old->empty()) {
        return make_ready_future<>();
    }
    smb.memtables->add_memtable();
    smb.memtables->erase(old);
    return with_gate(_streaming_flush_gate, [this, old, &smb, permit = std::move(permit)] () mutable {
        return with_gate(smb.flush_in_progress, [this, old, &smb, permit = std::move(permit)] () mutable {
            return with_lock(_sstables_lock.for_read(), [this, old, &smb, permit = std::move(permit)] () mutable {
                auto newtab = sstables::make_sstable(_schema,
                                                     _config.datadir, calculate_generation_for_new_table(),
                                                     get_highest_supported_format(),
                                                     sstables::sstable::format_types::big);

                newtab->set_unshared();

                auto fp = permit.release_sstable_write_permit();
                auto monitor = std::make_unique<database_sstable_write_monitor>(std::move(fp), newtab, _compaction_manager, _compaction_strategy, old->get_max_timestamp());
                auto&& priority = service::get_local_streaming_write_priority();
                auto fut = write_memtable_to_sstable(*old, newtab, *monitor, get_large_partition_handler(), incremental_backups_enabled(), priority, true);
                return fut.then_wrapped([this, newtab, old, &smb, permit = std::move(permit), monitor = std::move(monitor)] (future<> f) mutable {
                    if (!f.failed()) {
                        smb.sstables.push_back(monitored_sstable{std::move(monitor), newtab});
                        return make_ready_future<>();
                    } else {
                        monitor->write_failed();
                        newtab->mark_for_deletion();
                        auto ep = f.get_exception();
                        dblog.error("failed to write streamed sstable: {}", ep);
                        return make_exception_future<>(ep);
                    }
                });
            });
        });
    });
  });
}

future<>
table::seal_active_memtable(flush_permit&& permit) {
    auto old = _memtables->back();
    dblog.debug("Sealing active memtable of {}.{}, partitions: {}, occupancy: {}", _schema->ks_name(), _schema->cf_name(), old->partition_count(), old->occupancy());

    if (old->empty()) {
        dblog.debug("Memtable is empty");
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
            return with_lock(_sstables_lock.for_read(), [this, old, sstable_write_permit = std::move(sstable_write_permit)] () mutable {
                return this->try_flush_memtable_to_sstable(old, std::move(sstable_write_permit));
            }).then([this, &permit] (auto should_stop) mutable {
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
    }).then([this, memtable_size, old, op = std::move(op), previous_flush = std::move(previous_flush)] () mutable {
        _stats.pending_flushes--;
        _config.cf_stats->pending_memtables_flushes_count--;
        _config.cf_stats->pending_memtables_flushes_bytes -= memtable_size;

        if (_commitlog) {
            _commitlog->discard_completed_segments(_schema->id(), old->rp_set());
        }
        return previous_flush.finally([op = std::move(op)] { });
    });
    // FIXME: release commit log
    // FIXME: provide back-pressure to upper layers
}

future<stop_iteration>
table::try_flush_memtable_to_sstable(lw_shared_ptr<memtable> old, sstable_write_permit&& permit) {
  return with_scheduling_group(_config.memtable_scheduling_group, [this, old = std::move(old), permit = std::move(permit)] () mutable {
    auto gen = calculate_generation_for_new_table();

    auto newtab = sstables::make_sstable(_schema,
        _config.datadir, gen,
        get_highest_supported_format(),
        sstables::sstable::format_types::big);

    newtab->set_unshared();
    dblog.debug("Flushing to {}", newtab->get_filename());
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
    database_sstable_write_monitor monitor(std::move(permit), newtab, _compaction_manager, _compaction_strategy, old->get_max_timestamp());
    return do_with(std::move(monitor), [this, old, newtab] (auto& monitor) {
        auto&& priority = service::get_local_memtable_flush_priority();
        auto f = write_memtable_to_sstable(*old, newtab, monitor, get_large_partition_handler(), incremental_backups_enabled(), priority, false);
        // Switch back to default scheduling group for post-flush actions, to avoid them being staved by the memtable flush
        // controller. Cache update does not affect the input of the memtable cpu controller, so it can be subject to
        // priority inversion.
        return with_scheduling_group(default_scheduling_group(), [this, &monitor, old = std::move(old), newtab = std::move(newtab), f = std::move(f)] () mutable {
            return f.then([this, newtab, old, &monitor] {
                return newtab->open_data().then([this, old, newtab] () {
                    dblog.debug("Flushing to {} done", newtab->get_filename());
                    return with_scheduling_group(_config.memtable_to_cache_scheduling_group, [this, old, newtab] {
                        return update_cache(old, newtab);
                    });
                }).then([this, old, newtab] () noexcept {
                    _memtables->erase(old);
                    dblog.debug("Memtable for {} replaced", newtab->get_filename());
                    return stop_iteration::yes;
                });
            }).handle_exception([this, old, newtab, &monitor] (auto e) {
                monitor.write_failed();
                newtab->mark_for_deletion();
                dblog.error("failed to write sstable {}: {}", newtab->get_filename(), e);
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
    return _async_gate.close().then([this] {
        return when_all(await_pending_writes(), await_pending_reads()).discard_result().finally([this] {
            return when_all(_memtables->request_flush(), _streaming_memtables->request_flush()).discard_result().finally([this] {
                return _compaction_manager.remove(this).then([this] {
                    // Nest, instead of using when_all, so we don't lose any exceptions.
                    return _streaming_flush_gate.close();
                }).then([this] {
                    return _sstable_deletion_gate.close();
                });
            });
        });
    });
}

static io_error_handler error_handler_for_upload_dir() {
    return [] (std::exception_ptr eptr) {
        // do nothing about sstable exception and caller will just rethrow it.
    };
}

// This function will iterate through upload directory in column family,
// and will do the following for each sstable found:
// 1) Mutate sstable level to 0.
// 2) Create hard links to its components in column family dir.
// 3) Remove all of its components in upload directory.
// At the end, it's expected that upload dir is empty and all of its
// previous content was moved to column family dir.
//
// Return a vector containing descriptor of sstables to be loaded.
future<std::vector<sstables::entry_descriptor>>
distributed_loader::flush_upload_dir(distributed<database>& db, sstring ks_name, sstring cf_name) {
    struct work {
        std::unordered_map<int64_t, sstables::entry_descriptor> descriptors;
        std::vector<sstables::entry_descriptor> flushed;
    };

    return do_with(work(), [&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] (work& work) {
        auto& cf = db.local().find_column_family(ks_name, cf_name);

        return lister::scan_dir(lister::path(cf._config.datadir) / "upload", { directory_entry_type::regular },
                [&work] (lister::path parent_dir, directory_entry de) {
            auto comps = sstables::entry_descriptor::make_descriptor(parent_dir.native(), de.name);
            if (comps.component != component_type::TOC) {
                return make_ready_future<>();
            }
            work.descriptors.emplace(comps.generation, std::move(comps));
            return make_ready_future<>();
        }, &column_family::manifest_json_filter).then([&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name), &work] {
            work.flushed.reserve(work.descriptors.size());

            return do_for_each(work.descriptors, [&db, ks_name, cf_name, &work] (auto& pair) {
                return db.invoke_on(column_family::calculate_shard_from_sstable_generation(pair.first),
                        [ks_name, cf_name, &work, comps = pair.second] (database& db) {
                    auto& cf = db.find_column_family(ks_name, cf_name);

                    auto sst = sstables::make_sstable(cf.schema(), cf._config.datadir + "/upload", comps.generation,
                        comps.version, comps.format, gc_clock::now(),
                        [] (disk_error_signal_type&) { return error_handler_for_upload_dir(); });
                    auto gen = cf.calculate_generation_for_new_table();

                    // Read toc content as it will be needed for moving and deleting a sstable.
                    return sst->read_toc().then([sst, s = cf.schema()] {
                        if (s->is_counter() && !sst->has_scylla_component()) {
                            return make_exception_future<>(std::runtime_error("Loading non-Scylla SSTables containing counters is not supported. Use sstableloader instead."));
                        }
                        return sst->mutate_sstable_level(0);
                    }).then([&cf, sst, gen] {
                        return sst->create_links(cf._config.datadir, gen);
                    }).then([sst] {
                        return sstables::remove_by_toc_name(sst->toc_filename(), error_handler_for_upload_dir());
                    }).then([sst, &cf, gen, comps = comps, &work] () mutable {
                        comps.generation = gen;
                        comps.sstdir = cf._config.datadir;
                        return make_ready_future<sstables::entry_descriptor>(std::move(comps));
                    });
                }).then([&work] (sstables::entry_descriptor comps) mutable {
                    work.flushed.push_back(std::move(comps));
                    return make_ready_future<>();
                });
            });
        }).then([&work] {
            return make_ready_future<std::vector<sstables::entry_descriptor>>(std::move(work.flushed));
        });
    });
}

future<std::vector<sstables::entry_descriptor>>
table::reshuffle_sstables(std::set<int64_t> all_generations, int64_t start) {
    struct work {
        int64_t current_gen;
        std::set<int64_t> all_generations; // Stores generation of all live sstables in the system.
        std::map<int64_t, sstables::shared_sstable> sstables;
        std::unordered_map<int64_t, sstables::entry_descriptor> descriptors;
        std::vector<sstables::entry_descriptor> reshuffled;
        work(int64_t start, std::set<int64_t> gens)
            : current_gen(start ? start : 1)
            , all_generations(gens) {}
    };

    return do_with(work(start, std::move(all_generations)), [this] (work& work) {
        return lister::scan_dir(_config.datadir, { directory_entry_type::regular }, [this, &work] (lister::path parent_dir, directory_entry de) {
            auto comps = sstables::entry_descriptor::make_descriptor(parent_dir.native(), de.name);
            if (comps.component != component_type::TOC) {
                return make_ready_future<>();
            }
            // Skip generations that were already loaded by Scylla at a previous stage.
            if (work.all_generations.count(comps.generation) != 0) {
                return make_ready_future<>();
            }
            auto sst = sstables::make_sstable(_schema,
                                                         _config.datadir, comps.generation,
                                                         comps.version, comps.format);
            work.sstables.emplace(comps.generation, std::move(sst));
            work.descriptors.emplace(comps.generation, std::move(comps));
            // FIXME: This is the only place in which we actually issue disk activity aside from
            // directory metadata operations.
            //
            // But without the TOC information, we don't know which files we should link.
            // The alternative to that would be to change create link to try creating a
            // link for all possible files and handling the failures gracefuly, but that's not
            // exactly fast either.
            //
            // Those SSTables are not known by anyone in the system. So we don't have any kind of
            // object describing them. There isn't too much of a choice.
            return work.sstables[comps.generation]->read_toc();
        }, &manifest_json_filter).then([&work] {
            // Note: cannot be parallel because we will be shuffling things around at this stage. Can't race.
            return do_for_each(work.sstables, [&work] (auto& pair) {
                auto&& comps = std::move(work.descriptors.at(pair.first));
                comps.generation = work.current_gen;
                work.reshuffled.push_back(std::move(comps));

                if (pair.first == work.current_gen) {
                    ++work.current_gen;
                    return make_ready_future<>();
                }
                return pair.second->set_generation(work.current_gen++);
            });
        }).then([&work] {
            return make_ready_future<std::vector<sstables::entry_descriptor>>(std::move(work.reshuffled));
        });
    });
}

seastar::metrics::label column_family_label("cf");
seastar::metrics::label keyspace_label("ks");
void table::set_metrics() {
    auto cf = column_family_label(_schema->cf_name());
    auto ks = keyspace_label(_schema->ks_name());
    namespace ms = seastar::metrics;
    if (_config.enable_metrics_reporting) {
        _metrics.add_group("column_family", {
                ms::make_derive("memtable_switch", ms::description("Number of times flush has resulted in the memtable being switched out"), _stats.memtable_switch_count)(cf)(ks),
                ms::make_gauge("pending_tasks", ms::description("Estimated number of tasks pending for this column family"), _stats.pending_flushes)(cf)(ks),
                ms::make_gauge("live_disk_space", ms::description("Live disk space used"), _stats.live_disk_space_used)(cf)(ks),
                ms::make_gauge("total_disk_space", ms::description("Total disk space used"), _stats.total_disk_space_used)(cf)(ks),
                ms::make_gauge("live_sstable", ms::description("Live sstable count"), _stats.live_sstable_count)(cf)(ks),
                ms::make_gauge("pending_compaction", ms::description("Estimated number of compactions pending for this column family"), _stats.pending_compactions)(cf)(ks)
        });

        // Metrics related to row locking
        auto add_row_lock_metrics = [this, ks, cf] (row_locker::single_lock_stats& stats, sstring stat_name) {
            _metrics.add_group("column_family", {
                ms::make_total_operations(sprint("row_lock_%s_acquisitions", stat_name), stats.lock_acquisitions, ms::description(sprint("Row lock acquisitions for %s lock", stat_name)))(cf)(ks),
                ms::make_queue_length(sprint("row_lock_%s_operations_currently_waiting_for_lock", stat_name), stats.operations_currently_waiting_for_lock, ms::description(sprint("Operations currently waiting for %s lock", stat_name)))(cf)(ks),
                ms::make_histogram(sprint("row_lock_%s_waiting_time", stat_name), ms::description(sprint("Histogram representing time that operations spent on waiting for %s lock", stat_name)),
                        [&stats] {return stats.estimated_waiting_for_lock.get_histogram(std::chrono::microseconds(100));})(cf)(ks)
            });
        };
        add_row_lock_metrics(_row_locker_stats.exclusive_row, "exclusive_row");
        add_row_lock_metrics(_row_locker_stats.shared_row, "shared_row");
        add_row_lock_metrics(_row_locker_stats.exclusive_partition, "exclusive_partition");
        add_row_lock_metrics(_row_locker_stats.shared_partition, "shared_partition");

        // View metrics are created only for base tables, so there's no point in adding them to views (which cannot act as base tables for other views)
        if (!_schema->is_view()) {
            _metrics.add_group("column_family", {
                    ms::make_total_operations("view_updates_pushed_remote", _view_stats.view_updates_pushed_remote, ms::description("Number of updates (mutations) pushed to remote view replicas"))(cf)(ks),
                    ms::make_total_operations("view_updates_failed_remote", _view_stats.view_updates_failed_remote, ms::description("Number of updates (mutations) that failed to be pushed to remote view replicas"))(cf)(ks),
                    ms::make_total_operations("view_updates_pushed_local", _view_stats.view_updates_pushed_local, ms::description("Number of updates (mutations) pushed to local view replicas"))(cf)(ks),
                    ms::make_total_operations("view_updates_failed_local", _view_stats.view_updates_failed_local, ms::description("Number of updates (mutations) that failed to be pushed to local view replicas"))(cf)(ks),
            });
        }

        if (_schema->ks_name() != db::system_keyspace::NAME && _schema->ks_name() != db::schema_tables::v3::NAME && _schema->ks_name() != "system_traces") {
            _metrics.add_group("column_family", {
                    ms::make_histogram("read_latency", ms::description("Read latency histogram"), [this] {return _stats.estimated_read.get_histogram(std::chrono::microseconds(100));})(cf)(ks),
                    ms::make_histogram("write_latency", ms::description("Write latency histogram"), [this] {return _stats.estimated_write.get_histogram(std::chrono::microseconds(100));})(cf)(ks),
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

    for (auto&& tab : boost::range::join(_sstables_compacted_but_not_deleted,
                    // this might seem dangerous, but "move" here just avoids constness,
                    // making the two ranges compatible when compiling with boost 1.55.
                    // Noone is actually moving anything...
                                         std::move(*_sstables->all()))) {
        update_stats_for_new_sstable(tab->bytes_on_disk(), tab->get_shards_for_this_sstable());
    }
}

void
table::rebuild_sstable_list(const std::vector<sstables::shared_sstable>& new_sstables,
                                    const std::vector<sstables::shared_sstable>& old_sstables) {
    auto current_sstables = _sstables;
    auto new_sstable_list = _compaction_strategy.make_sstable_set(_schema);

    std::unordered_set<sstables::shared_sstable> s(old_sstables.begin(), old_sstables.end());

    // this might seem dangerous, but "move" here just avoids constness,
    // making the two ranges compatible when compiling with boost 1.55.
    // Noone is actually moving anything...
    for (auto&& tab : boost::range::join(new_sstables, std::move(*current_sstables->all()))) {
        if (!s.count(tab)) {
            new_sstable_list.insert(tab);
        }
    }
    _sstables = make_lw_shared(std::move(new_sstable_list));
}

void
table::on_compaction_completion(const std::vector<sstables::shared_sstable>& new_sstables,
                                    const std::vector<sstables::shared_sstable>& sstables_to_remove) {
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
    for (auto& sst : sstables_to_remove) {
        auto shards = sst->get_shards_for_this_sstable();
        if (shards.size() > 1) {
            throw std::runtime_error(sprint("A regular compaction for %s.%s INCORRECTLY used shared sstable %s. Only resharding work with those!",
                _schema->ks_name(), _schema->cf_name(), sst->toc_filename()));
        }
        if (!belongs_to_current_shard(shards)) {
            throw std::runtime_error(sprint("A regular compaction for %s.%s INCORRECTLY used sstable %s which doesn't belong to this shard!",
                _schema->ks_name(), _schema->cf_name(), sst->toc_filename()));
        }
    }

    auto new_compacted_but_not_deleted = _sstables_compacted_but_not_deleted;
    // rebuilding _sstables_compacted_but_not_deleted first to make the entire rebuild operation exception safe.
    new_compacted_but_not_deleted.insert(new_compacted_but_not_deleted.end(), sstables_to_remove.begin(), sstables_to_remove.end());

    rebuild_sstable_list(new_sstables, sstables_to_remove);

    _sstables_compacted_but_not_deleted = std::move(new_compacted_but_not_deleted);

    rebuild_statistics();

    // This is done in the background, so we can consider this compaction completed.
    seastar::with_gate(_sstable_deletion_gate, [this, sstables_to_remove] {
       return with_semaphore(_sstable_deletion_sem, 1, [this, sstables_to_remove = std::move(sstables_to_remove)] {
        return sstables::delete_atomically(sstables_to_remove, *get_large_partition_handler()).then_wrapped([this, sstables_to_remove] (future<> f) {
            std::exception_ptr eptr;
            try {
                f.get();
            } catch(...) {
                eptr = std::current_exception();
            }

            // unconditionally remove compacted sstables from _sstables_compacted_but_not_deleted,
            // or they could stay forever in the set, resulting in deleted files remaining
            // opened and disk space not being released until shutdown.
            std::unordered_set<sstables::shared_sstable> s(
                   sstables_to_remove.begin(), sstables_to_remove.end());
            auto e = boost::range::remove_if(_sstables_compacted_but_not_deleted, [&] (sstables::shared_sstable sst) -> bool {
                return s.count(sst);
            });
            _sstables_compacted_but_not_deleted.erase(e, _sstables_compacted_but_not_deleted.end());
            rebuild_statistics();

            if (eptr) {
                return make_exception_future<>(eptr);
            }
            return make_ready_future<>();
         });
        }).then([this] {
            // refresh underlying data source in row cache to prevent it from holding reference
            // to sstables files which were previously deleted.
            _cache.refresh_snapshot();
        });
    });
}

// For replace/remove_ancestors_needed_write, note that we need to update the compaction backlog
// manually. The new tables will be coming from a remote shard and thus unaccounted for in our
// list so far, and the removed ones will no longer be needed by us.
void table::replace_ancestors_needed_rewrite(std::unordered_set<uint64_t> ancestors, std::vector<sstables::shared_sstable> new_sstables) {
    std::vector<sstables::shared_sstable> old_sstables;

    for (auto& sst : new_sstables) {
        _compaction_strategy.get_backlog_tracker().add_sstable(sst);
    }

    for (auto& ancestor : ancestors) {
        auto it = _sstables_need_rewrite.find(ancestor);
        if (it != _sstables_need_rewrite.end()) {
            old_sstables.push_back(it->second);
            _compaction_strategy.get_backlog_tracker().remove_sstable(it->second);
            _sstables_need_rewrite.erase(it);
        }
    }
    rebuild_sstable_list(new_sstables, old_sstables);
    rebuild_statistics();
}

void table::remove_ancestors_needed_rewrite(std::unordered_set<uint64_t> ancestors) {
    std::vector<sstables::shared_sstable> old_sstables;
    for (auto& ancestor : ancestors) {
        auto it = _sstables_need_rewrite.find(ancestor);
        if (it != _sstables_need_rewrite.end()) {
            old_sstables.push_back(it->second);
            _compaction_strategy.get_backlog_tracker().remove_sstable(it->second);
            _sstables_need_rewrite.erase(it);
        }
    }
    rebuild_sstable_list({}, old_sstables);
    rebuild_statistics();
}

future<>
table::compact_sstables(sstables::compaction_descriptor descriptor, bool cleanup) {
    if (!descriptor.sstables.size()) {
        // if there is nothing to compact, just return.
        return make_ready_future<>();
    }

    return with_lock(_sstables_lock.for_read(), [this, descriptor = std::move(descriptor), cleanup] () mutable {
        auto create_sstable = [this] {
                auto gen = this->calculate_generation_for_new_table();
                auto sst = sstables::make_sstable(_schema, _config.datadir, gen,
                        get_highest_supported_format(),
                        sstables::sstable::format_types::big);
                sst->set_unshared();
                return sst;
        };
        auto sstables_to_compact = descriptor.sstables;
        return sstables::compact_sstables(std::move(descriptor), *this, create_sstable,
                cleanup).then([this, sstables_to_compact = std::move(sstables_to_compact)] (auto info) {
            _compaction_strategy.notify_completion(sstables_to_compact, info.new_sstables);
            this->on_compaction_completion(info.new_sstables, sstables_to_compact);
            return info;
        });
    }).then([this] (auto info) {
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
        return db::system_keyspace::update_compaction_history(info.ks, info.cf, info.ended_at,
            info.start_size, info.end_size, std::unordered_map<int32_t, int64_t>{});
    });
}

static bool needs_cleanup(const sstables::shared_sstable& sst,
                   const dht::token_range_vector& owned_ranges,
                   schema_ptr s) {
    auto first = sst->get_first_partition_key();
    auto last = sst->get_last_partition_key();
    auto first_token = dht::global_partitioner().get_token(*s, first);
    auto last_token = dht::global_partitioner().get_token(*s, last);
    dht::token_range sst_token_range = dht::token_range::make(first_token, last_token);

    // return true iff sst partition range isn't fully contained in any of the owned ranges.
    for (auto& r : owned_ranges) {
        if (r.contains(sst_token_range, dht::token_comparator())) {
            return false;
        }
    }
    return true;
}

future<> table::cleanup_sstables(sstables::compaction_descriptor descriptor) {
    dht::token_range_vector r = service::get_local_storage_service().get_local_ranges(_schema->ks_name());

    return do_with(std::move(descriptor.sstables), std::move(r), [this] (auto& sstables, auto& owned_ranges) {
        return do_for_each(sstables, [this, &owned_ranges] (auto& sst) {
            if (!owned_ranges.empty() && !needs_cleanup(sst, owned_ranges, _schema)) {
            return make_ready_future<>();
            }

            // this semaphore ensures that only one cleanup will run per shard.
            // That's to prevent node from running out of space when almost all sstables
            // need cleanup, so if sstables are cleaned in parallel, we may need almost
            // twice the disk space used by those sstables.
            static thread_local semaphore sem(1);

            return with_semaphore(sem, 1, [this, &sst] {
                // release reference to sstables cleaned up, otherwise space usage from their data and index
                // components cannot be reclaimed until all of them are cleaned.
                return this->compact_sstables(sstables::compaction_descriptor({ std::move(sst) }, sst->get_sstable_level()), true);
            });
        });
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
        dblog.error("Failed to trigger compaction: {}", std::current_exception());
    }
}

void table::do_trigger_compaction() {
    // But only submit if we're not locked out
    if (!_compaction_disabled) {
        _compaction_manager.submit(this);
    }
}

future<> table::run_compaction(sstables::compaction_descriptor descriptor) {
    return compact_sstables(std::move(descriptor));
}

void table::set_compaction_strategy(sstables::compaction_strategy_type strategy) {
    dblog.debug("Setting compaction strategy of {}.{} to {}", _schema->ks_name(), _schema->cf_name(), sstables::compaction_strategy::name(strategy));
    auto new_cs = make_compaction_strategy(strategy, _schema->compaction_strategy_options());

    _compaction_manager.register_backlog_tracker(new_cs.get_backlog_tracker());
    auto move_read_charges = new_cs.type() == _compaction_strategy.type();
    _compaction_strategy.get_backlog_tracker().transfer_ongoing_charges(new_cs.get_backlog_tracker(), move_read_charges);

    auto new_sstables = new_cs.make_sstable_set(_schema);
    for (auto&& s : *_sstables->all()) {
        new_cs.get_backlog_tracker().add_sstable(s);
        new_sstables.insert(s);
    }

    if (!move_read_charges) {
        _compaction_manager.stop_tracking_ongoing_compactions(this);
    }

    // now exception safe:
    _compaction_strategy = std::move(new_cs);
    _sstables = std::move(new_sstables);
}

size_t table::sstables_count() const {
    return _sstables->all()->size();
}

std::vector<uint64_t> table::sstable_count_per_level() const {
    std::vector<uint64_t> count_per_level;
    for (auto&& sst : *_sstables->all()) {
        auto level = sst->get_sstable_level();

        if (level + 1 > count_per_level.size()) {
            count_per_level.resize(level + 1, 0UL);
        }
        count_per_level[level]++;
    }
    return count_per_level;
}

int64_t table::get_unleveled_sstables() const {
    // TODO: when we support leveled compaction, we should return the number of
    // SSTables in L0. If leveled compaction is enabled in this column family,
    // then we should return zero, as we currently do.
    return 0;
}

future<std::unordered_set<sstring>> table::get_sstables_by_partition_key(const sstring& key) const {
    return do_with(std::unordered_set<sstring>(), lw_shared_ptr<sstables::sstable_set::incremental_selector>(make_lw_shared(get_sstable_set().make_incremental_selector())),
            partition_key(partition_key::from_nodetool_style_string(_schema, key)),
            [this] (std::unordered_set<sstring>& filenames, lw_shared_ptr<sstables::sstable_set::incremental_selector>& sel, partition_key& pk) {
        return do_with(dht::decorated_key(dht::global_partitioner().decorate_key(*_schema, pk)),
                [this, &filenames, &sel, &pk](dht::decorated_key& dk) mutable {
            auto sst = sel->select(dk).sstables;
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
    return *_sstables;
}

lw_shared_ptr<sstable_list> table::get_sstables() const {
    return _sstables->all();
}

std::vector<sstables::shared_sstable> table::select_sstables(const dht::partition_range& range) const {
    return _sstables->select(range);
}

std::vector<sstables::shared_sstable> table::candidates_for_compaction() const {
    return boost::copy_range<std::vector<sstables::shared_sstable>>(*get_sstables()
            | boost::adaptors::filtered([this] (auto& sst) {
        return !_sstables_need_rewrite.count(sst->generation()) && !_sstables_staging.count(sst->generation());
    }));
}

std::vector<sstables::shared_sstable> table::sstables_need_rewrite() const {
    return boost::copy_range<std::vector<sstables::shared_sstable>>(_sstables_need_rewrite | boost::adaptors::map_values);
}

// Gets the list of all sstables in the column family, including ones that are
// not used for active queries because they have already been compacted, but are
// waiting for delete_atomically() to return.
//
// As long as we haven't deleted them, compaction needs to ensure it doesn't
// garbage-collect a tombstone that covers data in an sstable that may not be
// successfully deleted.
lw_shared_ptr<sstable_list> table::get_sstables_including_compacted_undeleted() const {
    if (_sstables_compacted_but_not_deleted.empty()) {
        return get_sstables();
    }
    auto ret = make_lw_shared(*_sstables->all());
    for (auto&& s : _sstables_compacted_but_not_deleted) {
        ret->insert(s);
    }
    return ret;
}

const std::vector<sstables::shared_sstable>& table::compacted_undeleted_sstables() const {
    return _sstables_compacted_but_not_deleted;
}

inline bool table::manifest_json_filter(const lister::path&, const directory_entry& entry) {
    // Filter out directories. If type of the entry is unknown - check its name.
    if (entry.type.value_or(directory_entry_type::regular) != directory_entry_type::directory && entry.name == "manifest.json") {
        return false;
    }

    return true;
}

// TODO: possibly move it to seastar
template <typename Service, typename PtrType, typename Func>
static future<> invoke_shards_with_ptr(std::unordered_set<shard_id> shards, distributed<Service>& s, PtrType ptr, Func&& func) {
    return parallel_for_each(std::move(shards), [&s, &func, ptr] (shard_id id) {
        return s.invoke_on(id, [func, foreign = make_foreign(ptr)] (Service& s) mutable {
            return func(s, std::move(foreign));
        });
    });
}

future<> distributed_loader::open_sstable(distributed<database>& db, sstables::entry_descriptor comps,
        std::function<future<> (column_family&, sstables::foreign_sstable_open_info)> func, const io_priority_class& pc) {
    // loads components of a sstable from shard S and share it with all other
    // shards. Which shard a sstable will be opened at is decided using
    // calculate_shard_from_sstable_generation(), which is the inverse of
    // calculate_generation_for_new_table(). That ensures every sstable is
    // shard-local if reshard wasn't performed. This approach is also expected
    // to distribute evenly the resource usage among all shards.

    return db.invoke_on(column_family::calculate_shard_from_sstable_generation(comps.generation),
            [&db, comps = std::move(comps), func = std::move(func), &pc] (database& local) {

        return with_semaphore(local.sstable_load_concurrency_sem(), 1, [&db, &local, comps = std::move(comps), func = std::move(func), &pc] {
            auto& cf = local.find_column_family(comps.ks, comps.cf);

            auto f = sstables::sstable::load_shared_components(cf.schema(), comps.sstdir, comps.generation, comps.version, comps.format, pc);
            return f.then([&db, comps = std::move(comps), func = std::move(func)] (sstables::sstable_open_info info) {
                // shared components loaded, now opening sstable in all shards that own it with shared components
                return do_with(std::move(info), [&db, comps = std::move(comps), func = std::move(func)] (auto& info) {
                    // All shards that own the sstable is interested in it in addition to shard that
                    // is responsible for its generation. We may need to add manually this shard
                    // because sstable may not contain data that belong to it.
                    auto shards_interested_in_this_sstable = boost::copy_range<std::unordered_set<shard_id>>(info.owners);
                    shard_id shard_responsible_for_generation = column_family::calculate_shard_from_sstable_generation(comps.generation);
                    shards_interested_in_this_sstable.insert(shard_responsible_for_generation);

                    return invoke_shards_with_ptr(std::move(shards_interested_in_this_sstable), db, std::move(info.components),
                            [owners = info.owners, data = info.data.dup(), index = info.index.dup(), comps, func] (database& db, auto components) {
                        auto& cf = db.find_column_family(comps.ks, comps.cf);
                        return func(cf, sstables::foreign_sstable_open_info{std::move(components), owners, data, index});
                    });
                });
            });
        });
    });
}

// global_column_family_ptr provides a way to easily retrieve local instance of a given column family.
class global_column_family_ptr {
    distributed<database>& _db;
    utils::UUID _id;
private:
    column_family& get() const { return _db.local().find_column_family(_id); }
public:
    global_column_family_ptr(distributed<database>& db, sstring ks_name, sstring cf_name)
        : _db(db)
        , _id(_db.local().find_column_family(ks_name, cf_name).schema()->id()) {
    }

    column_family* operator->() const {
        return &get();
    }
    column_family& operator*() const {
        return get();
    }
};

template <typename Pred>
static future<std::vector<sstables::shared_sstable>>
load_sstables_with_open_info(std::vector<sstables::foreign_sstable_open_info> ssts_info, schema_ptr s, sstring dir, Pred&& pred) {
    return do_with(std::vector<sstables::shared_sstable>(), [ssts_info = std::move(ssts_info), s, dir, pred] (auto& ssts) mutable {
        return parallel_for_each(std::move(ssts_info), [&ssts, s, dir, pred] (auto& info) mutable {
            if (!pred(info)) {
                return make_ready_future<>();
            }
            auto sst = sstables::make_sstable(s, dir, info.generation, info.version, info.format);
            return sst->load(std::move(info)).then([&ssts, sst] {
                ssts.push_back(std::move(sst));
                return make_ready_future<>();
            });
        }).then([&ssts] () mutable {
            return std::move(ssts);
        });
    });
}

// Return all sstables that need resharding in the system. Only one instance of a shared sstable is returned.
static future<std::vector<sstables::shared_sstable>> get_all_shared_sstables(distributed<database>& db, sstring sstdir, global_column_family_ptr cf) {
    class all_shared_sstables {
        schema_ptr _schema;
        sstring _dir;
        std::unordered_map<int64_t, sstables::shared_sstable> _result;
    public:
        all_shared_sstables(const sstring& sstdir, global_column_family_ptr cf) : _schema(cf->schema()), _dir(sstdir) {}

        future<> operator()(std::vector<sstables::foreign_sstable_open_info> ssts_info) {
            return load_sstables_with_open_info(std::move(ssts_info), _schema, _dir, [this] (auto& info) {
                // skip loading of shared sstable that is already stored in _result.
                return !_result.count(info.generation);
            }).then([this] (std::vector<sstables::shared_sstable> sstables) {
                for (auto& sst : sstables) {
                    auto gen = sst->generation();
                    _result.emplace(gen, std::move(sst));
                }
                return make_ready_future<>();
            });
        }

        std::vector<sstables::shared_sstable> get() && {
            return boost::copy_range<std::vector<sstables::shared_sstable>>(std::move(_result) | boost::adaptors::map_values);
        }
    };

    return db.map_reduce(all_shared_sstables(sstdir, cf), [cf, sstdir] (database& db) mutable {
        return seastar::async([cf, sstdir] {
            return boost::copy_range<std::vector<sstables::foreign_sstable_open_info>>(cf->sstables_need_rewrite()
                | boost::adaptors::filtered([sstdir] (auto&& sst) { return sst->get_dir() == sstdir; })
                | boost::adaptors::transformed([] (auto&& sst) { return sst->get_open_info().get0(); }));
        });
    });
}

// checks whether or not a given column family is worth resharding by checking if any of its
// sstables has more than one owner shard.
static future<bool> worth_resharding(distributed<database>& db, global_column_family_ptr cf) {
    auto has_shared_sstables = [cf] (database& db) {
        return cf->has_shared_sstables();
    };
    return db.map_reduce0(has_shared_sstables, bool(false), std::logical_or<bool>());
}

// make a set of sstables available at another shard.
template <typename Func>
static future<> forward_sstables_to(shard_id shard, sstring directory, std::vector<sstables::shared_sstable> sstables, global_column_family_ptr cf, Func&& func) {
    return seastar::async([sstables = std::move(sstables), directory, shard, cf, func] () mutable {
        auto infos = boost::copy_range<std::vector<sstables::foreign_sstable_open_info>>(sstables
            | boost::adaptors::transformed([] (auto&& sst) { return sst->get_open_info().get0(); }));

        smp::submit_to(shard, [cf, func, infos = std::move(infos), directory] () mutable {
            return load_sstables_with_open_info(std::move(infos), cf->schema(), directory, [] (auto& p) {
                return true;
            }).then([func] (std::vector<sstables::shared_sstable> sstables) {
                return func(std::move(sstables));
            });
        }).get();
    });
}

// invokes each descriptor at its target shard, which involves forwarding sstables too.
template <typename Func>
static future<> invoke_all_resharding_jobs(global_column_family_ptr cf, sstring directory, std::vector<sstables::resharding_descriptor> jobs, Func&& func) {
    return parallel_for_each(std::move(jobs), [cf, func, &directory] (sstables::resharding_descriptor& job) mutable {
        return forward_sstables_to(job.reshard_at, directory, std::move(job.sstables), cf,
                [cf, func, level = job.level, max_sstable_bytes = job.max_sstable_bytes] (auto sstables) {
            // compaction manager ensures that only one reshard operation will run per shard.
            auto job = [func, sstables = std::move(sstables), level, max_sstable_bytes] () mutable {
                return func(std::move(sstables), level, max_sstable_bytes);
            };
            return cf->get_compaction_manager().run_resharding_job(&*cf, std::move(job));
        });
    });
}

static std::vector<sstables::shared_sstable> sstables_for_shard(const std::vector<sstables::shared_sstable>& sstables, shard_id shard) {
    auto belongs_to_shard = [] (const sstables::shared_sstable& sst, unsigned shard) {
        auto& shards = sst->get_shards_for_this_sstable();
        return boost::range::find(shards, shard) != shards.end();
    };

    return boost::copy_range<std::vector<sstables::shared_sstable>>(sstables
        | boost::adaptors::filtered([&] (auto& sst) { return belongs_to_shard(sst, shard); }));
}

void distributed_loader::reshard(distributed<database>& db, sstring ks_name, sstring cf_name) {
    assert(engine().cpu_id() == 0); // NOTE: should always run on shard 0!

    // ensures that only one column family is resharded at a time (that's okay because
    // actual resharding is parallelized), and that's needed to prevent the same column
    // family from being resharded in parallel (that could happen, for example, if
    // refresh (triggers resharding) is issued by user while resharding is going on).
    static semaphore sem(1);

    with_semaphore(sem, 1, [&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] () mutable {
        return seastar::async([&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] () mutable {
            global_column_family_ptr cf(db, ks_name, cf_name);

            if (cf->get_compaction_manager().stopped()) {
                return;
            }
            // fast path to detect that this column family doesn't need reshard.
            if (!worth_resharding(db, cf).get0()) {
                dblog.debug("Nothing to reshard for {}.{}", cf->schema()->ks_name(), cf->schema()->cf_name());
                return;
            }

            parallel_for_each(cf->_config.all_datadirs, [&db, cf] (const sstring& directory) {
                auto candidates = get_all_shared_sstables(db, directory, cf).get0();
                dblog.debug("{} candidates for resharding for {}.{}", candidates.size(), cf->schema()->ks_name(), cf->schema()->cf_name());
                auto jobs = cf->get_compaction_strategy().get_resharding_jobs(*cf, std::move(candidates));
                dblog.debug("{} resharding jobs for {}.{}", jobs.size(), cf->schema()->ks_name(), cf->schema()->cf_name());

                return invoke_all_resharding_jobs(cf, directory, std::move(jobs), [directory, &cf] (auto sstables, auto level, auto max_sstable_bytes) {
                    auto creator = [&cf, directory] (shard_id shard) mutable {
                        // we need generation calculated by instance of cf at requested shard,
                        // or resource usage wouldn't be fairly distributed among shards.
                        auto gen = smp::submit_to(shard, [&cf] () {
                            return cf->calculate_generation_for_new_table();
                        }).get0();

                        auto sst = sstables::make_sstable(cf->schema(), directory, gen,
                            get_highest_supported_format(), sstables::sstable::format_types::big,
                            gc_clock::now(), default_io_error_handler_gen());
                        return sst;
                    };
                    auto f = sstables::reshard_sstables(sstables, *cf, creator, max_sstable_bytes, level);

                    return f.then([&cf, sstables = std::move(sstables), directory] (std::vector<sstables::shared_sstable> new_sstables) mutable {
                        // an input sstable may belong to shard 1 and 2 and only have data which
                        // token belongs to shard 1. That means resharding will only create a
                        // sstable for shard 1, but both shards opened the sstable. So our code
                        // below should ask both shards to remove the resharded table, or it
                        // wouldn't be deleted by our deletion manager, and resharding would be
                        // triggered again in the subsequent boot.
                        return parallel_for_each(boost::irange(0u, smp::count), [&cf, directory, sstables, new_sstables] (auto shard) {
                            auto old_sstables_for_shard = sstables_for_shard(sstables, shard);
                            // nothing to do if no input sstable belongs to this shard.
                            if (old_sstables_for_shard.empty()) {
                                return make_ready_future<>();
                            }
                            auto new_sstables_for_shard = sstables_for_shard(new_sstables, shard);
                            // sanity checks
                            for (auto& sst : new_sstables_for_shard) {
                                auto& shards = sst->get_shards_for_this_sstable();
                                if (shards.size() != 1) {
                                    throw std::runtime_error(sprint("resharded sstable %s doesn't belong to only one shard", sst->get_filename()));
                                }
                                if (shards.front() != shard) {
                                    throw std::runtime_error(sprint("resharded sstable %s should belong to shard %d", sst->get_filename(), shard));
                                }
                            }

                            std::unordered_set<uint64_t> ancestors;
                            boost::range::transform(old_sstables_for_shard, std::inserter(ancestors, ancestors.end()),
                                std::mem_fn(&sstables::sstable::generation));

                            if (new_sstables_for_shard.empty()) {
                                // handles case where sstable needing rewrite doesn't produce any sstable
                                // for a shard it belongs to when resharded (the reason is explained above).
                                return smp::submit_to(shard, [cf, ancestors = std::move(ancestors)] () mutable {
                                    cf->remove_ancestors_needed_rewrite(ancestors);
                                });
                            } else {
                                return forward_sstables_to(shard, directory, new_sstables_for_shard, cf, [cf, ancestors = std::move(ancestors)] (auto sstables) {
                                    cf->replace_ancestors_needed_rewrite(std::move(ancestors), std::move(sstables));
                                });
                            }
                        }).then([&cf, sstables] {
                            // schedule deletion of shared sstables after we're certain that new unshared ones were successfully forwarded to respective shards.
                            sstables::delete_atomically(std::move(sstables), *cf->get_large_partition_handler()).handle_exception([op = sstables::background_jobs().start()] (std::exception_ptr eptr) {
                                try {
                                    std::rethrow_exception(eptr);
                                } catch (...) {
                                    dblog.warn("Exception in resharding when deleting sstable file: {}", eptr);
                                }
                            });
                        });
                    });
                });
            }).get();
        });
    });
}

future<> distributed_loader::load_new_sstables(distributed<database>& db, sstring ks, sstring cf, std::vector<sstables::entry_descriptor> new_tables) {
    return parallel_for_each(new_tables, [&db] (auto comps) {
        auto cf_sstable_open = [comps] (column_family& cf, sstables::foreign_sstable_open_info info) {
            auto f = cf.open_sstable(std::move(info), cf._config.datadir, comps.generation, comps.version, comps.format);
            return f.then([&cf] (sstables::shared_sstable sst) mutable {
                if (sst) {
                    cf._sstables_opened_but_not_loaded.push_back(sst);
                }
                return make_ready_future<>();
            });
        };
        return distributed_loader::open_sstable(db, comps, cf_sstable_open, service::get_local_compaction_priority());
    }).then([&db, ks, cf] {
        return db.invoke_on_all([ks = std::move(ks), cfname = std::move(cf)] (database& db) {
            auto& cf = db.find_column_family(ks, cfname);
            return cf.get_row_cache().invalidate([&cf] () noexcept {
                // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
                // atomically load all opened sstables into column family.
                for (auto& sst : cf._sstables_opened_but_not_loaded) {
                    cf.load_sstable(sst, true);
                }
                cf._sstables_opened_but_not_loaded.clear();
                cf.trigger_compaction();
            });
        });
    }).then([&db, ks, cf] () mutable {
        return smp::submit_to(0, [&db, ks = std::move(ks), cf = std::move(cf)] () mutable {
            distributed_loader::reshard(db, std::move(ks), std::move(cf));
        });
    });
}

future<sstables::entry_descriptor> distributed_loader::probe_file(distributed<database>& db, sstring sstdir, sstring fname) {
    using namespace sstables;

    entry_descriptor comps = entry_descriptor::make_descriptor(sstdir, fname);

    // Every table will have a TOC. Using a specific file as a criteria, as
    // opposed to, say verifying _sstables.count() to be zero is more robust
    // against parallel loading of the directory contents.
    if (comps.component != component_type::TOC) {
        return make_ready_future<entry_descriptor>(std::move(comps));
    }
    auto cf_sstable_open = [sstdir, comps, fname] (column_family& cf, sstables::foreign_sstable_open_info info) {
        cf.update_sstables_known_generation(comps.generation);
        if (shared_sstable sst = cf.get_staging_sstable(comps.generation)) {
            dblog.warn("SSTable {} is already present in staging/ directory. Moving from staging will be retried.", sst->get_filename());
            return seastar::async([sst = std::move(sst), comps = std::move(comps)] () {
                sst->move_to_new_dir_in_thread(comps.sstdir, comps.generation);
            });
        }
        {
            auto i = boost::range::find_if(*cf._sstables->all(), [gen = comps.generation] (sstables::shared_sstable sst) { return sst->generation() == gen; });
            if (i != cf._sstables->all()->end()) {
                auto new_toc = sstdir + "/" + fname;
                throw std::runtime_error(sprint("Attempted to add sstable generation %d twice: new=%s existing=%s",
                                                comps.generation, new_toc, (*i)->toc_filename()));
            }
        }
        return cf.open_sstable(std::move(info), sstdir, comps.generation, comps.version, comps.format).then([&cf] (sstables::shared_sstable sst) mutable {
            if (sst) {
                return cf.get_row_cache().invalidate([&cf, sst = std::move(sst)] () mutable noexcept {
                    // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
                    cf.load_sstable(sst);
                });
            }
            return make_ready_future<>();
        });
    };

    return distributed_loader::open_sstable(db, comps, cf_sstable_open).then_wrapped([fname] (future<> f) {
        try {
            f.get();
        } catch (malformed_sstable_exception& e) {
            dblog.error("malformed sstable {}: {}. Refusing to boot", fname, e.what());
            throw;
        } catch(...) {
            dblog.error("Unrecognized error while processing {}: {}. Refusing to boot",
                fname, std::current_exception());
            throw;
        }
        return make_ready_future<>();
    }).then([comps] () mutable {
        return make_ready_future<entry_descriptor>(std::move(comps));
    });
}

future<> distributed_loader::populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf) {
    // We can catch most errors when we try to load an sstable. But if the TOC
    // file is the one missing, we won't try to load the sstable at all. This
    // case is still an invalid case, but it is way easier for us to treat it
    // by waiting for all files to be loaded, and then checking if we saw a
    // file during scan_dir, without its corresponding TOC.
    enum class component_status {
        has_some_file,
        has_toc_file,
        has_temporary_toc_file,
    };

    struct sstable_descriptor {
        component_status status;
        sstables::sstable::version_types version;
        sstables::sstable::format_types format;
    };

    auto verifier = make_lw_shared<std::unordered_map<unsigned long, sstable_descriptor>>();

    return do_with(std::vector<future<>>(), [&db, sstdir = std::move(sstdir), verifier, ks, cf] (std::vector<future<>>& futures) {
        return lister::scan_dir(sstdir, { directory_entry_type::regular }, [&db, verifier, &futures] (lister::path sstdir, directory_entry de) {
            // FIXME: The secondary indexes are in this level, but with a directory type, (starting with ".")
            auto f = distributed_loader::probe_file(db, sstdir.native(), de.name).then([verifier, sstdir, de] (auto entry) {
                if (entry.component == component_type::TemporaryStatistics) {
                    return remove_file(sstables::sstable::filename(sstdir.native(), entry.ks, entry.cf, entry.version, entry.generation,
                        entry.format, component_type::TemporaryStatistics));
                }

                if (verifier->count(entry.generation)) {
                    if (verifier->at(entry.generation).status == component_status::has_toc_file) {
                        lister::path file_path(sstdir / de.name.c_str());
                        if (entry.component == component_type::TOC) {
                            throw sstables::malformed_sstable_exception("Invalid State encountered. TOC file already processed", file_path.native());
                        } else if (entry.component == component_type::TemporaryTOC) {
                            throw sstables::malformed_sstable_exception("Invalid State encountered. Temporary TOC file found after TOC file was processed", file_path.native());
                        }
                    } else if (entry.component == component_type::TOC) {
                        verifier->at(entry.generation).status = component_status::has_toc_file;
                    } else if (entry.component == component_type::TemporaryTOC) {
                        verifier->at(entry.generation).status = component_status::has_temporary_toc_file;
                    }
                } else {
                    if (entry.component == component_type::TOC) {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_toc_file, entry.version, entry.format});
                    } else if (entry.component == component_type::TemporaryTOC) {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_temporary_toc_file, entry.version, entry.format});
                    } else {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_some_file, entry.version, entry.format});
                    }
                }
                return make_ready_future<>();
            });

            // push future returned by probe_file into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            futures.push_back(std::move(f));

            return make_ready_future<>();
        }, &column_family::manifest_json_filter).then([&futures] {
            return when_all(futures.begin(), futures.end()).then([] (std::vector<future<>> ret) {
                std::exception_ptr eptr;

                for (auto& f : ret) {
                    try {
                        if (eptr) {
                            f.ignore_ready_future();
                        } else {
                            f.get();
                        }
                    } catch(...) {
                        eptr = std::current_exception();
                    }
                }

                if (eptr) {
                    return make_exception_future<>(eptr);
                }
                return make_ready_future<>();
            });
        }).then([verifier, sstdir, ks = std::move(ks), cf = std::move(cf)] {
            return do_for_each(*verifier, [sstdir = std::move(sstdir), ks = std::move(ks), cf = std::move(cf), verifier] (auto v) {
                if (v.second.status == component_status::has_temporary_toc_file) {
                    unsigned long gen = v.first;
                    sstables::sstable::version_types version = v.second.version;
                    sstables::sstable::format_types format = v.second.format;

                    if (engine().cpu_id() != 0) {
                        dblog.debug("At directory: {}, partial SSTable with generation {} not relevant for this shard, ignoring", sstdir, v.first);
                        return make_ready_future<>();
                    }
                    // shard 0 is the responsible for removing a partial sstable.
                    return sstables::sstable::remove_sstable_with_temp_toc(ks, cf, sstdir, gen, version, format);
                } else if (v.second.status != component_status::has_toc_file) {
                    throw sstables::malformed_sstable_exception(sprint("At directory: %s: no TOC found for SSTable with generation %d!. Refusing to boot", sstdir, v.first));
                }
                return make_ready_future<>();
            });
        });
    });

}

inline
flush_controller
make_flush_controller(db::config& cfg, seastar::scheduling_group sg, const ::io_priority_class& iop, std::function<double()> fn) {
    if (cfg.memtable_flush_static_shares() > 0) {
        return flush_controller(sg, iop, cfg.memtable_flush_static_shares());
    }
    return flush_controller(sg, iop, 50ms, cfg.virtual_dirty_soft_limit(), std::move(fn));
}

inline
std::unique_ptr<compaction_manager>
make_compaction_manager(db::config& cfg, database_config& dbcfg) {
    if (cfg.compaction_static_shares() > 0) {
        return std::make_unique<compaction_manager>(dbcfg.compaction_scheduling_group, service::get_local_compaction_priority(), dbcfg.available_memory, cfg.compaction_static_shares());
    }
    return std::make_unique<compaction_manager>(dbcfg.compaction_scheduling_group, service::get_local_compaction_priority(), dbcfg.available_memory);
}

utils::UUID database::empty_version = utils::UUID_gen::get_name_UUID(bytes{});

database::database() : database(db::config(), database_config())
{}

database::database(const db::config& cfg, database_config dbcfg)
    : _stats(make_lw_shared<db_stats>())
    , _cl_stats(std::make_unique<cell_locker_stats>())
    , _cfg(std::make_unique<db::config>(cfg))
    // Allow system tables a pool of 10 MB memory to write, but never block on other regions.
    , _system_dirty_memory_manager(*this, 10 << 20, cfg.virtual_dirty_soft_limit(), default_scheduling_group())
    , _dirty_memory_manager(*this, dbcfg.available_memory * 0.45, cfg.virtual_dirty_soft_limit(), dbcfg.statement_scheduling_group)
    , _streaming_dirty_memory_manager(*this, dbcfg.available_memory * 0.10, cfg.virtual_dirty_soft_limit(), dbcfg.streaming_scheduling_group)
    , _dbcfg(dbcfg)
    , _memtable_controller(make_flush_controller(*_cfg, dbcfg.memtable_scheduling_group, service::get_local_memtable_flush_priority(), [this, limit = float(_dirty_memory_manager.throttle_threshold())] {
        auto backlog = (_dirty_memory_manager.virtual_dirty_memory()) / limit;
        if (_dirty_memory_manager.has_extraneous_flushes_requested()) {
            backlog = std::max(backlog, _memtable_controller.backlog_of_shares(200));
        }
        return backlog;
    }))
    , _read_concurrency_sem(max_count_concurrent_reads,
        max_memory_concurrent_reads(),
        max_inactive_queue_length(),
        [this] {
            ++_stats->sstable_read_queue_overloaded;
            return std::make_exception_ptr(std::runtime_error("sstable inactive read queue overloaded"));
        })
    // No timeouts or queue length limits - a failure here can kill an entire repair.
    // Trust the caller to limit concurrency.
    , _streaming_concurrency_sem(max_count_streaming_concurrent_reads, max_memory_streaming_concurrent_reads())
    , _system_read_concurrency_sem(max_count_system_concurrent_reads, max_memory_system_concurrent_reads())
    , _data_query_stage("data_query", &column_family::query)
    , _mutation_query_stage()
    , _apply_stage("db_apply", &database::do_apply)
    , _version(empty_version)
    , _compaction_manager(make_compaction_manager(*_cfg, dbcfg))
    , _enable_incremental_backups(cfg.incremental_backups())
    , _querier_cache(_read_concurrency_sem, dbcfg.available_memory * 0.04)
    , _large_partition_handler(std::make_unique<db::cql_table_large_partition_handler>(_cfg->compaction_large_partition_warning_threshold_mb()*1024*1024))
    , _result_memory_limiter(dbcfg.available_memory / 10)
{
    local_schema_registry().init(*this); // TODO: we're never unbound.
    setup_metrics();

    _row_cache_tracker.set_compaction_scheduling_group(dbcfg.memory_compaction_scheduling_group);

    dblog.info("Row: max_vector_size: {}, internal_count: {}", size_t(row::max_vector_size), size_t(row::internal_count));
}

void backlog_controller::adjust() {
    auto backlog = _current_backlog();

    if (backlog >= _control_points.back().input) {
        update_controller(_control_points.back().output);
        return;
    }

    // interpolate to find out which region we are. This run infrequently and there are a fixed
    // number of points so a simple loop will do.
    size_t idx = 1;
    while ((idx < _control_points.size() - 1) && (_control_points[idx].input < backlog)) {
        idx++;
    }

    control_point& cp = _control_points[idx];
    control_point& last = _control_points[idx - 1];
    float result = last.output + (backlog - last.input) * (cp.output - last.output)/(cp.input - last.input);
    update_controller(result);
}

float backlog_controller::backlog_of_shares(float shares) const {
    size_t idx = 1;
    while ((idx < _control_points.size() - 1) && (_control_points[idx].output < shares)) {
        idx++;
    }
    const control_point& cp = _control_points[idx];
    const control_point& last = _control_points[idx - 1];
    // Compute the inverse function of the backlog in the interpolation interval that we fall
    // into.
    //
    // The formula for the backlog inside an interpolation point is y = a + bx, so the inverse
    // function is x = (y - a) / b

    return last.input + (shares - last.output) * (cp.input - last.input) / (cp.output - last.output);
}

void backlog_controller::update_controller(float shares) {
    _scheduling_group.set_shares(shares);
    if (!_inflight_update.available()) {
        return; // next timer will fix it
    }
    _inflight_update = engine().update_shares_for_class(_io_priority, uint32_t(shares));
}

void
dirty_memory_manager::setup_collectd(sstring namestr) {
    namespace sm = seastar::metrics;

    _metrics.add_group("memory", {
        sm::make_gauge(namestr + "_dirty_bytes", [this] { return real_dirty_memory(); },
                       sm::description("Holds the current size of a all non-free memory in bytes: used memory + released memory that hasn't been returned to a free memory pool yet. "
                                       "Total memory size minus this value represents the amount of available memory. "
                                       "If this value minus virtual_dirty_bytes is too high then this means that the dirty memory eviction lags behind.")),

        sm::make_gauge(namestr +"_virtual_dirty_bytes", [this] { return virtual_dirty_memory(); },
                       sm::description("Holds the size of used memory in bytes. Compare it to \"dirty_bytes\" to see how many memory is wasted (neither used nor available).")),
    });
}

static const metrics::label class_label("class");

void
database::setup_metrics() {
    _dirty_memory_manager.setup_collectd("regular");
    _system_dirty_memory_manager.setup_collectd("system");
    _streaming_dirty_memory_manager.setup_collectd("streaming");

    namespace sm = seastar::metrics;

    auto user_label_instance = class_label("user");
    auto streaming_label_instance = class_label("streaming");
    auto system_label_instance = class_label("system");

    _metrics.add_group("memory", {
        sm::make_gauge("dirty_bytes", [this] { return _dirty_memory_manager.real_dirty_memory() + _system_dirty_memory_manager.real_dirty_memory() + _streaming_dirty_memory_manager.real_dirty_memory(); },
                       sm::description("Holds the current size of all (\"regular\", \"system\" and \"streaming\") non-free memory in bytes: used memory + released memory that hasn't been returned to a free memory pool yet. "
                                       "Total memory size minus this value represents the amount of available memory. "
                                       "If this value minus virtual_dirty_bytes is too high then this means that the dirty memory eviction lags behind.")),

        sm::make_gauge("virtual_dirty_bytes", [this] { return _dirty_memory_manager.virtual_dirty_memory() + _system_dirty_memory_manager.virtual_dirty_memory() + _streaming_dirty_memory_manager.virtual_dirty_memory(); },
                       sm::description("Holds the size of all (\"regular\", \"system\" and \"streaming\") used memory in bytes. Compare it to \"dirty_bytes\" to see how many memory is wasted (neither used nor available).")),
    });

    _metrics.add_group("memtables", {
        sm::make_gauge("pending_flushes", _cf_stats.pending_memtables_flushes_count,
                       sm::description("Holds the current number of memtables that are currently being flushed to sstables. "
                                       "High value in this metric may be an indication of storage being a bottleneck.")),

        sm::make_gauge("pending_flushes_bytes", _cf_stats.pending_memtables_flushes_bytes,
                       sm::description("Holds the current number of bytes in memtables that are currently being flushed to sstables. "
                                       "High value in this metric may be an indication of storage being a bottleneck.")),
    });

    _metrics.add_group("database", {
        sm::make_gauge("requests_blocked_memory_current", [this] { return _dirty_memory_manager.region_group().blocked_requests(); },
                       sm::description(
                           seastar::format("Holds the current number of requests blocked due to reaching the memory quota ({}B). "
                                           "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"database\" component.", _dirty_memory_manager.throttle_threshold()))),

        sm::make_derive("requests_blocked_memory", [this] { return _dirty_memory_manager.region_group().blocked_requests_counter(); },
                       sm::description(seastar::format("Holds the current number of requests blocked due to reaching the memory quota ({}B). "
                                       "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"database\" component.", _dirty_memory_manager.throttle_threshold()))),

        sm::make_derive("clustering_filter_count", _cf_stats.clustering_filter_count,
                       sm::description("Counts bloom filter invocations.")),

        sm::make_derive("clustering_filter_sstables_checked", _cf_stats.sstables_checked_by_clustering_filter,
                       sm::description("Counts sstables checked after applying the bloom filter. "
                                       "High value indicates that bloom filter is not very efficient.")),

        sm::make_derive("clustering_filter_fast_path_count", _cf_stats.clustering_filter_fast_path_count,
                       sm::description("Counts number of times bloom filtering short cut to include all sstables when only one full range was specified.")),

        sm::make_derive("clustering_filter_surviving_sstables", _cf_stats.surviving_sstables_after_clustering_filter,
                       sm::description("Counts sstables that survived the clustering key filtering. "
                                       "High value indicates that bloom filter is not very efficient and still have to access a lot of sstables to get data.")),

        sm::make_derive("dropped_view_updates", _cf_stats.dropped_view_updates,
                       sm::description("Counts the number of view updates that have been dropped due to cluster overload. ")),

        sm::make_derive("total_writes", _stats->total_writes,
                       sm::description("Counts the total number of successful write operations performed by this shard.")),

        sm::make_derive("total_writes_failed", _stats->total_writes_failed,
                       sm::description("Counts the total number of failed write operations. "
                                       "A sum of this value plus total_writes represents a total amount of writes attempted on this shard.")),

        sm::make_derive("total_writes_timedout", _stats->total_writes_timedout,
                       sm::description("Counts write operations failed due to a timeout. A positive value is a sign of storage being overloaded.")),

        sm::make_derive("total_reads", _stats->total_reads,
                       sm::description("Counts the total number of successful reads on this shard.")),

        sm::make_derive("total_reads_failed", _stats->total_reads_failed,
                       sm::description("Counts the total number of failed read operations. "
                                       "Add the total_reads to this value to get the total amount of reads issued on this shard.")),

        sm::make_current_bytes("view_update_backlog", [this] { return get_view_update_backlog().current; },
                       sm::description("Holds the current size in bytes of the pending view updates for all tables")),

        sm::make_derive("querier_cache_lookups", _querier_cache.get_stats().lookups,
                       sm::description("Counts querier cache lookups (paging queries)")),

        sm::make_derive("querier_cache_misses", _querier_cache.get_stats().misses,
                       sm::description("Counts querier cache lookups that failed to find a cached querier")),

        sm::make_derive("querier_cache_drops", _querier_cache.get_stats().drops,
                       sm::description("Counts querier cache lookups that found a cached querier but had to drop it due to position mismatch")),

        sm::make_derive("querier_cache_time_based_evictions", _querier_cache.get_stats().time_based_evictions,
                       sm::description("Counts querier cache entries that timed out and were evicted.")),

        sm::make_derive("querier_cache_resource_based_evictions", _querier_cache.get_stats().resource_based_evictions,
                       sm::description("Counts querier cache entries that were evicted to free up resources "
                                       "(limited by reader concurency limits) necessary to create new readers.")),

        sm::make_derive("querier_cache_memory_based_evictions", _querier_cache.get_stats().memory_based_evictions,
                       sm::description("Counts querier cache entries that were evicted because the memory usage "
                                       "of the cached queriers were above the limit.")),

        sm::make_gauge("querier_cache_population", _querier_cache.get_stats().population,
                       sm::description("The number of entries currently in the querier cache.")),

        sm::make_derive("sstable_read_queue_overloads", _stats->sstable_read_queue_overloaded,
                       sm::description("Counts the number of times the sstable read queue was overloaded. "
                                       "A non-zero value indicates that we have to drop read requests because they arrive faster than we can serve them.")),

        sm::make_gauge("active_reads", [this] { return max_count_concurrent_reads - _read_concurrency_sem.available_resources().count; },
                       sm::description("Holds the number of currently active read operations. "),
                       {user_label_instance}),

        sm::make_gauge("active_reads_memory_consumption", [this] { return max_memory_concurrent_reads() - _read_concurrency_sem.available_resources().memory; },
                       sm::description(seastar::format("Holds the amount of memory consumed by currently active read operations. "
                                                       "If this value gets close to {} we are likely to start dropping new read requests. "
                                                       "In that case sstable_read_queue_overloads is going to get a non-zero value.", max_memory_concurrent_reads())),
                       {user_label_instance}),

        sm::make_gauge("queued_reads", [this] { return _read_concurrency_sem.waiters(); },
                       sm::description("Holds the number of currently queued read operations."),
                       {user_label_instance}),

        sm::make_gauge("active_reads", [this] { return max_count_streaming_concurrent_reads - _streaming_concurrency_sem.available_resources().count; },
                       sm::description("Holds the number of currently active read operations issued on behalf of streaming "),
                       {streaming_label_instance}),


        sm::make_gauge("active_reads_memory_consumption", [this] { return max_memory_streaming_concurrent_reads() - _streaming_concurrency_sem.available_resources().memory; },
                       sm::description(seastar::format("Holds the amount of memory consumed by currently active read operations issued on behalf of streaming "
                                                       "If this value gets close to {} we are likely to start dropping new read requests. "
                                                       "In that case sstable_read_queue_overloads is going to get a non-zero value.", max_memory_streaming_concurrent_reads())),
                       {streaming_label_instance}),

        sm::make_gauge("queued_reads", [this] { return _streaming_concurrency_sem.waiters(); },
                       sm::description("Holds the number of currently queued read operations on behalf of streaming."),
                       {streaming_label_instance}),

        sm::make_gauge("active_reads", [this] { return max_count_system_concurrent_reads - _system_read_concurrency_sem.available_resources().count; },
                       sm::description("Holds the number of currently active read operations from \"system\" keyspace tables. "),
                       {system_label_instance}),

        sm::make_gauge("active_reads_memory_consumption", [this] { return max_memory_system_concurrent_reads() - _system_read_concurrency_sem.available_resources().memory; },
                       sm::description(seastar::format("Holds the amount of memory consumed by currently active read operations from \"system\" keyspace tables. "
                                                       "If this value gets close to {} we are likely to start dropping new read requests. "
                                                       "In that case sstable_read_queue_overloads is going to get a non-zero value.", max_memory_system_concurrent_reads())),
                       {system_label_instance}),

        sm::make_gauge("queued_reads", [this] { return _system_read_concurrency_sem.waiters(); },
                       sm::description("Holds the number of currently queued read operations from \"system\" keyspace tables."),
                       {system_label_instance}),

        sm::make_gauge("total_result_bytes", [this] { return get_result_memory_limiter().total_used_memory(); },
                       sm::description("Holds the current amount of memory used for results.")),

        sm::make_derive("short_data_queries", _stats->short_data_queries,
                       sm::description("The rate of data queries (data or digest reads) that returned less rows than requested due to result size limiting.")),

        sm::make_derive("short_mutation_queries", _stats->short_mutation_queries,
                       sm::description("The rate of mutation queries that returned less rows than requested due to result size limiting.")),

        sm::make_derive("multishard_query_unpopped_fragments", _stats->multishard_query_unpopped_fragments,
                       sm::description("The total number of fragments that were extracted from the shard reader but were unconsumed by the query and moved back into the reader.")),

        sm::make_derive("multishard_query_unpopped_bytes", _stats->multishard_query_unpopped_bytes,
                       sm::description("The total number of bytes that were extracted from the shard reader but were unconsumed by the query and moved back into the reader.")),

        sm::make_derive("multishard_query_failed_reader_stops", _stats->multishard_query_failed_reader_stops,
                       sm::description("The number of times the stopping of a shard reader failed.")),

        sm::make_derive("multishard_query_failed_reader_saves", _stats->multishard_query_failed_reader_saves,
                       sm::description("The number of times the saving of a shard reader failed.")),

        sm::make_total_operations("counter_cell_lock_acquisition", _cl_stats->lock_acquisitions,
                                 sm::description("The number of acquired counter cell locks.")),

        sm::make_queue_length("counter_cell_lock_pending", _cl_stats->operations_waiting_for_lock,
                             sm::description("The number of counter updates waiting for a lock.")),

        sm::make_counter("large_partition_exceeding_threshold", [this] { return _large_partition_handler->stats().partitions_bigger_than_threshold; },
            sm::description("Number of large partitions exceeding compaction_large_partition_warning_threshold_mb. "
                "Large partitions have performance impact and should be avoided, check the documentation for details.")),
    });
}

database::~database() {
    _read_concurrency_sem.clear_inactive_reads();
    _streaming_concurrency_sem.clear_inactive_reads();
    _system_read_concurrency_sem.clear_inactive_reads();
}

void database::update_version(const utils::UUID& version) {
    _version = version;
}

const utils::UUID& database::get_version() const {
    return _version;
}

future<> distributed_loader::populate_keyspace(distributed<database>& db, sstring datadir, sstring ks_name) {
    auto ksdir = datadir + "/" + ks_name;
    auto& keyspaces = db.local().get_keyspaces();
    auto i = keyspaces.find(ks_name);
    if (i == keyspaces.end()) {
        dblog.warn("Skipping undefined keyspace: {}", ks_name);
        return make_ready_future<>();
    } else {
        dblog.info("Populating Keyspace {}", ks_name);
        auto& ks = i->second;
        auto& column_families = db.local().get_column_families();

        return parallel_for_each(ks.metadata()->cf_meta_data() | boost::adaptors::map_values,
            [ks_name, ksdir, &ks, &column_families, &db] (schema_ptr s) {
                utils::UUID uuid = s->id();
                lw_shared_ptr<column_family> cf = column_families[uuid];
                sstring cfname = cf->schema()->cf_name();
                auto sstdir = ks.column_family_directory(ksdir, cfname, uuid);
                dblog.info("Keyspace {}: Reading CF {} id={} version={}", ks_name, cfname, uuid, s->version());
                return ks.make_directory_for_column_family(cfname, uuid).then([&db, sstdir, uuid, ks_name, cfname] {
                    return distributed_loader::populate_column_family(db, sstdir + "/staging", ks_name, cfname);
                }).then([&db, sstdir, uuid, ks_name, cfname] {
                    return distributed_loader::populate_column_family(db, sstdir, ks_name, cfname);
                }).handle_exception([ks_name, cfname, sstdir](std::exception_ptr eptr) {
                    std::string msg =
                        sprint("Exception while populating keyspace '%s' with column family '%s' from file '%s': %s",
                               ks_name, cfname, sstdir, eptr);
                    dblog.error("Exception while populating keyspace '{}' with column family '{}' from file '{}': {}",
                                ks_name, cfname, sstdir, eptr);
                    throw std::runtime_error(msg.c_str());
                });
            });
    }
}

static future<> populate(distributed<database>& db, sstring datadir) {
    return lister::scan_dir(datadir, { directory_entry_type::directory }, [&db] (lister::path datadir, directory_entry de) {
        auto& ks_name = de.name;
        if (is_system_keyspace(ks_name)) {
            return make_ready_future<>();
        }
        return distributed_loader::populate_keyspace(db, datadir.native(), ks_name);
    });
}

template <typename Func>
static future<>
do_parse_schema_tables(distributed<service::storage_proxy>& proxy, const sstring& _cf_name, Func&& func) {
    using namespace db::schema_tables;
    static_assert(std::is_same<future<>, std::result_of_t<Func(schema_result_value_type&)>>::value,
                  "bad Func signature");


    auto cf_name = make_lw_shared<sstring>(_cf_name);
    return db::system_keyspace::query(proxy, db::schema_tables::NAME, *cf_name).then([] (auto rs) {
        auto names = std::set<sstring>();
        for (auto& r : rs->rows()) {
            auto keyspace_name = r.template get_nonnull<sstring>("keyspace_name");
            names.emplace(keyspace_name);
        }
        return std::move(names);
    }).then([&proxy, cf_name, func = std::forward<Func>(func)] (std::set<sstring>&& names) mutable {
        return parallel_for_each(names.begin(), names.end(), [&proxy, cf_name, func = std::forward<Func>(func)] (sstring name) mutable {
            if (is_system_keyspace(name)) {
                return make_ready_future<>();
            }

            return read_schema_partition_for_keyspace(proxy, *cf_name, name).then([func, cf_name] (auto&& v) mutable {
                return do_with(std::move(v), [func = std::forward<Func>(func), cf_name] (auto& v) {
                    return func(v).then_wrapped([cf_name, &v] (future<> f) {
                        try {
                            f.get();
                        } catch (std::exception& e) {
                            dblog.error("Skipping: {}. Exception occurred when loading system table {}: {}", v.first, *cf_name, e.what());
                        }
                    });
                });
            });
        });
    });
}

future<> database::parse_system_tables(distributed<service::storage_proxy>& proxy) {
    using namespace db::schema_tables;
    return do_parse_schema_tables(proxy, db::schema_tables::KEYSPACES, [this] (schema_result_value_type &v) {
        auto ksm = create_keyspace_from_schema_partition(v);
        return create_keyspace(ksm);
    }).then([&proxy, this] {
        return do_parse_schema_tables(proxy, db::schema_tables::TYPES, [this, &proxy] (schema_result_value_type &v) {
            auto&& user_types = create_types_from_schema_partition(v);
            auto& ks = this->find_keyspace(v.first);
            for (auto&& type : user_types) {
                ks.add_user_type(type);
            }
            return make_ready_future<>();
        });
    }).then([&proxy, this] {
        return do_parse_schema_tables(proxy, db::schema_tables::TABLES, [this, &proxy] (schema_result_value_type &v) {
            return create_tables_from_tables_partition(proxy, v.second).then([this] (std::map<sstring, schema_ptr> tables) {
                return parallel_for_each(tables.begin(), tables.end(), [this] (auto& t) {
                    return this->add_column_family_and_make_directory(t.second);
                });
            });
            });
    }).then([&proxy, this] {
        return do_parse_schema_tables(proxy, db::schema_tables::VIEWS, [this, &proxy] (schema_result_value_type &v) {
            return create_views_from_schema_partition(proxy, v.second).then([this] (std::vector<view_ptr> views) {
                return parallel_for_each(views.begin(), views.end(), [this] (auto&& v) {
                    return this->add_column_family_and_make_directory(v);
                });
            });
        });
    });
}

future<> distributed_loader::init_system_keyspace(distributed<database>& db) {
    return seastar::async([&db] {
        // We need to init commitlog on shard0 before it is inited on other shards
        // because it obtains the list of pre-existing segments for replay, which must
        // not include reserve segments created by active commitlogs.
        db.invoke_on(0, [] (database& db) {
            return db.init_commitlog();
        }).get();
        db.invoke_on_all([] (database& db) {
            if (engine().cpu_id() == 0) {
                return make_ready_future<>();
            }
            return db.init_commitlog();
        }).get();

        db.invoke_on_all([] (database& db) {
            auto& cfg = db.get_config();
            bool durable = cfg.data_file_directories().size() > 0;
            db::system_keyspace::make(db, durable, cfg.volatile_system_keyspace_for_testing());
        }).get();

        const auto& cfg = db.local().get_config();
        for (auto& data_dir : cfg.data_file_directories()) {
            for (auto ksname : system_keyspaces) {
                io_check(touch_directory, data_dir + "/" + ksname).get();
                distributed_loader::populate_keyspace(db, data_dir, ksname).get();
            }
        }

        db.invoke_on_all([] (database& db) {
            for (auto ksname : system_keyspaces) {
                auto& ks = db.find_keyspace(ksname);
                for (auto& pair : ks.metadata()->cf_meta_data()) {
                    auto cfm = pair.second;
                    auto& cf = db.find_column_family(cfm);
                    cf.mark_ready_for_writes();
                }
            }
            return make_ready_future<>();
        }).get();
    });
}

future<> distributed_loader::ensure_system_table_directories(distributed<database>& db) {
    return parallel_for_each(system_keyspaces, [&db](sstring ksname) {
        auto& ks = db.local().find_keyspace(ksname);
        return parallel_for_each(ks.metadata()->cf_meta_data(), [&ks] (auto& pair) {
            auto cfm = pair.second;
            return ks.make_directory_for_column_family(cfm->cf_name(), cfm->id());
        });
    });
}

future<> distributed_loader::init_non_system_keyspaces(distributed<database>& db, distributed<service::storage_proxy>& proxy) {
    return seastar::async([&db, &proxy] {
        db.invoke_on_all([&proxy] (database& db) {
            return db.parse_system_tables(proxy);
        }).get();

        const auto& cfg = db.local().get_config();
        parallel_for_each(cfg.data_file_directories(), [&db] (sstring directory) {
            return populate(db, directory);
        }).get();

        db.invoke_on_all([] (database& db) {
            return parallel_for_each(db.get_non_system_column_families(), [] (lw_shared_ptr<table> table) {
                // Make sure this is called even if the table is empty
                table->mark_ready_for_writes();
                return make_ready_future<>();
            });
        }).get();
    });
}

future<>
database::init_commitlog() {
    return db::commitlog::create_commitlog(db::commitlog::config::from_db_config(*_cfg, _dbcfg.available_memory)).then([this](db::commitlog&& log) {
        _commitlog = std::make_unique<db::commitlog>(std::move(log));
        _commitlog->add_flush_handler([this](db::cf_id_type id, db::replay_position pos) {
            if (_column_families.count(id) == 0) {
                // the CF has been removed.
                _commitlog->discard_completed_segments(id);
                return;
            }
            _column_families[id]->flush();
        }).release(); // we have longer life time than CL. Ignore reg anchor
    });
}

unsigned
database::shard_of(const dht::token& t) {
    return dht::shard_of(t);
}

unsigned
database::shard_of(const mutation& m) {
    return shard_of(m.token());
}

unsigned
database::shard_of(const frozen_mutation& m) {
    // FIXME: This lookup wouldn't be necessary if we
    // sent the partition key in legacy form or together
    // with token.
    schema_ptr schema = find_schema(m.column_family_id());
    return shard_of(dht::global_partitioner().get_token(*schema, m.key(*schema)));
}

void database::add_keyspace(sstring name, keyspace k) {
    if (_keyspaces.count(name) != 0) {
        throw std::invalid_argument("Keyspace " + name + " already exists");
    }
    _keyspaces.emplace(std::move(name), std::move(k));
}

future<> database::update_keyspace(const sstring& name) {
    auto& proxy = service::get_storage_proxy();
    return db::schema_tables::read_schema_partition_for_keyspace(proxy, db::schema_tables::KEYSPACES, name).then([this, name](db::schema_tables::schema_result_value_type&& v) {
        auto& ks = find_keyspace(name);

        auto tmp_ksm = db::schema_tables::create_keyspace_from_schema_partition(v);
        auto new_ksm = ::make_lw_shared<keyspace_metadata>(tmp_ksm->name(), tmp_ksm->strategy_name(), tmp_ksm->strategy_options(), tmp_ksm->durable_writes(),
                        boost::copy_range<std::vector<schema_ptr>>(ks.metadata()->cf_meta_data() | boost::adaptors::map_values), ks.metadata()->user_types());
        ks.update_from(std::move(new_ksm));
        return service::get_local_migration_manager().notify_update_keyspace(ks.metadata());
    });
}

void database::drop_keyspace(const sstring& name) {
    _keyspaces.erase(name);
}

void database::add_column_family(keyspace& ks, schema_ptr schema, column_family::config cfg) {
    schema = local_schema_registry().learn(schema);
    schema->registry_entry()->mark_synced();

    lw_shared_ptr<column_family> cf;
    if (cfg.enable_commitlog && _commitlog) {
       cf = make_lw_shared<column_family>(schema, std::move(cfg), *_commitlog, *_compaction_manager, *_cl_stats, _row_cache_tracker);
    } else {
       cf = make_lw_shared<column_family>(schema, std::move(cfg), column_family::no_commitlog(), *_compaction_manager, *_cl_stats, _row_cache_tracker);
    }

    auto uuid = schema->id();
    if (_column_families.count(uuid) != 0) {
        throw std::invalid_argument("UUID " + uuid.to_sstring() + " already mapped");
    }
    auto kscf = std::make_pair(schema->ks_name(), schema->cf_name());
    if (_ks_cf_to_uuid.count(kscf) != 0) {
        throw std::invalid_argument("Column family " + schema->cf_name() + " exists");
    }
    ks.add_or_update_column_family(schema);
    cf->start();
    _column_families.emplace(uuid, std::move(cf));
    _ks_cf_to_uuid.emplace(std::move(kscf), uuid);
    if (schema->is_view()) {
        find_column_family(schema->view_info()->base_id()).add_or_update_view(view_ptr(schema));
    }
}

future<> database::add_column_family_and_make_directory(schema_ptr schema) {
    auto& ks = find_keyspace(schema->ks_name());
    add_column_family(ks, schema, ks.make_column_family_config(*schema, get_config(), get_large_partition_handler()));
    find_column_family(schema).get_index_manager().reload();
    return ks.make_directory_for_column_family(schema->cf_name(), schema->id());
}

bool database::update_column_family(schema_ptr new_schema) {
    column_family& cfm = find_column_family(new_schema->id());
    bool columns_changed = !cfm.schema()->equal_columns(*new_schema);
    auto s = local_schema_registry().learn(new_schema);
    s->registry_entry()->mark_synced();
    cfm.set_schema(s);
    find_keyspace(s->ks_name()).metadata()->add_or_update_column_family(s);
    if (s->is_view()) {
        try {
            find_column_family(s->view_info()->base_id()).add_or_update_view(view_ptr(s));
        } catch (no_such_column_family&) {
            // Update view mutations received after base table drop.
        }
    }
    cfm.get_index_manager().reload();
    return columns_changed;
}

void database::remove(const column_family& cf) {
    auto s = cf.schema();
    auto& ks = find_keyspace(s->ks_name());
    _querier_cache.evict_all_for_table(s->id());
    _column_families.erase(s->id());
    ks.metadata()->remove_column_family(s);
    _ks_cf_to_uuid.erase(std::make_pair(s->ks_name(), s->cf_name()));
    if (s->is_view()) {
        try {
            find_column_family(s->view_info()->base_id()).remove_view(view_ptr(s));
        } catch (no_such_column_family&) {
            // Drop view mutations received after base table drop.
        }
    }
}

future<> database::drop_column_family(const sstring& ks_name, const sstring& cf_name, timestamp_func tsf, bool snapshot) {
    auto uuid = find_uuid(ks_name, cf_name);
    auto cf = _column_families.at(uuid);
    remove(*cf);
    cf->clear_views();
    auto& ks = find_keyspace(ks_name);
    return when_all_succeed(cf->await_pending_writes(), cf->await_pending_reads()).then([this, &ks, cf, tsf = std::move(tsf), snapshot] {
        return truncate(ks, *cf, std::move(tsf), snapshot).finally([this, cf] {
            return cf->stop();
        });
    }).finally([cf] {});
}

const utils::UUID& database::find_uuid(const sstring& ks, const sstring& cf) const {
    try {
        return _ks_cf_to_uuid.at(std::make_pair(ks, cf));
    } catch (...) {
        throw std::out_of_range("");
    }
}

const utils::UUID& database::find_uuid(const schema_ptr& schema) const {
    return find_uuid(schema->ks_name(), schema->cf_name());
}

keyspace& database::find_keyspace(const sstring& name) {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

const keyspace& database::find_keyspace(const sstring& name) const {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

bool database::has_keyspace(const sstring& name) const {
    return _keyspaces.count(name) != 0;
}

std::vector<sstring>  database::get_non_system_keyspaces() const {
    std::vector<sstring> res;
    for (auto const &i : _keyspaces) {
        if (!is_system_keyspace(i.first)) {
            res.push_back(i.first);
        }
    }
    return res;
}

std::vector<lw_shared_ptr<column_family>> database::get_non_system_column_families() const {
    return boost::copy_range<std::vector<lw_shared_ptr<column_family>>>(
        get_column_families()
            | boost::adaptors::map_values
            | boost::adaptors::filtered([](const lw_shared_ptr<column_family>& cf) {
                return !is_system_keyspace(cf->schema()->ks_name());
            }));
}

column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

const column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) const {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

column_family& database::find_column_family(const utils::UUID& uuid) {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid));
    }
}

const column_family& database::find_column_family(const utils::UUID& uuid) const {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid));
    }
}

bool database::column_family_exists(const utils::UUID& uuid) const {
    return _column_families.count(uuid);
}

void
keyspace::create_replication_strategy(const std::map<sstring, sstring>& options) {
    using namespace locator;

    auto& ss = service::get_local_storage_service();
    _replication_strategy =
            abstract_replication_strategy::create_replication_strategy(
                _metadata->name(), _metadata->strategy_name(),
                ss.get_token_metadata(), options);
}

locator::abstract_replication_strategy&
keyspace::get_replication_strategy() {
    return *_replication_strategy;
}


const locator::abstract_replication_strategy&
keyspace::get_replication_strategy() const {
    return *_replication_strategy;
}

void
keyspace::set_replication_strategy(std::unique_ptr<locator::abstract_replication_strategy> replication_strategy) {
    _replication_strategy = std::move(replication_strategy);
}

void keyspace::update_from(::lw_shared_ptr<keyspace_metadata> ksm) {
    _metadata = std::move(ksm);
   create_replication_strategy(_metadata->strategy_options());
}

column_family::config
keyspace::make_column_family_config(const schema& s, const db::config& db_config, db::large_partition_handler* lp_handler) const {
    column_family::config cfg;
    for (auto& extra : _config.all_datadirs) {
        cfg.all_datadirs.push_back(column_family_directory(extra, s.cf_name(), s.id()));
    }
    cfg.datadir = cfg.all_datadirs[0];
    cfg.enable_disk_reads = _config.enable_disk_reads;
    cfg.enable_disk_writes = _config.enable_disk_writes;
    cfg.enable_commitlog = _config.enable_commitlog;
    cfg.enable_cache = _config.enable_cache;
    cfg.compaction_enforce_min_threshold = _config.compaction_enforce_min_threshold;
    cfg.dirty_memory_manager = _config.dirty_memory_manager;
    cfg.streaming_dirty_memory_manager = _config.streaming_dirty_memory_manager;
    cfg.read_concurrency_semaphore = _config.read_concurrency_semaphore;
    cfg.streaming_read_concurrency_semaphore = _config.streaming_read_concurrency_semaphore;
    cfg.cf_stats = _config.cf_stats;
    cfg.enable_incremental_backups = _config.enable_incremental_backups;
    cfg.compaction_scheduling_group = _config.compaction_scheduling_group;
    cfg.memory_compaction_scheduling_group = _config.memory_compaction_scheduling_group;
    cfg.memtable_scheduling_group = _config.memtable_scheduling_group;
    cfg.memtable_to_cache_scheduling_group = _config.memtable_to_cache_scheduling_group;
    cfg.streaming_scheduling_group = _config.streaming_scheduling_group;
    cfg.statement_scheduling_group = _config.statement_scheduling_group;
    cfg.enable_metrics_reporting = db_config.enable_keyspace_column_family_metrics();
    cfg.large_partition_handler = lp_handler;
    cfg.view_update_concurrency_semaphore = _config.view_update_concurrency_semaphore;
    cfg.view_update_concurrency_semaphore_limit = _config.view_update_concurrency_semaphore_limit;

    return cfg;
}

sstring
keyspace::column_family_directory(const sstring& name, utils::UUID uuid) const {
    return column_family_directory(_config.datadir, name, uuid);
}

sstring
keyspace::column_family_directory(const sstring& base_path, const sstring& name, utils::UUID uuid) const {
    auto uuid_sstring = uuid.to_sstring();
    boost::erase_all(uuid_sstring, "-");
    return sprint("%s/%s-%s", base_path, name, uuid_sstring);
}

future<>
keyspace::make_directory_for_column_family(const sstring& name, utils::UUID uuid) {
    std::vector<sstring> cfdirs;
    for (auto& extra : _config.all_datadirs) {
        cfdirs.push_back(column_family_directory(extra, name, uuid));
    }
    return seastar::async([cfdirs = std::move(cfdirs)] {
        for (auto& cfdir : cfdirs) {
            io_check(recursive_touch_directory, cfdir).get();
        }
        io_check(touch_directory, cfdirs[0] + "/upload").get();
        io_check(touch_directory, cfdirs[0] + "/staging").get();
    });
}

no_such_keyspace::no_such_keyspace(const sstring& ks_name)
    : runtime_error{sprint("Can't find a keyspace %s", ks_name)}
{
}

no_such_column_family::no_such_column_family(const utils::UUID& uuid)
    : runtime_error{sprint("Can't find a column family with UUID %s", uuid)}
{
}

no_such_column_family::no_such_column_family(const sstring& ks_name, const sstring& cf_name)
    : runtime_error{sprint("Can't find a column family %s in keyspace %s", cf_name, ks_name)}
{
}

column_family& database::find_column_family(const schema_ptr& schema) {
    return find_column_family(schema->id());
}

const column_family& database::find_column_family(const schema_ptr& schema) const {
    return find_column_family(schema->id());
}

using strategy_class_registry = class_registry<
    locator::abstract_replication_strategy,
    const sstring&,
    locator::token_metadata&,
    locator::snitch_ptr&,
    const std::map<sstring, sstring>&>;

keyspace_metadata::keyspace_metadata(sstring name,
             sstring strategy_name,
             std::map<sstring, sstring> strategy_options,
             bool durable_writes,
             std::vector<schema_ptr> cf_defs,
             lw_shared_ptr<user_types_metadata> user_types)
    : _name{std::move(name)}
    , _strategy_name{strategy_class_registry::to_qualified_class_name(strategy_name.empty() ? "NetworkTopologyStrategy" : strategy_name)}
    , _strategy_options{std::move(strategy_options)}
    , _durable_writes{durable_writes}
    , _user_types{std::move(user_types)}
{
    for (auto&& s : cf_defs) {
        _cf_meta_data.emplace(s->cf_name(), s);
    }
}

void keyspace_metadata::validate() const {
    using namespace locator;

    auto& ss = service::get_local_storage_service();
    abstract_replication_strategy::validate_replication_strategy(name(), strategy_name(), ss.get_token_metadata(), strategy_options());
}

std::vector<schema_ptr> keyspace_metadata::tables() const {
    return boost::copy_range<std::vector<schema_ptr>>(_cf_meta_data
            | boost::adaptors::map_values
            | boost::adaptors::filtered([] (auto&& s) { return !s->is_view(); }));
}

std::vector<view_ptr> keyspace_metadata::views() const {
    return boost::copy_range<std::vector<view_ptr>>(_cf_meta_data
            | boost::adaptors::map_values
            | boost::adaptors::filtered(std::mem_fn(&schema::is_view))
            | boost::adaptors::transformed([] (auto&& s) { return view_ptr(s); }));
}

schema_ptr database::find_schema(const sstring& ks_name, const sstring& cf_name) const {
    try {
        return find_schema(find_uuid(ks_name, cf_name));
    } catch (std::out_of_range&) {
        std::throw_with_nested(no_such_column_family(ks_name, cf_name));
    }
}

schema_ptr database::find_schema(const utils::UUID& uuid) const {
    return find_column_family(uuid).schema();
}

bool database::has_schema(const sstring& ks_name, const sstring& cf_name) const {
    return _ks_cf_to_uuid.count(std::make_pair(ks_name, cf_name)) > 0;
}

std::vector<view_ptr> database::get_views() const {
    return boost::copy_range<std::vector<view_ptr>>(get_non_system_column_families()
            | boost::adaptors::filtered([] (auto& cf) { return cf->schema()->is_view(); })
            | boost::adaptors::transformed([] (auto& cf) { return view_ptr(cf->schema()); }));
}

void database::create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    keyspace ks(ksm, std::move(make_keyspace_config(*ksm)));
    ks.create_replication_strategy(ksm->strategy_options());
    _keyspaces.emplace(ksm->name(), std::move(ks));
}

future<>
database::create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    auto i = _keyspaces.find(ksm->name());
    if (i != _keyspaces.end()) {
        return make_ready_future<>();
    }

    create_in_memory_keyspace(ksm);
    auto& datadir = _keyspaces.at(ksm->name()).datadir();
    if (datadir != "") {
        return io_check(touch_directory, datadir);
    } else {
        return make_ready_future<>();
    }
}

std::set<sstring>
database::existing_index_names(const sstring& ks_name, const sstring& cf_to_exclude) const {
    std::set<sstring> names;
    for (auto& schema : find_keyspace(ks_name).metadata()->tables()) {
        if (!cf_to_exclude.empty() && schema->cf_name() == cf_to_exclude) {
            continue;
        }
        for (const auto& index_name : schema->index_names()) {
            names.emplace(index_name);
        }
    }
    return names;
}

// Based on:
//  - org.apache.cassandra.db.AbstractCell#reconcile()
//  - org.apache.cassandra.db.BufferExpiringCell#reconcile()
//  - org.apache.cassandra.db.BufferDeletedCell#reconcile()
int
compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right) {
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() > right.timestamp() ? 1 : -1;
    }
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? -1 : 1;
    }
    if (left.is_live()) {
        auto c = compare_unsigned(left.value(), right.value());
        if (c != 0) {
            return c;
        }
        if (left.is_live_and_has_ttl() != right.is_live_and_has_ttl()) {
            // prefer expiring cells.
            return left.is_live_and_has_ttl() ? 1 : -1;
        }
        if (left.is_live_and_has_ttl() && left.expiry() != right.expiry()) {
            return left.expiry() < right.expiry() ? -1 : 1;
        }
    } else {
        // Both are deleted
        if (left.deletion_time() != right.deletion_time()) {
            // Origin compares big-endian serialized deletion time. That's because it
            // delegates to AbstractCell.reconcile() which compares values after
            // comparing timestamps, which in case of deleted cells will hold
            // serialized expiry.
            return (uint32_t) left.deletion_time().time_since_epoch().count()
                   < (uint32_t) right.deletion_time().time_since_epoch().count() ? -1 : 1;
        }
    }
    return 0;
}

struct query_state {
    explicit query_state(schema_ptr s,
                         const query::read_command& cmd,
                         query::result_options opts,
                         const dht::partition_range_vector& ranges,
                         query::result_memory_accounter memory_accounter = { })
            : schema(std::move(s))
            , cmd(cmd)
            , builder(cmd.slice, opts, std::move(memory_accounter))
            , limit(cmd.row_limit)
            , partition_limit(cmd.partition_limit)
            , current_partition_range(ranges.begin())
            , range_end(ranges.end()){
    }
    schema_ptr schema;
    const query::read_command& cmd;
    query::result::builder builder;
    uint32_t limit;
    uint32_t partition_limit;
    bool range_empty = false;   // Avoid ubsan false-positive when moving after construction
    dht::partition_range_vector::const_iterator current_partition_range;
    dht::partition_range_vector::const_iterator range_end;
    uint32_t remaining_rows() const {
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
        const query::read_command& cmd,
        query::result_options opts,
        const dht::partition_range_vector& partition_ranges,
        tracing::trace_state_ptr trace_state,
        query::result_memory_limiter& memory_limiter,
        uint64_t max_size,
        db::timeout_clock::time_point timeout,
        query::querier_cache_context cache_ctx) {
    utils::latency_counter lc;
    _stats.reads.set_latency(lc);
    auto f = opts.request == query::result_request::only_digest
             ? memory_limiter.new_digest_read(max_size) : memory_limiter.new_data_read(max_size);
    return f.then([this, lc, s = std::move(s), &cmd, opts, &partition_ranges,
            trace_state = std::move(trace_state), timeout, cache_ctx = std::move(cache_ctx)] (query::result_memory_accounter accounter) mutable {
        auto qs_ptr = std::make_unique<query_state>(std::move(s), cmd, opts, partition_ranges, std::move(accounter));
        auto& qs = *qs_ptr;
        return do_until(std::bind(&query_state::done, &qs), [this, &qs, trace_state = std::move(trace_state), timeout, cache_ctx = std::move(cache_ctx)] {
            auto&& range = *qs.current_partition_range++;
            return data_query(qs.schema, as_mutation_source(), range, qs.cmd.slice, qs.remaining_rows(),
                              qs.remaining_partitions(), qs.cmd.timestamp, qs.builder, trace_state, timeout, cache_ctx);
        }).then([qs_ptr = std::move(qs_ptr), &qs] {
            return make_ready_future<lw_shared_ptr<query::result>>(
                    make_lw_shared<query::result>(qs.builder.build()));
        }).finally([lc, this]() mutable {
            _stats.reads.mark(lc);
            if (lc.is_start()) {
                _stats.estimated_read.add(lc.latency(), _stats.reads.hist.count);
            }
        });
    });
}

mutation_source
table::as_mutation_source() const {
    return mutation_source([this] (schema_ptr s,
                                   const dht::partition_range& range,
                                   const query::partition_slice& slice,
                                   const io_priority_class& pc,
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr) {
        return this->make_reader(std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
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

future<lw_shared_ptr<query::result>, cache_temperature>
database::query(schema_ptr s, const query::read_command& cmd, query::result_options opts, const dht::partition_range_vector& ranges,
                tracing::trace_state_ptr trace_state, uint64_t max_result_size, db::timeout_clock::time_point timeout) {
    column_family& cf = find_column_family(cmd.cf_id);
    query::querier_cache_context cache_ctx(_querier_cache, cmd.query_uuid, cmd.is_first_page);
    return _data_query_stage(&cf,
            std::move(s),
            seastar::cref(cmd),
            opts,
            seastar::cref(ranges),
            std::move(trace_state),
            seastar::ref(get_result_memory_limiter()),
            max_result_size,
            timeout,
            std::move(cache_ctx)).then_wrapped([this, s = _stats, hit_rate = cf.get_global_cache_hit_rate(), op = cf.read_in_progress()] (auto f) {
        if (f.failed()) {
            ++s->total_reads_failed;
            return make_exception_future<lw_shared_ptr<query::result>, cache_temperature>(f.get_exception());
        } else {
            ++s->total_reads;
            auto result = f.get0();
            s->short_data_queries += bool(result->is_short_read());
            return make_ready_future<lw_shared_ptr<query::result>, cache_temperature>(std::move(result), hit_rate);
        }
    });
}

future<reconcilable_result, cache_temperature>
database::query_mutations(schema_ptr s, const query::read_command& cmd, const dht::partition_range& range,
                          query::result_memory_accounter&& accounter, tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout) {
    column_family& cf = find_column_family(cmd.cf_id);
    query::querier_cache_context cache_ctx(_querier_cache, cmd.query_uuid, cmd.is_first_page);
    return _mutation_query_stage(std::move(s),
            cf.as_mutation_source(),
            seastar::cref(range),
            seastar::cref(cmd.slice),
            cmd.row_limit,
            cmd.partition_limit,
            cmd.timestamp,
            std::move(accounter),
            std::move(trace_state),
            timeout,
            std::move(cache_ctx)).then_wrapped([this, s = _stats, hit_rate = cf.get_global_cache_hit_rate(), op = cf.read_in_progress()] (auto f) {
        if (f.failed()) {
            ++s->total_reads_failed;
            return make_exception_future<reconcilable_result, cache_temperature>(f.get_exception());
        } else {
            ++s->total_reads;
            auto result = f.get0();
            s->short_mutation_queries += bool(result.is_short_read());
            return make_ready_future<reconcilable_result, cache_temperature>(std::move(result), hit_rate);
        }
    });
}

std::unordered_set<sstring> database::get_initial_tokens() {
    std::unordered_set<sstring> tokens;
    sstring tokens_string = get_config().initial_token();
    try {
        boost::split(tokens, tokens_string, boost::is_any_of(sstring(", ")));
    } catch (...) {
        throw std::runtime_error(sprint("Unable to parse initial_token=%s", tokens_string));
    }
    tokens.erase("");
    return tokens;
}

std::experimental::optional<gms::inet_address> database::get_replace_address() {
    auto& cfg = get_config();
    sstring replace_address = cfg.replace_address();
    sstring replace_address_first_boot = cfg.replace_address_first_boot();
    try {
        if (!replace_address.empty()) {
            return gms::inet_address(replace_address);
        } else if (!replace_address_first_boot.empty()) {
            return gms::inet_address(replace_address_first_boot);
        }
        return std::experimental::nullopt;
    } catch (...) {
        return std::experimental::nullopt;
    }
}

bool database::is_replacing() {
    sstring replace_address_first_boot = get_config().replace_address_first_boot();
    if (!replace_address_first_boot.empty() && db::system_keyspace::bootstrap_complete()) {
        dblog.info("Replace address on first boot requested; this node is already bootstrapped");
        return false;
    }
    return bool(get_replace_address());
}

void database::register_connection_drop_notifier(netw::messaging_service& ms) {
    ms.register_connection_drop_notifier([this] (gms::inet_address ep) {
        dblog.debug("Drop hit rate info for {} because of disconnect", ep);
        for (auto&& cf : get_non_system_column_families()) {
            cf->drop_hit_rate(ep);
        }
    });
}

std::ostream& operator<<(std::ostream& out, const column_family& cf) {
    return fprint(out, "{column_family: %s/%s}", cf._schema->ks_name(), cf._schema->cf_name());
}

std::ostream& operator<<(std::ostream& out, const database& db) {
    out << "{\n";
    for (auto&& e : db._column_families) {
        auto&& cf = *e.second;
        out << "(" << e.first.to_sstring() << ", " << cf.schema()->cf_name() << ", " << cf.schema()->ks_name() << "): " << cf << "\n";
    }
    out << "}";
    return out;
}

template<typename... Args>
void table::do_apply(db::rp_handle&& h, Args&&... args) {
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
        _stats.estimated_write.add(lc.latency(), _stats.writes.hist.count);
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

future<mutation> database::do_apply_counter_update(column_family& cf, const frozen_mutation& fm, schema_ptr m_schema,
                                                   db::timeout_clock::time_point timeout,tracing::trace_state_ptr trace_state) {
    auto m = fm.unfreeze(m_schema);
    m.upgrade(cf.schema());

    // prepare partition slice
    std::vector<column_id> static_columns;
    static_columns.reserve(m.partition().static_row().size());
    m.partition().static_row().for_each_cell([&] (auto id, auto&&) {
        static_columns.emplace_back(id);
    });

    query::clustering_row_ranges cr_ranges;
    cr_ranges.reserve(8);
    std::vector<column_id> regular_columns;
    regular_columns.reserve(32);

    for (auto&& cr : m.partition().clustered_rows()) {
        cr_ranges.emplace_back(query::clustering_range::make_singular(cr.key()));
        cr.row().cells().for_each_cell([&] (auto id, auto&&) {
            regular_columns.emplace_back(id);
        });
    }

    boost::sort(regular_columns);
    regular_columns.erase(std::unique(regular_columns.begin(), regular_columns.end()),
                          regular_columns.end());

    auto slice = query::partition_slice(std::move(cr_ranges), std::move(static_columns),
        std::move(regular_columns), { }, { }, cql_serialization_format::internal(), query::max_rows);

    return do_with(std::move(slice), std::move(m), std::vector<locked_cell>(),
                   [this, &cf, timeout, trace_state = std::move(trace_state), op = cf.write_in_progress()] (const query::partition_slice& slice, mutation& m, std::vector<locked_cell>& locks) mutable {
        tracing::trace(trace_state, "Acquiring counter locks");
        return cf.lock_counter_cells(m, timeout).then([&, m_schema = cf.schema(), trace_state = std::move(trace_state), timeout, this] (std::vector<locked_cell> lcs) mutable {
            locks = std::move(lcs);

            // Before counter update is applied it needs to be transformed from
            // deltas to counter shards. To do that, we need to read the current
            // counter state for each modified cell...

            tracing::trace(trace_state, "Reading counter values from the CF");
            return counter_write_query(m_schema, cf.as_mutation_source(), m.decorated_key(), slice, trace_state)
                    .then([this, &cf, &m, m_schema, timeout, trace_state] (auto mopt) {
                // ...now, that we got existing state of all affected counter
                // cells we can look for our shard in each of them, increment
                // its clock and apply the delta.
                transform_counter_updates_to_shards(m, mopt ? &*mopt : nullptr, cf.failed_counter_applies_to_memtable());
                tracing::trace(trace_state, "Applying counter update");
                return this->apply_with_commitlog(cf, m, timeout);
            }).then([&m] {
                return std::move(m);
            });
        });
    });
}

void table::apply_streaming_mutation(schema_ptr m_schema, utils::UUID plan_id, const frozen_mutation& m, bool fragmented) {
    if (dblog.is_enabled(logging::log_level::trace)) {
        dblog.trace("streaming apply {}", m.pretty_printer(m_schema));
    }
    if (fragmented) {
        apply_streaming_big_mutation(std::move(m_schema), plan_id, m);
        return;
    }
    _streaming_memtables->active_memtable().apply(m, m_schema);
}

void table::apply_streaming_big_mutation(schema_ptr m_schema, utils::UUID plan_id, const frozen_mutation& m) {
    auto it = _streaming_memtables_big.find(plan_id);
    if (it == _streaming_memtables_big.end()) {
        it = _streaming_memtables_big.emplace(plan_id, make_lw_shared<streaming_memtable_big>()).first;
        it->second->memtables = _config.enable_disk_writes ? make_streaming_memtable_big_list(*it->second) : make_memory_only_memtable_list();
    }
    auto entry = it->second;
    entry->memtables->active_memtable().apply(m, m_schema);
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


future<> dirty_memory_manager::shutdown() {
    _db_shutdown_requested = true;
    _should_flush.signal();
    return std::move(_waiting_flush).then([this] {
        return _virtual_region_group.shutdown().then([this] {
            return _real_region_group.shutdown();
        });
    });
}

future<> memtable_list::request_flush() {
    if (!may_flush()) {
        return make_ready_future<>();
    } else if (!_flush_coalescing) {
        _flush_coalescing = shared_promise<>();
        _dirty_memory_manager->start_extraneous_flush();
        auto ef = defer([this] { _dirty_memory_manager->finish_extraneous_flush(); });
        return _dirty_memory_manager->get_flush_permit().then([this, ef = std::move(ef)] (auto permit) mutable {
            auto current_flush = std::move(*_flush_coalescing);
            _flush_coalescing = {};
            return _dirty_memory_manager->flush_one(*this, std::move(permit)).then_wrapped([this, ef = std::move(ef),
                                                                                            current_flush = std::move(current_flush)] (auto f) mutable {
                if (f.failed()) {
                    current_flush.set_exception(f.get_exception());
                } else {
                    current_flush.set_value();
                }
            });
        });
    } else {
        return _flush_coalescing->get_shared_future();
    }
}

lw_shared_ptr<memtable> memtable_list::new_memtable() {
    return make_lw_shared<memtable>(_current_schema(), *_dirty_memory_manager, this, _compaction_scheduling_group);
}

future<flush_permit> flush_permit::reacquire_sstable_write_permit() && {
    return _manager->get_flush_permit(std::move(_background_permit));
}

future<> dirty_memory_manager::flush_one(memtable_list& mtlist, flush_permit&& permit) {
    return mtlist.seal_active_memtable_immediate(std::move(permit)).handle_exception([this, schema = mtlist.back()->schema()] (std::exception_ptr ep) {
        dblog.error("Failed to flush memtable, {}:{} - {}", schema->ks_name(), schema->cf_name(), ep);
        return make_exception_future<>(ep);
    });
}

future<> dirty_memory_manager::flush_when_needed() {
    if (!_db) {
        return make_ready_future<>();
    }
    // If there are explicit flushes requested, we must wait for them to finish before we stop.
    return do_until([this] { return _db_shutdown_requested; }, [this] {
        auto has_work = [this] { return has_pressure() || _db_shutdown_requested; };
        return _should_flush.wait(std::move(has_work)).then([this] {
            return get_flush_permit().then([this] (auto permit) {
                // We give priority to explicit flushes. They are mainly user-initiated flushes,
                // flushes coming from a DROP statement, or commitlog flushes.
                if (_flush_serializer.waiters()) {
                    return make_ready_future<>();
                }
                // condition abated while we waited for the semaphore
                if (!this->has_pressure() || _db_shutdown_requested) {
                    return make_ready_future<>();
                }
                // There are many criteria that can be used to select what is the best memtable to
                // flush. Most of the time we want some coordination with the commitlog to allow us to
                // release commitlog segments as early as we can.
                //
                // But during pressure condition, we'll just pick the CF that holds the largest
                // memtable. The advantage of doing this is that this is objectively the one that will
                // release the biggest amount of memory and is less likely to be generating tiny
                // SSTables.
                memtable& candidate_memtable = memtable::from_region(*(this->_virtual_region_group.get_largest_region()));

                if (candidate_memtable.empty()) {
                    // Soft pressure, but nothing to flush. It could be due to fsync or memtable_to_cache lagging.
                    // Back off to avoid OOMing with flush continuations.
                    return sleep(1ms);
                }

                // Do not wait. The semaphore will protect us against a concurrent flush. But we
                // want to start a new one as soon as the permits are destroyed and the semaphore is
                // made ready again, not when we are done with the current one.
                this->flush_one(*(candidate_memtable.get_memtable_list()), std::move(permit));
                return make_ready_future<>();
            });
        });
    }).finally([this] {
        // We'll try to acquire the permit here to make sure we only really stop when there are no
        // in-flight flushes. Our stop condition checks for the presence of waiters, but it could be
        // that we have no waiters, but a flush still in flight. We wait for all background work to
        // stop. When that stops, we know that the foreground work in the _flush_serializer has
        // stopped as well.
        return get_units(_background_work_flush_serializer, _max_background_work).discard_result();
    });
}

void dirty_memory_manager::start_reclaiming() noexcept {
    _should_flush.signal();
}

future<> database::apply_in_memory(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&& h, db::timeout_clock::time_point timeout) {
    auto& cf = find_column_family(m.column_family_id());
    return cf.dirty_memory_region_group().run_when_memory_available([this, &m, m_schema = std::move(m_schema), h = std::move(h)]() mutable {
        try {
            auto& cf = find_column_family(m.column_family_id());
            cf.apply(m, m_schema, std::move(h));
        } catch (no_such_column_family&) {
            dblog.error("Attempting to mutate non-existent table {}", m.column_family_id());
        }
    }, timeout);
}

future<> database::apply_in_memory(const mutation& m, column_family& cf, db::rp_handle&& h, db::timeout_clock::time_point timeout) {
    return cf.dirty_memory_region_group().run_when_memory_available([this, &m, &cf, h = std::move(h)]() mutable {
        cf.apply(m, std::move(h));
    }, timeout);
}

future<mutation> database::apply_counter_update(schema_ptr s, const frozen_mutation& m, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_state) {
  return update_write_metrics(seastar::futurize_apply([&] {
    if (!s->is_synced()) {
        throw std::runtime_error(sprint("attempted to mutate using not synced schema of %s.%s, version=%s",
                                        s->ks_name(), s->cf_name(), s->version()));
    }
    try {
        auto& cf = find_column_family(m.column_family_id());
        return do_apply_counter_update(cf, m, s, timeout, std::move(trace_state));
    } catch (no_such_column_family&) {
        dblog.error("Attempting to mutate non-existent table {}", m.column_family_id());
        throw;
    }
  }));
}

static future<> maybe_handle_reorder(std::exception_ptr exp) {
    try {
        std::rethrow_exception(exp);
        return make_exception_future(exp);
    } catch (mutation_reordered_with_truncate_exception&) {
        // This mutation raced with a truncate, so we can just drop it.
        dblog.debug("replay_position reordering detected");
        return make_ready_future<>();
    }
}

future<> database::apply_with_commitlog(column_family& cf, const mutation& m, db::timeout_clock::time_point timeout) {
    if (cf.commitlog() != nullptr) {
        return do_with(freeze(m), [this, &m, &cf, timeout] (frozen_mutation& fm) {
            commitlog_entry_writer cew(m.schema(), fm);
            return cf.commitlog()->add_entry(m.schema()->id(), cew, timeout);
        }).then([this, &m, &cf, timeout] (db::rp_handle h) {
            return apply_in_memory(m, cf, std::move(h), timeout).handle_exception(maybe_handle_reorder);
        });
    }
    return apply_in_memory(m, cf, {}, timeout);
}

future<> database::apply_with_commitlog(schema_ptr s, column_family& cf, utils::UUID uuid, const frozen_mutation& m, db::timeout_clock::time_point timeout) {
    auto cl = cf.commitlog();
    if (cl != nullptr) {
        commitlog_entry_writer cew(s, m);
        return cf.commitlog()->add_entry(uuid, cew, timeout).then([&m, this, s, timeout, cl](db::rp_handle h) {
            return this->apply_in_memory(m, s, std::move(h), timeout).handle_exception(maybe_handle_reorder);
        });
    }
    return apply_in_memory(m, std::move(s), {}, timeout);
}

future<> database::do_apply(schema_ptr s, const frozen_mutation& m, db::timeout_clock::time_point timeout) {
    // I'm doing a nullcheck here since the init code path for db etc
    // is a little in flux and commitlog is created only when db is
    // initied from datadir.
    auto uuid = m.column_family_id();
    auto& cf = find_column_family(uuid);
    if (!s->is_synced()) {
        throw std::runtime_error(sprint("attempted to mutate using not synced schema of %s.%s, version=%s",
                                 s->ks_name(), s->cf_name(), s->version()));
    }

    // Signal to view building code that a write is in progress,
    // so it knows when new writes start being sent to a new view.
    auto op = cf.write_in_progress();
    if (cf.views().empty()) {
        return apply_with_commitlog(std::move(s), cf, std::move(uuid), m, timeout).finally([op = std::move(op)] { });
    }
    future<row_locker::lock_holder> f = cf.push_view_replica_updates(s, m, timeout);
    return f.then([this, s = std::move(s), uuid = std::move(uuid), &m, timeout, &cf, op = std::move(op)] (row_locker::lock_holder lock) mutable {
        return apply_with_commitlog(std::move(s), cf, std::move(uuid), m, timeout).finally(
                // Hold the local lock on the base-table partition or row
                // taken before the read, until the update is done.
                [lock = std::move(lock), op = std::move(op)] { });
    });
}

template<typename Future>
Future database::update_write_metrics(Future&& f) {
    return f.then_wrapped([this, s = _stats] (auto f) {
        if (f.failed()) {
            ++s->total_writes_failed;
            try {
                f.get();
            } catch (const timed_out_error&) {
                ++s->total_writes_timedout;
                throw;
            }
            assert(0 && "should not reach");
        }
        ++s->total_writes;
        return f;
    });
}

future<> database::apply(schema_ptr s, const frozen_mutation& m, db::timeout_clock::time_point timeout) {
    if (dblog.is_enabled(logging::log_level::trace)) {
        dblog.trace("apply {}", m.pretty_printer(s));
    }
    return update_write_metrics(_apply_stage(this, std::move(s), seastar::cref(m), timeout));
}

future<> database::apply_streaming_mutation(schema_ptr s, utils::UUID plan_id, const frozen_mutation& m, bool fragmented) {
    if (!s->is_synced()) {
        throw std::runtime_error(sprint("attempted to mutate using not synced schema of %s.%s, version=%s",
                                 s->ks_name(), s->cf_name(), s->version()));
    }
    return with_scheduling_group(_dbcfg.streaming_scheduling_group, [this, s = std::move(s), &m, fragmented, plan_id] () mutable {
        return _streaming_dirty_memory_manager.region_group().run_when_memory_available([this, &m, plan_id, fragmented, s = std::move(s)] {
            auto uuid = m.column_family_id();
            auto& cf = find_column_family(uuid);
            cf.apply_streaming_mutation(s, plan_id, std::move(m), fragmented);
        });
    });
}

keyspace::config
database::make_keyspace_config(const keyspace_metadata& ksm) {
    keyspace::config cfg;
    if (_cfg->data_file_directories().size() > 0) {
        cfg.datadir = sprint("%s/%s", _cfg->data_file_directories()[0], ksm.name());
        for (auto& extra : _cfg->data_file_directories()) {
            cfg.all_datadirs.push_back(sprint("%s/%s", extra, ksm.name()));
        }
        cfg.enable_disk_writes = !_cfg->enable_in_memory_data_store();
        cfg.enable_disk_reads = true; // we allways read from disk
        cfg.enable_commitlog = ksm.durable_writes() && _cfg->enable_commitlog() && !_cfg->enable_in_memory_data_store();
        cfg.enable_cache = _cfg->enable_cache();

    } else {
        cfg.datadir = "";
        cfg.enable_disk_writes = false;
        cfg.enable_disk_reads = false;
        cfg.enable_commitlog = false;
        cfg.enable_cache = false;
    }
    cfg.compaction_enforce_min_threshold = _cfg->compaction_enforce_min_threshold();
    cfg.dirty_memory_manager = &_dirty_memory_manager;
    cfg.streaming_dirty_memory_manager = &_streaming_dirty_memory_manager;
    cfg.read_concurrency_semaphore = &_read_concurrency_sem;
    cfg.streaming_read_concurrency_semaphore = &_streaming_concurrency_sem;
    cfg.cf_stats = &_cf_stats;
    cfg.enable_incremental_backups = _enable_incremental_backups;

    cfg.compaction_scheduling_group = _dbcfg.compaction_scheduling_group;
    cfg.memory_compaction_scheduling_group = _dbcfg.memory_compaction_scheduling_group;
    cfg.memtable_scheduling_group = _dbcfg.memtable_scheduling_group;
    cfg.memtable_to_cache_scheduling_group = _dbcfg.memtable_to_cache_scheduling_group;
    cfg.streaming_scheduling_group = _dbcfg.streaming_scheduling_group;
    cfg.statement_scheduling_group = _dbcfg.statement_scheduling_group;
    cfg.enable_metrics_reporting = _cfg->enable_keyspace_column_family_metrics();

    cfg.view_update_concurrency_semaphore = &_view_update_concurrency_sem;
    cfg.view_update_concurrency_semaphore_limit = max_memory_pending_view_updates();
    return cfg;
}

namespace db {

std::ostream& operator<<(std::ostream& os, const write_type& t) {
    switch(t) {
        case write_type::SIMPLE: return os << "SIMPLE";
        case write_type::BATCH: return os << "BATCH";
        case write_type::UNLOGGED_BATCH: return os << "UNLOGGED_BATCH";
        case write_type::COUNTER: return os << "COUNTER";
        case write_type::BATCH_LOG: return os << "BATCH_LOG";
        case write_type::CAS: return os << "CAS";
        case write_type::VIEW: return os << "VIEW";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, db::consistency_level cl) {
    switch (cl) {
    case db::consistency_level::ANY: return os << "ANY";
    case db::consistency_level::ONE: return os << "ONE";
    case db::consistency_level::TWO: return os << "TWO";
    case db::consistency_level::THREE: return os << "THREE";
    case db::consistency_level::QUORUM: return os << "QUORUM";
    case db::consistency_level::ALL: return os << "ALL";
    case db::consistency_level::LOCAL_QUORUM: return os << "LOCAL_QUORUM";
    case db::consistency_level::EACH_QUORUM: return os << "EACH_QUORUM";
    case db::consistency_level::SERIAL: return os << "SERIAL";
    case db::consistency_level::LOCAL_SERIAL: return os << "LOCAL_SERIAL";
    case db::consistency_level::LOCAL_ONE: return os << "LOCAL_ONE";
    default: abort();
    }
}

}

std::ostream&
operator<<(std::ostream& os, const exploded_clustering_prefix& ecp) {
    // Can't pass to_hex() to transformed(), since it is overloaded, so wrap:
    auto enhex = [] (auto&& x) { return to_hex(x); };
    return fprint(os, "prefix{%s}", ::join(":", ecp._v | boost::adaptors::transformed(enhex)));
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell_view& acv) {
    if (acv.is_live()) {
        return fprint(os, "atomic_cell{%s;ts=%d;expiry=%d,ttl=%d}",
            to_hex(acv.value().linearize()),
            acv.timestamp(),
            acv.is_live_and_has_ttl() ? acv.expiry().time_since_epoch().count() : -1,
            acv.is_live_and_has_ttl() ? acv.ttl().count() : 0);
    } else {
        return fprint(os, "atomic_cell{DEAD;ts=%d;deletion_time=%d}",
            acv.timestamp(), acv.deletion_time().time_since_epoch().count());
    }
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell& ac) {
    return os << atomic_cell_view(ac);
}

sstring database::get_available_index_name(const sstring &ks_name, const sstring &cf_name,
                                           std::experimental::optional<sstring> index_name_root) const
{
    auto existing_names = existing_index_names(ks_name);
    auto base_name = index_metadata::get_default_index_name(cf_name, index_name_root);
    sstring accepted_name = base_name;
    int i = 0;
    while (existing_names.count(accepted_name) > 0) {
        accepted_name = base_name + "_" + std::to_string(++i);
    }
    return accepted_name;
}

schema_ptr database::find_indexed_table(const sstring& ks_name, const sstring& index_name) const {
    for (auto& schema : find_keyspace(ks_name).metadata()->tables()) {
        if (schema->has_index(index_name)) {
            return schema;
        }
    }
    return nullptr;
}

future<>
database::stop() {
    return _compaction_manager->stop().then([this] {
        // try to ensure that CL has done disk flushing
        if (_commitlog != nullptr) {
            return _commitlog->shutdown();
        }
        return make_ready_future<>();
    }).then([this] {
        return parallel_for_each(_column_families, [this] (auto& val_pair) {
            return val_pair.second->stop();
        });
    }).then([this] {
        return _view_update_concurrency_sem.wait(max_memory_pending_view_updates());
    }).then([this] {
        if (_commitlog != nullptr) {
            return _commitlog->release();
        }
        return make_ready_future<>();
    }).then([this] {
        return _system_dirty_memory_manager.shutdown();
    }).then([this] {
        return _dirty_memory_manager.shutdown();
    }).then([this] {
        return _streaming_dirty_memory_manager.shutdown();
    }).then([this] {
        return _memtable_controller.shutdown();
    });
}

future<> database::flush_all_memtables() {
    return parallel_for_each(_column_families, [this] (auto& cfp) {
        return cfp.second->flush();
    });
}

future<> database::truncate(sstring ksname, sstring cfname, timestamp_func tsf) {
    auto& ks = find_keyspace(ksname);
    auto& cf = find_column_family(ksname, cfname);
    return truncate(ks, cf, std::move(tsf));
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

future<> database::truncate(const keyspace& ks, column_family& cf, timestamp_func tsf, bool with_snapshot) {
    return cf.run_async([this, &ks, &cf, tsf = std::move(tsf), with_snapshot] {
        const auto durable = ks.metadata()->durable_writes();
        const auto auto_snapshot = with_snapshot && get_config().auto_snapshot();
        const auto should_flush = durable || auto_snapshot;

        // Force mutations coming in to re-acquire higher rp:s
        // This creates a "soft" ordering, in that we will guarantee that
        // any sstable written _after_ we issue the flush below will
        // only have higher rp:s than we will get from the discard_sstable
        // call.
        auto low_mark = cf.set_low_replay_position_mark();


        return cf.run_with_compaction_disabled([this, &cf, should_flush, auto_snapshot, tsf = std::move(tsf), low_mark]() mutable {
            future<> f = make_ready_future<>();
            if (should_flush) {
                // TODO:
                // this is not really a guarantee at all that we've actually
                // gotten all things to disk. Again, need queue-ish or something.
                f = cf.flush();
            } else {
                f = cf.clear();
            }
            return f.then([this, &cf, auto_snapshot, tsf = std::move(tsf), low_mark, should_flush] {
                dblog.debug("Discarding sstable data for truncated CF + indexes");
                // TODO: notify truncation

                return tsf().then([this, &cf, auto_snapshot, low_mark, should_flush](db_clock::time_point truncated_at) {
                    future<> f = make_ready_future<>();
                    if (auto_snapshot) {
                        auto name = sprint("%d-%s", truncated_at.time_since_epoch().count(), cf.schema()->cf_name());
                        f = cf.snapshot(name);
                    }
                    return f.then([this, &cf, truncated_at, low_mark, should_flush] {
                        return cf.discard_sstables(truncated_at).then([this, &cf, truncated_at, low_mark, should_flush](db::replay_position rp) {
                            // TODO: indexes.
                            // Note: since discard_sstables was changed to only count tables owned by this shard,
                            // we can get zero rp back. Changed assert, and ensure we save at least low_mark.
                            assert(low_mark <= rp || rp == db::replay_position());
                            rp = std::max(low_mark, rp);
                            return truncate_views(cf, truncated_at, should_flush).then([&cf, truncated_at, rp] {
                                return db::system_keyspace::save_truncation_record(cf, truncated_at, rp);
                            });
                        });
                    });
                });
            });
        });
    });
}

future<> database::truncate_views(const column_family& base, db_clock::time_point truncated_at, bool should_flush) {
    return parallel_for_each(base.views(), [this, truncated_at, should_flush] (view_ptr v) {
        auto& vcf = find_column_family(v);
        return vcf.run_with_compaction_disabled([&vcf, truncated_at, should_flush] {
            return (should_flush ? vcf.flush() : vcf.clear()).then([&vcf, truncated_at, should_flush] {
                return vcf.discard_sstables(truncated_at).then([&vcf, truncated_at, should_flush](db::replay_position rp) {
                    return db::system_keyspace::save_truncation_record(vcf, truncated_at, rp);
                });
            });
        });
    });
}

const sstring& database::get_snitch_name() const {
    return _cfg->endpoint_snitch();
}

// For the filesystem operations, this code will assume that all keyspaces are visible in all shards
// (as we have been doing for a lot of the other operations, like the snapshot itself).
future<> database::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names) {
    namespace bf = boost::filesystem;

    std::vector<sstring> data_dirs = _cfg->data_file_directories();
    lw_shared_ptr<lister::dir_entry_types> dirs_only_entries_ptr = make_lw_shared<lister::dir_entry_types>({ directory_entry_type::directory });
    lw_shared_ptr<sstring> tag_ptr = make_lw_shared<sstring>(std::move(tag));
    std::unordered_set<sstring> ks_names_set(keyspace_names.begin(), keyspace_names.end());

    return parallel_for_each(data_dirs, [this, tag_ptr, ks_names_set = std::move(ks_names_set), dirs_only_entries_ptr] (const sstring& parent_dir) {
        std::unique_ptr<lister::filter_type> filter = std::make_unique<lister::filter_type>([] (const lister::path& parent_dir, const directory_entry& dir_entry) { return true; });

        // if specific keyspaces names were given - filter only these keyspaces directories
        if (!ks_names_set.empty()) {
            filter = std::make_unique<lister::filter_type>([ks_names_set = std::move(ks_names_set)] (const lister::path& parent_dir, const directory_entry& dir_entry) {
                return ks_names_set.find(dir_entry.name) != ks_names_set.end();
            });
        }

        //
        // The keyspace data directories and their snapshots are arranged as follows:
        //
        //  <data dir>
        //  |- <keyspace name1>
        //  |  |- <column family name1>
        //  |     |- snapshots
        //  |        |- <snapshot name1>
        //  |          |- <snapshot file1>
        //  |          |- <snapshot file2>
        //  |          |- ...
        //  |        |- <snapshot name2>
        //  |        |- ...
        //  |  |- <column family name2>
        //  |  |- ...
        //  |- <keyspace name2>
        //  |- ...
        //
        return lister::scan_dir(parent_dir, *dirs_only_entries_ptr, [this, tag_ptr, dirs_only_entries_ptr] (lister::path parent_dir, directory_entry de) {
            // KS directory
            return lister::scan_dir(parent_dir / de.name.c_str(), *dirs_only_entries_ptr, [this, tag_ptr, dirs_only_entries_ptr] (lister::path parent_dir, directory_entry de) mutable {
                // CF directory
                return lister::scan_dir(parent_dir / de.name.c_str(), *dirs_only_entries_ptr, [this, tag_ptr, dirs_only_entries_ptr] (lister::path parent_dir, directory_entry de) mutable {
                    // "snapshots" directory
                    lister::path snapshots_dir(parent_dir / de.name.c_str());
                    if (tag_ptr->empty()) {
                        dblog.info("Removing {}", snapshots_dir.native());
                        // kill the whole "snapshots" subdirectory
                        return lister::rmdir(std::move(snapshots_dir));
                    } else {
                        return lister::scan_dir(std::move(snapshots_dir), *dirs_only_entries_ptr, [this, tag_ptr] (lister::path parent_dir, directory_entry de) {
                            lister::path snapshot_dir(parent_dir / de.name.c_str());
                            dblog.info("Removing {}", snapshot_dir.native());
                            return lister::rmdir(std::move(snapshot_dir));
                        }, [tag_ptr] (const lister::path& parent_dir, const directory_entry& dir_entry) { return dir_entry.name == *tag_ptr; });
                    }
                 }, [] (const lister::path& parent_dir, const directory_entry& dir_entry) { return dir_entry.name == "snapshots"; });
            });
        }, *filter);
    });
}

future<> update_schema_version_and_announce(distributed<service::storage_proxy>& proxy)
{
    return db::schema_tables::calculate_schema_digest(proxy).then([&proxy] (utils::UUID uuid) {
        return proxy.local().get_db().invoke_on_all([uuid] (database& db) {
            db.update_version(uuid);
            return make_ready_future<>();
        }).then([uuid] {
            return db::system_keyspace::update_schema_version(uuid).then([uuid] {
                dblog.info("Schema version changed to {}", uuid);
                return service::get_local_migration_manager().passive_announce(uuid);
            });
        });
    });
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
    semaphore requests;
    semaphore manifest_write;
    snapshot_manager() : requests(0), manifest_write(0) {}
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

    dblog.debug("Storing manifest {}", jsonfile);

    return io_check(recursive_touch_directory, jsondir).then([jsonfile, json = std::move(json)] {
        return open_checked_file_dma(general_disk_error_handler, jsonfile, open_flags::wo | open_flags::create | open_flags::truncate).then([json](file f) {
            return do_with(make_file_output_stream(std::move(f)), [json] (output_stream<char>& out) {
                return out.write(json.c_str(), json.size()).then([&out] {
                   return out.flush();
                }).then([&out] {
                   return out.close();
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

future<> table::snapshot(sstring name) {
    return flush().then([this, name = std::move(name)]() {
       return with_semaphore(_sstable_deletion_sem, 1, [this, name = std::move(name)]() {
        auto tables = boost::copy_range<std::vector<sstables::shared_sstable>>(*_sstables->all());
        return do_with(std::move(tables), [this, name](std::vector<sstables::shared_sstable> & tables) {
            auto jsondir = _config.datadir + "/snapshots/" + name;
            return io_check(recursive_touch_directory, jsondir).then([this, name, jsondir, &tables] {
                return parallel_for_each(tables, [name](sstables::shared_sstable sstable) {
                    auto dir = sstable->get_dir() + "/snapshots/" + name;
                    return io_check(recursive_touch_directory, dir).then([sstable, dir] {
                        return sstable->create_links(dir).then_wrapped([] (future<> f) {
                            // If the SSTables are shared, one of the CPUs will fail here.
                            // That is completely fine, though. We only need one link.
                            try {
                                f.get();
                            } catch (std::system_error& e) {
                                if (e.code() != std::error_code(EEXIST, std::system_category())) {
                                    throw;
                                }
                            }
                            return make_ready_future<>();
                        });
                    });
                });
            }).then([jsondir, &tables] {
                return io_check(sync_directory, std::move(jsondir));
            }).finally([this, &tables, jsondir] {
                auto shard = std::hash<sstring>()(jsondir) % smp::count;
                std::unordered_set<sstring> table_names;
                for (auto& sst : tables) {
                    auto f = sst->get_filename();
                    auto rf = f.substr(sst->get_dir().size() + 1);
                    table_names.insert(std::move(rf));
                }
                return smp::submit_to(shard, [requester = engine().cpu_id(), jsondir = std::move(jsondir),
                                              tables = std::move(table_names), datadir = _config.datadir] {

                    if (pending_snapshots.count(jsondir) == 0) {
                        pending_snapshots.emplace(jsondir, make_lw_shared<snapshot_manager>());
                    }
                    auto snapshot = pending_snapshots.at(jsondir);
                    for (auto&& sst: tables) {
                        snapshot->files.insert(std::move(sst));
                    }

                    snapshot->requests.signal(1);
                    auto my_work = make_ready_future<>();
                    if (requester == engine().cpu_id()) {
                        my_work = snapshot->requests.wait(smp::count).then([jsondir = std::move(jsondir),
                                                                            snapshot] () mutable {
                            return seal_snapshot(jsondir).then([snapshot] {
                                snapshot->manifest_write.signal(smp::count);
                                return make_ready_future<>();
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
            lister::path snapshots_dir = lister::path(datadir) / "snapshots";
            auto file_exists = io_check([&snapshots_dir] { return engine().file_exists(snapshots_dir.native()); }).get0();
            if (!file_exists) {
                continue;
            }

            lister::scan_dir(snapshots_dir,  { directory_entry_type::directory }, [this, datadir, &all_snapshots] (lister::path snapshots_dir, directory_entry de) {
                auto snapshot_name = de.name;
                all_snapshots.emplace(snapshot_name, snapshot_details());
                return lister::scan_dir(snapshots_dir / snapshot_name.c_str(),  { directory_entry_type::regular }, [this, datadir, &all_snapshots, snapshot_name] (lister::path snapshot_dir, directory_entry de) {
                    return io_check(file_size, (snapshot_dir / de.name.c_str()).native()).then([this, datadir, &all_snapshots, snapshot_name, snapshot_dir, name = de.name] (auto size) {
                        // The manifest is the only file expected to be in this directory not belonging to the SSTable.
                        // For it, we account the total size, but zero it for the true size calculation.
                        //
                        // All the others should just generate an exception: there is something wrong, so don't blindly
                        // add it to the size.
                        if (name != "manifest.json") {
                            sstables::entry_descriptor::make_descriptor(snapshot_dir.native(), name);
                            all_snapshots.at(snapshot_name).total += size;
                        } else {
                            size = 0;
                        }
                        return io_check(file_size, (lister::path(datadir) / name.c_str()).native()).then_wrapped([&all_snapshots, snapshot_name, size] (auto fut) {
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

future<> table::flush() {
    return _memtables->request_flush();
}

// FIXME: We can do much better than this in terms of cache management. Right
// now, we only have to flush the touched ranges because of the possibility of
// streaming containing token ownership changes.
//
// Right now we can't differentiate between that and a normal repair process,
// so we always flush. When we can differentiate those streams, we should not
// be indiscriminately touching the cache during repair. We will just have to
// invalidate the entries that are relevant to things we already have in the cache.
future<> table::flush_streaming_mutations(utils::UUID plan_id, dht::partition_range_vector ranges) {
    // This will effectively take the gate twice for this call. The proper way to fix that would
    // be to change seal_active_streaming_memtable_delayed to take a range parameter. However, we
    // need this code to go away as soon as we can (see FIXME above). So the double gate is a better
    // temporary counter measure.
    dblog.debug("Flushing streaming memtable, plan={}", plan_id);
    return with_gate(_streaming_flush_gate, [this, plan_id, ranges = std::move(ranges)] () mutable {
        return flush_streaming_big_mutations(plan_id).then([this, ranges = std::move(ranges)] (auto sstables) mutable {
            return _streaming_memtables->seal_active_memtable_delayed().then([this] {
                return _streaming_flush_phaser.advance_and_await();
            }).then([this, sstables = std::move(sstables), ranges = std::move(ranges)] () mutable {
                return _cache.invalidate([this, sstables = std::move(sstables)] () mutable noexcept {
                    // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
                    for (auto&& sst : sstables) {
                        // seal_active_streaming_memtable_big() ensures sst is unshared.
                        this->add_sstable(sst.sstable, {engine().cpu_id()});
                    }
                    this->try_trigger_compaction();
                }, std::move(ranges));
            });
        });
    });
}

future<std::vector<table::monitored_sstable>> table::flush_streaming_big_mutations(utils::UUID plan_id) {
    auto it = _streaming_memtables_big.find(plan_id);
    if (it == _streaming_memtables_big.end()) {
        return make_ready_future<std::vector<monitored_sstable>>(std::vector<monitored_sstable>());
    }
    auto entry = it->second;
    _streaming_memtables_big.erase(it);
    return entry->memtables->request_flush().then([entry] {
        return entry->flush_in_progress.close();
    }).then([this, entry] {
        return parallel_for_each(entry->sstables, [this] (auto& sst) {
            return sst.sstable->seal_sstable(this->incremental_backups_enabled()).then([&sst] {
                return sst.sstable->open_data();
            });
        }).then([this, entry] {
            return std::move(entry->sstables);
        });
    });
}

future<> table::fail_streaming_mutations(utils::UUID plan_id) {
    auto it = _streaming_memtables_big.find(plan_id);
    if (it == _streaming_memtables_big.end()) {
        return make_ready_future<>();
    }
    auto entry = it->second;
    _streaming_memtables_big.erase(it);
    return entry->flush_in_progress.close().then([this, entry] {
        for (auto&& sst : entry->sstables) {
            sst.monitor->write_failed();
            sst.sstable->mark_for_deletion();
        }
    });
}

future<> table::clear() {
    if (_commitlog) {
        _commitlog->discard_completed_segments(_schema->id());
    }
    _memtables->clear();
    _memtables->add_memtable();
    _streaming_memtables->clear();
    _streaming_memtables->add_memtable();
    _streaming_memtables_big.clear();
    return _cache.invalidate([] { /* There is no underlying mutation source */ });
}

// NOTE: does not need to be futurized, but might eventually, depending on
// if we implement notifications, whatnot.
future<db::replay_position> table::discard_sstables(db_clock::time_point truncated_at) {
    assert(_compaction_disabled > 0);

    return with_lock(_sstables_lock.for_read(), [this, truncated_at] {
        struct pruner {
            column_family& cf;
            db::replay_position rp;
            std::vector<sstables::shared_sstable> remove;

            pruner(column_family& cf)
                : cf(cf) {}

            void prune(db_clock::time_point truncated_at) {
                auto gc_trunc = to_gc_clock(truncated_at);

                auto pruned = make_lw_shared(cf._compaction_strategy.make_sstable_set(cf._schema));

                for (auto& p : *cf._sstables->all()) {
                    if (p->max_data_age() <= gc_trunc) {
                        // Only one shard that own the sstable will submit it for deletion to avoid race condition in delete procedure.
                        if (*boost::min_element(p->get_shards_for_this_sstable()) == engine().cpu_id()) {
                            rp = std::max(p->get_stats_metadata().position, rp);
                            remove.emplace_back(p);
                        }
                        continue;
                    }
                    pruned->insert(p);
                }

                cf._sstables = std::move(pruned);
            }
        };
        auto p = make_lw_shared<pruner>(*this);
        return _cache.invalidate([p, truncated_at] {
            p->prune(truncated_at);
            dblog.debug("cleaning out row cache");
        }).then([this, p]() mutable {
            return parallel_for_each(p->remove, [this](sstables::shared_sstable s) {
                _compaction_strategy.get_backlog_tracker().remove_sstable(s);
                return sstables::delete_atomically({s}, *get_large_partition_handler());
            }).then([p] {
                return make_ready_future<db::replay_position>(p->rp);
            });
        });
    });
}

future<int64_t>
table::disable_sstable_write() {
    _sstable_writes_disabled_at = std::chrono::steady_clock::now();
    return _sstables_lock.write_lock().then([this] {
        if (_sstables->all()->empty()) {
            return make_ready_future<int64_t>(0);
        }
        int64_t max = 0;
        for (auto&& s : *_sstables->all()) {
            max = std::max(max, s->generation());
        }
        return make_ready_future<int64_t>(max);
    });
}

std::ostream& operator<<(std::ostream& os, const user_types_metadata& m) {
    os << "org.apache.cassandra.config.UTMetaData@" << &m;
    return os;
}

std::ostream& operator<<(std::ostream& os, const keyspace_metadata& m) {
    os << "KSMetaData{";
    os << "name=" << m._name;
    os << ", strategyClass=" << m._strategy_name;
    os << ", strategyOptions={";
    int n = 0;
    for (auto& p : m._strategy_options) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ", cfMetaData={";
    n = 0;
    for (auto& p : m._cf_meta_data) {
        if (n++ != 0) {
            os << ", ";
        }
        os << p.first << "=" << p.second;
    }
    os << "}";
    os << ", durable_writes=" << m._durable_writes;
    os << ", userTypes=" << m._user_types;
    os << "}";
    return os;
}

void table::set_schema(schema_ptr s) {
    dblog.debug("Changing schema version of {}.{} ({}) from {} to {}",
                _schema->ks_name(), _schema->cf_name(), _schema->id(), _schema->version(), s->version());

    for (auto& m : *_memtables) {
        m->set_schema(s);
    }

    for (auto& m : *_streaming_memtables) {
        m->set_schema(s);
    }

    for (auto smb : _streaming_memtables_big) {
        for (auto m : *smb.second->memtables) {
            m->set_schema(s);
        }
    }

    _cache.set_schema(s);
    _counter_cell_locks->set_schema(s);
    _schema = std::move(s);

    set_compaction_strategy(_schema->compaction_strategy());
    trigger_compaction();
}

static std::vector<view_ptr>::iterator find_view(std::vector<view_ptr>& views, const view_ptr& v) {
    return std::find_if(views.begin(), views.end(), [&v] (auto&& e) {
        return e->id() == v->id();
    });
}

void table::add_or_update_view(view_ptr v) {
    v->view_info()->initialize_base_dependent_fields(*schema());
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

static size_t memory_usage_of(const std::vector<frozen_mutation_and_schema>& ms) {
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
        std::vector<view_ptr>&& views,
        mutation&& m,
        flat_mutation_reader_opt existings) const {
    auto base_token = m.token();
    return db::view::generate_view_updates(
            base,
            std::move(views),
            flat_mutation_reader_from_mutations({std::move(m)}),
            std::move(existings)).then([this, base_token = std::move(base_token)] (std::vector<frozen_mutation_and_schema>&& updates) mutable {
        auto units = seastar::consume_units(*_config.view_update_concurrency_semaphore, memory_usage_of(updates));
        db::view::mutate_MV(std::move(base_token), std::move(updates), _view_stats, std::move(units)).handle_exception([] (auto ignored) { });
    });
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
        std::vector<view_ptr> views,
        dht::token base_token,
        flat_mutation_reader&& reader) {
    auto& schema = reader.schema();
    return db::view::generate_view_updates(
            schema,
            std::move(views),
            std::move(reader),
            { }).then([base_token = std::move(base_token), this] (std::vector<frozen_mutation_and_schema>&& updates) mutable {
        size_t update_size = memory_usage_of(updates);
        size_t units_to_wait_for = std::min(_config.view_update_concurrency_semaphore_limit, update_size);
        return seastar::get_units(*_config.view_update_concurrency_semaphore, units_to_wait_for).then(
                [base_token = std::move(base_token),
                 updates = std::move(updates),
                 units_to_consume = update_size - units_to_wait_for,
                 this] (db::timeout_semaphore_units&& units) mutable {
            units.adopt(seastar::consume_units(*_config.view_update_concurrency_semaphore, units_to_consume));
            return db::view::mutate_MV(std::move(base_token), std::move(updates), _view_stats, std::move(units));
        });
    });
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
                sstring me = sprint("%s.%s", _schema->ks_name(), _schema->cf_name());
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

flat_mutation_reader make_local_shard_sstable_reader(schema_ptr s,
        lw_shared_ptr<sstables::sstable_set> sstables,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        reader_resource_tracker resource_tracker,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        sstables::read_monitor_generator& monitor_generator)
{
    auto reader_factory_fn = [s, &slice, &pc, resource_tracker, fwd, fwd_mr, &monitor_generator] (sstables::shared_sstable& sst, const dht::partition_range& pr) {
        flat_mutation_reader reader = sst->read_range_rows_flat(s, pr, slice, pc, resource_tracker, fwd, fwd_mr, monitor_generator(sst));
        if (sst->is_shared()) {
            using sig = bool (&)(const dht::decorated_key&);
            reader = make_filtering_reader(std::move(reader), sig(belongs_to_current_shard));
        }
        return reader;
    };
    return make_combined_reader(s, std::make_unique<incremental_reader_selector>(s,
                    std::move(sstables),
                    pr,
                    std::move(trace_state),
                    std::move(reader_factory_fn)),
            fwd,
            fwd_mr);
}

flat_mutation_reader make_range_sstable_reader(schema_ptr s,
        lw_shared_ptr<sstables::sstable_set> sstables,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        reader_resource_tracker resource_tracker,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        sstables::read_monitor_generator& monitor_generator)
{
    auto reader_factory_fn = [s, &slice, &pc, resource_tracker, fwd, fwd_mr, &monitor_generator] (sstables::shared_sstable& sst, const dht::partition_range& pr) {
        return sst->read_range_rows_flat(s, pr, slice, pc, resource_tracker, fwd, fwd_mr, monitor_generator(sst));
    };
    return make_combined_reader(s, std::make_unique<incremental_reader_selector>(s,
                    std::move(sstables),
                    pr,
                    std::move(trace_state),
                    std::move(reader_factory_fn)),
            fwd,
            fwd_mr);
}

future<>
write_memtable_to_sstable(memtable& mt, sstables::shared_sstable sst,
                          sstables::write_monitor& monitor, db::large_partition_handler* lp_handler,
                          bool backup, const io_priority_class& pc, bool leave_unsealed) {
    sstables::sstable_writer_config cfg;
    cfg.replay_position = mt.replay_position();
    cfg.backup = backup;
    cfg.leave_unsealed = leave_unsealed;
    cfg.monitor = &monitor;
    cfg.large_partition_handler = lp_handler;
    return sst->write_components(mt.make_flush_reader(mt.schema(), pc), mt.partition_count(),
        mt.schema(), cfg, mt.get_stats(), pc);
}

future<>
write_memtable_to_sstable(memtable& mt, sstables::shared_sstable sst, db::large_partition_handler* lp_handler) {
    return do_with(permit_monitor(sstable_write_permit::unconditional()), [&mt, sst, lp_handler] (auto& monitor) {
        return write_memtable_to_sstable(mt, std::move(sst), monitor, lp_handler);
    });
}

std::ostream& operator<<(std::ostream& os, gc_clock::time_point tp) {
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
    std::ostream tmp(os.rdbuf());
    tmp << std::setw(12) << sec;
    return os;
}

const timeout_config infinite_timeout_config = {
        // not really infinite, but long enough
        1h, 1h, 1h, 1h, 1h, 1h, 1h,
};
