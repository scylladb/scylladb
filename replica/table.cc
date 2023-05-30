/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/defer.hh>

#include "replica/database.hh"
#include "replica/data_dictionary_impl.hh"
#include "replica/compaction_group.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "db/schema_tables.hh"
#include "cell_locking.hh"
#include "utils/logalloc.hh"
#include "checked-file-impl.hh"
#include "view_info.hh"
#include "db/data_listeners.hh"
#include "memtable-sstable.hh"
#include "compaction/compaction_manager.hh"
#include "compaction/table_state.hh"
#include "sstables/sstable_directory.hh"
#include "db/system_keyspace.hh"
#include "query-result-writer.hh"
#include "db/view/view_update_generator.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include "utils/error_injection.hh"
#include "utils/histogram_metrics_helper.hh"
#include "utils/fb_utilities.hh"
#include "mutation/mutation_source_metadata.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "db/config.hh"
#include "db/commitlog/commitlog.hh"
#include "utils/lister.hh"
#include "dht/token.hh"
#include "dht/i_partitioner.hh"
#include "replica/global_table_ptr.hh"

#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm.hpp>
#include "utils/error_injection.hh"
#include "readers/reversing_v2.hh"
#include "readers/from_mutations_v2.hh"
#include "readers/empty_v2.hh"
#include "readers/multi_range.hh"
#include "readers/combined.hh"
#include "readers/compacting.hh"

namespace replica {

static logging::logger tlogger("table");
static seastar::metrics::label column_family_label("cf");
static seastar::metrics::label keyspace_label("ks");

using namespace std::chrono_literals;

void table::update_sstables_known_generation(sstables::generation_type generation) {
    auto gen = generation ? generation.as_int() : 0;
    if (_sstable_generation_generator) {
        _sstable_generation_generator->update_known_generation(gen);
    } else {
        _sstable_generation_generator.emplace(gen);
    }
    tlogger.debug("{}.{} updated highest known generation to {}", schema()->ks_name(), schema()->cf_name(), gen);
}

sstables::generation_type table::calculate_generation_for_new_table() {
    assert(_sstable_generation_generator);
    auto ret = std::invoke(*_sstable_generation_generator,
                           uuid_identifiers{_sstables_manager.uuid_sstable_identifiers()});
    tlogger.debug("{}.{} new sstable generation {}", schema()->ks_name(), schema()->cf_name(), ret);
    return ret;
}

flat_mutation_reader_v2
table::make_sstable_reader(schema_ptr s,
                                   reader_permit permit,
                                   lw_shared_ptr<sstables::sstable_set> sstables,
                                   const dht::partition_range& pr,
                                   const query::partition_slice& slice,
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr,
                                   const sstables::sstable_predicate& predicate) const {
    // CAVEAT: if make_sstable_reader() is called on a single partition
    // we want to optimize and read exactly this partition. As a
    // consequence, fast_forward_to() will *NOT* work on the result,
    // regardless of what the fwd_mr parameter says.
    if (pr.is_singular() && pr.start()->value().has_key()) {
        const dht::ring_position& pos = pr.start()->value();
        if (_erm->shard_of(*s, pos.token()) != this_shard_id()) {
            return make_empty_flat_reader_v2(s, std::move(permit)); // range doesn't belong to this shard
        }

        return sstables->create_single_key_sstable_reader(const_cast<column_family*>(this), std::move(s), std::move(permit),
                _stats.estimated_sstable_per_read, pr, slice, std::move(trace_state), fwd, fwd_mr, predicate);
    } else {
        return sstables->make_local_shard_sstable_reader(std::move(s), std::move(permit), pr, slice,
                std::move(trace_state), fwd, fwd_mr, default_read_monitor_generator(), predicate);
    }
}

lw_shared_ptr<sstables::sstable_set> compaction_group::make_compound_sstable_set() {
    return make_lw_shared(sstables::make_compound_sstable_set(_t.schema(), { _main_sstables, _maintenance_sstables }));
}

lw_shared_ptr<sstables::sstable_set> table::make_compound_sstable_set() {
    if (auto cg = single_compaction_group_if_available()) {
        return cg->make_compound_sstable_set();
    }
    // TODO: switch to a specialized set for groups which assumes disjointness across compound sets and incrementally read from them.
    // FIXME: avoid recreation of compound_set for groups which had no change. usually, only one group will be changed at a time.
    auto sstable_sets = boost::copy_range<std::vector<lw_shared_ptr<sstables::sstable_set>>>(compaction_groups()
        | boost::adaptors::transformed(std::mem_fn(&compaction_group::make_compound_sstable_set)));
    return make_lw_shared(sstables::make_compound_sstable_set(schema(), std::move(sstable_sets)));
}

lw_shared_ptr<sstables::sstable_set> table::make_maintenance_sstable_set() const {
    // Level metadata is not used because (level 0) maintenance sstables are disjoint and must be stored for efficient retrieval in the partitioned set
    bool use_level_metadata = false;
    return make_lw_shared<sstables::sstable_set>(
            sstables::make_partitioned_sstable_set(_schema, use_level_metadata));
}

void table::refresh_compound_sstable_set() {
    _sstables = make_compound_sstable_set();
}

// Exposed for testing, not performance critical.
future<table::const_mutation_partition_ptr>
table::find_partition(schema_ptr s, reader_permit permit, const dht::decorated_key& key) const {
    return do_with(dht::partition_range::make_singular(key), [s = std::move(s), permit = std::move(permit), this] (auto& range) mutable {
        return with_closeable(this->make_reader_v2(std::move(s), std::move(permit), range), [] (flat_mutation_reader_v2& reader) {
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

void
table::add_memtables_to_reader_list(std::vector<flat_mutation_reader_v2>& readers,
        const schema_ptr& s,
        const reader_permit& permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const tracing::trace_state_ptr& trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        std::function<void(size_t)> reserve_fn) const {
    auto add_memtables_from_cg = [&] (compaction_group& cg) mutable {
        for (auto&& mt: *cg.memtables()) {
            if (auto reader_opt = mt->make_flat_reader_opt(s, permit, range, slice, trace_state, fwd, fwd_mr)) {
                readers.emplace_back(std::move(*reader_opt));
            }
        }
    };

    // point queries can be optimized as they span a single compaction group.
    if (range.is_singular() && range.start()->value().has_key()) {
        const dht::ring_position& pos = range.start()->value();
        auto& cg = compaction_group_for_token(pos.token());
        reserve_fn(cg.memtable_count());
        add_memtables_from_cg(cg);
        return;
    }
    reserve_fn(boost::accumulate(compaction_groups() | boost::adaptors::transformed(std::mem_fn(&compaction_group::memtable_count)), uint64_t(0)));
    // TODO: implement a incremental reader selector for memtable, using existing reader_selector interface for combined_reader.
    for (const compaction_group_ptr& cg : compaction_groups()) {
        add_memtables_from_cg(*cg);
    }
}

flat_mutation_reader_v2
table::make_reader_v2(schema_ptr s,
                           reader_permit permit,
                           const dht::partition_range& range,
                           const query::partition_slice& query_slice,
                           tracing::trace_state_ptr trace_state,
                           streamed_mutation::forwarding fwd,
                           mutation_reader::forwarding fwd_mr) const {
    if (_virtual_reader) [[unlikely]] {
        return (*_virtual_reader).make_reader_v2(s, std::move(permit), range, query_slice, trace_state, fwd, fwd_mr);
    }

    bool reversed = query_slice.is_reversed();
    std::unique_ptr<query::partition_slice> unreversed_slice;
    if (reversed && !_config.enable_optimized_reversed_reads()) [[unlikely]] {
        // Make the code below perform a forward query. We'll wrap the result into `make_reversing_reader` at the end.
        reversed = false;
        s = s->make_reversed();
        unreversed_slice = std::make_unique<query::partition_slice>(query::half_reverse_slice(*s, query_slice));
    }
    auto& slice = unreversed_slice ? *unreversed_slice : query_slice;

    std::vector<flat_mutation_reader_v2> readers;

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

    add_memtables_to_reader_list(readers, s, permit, range, slice, trace_state, fwd, fwd_mr, [&] (size_t memtable_count) {
        readers.reserve(memtable_count + 1);
    });

    const auto bypass_cache = slice.options.contains(query::partition_slice::option::bypass_cache);
    if (cache_enabled() && !bypass_cache && !(reversed && _config.reversed_reads_auto_bypass_cache())) {
        if (auto reader_opt = _cache.make_reader_opt(s, permit, range, slice, &_compaction_manager.get_tombstone_gc_state(), std::move(trace_state), fwd, fwd_mr)) {
            readers.emplace_back(std::move(*reader_opt));
        }
    } else {
        readers.emplace_back(make_sstable_reader(s, permit, _sstables, range, slice, std::move(trace_state), fwd, fwd_mr));
    }

    auto rd = make_combined_reader(s, permit, std::move(readers), fwd, fwd_mr);

    if (_config.data_listeners && !_config.data_listeners->empty()) {
        rd = _config.data_listeners->on_read(s, range, slice, std::move(rd));
    }

    if (unreversed_slice) [[unlikely]] {
        return make_reversing_reader(std::move(rd), permit.max_result_size(), std::move(unreversed_slice));
    }

    return rd;
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

sstables::shared_sstable table::make_streaming_staging_sstable() {
    return make_streaming_sstable_for_write(sstables::staging_dir);
}

flat_mutation_reader_v2
table::make_streaming_reader(schema_ptr s, reader_permit permit,
                           const dht::partition_range_vector& ranges) const {
    auto& slice = s->full_slice();

    auto source = mutation_source([this] (schema_ptr s, reader_permit permit, const dht::partition_range& range, const query::partition_slice& slice,
                                      tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd, mutation_reader::forwarding fwd_mr) {
        std::vector<flat_mutation_reader_v2> readers;
        add_memtables_to_reader_list(readers, s, permit, range, slice, trace_state, fwd, fwd_mr, [&] (size_t memtable_count) {
            readers.reserve(memtable_count + 1);
        });
        readers.emplace_back(make_sstable_reader(s, permit, _sstables, range, slice, std::move(trace_state), fwd, fwd_mr));
        return make_combined_reader(s, std::move(permit), std::move(readers), fwd, fwd_mr);
    });

    return make_flat_multi_range_reader(s, std::move(permit), std::move(source), ranges, slice, nullptr, mutation_reader::forwarding::no);
}

flat_mutation_reader_v2 table::make_streaming_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
        const query::partition_slice& slice, mutation_reader::forwarding fwd_mr) const {
    auto trace_state = tracing::trace_state_ptr();
    const auto fwd = streamed_mutation::forwarding::no;

    std::vector<flat_mutation_reader_v2> readers;
    add_memtables_to_reader_list(readers, schema, permit, range, slice, trace_state, fwd, fwd_mr, [&] (size_t memtable_count) {
        readers.reserve(memtable_count + 1);
    });
    readers.emplace_back(make_sstable_reader(schema, permit, _sstables, range, slice, std::move(trace_state), fwd, fwd_mr));
    return make_combined_reader(std::move(schema), std::move(permit), std::move(readers), fwd, fwd_mr);
}

flat_mutation_reader_v2 table::make_streaming_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
        lw_shared_ptr<sstables::sstable_set> sstables) const {
    auto& slice = schema->full_slice();
    auto trace_state = tracing::trace_state_ptr();
    const auto fwd = streamed_mutation::forwarding::no;
    const auto fwd_mr = mutation_reader::forwarding::no;
    return sstables->make_range_sstable_reader(std::move(schema), std::move(permit), range, slice,
            std::move(trace_state), fwd, fwd_mr);
}

flat_mutation_reader_v2 table::make_nonpopulating_cache_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& range,
        const query::partition_slice& slice, tracing::trace_state_ptr ts) {
    if (!range.is_singular()) {
        throw std::runtime_error("table::make_cache_reader(): only singular ranges are supported");
    }
    return _cache.make_nonpopulating_reader(std::move(schema), std::move(permit), range, slice, std::move(ts));
}

future<std::vector<locked_cell>> table::lock_counter_cells(const mutation& m, db::timeout_clock::time_point timeout) {
    assert(m.schema() == _counter_cell_locks->schema());
    return _counter_cell_locks->lock_cells(m.decorated_key(), partition_cells_range(m.partition()), timeout);
}

std::vector<memtable*> table::active_memtables() {
    return boost::copy_range<std::vector<memtable*>>(compaction_groups() | boost::adaptors::transformed([] (const compaction_group_ptr& cg) {
        return &cg->memtables()->active_memtable();
    }));
}

api::timestamp_type compaction_group::min_memtable_timestamp() const {
    if (_memtables->empty()) {
        return api::max_timestamp;
    }

    return *boost::range::min_element(
        *_memtables
        | boost::adaptors::transformed(
            [](const shared_memtable& m) { return m->get_min_timestamp(); }
        )
    );
}

api::timestamp_type table::min_memtable_timestamp() const {
    return *boost::range::min_element(compaction_groups() | boost::adaptors::transformed(std::mem_fn(&compaction_group::min_memtable_timestamp)));
}

// Not performance critical. Currently used for testing only.
future<bool>
table::for_all_partitions_slow(schema_ptr s, reader_permit permit, std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const {
    struct iteration_state {
        flat_mutation_reader_v2 reader;
        std::function<bool (const dht::decorated_key&, const mutation_partition&)> func;
        bool ok = true;
        bool empty = false;
    public:
        bool done() const { return !ok || empty; }
        iteration_state(schema_ptr s, reader_permit permit, const column_family& cf,
                std::function<bool (const dht::decorated_key&, const mutation_partition&)>&& func)
            : reader(cf.make_reader_v2(std::move(s), std::move(permit)))
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

sstables::shared_sstable table::make_sstable(sstring dir) {
    auto& sstm = get_sstables_manager();
    return sstm.make_sstable(_schema, *_storage_opts, dir, calculate_generation_for_new_table(), sstm.get_highest_supported_format(), sstables::sstable::format_types::big);
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

inline void table::add_sstable_to_backlog_tracker(compaction_backlog_tracker& tracker, sstables::shared_sstable sstable) {
    tracker.replace_sstables({}, {std::move(sstable)});
}

inline void table::remove_sstable_from_backlog_tracker(compaction_backlog_tracker& tracker, sstables::shared_sstable sstable) {
    tracker.replace_sstables({std::move(sstable)}, {});
}

void compaction_group::backlog_tracker_adjust_charges(const std::vector<sstables::shared_sstable>& old_sstables, const std::vector<sstables::shared_sstable>& new_sstables) {
    auto& tracker = get_backlog_tracker();
    tracker.replace_sstables(old_sstables, new_sstables);
}

lw_shared_ptr<sstables::sstable_set>
compaction_group::do_add_sstable(lw_shared_ptr<sstables::sstable_set> sstables, sstables::shared_sstable sstable,
        enable_backlog_tracker backlog_tracker) {
    if (belongs_to_other_shard(sstable->get_shards_for_this_sstable())) {
        on_internal_error(tlogger, format("Attempted to load the shared SSTable {} at table", sstable->get_filename()));
    }
    // allow in-progress reads to continue using old list
    auto new_sstables = make_lw_shared<sstables::sstable_set>(*sstables);
    new_sstables->insert(sstable);
    if (backlog_tracker) {
        table::add_sstable_to_backlog_tracker(get_backlog_tracker(), sstable);
    }
    return new_sstables;
}

void compaction_group::add_sstable(sstables::shared_sstable sstable) {
    auto sstable_size = sstable->bytes_on_disk();
    _main_sstables = do_add_sstable(_main_sstables, std::move(sstable), enable_backlog_tracker::yes);
    _main_set_disk_space_used += sstable_size;
}

const lw_shared_ptr<sstables::sstable_set>& compaction_group::main_sstables() const noexcept {
    return _main_sstables;
}

void compaction_group::set_main_sstables(lw_shared_ptr<sstables::sstable_set> new_main_sstables) {
    _main_sstables = std::move(new_main_sstables);
    _main_set_disk_space_used = calculate_disk_space_used_for(*_main_sstables);
}

void compaction_group::add_maintenance_sstable(sstables::shared_sstable sst) {
    auto sstable_size = sst->bytes_on_disk();
    _maintenance_sstables = do_add_sstable(_maintenance_sstables, std::move(sst), enable_backlog_tracker::no);
    _maintenance_set_disk_space_used += sstable_size;
}

const lw_shared_ptr<sstables::sstable_set>& compaction_group::maintenance_sstables() const noexcept {
    return _maintenance_sstables;
}

void compaction_group::set_maintenance_sstables(lw_shared_ptr<sstables::sstable_set> new_maintenance_sstables) {
    _maintenance_sstables = std::move(new_maintenance_sstables);
    _maintenance_set_disk_space_used = calculate_disk_space_used_for(*_maintenance_sstables);
}

void table::add_sstable(compaction_group& cg, sstables::shared_sstable sstable) {
    cg.add_sstable(std::move(sstable));
    refresh_compound_sstable_set();
}

void table::add_maintenance_sstable(compaction_group& cg, sstables::shared_sstable sst) {
    cg.add_maintenance_sstable(std::move(sst));
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

std::vector<std::unique_ptr<compaction_group>> table::make_compaction_groups() {
    std::vector<std::unique_ptr<compaction_group>> ret;
    auto&& ranges = dht::split_token_range_msb(_x_log2_compaction_groups);
    ret.reserve(ranges.size());
    tlogger.debug("Created {} compaction groups for {}.{}", ranges.size(), _schema->ks_name(), _schema->cf_name());
    size_t i = 0;
    for (auto&& range : ranges) {
        auto group_id = fmt::format("{}/{}", i++, ranges.size());
        ret.emplace_back(std::make_unique<compaction_group>(*this, std::move(group_id), std::move(range)));
    }
    return ret;
}

compaction_group* table::single_compaction_group_if_available() const noexcept {
    return _compaction_groups.size() == 1 ? &*_compaction_groups[0] : nullptr;
}

compaction_group& table::compaction_group_for_token(dht::token token) const noexcept {
    auto idx = dht::compaction_group_of(_x_log2_compaction_groups, token);
    if (idx >= _compaction_groups.size()) {
        on_fatal_internal_error(tlogger, format("compaction_group_for_token: index out of range: idx={} size_log2={} size={} token={}", idx, _x_log2_compaction_groups, _compaction_groups.size(), token));
    }
    auto& ret = *_compaction_groups[idx];
    if (!ret.token_range().contains(token, dht::token_comparator())) {
        on_fatal_internal_error(tlogger, format("compaction_group_for_token: compaction_group idx={} range={} does not contain token={}",
                idx, ret.token_range(), token));
    }
    return ret;
}

compaction_group& table::compaction_group_for_key(partition_key_view key, const schema_ptr& s) const noexcept {
    // fast path when table owns a single compaction group, to avoid overhead of calculating token.
    if (auto cg = single_compaction_group_if_available()) {
        return *cg;
    }
    return compaction_group_for_token(dht::get_token(*s, key));
}

compaction_group& table::compaction_group_for_sstable(const sstables::shared_sstable& sst) const noexcept {
    // FIXME: a sstable can belong to more than one group, change interface to reflect that.
    return compaction_group_for_token(sst->get_first_decorated_key().token());
}

const std::vector<std::unique_ptr<compaction_group>>& table::compaction_groups() const noexcept {
    return _compaction_groups;
}

future<> table::parallel_foreach_compaction_group(std::function<future<>(compaction_group&)> action) {
    // TODO: place a barrier here when we allow dynamic groups.
    co_await coroutine::parallel_for_each(compaction_groups(), [&] (const compaction_group_ptr& cg) {
        return action(*cg);
    });
}

void table::update_stats_for_new_sstable(const sstables::shared_sstable& sst) noexcept {
    _stats.live_disk_space_used += sst->bytes_on_disk();
    _stats.total_disk_space_used += sst->bytes_on_disk();
    _stats.live_sstable_count++;
}

future<>
table::do_add_sstable_and_update_cache(sstables::shared_sstable sst, sstables::offstrategy offstrategy) {
    auto permit = co_await seastar::get_units(_sstable_set_mutation_sem, 1);
    co_return co_await get_row_cache().invalidate(row_cache::external_updater([this, sst, offstrategy] () noexcept {
        // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
        // atomically load all opened sstables into column family.
        compaction_group& cg = compaction_group_for_sstable(sst);
        if (!offstrategy) {
            add_sstable(cg, sst);
        } else {
            add_maintenance_sstable(cg, sst);
        }
        update_stats_for_new_sstable(sst);
    }), dht::partition_range::make({sst->get_first_decorated_key(), true}, {sst->get_last_decorated_key(), true}));
}

future<>
table::add_sstable_and_update_cache(sstables::shared_sstable sst, sstables::offstrategy offstrategy) {
    co_await do_add_sstable_and_update_cache(std::move(sst), offstrategy);
    trigger_compaction();
}

future<>
table::add_sstables_and_update_cache(const std::vector<sstables::shared_sstable>& ssts) {
    for (auto& sst : ssts) {
        try {
            co_await do_add_sstable_and_update_cache(sst, sstables::offstrategy::no);
        } catch (...) {
            tlogger.error("Failed to load SSTable {}: {}", sst->toc_filename(), std::current_exception());
            throw;
        }
    }
    trigger_compaction();
}

future<>
table::update_cache(compaction_group& cg, lw_shared_ptr<memtable> m, std::vector<sstables::shared_sstable> ssts) {
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
    auto adder = row_cache::external_updater([this, m, ssts = std::move(ssts), new_ssts_ms = std::move(*ms_opt), &cg] () mutable {
        for (auto& sst : ssts) {
            add_sstable(cg, sst);
            update_stats_for_new_sstable(sst);
        }
        m->mark_flushed(std::move(new_ssts_ms));
        try_trigger_compaction(cg);
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
    compaction::table_state& _ts;
    const sstables::writer_offset_tracker* _tracker = nullptr;
    uint64_t _progress_seen = 0;
    api::timestamp_type _maximum_timestamp;
public:
    database_sstable_write_monitor(lw_shared_ptr<sstable_write_permit> permit, sstables::shared_sstable sst,
        compaction_group& cg, api::timestamp_type max_timestamp)
            : permit_monitor(std::move(permit))
            , _sst(std::move(sst))
            , _ts(cg.as_table_state())
            , _maximum_timestamp(max_timestamp)
    {}

    database_sstable_write_monitor(const database_sstable_write_monitor&) = delete;
    database_sstable_write_monitor(database_sstable_write_monitor&& x) = default;

    ~database_sstable_write_monitor() {
        // We failed to finish handling this SSTable, so we have to update the backlog_tracker
        // about it.
        if (_sst) {
            _ts.get_backlog_tracker().revert_charges(_sst);
        }
    }

    virtual void on_write_started(const sstables::writer_offset_tracker& t) override {
        _tracker = &t;
        _ts.get_backlog_tracker().register_partially_written_sstable(_sst, *this);
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

// The function never fails.
// It either succeeds eventually after retrying or aborts.
future<>
table::seal_active_memtable(compaction_group& cg, flush_permit&& flush_permit) noexcept {
    auto old = cg.memtables()->back();
    tlogger.debug("Sealing active memtable of {}.{}, partitions: {}, occupancy: {}", _schema->ks_name(), _schema->cf_name(), old->partition_count(), old->occupancy());

    if (old->empty()) {
        tlogger.debug("Memtable is empty");
        co_return co_await _flush_barrier.advance_and_await();
    }

    auto permit = std::move(flush_permit);
    auto r = exponential_backoff_retry(100ms, 10s);
    // Try flushing for around half an hour (30 minutes every 10 seconds)
    int default_retries = 30 * 60 / 10;
    int allowed_retries = default_retries;
    std::optional<utils::phased_barrier::operation> op;
    size_t memtable_size;
    future<> previous_flush = make_ready_future<>();

    auto with_retry = [&] (std::function<future<>()> func) -> future<> {
        for (;;) {
            std::exception_ptr ex;
            try {
                co_return co_await func();
            } catch (...) {
                ex = std::current_exception();
                _config.cf_stats->failed_memtables_flushes_count++;

                auto should_retry = [](auto* ep) {
                    int ec = ep->code().value();
                    return ec == ENOSPC || ec == EDQUOT;
                };
                if (try_catch<std::bad_alloc>(ex)) {
                    // There is a chance something else will free the memory, so we can try again
                    allowed_retries--;
                } else if (auto ep = try_catch<std::system_error>(ex)) {
                    allowed_retries = should_retry(ep) ? default_retries : 0;
                } else if (auto ep = try_catch<storage_io_error>(ex)) {
                    allowed_retries = should_retry(ep) ? default_retries : 0;
                } else {
                    allowed_retries = 0;
                }

                if (allowed_retries <= 0) {
                    // At this point we don't know what has happened and it's better to potentially
                    // take the node down and rely on commitlog to replay.
                    //
                    // FIXME: enter maintenance mode when available.
                    // since replaying the commitlog with a corrupt mutation
                    // may end up in an infinite crash loop.
                    tlogger.error("Memtable flush failed due to: {}. Aborting, at {}", ex, current_backtrace());
                    std::abort();
                }
            }
            if (_async_gate.is_closed()) {
                tlogger.warn("Memtable flush failed due to: {}. Dropped due to shutdown", ex);
                co_await std::move(previous_flush);
                co_await coroutine::return_exception_ptr(std::move(ex));
            }
            tlogger.warn("Memtable flush failed due to: {}. Will retry in {}ms", ex, r.sleep_time().count());
            co_await r.retry();
        }
    };

    co_await with_retry([&] {
        tlogger.debug("seal_active_memtable: adding memtable");
        utils::get_local_injector().inject("table_seal_active_memtable_add_memtable", []() {
            throw std::bad_alloc();
        });

        cg.memtables()->add_memtable();

        // no exceptions allowed (nor expected) from this point on
        _stats.memtable_switch_count++;
        [&] () noexcept {
            // This will set evictable occupancy of the old memtable region to zero, so that
            // this region is considered last for flushing by dirty_memory_manager::flush_when_needed().
            // If we don't do that, the flusher may keep picking up this memtable list for flushing after
            // the permit is released even though there is not much to flush in the active memtable of this list.
            old->region().ground_evictable_occupancy();
            memtable_size = old->occupancy().total_space();
        }();
        return make_ready_future<>();
    });

    co_await with_retry([&] {
        previous_flush = _flush_barrier.advance_and_await();
        utils::get_local_injector().inject("table_seal_active_memtable_start_op", []() {
            throw std::bad_alloc();
        });
        op = _flush_barrier.start();

        // no exceptions allowed (nor expected) from this point on
        _stats.pending_flushes++;
        _config.cf_stats->pending_memtables_flushes_count++;
        _config.cf_stats->pending_memtables_flushes_bytes += memtable_size;
        return make_ready_future<>();
    });

    auto undo_stats = std::make_optional(deferred_action([this, memtable_size] () noexcept {
        _stats.pending_flushes--;
        _config.cf_stats->pending_memtables_flushes_count--;
        _config.cf_stats->pending_memtables_flushes_bytes -= memtable_size;
    }));

    co_await with_retry([&] () -> future<> {
        // Reacquiring the write permit might be needed if retrying flush
        if (!permit.has_sstable_write_permit()) {
            tlogger.debug("seal_active_memtable: reacquiring write permit");
            utils::get_local_injector().inject("table_seal_active_memtable_reacquire_write_permit", []() {
                throw std::bad_alloc();
            });
            permit = co_await std::move(permit).reacquire_sstable_write_permit();
        }
        auto write_permit = permit.release_sstable_write_permit();

        utils::get_local_injector().inject("table_seal_active_memtable_try_flush", []() {
            throw std::system_error(ENOSPC, std::system_category(), "Injected error");
        });
        co_return co_await this->try_flush_memtable_to_sstable(cg, old, std::move(write_permit));
    });

    undo_stats.reset();

    if (_commitlog) {
        _commitlog->discard_completed_segments(_schema->id(), old->get_and_discard_rp_set());
    }
    co_await std::move(previous_flush);
    // keep `op` alive until after previous_flush resolves

    // FIXME: release commit log
    // FIXME: provide back-pressure to upper layers
}

future<>
table::try_flush_memtable_to_sstable(compaction_group& cg, lw_shared_ptr<memtable> old, sstable_write_permit&& permit) {
    auto try_flush = [this, old = std::move(old), permit = make_lw_shared(std::move(permit)), &cg] () mutable -> future<> {
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

        auto newtabs = std::vector<sstables::shared_sstable>();
        auto metadata = mutation_source_metadata{};
        metadata.min_timestamp = old->get_min_timestamp();
        metadata.max_timestamp = old->get_max_timestamp();
        auto estimated_partitions = _compaction_strategy.adjust_partition_estimate(metadata, old->partition_count());

        if (!_async_gate.is_closed()) {
            co_await _compaction_manager.maybe_wait_for_sstable_count_reduction(cg.as_table_state());
        }

        auto consumer = _compaction_strategy.make_interposer_consumer(metadata, [this, old, permit, &newtabs, estimated_partitions, &cg] (flat_mutation_reader_v2 reader) mutable -> future<> {
          std::exception_ptr ex;
          try {
            sstables::sstable_writer_config cfg = get_sstables_manager().configure_writer("memtable");
            cfg.backup = incremental_backups_enabled();
            cfg.erm = _erm;

            auto newtab = make_sstable();
            newtabs.push_back(newtab);
            tlogger.debug("Flushing to {}", newtab->get_filename());

            auto monitor = database_sstable_write_monitor(permit, newtab, cg,
                old->get_max_timestamp());

            co_return co_await write_memtable_to_sstable(std::move(reader), *old, newtab, estimated_partitions, monitor, cfg);
          } catch (...) {
            ex = std::current_exception();
          }
          co_await reader.close();
          co_await coroutine::return_exception_ptr(std::move(ex));
        });

        auto f = consumer(old->make_flush_reader(
            old->schema(),
            compaction_concurrency_semaphore().make_tracking_only_permit(old->schema().get(), "try_flush_memtable_to_sstable()", db::no_timeout, {})));

        // Switch back to default scheduling group for post-flush actions, to avoid them being staved by the memtable flush
        // controller. Cache update does not affect the input of the memtable cpu controller, so it can be subject to
        // priority inversion.
        auto post_flush = [this, old = std::move(old), &newtabs, f = std::move(f), &cg] () mutable -> future<> {
            try {
                co_await std::move(f);
                co_await coroutine::parallel_for_each(newtabs, [] (auto& newtab) -> future<> {
                    co_await newtab->open_data();
                    tlogger.debug("Flushing to {} done", newtab->get_filename());
                });

                co_await with_scheduling_group(_config.memtable_to_cache_scheduling_group, [this, old, &newtabs, &cg] {
                    return update_cache(cg, old, newtabs);
                });
                cg.memtables()->erase(old);
                tlogger.debug("Memtable for {}.{} replaced, into {} sstables", old->schema()->ks_name(), old->schema()->cf_name(), newtabs.size());
                co_return;
            } catch (const std::exception& e) {
                for (auto& newtab : newtabs) {
                    newtab->mark_for_deletion();
                    tlogger.error("failed to write sstable {}: {}", newtab->get_filename(), e);
                }
                _config.cf_stats->failed_memtables_flushes_count++;
                // If we failed this write we will try the write again and that will create a new flush reader
                // that will decrease dirty memory again. So we need to reset the accounting.
                old->revert_flushed_memory();
                throw;
            }
        };
        co_return co_await with_scheduling_group(default_scheduling_group(), std::ref(post_flush));
    };
    co_return co_await with_scheduling_group(_config.memtable_scheduling_group, std::ref(try_flush));
}

void
table::start() {
    start_compaction();
}

future<>
table::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    co_await _async_gate.close();
    co_await await_pending_ops();
    co_await parallel_foreach_compaction_group(std::mem_fn(&compaction_group::stop));
    co_await _sstable_deletion_gate.close();
    co_await get_row_cache().invalidate(row_cache::external_updater([this] {
        for (const compaction_group_ptr& cg : compaction_groups()) {
            cg->clear_sstables();
        }
        _sstables = make_compound_sstable_set();
    }));
    _cache.refresh_snapshot();
}

void table::set_metrics() {
    auto cf = column_family_label(_schema->cf_name());
    auto ks = keyspace_label(_schema->ks_name());
    namespace ms = seastar::metrics;
    if (_config.enable_metrics_reporting) {
        _metrics.add_group("column_family", {
                ms::make_counter("memtable_switch", ms::description("Number of times flush has resulted in the memtable being switched out"), _stats.memtable_switch_count)(cf)(ks).set_skip_when_empty(),
                ms::make_counter("memtable_partition_writes", [this] () { return _stats.memtable_partition_insertions + _stats.memtable_partition_hits; }, ms::description("Number of write operations performed on partitions in memtables"))(cf)(ks).set_skip_when_empty(),
                ms::make_counter("memtable_partition_hits", _stats.memtable_partition_hits, ms::description("Number of times a write operation was issued on an existing partition in memtables"))(cf)(ks).set_skip_when_empty(),
                ms::make_counter("memtable_row_writes", _stats.memtable_app_stats.row_writes, ms::description("Number of row writes performed in memtables"))(cf)(ks).set_skip_when_empty(),
                ms::make_counter("memtable_row_hits", _stats.memtable_app_stats.row_hits, ms::description("Number of rows overwritten by write operations in memtables"))(cf)(ks).set_skip_when_empty().set_skip_when_empty(),
                ms::make_counter("memtable_rows_dropped_by_tombstones", _stats.memtable_app_stats.rows_dropped_by_tombstones, ms::description("Number of rows dropped in memtables by a tombstone write"))(cf)(ks).set_skip_when_empty(),
                ms::make_counter("memtable_rows_compacted_with_tombstones", _stats.memtable_app_stats.rows_compacted_with_tombstones, ms::description("Number of rows scanned during write of a tombstone for the purpose of compaction in memtables"))(cf)(ks).set_skip_when_empty(),
                ms::make_counter("memtable_range_tombstone_reads", _stats.memtable_range_tombstone_reads, ms::description("Number of range tombstones read from memtables"))(cf)(ks).set_skip_when_empty(),
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
                ms::make_total_operations(format("row_lock_{}_acquisitions", stat_name), stats.lock_acquisitions, ms::description(format("Row lock acquisitions for {} lock", stat_name)))(cf)(ks).set_skip_when_empty(),
                ms::make_queue_length(format("row_lock_{}_operations_currently_waiting_for_lock", stat_name), stats.operations_currently_waiting_for_lock, ms::description(format("Operations currently waiting for {} lock", stat_name)))(cf)(ks),
                ms::make_histogram(format("row_lock_{}_waiting_time", stat_name), ms::description(format("Histogram representing time that operations spent on waiting for {} lock", stat_name)),
                        [&stats] {return to_metrics_histogram(stats.estimated_waiting_for_lock);})(cf)(ks).aggregate({seastar::metrics::shard_label}).set_skip_when_empty()
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

        if (!is_internal_keyspace(_schema->ks_name())) {
            _metrics.add_group("column_family", {
                    ms::make_summary("read_latency_summary", ms::description("Read latency summary"), [this] {return to_metrics_summary(_stats.reads.summary());})(cf)(ks).set_skip_when_empty(),
                    ms::make_summary("write_latency_summary", ms::description("Write latency summary"), [this] {return to_metrics_summary(_stats.writes.summary());})(cf)(ks).set_skip_when_empty(),
                    ms::make_summary("cas_prepare_latency_summary", ms::description("CAS prepare round latency summary"), [this] {return to_metrics_summary(_stats.cas_prepare.summary());})(cf)(ks).set_skip_when_empty(),
                    ms::make_summary("cas_propose_latency_summary", ms::description("CAS accept round latency summary"), [this] {return to_metrics_summary(_stats.cas_accept.summary());})(cf)(ks).set_skip_when_empty(),
                    ms::make_summary("cas_commit_latency_summary", ms::description("CAS learn round latency summary"), [this] {return to_metrics_summary(_stats.cas_learn.summary());})(cf)(ks).set_skip_when_empty(),

                    ms::make_histogram("read_latency", ms::description("Read latency histogram"), [this] {return to_metrics_histogram(_stats.reads.histogram());})(cf)(ks).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),
                    ms::make_histogram("write_latency", ms::description("Write latency histogram"), [this] {return to_metrics_histogram(_stats.writes.histogram());})(cf)(ks).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),
                    ms::make_histogram("cas_prepare_latency", ms::description("CAS prepare round latency histogram"), [this] {return to_metrics_histogram(_stats.cas_prepare.histogram());})(cf)(ks).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),
                    ms::make_histogram("cas_propose_latency", ms::description("CAS accept round latency histogram"), [this] {return to_metrics_histogram(_stats.cas_accept.histogram());})(cf)(ks).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),
                    ms::make_histogram("cas_commit_latency", ms::description("CAS learn round latency histogram"), [this] {return to_metrics_histogram(_stats.cas_learn.histogram());})(cf)(ks).aggregate({seastar::metrics::shard_label}).set_skip_when_empty(),
                    ms::make_gauge("cache_hit_rate", ms::description("Cache hit rate"), [this] {return float(_global_cache_hit_rate);})(cf)(ks)
            });
        }
    }
}

void table::deregister_metrics() {
    _metrics.clear();
    _view_stats._metrics.clear();
}

size_t compaction_group::live_sstable_count() const noexcept {
    return _main_sstables->size() + _maintenance_sstables->size();
}

uint64_t compaction_group::live_disk_space_used() const noexcept {
    return _main_set_disk_space_used + _maintenance_set_disk_space_used;
}

uint64_t compaction_group::total_disk_space_used() const noexcept {
    return live_disk_space_used() + boost::accumulate(_sstables_compacted_but_not_deleted | boost::adaptors::transformed(std::mem_fn(&sstables::sstable::bytes_on_disk)), uint64_t(0));
}

uint64_t compaction_group::calculate_disk_space_used_for(const sstables::sstable_set& set) {
    uint64_t disk_space_used = 0;

    set.for_each_sstable([&] (const sstables::shared_sstable& sst) {
        disk_space_used += sst->bytes_on_disk();
    });
    return disk_space_used;
}

void table::rebuild_statistics() {
    _stats.live_disk_space_used = 0;
    _stats.live_sstable_count = 0;
    _stats.total_disk_space_used = 0;

    for (const compaction_group_ptr& cg : compaction_groups()) {
        _stats.live_disk_space_used += cg->live_disk_space_used();
        _stats.total_disk_space_used += cg->total_disk_space_used();
        _stats.live_sstable_count += cg->live_sstable_count();
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
        co_await coroutine::maybe_yield();
    }
    co_return make_lw_shared<sstables::sstable_set>(std::move(new_sstable_list));
}

future<>
compaction_group::delete_sstables_atomically(std::vector<sstables::shared_sstable> sstables_to_remove) {
    return seastar::try_with_gate(_t._sstable_deletion_gate, [this, sstables_to_remove = std::move(sstables_to_remove)] () mutable {
        return with_semaphore(_t._sstable_deletion_sem, 1, [this, sstables_to_remove = std::move(sstables_to_remove)] () mutable {
            return _t.get_sstables_manager().delete_atomically(std::move(sstables_to_remove));
        });
    }).handle_exception([] (std::exception_ptr ex) {
        // There is nothing more we can do here.
        // Any remaining SSTables will eventually be re-compacted and re-deleted.
        tlogger.error("Compacted SSTables deletion failed: {}. Ignored.", std::move(ex));
    });
}

future<>
compaction_group::update_sstable_lists_on_off_strategy_completion(sstables::compaction_completion_desc desc) {
    class sstable_lists_updater : public row_cache::external_updater_impl {
        using sstables_t = std::vector<sstables::shared_sstable>;
        table& _t;
        compaction_group& _cg;
        table::sstable_list_builder _builder;
        const sstables_t& _old_maintenance;
        const sstables_t& _new_main;
        lw_shared_ptr<sstables::sstable_set> _new_maintenance_list;
        lw_shared_ptr<sstables::sstable_set> _new_main_list;
    public:
        explicit sstable_lists_updater(compaction_group& cg, table::sstable_list_builder::permit_t permit, const sstables_t& old_maintenance, const sstables_t& new_main)
                : _t(cg._t), _cg(cg), _builder(std::move(permit)), _old_maintenance(old_maintenance), _new_main(new_main) {
        }
        virtual future<> prepare() override {
            sstables_t empty;
            // adding new sstables, created by off-strategy operation, to main set
            _new_main_list = co_await _builder.build_new_list(*_cg.main_sstables(), _t._compaction_strategy.make_sstable_set(_t._schema), _new_main, empty);
            // removing old sstables, used as input by off-strategy, from the maintenance set
            _new_maintenance_list = co_await _builder.build_new_list(*_cg.maintenance_sstables(), std::move(*_t.make_maintenance_sstable_set()), empty, _old_maintenance);
        }
        virtual void execute() override {
            _cg.set_main_sstables(std::move(_new_main_list));
            _cg.set_maintenance_sstables(std::move(_new_maintenance_list));
            _t.refresh_compound_sstable_set();
            // Input sstables aren't not removed from backlog tracker because they come from the maintenance set.
            _cg.backlog_tracker_adjust_charges({}, _new_main);
        }
        static std::unique_ptr<row_cache::external_updater_impl> make(compaction_group& cg, table::sstable_list_builder::permit_t permit, const sstables_t& old_maintenance, const sstables_t& new_main) {
            return std::make_unique<sstable_lists_updater>(cg, std::move(permit), old_maintenance, new_main);
        }
    };
    auto permit = co_await seastar::get_units(_t._sstable_set_mutation_sem, 1);
    auto updater = row_cache::external_updater(sstable_lists_updater::make(*this, std::move(permit), desc.old_sstables, desc.new_sstables));

    // row_cache::invalidate() is only used to synchronize sstable list updates, to prevent race conditions from occurring,
    // meaning nothing is actually invalidated.
    dht::partition_range_vector empty_ranges = {};
    co_await _t.get_row_cache().invalidate(std::move(updater), std::move(empty_ranges));
    _t.get_row_cache().refresh_snapshot();
    _t.rebuild_statistics();

    std::unordered_set<sstables::shared_sstable> output(desc.new_sstables.begin(), desc.new_sstables.end());
    // Input SSTables that weren't added to the main SSTable set, can be unlinked.
    // An input SSTable remains linked if it hadn't gone through reshape compaction. Such a SSTable
    // will only be moved from maintenance (source) to main (destination) set.
    auto sstables_to_remove = boost::copy_range<std::vector<sstables::shared_sstable>>(desc.old_sstables
            | boost::adaptors::filtered([&output] (const sstables::shared_sstable& input_sst) {
                return !output.contains(input_sst);
            }));
    co_await delete_sstables_atomically(std::move(sstables_to_remove));
}

future<>
compaction_group::update_main_sstable_list_on_compaction_completion(sstables::compaction_completion_desc desc) {
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
        auto& schema = _t.schema();
        if (shards.size() > 1) {
            throw std::runtime_error(format("A regular compaction for {}.{} INCORRECTLY used shared sstable {}. Only resharding work with those!",
                schema->ks_name(), schema->cf_name(), sst->toc_filename()));
        }
        if (!belongs_to_current_shard(shards)) {
            throw std::runtime_error(format("A regular compaction for {}.{} INCORRECTLY used sstable {} which doesn't belong to this shard!",
                schema->ks_name(), schema->cf_name(), sst->toc_filename()));
        }
    }

    // Precompute before so undo_compacted_but_not_deleted can be sure not to throw
    std::unordered_set<sstables::shared_sstable> s(
           desc.old_sstables.begin(), desc.old_sstables.end());
    auto& sstables_compacted_but_not_deleted = _sstables_compacted_but_not_deleted;
    sstables_compacted_but_not_deleted.insert(sstables_compacted_but_not_deleted.end(), desc.old_sstables.begin(), desc.old_sstables.end());
    // After we are done, unconditionally remove compacted sstables from _sstables_compacted_but_not_deleted,
    // or they could stay forever in the set, resulting in deleted files remaining
    // opened and disk space not being released until shutdown.
    auto undo_compacted_but_not_deleted = defer([&] {
        auto e = boost::range::remove_if(sstables_compacted_but_not_deleted, [&] (sstables::shared_sstable sst) {
            return s.contains(sst);
        });
        sstables_compacted_but_not_deleted.erase(e, sstables_compacted_but_not_deleted.end());
        _t.rebuild_statistics();
    });

    class sstable_list_updater : public row_cache::external_updater_impl {
        table& _t;
        compaction_group& _cg;
        table::sstable_list_builder _builder;
        const sstables::compaction_completion_desc& _desc;
        lw_shared_ptr<sstables::sstable_set> _new_sstables;
    public:
        explicit sstable_list_updater(compaction_group& cg, table::sstable_list_builder::permit_t permit, sstables::compaction_completion_desc& d)
            : _t(cg._t), _cg(cg), _builder(std::move(permit)), _desc(d) {}
        virtual future<> prepare() override {
            _new_sstables = co_await _builder.build_new_list(*_cg.main_sstables(), _t._compaction_strategy.make_sstable_set(_t._schema), _desc.new_sstables, _desc.old_sstables);
        }
        virtual void execute() override {
            _cg.set_main_sstables(std::move(_new_sstables));
            _t.refresh_compound_sstable_set();
            _cg.backlog_tracker_adjust_charges(_desc.old_sstables, _desc.new_sstables);
        }
        static std::unique_ptr<row_cache::external_updater_impl> make(compaction_group& cg, table::sstable_list_builder::permit_t permit, sstables::compaction_completion_desc& d) {
            return std::make_unique<sstable_list_updater>(cg, std::move(permit), d);
        }
    };
    auto permit = co_await seastar::get_units(_t._sstable_set_mutation_sem, 1);
    auto updater = row_cache::external_updater(sstable_list_updater::make(*this, std::move(permit), desc));
    auto& cache = _t.get_row_cache();

    co_await cache.invalidate(std::move(updater), std::move(desc.ranges_for_cache_invalidation));

    // refresh underlying data source in row cache to prevent it from holding reference
    // to sstables files that are about to be deleted.
    cache.refresh_snapshot();

    _t.rebuild_statistics();

    co_await delete_sstables_atomically(std::move(desc.old_sstables));
}

future<>
table::compact_all_sstables(tasks::task_info info) {
    co_await flush();
    co_await parallel_foreach_compaction_group([this, info] (compaction_group& cg) {
        return _compaction_manager.perform_major_compaction(cg.as_table_state(), info);
    });
}

void table::start_compaction() {
    set_compaction_strategy(_schema->compaction_strategy());
}

void table::trigger_compaction() {
    for (const compaction_group_ptr& cg : compaction_groups()) {
        cg->trigger_compaction();
    }
}

void table::try_trigger_compaction(compaction_group& cg) noexcept {
    try {
        cg.trigger_compaction();
    } catch (...) {
        tlogger.error("Failed to trigger compaction: {}", std::current_exception());
    }
}

void compaction_group::trigger_compaction() {
    // But not if we're locked out or stopping
    if (!_t._async_gate.is_closed()) {
        _t._compaction_manager.submit(as_table_state());
    }
}

void table::trigger_offstrategy_compaction() {
    // Run in background.
    // This is safe since the the compaction task is tracked
    // by the compaction_manager until stop()
    (void)perform_offstrategy_compaction().then_wrapped([this] (future<bool> f) {
        if (f.failed()) {
            auto ex = f.get_exception();
            tlogger.warn("Offstrategy compaction of {}.{} failed: {}, ignoring", schema()->ks_name(), schema()->cf_name(), ex);
        }
    });
}

future<bool> table::perform_offstrategy_compaction() {
    // If the user calls trigger_offstrategy_compaction() to trigger
    // off-strategy explicitly, cancel the timeout based automatic trigger.
    _off_strategy_trigger.cancel();
    bool performed = false;
    co_await parallel_foreach_compaction_group([this, &performed] (compaction_group& cg) -> future<> {
        performed |= co_await _compaction_manager.perform_offstrategy(cg.as_table_state());
    });
    co_return performed;
}

future<> table::perform_cleanup_compaction(compaction::owned_ranges_ptr sorted_owned_ranges) {
    co_await flush();

    if (_compaction_groups.size() == 1) {
        auto& cg = *_compaction_groups[0];
        co_return co_await get_compaction_manager().perform_cleanup(std::move(sorted_owned_ranges), cg.as_table_state());
    }

    // candidate ranges for the next compaction_group
    std::deque<dht::token_range> candidates;
    for (auto&& range : *sorted_owned_ranges) {
        candidates.emplace_back(std::move(range));
    }

    auto cmp = dht::token_comparator();
    dht::token_range_vector cg_ranges;
    std::unordered_map<dht::token_range, compaction::owned_ranges_ptr> cg_ranges_map;
    for (const auto& cg : _compaction_groups) {
        const auto& cg_range = cg->token_range();
        while (!candidates.empty()) {
            auto range = std::move(candidates.front());
            auto trimmed = range.intersection(cg_range, cmp);
            if (!trimmed) {
                assert(!cg_ranges.empty());
                break;
            }
            cg_ranges.emplace_back(*trimmed);
            candidates.pop_front();
            if (!trimmed->contains(range, cmp)) {
                auto remainder = range.subtract(*trimmed, cmp);
                assert(remainder.size() == 1);
                candidates.emplace_front(std::move(remainder[0]));
                break;
            }
        }
        cg_ranges_map[cg_range] = compaction::make_owned_ranges_ptr(std::move(cg_ranges));
        co_await coroutine::maybe_yield();
    }
    co_await parallel_foreach_compaction_group([&] (compaction_group& cg) {
        auto&& cg_ranges = std::move(cg_ranges_map.at(cg.token_range()));
        return get_compaction_manager().perform_cleanup(std::move(cg_ranges), cg.as_table_state());
    });
}

unsigned table::estimate_pending_compactions() const {
    return boost::accumulate(compaction_groups() | boost::adaptors::transformed([this] (const compaction_group_ptr& cg) {
        return _compaction_strategy.estimated_pending_compactions(cg->as_table_state());
    }), unsigned(0));
}

void compaction_group::set_compaction_strategy_state(compaction::compaction_strategy_state compaction_strategy_state) noexcept {
    _compaction_strategy_state = std::move(compaction_strategy_state);
}

void table::set_compaction_strategy(sstables::compaction_strategy_type strategy) {
    tlogger.debug("Setting compaction strategy of {}.{} to {}", _schema->ks_name(), _schema->cf_name(), sstables::compaction_strategy::name(strategy));
    auto new_cs = make_compaction_strategy(strategy, _schema->compaction_strategy_options());

    struct compaction_group_sstable_set_updater {
        table& t;
        compaction_group& cg;
        compaction_backlog_tracker new_bt;
        compaction::compaction_strategy_state new_cs_state;
        lw_shared_ptr<sstables::sstable_set> new_sstables;

        compaction_group_sstable_set_updater(table& t, compaction_group& cg, sstables::compaction_strategy& new_cs)
            : t(t)
            , cg(cg)
            , new_bt(new_cs.make_backlog_tracker())
            , new_cs_state(compaction::compaction_strategy_state::make(new_cs)) {
        }

        void prepare(sstables::compaction_strategy& new_cs) {
            auto move_read_charges = new_cs.type() == t._compaction_strategy.type();
            cg.get_backlog_tracker().copy_ongoing_charges(new_bt, move_read_charges);

            new_sstables = make_lw_shared<sstables::sstable_set>(new_cs.make_sstable_set(t._schema));
            std::vector<sstables::shared_sstable> new_sstables_for_backlog_tracker;
            new_sstables_for_backlog_tracker.reserve(cg.main_sstables()->size());
            cg.main_sstables()->for_each_sstable([this, &new_sstables_for_backlog_tracker] (const sstables::shared_sstable& s) {
                new_sstables->insert(s);
                new_sstables_for_backlog_tracker.push_back(s);
            });
            new_bt.replace_sstables({}, std::move(new_sstables_for_backlog_tracker));
        }

        void execute() noexcept {
            t._compaction_manager.register_backlog_tracker(cg.as_table_state(), std::move(new_bt));
            cg.set_main_sstables(std::move(new_sstables));
            cg.set_compaction_strategy_state(std::move(new_cs_state));
        }
    };
    std::vector<compaction_group_sstable_set_updater> cg_sstable_set_updaters;

    for (const compaction_group_ptr& cg : compaction_groups()) {
        compaction_group_sstable_set_updater updater(*this, *cg, new_cs);
        updater.prepare(new_cs);
        cg_sstable_set_updaters.push_back(std::move(updater));
    }
    // now exception safe:
    _compaction_strategy = std::move(new_cs);
    for (auto& updater : cg_sstable_set_updaters) {
        updater.execute();
    }
    refresh_compound_sstable_set();
}

size_t table::sstables_count() const {
    return _sstables->size();
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

future<std::unordered_set<sstables::shared_sstable>> table::get_sstables_by_partition_key(const sstring& key) const {
    auto pk = partition_key::from_nodetool_style_string(_schema, key);
    auto dk = dht::decorate_key(*_schema, pk);
    auto hk = sstables::sstable::make_hashed_key(*_schema, dk.key());

    auto sel = std::make_unique<sstables::sstable_set::incremental_selector>(get_sstable_set().make_incremental_selector());
    const auto& sst = sel->select(dk).sstables;

    std::unordered_set<sstables::shared_sstable> ssts;
    for (auto s : sst) {
        if (co_await s->has_partition_key(hk, dk)) {
            ssts.insert(s);
        }
    }
    co_return ssts;
}

const sstables::sstable_set& table::get_sstable_set() const {
    return *_sstables;
}

lw_shared_ptr<const sstable_list> table::get_sstables() const {
    return _sstables->all();
}

std::vector<sstables::shared_sstable> table::select_sstables(const dht::partition_range& range) const {
    return _sstables->select(range);
}

// Gets the list of all sstables in the column family, including ones that are
// not used for active queries because they have already been compacted, but are
// waiting for delete_atomically() to return.
//
// As long as we haven't deleted them, compaction needs to ensure it doesn't
// garbage-collect a tombstone that covers data in an sstable that may not be
// successfully deleted.
lw_shared_ptr<const sstable_list> table::get_sstables_including_compacted_undeleted() const {
    bool no_compacted_undeleted_sstable = std::ranges::all_of(compaction_groups(), [] (const compaction_group_ptr& cg) {
        return cg->compacted_undeleted_sstables().empty();
    });
    if (no_compacted_undeleted_sstable) {
        return get_sstables();
    }
    auto ret = make_lw_shared<sstable_list>(*_sstables->all());
    for (const compaction_group_ptr& cg : compaction_groups()) {
        for (auto&& s: cg->compacted_undeleted_sstables()) {
            ret->insert(s);
        }
    }
    return ret;
}

const std::vector<sstables::shared_sstable>& compaction_group::compacted_undeleted_sstables() const noexcept {
    return _sstables_compacted_but_not_deleted;
}

lw_shared_ptr<memtable_list>
table::make_memory_only_memtable_list() {
    auto get_schema = [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(get_schema), _config.dirty_memory_manager, _stats, _config.memory_compaction_scheduling_group);
}

lw_shared_ptr<memtable_list>
table::make_memtable_list(compaction_group& cg) {
    auto seal = [this, &cg] (flush_permit&& permit) {
        return seal_active_memtable(cg, std::move(permit));
    };
    auto get_schema = [this] { return schema(); };
    return make_lw_shared<memtable_list>(std::move(seal), std::move(get_schema), _config.dirty_memory_manager, _stats, _config.memory_compaction_scheduling_group);
}

compaction_group::compaction_group(table& t, std::string group_id, dht::token_range token_range)
    : _t(t)
    , _table_state(std::make_unique<table_state>(t, *this))
    , _group_id(std::move(group_id))
    , _token_range(std::move(token_range))
    , _compaction_strategy_state(compaction::compaction_strategy_state::make(_t._compaction_strategy))
    , _memtables(_t._config.enable_disk_writes ? _t.make_memtable_list(*this) : _t.make_memory_only_memtable_list())
    , _main_sstables(make_lw_shared<sstables::sstable_set>(t._compaction_strategy.make_sstable_set(t.schema())))
    , _maintenance_sstables(t.make_maintenance_sstable_set())
{
    _t._compaction_manager.add(as_table_state());
}

future<> compaction_group::stop() noexcept {
    try {
        // FIXME: make memtable_list::flush() noexcept too.
        return _memtables->flush().finally([this] {
            return _t._compaction_manager.remove(as_table_state());
        });
    } catch (...) {
        return current_exception_as_future<>();
    }
}

void compaction_group::clear_sstables() {
    _main_sstables = make_lw_shared<sstables::sstable_set>(_t._compaction_strategy.make_sstable_set(_t._schema));
    _maintenance_sstables = _t.make_maintenance_sstable_set();
}

static std::atomic<unsigned> minimum_x_log2_compaction_groups{0};

void set_minimum_x_log2_compaction_groups(unsigned x_log2_compaction_groups) {
    minimum_x_log2_compaction_groups.store(x_log2_compaction_groups, std::memory_order_relaxed);
}

static inline unsigned get_x_log2_compaction_groups(unsigned x_log2_compaction_groups) {
    return std::max(x_log2_compaction_groups, minimum_x_log2_compaction_groups.load(std::memory_order_relaxed));
}

table::table(schema_ptr schema, config config, lw_shared_ptr<const storage_options> sopts, db::commitlog* cl, compaction_manager& compaction_manager,
        sstables::sstables_manager& sst_manager, cell_locker_stats& cl_stats, cache_tracker& row_cache_tracker,
        locator::effective_replication_map_ptr erm)
    : _schema(std::move(schema))
    , _config(std::move(config))
    , _erm(std::move(erm))
    , _storage_opts(std::move(sopts))
    , _view_stats(format("{}_{}_view_replica_update", _schema->ks_name(), _schema->cf_name()),
                         keyspace_label(_schema->ks_name()),
                         column_family_label(_schema->cf_name())
                        )
    , _x_log2_compaction_groups(get_x_log2_compaction_groups(_config.x_log2_compaction_groups))
    , _compaction_manager(compaction_manager)
    , _compaction_strategy(make_compaction_strategy(_schema->compaction_strategy(), _schema->compaction_strategy_options()))
    , _compaction_groups(make_compaction_groups())
    , _sstables(make_compound_sstable_set())
    , _cache(_schema, sstables_as_snapshot_source(), row_cache_tracker, is_continuous::yes)
    , _commitlog(cl)
    , _durable_writes(true)
    , _sstables_manager(sst_manager)
    , _index_manager(this->as_data_dictionary())
    , _counter_cell_locks(_schema->is_counter() ? std::make_unique<cell_locker>(_schema, cl_stats) : nullptr)
    , _row_locker(_schema)
    , _off_strategy_trigger([this] { trigger_offstrategy_compaction(); })
{
    if (!_config.enable_disk_writes) {
        tlogger.warn("Writes disabled, column family no durable.");
    }
    set_metrics();
}

void table::update_effective_replication_map(locator::effective_replication_map_ptr erm) {
    _erm = std::move(erm);
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
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd,
                mutation_reader::forwarding fwd_mr) {
            auto reader = make_sstable_reader(std::move(s), std::move(permit), sst_set, r, slice, std::move(trace_state), fwd, fwd_mr);
            return make_compacting_reader(
                std::move(reader),
                gc_clock::now(),
                [](const dht::decorated_key&) { return api::max_timestamp; },
                _compaction_manager.get_tombstone_gc_state(),
                fwd);
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
    for (const compaction_group_ptr& cg : compaction_groups()) {
        for (auto& m : *cg->memtables()) {
            res += m->region().occupancy();
        }
    }
    return res;
}


future<>
table::seal_snapshot(sstring jsondir, std::vector<snapshot_file_set> file_sets) {
    std::ostringstream ss;
    int n = 0;
    ss << "{" << std::endl << "\t\"files\" : [ ";
    for (const auto& fsp : file_sets) {
      for (const auto& rf : *fsp) {
        if (n++ > 0) {
            ss << ", ";
        }
        ss << "\"" << rf << "\"";
        co_await coroutine::maybe_yield();
      }
    }
    ss << " ]" << std::endl << "}" << std::endl;

    auto json = ss.str();
    auto jsonfile = jsondir + "/manifest.json";

    tlogger.debug("Storing manifest {}", jsonfile);

    co_await io_check([jsondir] { return recursive_touch_directory(jsondir); });
    auto f = co_await open_checked_file_dma(general_disk_error_handler, jsonfile, open_flags::wo | open_flags::create | open_flags::truncate);
    auto out = co_await make_file_output_stream(std::move(f));
    std::exception_ptr ex;
    try {
        co_await out.write(json.c_str(), json.size());
        co_await out.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await out.close();

    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }

    co_await io_check(sync_directory, std::move(jsondir));
}

future<> table::write_schema_as_cql(database& db, sstring dir) const {
    std::ostringstream ss;

    this->schema()->describe(db, ss, false);

    auto schema_description = ss.str();
    auto schema_file_name = dir + "/schema.cql";
    auto f = co_await open_checked_file_dma(general_disk_error_handler, schema_file_name, open_flags::wo | open_flags::create | open_flags::truncate);
    auto out = co_await make_file_output_stream(std::move(f));
    std::exception_ptr ex;

    try {
        co_await out.write(schema_description.c_str(), schema_description.size());
        co_await out.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await out.close();

    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

// Runs the orchestration code on an arbitrary shard to balance the load.
future<> table::snapshot_on_all_shards(sharded<database>& sharded_db, const global_table_ptr& table_shards, sstring name) {
    auto jsondir = table_shards->_config.datadir + "/snapshots/" + name;
    auto orchestrator = std::hash<sstring>()(jsondir) % smp::count;

    co_await smp::submit_to(orchestrator, [&] () -> future<> {
        auto& t = *table_shards;
        auto s = t.schema();
        tlogger.debug("Taking snapshot of {}.{}: directory={}", s->ks_name(), s->cf_name(), jsondir);

        std::vector<table::snapshot_file_set> file_sets;
        file_sets.reserve(smp::count);

        co_await coroutine::parallel_for_each(boost::irange(0u, smp::count), [&] (unsigned shard) -> future<> {
            file_sets.emplace_back(co_await smp::submit_to(shard, [&] {
                return table_shards->take_snapshot(sharded_db.local(), jsondir);
            }));
        });

        co_await t.finalize_snapshot(sharded_db.local(), std::move(jsondir), std::move(file_sets));
    });
}

future<table::snapshot_file_set> table::take_snapshot(database& db, sstring jsondir) {
    tlogger.trace("take_snapshot {}", jsondir);

    auto sstable_deletion_guard = co_await get_units(_sstable_deletion_sem, 1);
    std::exception_ptr ex;

    auto tables = boost::copy_range<std::vector<sstables::shared_sstable>>(*_sstables->all());
    auto table_names = std::make_unique<std::unordered_set<sstring>>();

    co_await io_check([&jsondir] { return recursive_touch_directory(jsondir); });
    co_await max_concurrent_for_each(tables, db.get_sharded_sst_dir_semaphore().local()._concurrency, [&db, &jsondir, &table_names] (sstables::shared_sstable sstable) {
        table_names->insert(sstable->component_basename(sstables::component_type::Data));
        return with_semaphore(db.get_sharded_sst_dir_semaphore().local()._sem, 1, [&jsondir, sstable] {
            return io_check([sstable, &dir = jsondir] {
                return sstable->snapshot(dir);
            });
        });
    });
    co_await io_check(sync_directory, jsondir);
    co_return make_foreign(std::move(table_names));
}

future<> table::finalize_snapshot(database& db, sstring jsondir, std::vector<snapshot_file_set> file_sets) {
    std::exception_ptr ex;

    tlogger.debug("snapshot {}: writing schema.cql", jsondir);
    co_await write_schema_as_cql(db, jsondir).handle_exception([&] (std::exception_ptr ptr) {
        tlogger.error("Failed writing schema file in snapshot in {} with exception {}", jsondir, ptr);
        ex = std::move(ptr);
    });
    tlogger.debug("snapshot {}: seal_snapshot", jsondir);
    co_await seal_snapshot(jsondir, std::move(file_sets)).handle_exception([&] (std::exception_ptr ptr) {
        tlogger.error("Failed to seal snapshot in {}: {}.", jsondir, ptr);
        ex = std::move(ptr);
    });

    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
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
            fs::path snapshots_dir = fs::path(datadir) / sstables::snapshots_dir;
            auto file_exists = io_check([&snapshots_dir] { return seastar::file_exists(snapshots_dir.native()); }).get0();
            if (!file_exists) {
                continue;
            }

            lister::scan_dir(snapshots_dir,  lister::dir_entry_types::of<directory_entry_type::directory>(), [datadir, &all_snapshots] (fs::path snapshots_dir, directory_entry de) {
                auto snapshot_name = de.name;
                all_snapshots.emplace(snapshot_name, snapshot_details());
                return lister::scan_dir(snapshots_dir / fs::path(snapshot_name),  lister::dir_entry_types::of<directory_entry_type::regular>(), [datadir, &all_snapshots, snapshot_name] (fs::path snapshot_dir, directory_entry de) {
                    return io_check(file_stat, (snapshot_dir / de.name).native(), follow_symlink::no).then([datadir, &all_snapshots, snapshot_name, snapshot_dir, name = de.name] (stat_data sd) {
                        auto size = sd.allocated_size;

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

future<> compaction_group::flush() {
    return _memtables->flush();
}

bool compaction_group::can_flush() const {
    return _memtables->can_flush();
}

lw_shared_ptr<memtable_list>& compaction_group::memtables() noexcept {
    return _memtables;
}

size_t compaction_group::memtable_count() const noexcept {
    return _memtables->size();
}

future<> table::flush(std::optional<db::replay_position> pos) {
    if (pos && *pos < _flush_rp) {
        co_return;
    }
    auto op = _pending_flushes_phaser.start();
    auto fp = _highest_rp;
    co_await parallel_foreach_compaction_group(std::mem_fn(&compaction_group::flush));
    _flush_rp = std::max(_flush_rp, fp);
}

bool table::can_flush() const {
    return std::ranges::any_of(compaction_groups(), std::mem_fn(&compaction_group::can_flush));
}

future<> compaction_group::clear_memtables() {
    if (_t.commitlog()) {
        for (auto& t : *_memtables) {
            _t.commitlog()->discard_completed_segments(_t.schema()->id(), t->get_and_discard_rp_set());
        }
    }
    auto old_memtables = _memtables->clear_and_add();
    for (auto& smt : old_memtables) {
        co_await smt->clear_gently();
    }
}

future<> table::clear() {
    auto permits = co_await _config.dirty_memory_manager->get_all_flush_permits();

    co_await parallel_foreach_compaction_group(std::mem_fn(&compaction_group::clear_memtables));

    co_await _cache.invalidate(row_cache::external_updater([] { /* There is no underlying mutation source */ }));
}

// NOTE: does not need to be futurized, but might eventually, depending on
// if we implement notifications, whatnot.
future<db::replay_position> table::discard_sstables(db_clock::time_point truncated_at) {
    assert(std::ranges::all_of(compaction_groups(), [this] (const compaction_group_ptr& cg) {
        return _compaction_manager.compaction_disabled(cg->as_table_state());
    }));

    struct pruner {
        column_family& cf;
        db::replay_position rp;
        struct removed_sstable {
            compaction_group& cg;
            sstables::shared_sstable sst;
            replica::enable_backlog_tracker enable_backlog_tracker;
        };
        std::vector<removed_sstable> remove;

        pruner(column_family& cf)
            : cf(cf) {}

        void prune(compaction_group& cg, db_clock::time_point truncated_at) {
            auto gc_trunc = to_gc_clock(truncated_at);

            auto pruned = make_lw_shared<sstables::sstable_set>(cf._compaction_strategy.make_sstable_set(cf._schema));
            auto maintenance_pruned = cf.make_maintenance_sstable_set();

            auto prune = [this, &cg, &gc_trunc] (lw_shared_ptr<sstables::sstable_set>& pruned,
                                            const lw_shared_ptr<sstables::sstable_set>& pruning,
                                            replica::enable_backlog_tracker enable_backlog_tracker) mutable {
                pruning->for_each_sstable([&] (const sstables::shared_sstable& p) mutable {
                    if (p->max_data_age() <= gc_trunc) {
                        rp = std::max(p->get_stats_metadata().position, rp);
                        remove.emplace_back(removed_sstable{cg, p, enable_backlog_tracker});
                        return;
                    }
                    pruned->insert(p);
                });
            };
            prune(pruned, cg.main_sstables(), enable_backlog_tracker::yes);
            prune(maintenance_pruned, cg.maintenance_sstables(), enable_backlog_tracker::no);

            cg.set_main_sstables(std::move(pruned));
            cg.set_maintenance_sstables(std::move(maintenance_pruned));
        }
    };
    auto p = make_lw_shared<pruner>(*this);
    co_await _cache.invalidate(row_cache::external_updater([this, p, truncated_at] {
        for (const compaction_group_ptr& cg : compaction_groups()) {
            p->prune(*cg, truncated_at);
        }
        refresh_compound_sstable_set();
        tlogger.debug("cleaning out row cache");
    }));
    rebuild_statistics();
    co_await coroutine::parallel_for_each(p->remove, [this, p] (pruner::removed_sstable& r) -> future<> {
        if (r.enable_backlog_tracker) {
            remove_sstable_from_backlog_tracker(r.cg.get_backlog_tracker(), r.sst);
        }
        co_await get_sstables_manager().delete_atomically({r.sst});
        erase_sstable_cleanup_state(r.sst);
    });
    co_return p->rp;
}

void table::set_schema(schema_ptr s) {
    assert(s->is_counter() == _schema->is_counter());
    tlogger.debug("Changing schema version of {}.{} ({}) from {} to {}",
                _schema->ks_name(), _schema->cf_name(), _schema->id(), _schema->version(), s->version());

    for (const compaction_group_ptr& cg : compaction_groups()) {
        for (auto& m: *cg->memtables()) {
            m->set_schema(s);
        }
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

std::vector<view_ptr> table::affected_views(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& base, const mutation& update) const {
    //FIXME: Avoid allocating a vector here; consider returning the boost iterator.
    return boost::copy_range<std::vector<view_ptr>>(_views | boost::adaptors::filtered([&] (auto&& view) {
        return db::view::partition_key_matches(gen->get_db().as_data_dictionary(), *base, *view->view_info(), update.decorated_key());
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
future<> table::generate_and_propagate_view_updates(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& base,
        reader_permit permit,
        std::vector<db::view::view_and_base>&& views,
        mutation&& m,
        flat_mutation_reader_v2_opt existings,
        tracing::trace_state_ptr tr_state,
        gc_clock::time_point now) const {
    auto base_token = m.token();
    auto m_schema = m.schema();
    db::view::view_update_builder builder = db::view::make_view_update_builder(
            gen->get_db().as_data_dictionary(),
            *this,
            base,
            std::move(views),
            make_flat_mutation_reader_from_mutations_v2(std::move(m_schema), std::move(permit), std::move(m)),
            std::move(existings),
            now);

    std::exception_ptr err = nullptr;
    while (true) {
        std::optional<utils::chunked_vector<frozen_mutation_and_schema>> updates;
        try {
            updates = co_await builder.build_some();
        } catch (...) {
            err = std::current_exception();
            break;
        }
        if (!updates) {
            break;
        }
        tracing::trace(tr_state, "Generated {} view update mutations", updates->size());
        auto units = seastar::consume_units(*_config.view_update_concurrency_semaphore, memory_usage_of(*updates));
        try {
            co_await gen->mutate_MV(base_token, std::move(*updates), _view_stats, *_config.cf_stats, tr_state,
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
        shared_ptr<db::view::view_update_generator> gen,
        std::vector<db::view::view_and_base> views,
        dht::token base_token,
        flat_mutation_reader_v2&& reader,
        gc_clock::time_point now) {
    auto schema = reader.schema();
    db::view::view_update_builder builder = db::view::make_view_update_builder(
            gen->get_db().as_data_dictionary(),
            *this,
            schema,
            std::move(views),
            std::move(reader),
            { },
            now);

    std::exception_ptr err;
    while (true) {
        try {
            auto updates = co_await builder.build_some();
            if (!updates) {
                break;
            }
            size_t update_size = memory_usage_of(*updates);
            size_t units_to_wait_for = std::min(_config.view_update_concurrency_semaphore_limit, update_size);
            auto units = co_await seastar::get_units(*_config.view_update_concurrency_semaphore, units_to_wait_for);
            units.adopt(seastar::consume_units(*_config.view_update_concurrency_semaphore, update_size - units_to_wait_for));
            co_await gen->mutate_MV(base_token, std::move(*updates), _view_stats, *_config.cf_stats,
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

table::cache_hit_rate table::get_my_hit_rate() const {
    return cache_hit_rate { _global_cache_hit_rate, lowres_clock::now()};
}

table::cache_hit_rate table::get_hit_rate(const gms::gossiper& gossiper, gms::inet_address addr) {
    if (utils::fb_utilities::get_broadcast_address() == addr) {
        return get_my_hit_rate();
    }
    auto it = _cluster_cache_hit_rates.find(addr);
    if (it == _cluster_cache_hit_rates.end()) {
        // no data yet, get it from the gossiper
        auto* eps = gossiper.get_endpoint_state_for_endpoint_ptr(addr);
        if (eps) {
            auto* state = eps->get_application_state_ptr(gms::application_state::CACHE_HITRATES);
            float f = -1.0f; // missing state means old node
            if (state) {
                sstring me = format("{}.{}", _schema->ks_name(), _schema->cf_name());
                auto i = state->value().find(me);
                if (i != sstring::npos) {
                    f = strtof(&state->value()[i + me.size() + 1], nullptr);
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
void table::do_apply(compaction_group& cg, db::rp_handle&& h, Args&&... args) {
    if (_async_gate.is_closed()) {
        on_internal_error(tlogger, "Table async_gate is closed");
    }

    utils::latency_counter lc;
    _stats.writes.set_latency(lc);
    db::replay_position rp = h;
    check_valid_rp(rp);
    try {
        cg.memtables()->active_memtable().apply(std::forward<Args>(args)..., std::move(h));
        _highest_rp = std::max(_highest_rp, rp);
    } catch (...) {
        _failed_counter_applies_to_memtable++;
        throw;
    }
    _stats.writes.mark(lc);
}

future<> table::apply(const mutation& m, db::rp_handle&& h, db::timeout_clock::time_point timeout) {
    return dirty_memory_region_group().run_when_memory_available([this, &m, h = std::move(h)] () mutable {
        do_apply(compaction_group_for_token(m.token()), std::move(h), m);
    }, timeout);
}

template void table::do_apply(compaction_group& cg, db::rp_handle&&, const mutation&);

future<> table::apply(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&& h, db::timeout_clock::time_point timeout) {
    if (_virtual_writer) [[unlikely]] {
        return (*_virtual_writer)(m);
    }

    return dirty_memory_region_group().run_when_memory_available([this, &m, m_schema = std::move(m_schema), h = std::move(h)]() mutable {
        do_apply(compaction_group_for_key(m.key(), m_schema), std::move(h), m, m_schema);
    }, timeout);
}

template void table::do_apply(compaction_group& cg, db::rp_handle&&, const frozen_mutation&, const schema_ptr&);

future<>
write_memtable_to_sstable(flat_mutation_reader_v2 reader,
                          memtable& mt, sstables::shared_sstable sst,
                          size_t estimated_partitions,
                          sstables::write_monitor& monitor,
                          sstables::sstable_writer_config& cfg) {
    cfg.replay_position = mt.replay_position();
    cfg.monitor = &monitor;
    cfg.origin = "memtable";
    schema_ptr s = reader.schema();
    return sst->write_components(std::move(reader), estimated_partitions, s, cfg, mt.get_encoding_stats());
}

future<>
write_memtable_to_sstable(reader_permit permit, memtable& mt, sstables::shared_sstable sst,
                          sstables::write_monitor& monitor,
                          sstables::sstable_writer_config& cfg) {
    return write_memtable_to_sstable(mt.make_flush_reader(mt.schema(), std::move(permit)), mt, std::move(sst), mt.partition_count(), monitor, cfg);
}

future<>
write_memtable_to_sstable(memtable& mt, sstables::shared_sstable sst, sstables::sstable_writer_config cfg) {
    return do_with(
            replica::permit_monitor(make_lw_shared(sstable_write_permit::unconditional())),
            std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, "write_memtable_to_sstable"),
            cfg,
            [&mt, sst] (auto& monitor, auto& semaphore, auto& cfg) {
        return write_memtable_to_sstable(semaphore->make_tracking_only_permit(mt.schema().get(), "mt_to_sst", db::no_timeout, {}), mt, std::move(sst), monitor, cfg)
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
            , builder(cmd.slice, opts, std::move(memory_accounter), cmd.tombstone_limit)
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
        query::result_options opts,
        const dht::partition_range_vector& partition_ranges,
        tracing::trace_state_ptr trace_state,
        query::result_memory_limiter& memory_limiter,
        db::timeout_clock::time_point timeout,
        std::optional<query::querier>* saved_querier) {
    if (cmd.get_row_limit() == 0 || cmd.slice.partition_row_limit() == 0 || cmd.partition_limit == 0) {
        co_return make_lw_shared<query::result>();
    }

    _async_gate.enter();
    utils::latency_counter lc;
    _stats.reads.set_latency(lc);

    auto finally = defer([&] () noexcept {
        _stats.reads.mark(lc);
        _async_gate.leave();
    });

    const auto short_read_allowed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
    auto accounter = co_await (opts.request == query::result_request::only_digest
             ? memory_limiter.new_digest_read(permit.max_result_size(), short_read_allowed)
             : memory_limiter.new_data_read(permit.max_result_size(), short_read_allowed));

    query_state qs(s, cmd, opts, partition_ranges, std::move(accounter));

    std::optional<query::querier> querier_opt;
    if (saved_querier) {
        querier_opt = std::move(*saved_querier);
    }

    while (!qs.done()) {
        auto&& range = *qs.current_partition_range++;

        if (!querier_opt) {
            query::querier_base::querier_config conf(_config.tombstone_warn_threshold);
            querier_opt = query::querier(as_mutation_source(), s, permit, range, qs.cmd.slice, trace_state, conf);
        }
        auto& q = *querier_opt;

        std::exception_ptr ex;
      try {
        co_await q.consume_page(query_result_builder(*s, qs.builder), qs.remaining_rows(), qs.remaining_partitions(), qs.cmd.timestamp, trace_state);
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

    std::optional<full_position> last_pos;
    if (querier_opt && querier_opt->current_position()) {
        last_pos.emplace(*querier_opt->current_position());
    }

    if (!saved_querier || (querier_opt && !querier_opt->are_limits_reached() && !qs.builder.is_short_read())) {
        co_await querier_opt->close();
        querier_opt = {};
    }
    if (saved_querier) {
        *saved_querier = std::move(querier_opt);
    }

    co_return make_lw_shared<query::result>(qs.builder.build(std::move(last_pos)));
}

future<reconcilable_result>
table::mutation_query(schema_ptr s,
        reader_permit permit,
        const query::read_command& cmd,
        const dht::partition_range& range,
        tracing::trace_state_ptr trace_state,
        query::result_memory_accounter accounter,
        db::timeout_clock::time_point timeout,
        std::optional<query::querier>* saved_querier) {
    if (cmd.get_row_limit() == 0 || cmd.slice.partition_row_limit() == 0 || cmd.partition_limit == 0) {
        co_return reconcilable_result();
    }

    std::optional<query::querier> querier_opt;
    if (saved_querier) {
        querier_opt = std::move(*saved_querier);
    }
    if (!querier_opt) {
        query::querier_base::querier_config conf(_config.tombstone_warn_threshold);
        querier_opt = query::querier(as_mutation_source(), s, permit, range, cmd.slice, trace_state, conf);
    }
    auto& q = *querier_opt;

    std::exception_ptr ex;
  try {
    // Un-reverse the schema sent to the coordinator, it expects the
    // legacy format.
    auto result_schema = cmd.slice.is_reversed() ? s->make_reversed() : s;
    auto rrb = reconcilable_result_builder(*result_schema, cmd.slice, std::move(accounter));
    auto r = co_await q.consume_page(std::move(rrb), cmd.get_row_limit(), cmd.partition_limit, cmd.timestamp, trace_state);

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
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr) {
        return this->make_reader_v2(std::move(s), std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr);
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

void
table::enable_auto_compaction() {
    // FIXME: unmute backlog. turn table backlog back on.
    //      see table::disable_auto_compaction() notes.
    _compaction_disabled_by_user = false;
    trigger_compaction();
}

future<>
table::disable_auto_compaction() {
    // FIXME: mute backlog. When we disable background compactions
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
    return with_gate(_async_gate, [this] {
        return parallel_foreach_compaction_group([this] (compaction_group& cg) {
            return _compaction_manager.stop_ongoing_compactions("disable auto-compaction", &cg.as_table_state(), sstables::compaction_type::Compaction);
        });
    });
}

void table::set_tombstone_gc_enabled(bool tombstone_gc_enabled) noexcept {
    _tombstone_gc_enabled = tombstone_gc_enabled;
    tlogger.info0("Tombstone GC was {} for {}.{}", tombstone_gc_enabled ? "enabled" : "disabled", _schema->ks_name(), _schema->cf_name());
    if (_tombstone_gc_enabled) {
        trigger_compaction();
    }
}

flat_mutation_reader_v2
table::make_reader_v2_excluding_staging(schema_ptr s,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr) const {
    std::vector<flat_mutation_reader_v2> readers;

    add_memtables_to_reader_list(readers, s, permit, range, slice, trace_state, fwd, fwd_mr, [&] (size_t memtable_count) {
        readers.reserve(memtable_count + 1);
    });

    static std::predicate<const sstable&> auto excl_staging_predicate = [] (const sstable& sst) {
        return !sst.requires_view_building();
    };

    readers.emplace_back(make_sstable_reader(s, permit, _sstables, range, slice, std::move(trace_state), fwd, fwd_mr, excl_staging_predicate));
    return make_combined_reader(s, std::move(permit), std::move(readers), fwd, fwd_mr);
}

future<> table::move_sstables_from_staging(std::vector<sstables::shared_sstable> sstables) {
    auto units = co_await get_units(_sstable_deletion_sem, 1);
    sstables::delayed_commit_changes delay_commit;
    std::unordered_set<compaction_group*> compaction_groups_to_notify;
    for (auto sst : sstables) {
        try {
            // Off-strategy can happen in parallel to view building, so the SSTable may be deleted already if the former
            // completed first.
            // The _sstable_deletion_sem prevents list update on off-strategy completion and move_sstables_from_staging()
            // from stepping on each other's toe.
            co_await sst->change_state(sstables::normal_dir, &delay_commit);
            auto& cg = compaction_group_for_sstable(sst);
            if (get_compaction_manager().requires_cleanup(cg.as_table_state(), sst)) {
                compaction_groups_to_notify.insert(&cg);
            }
            // If view building finished faster, SSTable with repair origin still exists.
            // It can also happen the SSTable is not going through reshape, so it doesn't have a repair origin.
            // That being said, we'll only add this SSTable to tracker if its origin is other than repair.
            // Otherwise, we can count on off-strategy completion to add it when updating lists.
            if (sst->get_origin() != sstables::repair_origin) {
                add_sstable_to_backlog_tracker(cg.get_backlog_tracker(), sst);
            }
        } catch (...) {
            tlogger.warn("Failed to move sstable {} from staging: {}", sst->get_filename(), std::current_exception());
            throw;
        }
    }

    co_await delay_commit.commit();

    for (auto* cg : compaction_groups_to_notify) {
        cg->get_staging_done_condition().broadcast();
    }

    // Off-strategy timer will be rearmed, so if there's more incoming data through repair / streaming,
    // the timer can be updated once again. In practice, it allows off-strategy compaction to kick off
    // at the end of the node operation on behalf of this table, which brings more efficiency in terms
    // of write amplification.
    do_update_off_strategy_trigger();
}

/**
 * Given an update for the base table, calculates the set of potentially affected views,
 * generates the relevant updates, and sends them to the paired view replicas.
 */
future<row_locker::lock_holder> table::push_view_replica_updates(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& s, const frozen_mutation& fm,
        db::timeout_clock::time_point timeout, tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem) const {
    //FIXME: Avoid unfreezing here.
    auto m = fm.unfreeze(s);
    return push_view_replica_updates(std::move(gen), s, std::move(m), timeout, std::move(tr_state), sem);
}

future<row_locker::lock_holder> table::do_push_view_replica_updates(shared_ptr<db::view::view_update_generator> gen, schema_ptr s, mutation m, db::timeout_clock::time_point timeout, mutation_source source,
        tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem, query::partition_slice::option_set custom_opts) const {
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
    auto views = db::view::with_base_info_snapshot(affected_views(gen, base, m));
    if (views.empty()) {
        co_return row_locker::lock_holder();
    }
    auto cr_ranges = co_await db::view::calculate_affected_clustering_ranges(gen->get_db().as_data_dictionary(), *base, m.decorated_key(), m.partition(), views);
    const bool need_regular = !cr_ranges.empty();
    const bool need_static = db::view::needs_static_row(m.partition(), views);
    if (!need_regular && !need_static) {
        tracing::trace(tr_state, "View updates do not require read-before-write");
        co_await generate_and_propagate_view_updates(gen, base, sem.make_tracking_only_permit(s.get(), "push-view-updates-1", timeout, tr_state), std::move(views), std::move(m), { }, tr_state, now);
        // In this case we are not doing a read-before-write, just a
        // write, so no lock is needed.
        co_return row_locker::lock_holder();
    }
    // We read whole sets of regular and/or static columns in case the update now causes a base row to pass
    // a view's filters, and a view happens to include columns that have no value in this update.
    // Also, one of those columns can determine the lifetime of the base row, if it has a TTL.
    query::column_id_vector static_columns;
    query::column_id_vector regular_columns;
    if (need_regular) {
        boost::copy(base->regular_columns() | boost::adaptors::transformed(std::mem_fn(&column_definition::id)), std::back_inserter(regular_columns));
    }
    if (need_static) {
        boost::copy(base->static_columns() | boost::adaptors::transformed(std::mem_fn(&column_definition::id)), std::back_inserter(static_columns));
    }
    query::partition_slice::option_set opts;
    opts.set(query::partition_slice::option::send_partition_key);
    opts.set_if<query::partition_slice::option::send_clustering_key>(need_regular);
    opts.set_if<query::partition_slice::option::distinct>(need_static && !need_regular);
    opts.set_if<query::partition_slice::option::always_return_static_content>(need_static);
    opts.set(query::partition_slice::option::send_timestamp);
    opts.set(query::partition_slice::option::send_ttl);
    opts.add(custom_opts);
    auto slice = query::partition_slice(
            std::move(cr_ranges), std::move(static_columns), std::move(regular_columns), std::move(opts), { }, query::max_rows);
    // Take the shard-local lock on the base-table row or partition as needed.
    // We'll return this lock to the caller, which will release it after
    // writing the base-table update.
    future<row_locker::lock_holder> lockf = local_base_lock(base, m.decorated_key(), slice.default_row_ranges(), timeout);
    co_await utils::get_local_injector().inject("table_push_view_replica_updates_timeout", timeout);
    auto lock = co_await std::move(lockf);
    auto pk = dht::partition_range::make_singular(m.decorated_key());
    auto permit = sem.make_tracking_only_permit(base.get(), "push-view-updates-2", timeout, tr_state);
    auto reader = source.make_reader_v2(base, permit, pk, slice, tr_state, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    co_await this->generate_and_propagate_view_updates(gen, base, std::move(permit), std::move(views), std::move(m), std::move(reader), tr_state, now);
    tracing::trace(tr_state, "View updates for {}.{} were generated and propagated", base->ks_name(), base->cf_name());
    // return the local partition/row lock we have taken so it
    // remains locked until the caller is done modifying this
    // partition/row and destroys the lock object.
    co_return std::move(lock);

}

future<row_locker::lock_holder> table::push_view_replica_updates(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout,
        tracing::trace_state_ptr tr_state, reader_concurrency_semaphore& sem) const {
    return do_push_view_replica_updates(std::move(gen), s, std::move(m), timeout, as_mutation_source(),
            std::move(tr_state), sem, {});
}

future<row_locker::lock_holder>
table::stream_view_replica_updates(shared_ptr<db::view::view_update_generator> gen, const schema_ptr& s, mutation&& m, db::timeout_clock::time_point timeout,
        std::vector<sstables::shared_sstable>& excluded_sstables) const {
    return do_push_view_replica_updates(
            std::move(gen),
            s,
            std::move(m),
            timeout,
            as_mutation_source_excluding_staging(),
            tracing::trace_state_ptr(),
            *_config.streaming_read_concurrency_semaphore,
            query::partition_slice::option_set::of<query::partition_slice::option::bypass_cache>());
}

mutation_source
table::as_mutation_source_excluding_staging() const {
    return mutation_source([this] (schema_ptr s,
                                   reader_permit permit,
                                   const dht::partition_range& range,
                                   const query::partition_slice& slice,
                                   tracing::trace_state_ptr trace_state,
                                   streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding fwd_mr) {
        return this->make_reader_v2_excluding_staging(std::move(s), std::move(permit), range, slice, std::move(trace_state), fwd, fwd_mr);
    });
}

std::vector<mutation_source> table::select_memtables_as_mutation_sources(dht::token token) const {
    auto& cg = compaction_group_for_token(token);
    std::vector<mutation_source> mss;
    mss.reserve(cg.memtables()->size());
    for (auto& mt : *cg.memtables()) {
        mss.emplace_back(mt->as_data_source());
    }
    return mss;
}

class compaction_group::table_state : public compaction::table_state {
    table& _t;
    compaction_group& _cg;
public:
    explicit table_state(table& t, compaction_group& cg) : _t(t), _cg(cg) {}

    const schema_ptr& schema() const noexcept override {
        return _t.schema();
    }
    unsigned min_compaction_threshold() const noexcept override {
        // During receiving stream operations, the less we compact the faster streaming is. For
        // bootstrap and replace thereThere are no readers so it is fine to be less aggressive with
        // compactions as long as we don't ignore them completely (this could create a problem for
        // when streaming ends)
        if (_t._is_bootstrap_or_replace) {
            auto target = std::min(_t.schema()->max_compaction_threshold(), 16);
            return std::max(_t.schema()->min_compaction_threshold(), target);
        } else {
            return _t.schema()->min_compaction_threshold();
        }
    }
    bool compaction_enforce_min_threshold() const noexcept override {
        return _t.get_config().compaction_enforce_min_threshold || _t._is_bootstrap_or_replace;
    }
    const sstables::sstable_set& main_sstable_set() const override {
        return *_cg.main_sstables();
    }
    const sstables::sstable_set& maintenance_sstable_set() const override {
        return *_cg.maintenance_sstables();
    }
    std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point query_time) const override {
        return sstables::get_fully_expired_sstables(*this, sstables, query_time);
    }
    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept override {
        return _cg.compacted_undeleted_sstables();
    }
    sstables::compaction_strategy& get_compaction_strategy() const noexcept override {
        return _t.get_compaction_strategy();
    }
    compaction::compaction_strategy_state& get_compaction_strategy_state() noexcept override {
        return _cg._compaction_strategy_state;
    }
    reader_permit make_compaction_reader_permit() const override {
        return _t.compaction_concurrency_semaphore().make_tracking_only_permit(schema().get(), "compaction", db::no_timeout, {});
    }
    sstables::sstables_manager& get_sstables_manager() noexcept override {
        return _t.get_sstables_manager();
    }
    sstables::shared_sstable make_sstable() const override {
        return _t.make_sstable();
    }
    sstables::sstable_writer_config configure_writer(sstring origin) const override {
        auto cfg = _t.get_sstables_manager().configure_writer(std::move(origin));
        cfg.erm = _t.get_effective_replication_map();
        return cfg;
    }
    api::timestamp_type min_memtable_timestamp() const override {
        return _cg.min_memtable_timestamp();
    }
    future<> on_compaction_completion(sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy) override {
        if (offstrategy) {
            co_await _cg.update_sstable_lists_on_off_strategy_completion(std::move(desc));
            _cg.trigger_compaction();
            co_return;
        }
        co_await _cg.update_main_sstable_list_on_compaction_completion(std::move(desc));
    }
    bool is_auto_compaction_disabled_by_user() const noexcept override {
        return _t.is_auto_compaction_disabled_by_user();
    }
    bool tombstone_gc_enabled() const noexcept override {
        return _t._tombstone_gc_enabled;
    }
    const tombstone_gc_state& get_tombstone_gc_state() const noexcept override {
        return _t.get_compaction_manager().get_tombstone_gc_state();
    }
    compaction_backlog_tracker& get_backlog_tracker() override {
        return _t._compaction_manager.get_backlog_tracker(*this);
    }
    const std::string& get_group_id() const noexcept override {
        return _cg.get_group_id();
    }

    seastar::condition_variable& get_staging_done_condition() noexcept override {
        return _cg.get_staging_done_condition();
    }
};

compaction_backlog_tracker& compaction_group::get_backlog_tracker() {
    return as_table_state().get_backlog_tracker();
}

compaction::table_state& compaction_group::as_table_state() const noexcept {
    return *_table_state;
}

compaction::table_state& table::as_table_state() const noexcept {
    // FIXME: kill it once we're done with all remaining users.
    return _compaction_groups[0]->as_table_state();
}

future<> table::parallel_foreach_table_state(std::function<future<>(table_state&)> action) {
    return parallel_foreach_compaction_group([action = std::move(action)] (compaction_group& cg) -> future<> {
       return action(cg.as_table_state());
    });
}

data_dictionary::table
table::as_data_dictionary() const {
    static constinit data_dictionary_impl _impl;
    return _impl.wrap(*this);
}

bool table::erase_sstable_cleanup_state(const sstables::shared_sstable& sst) {
    // FIXME: it's possible that the sstable belongs to multiple compaction_groups
    auto& cg = compaction_group_for_sstable(sst);
    return get_compaction_manager().erase_sstable_cleanup_state(cg.as_table_state(), sst);
}

bool table::requires_cleanup(const sstables::shared_sstable& sst) const {
    auto& cg = compaction_group_for_sstable(sst);
    return get_compaction_manager().requires_cleanup(cg.as_table_state(), sst);
}

bool table::requires_cleanup(const sstables::sstable_set& set) const {
    return bool(set.for_each_sstable_until([this] (const sstables::shared_sstable &sst) {
        auto& cg = compaction_group_for_sstable(sst);
        return stop_iteration(_compaction_manager.requires_cleanup(cg.as_table_state(), sst));
    }));
}

} // namespace replica
