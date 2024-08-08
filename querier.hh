/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/closeable.hh>

#include "mutation/mutation_compactor.hh"
#include "reader_concurrency_semaphore.hh"
#include "readers/mutation_source.hh"
#include "full_position.hh"

#include <boost/intrusive/set.hpp>

#include <variant>

namespace query {

extern logging::logger qrlogger;

/// Consume a page worth of data from the reader.
///
/// Uses `compaction_state` for compacting the fragments and `consumer` for
/// building the results.
/// Returns a future containing a tuple with the last consumed clustering key,
/// or std::nullopt if the last row wasn't a clustering row, and whatever the
/// consumer's `consume_end_of_stream()` method returns.
template <typename Consumer>
requires CompactedFragmentsConsumerV2<Consumer>
auto consume_page(mutation_reader& reader,
        lw_shared_ptr<compact_for_query_state_v2> compaction_state,
        const query::partition_slice& slice,
        Consumer&& consumer,
        uint64_t row_limit,
        uint32_t partition_limit,
        gc_clock::time_point query_time) {
    return reader.peek().then([=, &reader, consumer = std::move(consumer)] (
                mutation_fragment_v2* next_fragment) mutable {
        const auto next_fragment_region = next_fragment ? next_fragment->position().region() : partition_region::partition_start;
        compaction_state->start_new_page(row_limit, partition_limit, query_time, next_fragment_region, consumer);

        auto reader_consumer = compact_for_query_v2<Consumer>(compaction_state, std::move(consumer));

        return reader.consume(std::move(reader_consumer));
    });
}

class querier_base {
    friend class querier_utils;

public:
    struct querier_config {
        uint32_t tombstone_warn_threshold {0}; // 0 disabled
        querier_config() = default;
        explicit querier_config(uint32_t warn)
            : tombstone_warn_threshold(warn) {}
    };

protected:
    schema_ptr _schema;
    reader_permit _permit;
    lw_shared_ptr<const dht::partition_range> _range;
    std::unique_ptr<const query::partition_slice> _slice;
    std::variant<mutation_reader, reader_concurrency_semaphore::inactive_read_handle> _reader;
    dht::partition_ranges_view _query_ranges;
    querier_config _qr_config;

public:
    querier_base(reader_permit permit, lw_shared_ptr<const dht::partition_range> range,
            std::unique_ptr<const query::partition_slice> slice, mutation_reader reader, dht::partition_ranges_view query_ranges)
        : _schema(reader.schema())
        , _permit(std::move(permit))
        , _range(std::move(range))
        , _slice(std::move(slice))
        , _reader(std::move(reader))
        , _query_ranges(query_ranges)
    { }

    querier_base(schema_ptr schema, reader_permit permit, dht::partition_range range,
            query::partition_slice slice, const mutation_source& ms, tracing::trace_state_ptr trace_ptr,
            querier_config config)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _range(make_lw_shared<const dht::partition_range>(std::move(range)))
        , _slice(std::make_unique<const query::partition_slice>(std::move(slice)))
        , _reader(ms.make_reader_v2(_schema, _permit, *_range, *_slice, std::move(trace_ptr), streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
        , _query_ranges(*_range)
        , _qr_config(std::move(config))
    { }

    querier_base(querier_base&&) = default;
    querier_base& operator=(querier_base&&) = default;

    virtual ~querier_base() = default;

    const ::schema& schema() const {
        return *_schema;
    }

    reader_permit& permit() {
        return _permit;
    }

    bool is_reversed() const {
        return _slice->is_reversed();
    }

    virtual std::optional<full_position_view> current_position() const = 0;

    dht::partition_ranges_view ranges() const {
        return _query_ranges;
    }

    size_t memory_usage() const {
        return _permit.consumed_resources().memory;
    }

    future<> close() noexcept;
};

/// One-stop object for serving queries.
///
/// Encapsulates all state and logic for serving all pages for a given range
/// of a query on a given shard. Can be used with any CompactedMutationsConsumer
/// certified result-builder.
/// Intended to be created on the first page of a query then saved and reused on
/// subsequent pages.
/// (1) Create with the parameters of your query.
/// (2) Call consume_page() with your consumer to consume the contents of the
///     next page.
/// (3) At the end of the page save the querier if you expect more pages.
///     The are_limits_reached() method can be used to determine whether the
///     page was filled or not. Also check your result builder for short reads.
///     Most result builders have memory-accounters that will stop the read
///     once some memory limit was reached. This is called a short read as the
///     read stops before the row and/or partition limits are reached.
/// (4) At the beginning of the next page validate whether it can be used with
///     the page's schema and start position. In case a schema or position
///     mismatch is detected the querier shouldn't be used to produce the next
///     page. It should be dropped instead and a new one should be created
///     instead.
class querier : public querier_base {
    static thread_local logger::rate_limit row_tombstone_warn_rate_limit;
    static thread_local logger::rate_limit cell_tombstone_warn_rate_limit;

    lw_shared_ptr<compact_for_query_state_v2> _compaction_state;

private:
    void maybe_log_tombstone_warning(std::string_view what, uint64_t live, uint64_t dead, logger::rate_limit& rl);

public:
    querier(const mutation_source& ms,
            schema_ptr schema,
            reader_permit permit,
            dht::partition_range range,
            query::partition_slice slice,
            tracing::trace_state_ptr trace_ptr,
            querier_config config = {})
        : querier_base(schema, permit, std::move(range), std::move(slice), ms, std::move(trace_ptr), std::move(config))
        , _compaction_state(make_lw_shared<compact_for_query_state_v2>(*schema, gc_clock::time_point{}, *_slice, 0, 0)) {
    }

    bool are_limits_reached() const {
        return  _compaction_state->are_limits_reached();
    }

    template <typename Consumer>
    requires CompactedFragmentsConsumerV2<Consumer>
    auto consume_page(Consumer&& consumer,
            uint64_t row_limit,
            uint32_t partition_limit,
            gc_clock::time_point query_time,
            tracing::trace_state_ptr trace_ptr = {}) {
        return ::query::consume_page(std::get<mutation_reader>(_reader), _compaction_state, *_slice, std::move(consumer), row_limit,
                partition_limit, query_time).then_wrapped([this, trace_ptr = std::move(trace_ptr)] (auto&& fut) {
            const auto& cstats = _compaction_state->stats();
            tracing::trace(trace_ptr, "Page stats: {} partition(s), {} static row(s) ({} live, {} dead), {} clustering row(s) ({} live, {} dead), {} range tombstone(s) and {} cell(s) ({} live, {} dead)",
                    cstats.partitions,
                    cstats.static_rows.total(),
                    cstats.static_rows.live,
                    cstats.static_rows.dead,
                    cstats.clustering_rows.total(),
                    cstats.clustering_rows.live,
                    cstats.clustering_rows.dead,
                    cstats.range_tombstones,
                    cstats.live_cells() + cstats.dead_cells(),
                    cstats.live_cells(),
                    cstats.dead_cells());
            maybe_log_tombstone_warning(
                    "rows",
                    cstats.static_rows.live + cstats.clustering_rows.live,
                    cstats.static_rows.dead + cstats.clustering_rows.dead + cstats.range_tombstones,
                    row_tombstone_warn_rate_limit);
            maybe_log_tombstone_warning("cells", cstats.live_cells(), cstats.dead_cells(), cell_tombstone_warn_rate_limit);
            return std::move(fut);
        });
    }

    virtual std::optional<full_position_view> current_position() const override {
        const dht::decorated_key* dk = _compaction_state->current_partition();
        if (!dk) {
            return {};
        }
        return full_position_view(dk->key(), _compaction_state->current_position());
    }
};

/// Local state of a multishard query.
///
/// This querier is not intended to be used directly to read pages. Instead it
/// is merely a shard local state of a suspended multishard query and is
/// intended to be used for storing the state of the query on each shard where
/// it executes. It stores the local reader and the referenced parameters it was
/// created with (similar to other queriers).
/// For position validation purposes (at lookup) the reader's position is
/// considered to be the same as that of the query.
class shard_mutation_querier : public querier_base {
    std::unique_ptr<const dht::partition_range_vector> _query_ranges;
    full_position _nominal_pos;

private:
    shard_mutation_querier(
            std::unique_ptr<const dht::partition_range_vector> query_ranges,
            lw_shared_ptr<const dht::partition_range> reader_range,
            std::unique_ptr<const query::partition_slice> reader_slice,
            mutation_reader reader,
            reader_permit permit,
            full_position nominal_pos)
        : querier_base(permit, std::move(reader_range), std::move(reader_slice), std::move(reader), *query_ranges)
        , _query_ranges(std::move(query_ranges))
        , _nominal_pos(std::move(nominal_pos)) {
    }


public:
    shard_mutation_querier(
            const dht::partition_range_vector query_ranges,
            lw_shared_ptr<const dht::partition_range> reader_range,
            std::unique_ptr<const query::partition_slice> reader_slice,
            mutation_reader reader,
            reader_permit permit,
            full_position nominal_pos)
        : shard_mutation_querier(std::make_unique<const dht::partition_range_vector>(std::move(query_ranges)), std::move(reader_range),
                std::move(reader_slice), std::move(reader), std::move(permit), std::move(nominal_pos)) {
    }

    virtual std::optional<full_position_view> current_position() const override {
        return _nominal_pos;
    }

    lw_shared_ptr<const dht::partition_range> reader_range() && {
        return std::move(_range);
    }

    std::unique_ptr<const query::partition_slice> reader_slice() && {
        return std::move(_slice);
    }

    mutation_reader reader() && {
        return std::move(std::get<mutation_reader>(_reader));
    }
};

/// Special-purpose cache for saving queriers between pages.
///
/// Queriers are saved at the end of the page and looked up at the beginning of
/// the next page. The lookup() always removes the querier from the cache, it
/// has to be inserted again at the end of the page.
/// Lookup provides the following extra logic, special to queriers:
/// * It accepts a factory function which is used to create a new querier if
///     the lookup fails (see below). This allows for simple call sites.
/// * It does range matching. A query sometimes will result in multiple querier
///     objects executing on the same node and shard parallelly. To identify the
///     appropriate querier lookup() will consider - in addition to the lookup
///     key - the read range.
/// * It does schema version and position checking. In some case a subsequent
///     page will have a different schema version or will start from a position
///     that is before the end position of the previous page. lookup() will
///     recognize these cases and drop the previous querier and create a new one.
///
/// Inserted queriers will have a TTL. When this expires the querier is
/// evicted. This is to avoid excess and unnecessary resource usage due to
/// abandoned queriers.
/// Registers cached readers with the reader concurrency semaphore, as inactive
/// readers, so the latter can evict them if needed.
/// Keeps the total memory consumption of cached queriers
/// below max_queriers_memory_usage by evicting older entries upon inserting
/// new ones if the the memory consupmtion would go above the limit.
class querier_cache {
public:
    static const std::chrono::seconds default_entry_ttl;

    struct stats {
        // The number of inserts into the cache.
        uint64_t inserts = 0;
        // The number of cache lookups.
        uint64_t lookups = 0;
        // The subset of lookups that missed.
        uint64_t misses = 0;
        // The subset of lookups that hit but the looked up querier had to be
        // dropped due to position mismatch.
        uint64_t drops = 0;
        // The number of queriers evicted due to their TTL expiring.
        uint64_t time_based_evictions = 0;
        // The number of queriers evicted to free up resources to be able to
        // create new readers.
        uint64_t resource_based_evictions = 0;
        // The number of queriers currently in the cache.
        uint64_t population = 0;
        // The number of queries dropped due to scheduling group mismatch
        // between semaphores
        uint64_t scheduling_group_mismatches = 0;
    };

    using index = std::unordered_multimap<query_id, std::unique_ptr<querier_base>>;
    using is_user_semaphore_func = std::function<bool(const reader_concurrency_semaphore&)>;

private:
    index _data_querier_index;
    index _mutation_querier_index;
    index _shard_mutation_querier_index;
    std::chrono::seconds _entry_ttl;
    stats _stats;
    gate _closing_gate;
    is_user_semaphore_func _is_user_semaphore_func;

private:
    template <typename Querier>
    void insert_querier(
            query_id key,
            querier_cache::index& index,
            querier_cache::stats& stats,
            Querier&& q,
            std::chrono::seconds ttl,
            tracing::trace_state_ptr trace_state);

    template <typename Querier>
    std::optional<Querier> lookup_querier(
        querier_cache::index& index,
        query_id key,
        const schema& s,
        dht::partition_ranges_view ranges,
        const query::partition_slice& slice,
        reader_concurrency_semaphore& current_sem,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout);

public:
    querier_cache(is_user_semaphore_func is_user_semaphore_func, std::chrono::seconds entry_ttl = default_entry_ttl);

    querier_cache(const querier_cache&) = delete;
    querier_cache& operator=(const querier_cache&) = delete;

    // this is captured
    querier_cache(querier_cache&&) = delete;
    querier_cache& operator=(querier_cache&&) = delete;

    void insert_data_querier(query_id key, querier&& q, tracing::trace_state_ptr trace_state);

    void insert_mutation_querier(query_id key, querier&& q, tracing::trace_state_ptr trace_state);

    void insert_shard_querier(query_id key, shard_mutation_querier&& q, tracing::trace_state_ptr trace_state);

    /// Lookup a data querier in the cache.
    ///
    /// Queriers are found based on `key` and `range`. There may be multiple
    /// queriers for the same `key` differentiated by their read range. Since
    /// each subsequent page may have a narrower read range then the one before
    /// it ranges cannot be simply matched based on equality. For matching we
    /// use the fact that the coordinator splits the query range into
    /// non-overlapping ranges. Thus both bounds of any range, or in case of
    /// singular ranges only the start bound are guaranteed to be unique.
    ///
    /// The found querier is checked for a matching position and schema version.
    /// The start position of the querier is checked against the start position
    /// of the page using the `range' and `slice'.
    std::optional<querier> lookup_data_querier(query_id key,
            const schema& s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            reader_concurrency_semaphore& current_sem,
            tracing::trace_state_ptr trace_state,
            db::timeout_clock::time_point timeout);

    /// Lookup a mutation querier in the cache.
    ///
    /// See \ref lookup_data_querier().
    std::optional<querier> lookup_mutation_querier(query_id key,
            const schema& s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            reader_concurrency_semaphore& current_sem,
            tracing::trace_state_ptr trace_state,
            db::timeout_clock::time_point timeout);

    /// Lookup a shard mutation querier in the cache.
    ///
    /// See \ref lookup_data_querier().
    std::optional<shard_mutation_querier> lookup_shard_mutation_querier(query_id key,
            const schema& s,
            const dht::partition_range_vector& ranges,
            const query::partition_slice& slice,
            reader_concurrency_semaphore& current_sem,
            tracing::trace_state_ptr trace_state,
            db::timeout_clock::time_point timeout);

    /// Change the ttl of cache entries
    ///
    /// Applies only to entries inserted after the change.
    void set_entry_ttl(std::chrono::seconds entry_ttl);

    /// Evict a querier.
    ///
    /// Return true if a querier was evicted and false otherwise (if the cache
    /// is empty).
    future<bool> evict_one() noexcept;

    /// Close all queriers and wait on background work.
    ///
    /// Should be used before destroying the querier_cache.
    future<> stop() noexcept;

    const stats& get_stats() const {
        return _stats;
    }
};

} // namespace query
