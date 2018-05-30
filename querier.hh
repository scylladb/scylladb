/*
 * Copyright (C) 2018 ScyllaDB
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

#pragma once

#include "mutation_compactor.hh"
#include "mutation_reader.hh"

#include <boost/intrusive/set.hpp>

#include <variant>

/// One-stop object for serving queries.
///
/// Encapsulates all state and logic for serving all pages for a given range
/// of a query on a given shard. Can serve mutation or data queries. Can be
/// used with any CompactedMutationsConsumer certified result-builder.
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
/// (4) At the beginning of the next page use can_be_used_for_page() to
///     determine whether it can be used with the page's schema and start
///     position. If a schema or position mismatch is detected the querier
///     cannot be used to produce the next page and a new one has to be created
///     instead.
class querier {
public:
    enum class can_use {
        yes,
        no_emit_only_live_rows_mismatch,
        no_schema_version_mismatch,
        no_ring_pos_mismatch,
        no_clustering_pos_mismatch
    };
private:
    template <typename Consumer>
    class clustering_position_tracker {
        std::unique_ptr<Consumer> _consumer;
        lw_shared_ptr<std::optional<clustering_key_prefix>> _last_ckey;

    public:
        clustering_position_tracker(std::unique_ptr<Consumer>&& consumer, lw_shared_ptr<std::optional<clustering_key_prefix>> last_ckey)
            : _consumer(std::move(consumer))
            , _last_ckey(std::move(last_ckey)) {
        }

        void consume_new_partition(const dht::decorated_key& dk) {
            _last_ckey->reset();
            _consumer->consume_new_partition(dk);
        }
        void consume(tombstone t) {
            _consumer->consume(t);
        }
        stop_iteration consume(static_row&& sr, tombstone t, bool is_live) {
            return _consumer->consume(std::move(sr), std::move(t), is_live);
        }
        stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_live) {
            *_last_ckey = cr.key();
            return _consumer->consume(std::move(cr), std::move(t), is_live);
        }
        stop_iteration consume(range_tombstone&& rt) {
            return _consumer->consume(std::move(rt));
        }
        stop_iteration consume_end_of_partition() {
            return _consumer->consume_end_of_partition();
        }
        auto consume_end_of_stream() {
            return _consumer->consume_end_of_stream();
        }
    };

    struct position {
        const dht::decorated_key* partition_key;
        const clustering_key_prefix* clustering_key;
    };

    schema_ptr _schema;
    std::unique_ptr<dht::partition_range> _range;
    std::unique_ptr<query::partition_slice> _slice;
    flat_mutation_reader _reader;
    std::variant<lw_shared_ptr<compact_for_mutation_query_state>, lw_shared_ptr<compact_for_data_query_state>> _compaction_state;
    lw_shared_ptr<std::optional<clustering_key_prefix>> _last_ckey;

    std::variant<lw_shared_ptr<compact_for_mutation_query_state>, lw_shared_ptr<compact_for_data_query_state>> make_compaction_state(
            const schema& s,
            uint32_t row_limit,
            uint32_t partition_limit,
            gc_clock::time_point query_time,
            emit_only_live_rows only_live) const {
        if (only_live == emit_only_live_rows::yes) {
            return make_lw_shared<compact_for_query_state<emit_only_live_rows::yes>>(s, query_time, *_slice, row_limit, partition_limit);
        } else {
            return make_lw_shared<compact_for_query_state<emit_only_live_rows::no>>(s, query_time, *_slice, row_limit, partition_limit);
        }
    }

    position current_position() const;

    bool ring_position_matches(const dht::partition_range& range, const position& pos) const;

    bool clustering_position_matches(const query::partition_slice& slice, const position& pos) const;

public:
    querier(const mutation_source& ms,
            schema_ptr schema,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_ptr,
            emit_only_live_rows only_live)
        : _schema(schema)
        , _range(std::make_unique<dht::partition_range>(range))
        , _slice(std::make_unique<query::partition_slice>(slice))
        , _reader(ms.make_reader(schema, *_range, *_slice, pc, std::move(trace_ptr),
                    streamed_mutation::forwarding::no, mutation_reader::forwarding::no))
        , _compaction_state(make_compaction_state(*schema, 0, 0, gc_clock::time_point{}, only_live))
        , _last_ckey(make_lw_shared<std::optional<clustering_key_prefix>>()) {
    }

    bool is_reversed() const {
        return _slice->options.contains(query::partition_slice::option::reversed);
    }

    bool are_limits_reached() const {
        return std::visit([] (const auto& cs) { return cs->are_limits_reached(); }, _compaction_state);
    }

    /// Does the querier's range matches `range`?
    ///
    /// A query can have more then one querier executing parallelly for
    /// different sub-ranges on the same shard. This method helps identifying
    /// the appropriate one for the `range'.
    /// Since ranges can be narrowed from page-to-page (as the query moves
    /// through it) we cannot just check the two ranges for equality.
    /// Instead we exploit the fact the a query-range will always be split into
    /// non-overlapping sub ranges and thus each bound of a range is unique.
    /// Thus if any of the range's bounds are equal we have a match.
    /// For singular ranges we just check the one bound.
    bool matches(const dht::partition_range& range) const;

    /// Can the querier be used for the next page?
    ///
    /// The querier can only be used for the next page if the only_live, the
    /// schema versions, the ring and the clustering positions match.
    can_use can_be_used_for_page(emit_only_live_rows only_live, const schema& s,
            const dht::partition_range& range, const query::partition_slice& slice) const;

    template <typename Consumer>
    GCC6_CONCEPT(
        requires CompactedFragmentsConsumer<Consumer>
    )
    auto consume_page(Consumer&& consumer,
            uint32_t row_limit,
            uint32_t partition_limit,
            gc_clock::time_point query_time,
            db::timeout_clock::time_point timeout) {
        return std::visit([=, consumer = std::move(consumer)] (auto& compaction_state) mutable {
            // FIXME: #3158
            // consumer cannot be moved after consume_new_partition() is called
            // on it because it stores references to some of it's own members.
            // Move it to the heap before any consumption begins to avoid
            // accidents.
            return _reader.peek().then([=, consumer = std::make_unique<Consumer>(std::move(consumer))] (mutation_fragment* next_fragment) mutable {
                const auto next_fragment_kind = next_fragment ? next_fragment->mutation_fragment_kind() : mutation_fragment::kind::partition_end;
                compaction_state->start_new_page(row_limit, partition_limit, query_time, next_fragment_kind, *consumer);

                const auto is_reversed = flat_mutation_reader::consume_reversed_partitions(
                        _slice->options.contains(query::partition_slice::option::reversed));

                using compaction_state_type = typename std::remove_reference<decltype(*compaction_state)>::type;
                constexpr auto only_live = compaction_state_type::parameters::only_live;
                auto reader_consumer = make_stable_flattened_mutations_consumer<compact_for_query<only_live, clustering_position_tracker<Consumer>>>(
                        compaction_state,
                        clustering_position_tracker(std::move(consumer), _last_ckey));

                return _reader.consume(std::move(reader_consumer), is_reversed, timeout);
            });
        }, _compaction_state);
    }

    size_t memory_usage() const {
        return _reader.buffer_size();
    }

    schema_ptr schema() const {
        return _schema;
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
///     objects executing on the same node and shard paralelly. To identify the
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
/// Provides a way to evict readers one-by-one via `evict_one()`. This can be
/// used by the concurrency-limiting code to evict cached readers to free up
/// resources for admitting new ones.
/// Keeps the total memory consumption of cached queriers
/// below max_queriers_memory_usage by evicting older entries upon inserting
/// new ones if the the memory consupmtion would go above the limit.
class querier_cache {
public:
    static const std::chrono::seconds default_entry_ttl;
    static const size_t max_queriers_memory_usage;

    struct stats {
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
        // The number of queriers evicted to because the maximum memory usage
        // was reached.
        uint64_t memory_based_evictions = 0;
        // The number of queriers currently in the cache.
        uint64_t population = 0;
    };

private:
    class entry : public boost::intrusive::set_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
        // Self reference so that we can remove the entry given an `entry&`.
        std::list<entry>::iterator _pos;
        const utils::UUID _key;
        const lowres_clock::time_point _expires;
        querier _value;

    public:
        entry(utils::UUID key, querier q, lowres_clock::time_point expires)
            : _key(key)
            , _expires(expires)
            , _value(std::move(q)) {
        }

        std::list<entry>::iterator pos() const {
            return _pos;
        }

        void set_pos(std::list<entry>::iterator pos) {
            _pos = pos;
        }

        const utils::UUID& key() const {
            return _key;
        }

        const ::schema& schema() const {
            return *_value.schema();
        }

        bool is_expired(const lowres_clock::time_point& now) const {
            return _expires <= now;
        }

        size_t memory_usage() const {
            return _value.memory_usage();
        }

        const querier& value() const & {
            return _value;
        }

        querier value() && {
            return std::move(_value);
        }
    };

    struct key_of_entry {
        using type = utils::UUID;
        const type& operator()(const entry& e) { return e.key(); }
    };

    using entries = std::list<entry>;
    using index = boost::intrusive::multiset<entry, boost::intrusive::key_of_value<key_of_entry>,
          boost::intrusive::constant_time_size<false>>;

private:
    entries _entries;
    index _index;
    timer<lowres_clock> _expiry_timer;
    std::chrono::seconds _entry_ttl;
    stats _stats;

    entries::iterator find_querier(utils::UUID key, const dht::partition_range& range, tracing::trace_state_ptr trace_state);

    void scan_cache_entries();

public:
    querier_cache(std::chrono::seconds entry_ttl = default_entry_ttl);

    querier_cache(const querier_cache&) = delete;
    querier_cache& operator=(const querier_cache&) = delete;

    // this is captured
    querier_cache(querier_cache&&) = delete;
    querier_cache& operator=(querier_cache&&) = delete;

    void insert(utils::UUID key, querier&& q, tracing::trace_state_ptr trace_state);

    /// Lookup a querier in the cache.
    ///
    /// If the querier doesn't exist, use `create_fun' to create it.
    ///
    /// Queriers are found based on `key` and `range`. There may be multiple
    /// queriers for the same `key` differentiated by their read range. Since
    /// each subsequent page may have a narrower read range then the one before
    /// it ranges cannot be simply matched based on equality. For matching we
    /// use the fact that the coordinator splits the query range into
    /// non-overlapping ranges. Thus both bounds of any range, or in case of
    /// singular ranges only the start bound are guaranteed to be unique.
    ///
    /// The found querier is checked for a matching read-kind and schema
    /// version. The start position is also checked against the current
    /// position of the querier using the `range' and `slice'. If there is a
    /// mismatch drop the querier and create a new one with `create_fun'.
    querier lookup(utils::UUID key,
            emit_only_live_rows only_live,
            const schema& s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            const noncopyable_function<querier()>& create_fun);

    void set_entry_ttl(std::chrono::seconds entry_ttl);

    /// Evict a querier.
    ///
    /// Return true if a querier was evicted and false otherwise (if the cache
    /// is empty).
    bool evict_one();

    /// Evict all queriers that belong to a table.
    ///
    /// Should be used when dropping a table.
    void evict_all_for_table(const utils::UUID& schema_id);

    const stats& get_stats() const {
        return _stats;
    }
};

class querier_cache_context {
    querier_cache* _cache{};
    utils::UUID _key;
    bool _is_first_page;

public:
    querier_cache_context() = default;
    querier_cache_context(querier_cache& cache, utils::UUID key, bool is_first_page);
    void insert(querier&& q, tracing::trace_state_ptr trace_state);
    querier lookup(emit_only_live_rows only_live,
            const schema& s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            const noncopyable_function<querier()>& create_fun);
};
