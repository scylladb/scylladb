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

#include "querier.hh"

#include "schema.hh"

#include <boost/range/adaptor/map.hpp>

namespace query {

enum class can_use {
    yes,
    no_schema_version_mismatch,
    no_ring_pos_mismatch,
    no_clustering_pos_mismatch
};

static sstring cannot_use_reason(can_use cu)
{
    switch (cu)
    {
        case can_use::yes:
            return "can be used";
        case can_use::no_schema_version_mismatch:
            return "schema version mismatch";
        case can_use::no_ring_pos_mismatch:
            return "ring pos mismatch";
        case can_use::no_clustering_pos_mismatch:
            return "clustering pos mismatch";
    }
    return "unknown reason";
}

static bool ring_position_matches(const schema& s, const dht::partition_range& range, const query::partition_slice& slice,
        const position_view& pos) {
    const auto is_reversed = flat_mutation_reader::consume_reversed_partitions(slice.options.contains(query::partition_slice::option::reversed));

    const auto expected_start = dht::ring_position_view(*pos.partition_key);
    // If there are no clustering columns or the select is distinct we don't
    // have clustering rows at all. In this case we can be sure we won't have
    // anything more in the last page's partition and thus the start bound is
    // exclusive. Otherwise there migh be clustering rows still and it is
    // inclusive.
    const auto expected_inclusiveness = s.clustering_key_size() > 0 &&
        !slice.options.contains<query::partition_slice::option::distinct>() &&
        pos.clustering_key;
    const auto comparator = dht::ring_position_comparator(s);

    if (is_reversed && !range.is_singular()) {
        const auto& end = range.end();
        return end && comparator(end->value(), expected_start) == 0 && end->is_inclusive() == expected_inclusiveness;
    }

    const auto& start = range.start();
    return start && comparator(start->value(), expected_start) == 0 && start->is_inclusive() == expected_inclusiveness;
}

static bool clustering_position_matches(const schema& s, const query::partition_slice& slice, const position_view& pos) {
    const auto& row_ranges = slice.row_ranges(s, pos.partition_key->key());

    if (row_ranges.empty()) {
        // This is a valid slice on the last page of a query with
        // clustering restrictions. It simply means the query is
        // effectively over, no further results are expected. We
        // can assume the clustering position matches.
        return true;
    }

    if (!pos.clustering_key) {
        // We stopped at a non-clustering position so the partition's clustering
        // row ranges should be the default row ranges.
        return &row_ranges == &slice.default_row_ranges();
    }

    clustering_key_prefix::equality eq(s);

    const auto is_reversed = flat_mutation_reader::consume_reversed_partitions(slice.options.contains(query::partition_slice::option::reversed));

    // If the page ended mid-partition the first partition range should start
    // with the last clustering key (exclusive).
    const auto& first_row_range = row_ranges.front();
    const auto& start = is_reversed ? first_row_range.end() : first_row_range.start();
    if (!start) {
        return false;
    }
    return !start->is_inclusive() && eq(start->value(), *pos.clustering_key);
}

static bool ranges_match(const schema& s, const dht::partition_range& original_range, const dht::partition_range& new_range) {
    if (original_range.is_singular() != new_range.is_singular()) {
        return false;
    }

    const auto cmp = dht::ring_position_comparator(s);
    const auto bound_eq = [&] (const stdx::optional<dht::partition_range::bound>& a, const stdx::optional<dht::partition_range::bound>& b) {
        return bool(a) == bool(b) && (!a || a->equal(*b, cmp));
    };

    // For singular ranges end() == start() so they are interchangeable.
    // For non-singular ranges we check only the end().
    return bound_eq(original_range.end(), new_range.end());
}

static bool ranges_match(const schema& s, dht::partition_ranges_view original_ranges, dht::partition_ranges_view new_ranges) {
    if (new_ranges.empty()) {
        return false;
    }
    if (original_ranges.size() == 1) {
        if (new_ranges.size() != 1) {
            return false;
        }
        return ranges_match(s, original_ranges.front(), new_ranges.front());
    }

    // As the query progresses the number of to-be-read ranges can never surpass
    // that of the original ranges.
    if (original_ranges.size() < new_ranges.size()) {
        return false;
    }

    // If there is a difference in the size of the range lists we assume we
    // already read ranges from the original list and these ranges are missing
    // from the head of the new list.
    auto new_ranges_it = new_ranges.begin();
    auto original_ranges_it = original_ranges.begin() + (original_ranges.size() - new_ranges.size());

    // The first range in the new list can be partially read so we only check
    // that one of its bounds match that of its original counterpart, just like
    // we do with single ranges.
    if (!ranges_match(s, *original_ranges_it++, *new_ranges_it++)) {
        return false;
    }

    const auto cmp = dht::ring_position_comparator(s);

    // The rest of the list, those ranges that we didn't even started reading
    // yet should be *identical* to their original counterparts.
    return std::equal(original_ranges_it, original_ranges.end(), new_ranges_it,
            [&cmp] (const dht::partition_range& a, const dht::partition_range& b) { return a.equal(b, cmp); });
}

template <typename Querier>
static can_use can_be_used_for_page(const Querier& q, const schema& s, const dht::partition_range& range, const query::partition_slice& slice) {
    if (s.version() != q.schema()->version()) {
        return can_use::no_schema_version_mismatch;
    }

    const auto pos = q.current_position();

    if (!pos.partition_key) {
        // There was nothing read so far so we assume we are ok.
        return can_use::yes;
    }

    if (!ring_position_matches(s, range, slice, pos)) {
        return can_use::no_ring_pos_mismatch;
    }
    if (!clustering_position_matches(s, slice, pos)) {
        return can_use::no_clustering_pos_mismatch;
    }
    return can_use::yes;
}

// The time-to-live of a cache-entry.
const std::chrono::seconds querier_cache::default_entry_ttl{10};

void querier_cache::scan_cache_entries() {
    const auto now = lowres_clock::now();

    auto it = _entries.begin();
    const auto end = _entries.end();
    while (it != end && it->is_expired(now)) {
        ++_stats.time_based_evictions;
        --_stats.population;
        _sem.unregister_inactive_read(it->get_inactive_handle());
        it = _entries.erase(it);
    }
}

static querier_cache::entries::iterator find_querier(querier_cache::entries& entries, querier_cache::index& index, utils::UUID key,
        dht::partition_ranges_view ranges, tracing::trace_state_ptr trace_state) {
    const auto queriers = index.equal_range(key);

    if (queriers.first == index.end()) {
        tracing::trace(trace_state, "Found no cached querier for key {}", key);
        return entries.end();
    }

    const auto it = std::find_if(queriers.first, queriers.second, [&] (const querier_cache::entry& e) {
        return ranges_match(e.schema(), e.ranges(), ranges);
    });

    if (it == queriers.second) {
        tracing::trace(trace_state, "Found cached querier(s) for key {} but none matches the query range(s) {}", key, ranges);
        return entries.end();
    }
    tracing::trace(trace_state, "Found cached querier for key {} and range(s) {}", key, ranges);
    return it->pos();
}

querier_cache::querier_cache(reader_concurrency_semaphore& sem, size_t max_cache_size, std::chrono::seconds entry_ttl)
    : _sem(sem)
    , _expiry_timer([this] { scan_cache_entries(); })
    , _entry_ttl(entry_ttl)
    , _max_queriers_memory_usage(max_cache_size) {
    _expiry_timer.arm_periodic(entry_ttl / 2);
}

class querier_inactive_read : public reader_concurrency_semaphore::inactive_read {
    querier_cache::entries& _entries;
    querier_cache::entries::iterator _pos;
    querier_cache::stats& _stats;

public:
    querier_inactive_read(querier_cache::entries& entries, querier_cache::entries::iterator pos, querier_cache::stats& stats)
        : _entries(entries)
        , _pos(pos)
        , _stats(stats) {
    }
    virtual void evict() override {
        _entries.erase(_pos);
        ++_stats.resource_based_evictions;
        --_stats.population;
    }
};

template <typename Querier>
static void insert_querier(
        reader_concurrency_semaphore& sem,
        querier_cache::entries& entries,
        querier_cache::index& index,
        querier_cache::stats& stats,
        size_t max_queriers_memory_usage,
        utils::UUID key,
        Querier&& q,
        lowres_clock::time_point expires,
        tracing::trace_state_ptr trace_state) {
    // FIXME: see #3159
    // In reverse mode flat_mutation_reader drops any remaining rows of the
    // current partition when the page ends so it cannot be reused across
    // pages.
    if (q.is_reversed()) {
        return;
    }

    tracing::trace(trace_state, "Caching querier with key {}", key);

    auto memory_usage = boost::accumulate(entries | boost::adaptors::transformed(std::mem_fn(&querier_cache::entry::memory_usage)), size_t(0));

    // We add the memory-usage of the to-be added querier to the memory-usage
    // of all the cached queriers. We now need to makes sure this number is
    // smaller then the maximum allowed memory usage. If it isn't we evict
    // cached queriers and substract their memory usage from this number until
    // it goes below the limit.
    memory_usage += q.memory_usage();

    if (memory_usage >= max_queriers_memory_usage) {
        while (!entries.empty() && memory_usage >= max_queriers_memory_usage) {
            auto it = entries.begin();
            memory_usage -= it->memory_usage();
            auto ir = sem.unregister_inactive_read(it->get_inactive_handle());
            ir->evict();
            // querier_inactive_read::evict() updates resource_based_evictions,
            // (and population) while we want memory_based_evictions:
            --stats.resource_based_evictions;
            ++stats.memory_based_evictions;
        }
    }

    auto& e = entries.emplace_back(key, std::move(q), expires);
    e.set_pos(--entries.end());

    if (auto irh = sem.register_inactive_read(std::make_unique<querier_inactive_read>(entries, e.pos(), stats))) {
        e.set_inactive_handle(irh);
        index.insert(e);
        ++stats.population;
    }
}

void querier_cache::insert(utils::UUID key, data_querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(_sem, _entries, _data_querier_index, _stats, _max_queriers_memory_usage, key, std::move(q), lowres_clock::now() + _entry_ttl,
            std::move(trace_state));
}

void querier_cache::insert(utils::UUID key, mutation_querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(_sem, _entries, _mutation_querier_index, _stats, _max_queriers_memory_usage, key, std::move(q), lowres_clock::now() + _entry_ttl,
            std::move(trace_state));
}

void querier_cache::insert(utils::UUID key, shard_mutation_querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(_sem, _entries, _shard_mutation_querier_index, _stats, _max_queriers_memory_usage, key, std::move(q), lowres_clock::now() + _entry_ttl,
            std::move(trace_state));
}

template <typename Querier>
static std::optional<Querier> lookup_querier(
        reader_concurrency_semaphore& sem,
        querier_cache::entries& entries,
        querier_cache::index& index,
        querier_cache::stats& stats,
        utils::UUID key,
        const schema& s,
        dht::partition_ranges_view ranges,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    auto it = find_querier(entries, index, key, ranges, trace_state);
    ++stats.lookups;
    if (it == entries.end()) {
        ++stats.misses;
        return std::nullopt;
    }

    auto q = std::move(*it).template value<Querier>();
    sem.unregister_inactive_read(it->get_inactive_handle());
    entries.erase(it);
    --stats.population;

    const auto can_be_used = can_be_used_for_page(q, s, ranges.front(), slice);
    if (can_be_used == can_use::yes) {
        tracing::trace(trace_state, "Reusing querier");
        return std::optional<Querier>(std::move(q));
    }

    tracing::trace(trace_state, "Dropping querier because {}", cannot_use_reason(can_be_used));
    ++stats.drops;
    return std::nullopt;
}

std::optional<data_querier> querier_cache::lookup_data_querier(utils::UUID key,
        const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    return lookup_querier<data_querier>(_sem, _entries, _data_querier_index, _stats, key, s, range, slice, std::move(trace_state));
}

std::optional<mutation_querier> querier_cache::lookup_mutation_querier(utils::UUID key,
        const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    return lookup_querier<mutation_querier>(_sem, _entries, _mutation_querier_index, _stats, key, s, range, slice, std::move(trace_state));
}

std::optional<shard_mutation_querier> querier_cache::lookup_shard_mutation_querier(utils::UUID key,
        const schema& s,
        const dht::partition_range_vector& ranges,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    return lookup_querier<shard_mutation_querier>(_sem, _entries, _shard_mutation_querier_index, _stats, key, s, ranges, slice,
            std::move(trace_state));
}

void querier_cache::set_entry_ttl(std::chrono::seconds entry_ttl) {
    _entry_ttl = entry_ttl;
    _expiry_timer.rearm(lowres_clock::now() + _entry_ttl / 2, _entry_ttl / 2);
}

bool querier_cache::evict_one() {
    if (_entries.empty()) {
        return false;
    }

    ++_stats.resource_based_evictions;
    --_stats.population;
    _sem.unregister_inactive_read(_entries.front().get_inactive_handle());
    _entries.pop_front();

    return true;
}

void querier_cache::evict_all_for_table(const utils::UUID& schema_id) {
    auto it = _entries.begin();
    const auto end = _entries.end();
    while (it != end) {
        if (it->schema().id() == schema_id) {
            --_stats.population;
            _sem.unregister_inactive_read(it->get_inactive_handle());
            it = _entries.erase(it);
        } else {
            ++it;
        }
    }
}

querier_cache_context::querier_cache_context(querier_cache& cache, utils::UUID key, bool is_first_page)
    : _cache(&cache)
    , _key(key)
    , _is_first_page(is_first_page) {
}

void querier_cache_context::insert(data_querier&& q, tracing::trace_state_ptr trace_state) {
    if (_cache && _key != utils::UUID{}) {
        _cache->insert(_key, std::move(q), std::move(trace_state));
    }
}

void querier_cache_context::insert(mutation_querier&& q, tracing::trace_state_ptr trace_state) {
    if (_cache && _key != utils::UUID{}) {
        _cache->insert(_key, std::move(q), std::move(trace_state));
    }
}

void querier_cache_context::insert(shard_mutation_querier&& q, tracing::trace_state_ptr trace_state) {
    if (_cache && _key != utils::UUID{}) {
        _cache->insert(_key, std::move(q), std::move(trace_state));
    }
}

std::optional<data_querier> querier_cache_context::lookup_data_querier(const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    if (_cache && _key != utils::UUID{} && !_is_first_page) {
        return _cache->lookup_data_querier(_key, s, range, slice, std::move(trace_state));
    }
    return std::nullopt;
}

std::optional<mutation_querier> querier_cache_context::lookup_mutation_querier(const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    if (_cache && _key != utils::UUID{} && !_is_first_page) {
        return _cache->lookup_mutation_querier(_key, s, range, slice, std::move(trace_state));
    }
    return std::nullopt;
}

std::optional<shard_mutation_querier> querier_cache_context::lookup_shard_mutation_querier(const schema& s,
        const dht::partition_range_vector& ranges,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    if (_cache && _key != utils::UUID{} && !_is_first_page) {
        return _cache->lookup_shard_mutation_querier(_key, s, ranges, slice, std::move(trace_state));
    }
    return std::nullopt;
}

} // namespace query
