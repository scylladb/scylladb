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

static sstring cannot_use_reason(querier::can_use cu)
{
    switch (cu)
    {
        case querier::can_use::yes:
            return "can be used";
        case querier::can_use::no_emit_only_live_rows_mismatch:
            return "emit only live rows mismatch";
        case querier::can_use::no_schema_version_mismatch:
            return "schema version mismatch";
        case querier::can_use::no_ring_pos_mismatch:
            return "ring pos mismatch";
        case querier::can_use::no_clustering_pos_mismatch:
            return "clustering pos mismatch";
    }
    return "unknown reason";
}

querier::position querier::current_position() const {
    const dht::decorated_key* dk = std::visit([] (const auto& cs) { return cs->current_partition(); }, _compaction_state);
    const clustering_key_prefix* clustering_key = *_last_ckey ? &**_last_ckey : nullptr;
    return {dk, clustering_key};
}

bool querier::ring_position_matches(const dht::partition_range& range, const querier::position& pos) const {
    const auto is_reversed = flat_mutation_reader::consume_reversed_partitions(_slice->options.contains(query::partition_slice::option::reversed));

    const auto expected_start = dht::ring_position_view(*pos.partition_key);
    // If there are no clustering columns or the select is distinct we don't
    // have clustering rows at all. In this case we can be sure we won't have
    // anything more in the last page's partition and thus the start bound is
    // exclusive. Otherwise there migh be clustering rows still and it is
    // inclusive.
    const auto expected_inclusiveness = _schema->clustering_key_size() > 0 &&
        !_slice->options.contains<query::partition_slice::option::distinct>() &&
        pos.clustering_key;
    const auto comparator = dht::ring_position_comparator(*_schema);

    if (is_reversed && !range.is_singular()) {
        const auto& end = range.end();
        return end && comparator(end->value(), expected_start) == 0 && end->is_inclusive() == expected_inclusiveness;
    }

    const auto& start = range.start();
    return start && comparator(start->value(), expected_start) == 0 && start->is_inclusive() == expected_inclusiveness;
}

bool querier::clustering_position_matches(const query::partition_slice& slice, const querier::position& pos) const {
    const auto& row_ranges = slice.row_ranges(*_schema, pos.partition_key->key());

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

    clustering_key_prefix::equality eq(*_schema);

    const auto is_reversed = flat_mutation_reader::consume_reversed_partitions(_slice->options.contains(query::partition_slice::option::reversed));

    // If the page ended mid-partition the first partition range should start
    // with the last clustering key (exclusive).
    const auto& first_row_range = row_ranges.front();
    const auto& start = is_reversed ? first_row_range.end() : first_row_range.start();
    if (!start) {
        return false;
    }
    return !start->is_inclusive() && eq(start->value(), *pos.clustering_key);
}

bool querier::matches(const dht::partition_range& range) const {
    const auto& qr = *_range;
    if (qr.is_singular() != range.is_singular()) {
        return false;
    }

    const auto cmp = dht::ring_position_comparator(*_schema);
    const auto bound_eq = [&] (const stdx::optional<dht::partition_range::bound>& a, const stdx::optional<dht::partition_range::bound>& b) {
        return bool(a) == bool(b) && (!a || a->equal(*b, cmp));
    };

    return qr.is_singular() ?
        bound_eq(qr.start(), range.start()) :
        bound_eq(qr.start(), range.start()) || bound_eq(qr.end(), range.end());
}

querier::can_use querier::can_be_used_for_page(emit_only_live_rows only_live, const ::schema& s,
        const dht::partition_range& range, const query::partition_slice& slice) const {
    if (only_live != emit_only_live_rows(std::holds_alternative<lw_shared_ptr<compact_for_data_query_state>>(_compaction_state))) {
        return can_use::no_emit_only_live_rows_mismatch;
    }
    if (s.version() != _schema->version()) {
        return can_use::no_schema_version_mismatch;
    }

    const auto pos = current_position();

    if (!pos.partition_key) {
        // There was nothing read so far so we assume we are ok.
        return can_use::yes;
    }

    if (!ring_position_matches(range, pos)) {
        return can_use::no_ring_pos_mismatch;
    }
    if (!clustering_position_matches(slice, pos)) {
        return can_use::no_clustering_pos_mismatch;
    }
    return can_use::yes;
}

// The time-to-live of a cache-entry.
const std::chrono::seconds querier_cache::default_entry_ttl{10};

const size_t querier_cache::max_queriers_memory_usage = memory::stats().total_memory() * 0.04;

void querier_cache::scan_cache_entries() {
    const auto now = lowres_clock::now();

    auto it = _entries.begin();
    const auto end = _entries.end();
    while (it != end && it->is_expired(now)) {
        ++_stats.time_based_evictions;
        --_stats.population;
        it = _entries.erase(it);
    }
}

querier_cache::entries::iterator querier_cache::find_querier(utils::UUID key, const dht::partition_range& range, tracing::trace_state_ptr trace_state) {
    const auto queriers = _index.equal_range(key);

    if (queriers.first == _index.end()) {
        tracing::trace(trace_state, "Found no cached querier for key {}", key);
        return _entries.end();
    }

    const auto it = std::find_if(queriers.first, queriers.second, [&] (const entry& e) {
        return e.value().matches(range);
    });

    if (it == queriers.second) {
        tracing::trace(trace_state, "Found cached querier(s) for key {} but none matches the query range {}", key, range);
    }
    tracing::trace(trace_state, "Found cached querier for key {} and range {}", key, range);
    return it->pos();
}

querier_cache::querier_cache(std::chrono::seconds entry_ttl)
    : _expiry_timer([this] { scan_cache_entries(); })
    , _entry_ttl(entry_ttl) {
    _expiry_timer.arm_periodic(entry_ttl / 2);
}

void querier_cache::insert(utils::UUID key, querier&& q, tracing::trace_state_ptr trace_state) {
    // FIXME: see #3159
    // In reverse mode flat_mutation_reader drops any remaining rows of the
    // current partition when the page ends so it cannot be reused across
    // pages.
    if (q.is_reversed()) {
        return;
    }

    tracing::trace(trace_state, "Caching querier with key {}", key);

    auto memory_usage = boost::accumulate(_entries | boost::adaptors::transformed(std::mem_fn(&entry::memory_usage)), size_t(0));

    // We add the memory-usage of the to-be added querier to the memory-usage
    // of all the cached queriers. We now need to makes sure this number is
    // smaller then the maximum allowed memory usage. If it isn't we evict
    // cached queriers and substract their memory usage from this number until
    // it goes below the limit.
    memory_usage += q.memory_usage();

    if (memory_usage >= max_queriers_memory_usage) {
        auto it = _entries.begin();
        const auto end = _entries.end();
        while (it != end && memory_usage >= max_queriers_memory_usage) {
            ++_stats.memory_based_evictions;
            memory_usage -= it->memory_usage();
            --_stats.population;
            it = _entries.erase(it);
        }
    }

    auto& e = _entries.emplace_back(key, std::move(q), lowres_clock::now() + _entry_ttl);
    e.set_pos(--_entries.end());
    _index.insert(e);
    ++_stats.population;
}

querier querier_cache::lookup(utils::UUID key,
        emit_only_live_rows only_live,
        const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        const noncopyable_function<querier()>& create_fun) {
    auto it = find_querier(key, range, trace_state);
    ++_stats.lookups;
    if (it == _entries.end()) {
        ++_stats.misses;
        return create_fun();
    }

    auto q = std::move(*it).value();
    _entries.erase(it);
    --_stats.population;

    const auto can_be_used = q.can_be_used_for_page(only_live, s, range, slice);
    if (can_be_used == querier::can_use::yes) {
        tracing::trace(trace_state, "Reusing querier");
        return q;
    }

    tracing::trace(trace_state, "Dropping querier because {}", cannot_use_reason(can_be_used));
    ++_stats.drops;
    return create_fun();
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
    _entries.pop_front();

    return true;
}

void querier_cache::evict_all_for_table(const utils::UUID& schema_id) {
    auto it = _entries.begin();
    const auto end = _entries.end();
    while (it != end) {
        if (it->schema().id() == schema_id) {
            --_stats.population;
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

void querier_cache_context::insert(querier&& q, tracing::trace_state_ptr trace_state) {
    if (_cache && _key != utils::UUID{}) {
        _cache->insert(_key, std::move(q), std::move(trace_state));
    }
}

querier querier_cache_context::lookup(emit_only_live_rows only_live,
        const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        const noncopyable_function<querier()>& create_fun) {
    if (_cache && _key != utils::UUID{} && !_is_first_page) {
        return _cache->lookup(_key, only_live, s, range, slice, std::move(trace_state), create_fun);
    }
    return create_fun();
}
