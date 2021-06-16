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

#include <seastar/core/coroutine.hh>

#include "querier.hh"

#include "schema.hh"
#include "log.hh"

#include <boost/range/adaptor/map.hpp>

namespace query {

logging::logger qlogger("querier_cache");

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
    const auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);

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

    const auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);

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
    const auto bound_eq = [&] (const std::optional<dht::partition_range::bound>& a, const std::optional<dht::partition_range::bound>& b) {
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
    if (s.version() != q.schema().version()) {
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

static std::unique_ptr<querier_base> find_querier(querier_cache::index& index, utils::UUID key,
        dht::partition_ranges_view ranges, tracing::trace_state_ptr trace_state) {
    const auto queriers = index.equal_range(key);

    if (queriers.first == index.end()) {
        tracing::trace(trace_state, "Found no cached querier for key {}", key);
        return nullptr;
    }

    const auto it = std::find_if(queriers.first, queriers.second, [&] (const querier_cache::index::value_type& e) {
        return ranges_match(e.second->schema(), e.second->ranges(), ranges);
    });

    if (it == queriers.second) {
        tracing::trace(trace_state, "Found cached querier(s) for key {} but none matches the query range(s) {}", key, ranges);
        return nullptr;
    }
    tracing::trace(trace_state, "Found cached querier for key {} and range(s) {}", key, ranges);
    auto ptr = std::move(it->second);
    index.erase(it);
    return ptr;
}

querier_cache::querier_cache(std::chrono::seconds entry_ttl)
    : _entry_ttl(entry_ttl) {
}

struct querier_utils {
    static flat_mutation_reader get_reader(querier_base& q) noexcept {
        return std::move(std::get<flat_mutation_reader>(q._reader));
    }
    static reader_concurrency_semaphore::inactive_read_handle get_inactive_read_handle(querier_base& q) noexcept {
        return std::move(std::get<reader_concurrency_semaphore::inactive_read_handle>(q._reader));
    }
    static void set_reader(querier_base& q, flat_mutation_reader r) noexcept {
        q._reader = std::move(r);
    }
    static void set_inactive_read_handle(querier_base& q, reader_concurrency_semaphore::inactive_read_handle h) noexcept {
        q._reader = std::move(h);
    }
};

template <typename Querier>
void querier_cache::insert_querier(
        utils::UUID key,
        querier_cache::index& index,
        querier_cache::stats& stats,
        Querier&& q,
        std::chrono::seconds ttl,
        tracing::trace_state_ptr trace_state) {
    // FIXME: see #3159
    // In reverse mode flat_mutation_reader drops any remaining rows of the
    // current partition when the page ends so it cannot be reused across
    // pages.
    if (q.is_reversed()) {
        (void)with_gate(_closing_gate, [this, q = std::move(q)] () mutable {
            return q.close().finally([q = std::move(q)] {});
        });
        return;
    }

    ++stats.inserts;

    tracing::trace(trace_state, "Caching querier with key {}", key);

    auto& sem = q.permit().semaphore();

    auto irh = sem.register_inactive_read(querier_utils::get_reader(q));
    if (!irh) {
        ++stats.resource_based_evictions;
        return;
    }
  try {
    auto cleanup_irh = defer([&] {
        sem.unregister_inactive_read(std::move(irh));
    });

    auto it = index.emplace(key, std::make_unique<Querier>(std::move(q)));

    ++stats.population;
    auto cleanup_index = defer([&] {
        index.erase(it);
        --stats.population;
    });

    auto notify_handler = [&stats, &index, it] (reader_concurrency_semaphore::evict_reason reason) {
        index.erase(it);
        switch (reason) {
            case reader_concurrency_semaphore::evict_reason::permit:
                ++stats.resource_based_evictions;
                break;
            case reader_concurrency_semaphore::evict_reason::time:
                ++stats.time_based_evictions;
                break;
            case reader_concurrency_semaphore::evict_reason::manual:
                break;
        }
        --stats.population;
    };

    sem.set_notify_handler(irh, std::move(notify_handler), ttl);
    querier_utils::set_inactive_read_handle(*it->second, std::move(irh));
    cleanup_index.cancel();
    cleanup_irh.cancel();
  } catch (...) {
    // It is okay to swallow the exception since
    // we're allowed to drop the reader upon registration
    // due to lack of resources - in which case we already
    // drop the querier.
    qlogger.warn("Failed to insert querier into index: {}. Ignored as if it was evicted upon registration", std::current_exception());
  }
}

void querier_cache::insert(utils::UUID key, data_querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(key, _data_querier_index, _stats, std::move(q), _entry_ttl, std::move(trace_state));
}

void querier_cache::insert(utils::UUID key, mutation_querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(key, _mutation_querier_index, _stats, std::move(q), _entry_ttl, std::move(trace_state));
}

void querier_cache::insert(utils::UUID key, shard_mutation_querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(key, _shard_mutation_querier_index, _stats, std::move(q), _entry_ttl, std::move(trace_state));
}

template <typename Querier>
std::optional<Querier> querier_cache::lookup_querier(
        querier_cache::index& index,
        utils::UUID key,
        const schema& s,
        dht::partition_ranges_view ranges,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    auto base_ptr = find_querier(index, key, ranges, trace_state);
    auto& stats = _stats;
    ++stats.lookups;
    if (!base_ptr) {
        ++stats.misses;
        return std::nullopt;
    }

    auto* q_ptr = dynamic_cast<Querier*>(base_ptr.get());
    if (!q_ptr) {
        throw std::runtime_error("lookup_querier(): found querier is not of the expected type");
    }
    auto& q = *q_ptr;
    auto reader_opt = q.permit().semaphore().unregister_inactive_read(querier_utils::get_inactive_read_handle(q));
    if (!reader_opt) {
        throw std::runtime_error("lookup_querier(): found querier that is evicted");
    }
    querier_utils::set_reader(q, std::move(*reader_opt));
    --stats.population;

    const auto can_be_used = can_be_used_for_page(q, s, ranges.front(), slice);
    if (can_be_used == can_use::yes) {
        tracing::trace(trace_state, "Reusing querier");
        return std::optional<Querier>(std::move(q));
    }

    tracing::trace(trace_state, "Dropping querier because {}", cannot_use_reason(can_be_used));
    ++stats.drops;

    // Close and drop the querier in the background.
    // It is safe to do so, since _closing_gate is closed and
    // waited on in querier_cache::stop()
    (void)with_gate(_closing_gate, [this, q = std::move(q)] () mutable {
        return q.close().finally([q = std::move(q)] {});
    });

    return std::nullopt;
}

std::optional<data_querier> querier_cache::lookup_data_querier(utils::UUID key,
        const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    return lookup_querier<data_querier>(_data_querier_index, key, s, range, slice, std::move(trace_state));
}

std::optional<mutation_querier> querier_cache::lookup_mutation_querier(utils::UUID key,
        const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    return lookup_querier<mutation_querier>(_mutation_querier_index, key, s, range, slice, std::move(trace_state));
}

std::optional<shard_mutation_querier> querier_cache::lookup_shard_mutation_querier(utils::UUID key,
        const schema& s,
        const dht::partition_range_vector& ranges,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state) {
    return lookup_querier<shard_mutation_querier>(_shard_mutation_querier_index, key, s, ranges, slice,
            std::move(trace_state));
}

future<> querier_base::close() noexcept {
    struct variant_closer {
        querier_base& q;
        future<> operator()(flat_mutation_reader& reader) {
            return reader.close();
        }
        future<> operator()(reader_concurrency_semaphore::inactive_read_handle& irh) {
            auto reader_opt = q.permit().semaphore().unregister_inactive_read(std::move(irh));
            return reader_opt ? reader_opt->close() : make_ready_future<>();
        }
    };
    return std::visit(variant_closer{*this}, _reader);
}

void querier_cache::set_entry_ttl(std::chrono::seconds entry_ttl) {
    _entry_ttl = entry_ttl;
}

future<bool> querier_cache::evict_one() noexcept {
    for (auto ip : {&_data_querier_index, &_mutation_querier_index, &_shard_mutation_querier_index}) {
        auto& idx = *ip;
        if (idx.empty()) {
            continue;
        }
        auto it = idx.begin();
        auto reader_opt = it->second->permit().semaphore().unregister_inactive_read(querier_utils::get_inactive_read_handle(*it->second));
        idx.erase(it);
        ++_stats.resource_based_evictions;
        --_stats.population;
        if (reader_opt) {
            co_await reader_opt->close();
        }
        co_return true;
    }
    co_return false;
}

future<> querier_cache::evict_all_for_table(const utils::UUID& schema_id) noexcept {
    for (auto ip : {&_data_querier_index, &_mutation_querier_index, &_shard_mutation_querier_index}) {
        auto& idx = *ip;
        for (auto it = idx.begin(); it != idx.end();) {
            if (it->second->schema().id() == schema_id) {
                auto reader_opt = it->second->permit().semaphore().unregister_inactive_read(querier_utils::get_inactive_read_handle(*it->second));
                it = idx.erase(it);
                --_stats.population;
                if (reader_opt) {
                    co_await reader_opt->close();
                }
            } else {
                ++it;
            }
        }
    }
    co_return;
}

future<> querier_cache::stop() noexcept {
    co_await _closing_gate.close();

    for (auto* ip : {&_data_querier_index, &_mutation_querier_index, &_shard_mutation_querier_index}) {
        auto& idx = *ip;
        for (auto it = idx.begin(); it != idx.end(); it = idx.erase(it)) {
            co_await it->second->close();
            --_stats.population;
        }
    }
}

querier_cache_context::querier_cache_context(querier_cache& cache, utils::UUID key, query::is_first_page is_first_page)
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
