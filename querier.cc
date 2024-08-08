/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "querier.hh"
#include "dht/i_partitioner.hh"
#include "reader_concurrency_semaphore.hh"
#include "schema/schema.hh"
#include "log.hh"
#include "utils/error_injection.hh"

#include <boost/range/adaptor/map.hpp>

namespace query {

logging::logger qlogger("querier_cache");
logging::logger qrlogger("querier");

enum class can_use {
    yes,
    no_schema_version_mismatch,
    no_ring_pos_mismatch,
    no_clustering_pos_mismatch,
    no_scheduling_group_mismatch,
    no_fatal_semaphore_mismatch
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
        case can_use::no_scheduling_group_mismatch:
            return "scheduling group mismatch";
        case can_use::no_fatal_semaphore_mismatch:
            return "fatal semaphore mismatch";
    }
    return "unknown reason";
}

static bool ring_position_matches(const schema& s, const dht::partition_range& range, const query::partition_slice& slice,
        full_position_view pos) {
    const auto is_reversed = slice.is_reversed();

    const auto expected_start = dht::ring_position(dht::decorate_key(s, pos.partition));
    // If there are no clustering columns or the select is distinct we don't
    // have clustering rows at all. In this case we can be sure we won't have
    // anything more in the last page's partition and thus the start bound is
    // exclusive. Otherwise there might be clustering rows still and it is
    // inclusive.
    const auto expected_inclusiveness = s.clustering_key_size() > 0 &&
        !slice.options.contains<query::partition_slice::option::distinct>() &&
        pos.position.region() == partition_region::clustered;
    const auto comparator = dht::ring_position_comparator(s);

    if (is_reversed && !range.is_singular()) {
        const auto& end = range.end();
        return end && comparator(end->value(), expected_start) == 0 && end->is_inclusive() == expected_inclusiveness;
    }

    const auto& start = range.start();
    return start && comparator(start->value(), expected_start) == 0 && start->is_inclusive() == expected_inclusiveness;
}

static bool clustering_position_matches(const schema& s, const query::partition_slice& slice, full_position_view pos) {
    const auto& row_ranges = slice.row_ranges(s, pos.partition);

    if (row_ranges.empty()) {
        // This is a valid slice on the last page of a query with
        // clustering restrictions. It simply means the query is
        // effectively over, no further results are expected. We
        // can assume the clustering position matches.
        return true;
    }

    if (pos.position.region() != partition_region::clustered) {
        // We stopped at a non-clustering position so the partition's clustering
        // row ranges should be the default row ranges.
        return &row_ranges == &slice.default_row_ranges();
    }

    clustering_key_prefix::equality eq(s);

    const auto is_reversed = slice.is_reversed();

    // If the page ended mid-partition the first partition range should start
    // with the last clustering key (exclusive).
    const auto& first_row_range = row_ranges.front();
    const auto& start = is_reversed ? first_row_range.end() : first_row_range.start();
    if (!start) {
        return false;
    }
    return !start->is_inclusive() && eq(start->value(), pos.position.key());
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
static can_use can_be_used_for_page(querier_cache::is_user_semaphore_func& is_user_semaphore, Querier& q, const schema& s, const dht::partition_range& range, const query::partition_slice& slice, reader_concurrency_semaphore& current_sem) {
    if (s.version() != q.schema().version()) {
        return can_use::no_schema_version_mismatch;
    }

    auto& querier_sem = q.permit().semaphore();
    if (&querier_sem != &current_sem) {
        if (is_user_semaphore(querier_sem) && is_user_semaphore(current_sem)) {
            return can_use::no_scheduling_group_mismatch;
        }
        return can_use::no_fatal_semaphore_mismatch;
    }

    const auto pos_opt = q.current_position();
    if (!pos_opt) {
        // There was nothing read so far so we assume we are ok.
        return can_use::yes;
    }

    if (!ring_position_matches(s, range, slice, *pos_opt)) {
        return can_use::no_ring_pos_mismatch;
    }
    if (!clustering_position_matches(s, slice, *pos_opt)) {
        return can_use::no_clustering_pos_mismatch;
    }
    return can_use::yes;
}

// The time-to-live of a cache-entry.
const std::chrono::seconds querier_cache::default_entry_ttl{10};

static std::unique_ptr<querier_base> find_querier(querier_cache::index& index, query_id key,
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

querier_cache::querier_cache(is_user_semaphore_func is_user_semaphore_func, std::chrono::seconds entry_ttl)
    : _entry_ttl(entry_ttl), _is_user_semaphore_func(is_user_semaphore_func) {
}

struct querier_utils {
    static mutation_reader get_reader(querier_base& q) noexcept {
        return std::move(std::get<mutation_reader>(q._reader));
    }
    static reader_concurrency_semaphore::inactive_read_handle get_inactive_read_handle(querier_base& q) noexcept {
        return std::move(std::get<reader_concurrency_semaphore::inactive_read_handle>(q._reader));
    }
    static void set_reader(querier_base& q, mutation_reader r) noexcept {
        q._reader = std::move(r);
    }
    static void set_inactive_read_handle(querier_base& q, reader_concurrency_semaphore::inactive_read_handle h) noexcept {
        q._reader = std::move(h);
    }
};

template <typename Querier>
void querier_cache::insert_querier(
        query_id key,
        querier_cache::index& index,
        querier_cache::stats& stats,
        Querier&& q,
        std::chrono::seconds ttl,
        tracing::trace_state_ptr trace_state) {
    // FIXME: see #3159
    // In reverse mode mutation_reader drops any remaining rows of the
    // current partition when the page ends so it cannot be reused across
    // pages.
    if (q.is_reversed()) {
        (void)with_gate(_closing_gate, [q = std::move(q)] () mutable {
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
    auto cleanup_irh = defer([&] () noexcept {
        sem.unregister_inactive_read(std::move(irh));
    });

    auto it = index.emplace(key, std::make_unique<Querier>(std::move(q)));

    ++stats.population;
    auto cleanup_index = defer([&] () noexcept {
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


    if (const auto override_ttl = utils::get_local_injector().inject_parameter<uint64_t>("querier-cache-ttl-seconds"); override_ttl) {
        ttl = std::chrono::seconds(*override_ttl);
    }

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

void querier_cache::insert_data_querier(query_id key, querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(key, _data_querier_index, _stats, std::move(q), _entry_ttl, std::move(trace_state));
}

void querier_cache::insert_mutation_querier(query_id key, querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(key, _mutation_querier_index, _stats, std::move(q), _entry_ttl, std::move(trace_state));
}

void querier_cache::insert_shard_querier(query_id key, shard_mutation_querier&& q, tracing::trace_state_ptr trace_state) {
    insert_querier(key, _shard_mutation_querier_index, _stats, std::move(q), _entry_ttl, std::move(trace_state));
}

template <typename Querier>
std::optional<Querier> querier_cache::lookup_querier(
        querier_cache::index& index,
        query_id key,
        const schema& s,
        dht::partition_ranges_view ranges,
        const query::partition_slice& slice,
        reader_concurrency_semaphore& current_sem,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
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
    reader_opt->set_timeout(timeout);
    querier_utils::set_reader(q, std::move(*reader_opt));
    --stats.population;

    const auto can_be_used = can_be_used_for_page(_is_user_semaphore_func, q, s, ranges.front(), slice, current_sem);
    if (can_be_used == can_use::yes) {
        tracing::trace(trace_state, "Reusing querier");
        return std::optional<Querier>(std::move(q));
    }

    tracing::trace(trace_state, "Dropping querier because {}", cannot_use_reason(can_be_used));
    ++stats.drops;

    auto permit = q.permit();

    // Save semaphore name and address for later to use it in
    // error/warning message
    auto q_semaphore_name = permit.semaphore().name();
    auto q_semaphore_address = reinterpret_cast<uintptr_t>(&permit.semaphore());

    // Close and drop the querier in the background.
    // It is safe to do so, since _closing_gate is closed and
    // waited on in querier_cache::stop()
    (void)with_gate(_closing_gate, [q = std::move(q)] () mutable {
        return q.close().finally([q = std::move(q)] {});
    });

    if (can_be_used == can_use::no_scheduling_group_mismatch) {
        ++stats.scheduling_group_mismatches;
        qlogger.warn("user semaphore mismatch detected, dropping reader {}: "
                "reader belongs to {} (0x{:x}) but the query class appropriate is {} (0x{:x})",
                    permit.description(),
                    q_semaphore_name,
                    q_semaphore_address,
                    current_sem.name(),
                    reinterpret_cast<uintptr_t>(&current_sem));
    }
    else if (can_be_used == can_use::no_fatal_semaphore_mismatch) {
        on_internal_error(qlogger, format("semaphore mismatch detected, dropping reader {}: "
                "reader belongs to {} (0x{:x}) but the query class appropriate is {} (0x{:x})",
                permit.description(),
                q_semaphore_name,
                q_semaphore_address,
                current_sem.name(),
                reinterpret_cast<uintptr_t>(&current_sem)));
    }

    return std::nullopt;
}

std::optional<querier> querier_cache::lookup_data_querier(query_id key,
        const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        reader_concurrency_semaphore& current_sem,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
    return lookup_querier<querier>(_data_querier_index, key, s, range, slice, current_sem, std::move(trace_state), timeout);
}

std::optional<querier> querier_cache::lookup_mutation_querier(query_id key,
        const schema& s,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        reader_concurrency_semaphore& current_sem,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
    return lookup_querier<querier>(_mutation_querier_index, key, s, range, slice, current_sem, std::move(trace_state), timeout);
}

std::optional<shard_mutation_querier> querier_cache::lookup_shard_mutation_querier(query_id key,
        const schema& s,
        const dht::partition_range_vector& ranges,
        const query::partition_slice& slice,
        reader_concurrency_semaphore& current_sem,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout) {
    return lookup_querier<shard_mutation_querier>(_shard_mutation_querier_index, key, s, ranges, slice, current_sem,
            std::move(trace_state), timeout);
}

future<> querier_base::close() noexcept {
    struct variant_closer {
        querier_base& q;
        future<> operator()(mutation_reader& reader) {
            return reader.close();
        }
        future<> operator()(reader_concurrency_semaphore::inactive_read_handle& irh) {
            auto reader_opt = q.permit().semaphore().unregister_inactive_read(std::move(irh));
            return reader_opt ? reader_opt->close() : make_ready_future<>();
        }
    };
    return std::visit(variant_closer{*this}, _reader);
}

thread_local logger::rate_limit querier::row_tombstone_warn_rate_limit{std::chrono::seconds(10)};
thread_local logger::rate_limit querier::cell_tombstone_warn_rate_limit{std::chrono::seconds(10)};

void querier::maybe_log_tombstone_warning(std::string_view what, uint64_t live, uint64_t dead, logger::rate_limit& rl) {
    if (!_qr_config.tombstone_warn_threshold || dead < _qr_config.tombstone_warn_threshold) {
        return;
    }
    if (_range->is_singular()) {
        qrlogger.log(log_level::warn, rl, "Read {} live {} and {} dead {}/tombstones for {}.{} partition key \"{}\" {} (see tombstone_warn_threshold)",
                      live, what, dead, what, _schema->ks_name(), _schema->cf_name(), _range->start()->value().key()->with_schema(*_schema), (*_range));
    } else {
        qrlogger.log(log_level::warn, rl, "Read {} live {} and {} dead {}/tombstones for {}.{} <partition-range-scan> {} (see tombstone_warn_threshold)",
                      live, what, dead, what, _schema->ks_name(), _schema->cf_name(), (*_range));
    }
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

} // namespace query
