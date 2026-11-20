/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/position_range_utils.hh"
#include "utils/assert.hh"
#include "utils/on_internal_error.hh"

#include <algorithm>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

position_range position_range::from_range(const query::clustering_range& range) {
    auto bv_range = bound_view::from_range(range);
    return {
        position_in_partition(position_in_partition::range_tag_t(), bv_range.first),
        position_in_partition(position_in_partition::range_tag_t(), bv_range.second)
    };
}

position_range::position_range(const query::clustering_range& range)
    : position_range(from_range(range))
{ }

position_range::position_range(query::clustering_range&& range)
    : position_range(range) // FIXME: optimize
{ }

std::optional<query::clustering_range> position_range_to_clustering_range(const position_range& r, const schema& s) {
    SCYLLA_ASSERT(r.start().get_type() == partition_region::clustered);
    SCYLLA_ASSERT(r.end().get_type() == partition_region::clustered);

    if (r.start().has_key() && r.end().has_key()
            && clustering_key_prefix::equality(s)(r.start().key(), r.end().key())) {
        SCYLLA_ASSERT(r.start().get_bound_weight() != r.end().get_bound_weight());

        if (r.end().get_bound_weight() == bound_weight::after_all_prefixed
                && r.start().get_bound_weight() != bound_weight::after_all_prefixed) {
            // [before x, after x) and [for x, after x) get converted to [x, x].
            return query::clustering_range::make_singular(r.start().key());
        }

        // [before x, for x) does not contain any keys.
        return std::nullopt;
    }

    // position_range -> clustering_range
    // (recall that position_ranges are always left-closed, right opened):
    // [before x, ...), [for x, ...) -> [x, ...
    // [after x, ...) -> (x, ...
    // [..., before x), [..., for x) -> ..., x)
    // [..., after x) -> ..., x]

    auto to_bound = [&s] (const position_in_partition& p, bool left) -> std::optional<query::clustering_range::bound> {
        if (p.is_before_all_clustered_rows(s)) {
            SCYLLA_ASSERT(left);
            return {};
        }

        if (p.is_after_all_clustered_rows(s)) {
            SCYLLA_ASSERT(!left);
            return {};
        }

        SCYLLA_ASSERT(p.has_key());

        auto bw = p.get_bound_weight();
        bool inclusive = left
            ? bw != bound_weight::after_all_prefixed
            : bw == bound_weight::after_all_prefixed;

        return query::clustering_range::bound{p.key(), inclusive};
    };

    return query::clustering_range{to_bound(r.start(), true), to_bound(r.end(), false)};
}

std::optional<position_range> position_range_intersection(const schema& s, const position_range& a, const position_range& b) {
    position_in_partition::less_compare less(s);
    const auto& start = less(a.start(), b.start()) ? b.start() : a.start();
    const auto& end = less(b.end(), a.end()) ? b.end() : a.end();
    if (!less(start, end)) {
        return std::nullopt;
    }
    return position_range(start, end);
}

// Appends r to ranges, which is sorted by start and non-overlapping, merging it
// with the last range if the two overlap or are adjacent. Drops r if it's empty.
// The start of r must not be smaller than the start of the last range in ranges.
static void append_coalescing(const schema& s, position_range_vector& ranges, position_range r) {
    position_in_partition::less_compare less(s);
    if (!less(r.start(), r.end())) {
        return;
    }
    if (!ranges.empty() && less(r.start(), ranges.back().start())) {
        utils::on_internal_error(fmt::format("position ranges are not sorted by start: {} comes after {}", r, ranges.back()));
    }
    if (!ranges.empty() && !less(ranges.back().end(), r.start())) {
        if (less(ranges.back().end(), r.end())) {
            ranges.back().set_end(std::move(r).end());
        }
        return;
    }
    ranges.push_back(std::move(r));
}

void sort_position_ranges(const schema& s, position_range_vector& ranges) {
    position_in_partition::less_compare less(s);
    std::sort(ranges.begin(), ranges.end(), [&less] (const position_range& a, const position_range& b) {
        return less(a.start(), b.start());
    });
}

seastar::future<position_range_vector> deoverlap_position_ranges(const schema& s, position_range_vector ranges) {
    position_range_vector result;
    for (auto& r : ranges) {
        append_coalescing(s, result, std::move(r));
        co_await seastar::coroutine::maybe_yield();
    }
    co_return result;
}

seastar::future<position_range_vector> merge_position_ranges(const schema& s, position_range_vector a, position_range_vector b) {
    position_in_partition::less_compare less(s);
    position_range_vector result;
    auto i = a.begin();
    auto j = b.begin();
    while (i != a.end() || j != b.end()) {
        if (j == b.end() || (i != a.end() && less(i->start(), j->start()))) {
            append_coalescing(s, result, std::move(*i++));
        } else {
            append_coalescing(s, result, std::move(*j++));
        }
        co_await seastar::coroutine::maybe_yield();
    }
    co_return result;
}
