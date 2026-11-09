/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/position_range_utils.hh"
#include "utils/assert.hh"

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
