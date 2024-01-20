/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "range_tombstone.hh"

#include <boost/range/algorithm/upper_bound.hpp>
#include <boost/range/numeric.hpp>

std::optional<range_tombstone> range_tombstone::apply(const schema& s, range_tombstone&& src)
{
    bound_view::compare cmp(s);
    if (tomb == src.tomb) {
        if (cmp(end_bound(), src.end_bound())) {
            end = std::move(src.end);
            end_kind = src.end_kind;
        }
        return { };
    }
    if (tomb < src.tomb) {
        std::swap(*this, src);
    }
    if (cmp(end_bound(), src.end_bound())) {
        return range_tombstone(end, invert_kind(end_kind), std::move(src.end), src.end_kind, src.tomb);
    }
    return { };
}

position_in_partition_view range_tombstone::position() const {
    return position_in_partition_view(position_in_partition_view::range_tombstone_tag_t(), start_bound());
}

position_in_partition_view range_tombstone::end_position() const {
    return position_in_partition_view(position_in_partition_view::range_tombstone_tag_t(), end_bound());
}

void range_tombstone_accumulator::update_current_tombstone() {
    _current_tombstone = boost::accumulate(_range_tombstones, _partition_tombstone, [] (tombstone t, const range_tombstone& rt) {
        t.apply(rt.tomb);
        return t;
    });
}

void range_tombstone_accumulator::drop_unneeded_tombstones(const clustering_key_prefix& ck, int w) {
    auto cmp = [&] (const range_tombstone& rt, const clustering_key_prefix& ck, int w) {
        auto bv = rt.end_bound();
        return _cmp(bv.prefix(), weight(bv.kind()), ck, w);
    };
    bool dropped = false;
    while (!_range_tombstones.empty() && cmp(*_range_tombstones.begin(), ck, w)) {
        dropped = true;
        _range_tombstones.pop_front();
    }
    if (dropped) {
        update_current_tombstone();
    }
}

void range_tombstone_accumulator::apply(range_tombstone rt) {
    drop_unneeded_tombstones(rt.start, weight(rt.start_kind));
    _current_tombstone.apply(rt.tomb);

    auto cmp = [&] (const range_tombstone& rt1, const range_tombstone& rt2) {
        return _cmp(rt1.end_bound(), rt2.end_bound());
    };
    _range_tombstones.insert(boost::upper_bound(_range_tombstones, rt, cmp), std::move(rt));
}

void range_tombstone_accumulator::clear() {
    _range_tombstones.clear();
    _partition_tombstone = { };
    _current_tombstone = { };
}
