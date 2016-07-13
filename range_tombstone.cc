/*
 * Copyright (C) 2016 ScyllaDB
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

#include "range_tombstone.hh"

std::ostream& operator<<(std::ostream& out, const bound_kind k) {
    switch(k) {
    case bound_kind::excl_end:
        return out << "excl end";
    case bound_kind::incl_start:
        return out << "incl start";
    case bound_kind::incl_end:
        return out << "incl end";
    case bound_kind::excl_start:
        return out << "excl start";
    }
    abort();
}

bound_kind invert_kind(bound_kind k) {
    switch(k) {
    case bound_kind::excl_start: return bound_kind::incl_end;
    case bound_kind::incl_start: return bound_kind::excl_end;
    case bound_kind::excl_end:   return bound_kind::incl_start;
    case bound_kind::incl_end:   return bound_kind::excl_start;
    }
    abort();
}

int32_t weight(bound_kind k) {
    switch(k) {
    case bound_kind::excl_end:
        return -2;
    case bound_kind::incl_start:
        return -1;
    case bound_kind::incl_end:
        return 1;
    case bound_kind::excl_start:
        return 2;
    }
    abort();
}

const thread_local clustering_key_prefix bound_view::empty_prefix = clustering_key::make_empty();

std::ostream& operator<<(std::ostream& out, const range_tombstone& rt) {
    if (rt) {
        return out << "{range_tombstone: start=" << rt.start_bound() << ", end=" << rt.end_bound() << ", " << rt.tomb << "}";
    } else {
        return out << "{range_tombstone: none}";
    }
}

stdx::optional<range_tombstone> range_tombstone::apply(const schema& s, range_tombstone&& src)
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

void range_tombstone_accumulator::update_current_tombstone() {
    _current_tombstone = boost::accumulate(_range_tombstones, _partition_tombstone, [] (tombstone t, const range_tombstone& rt) {
        t.apply(rt.tomb);
        return t;
    });
}

void range_tombstone_accumulator::drop_unneeded_tombstones(const clustering_key_prefix& ck, int w) {
    auto cmp = [&] (bound_view bv, const clustering_key_prefix& ck, int w) {
        return _reversed ? _cmp(ck, w, bv.prefix, weight(bv.kind)) : _cmp(bv.prefix, weight(bv.kind), ck, w);
    };
    while (!_range_tombstones.empty() && cmp(_range_tombstones.begin()->end_bound(), ck, w)) {
        _range_tombstones.pop_front();
    }
    update_current_tombstone();
}

void range_tombstone_accumulator::apply(const range_tombstone& rt) {
    drop_unneeded_tombstones(rt.start, weight(rt.start_kind));
    _current_tombstone.apply(rt.tomb);

    auto cmp = [&] (const range_tombstone& rt1, const range_tombstone& rt2) {
        return _reversed ? _cmp(rt2.end_bound(), rt1.end_bound()) : _cmp(rt1.end_bound(), rt2.end_bound());
    };
    _range_tombstones.insert(boost::upper_bound(_range_tombstones, rt, cmp), std::move(rt));
}

void range_tombstone_accumulator::clear() {
    _range_tombstones.clear();
    _partition_tombstone = { };
    _current_tombstone = { };
}