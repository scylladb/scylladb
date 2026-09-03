/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/token_range_tombstone.hh"
#include "mutation/mutation.hh"

#include <algorithm>

token_range_tombstone_list::token_range_tombstone_list(std::initializer_list<token_range_tombstone> rts) {
    for (auto&& rt : rts) {
        apply(rt);
    }
}

// Returns the index of the first entry which overlaps or touches (start, ...],
// that is the first entry whose end is not before start.
static size_t lower_bound_index(const utils::chunked_vector<token_range_tombstone>& v, const dht::token& start) {
    return std::ranges::lower_bound(v, start, std::less<>(), [] (const token_range_tombstone& rt) {
        return rt.end_inclusive();
    }) - v.begin();
}

void token_range_tombstone_list::apply(const token_range_tombstone& rt) {
    if (rt.empty()) {
        return;
    }

    // The entries which the new tombstone overlaps or touches, and which
    // therefore have to be rewritten, are [i, j). Entries which merely touch
    // are included so that the result can be coalesced with them.
    const size_t i = lower_bound_index(_tombstones, rt.start_exclusive());
    size_t j = i;
    while (j != _tombstones.size() && _tombstones[j].start_exclusive() <= rt.end_inclusive()) {
        ++j;
    }

    utils::chunked_vector<token_range_tombstone> merged;
    auto emit = [&] (dht::token start, dht::token end, tombstone tomb) {
        if (!(start < end) || !tomb) {
            return;
        }
        if (!merged.empty() && merged.back().end_inclusive() == start && merged.back().tomb() == tomb) {
            merged.back().set_end_inclusive(std::move(end));
            return;
        }
        merged.emplace_back(std::move(start), std::move(end), tomb);
    };

    // Sweep over the affected entries left to right. Everything below the
    // cursor has already been emitted.
    dht::token cursor = rt.start_exclusive();
    for (size_t k = i; k != j; ++k) {
        const token_range_tombstone& e = _tombstones[k];
        if (e.start_exclusive() < cursor) {
            // The part of the old entry which precedes the new tombstone.
            emit(e.start_exclusive(), std::min(e.end_inclusive(), cursor), e.tomb());
        }
        if (e.end_inclusive() <= cursor) {
            // The old entry only touches the new tombstone from the left.
            continue;
        }
        if (cursor < e.start_exclusive()) {
            // A gap covered by the new tombstone alone.
            emit(cursor, e.start_exclusive(), rt.tomb());
            cursor = e.start_exclusive();
        }
        // The overlap, where the newer of the two tombstones wins.
        auto overlap_end = std::min(e.end_inclusive(), rt.end_inclusive());
        tombstone merged_tomb = e.tomb();
        merged_tomb.apply(rt.tomb());
        emit(cursor, overlap_end, merged_tomb);
        cursor = overlap_end;
        if (rt.end_inclusive() < e.end_inclusive()) {
            // The part of the old entry which follows the new tombstone.
            emit(rt.end_inclusive(), e.end_inclusive(), e.tomb());
        }
    }
    emit(cursor, rt.end_inclusive(), rt.tomb());

    // Coalesce with the entry which precedes the rewritten range, if any.
    size_t replace_from = i;
    if (replace_from && !merged.empty()) {
        const token_range_tombstone& prev = _tombstones[replace_from - 1];
        if (prev.end_inclusive() == merged.front().start_exclusive() && prev.tomb() == merged.front().tomb()) {
            merged.front().set_start_exclusive(prev.start_exclusive());
            --replace_from;
        }
    }

    _tombstones.erase(_tombstones.begin() + replace_from, _tombstones.begin() + j);
    _tombstones.insert(_tombstones.begin() + replace_from, merged.begin(), merged.end());
}

void token_range_tombstone_list::apply(const token_range_tombstone_list& list) {
    for (auto&& rt : list) {
        apply(rt);
    }
}

tombstone token_range_tombstone_list::search(const dht::token& t) const noexcept {
    auto i = lower_bound_index(_tombstones, t);
    if (i == _tombstones.size()) {
        return {};
    }
    const token_range_tombstone& rt = _tombstones[i];
    return rt.contains(t) ? rt.tomb() : tombstone();
}

tombstone token_range_tombstone_list::max_tombstone() const noexcept {
    tombstone t;
    for (auto&& rt : _tombstones) {
        t.apply(rt.tomb());
    }
    return t;
}

void token_range_tombstone_list::apply_to(mutation& m) const {
    m.apply(*this);
}

token_range_tombstone_list token_range_tombstone_list::slice(const dht::token& start, const dht::token& end) const {
    token_range_tombstone_list result;
    for (size_t k = lower_bound_index(_tombstones, start); k != _tombstones.size(); ++k) {
        const token_range_tombstone& rt = _tombstones[k];
        if (end <= rt.start_exclusive()) {
            break;
        }
        token_range_tombstone trimmed(std::max(rt.start_exclusive(), start), std::min(rt.end_inclusive(), end), rt.tomb());
        if (!trimmed.empty()) {
            result._tombstones.emplace_back(std::move(trimmed));
        }
    }
    return result;
}
