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

#include "range_tombstone_list.hh"
#include "utils/allocation_strategy.hh"

range_tombstone_list::range_tombstone_list(const range_tombstone_list& x)
        : _tombstones(x._tombstones.value_comp()) {
    auto cloner = [] (const auto& x) {
        return current_allocator().construct<range_tombstone>(x);
    };
    _tombstones.clone_from(x._tombstones, cloner, current_deleter<range_tombstone>());
}

range_tombstone_list::~range_tombstone_list() {
    _tombstones.clear_and_dispose(current_deleter<range_tombstone>());
}

void range_tombstone_list::apply(const schema& s,
        clustering_key_prefix start, bound_kind start_kind,
        clustering_key_prefix end,
        bound_kind end_kind,
        tombstone tomb) {
    if (!_tombstones.empty()) {
        bound_view::compare less(s);
        bound_view start_bound(start, start_kind);
        auto last = --_tombstones.end();
        if (less(start_bound, last->end_bound())) {
            auto it = _tombstones.upper_bound(start_bound, [less](auto&& sb, auto&& rt) {
                return less(sb, rt.end_bound());
            });
            insert_from(s, std::move(it), std::move(start), start_kind, std::move(end), end_kind, std::move(tomb));
            return;
        }
    }
    auto rt = current_allocator().construct<range_tombstone>(
            std::move(start), start_kind, std::move(end), end_kind, std::move(tomb));
    _tombstones.push_back(*rt);
}

/*
 * Inserts a new element starting at the position pointed to by the iterator, it.
 * This method assumes that:
 *    (it - 1)->end <= start < it->end
 *
 * A range tombstone list is a list of ranges [s_0, e_0]...[s_n, e_n] such that:
 *   - s_i is a start bound and e_i is a end bound
 *   - s_i < e_i
 *   - e_i <= s_i+1
 * Basically, ranges are ordered and non-overlapping.
 */
void range_tombstone_list::insert_from(const schema& s,
    range_tombstones_type::iterator it,
    clustering_key_prefix start,
    bound_kind start_kind,
    clustering_key_prefix end,
    bound_kind end_kind,
    tombstone tomb)
{
    bound_view::compare less(s);
    bound_view end_bound(end, end_kind);
    while (it != _tombstones.end()) {
        bound_view start_bound(start, start_kind);

        if (less(end_bound, start_bound)) {
            return;
        }

        if (tomb.timestamp > it->tomb.timestamp) {
            // We overwrite the current tombstone.

            if (less(it->start_bound(), start_bound)) {
                auto new_end = bound_view(start, invert_kind(start_kind));
                if (!less(new_end, it->start_bound())) {
                    auto rt = current_allocator().construct<range_tombstone>(it->start_bound(), new_end, it->tomb);
                    it = _tombstones.insert_before(it, *rt);
                    ++it;
                    *it = {start_bound, it->end_bound(), it->tomb};
                }
            }

            // Here start <= it->start.

            if (less(end_bound, it->start_bound())) {
                // Here end < it->start, so the new tombstone is before the current one.
                auto rt = current_allocator().construct<range_tombstone>(
                        std::move(start), start_kind, std::move(end), end_kind, std::move(tomb));
                _tombstones.insert_before(it, *rt);
                return;
            }

            if (less(end_bound, it->end_bound())) {
                // Here start <= it->start and end < it->end.
                auto rt = current_allocator().construct<range_tombstone>(
                        std::move(start), start_kind, end, end_kind, std::move(tomb));
                it = _tombstones.insert_before(it, *rt);
                ++it;
                auto new_start_kind = invert_kind(end_kind);
                *it = {std::move(end), new_start_kind, it->end, it->end_kind, it->tomb};
                return;
            }

            // Here start <= it->start and end >= it->end.

            // If we're on the last tombstone, or if we end before the next start, we set the
            // new tombstone and are done.
            auto next = std::next(it);
            if (next == _tombstones.end() || !less(next->start_bound(), end_bound)) {
                *it = {std::move(start), start_kind, std::move(end), end_kind, std::move(tomb)};
                return;
            }

            // We overlap with the next tombstone.

            *it = {std::move(start), start_kind, next->start, invert_kind(next->start_kind), tomb};
            start = next->start;
            start_kind = next->start_kind;
            ++it;
        } else {
            // We don't overwrite the current tombstone.

            if (less(start_bound, it->start_bound())) {
                // The new tombstone starts before the current one.
                if (less(it->start_bound(), end_bound)) {
                    // Here start < it->start and it->start < end.
                    auto new_end_kind = invert_kind(it->start_kind);
                    if (!less(bound_view(it->start, new_end_kind), start_bound)) {
                        auto rt = current_allocator().construct<range_tombstone>(
                                std::move(start), start_kind, it->start, new_end_kind, tomb);
                        it = _tombstones.insert_before(it, *rt);
                        ++it;
                    }
                } else {
                    // Here start < it->start and end <= it->start, so just insert the new tombstone.
                    auto rt = current_allocator().construct<range_tombstone>(
                            std::move(start), start_kind, std::move(end), end_kind, std::move(tomb));
                    _tombstones.insert_before(it, *rt);
                   return;
                }
            }

            if (less(it->end_bound(), end_bound)) {
                // Here the current tombstone overwrites a range of the new one.
                start = it->end;
                start_kind = invert_kind(it->end_kind);
                ++it;
            } else {
                // Here the current tombstone completely overwrites the new one.
                return;
            }
        }
    }

    // If we got here, then just insert the remainder at the end.
    auto rt = current_allocator().construct<range_tombstone>(
            std::move(start), start_kind, std::move(end), end_kind, std::move(tomb));
    _tombstones.push_back(*rt);
}

/*
 * Returns the tombstone covering the specified key, or an empty tombstone otherwise.
 */
tombstone range_tombstone_list::search_tombstone_covering(const schema& s, const clustering_key_prefix& key) const {
    bound_view::compare less(s);
    auto it = _tombstones.upper_bound(key, [less](auto&& k, auto&& rt) {
        return less(k, rt.end_bound());
    });

    if (it == _tombstones.end() || less(key, it->start_bound())) {
        return {};
    }

    return it->tomb;
}
