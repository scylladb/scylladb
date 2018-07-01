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

#include <boost/range/adaptor/reversed.hpp>
#include "range_tombstone_list.hh"
#include "utils/allocation_strategy.hh"
#include <seastar/util/variant_utils.hh>

range_tombstone_list::range_tombstone_list(const range_tombstone_list& x)
        : _tombstones(x._tombstones.value_comp()) {
    auto cloner = [] (const range_tombstone& x) {
        return current_allocator().construct<range_tombstone>(x);
    };
    _tombstones.clone_from(x._tombstones, cloner, current_deleter<range_tombstone>());
}

range_tombstone_list::~range_tombstone_list() {
    _tombstones.clear_and_dispose(current_deleter<range_tombstone>());
}

void range_tombstone_list::apply_reversibly(const schema& s,
        clustering_key_prefix start, bound_kind start_kind,
        clustering_key_prefix end,
        bound_kind end_kind,
        tombstone tomb,
        reverter& rev)
{
    if (!_tombstones.empty()) {
        bound_view::compare less(s);
        bound_view start_bound(start, start_kind);
        auto last = --_tombstones.end();
        range_tombstones_type::iterator it;
        if (less(start_bound, last->end_bound())) {
            it = _tombstones.upper_bound(start_bound, [less](auto&& sb, auto&& rt) {
                return less(sb, rt.end_bound());
            });
        } else {
            it = _tombstones.end();
        }
        insert_from(s, std::move(it), std::move(start), start_kind, std::move(end), end_kind, std::move(tomb), rev);
        return;
    }
    auto rt = alloc_strategy_unique_ptr<range_tombstone>(current_allocator().construct<range_tombstone>(
            std::move(start), start_kind, std::move(end), end_kind, std::move(tomb)));
    rev.insert(_tombstones.end(), *rt);
    rt.release();
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
    tombstone tomb,
    reverter& rev)
{
    bound_view::compare less(s);
    bound_view end_bound(end, end_kind);
    if (it != _tombstones.begin()) {
        auto prev = std::prev(it);
        if (prev->tomb == tomb && prev->end_bound().adjacent(s, bound_view(start, start_kind))) {
            start = prev->start;
            start_kind = prev->start_kind;
            rev.erase(prev);
        }
    }
    while (it != _tombstones.end()) {
        bound_view start_bound(start, start_kind);
        if (less(end_bound, start_bound)) {
            return;
        }

        if (less(end_bound, it->start_bound())) {
            // not overlapping
            if (it->tomb == tomb && end_bound.adjacent(s, it->start_bound())) {
                rev.update(it, {std::move(start), start_kind, it->end, it->end_kind, tomb});
            } else {
                auto rt = alloc_strategy_unique_ptr<range_tombstone>(
                    current_allocator().construct<range_tombstone>(std::move(start), start_kind, std::move(end),
                    end_kind, tomb));
                rev.insert(it, *rt);
                rt.release();
            }
            return;
        }

        auto c = tomb.compare(it->tomb);
        if (c == 0) {
            // same timestamp, overlapping or adjacent, so merge.
            if (less(it->start_bound(), start_bound)) {
                start = it->start;
                start_kind = it->start_kind;
            }
            if (less(end_bound, it->end_bound())) {
                end = it->end;
                end_kind = it->end_kind;
                end_bound = bound_view(end, end_kind);
            }
            it = rev.erase(it);
        } else if (c > 0) {
            // We overwrite the current tombstone.

            if (less(it->start_bound(), start_bound)) {
                auto new_end = bound_view(start, invert_kind(start_kind));
                if (!less(new_end, it->start_bound())) {
                    // Here it->start < start
                    auto rt = alloc_strategy_unique_ptr<range_tombstone>(
                        current_allocator().construct<range_tombstone>(it->start_bound(), new_end, it->tomb));
                    rev.update(it, {start_bound, it->end_bound(), it->tomb});
                    rev.insert(it, *rt);
                    rt.release();
                }
            }

            if (less(end_bound, it->end_bound())) {
                // Here start <= it->start and end < it->end.
                auto rt = alloc_strategy_unique_ptr<range_tombstone>(
                    current_allocator().construct<range_tombstone>(std::move(start), start_kind, end, end_kind, std::move(tomb)));
                rev.update(it, {std::move(end), invert_kind(end_kind), it->end, it->end_kind, it->tomb});
                rev.insert(it, *rt);
                rt.release();
                return;
            }

            // Here start <= it->start and end >= it->end.
            it = rev.erase(it);
        } else {
            // We don't overwrite the current tombstone.

            if (less(start_bound, it->start_bound())) {
                // The new tombstone starts before the current one.
                if (less(it->start_bound(), end_bound)) {
                    // Here start < it->start and it->start < end.
                    auto new_end_kind = invert_kind(it->start_kind);
                    if (!less(bound_view(it->start, new_end_kind), start_bound)) {
                        auto rt = alloc_strategy_unique_ptr<range_tombstone>(current_allocator().construct<range_tombstone>(
                                std::move(start), start_kind, it->start, new_end_kind, tomb));
                        it = rev.insert(it, *rt);
                        rt.release();
                        ++it;
                    }
                } else {
                    // Here start < it->start and end <= it->start, so just insert the new tombstone.
                    auto rt = alloc_strategy_unique_ptr<range_tombstone>(current_allocator().construct<range_tombstone>(
                            std::move(start), start_kind, std::move(end), end_kind, std::move(tomb)));
                    rev.insert(it, *rt);
                    rt.release();
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
    auto rt = alloc_strategy_unique_ptr<range_tombstone>(current_allocator().construct<range_tombstone>(
            std::move(start), start_kind, std::move(end), end_kind, std::move(tomb)));
    rev.insert(it, *rt);
    rt.release();
}

range_tombstone_list::range_tombstones_type::iterator range_tombstone_list::find(const schema& s, const range_tombstone& rt) {
    bound_view::compare less(s);
    auto it = _tombstones.find(rt, [less](auto&& rt1, auto&& rt2) {
        return less(rt1.end_bound(), rt2.end_bound());
    });

    if (it != _tombstones.end() && it->equal(s, rt)) {
        return it;
    }
    return _tombstones.end();
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

range_tombstone_list range_tombstone_list::difference(const schema& s, const range_tombstone_list& other) const {
    range_tombstone_list diff(s);
    bound_view::compare cmp_rt(s);
    auto other_rt = other.begin();
    auto this_rt = begin();
    if (this_rt == end()) {
        return diff;
    }
    bound_view cur_start = this_rt->start_bound();
    bound_view cur_end = this_rt->end_bound();
    auto advance_this_rt = [&] () {
        if (++this_rt != end()) {
            cur_start = this_rt->start_bound();
            cur_end = this_rt->end_bound();
        }
    };
    while (this_rt != end() && other_rt != other.end()) {
        if (cmp_rt(cur_end, other_rt->start_bound())) {
            diff.apply(s, cur_start, cur_end, this_rt->tomb);
            advance_this_rt();
            continue;
        }
        if (cmp_rt(other_rt->end_bound(), cur_start)) {
            ++other_rt;
            continue;
        }
        auto new_end = bound_view(other_rt->start_bound().prefix(), invert_kind(other_rt->start_bound().kind()));
        if (cmp_rt(cur_start, new_end)) {
            diff.apply(s, cur_start, new_end, this_rt->tomb);
            cur_start = other_rt->start_bound();
        }
        if (cmp_rt(cur_end, other_rt->end_bound())) {
            if (this_rt->tomb > other_rt->tomb) {
                diff.apply(s, cur_start, cur_end, this_rt->tomb);
            }
            advance_this_rt();
        } else {
            auto end = other_rt->end_bound();
            if (this_rt->tomb > other_rt->tomb) {
                diff.apply(s, cur_start, end, this_rt->tomb);
            }
            cur_start = bound_view(end.prefix(), invert_kind(end.kind()));
            ++other_rt;
            if (cmp_rt(cur_end, cur_start)) {
                advance_this_rt();
            }
        }
    }
    while (this_rt != end()) {
        diff.apply(s, cur_start, cur_end, this_rt->tomb);
        advance_this_rt();
    }
    return diff;
}

stop_iteration range_tombstone_list::clear_gently() noexcept {
    auto del = current_deleter<range_tombstone>();
    auto i = _tombstones.begin();
    auto end = _tombstones.end();
    while (i != end) {
        i = _tombstones.erase_and_dispose(i, del);
        if (need_preempt()) {
            return stop_iteration::no;
        }
    }
    return stop_iteration::yes;
}

void range_tombstone_list::apply(const schema& s, const range_tombstone_list& rt_list) {
    for (auto&& rt : rt_list) {
        apply(s, rt);
    }
}

// See reversibly_mergeable.hh
range_tombstone_list::reverter range_tombstone_list::apply_reversibly(const schema& s, range_tombstone_list& rt_list) {
    reverter rev(s, *this);
    for (auto&& rt : rt_list) {
        apply_reversibly(s, rt.start, rt.start_kind, rt.end, rt.end_kind, rt.tomb, rev);
    }
    return rev;
}

boost::iterator_range<range_tombstone_list::const_iterator>
range_tombstone_list::slice(const schema& s, const query::clustering_range& r) const {
    auto bv_range = bound_view::from_range(r);
    struct order_by_end {
        bound_view::compare less;
        order_by_end(const schema& s) : less(s) {}
        bool operator()(bound_view v, const range_tombstone& rt) const { return less(v, rt.end_bound()); }
        bool operator()(const range_tombstone& rt, bound_view v) const { return less(rt.end_bound(), v); }
    };
    struct order_by_start {
        bound_view::compare less;
        order_by_start(const schema& s) : less(s) {}
        bool operator()(bound_view v, const range_tombstone& rt) const { return less(v, rt.start_bound()); }
        bool operator()(const range_tombstone& rt, bound_view v) const { return less(rt.start_bound(), v); }
    };
    return boost::make_iterator_range(
        _tombstones.lower_bound(bv_range.first, order_by_end{s}),
        _tombstones.upper_bound(bv_range.second, order_by_start{s}));
}

boost::iterator_range<range_tombstone_list::const_iterator>
range_tombstone_list::slice(const schema& s, position_in_partition_view start, position_in_partition_view end) const {
    struct order_by_end {
        position_in_partition::less_compare less;
        order_by_end(const schema& s) : less(s) {}
        bool operator()(position_in_partition_view v, const range_tombstone& rt) const { return less(v, rt.end_position()); }
        bool operator()(const range_tombstone& rt, position_in_partition_view v) const { return less(rt.end_position(), v); }
    };
    struct order_by_start {
        position_in_partition::less_compare less;
        order_by_start(const schema& s) : less(s) {}
        bool operator()(position_in_partition_view v, const range_tombstone& rt) const { return less(v, rt.position()); }
        bool operator()(const range_tombstone& rt, position_in_partition_view v) const { return less(rt.position(), v); }
    };
    return boost::make_iterator_range(
        _tombstones.upper_bound(start, order_by_end{s}), // end_position() is exclusive, hence upper_bound()
        _tombstones.lower_bound(end, order_by_start{s}));
}

range_tombstone_list::iterator
range_tombstone_list::erase(const_iterator a, const_iterator b) {
    return _tombstones.erase_and_dispose(a, b, current_deleter<range_tombstone>());
}

void range_tombstone_list::trim(const schema& s, const query::clustering_row_ranges& ranges) {
    range_tombstone_list list(s);
    bound_view::compare less(s);
    for (auto&& range : ranges) {
        auto start = bound_view::from_range_start(range);
        auto end = bound_view::from_range_end(range);
        for (const range_tombstone& rt : slice(s, range)) {
            list.apply(s, range_tombstone(
                std::max(rt.start_bound(), start, less),
                std::min(rt.end_bound(), end, less),
                rt.tomb));
        }
    }
    *this = std::move(list);
}

range_tombstone_list::range_tombstones_type::iterator
range_tombstone_list::reverter::insert(range_tombstones_type::iterator it, range_tombstone& new_rt) {
    _ops.emplace_back(insert_undo_op(new_rt));
    return _dst._tombstones.insert_before(it, new_rt);
}

range_tombstone_list::range_tombstones_type::iterator
range_tombstone_list::reverter::erase(range_tombstones_type::iterator it) {
    _ops.reserve(_ops.size() + 1);
    _ops.emplace_back(erase_undo_op(*it));
    return _dst._tombstones.erase(it);
}

void range_tombstone_list::reverter::update(range_tombstones_type::iterator it, range_tombstone&& new_rt) {
    _ops.reserve(_ops.size() + 1);
    swap(*it, new_rt);
    _ops.emplace_back(update_undo_op(std::move(new_rt), *it));
}

void range_tombstone_list::reverter::revert() noexcept {
    for (auto&& rt : _ops | boost::adaptors::reversed) {
        seastar::visit(rt, [this] (auto& op) {
            op.undo(_s, _dst);
        });
    }
    cancel();
}

range_tombstone_list::range_tombstones_type::iterator
range_tombstone_list::nop_reverter::insert(range_tombstones_type::iterator it, range_tombstone& new_rt) {
    return _dst._tombstones.insert_before(it, new_rt);
}

range_tombstone_list::range_tombstones_type::iterator
range_tombstone_list::nop_reverter::erase(range_tombstones_type::iterator it) {
    return _dst._tombstones.erase_and_dispose(it, alloc_strategy_deleter<range_tombstone>());
}

void range_tombstone_list::nop_reverter::update(range_tombstones_type::iterator it, range_tombstone&& new_rt) {
    *it = std::move(new_rt);
}

void range_tombstone_list::insert_undo_op::undo(const schema& s, range_tombstone_list& rt_list) noexcept {
    auto it = rt_list.find(s, _new_rt);
    assert (it != rt_list.end());
    rt_list._tombstones.erase_and_dispose(it, current_deleter<range_tombstone>());
}

void range_tombstone_list::erase_undo_op::undo(const schema& s, range_tombstone_list& rt_list) noexcept {
    rt_list._tombstones.insert(*_rt.release());
}

void range_tombstone_list::update_undo_op::undo(const schema& s, range_tombstone_list& rt_list) noexcept {
    auto it = rt_list.find(s, _new_rt);
    assert (it != rt_list.end());
    *it = std::move(_old_rt);
}

std::ostream& operator<<(std::ostream& out, const range_tombstone_list& list) {
    return out << "{" << ::join(", ", list) << "}";
}

bool range_tombstone_list::equal(const schema& s, const range_tombstone_list& other) const {
    return boost::equal(_tombstones, other._tombstones, [&s] (auto&& rt1, auto&& rt2) {
        return rt1.equal(s, rt2);
    });
}

stop_iteration range_tombstone_list::apply_monotonically(const schema& s, range_tombstone_list&& list, is_preemptible preemptible) {
    auto del = current_deleter<range_tombstone>();
    auto it = list.begin();
    while (it != list.end()) {
        // FIXME: Optimize by stealing the entry
        apply_monotonically(s, *it);
        it = list._tombstones.erase_and_dispose(it, del);
        if (preemptible && need_preempt()) {
            return stop_iteration::no;
        }
    }
    return stop_iteration::yes;
}

void range_tombstone_list::apply_monotonically(const schema& s, const range_tombstone_list& list) {
    for (auto&& rt : list) {
        apply_monotonically(s, rt);
    }
}

void range_tombstone_list::apply_monotonically(const schema& s, const range_tombstone& rt) {
    // FIXME: Optimize given this has relaxed exception guarantees.
    // Note that apply() doesn't have monotonic guarantee because it doesn't restore erased entries.
    reverter rev(s, *this);
    apply_reversibly(s, rt.start, rt.start_kind, rt.end, rt.end_kind, rt.tomb, rev);
    rev.cancel();
}
