/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/adaptor/reversed.hpp>
#include "range_tombstone_list.hh"
#include "utils/assert.hh"
#include "utils/allocation_strategy.hh"
#include <seastar/util/variant_utils.hh>

range_tombstone_list::range_tombstone_list(const range_tombstone_list& x)
        : _tombstones(x._tombstones.value_comp()) {
    auto cloner = [] (const range_tombstone_entry& x) {
        return current_allocator().construct<range_tombstone_entry>(x);
    };
    _tombstones.clone_from(x._tombstones, cloner, current_deleter<range_tombstone_entry>());
}

range_tombstone_list::~range_tombstone_list() {
    _tombstones.clear_and_dispose(current_deleter<range_tombstone_entry>());
}

template <typename... Args>
static auto construct_range_tombstone_entry(Args&&... args) {
    return alloc_strategy_unique_ptr<range_tombstone_entry>(current_allocator().construct<range_tombstone_entry>(range_tombstone(std::forward<Args>(args)...)));
}

void range_tombstone_list::apply_reversibly(const schema& s,
        clustering_key_prefix start_key, bound_kind start_kind,
        clustering_key_prefix end_key,
        bound_kind end_kind,
        tombstone tomb,
        reverter& rev)
{
    position_in_partition::less_compare less(s);
    position_in_partition start(position_in_partition::range_tag_t(), bound_view(std::move(start_key), start_kind));
    position_in_partition end(position_in_partition::range_tag_t(), bound_view(std::move(end_key), end_kind));

    if (!less(start, end)) {
        return;
    }

    if (!_tombstones.empty()) {
        auto last = --_tombstones.end();
        range_tombstones_type::iterator it;
        if (less(start, last->end_position())) {
            it = _tombstones.upper_bound(start, [less](auto&& sb, auto&& rt) {
                return less(sb, rt.end_position());
            });
        } else {
            it = _tombstones.end();
        }
        insert_from(s, std::move(it), std::move(start), std::move(end), std::move(tomb), rev);
        return;
    }
    auto rt = construct_range_tombstone_entry(std::move(start), std::move(end), std::move(tomb));
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
    position_in_partition start,
    position_in_partition end,
    tombstone tomb,
    reverter& rev)
{
    position_in_partition::tri_compare cmp(s);

    if (it != _tombstones.begin()) {
        auto prev = std::prev(it);
        if (prev->tombstone().tomb == tomb && cmp(prev->end_position(), start) == 0) {
            start = prev->position();
            rev.erase(prev);
        }
    }
    while (it != _tombstones.end()) {
        if (cmp(end, start) <= 0) {
            return;
        }

        if (cmp(end, it->position()) < 0) {
            // not overlapping
            if (it->tombstone().tomb == tomb && cmp(end, it->position()) == 0) {
                rev.update(it, {std::move(start), std::move(end), tomb});
            } else {
                auto rt = construct_range_tombstone_entry(std::move(start), std::move(end), tomb);
                rev.insert(it, *rt);
                rt.release();
            }
            return;
        }

        auto c = tomb <=> it->tombstone().tomb;
        if (c == 0) {
            // same timestamp, overlapping or adjacent, so merge.
            if (cmp(it->position(), start) < 0) {
                start = it->position();
            }
            if (cmp(end, it->end_position()) < 0) {
                end = it->end_position();
            }
            it = rev.erase(it);
        } else if (c > 0) {
            // We overwrite the current tombstone.

            if (cmp(it->position(), start) < 0) {
                {
                    auto rt = construct_range_tombstone_entry(it->position(), start, it->tombstone().tomb);
                    rev.update(it, {start, it->end_position(), it->tombstone().tomb});
                    rev.insert(it, *rt);
                    rt.release();
                }
            }

            if (cmp(end, it->end_position()) < 0) {
                // Here start <= it->start and end < it->end.
                auto rt = construct_range_tombstone_entry(std::move(start), end, std::move(tomb));
                rev.update(it, {std::move(end), it->end_position(), it->tombstone().tomb});
                rev.insert(it, *rt);
                rt.release();
                return;
            }

            // Here start <= it->start and end >= it->end.
            it = rev.erase(it);
        } else {
            // We don't overwrite the current tombstone.

            if (cmp(start, it->position()) < 0) {
                // The new tombstone starts before the current one.
                if (cmp(it->position(), end) < 0) {
                    // Here start < it->start and it->start < end.
                    {
                        auto rt = construct_range_tombstone_entry(std::move(start), it->position(), tomb);
                        it = rev.insert(it, *rt);
                        rt.release();
                        ++it;
                    }
                } else {
                    // Here start < it->start and end <= it->start, so just insert the new tombstone.
                    auto rt = construct_range_tombstone_entry(std::move(start), std::move(end), std::move(tomb));
                    rev.insert(it, *rt);
                    rt.release();
                    return;
                }
            }

            if (cmp(it->end_position(), end) < 0) {
                // Here the current tombstone overwrites a range of the new one.
                start = it->end_position();
                ++it;
            } else {
                // Here the current tombstone completely overwrites the new one.
                return;
            }
        }
    }

    // If we got here, then just insert the remainder at the end.
    auto rt = construct_range_tombstone_entry(std::move(start), std::move(end), std::move(tomb));
    rev.insert(it, *rt);
    rt.release();
}

range_tombstone_list::range_tombstones_type::iterator range_tombstone_list::find(const schema& s, const range_tombstone_entry& rt) {
    bound_view::compare less(s);
    auto it = _tombstones.find(rt, [less](auto&& rt1, auto&& rt2) {
        return less(rt1.end_bound(), rt2.end_bound());
    });

    if (it != _tombstones.end() && it->tombstone().equal(s, rt.tombstone())) {
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

    return it->tombstone().tomb;
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
            diff.apply(s, cur_start, cur_end, this_rt->tombstone().tomb);
            advance_this_rt();
            continue;
        }
        if (cmp_rt(other_rt->end_bound(), cur_start)) {
            ++other_rt;
            continue;
        }
        auto new_end = bound_view(other_rt->start_bound().prefix(), invert_kind(other_rt->start_bound().kind()));
        if (cmp_rt(cur_start, new_end)) {
            diff.apply(s, cur_start, new_end, this_rt->tombstone().tomb);
            cur_start = other_rt->start_bound();
        }
        if (cmp_rt(cur_end, other_rt->end_bound())) {
            if (this_rt->tombstone().tomb > other_rt->tombstone().tomb) {
                diff.apply(s, cur_start, cur_end, this_rt->tombstone().tomb);
            }
            advance_this_rt();
        } else {
            auto end = other_rt->end_bound();
            if (this_rt->tombstone().tomb > other_rt->tombstone().tomb) {
                diff.apply(s, cur_start, end, this_rt->tombstone().tomb);
            }
            cur_start = bound_view(end.prefix(), invert_kind(end.kind()));
            ++other_rt;
            if (cmp_rt(cur_end, cur_start)) {
                advance_this_rt();
            }
        }
    }
    while (this_rt != end()) {
        diff.apply(s, cur_start, cur_end, this_rt->tombstone().tomb);
        advance_this_rt();
    }
    return diff;
}

stop_iteration range_tombstone_list::clear_gently() noexcept {
    auto del = current_deleter<range_tombstone_entry>();
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
        apply(s, rt.tombstone());
    }
}

// See reversibly_mergeable.hh
range_tombstone_list::reverter range_tombstone_list::apply_reversibly(const schema& s, range_tombstone_list& rt_list) {
    reverter rev(s, *this);
    for (auto&& rt : rt_list) {
        apply_reversibly(s, rt.tombstone().start, rt.tombstone().start_kind, rt.tombstone().end, rt.tombstone().end_kind, rt.tombstone().tomb, rev);
    }
    return rev;
}

namespace {
struct bv_order_by_end {
    bound_view::compare less;
    bv_order_by_end(const schema& s) : less(s) {}
    bool operator()(bound_view v, const range_tombstone_entry& rt) const { return less(v, rt.end_bound()); }
    bool operator()(const range_tombstone_entry& rt, bound_view v) const { return less(rt.end_bound(), v); }
};
struct bv_order_by_start {
    bound_view::compare less;
    bv_order_by_start(const schema& s) : less(s) {}
    bool operator()(bound_view v, const range_tombstone_entry& rt) const { return less(v, rt.start_bound()); }
    bool operator()(const range_tombstone_entry& rt, bound_view v) const { return less(rt.start_bound(), v); }
};

struct pos_order_by_end {
    position_in_partition::less_compare less;
    pos_order_by_end(const schema& s) : less(s) {}
    bool operator()(position_in_partition_view v, const range_tombstone_entry& rt) const { return less(v, rt.end_position()); }
    bool operator()(const range_tombstone_entry& rt, position_in_partition_view v) const { return less(rt.end_position(), v); }
};
struct pos_order_by_start {
    position_in_partition::less_compare less;
    pos_order_by_start(const schema& s) : less(s) {}
    bool operator()(position_in_partition_view v, const range_tombstone_entry& rt) const { return less(v, rt.position()); }
    bool operator()(const range_tombstone_entry& rt, position_in_partition_view v) const { return less(rt.position(), v); }
};
} // namespace

range_tombstone_list::iterator_range
range_tombstone_list::slice(const schema& s, const query::clustering_range& r) const {
    auto bv_range = bound_view::from_range(r);
    return boost::make_iterator_range(
        _tombstones.lower_bound(bv_range.first, bv_order_by_end{s}),
        _tombstones.upper_bound(bv_range.second, bv_order_by_start{s}));
}

range_tombstone_list::iterator_range
range_tombstone_list::slice(const schema& s, position_in_partition_view start, position_in_partition_view end) const {
    return boost::make_iterator_range(
        _tombstones.upper_bound(start, pos_order_by_end{s}), // end_position() is exclusive, hence upper_bound()
        _tombstones.lower_bound(end, pos_order_by_start{s}));
}

range_tombstone_list::iterator_range
range_tombstone_list::lower_slice(const schema& s, bound_view start, position_in_partition_view before) const {
    return boost::make_iterator_range(
        _tombstones.lower_bound(start, bv_order_by_end{s}),
        _tombstones.lower_bound(before, pos_order_by_end{s}));
}

range_tombstone_list::iterator_range
range_tombstone_list::upper_slice(const schema& s, position_in_partition_view after, bound_view end) const {
    return boost::make_iterator_range(
        _tombstones.upper_bound(after, pos_order_by_start{s}),
        _tombstones.upper_bound(end, bv_order_by_start{s}));
}


range_tombstone_list::iterator
range_tombstone_list::erase(const_iterator a, const_iterator b) {
    return _tombstones.erase_and_dispose(a, b, current_deleter<range_tombstone_entry>());
}

void range_tombstone_list::trim(const schema& s, const query::clustering_row_ranges& ranges) {
    range_tombstone_list list(s);
    bound_view::compare less(s);
    for (auto&& range : ranges) {
        auto start = bound_view::from_range_start(range);
        auto end = bound_view::from_range_end(range);
        for (const auto& rt : slice(s, range)) {
            list.apply(s, range_tombstone(
                std::max(rt.start_bound(), start, less),
                std::min(rt.end_bound(), end, less),
                rt.tombstone().tomb));
        }
    }
    *this = std::move(list);
}

range_tombstone_list::range_tombstones_type::iterator
range_tombstone_list::reverter::insert(range_tombstones_type::iterator it, range_tombstone_entry& new_rt) {
    _ops.emplace_back(insert_undo_op(new_rt));
    return _dst._tombstones.insert_before(it, new_rt);
}

range_tombstone_list::range_tombstones_type::iterator
range_tombstone_list::reverter::erase(range_tombstones_type::iterator it) {
    _ops.emplace_back(std::in_place_type<erase_undo_op>, *it);
    return _dst._tombstones.erase(it);
}

void range_tombstone_list::reverter::update(range_tombstones_type::iterator it, range_tombstone&& new_rt) {
    _ops.emplace_back(std::in_place_type<update_undo_op>, std::move(it->tombstone()), *it);
    it->tombstone() = std::move(new_rt);
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
range_tombstone_list::nop_reverter::insert(range_tombstones_type::iterator it, range_tombstone_entry& new_rt) {
    return _dst._tombstones.insert_before(it, new_rt);
}

range_tombstone_list::range_tombstones_type::iterator
range_tombstone_list::nop_reverter::erase(range_tombstones_type::iterator it) {
    return _dst._tombstones.erase_and_dispose(it, alloc_strategy_deleter<range_tombstone_entry>());
}

void range_tombstone_list::nop_reverter::update(range_tombstones_type::iterator it, range_tombstone&& new_rt) {
    *it = std::move(new_rt);
}

void range_tombstone_list::insert_undo_op::undo(const schema& s, range_tombstone_list& rt_list) noexcept {
    auto it = rt_list.find(s, _new_rt);
    SCYLLA_ASSERT (it != rt_list.end());
    rt_list._tombstones.erase_and_dispose(it, current_deleter<range_tombstone_entry>());
}

void range_tombstone_list::erase_undo_op::undo(const schema& s, range_tombstone_list& rt_list) noexcept {
    rt_list._tombstones.insert(*_rt.release());
}

void range_tombstone_list::update_undo_op::undo(const schema& s, range_tombstone_list& rt_list) noexcept {
    auto it = rt_list.find(s, _new_rt);
    SCYLLA_ASSERT (it != rt_list.end());
    *it = std::move(_old_rt);
}

bool range_tombstone_list::equal(const schema& s, const range_tombstone_list& other) const {
    return boost::equal(_tombstones, other._tombstones, [&s] (auto&& rt1, auto&& rt2) {
        return rt1.tombstone().equal(s, rt2.tombstone());
    });
}

stop_iteration range_tombstone_list::apply_monotonically(const schema& s, range_tombstone_list&& list, is_preemptible preemptible) {
    auto del = current_deleter<range_tombstone_entry>();
    auto it = list.begin();
    while (it != list.end()) {
        // FIXME: Optimize by stealing the entry
        apply_monotonically(s, it->tombstone());
        it = list._tombstones.erase_and_dispose(it, del);
        if (preemptible && need_preempt()) {
            return stop_iteration::no;
        }
    }
    return stop_iteration::yes;
}

void range_tombstone_list::apply_monotonically(const schema& s, const range_tombstone_list& list) {
    for (auto&& rt : list) {
        apply_monotonically(s, rt.tombstone());
    }
}

void range_tombstone_list::apply_monotonically(const schema& s, const range_tombstone& rt) {
    // FIXME: Optimize given this has relaxed exception guarantees.
    // Note that apply() doesn't have monotonic guarantee because it doesn't restore erased entries.
    reverter rev(s, *this);
    apply_reversibly(s, rt.start, rt.start_kind, rt.end, rt.end_kind, rt.tomb, rev);
    rev.cancel();
}
