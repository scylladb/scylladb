/*
 * Copyright (C) 2016-present ScyllaDB
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

#pragma once

#include <seastar/util/defer.hh>
#include "range_tombstone.hh"
#include "query-request.hh"
#include "position_in_partition.hh"
#include "utils/preempt.hh"
#include <iosfwd>
#include <variant>

class range_tombstone_list final {
    using range_tombstones_type = range_tombstone::container_type;
    class insert_undo_op {
        const range_tombstone& _new_rt;
    public:
        insert_undo_op(const range_tombstone& new_rt)
                : _new_rt(new_rt) { }
        void undo(const schema& s, range_tombstone_list& rt_list) noexcept;
    };
    class erase_undo_op {
        alloc_strategy_unique_ptr<range_tombstone> _rt;
    public:
        erase_undo_op(range_tombstone& rt)
                : _rt(&rt) { }
        void undo(const schema& s, range_tombstone_list& rt_list) noexcept;
    };
    class update_undo_op {
        range_tombstone _old_rt;
        const range_tombstone& _new_rt;
    public:
        update_undo_op(range_tombstone&& old_rt, const range_tombstone& new_rt)
                : _old_rt(std::move(old_rt)), _new_rt(new_rt) { }
        void undo(const schema& s, range_tombstone_list& rt_list) noexcept;
    };
    class reverter {
    private:
        using op = std::variant<erase_undo_op, insert_undo_op, update_undo_op>;
        std::vector<op> _ops;
        const schema& _s;
    protected:
        range_tombstone_list& _dst;
    public:
        reverter(const schema& s, range_tombstone_list& dst)
                : _s(s)
                , _dst(dst) { }
        virtual ~reverter() {
            revert();
        }
        reverter(reverter&&) = default;
        reverter& operator=(reverter&&) = default;
        reverter(const reverter&) = delete;
        reverter& operator=(reverter&) = delete;
        virtual range_tombstones_type::iterator insert(range_tombstones_type::iterator it, range_tombstone& new_rt);
        virtual range_tombstones_type::iterator erase(range_tombstones_type::iterator it);
        virtual void update(range_tombstones_type::iterator it, range_tombstone&& new_rt);
        void revert() noexcept;
        void cancel() noexcept {
            _ops.clear();
        }
    };
    class nop_reverter : public reverter {
    public:
        nop_reverter(const schema& s, range_tombstone_list& rt_list)
                : reverter(s, rt_list) { }
        virtual range_tombstones_type::iterator insert(range_tombstones_type::iterator it, range_tombstone& new_rt) override;
        virtual range_tombstones_type::iterator erase(range_tombstones_type::iterator it) override;
        virtual void update(range_tombstones_type::iterator it, range_tombstone&& new_rt) override;
    };
private:
    range_tombstones_type _tombstones;
public:
    // ForwardIterator<range_tombstone>
    using iterator = range_tombstones_type::iterator;
    using const_iterator = range_tombstones_type::const_iterator;

    struct copy_comparator_only { };
    range_tombstone_list(const schema& s)
        : _tombstones(range_tombstone::compare(s))
    { }
    range_tombstone_list(const range_tombstone_list& x, copy_comparator_only)
        : _tombstones(x._tombstones.key_comp())
    { }
    range_tombstone_list(const range_tombstone_list&);
    range_tombstone_list& operator=(range_tombstone_list&) = delete;
    range_tombstone_list(range_tombstone_list&&) = default;
    range_tombstone_list& operator=(range_tombstone_list&&) = default;
    ~range_tombstone_list();
    size_t size() const {
        return _tombstones.size();
    }
    bool empty() const {
        return _tombstones.empty();
    }
    auto begin() {
        return _tombstones.begin();
    }
    auto begin() const {
        return _tombstones.begin();
    }
    auto rbegin() {
        return _tombstones.rbegin();
    }
    auto rbegin() const {
        return _tombstones.rbegin();
    }
    auto end() {
        return _tombstones.end();
    }
    auto end() const {
        return _tombstones.end();
    }
    auto rend() {
        return _tombstones.rend();
    }
    auto rend() const {
        return _tombstones.rend();
    }
    void apply(const schema& s, const bound_view& start_bound, const bound_view& end_bound, tombstone tomb) {
        apply(s, start_bound.prefix(), start_bound.kind(), end_bound.prefix(), end_bound.kind(), std::move(tomb));
    }
    void apply(const schema& s, const range_tombstone& rt) {
        apply(s, rt.start, rt.start_kind, rt.end, rt.end_kind, rt.tomb);
    }
    void apply(const schema& s, range_tombstone&& rt) {
        apply(s, std::move(rt.start), rt.start_kind, std::move(rt.end), rt.end_kind, std::move(rt.tomb));
    }
    void apply(const schema& s, clustering_key_prefix start, bound_kind start_kind,
               clustering_key_prefix end, bound_kind end_kind, tombstone tomb) {
        nop_reverter rev(s, *this);
        apply_reversibly(s, std::move(start), start_kind, std::move(end), end_kind, std::move(tomb), rev);
    }
    // Monotonic exception guarantees. In case of failure the object will contain at least as much information as before the call.
    void apply_monotonically(const schema& s, const range_tombstone& rt);
    // Merges another list with this object.
    // Monotonic exception guarantees. In case of failure the object will contain at least as much information as before the call.
    void apply_monotonically(const schema& s, const range_tombstone_list& list);
    /// Merges another list with this object.
    /// The other list must be governed by the same allocator as this object.
    ///
    /// Monotonic exception guarantees. In case of failure the object will contain at least as much information as before the call.
    /// The other list will be left in a state such that it would still commute with this object to the same state as it
    /// would if the call didn't fail.
    stop_iteration apply_monotonically(const schema& s, range_tombstone_list&& list, is_preemptible = is_preemptible::no);
public:
    tombstone search_tombstone_covering(const schema& s, const clustering_key_prefix& key) const;

    using iterator_range = boost::iterator_range<const_iterator>;
    // Returns range of tombstones which overlap with given range
    iterator_range slice(const schema& s, const query::clustering_range&) const;
    // Returns range tombstones which overlap with [start, end)
    iterator_range slice(const schema& s, position_in_partition_view start, position_in_partition_view end) const;
    iterator_range upper_slice(const schema& s, position_in_partition_view start, bound_view end) const;
    iterator erase(const_iterator, const_iterator);

    // Pops the first element and bans (in theory) further additions
    range_tombstone* pop_front_and_lock() {
        return _tombstones.unlink_leftmost_without_rebalance();
    }

    // Ensures that every range tombstone is strictly contained within given clustering ranges.
    // Preserves all information which may be relevant for rows from that ranges.
    void trim(const schema& s, const query::clustering_row_ranges&);
    range_tombstone_list difference(const schema& s, const range_tombstone_list& rt_list) const;
    // Erases the range tombstones for which filter returns true.
    template <typename Pred>
    void erase_where(Pred filter) {
        static_assert(std::is_same<bool, std::result_of_t<Pred(const range_tombstone&)>>::value,
                      "bad Pred signature");
        auto it = begin();
        while (it != end()) {
            if (filter(*it)) {
                it = _tombstones.erase_and_dispose(it, current_deleter<range_tombstone>());
            } else {
                ++it;
            }
        }
    }
    void clear() {
        _tombstones.clear_and_dispose(current_deleter<range_tombstone>());
    }

    template <typename T>
    requires std::constructible_from<T, range_tombstone>
    auto pop_as(iterator it) {
        range_tombstone& rt = *it;
        _tombstones.erase(it);
        auto rt_deleter = seastar::defer([&rt] { current_deleter<range_tombstone>()(&rt); });
        return T(std::move(rt));
    }

    // Removes elements of this list in batches.
    // Returns stop_iteration::yes iff there is no more elements to remove.
    stop_iteration clear_gently() noexcept;
    void apply(const schema& s, const range_tombstone_list& rt_list);
    // See reversibly_mergeable.hh
    reverter apply_reversibly(const schema& s, range_tombstone_list& rt_list);

    friend std::ostream& operator<<(std::ostream& out, const range_tombstone_list&);
    bool equal(const schema&, const range_tombstone_list&) const;
    size_t external_memory_usage(const schema& s) const {
        size_t result = 0;
        for (auto& rtb : _tombstones) {
            result += rtb.memory_usage(s);
        }
        return result;
    }
private:
    void apply_reversibly(const schema& s, clustering_key_prefix start, bound_kind start_kind,
                          clustering_key_prefix end, bound_kind end_kind, tombstone tomb, reverter& rev);
    void insert_from(const schema& s, range_tombstones_type::iterator it, clustering_key_prefix start,
                     bound_kind start_kind, clustering_key_prefix end, bound_kind end_kind, tombstone tomb, reverter& rev);
    range_tombstones_type::iterator find(const schema& s, const range_tombstone& rt);
};
