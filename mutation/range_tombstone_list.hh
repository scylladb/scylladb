/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/defer.hh>
#include "range_tombstone.hh"
#include "query-request.hh"
#include "utils/assert.hh"
#include "utils/preempt.hh"
#include "utils/chunked_vector.hh"
#include <variant>

class position_in_partition_view;

class range_tombstone_entry {
    range_tombstone _tombstone;
    bi::set_member_hook<bi::link_mode<bi::auto_unlink>> _link;

public:
    struct compare {
        range_tombstone::compare _c;
        compare(const schema& s) : _c(s) {}
        bool operator()(const range_tombstone_entry& rt1, const range_tombstone_entry& rt2) const {
            return _c(rt1._tombstone, rt2._tombstone);
        }
    };

    using container_type = bi::set<range_tombstone_entry,
            bi::member_hook<range_tombstone_entry, bi::set_member_hook<bi::link_mode<bi::auto_unlink>>, &range_tombstone_entry::_link>,
            bi::compare<range_tombstone_entry::compare>,
            bi::constant_time_size<false>>;

    range_tombstone_entry(const range_tombstone_entry& rt)
        : _tombstone(rt._tombstone)
    {
    }

    range_tombstone_entry(range_tombstone_entry&& rt) noexcept
            : _tombstone(std::move(rt._tombstone))
    {
        update_node(rt._link);
    }
    range_tombstone_entry(range_tombstone&& rt) noexcept
        : _tombstone(std::move(rt))
    { }

    range_tombstone_entry& operator=(range_tombstone_entry&& rt) noexcept {
        update_node(rt._link);
        _tombstone = std::move(rt._tombstone);
        return *this;
    }

    range_tombstone& tombstone() noexcept { return _tombstone; }
    const range_tombstone& tombstone() const noexcept { return _tombstone; }

    const bound_view start_bound() const { return _tombstone.start_bound(); }
    const bound_view end_bound() const { return _tombstone.end_bound(); }
    position_in_partition_view position() const { return _tombstone.position(); }
    position_in_partition_view end_position() const { return _tombstone.end_position(); }

    size_t memory_usage(const schema& s) const noexcept {
        return sizeof(range_tombstone_entry) + _tombstone.external_memory_usage(s);
    }

private:
    void update_node(bi::set_member_hook<bi::link_mode<bi::auto_unlink>>& other_link) noexcept {
        if (other_link.is_linked()) {
            // Move the link in case we're being relocated by LSA.
            container_type::node_algorithms::replace_node(other_link.this_ptr(), _link.this_ptr());
            container_type::node_algorithms::init(other_link.this_ptr());
        }
    }
};

template <>
struct fmt::formatter<range_tombstone_entry> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const range_tombstone_entry& rt, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", rt.tombstone());
    }
};

class range_tombstone_list final {
    using range_tombstones_type = range_tombstone_entry::container_type;
    class insert_undo_op {
        const range_tombstone_entry& _new_rt;
    public:
        insert_undo_op(const range_tombstone_entry& new_rt)
                : _new_rt(new_rt) { }
        void undo(const schema& s, range_tombstone_list& rt_list) noexcept;
    };
    class erase_undo_op {
        alloc_strategy_unique_ptr<range_tombstone_entry> _rt;
    public:
        erase_undo_op(range_tombstone_entry& rt)
                : _rt(&rt) { }
        void undo(const schema& s, range_tombstone_list& rt_list) noexcept;
    };
    class update_undo_op {
        range_tombstone _old_rt;
        const range_tombstone_entry& _new_rt;
    public:
        update_undo_op(range_tombstone&& old_rt, const range_tombstone_entry& new_rt)
                : _old_rt(std::move(old_rt)), _new_rt(new_rt) { }
        void undo(const schema& s, range_tombstone_list& rt_list) noexcept;
    };
    class reverter {
    private:
        using op = std::variant<erase_undo_op, insert_undo_op, update_undo_op>;
        utils::chunked_vector<op> _ops;
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
        reverter(const reverter&) = delete;
        reverter& operator=(reverter&) = delete;
        virtual range_tombstones_type::iterator insert(range_tombstones_type::iterator it, range_tombstone_entry& new_rt);
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
        virtual range_tombstones_type::iterator insert(range_tombstones_type::iterator it, range_tombstone_entry& new_rt) override;
        virtual range_tombstones_type::iterator erase(range_tombstones_type::iterator it) override;
        virtual void update(range_tombstones_type::iterator it, range_tombstone&& new_rt) override;
    };
private:
    range_tombstones_type _tombstones;
public:
    // ForwardIterator<range_tombstone>
    using iterator = range_tombstones_type::iterator;
    using reverse_iterator = range_tombstones_type::reverse_iterator;
    using const_iterator = range_tombstones_type::const_iterator;

    struct copy_comparator_only { };
    range_tombstone_list(const schema& s)
        : _tombstones(range_tombstone_entry::compare(s))
    { }
    range_tombstone_list(const range_tombstone_list& x, copy_comparator_only)
        : _tombstones(x._tombstones.key_comp())
    { }
    range_tombstone_list(const range_tombstone_list&);
    range_tombstone_list& operator=(range_tombstone_list&) = delete;
    range_tombstone_list(range_tombstone_list&&) = default;
    range_tombstone_list& operator=(range_tombstone_list&&) = default;
    ~range_tombstone_list();
    size_t size() const noexcept {
        return _tombstones.size();
    }
    bool empty() const noexcept {
        return _tombstones.empty();
    }
    auto begin() noexcept {
        return _tombstones.begin();
    }
    auto begin() const noexcept {
        return _tombstones.begin();
    }
    auto rbegin() noexcept {
        return _tombstones.rbegin();
    }
    auto rbegin() const noexcept {
        return _tombstones.rbegin();
    }
    auto end() noexcept {
        return _tombstones.end();
    }
    auto end() const noexcept {
        return _tombstones.end();
    }
    auto rend() noexcept {
        return _tombstones.rend();
    }
    auto rend() const noexcept {
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
    // Returns range tombstones which overlap with given range
    iterator_range slice(const schema& s, const query::clustering_range&) const;
    // Returns range tombstones which overlap with [start, end)
    iterator_range slice(const schema& s, position_in_partition_view start, position_in_partition_view end) const;

    // Returns range tombstones with ends inside [start, before).
    iterator_range lower_slice(const schema& s, bound_view start, position_in_partition_view before) const;
    // Returns range tombstones with starts inside (after, end].
    iterator_range upper_slice(const schema& s, position_in_partition_view after, bound_view end) const;

    iterator erase(const_iterator, const_iterator);

    // Pops the first element and bans (in theory) further additions
    // The list is assumed not to be empty
    range_tombstone pop_front_and_lock() {
        range_tombstone_entry* rt = _tombstones.unlink_leftmost_without_rebalance();
        SCYLLA_ASSERT(rt != nullptr);
        auto _ = seastar::defer([rt] () noexcept { current_deleter<range_tombstone_entry>()(rt); });
        return std::move(rt->tombstone());
    }

    // Ensures that every range tombstone is strictly contained within given clustering ranges.
    // Preserves all information which may be relevant for rows from that ranges.
    void trim(const schema& s, const query::clustering_row_ranges&);
    range_tombstone_list difference(const schema& s, const range_tombstone_list& rt_list) const;
    // Erases the range tombstones for which filter returns true.
    template <typename Pred>
    requires std::is_invocable_r_v<bool, Pred, const range_tombstone&>
    void erase_where(Pred filter) {
        auto it = begin();
        while (it != end()) {
            if (filter(it->tombstone())) {
                it = _tombstones.erase_and_dispose(it, current_deleter<range_tombstone_entry>());
            } else {
                ++it;
            }
        }
    }
    void clear() noexcept {
        _tombstones.clear_and_dispose(current_deleter<range_tombstone_entry>());
    }

    range_tombstone pop(iterator it) {
        range_tombstone rt(std::move(it->tombstone()));
        _tombstones.erase_and_dispose(it, current_deleter<range_tombstone_entry>());
        return rt;
    }

    // Removes elements of this list in batches.
    // Returns stop_iteration::yes iff there is no more elements to remove.
    stop_iteration clear_gently() noexcept;
    void apply(const schema& s, const range_tombstone_list& rt_list);
    // See reversibly_mergeable.hh
    reverter apply_reversibly(const schema& s, range_tombstone_list& rt_list);

    bool equal(const schema&, const range_tombstone_list&) const;
    size_t external_memory_usage(const schema& s) const noexcept {
        size_t result = 0;
        for (auto& rtb : _tombstones) {
            result += rtb.tombstone().memory_usage(s);
        }
        return result;
    }
private:
    void apply_reversibly(const schema& s, clustering_key_prefix start, bound_kind start_kind,
                          clustering_key_prefix end, bound_kind end_kind, tombstone tomb, reverter& rev);

    void insert_from(const schema& s,
                     range_tombstones_type::iterator it,
                     position_in_partition start,
                     position_in_partition end,
                     tombstone tomb,
                     reverter& rev);

    range_tombstones_type::iterator find(const schema& s, const range_tombstone_entry& rt);
};

template <>
struct fmt::formatter<range_tombstone_list> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const range_tombstone_list& list, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{{}}}", fmt::join(list, ", "));
    }
};
