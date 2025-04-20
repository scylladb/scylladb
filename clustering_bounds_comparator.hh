
/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <functional>
#include "keys.hh"
#include "schema/schema_fwd.hh"
#include "interval.hh"

/**
 * Represents the kind of bound in a range tombstone.
 */
enum class bound_kind : uint8_t {
    excl_end = 0,
    incl_start = 1,
    // values 2 to 5 are reserved for forward Origin compatibility
    incl_end = 6,
    excl_start = 7,
};

// Swaps start <-> end && incl <-> excl
bound_kind invert_kind(bound_kind k);
// Swaps start <-> end
bound_kind reverse_kind(bound_kind k);
int32_t weight(bound_kind k);

class bound_view {
    const static thread_local clustering_key _empty_prefix;
    std::reference_wrapper<const clustering_key_prefix> _prefix;
    bound_kind _kind;
public:
    bound_view(const clustering_key_prefix& prefix, bound_kind kind)
        : _prefix(prefix)
        , _kind(kind)
    { }
    bound_view(const bound_view& other) noexcept = default;
    bound_view& operator=(const bound_view& other) noexcept = default;

    bound_kind kind() const { return _kind; }
    const clustering_key_prefix& prefix() const { return _prefix; }

    struct tri_compare {
        // To make it assignable and to avoid taking a schema_ptr, we
        // wrap the schema reference.
        std::reference_wrapper<const schema> _s;
        tri_compare(const schema& s) : _s(s)
        { }
        std::strong_ordering operator()(const clustering_key_prefix& p1, int32_t w1, const clustering_key_prefix& p2, int32_t w2) const {
            auto type = _s.get().clustering_key_prefix_type();
            auto res = prefix_equality_tri_compare(type->types().begin(),
                type->begin(p1.representation()), type->end(p1.representation()),
                type->begin(p2.representation()), type->end(p2.representation()),
                ::tri_compare);
            if (res != 0) {
                return res;
            }
            auto d1 = p1.size(_s);
            auto d2 = p2.size(_s);

            /*
             * The logic below is
             *
             * if d1 == d2 { return w1 <=> w2 }
             * if d1 < d2  { if w1 <= 0 return less else return greater
             * if d1 > d2  { if w2 <= 0 return greater else return less
             *
             * Those w vs 0 checks are effectively { w <=> 0.5 } which is the same
             * as { 2*w <=> 1 }. Next, w1 <=> w2 is equivalent to 2*w1 <=> 2*w2, so
             * all three branches can be collapsed into a single <=>.
             *
             * This looks like veiling the above ifs for no reason, but it really
             * helps compiler generate jump-less assembly.
             */

            return ((d1 <= d2) ? w1 << 1 : 1) <=> ((d2 <= d1) ? w2 << 1 : 1);
        }
        std::strong_ordering operator()(const bound_view b, const clustering_key_prefix& p) const {
            return operator()(b._prefix, weight(b._kind), p, 0);
        }
        std::strong_ordering operator()(const clustering_key_prefix& p, const bound_view b) const {
            return operator()(p, 0, b._prefix, weight(b._kind));
        }
        std::strong_ordering operator()(const bound_view b1, const bound_view b2) const {
            return operator()(b1._prefix, weight(b1._kind), b2._prefix, weight(b2._kind));
        }
    };
    struct compare {
        // To make it assignable and to avoid taking a schema_ptr, we
        // wrap the schema reference.
        tri_compare _cmp;
        compare(const schema& s) : _cmp(s)
        { }
        bool operator()(const clustering_key_prefix& p1, int32_t w1, const clustering_key_prefix& p2, int32_t w2) const {
            return _cmp(p1, w1, p2, w2) < 0;
        }
        bool operator()(const bound_view b, const clustering_key_prefix& p) const {
            return operator()(b._prefix, weight(b._kind), p, 0);
        }
        bool operator()(const clustering_key_prefix& p, const bound_view b) const {
            return operator()(p, 0, b._prefix, weight(b._kind));
        }
        bool operator()(const bound_view b1, const bound_view b2) const {
            return operator()(b1._prefix, weight(b1._kind), b2._prefix, weight(b2._kind));
        }
    };
    bool equal(const schema& s, const bound_view other) const {
        return _kind == other._kind && _prefix.get().equal(s, other._prefix.get());
    }
    bool adjacent(const schema& s, const bound_view other) const {
        return invert_kind(other._kind) == _kind && _prefix.get().equal(s, other._prefix.get());
    }
    static bound_view bottom() {
        return {_empty_prefix, bound_kind::incl_start};
    }
    static bound_view top() {
        return {_empty_prefix, bound_kind::incl_end};
    }
    template<template<typename> typename R>
    requires Interval<R, clustering_key_prefix_view>
    static bound_view from_range_start(const R<clustering_key_prefix>& range) {
        return range.start()
               ? bound_view(range.start()->value(), range.start()->is_inclusive() ? bound_kind::incl_start : bound_kind::excl_start)
               : bottom();
    }
    template<template<typename> typename R>
    requires Interval<R, clustering_key_prefix>
    static bound_view from_range_end(const R<clustering_key_prefix>& range) {
        return range.end()
               ? bound_view(range.end()->value(), range.end()->is_inclusive() ? bound_kind::incl_end : bound_kind::excl_end)
               : top();
    }
    template<template<typename> typename R>
    requires Interval<R, clustering_key_prefix>
    static std::pair<bound_view, bound_view> from_range(const R<clustering_key_prefix>& range) {
        return {from_range_start(range), from_range_end(range)};
    }
    template<template<typename> typename R>
    requires Interval<R, clustering_key_prefix_view>
    static std::optional<typename R<clustering_key_prefix_view>::bound> to_interval_bound(const bound_view& bv) {
        if (&bv._prefix.get() == &_empty_prefix) {
            return {};
        }
        bool inclusive = bv._kind != bound_kind::excl_end && bv._kind != bound_kind::excl_start;
        return {typename R<clustering_key_prefix_view>::bound(bv._prefix.get().view(), inclusive)};
    }
    friend fmt::formatter<bound_view>;
};

template <> struct fmt::formatter<bound_kind> : fmt::formatter<string_view> {
    auto format(bound_kind, fmt::format_context& ctx) const -> decltype(ctx.out());
};
template <> struct fmt::formatter<bound_view> : fmt::formatter<string_view> {
    auto format(const bound_view& b, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{bound: prefix={},kind={}}}", b._prefix.get(), b._kind);
    }
};
