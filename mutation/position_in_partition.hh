/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "types/types.hh"
#include "keys.hh"
#include "clustering_bounds_comparator.hh"
#include "query-request.hh"

#include <optional>
#include <cstdlib>

inline
lexicographical_relation relation_for_lower_bound(composite_view v) {
    switch (v.last_eoc()) {
        case composite::eoc::start:
        case composite::eoc::none:
            return lexicographical_relation::before_all_prefixed;
        case composite::eoc::end:
            return lexicographical_relation::after_all_prefixed;
    }
    abort();
}

inline
lexicographical_relation relation_for_upper_bound(composite_view v) {
    switch (v.last_eoc()) {
        case composite::eoc::start:
            return lexicographical_relation::before_all_prefixed;
        case composite::eoc::none:
            return lexicographical_relation::before_all_strictly_prefixed;
        case composite::eoc::end:
            return lexicographical_relation::after_all_prefixed;
    }
    abort();
}

enum class bound_weight : int8_t {
    before_all_prefixed = -1,
    equal = 0,
    after_all_prefixed = 1,
};

inline
bound_weight reversed(bound_weight w) {
    switch (w) {
        case bound_weight::equal:
            return w;
        case bound_weight::before_all_prefixed:
            return bound_weight::after_all_prefixed;
        case bound_weight::after_all_prefixed:
            return bound_weight::before_all_prefixed;
    }
    std::abort();
}

inline
bound_weight position_weight(bound_kind k) {
    switch (k) {
    case bound_kind::excl_end:
    case bound_kind::incl_start:
        return bound_weight::before_all_prefixed;
    case bound_kind::incl_end:
    case bound_kind::excl_start:
        return bound_weight::after_all_prefixed;
    }
    abort();
}

enum class partition_region : uint8_t {
    partition_start,
    static_row,
    clustered,
    partition_end,
};

struct view_and_holder;

template <>
struct fmt::formatter<partition_region> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const ::partition_region& r, FormatContext& ctx) const {
        switch (r) {
            case partition_region::partition_start:
                return formatter<string_view>::format("partition_start", ctx);
            case partition_region::static_row:
                return formatter<string_view>::format("static_row", ctx);
            case partition_region::clustered:
                return formatter<string_view>::format("clustered", ctx);
            case partition_region::partition_end:
                return formatter<string_view>::format("partition_end", ctx);
        }
        std::abort(); // compiler will error before we reach here
    }
};

partition_region parse_partition_region(std::string_view);

class position_in_partition_view {
    friend class position_in_partition;

    partition_region _type;
    bound_weight _bound_weight = bound_weight::equal;
    const clustering_key_prefix* _ck; // nullptr when _type != clustered
public:
    position_in_partition_view(partition_region type, bound_weight weight, const clustering_key_prefix* ck)
        : _type(type)
        , _bound_weight(weight)
        , _ck(ck)
    { }
    bool is_before_key() const {
        return _bound_weight == bound_weight::before_all_prefixed;
    }
    bool is_after_key() const {
        return _bound_weight == bound_weight::after_all_prefixed;
    }
private:
    // Returns placement of this position_in_partition relative to *_ck,
    // or lexicographical_relation::at_prefix if !_ck.
    lexicographical_relation relation() const {
        // FIXME: Currently position_range cannot represent a range end bound which
        // includes just the prefix key or a range start which excludes just a prefix key.
        // In both cases we should return lexicographical_relation::before_all_strictly_prefixed here.
        // Refs #1446.
        if (_bound_weight == bound_weight::after_all_prefixed) {
            return lexicographical_relation::after_all_prefixed;
        } else {
            return lexicographical_relation::before_all_prefixed;
        }
    }
public:
    struct partition_start_tag_t { };
    struct end_of_partition_tag_t { };
    struct static_row_tag_t { };
    struct clustering_row_tag_t { };
    struct range_tag_t { };
    using range_tombstone_tag_t = range_tag_t;

    explicit position_in_partition_view(partition_start_tag_t) : _type(partition_region::partition_start), _ck(nullptr) { }
    explicit position_in_partition_view(end_of_partition_tag_t) : _type(partition_region::partition_end), _ck(nullptr) { }
    explicit position_in_partition_view(static_row_tag_t) : _type(partition_region::static_row), _ck(nullptr) { }
    position_in_partition_view(clustering_row_tag_t, const clustering_key_prefix& ck)
        : _type(partition_region::clustered), _ck(&ck) { }
    position_in_partition_view(const clustering_key_prefix& ck)
        : _type(partition_region::clustered), _ck(&ck) { }
    position_in_partition_view(range_tag_t, bound_view bv)
        : _type(partition_region::clustered), _bound_weight(position_weight(bv.kind())), _ck(&bv.prefix()) { }
    position_in_partition_view(const clustering_key_prefix& ck, bound_weight w)
        : _type(partition_region::clustered), _bound_weight(w), _ck(&ck) { }

    static position_in_partition_view for_range_start(const query::clustering_range& r) {
        return {position_in_partition_view::range_tag_t(), bound_view::from_range_start(r)};
    }

    static position_in_partition_view for_range_end(const query::clustering_range& r) {
        return {position_in_partition_view::range_tag_t(), bound_view::from_range_end(r)};
    }

    static position_in_partition_view before_all_clustered_rows() {
        return {range_tag_t(), bound_view::bottom()};
    }

    static position_in_partition_view after_all_clustered_rows() {
        return {position_in_partition_view::range_tag_t(), bound_view::top()};
    }

    static position_in_partition_view for_partition_start() {
        return position_in_partition_view(partition_start_tag_t());
    }

    static position_in_partition_view for_partition_end() {
        return position_in_partition_view(end_of_partition_tag_t());
    }

    static position_in_partition_view for_static_row() {
        return position_in_partition_view(static_row_tag_t());
    }

    static position_in_partition_view for_key(const clustering_key& ck) {
        return {clustering_row_tag_t(), ck};
    }

    // Returns a view, as the first element of the returned pair, to before_key(pos._ck)
    // if pos.is_clustering_row() else returns pos as-is.
    // The second element of the pair needs to be kept alive as long as the first element is used.
    // The returned view is valid as long as the view passed to this method is valid.
    static view_and_holder after_key(const schema&, position_in_partition_view);

    static position_in_partition_view after_all_prefixed(const clustering_key& ck) {
        return {partition_region::clustered, bound_weight::after_all_prefixed, &ck};
    }

    // Returns a view to after_all_prefixed(pos._ck) if pos.is_clustering_row() else returns pos as-is.
    static position_in_partition_view after_all_prefixed(position_in_partition_view pos) {
        return {partition_region::clustered, pos._bound_weight == bound_weight::equal ? bound_weight::after_all_prefixed : pos._bound_weight, pos._ck};
    }

    static position_in_partition_view before_key(const clustering_key& ck) {
        return {partition_region::clustered, bound_weight::before_all_prefixed, &ck};
    }

    // Returns a view to before_key(pos._ck) if pos.is_clustering_row() else returns pos as-is.
    static position_in_partition_view before_key(position_in_partition_view pos) {
        return {partition_region::clustered, pos._bound_weight == bound_weight::equal ? bound_weight::before_all_prefixed : pos._bound_weight, pos._ck};
    }

    partition_region region() const { return _type; }
    bound_weight get_bound_weight() const { return _bound_weight; }
    bool is_partition_start() const { return _type == partition_region::partition_start; }
    bool is_partition_end() const { return _type == partition_region::partition_end; }
    bool is_static_row() const { return _type == partition_region::static_row; }
    bool is_clustering_row() const { return has_clustering_key() && _bound_weight == bound_weight::equal; }
    bool has_clustering_key() const { return _type == partition_region::clustered; }

    // Returns true if all fragments that can be seen for given schema have
    // positions >= than this. partition_start is ignored.
    bool is_before_all_fragments(const schema& s) const {
        return _type == partition_region::partition_start || _type == partition_region::static_row
               || (_type == partition_region::clustered && !s.has_static_columns() && _bound_weight == bound_weight::before_all_prefixed && key().is_empty(s));
    }

    bool is_after_all_clustered_rows(const schema& s) const {
        return is_partition_end() || (_ck && _ck->is_empty(s) && _bound_weight == bound_weight::after_all_prefixed);
    }

    bool has_key() const { return bool(_ck); }

    // Valid when has_key() == true
    const clustering_key_prefix& key() const {
        return *_ck;
    }

    // Can be called only when !is_static_row && !is_clustering_row().
    bound_view as_start_bound_view() const {
        SCYLLA_ASSERT(_bound_weight != bound_weight::equal);
        return bound_view(*_ck, _bound_weight == bound_weight::before_all_prefixed ? bound_kind::incl_start : bound_kind::excl_start);
    }

    bound_view as_end_bound_view() const {
        SCYLLA_ASSERT(_bound_weight != bound_weight::equal);
        return bound_view(*_ck, _bound_weight == bound_weight::before_all_prefixed ? bound_kind::excl_end : bound_kind::incl_end);
    }

    class printer {
        const schema& _schema;
        const position_in_partition_view& _pipv;
    public:
        printer(const schema& schema, const position_in_partition_view& pipv) : _schema(schema), _pipv(pipv) {}
        friend fmt::formatter<printer>;
    };

    // Create a position which is the same as this one but governed by a schema with reversed clustering key order.
    position_in_partition_view reversed() const {
        return position_in_partition_view(_type, ::reversed(_bound_weight), _ck);
    }

    friend fmt::formatter<printer>;
    friend fmt::formatter<position_in_partition_view>;
    friend bool no_clustering_row_between(const schema&, position_in_partition_view, position_in_partition_view);
};

template <>
struct fmt::formatter<position_in_partition_view> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const ::position_in_partition_view& pos, FormatContext& ctx) const {
        fmt::format_to(ctx.out(), "{{position: {}, ", pos._type);
        if (pos._ck) {
            fmt::format_to(ctx.out(), "{}, ", *pos._ck);
        } else {
            fmt::format_to(ctx.out(), "null, ");
        }
        return fmt::format_to(ctx.out(), "{}}}", int32_t(pos._bound_weight));
    }
};

template <>
struct fmt::formatter<position_in_partition_view::printer> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const ::position_in_partition_view::printer& p, FormatContext& ctx) const {
        auto& pos = p._pipv;
        fmt::format_to(ctx.out(), "{{position: {},", pos._type);
        if (pos._ck) {
            fmt::format_to(ctx.out(), "{}", clustering_key_prefix::with_schema_wrapper(p._schema, *pos._ck));
        } else {
            fmt::format_to(ctx.out(), "null");
        }
        return fmt::format_to(ctx.out(), ", {}}}", int32_t(pos._bound_weight));
    }
};

class position_in_partition {
    partition_region _type;
    bound_weight _bound_weight = bound_weight::equal;
    std::optional<clustering_key_prefix> _ck;
public:
    friend class clustering_interval_set;
    struct partition_start_tag_t { };
    struct end_of_partition_tag_t { };
    struct static_row_tag_t { };
    struct after_static_row_tag_t { };
    struct clustering_row_tag_t { };
    struct after_clustering_row_tag_t { };
    struct before_clustering_row_tag_t { };
    struct range_tag_t { };
    using range_tombstone_tag_t = range_tag_t;
    partition_region get_type() const { return _type; }
    bound_weight get_bound_weight() const { return _bound_weight; }
    const std::optional<clustering_key_prefix>& get_clustering_key_prefix() const { return _ck; }
    position_in_partition(partition_region type, bound_weight weight, std::optional<clustering_key_prefix> ck)
        : _type(type), _bound_weight(weight), _ck(std::move(ck)) { }
    explicit position_in_partition(partition_start_tag_t) : _type(partition_region::partition_start) { }
    explicit position_in_partition(end_of_partition_tag_t) : _type(partition_region::partition_end) { }
    explicit position_in_partition(static_row_tag_t) : _type(partition_region::static_row) { }
    position_in_partition(clustering_row_tag_t, clustering_key_prefix ck)
        : _type(partition_region::clustered), _ck(std::move(ck)) { }
    position_in_partition(after_clustering_row_tag_t, const schema& s, clustering_key_prefix ck)
        : _type(partition_region::clustered)
        , _bound_weight(bound_weight::after_all_prefixed)
        , _ck(std::move(ck))
    {
        if (clustering_key::make_full(s, *_ck)) { // Refs #1446
            _bound_weight = bound_weight::before_all_prefixed;
        }
    }
    position_in_partition(after_clustering_row_tag_t, const schema& s, position_in_partition_view pos)
        : position_in_partition(after_clustering_row_tag_t(), s, position_in_partition(pos))
    { }
    position_in_partition(after_clustering_row_tag_t, const schema& s, position_in_partition&& pos)
        : _type(partition_region::clustered)
        , _bound_weight(pos._bound_weight != bound_weight::equal ? pos._bound_weight : bound_weight::after_all_prefixed)
        , _ck(std::move(pos._ck))
    {
        if (pos._bound_weight == bound_weight::equal && _ck && clustering_key::make_full(s, *_ck)) { // Refs #1446
            _bound_weight = bound_weight::before_all_prefixed;
        }
    }
    position_in_partition(before_clustering_row_tag_t, clustering_key_prefix ck)
        : _type(partition_region::clustered), _bound_weight(bound_weight::before_all_prefixed), _ck(std::move(ck)) { }
    position_in_partition(before_clustering_row_tag_t, position_in_partition_view pos)
        : _type(partition_region::clustered)
        , _bound_weight(pos._bound_weight != bound_weight::equal ? pos._bound_weight : bound_weight::before_all_prefixed)
        , _ck(*pos._ck) { }
    position_in_partition(range_tag_t, bound_view bv)
        : _type(partition_region::clustered), _bound_weight(position_weight(bv.kind())), _ck(bv.prefix()) { }
    position_in_partition(range_tag_t, bound_kind kind, clustering_key_prefix&& prefix)
        : _type(partition_region::clustered), _bound_weight(position_weight(kind)), _ck(std::move(prefix)) { }
    position_in_partition(after_static_row_tag_t) :
        position_in_partition(range_tag_t(), bound_view::bottom()) { }
    explicit position_in_partition(position_in_partition_view view)
        : _type(view._type), _bound_weight(view._bound_weight)
        {
            if (view._ck) {
                _ck = *view._ck;
            }
        }

    // Strong exception guarantees.
    position_in_partition& operator=(position_in_partition_view view) {
        // The copy assignment to _ck can throw (because it allocates),
        // but assignments to _type and _bound_weight can't throw.
        // Thus, to achieve strong exception guarantees,
        // we only need to perform the _ck assignment before others.
        if (view._ck) {
            _ck = *view._ck;
        } else {
            _ck.reset();
        }
        _type = view._type;
        _bound_weight = view._bound_weight;
        return *this;
    }

    static position_in_partition before_all_clustered_rows() {
        return {position_in_partition::range_tag_t(), bound_view::bottom()};
    }

    static position_in_partition after_all_clustered_rows() {
        return {position_in_partition::range_tag_t(), bound_view::top()};
    }

    // If given position is a clustering row position, returns a position
    // right before it. Otherwise, returns it unchanged.
    // The position "pos" must be a clustering position.
    static position_in_partition before_key(position_in_partition_view pos) {
        return {before_clustering_row_tag_t(), pos};
    }

    static position_in_partition before_key(clustering_key ck) {
        return {before_clustering_row_tag_t(), std::move(ck)};
    }

    static position_in_partition after_key(const schema& s, clustering_key ck) {
        return {after_clustering_row_tag_t(), s, std::move(ck)};
    }

    // If given position is a clustering row position, returns a position
    // right after it. Otherwise returns it unchanged.
    // The position "pos" must be a clustering position.
    static position_in_partition after_key(const schema& s, position_in_partition_view pos) {
        return {after_clustering_row_tag_t(), s, pos};
    }

    static position_in_partition after_key(const schema& s, position_in_partition&& pos) noexcept {
        return {after_clustering_row_tag_t(), s, std::move(pos)};
    }

    static position_in_partition for_key(clustering_key ck) {
        return {clustering_row_tag_t(), std::move(ck)};
    }

    static position_in_partition for_partition_start() {
        return position_in_partition{partition_start_tag_t()};
    }

    static position_in_partition for_partition_end() {
        return position_in_partition(end_of_partition_tag_t());
    }

    static position_in_partition for_static_row() {
        return position_in_partition{static_row_tag_t()};
    }

    static position_in_partition min() {
        return for_static_row();
    }

    static position_in_partition for_range_start(const query::clustering_range&);
    static position_in_partition for_range_end(const query::clustering_range&);

    partition_region region() const { return _type; }
    bool is_partition_start() const { return _type == partition_region::partition_start; }
    bool is_partition_end() const { return _type == partition_region::partition_end; }
    bool is_static_row() const { return _type == partition_region::static_row; }
    bool is_clustering_row() const { return has_clustering_key() && _bound_weight == bound_weight::equal; }
    bool has_clustering_key() const { return _type == partition_region::clustered; }

    bool is_after_all_clustered_rows(const schema& s) const {
        return is_partition_end() || (_ck && _ck->is_empty(s) && _bound_weight == bound_weight::after_all_prefixed);
    }

    bool is_before_all_clustered_rows(const schema& s) const {
        return _type < partition_region::clustered
               || (_type == partition_region::clustered && _ck->is_empty(s) && _bound_weight == bound_weight::before_all_prefixed);
    }

    template<typename Hasher>
    void feed_hash(Hasher& hasher, const schema& s) const {
        ::feed_hash(hasher, _bound_weight);
        if (_ck) {
            ::feed_hash(hasher, true);
            ::feed_hash(hasher, *_ck, s);
        } else {
            ::feed_hash(hasher, false);
        }
    }

    bool has_key() const { return bool(_ck); }

    const clustering_key_prefix& key() const {
        return *_ck;
    }
    operator position_in_partition_view() const {
        return { _type, _bound_weight, _ck ? &*_ck : nullptr };
    }

    size_t external_memory_usage() const {
        return _ck ? _ck->external_memory_usage() : 0;
    }

    // Defines total order on the union of position_and_partition and composite objects.
    //
    // The ordering is compatible with position_range (r). The following is satisfied for
    // all cells with name c included by the range:
    //
    //   r.start() <= c < r.end()
    //
    // The ordering on composites given by this is compatible with but weaker than the cell name order.
    //
    // The ordering on position_in_partition given by this is compatible but weaker than the ordering
    // given by position_in_partition::tri_compare.
    //
    class composite_tri_compare {
        const schema& _s;
    public:
        static int rank(partition_region t) {
            return static_cast<int>(t);
        }

        composite_tri_compare(const schema& s) : _s(s) {}

        std::strong_ordering operator()(position_in_partition_view a, position_in_partition_view b) const {
            if (a._type != b._type) {
                return rank(a._type) <=> rank(b._type);
            }
            if (!a._ck) {
                return std::strong_ordering::equal;
            }
            auto&& types = _s.clustering_key_type()->types();
            auto cmp = [&] (const data_type& t, managed_bytes_view c1, managed_bytes_view c2) { return t->compare(c1, c2); };
            return lexicographical_tri_compare(types.begin(), types.end(),
                a._ck->begin(_s), a._ck->end(_s),
                b._ck->begin(_s), b._ck->end(_s),
                cmp, a.relation(), b.relation());
        }

        std::strong_ordering operator()(position_in_partition_view a, composite_view b) const {
            if (b.empty()) {
                return std::strong_ordering::greater; // a cannot be empty.
            }
            partition_region b_type = b.is_static() ? partition_region::static_row : partition_region::clustered;
            if (a._type != b_type) {
                return rank(a._type) <=> rank(b_type);
            }
            if (!a._ck) {
                return std::strong_ordering::equal;
            }
            auto&& types = _s.clustering_key_type()->types();
            auto b_values = b.values();
            auto cmp = [&] (const data_type& t, managed_bytes_view c1, managed_bytes_view c2) { return t->compare(c1, c2); };
            return lexicographical_tri_compare(types.begin(), types.end(),
                a._ck->begin(_s), a._ck->end(_s),
                b_values.begin(), b_values.end(),
                cmp, a.relation(), relation_for_lower_bound(b));
        }

        std::strong_ordering operator()(composite_view a, position_in_partition_view b) const {
            return 0 <=> (*this)(b, a);
        }

        std::strong_ordering operator()(composite_view a, composite_view b) const {
            if (a.is_static() != b.is_static()) {
                return a.is_static() ? std::strong_ordering::less : std::strong_ordering::greater;
            }
            auto&& types = _s.clustering_key_type()->types();
            auto a_values = a.values();
            auto b_values = b.values();
            auto cmp = [&] (const data_type& t, bytes_view c1, bytes_view c2) { return t->compare(c1, c2); };
            return lexicographical_tri_compare(types.begin(), types.end(),
                a_values.begin(), a_values.end(),
                b_values.begin(), b_values.end(),
                cmp,
                relation_for_lower_bound(a),
                relation_for_lower_bound(b));
        }
    };

    // Less comparator giving the same order as composite_tri_compare.
    class composite_less_compare {
        composite_tri_compare _cmp;
    public:
        composite_less_compare(const schema& s) : _cmp(s) {}

        template<typename T, typename U>
        bool operator()(const T& a, const U& b) const {
            return _cmp(a, b) < 0;
        }
    };

    class tri_compare {
        bound_view::tri_compare _cmp;
    private:
        template<typename T, typename U>
        std::strong_ordering compare(const T& a, const U& b) const {
            if (a._type != b._type) {
                return composite_tri_compare::rank(a._type) <=> composite_tri_compare::rank(b._type);
            }
            if (!a._ck) {
                return std::strong_ordering::equal;
            }
            return _cmp(*a._ck, int8_t(a._bound_weight), *b._ck, int8_t(b._bound_weight));
        }
    public:
        tri_compare(const schema& s) : _cmp(s) { }
        std::strong_ordering operator()(const position_in_partition& a, const position_in_partition& b) const {
            return compare(a, b);
        }
        std::strong_ordering operator()(const position_in_partition_view& a, const position_in_partition_view& b) const {
            return compare(a, b);
        }
        std::strong_ordering operator()(const position_in_partition& a, const position_in_partition_view& b) const {
            return compare(a, b);
        }
        std::strong_ordering operator()(const position_in_partition_view& a, const position_in_partition& b) const {
            return compare(a, b);
        }
    };
    class less_compare {
        tri_compare _cmp;
    public:
        less_compare(const schema& s) : _cmp(s) { }
        bool operator()(const position_in_partition& a, const position_in_partition& b) const {
            return _cmp(a, b) < 0;
        }
        bool operator()(const position_in_partition_view& a, const position_in_partition_view& b) const {
            return _cmp(a, b) < 0;
        }
        bool operator()(const position_in_partition& a, const position_in_partition_view& b) const {
            return _cmp(a, b) < 0;
        }
        bool operator()(const position_in_partition_view& a, const position_in_partition& b) const {
            return _cmp(a, b) < 0;
        }
    };
    class equal_compare {
        clustering_key_prefix::equality _equal;
        template<typename T, typename U>
        bool compare(const T& a, const U& b) const {
            if (a._type != b._type) {
                return false;
            }
            bool a_rt_weight = bool(a._ck);
            bool b_rt_weight = bool(b._ck);
            return a_rt_weight == b_rt_weight
                   && (!a_rt_weight || (a._bound_weight == b._bound_weight && _equal(*a._ck, *b._ck)));
        }
    public:
        equal_compare(const schema& s) : _equal(s) { }
        bool operator()(const position_in_partition& a, const position_in_partition& b) const {
            return compare(a, b);
        }
        bool operator()(const position_in_partition_view& a, const position_in_partition_view& b) const {
            return compare(a, b);
        }
        bool operator()(const position_in_partition_view& a, const position_in_partition& b) const {
            return compare(a, b);
        }
        bool operator()(const position_in_partition& a, const position_in_partition_view& b) const {
            return compare(a, b);
        }
    };

    // Create a position which is the same as this one but governed by a schema with reversed clustering key order.
    position_in_partition reversed() const& {
        return position_in_partition(_type, ::reversed(_bound_weight), _ck);
    }

    // Create a position which is the same as this one but governed by a schema with reversed clustering key order.
    position_in_partition reversed() && {
        return position_in_partition(_type, ::reversed(_bound_weight), std::move(_ck));
    }
};

template <>
struct fmt::formatter<position_in_partition> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const ::position_in_partition& pos, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", position_in_partition_view(pos));
    }
};

struct view_and_holder {
    std::optional<position_in_partition> holder;
    position_in_partition_view view;

    view_and_holder(position_in_partition pos)
        : holder(std::move(pos))
        , view(*holder)
    { }

    explicit view_and_holder(position_in_partition_view pos)
        : view(pos)
    { }

    view_and_holder(view_and_holder&& other) noexcept
        : holder(std::move(other.holder))
        , view(holder ? *holder : other.view)
    { }

    view_and_holder& operator=(view_and_holder&& other) noexcept {
        holder = std::move(other.holder);
        view = holder ? *holder: other.view;
        return *this;
    }
};

inline
view_and_holder position_in_partition_view::after_key(const schema& s, position_in_partition_view pos) {
    if (!pos.is_clustering_row()) {
        return view_and_holder(pos);
    }
    if (pos.key().is_full(s)) {
        return view_and_holder(position_in_partition_view::after_all_prefixed(pos.key()));
    }
    // FIXME: This wouldn't be needed if we had a bound weight to represent this.
    return view_and_holder(position_in_partition::after_key(s, clustering_key(pos.key())));
}

inline
position_in_partition position_in_partition::for_range_start(const query::clustering_range& r) {
    return {position_in_partition::range_tag_t(), bound_view::from_range_start(r)};
}

inline
position_in_partition position_in_partition::for_range_end(const query::clustering_range& r) {
    return {position_in_partition::range_tag_t(), bound_view::from_range_end(r)};
}

// Returns true if and only if there can't be any clustering_row with position > a and < b.
// It is assumed that a <= b.
inline
bool no_clustering_row_between(const schema& s, position_in_partition_view a, position_in_partition_view b) {
    clustering_key_prefix::equality eq(s);
    if (a._ck && b._ck) {
        return eq(*a._ck, *b._ck) && (a._bound_weight != bound_weight::before_all_prefixed || b._bound_weight != bound_weight::after_all_prefixed);
    } else {
        return !a._ck && !b._ck;
    }
}

// Returns true if and only if there can't be any clustering_row with position >= a and < b.
// It is assumed that a <= b.
inline
bool no_clustering_row_between_weak(const schema& s, position_in_partition_view a, position_in_partition_view b) {
    clustering_key_prefix::equality eq(s);
    if (a.has_key() && b.has_key()) {
        return eq(a.key(), b.key())
               && (a.get_bound_weight() == bound_weight::after_all_prefixed
                   || b.get_bound_weight() != bound_weight::after_all_prefixed);
    } else {
        return !a.has_key() && !b.has_key();
    }
}

// Includes all position_in_partition objects "p" for which: start <= p < end
// And only those.
class position_range {
private:
    position_in_partition _start;
    position_in_partition _end;
public:
    static position_range from_range(const query::clustering_range&);

    static position_range for_static_row() {
        return {
            position_in_partition(position_in_partition::static_row_tag_t()),
            position_in_partition(position_in_partition::after_static_row_tag_t())
        };
    }

    static position_range full() {
        return {
            position_in_partition(position_in_partition::static_row_tag_t()),
            position_in_partition::after_all_clustered_rows()
        };
    }

    static position_range all_clustered_rows() {
        return {
            position_in_partition::before_all_clustered_rows(),
            position_in_partition::after_all_clustered_rows()
        };
    }

    position_range(position_range&&) = default;
    position_range& operator=(position_range&&) = default;
    position_range(const position_range&) = default;
    position_range& operator=(const position_range&) = default;

    // Constructs position_range which covers the same rows as given clustering_range.
    // position_range includes a fragment if it includes position of that fragment.
    position_range(const query::clustering_range&);
    position_range(query::clustering_range&&);

    position_range(position_in_partition start, position_in_partition end)
        : _start(std::move(start))
        , _end(std::move(end))
    { }

    void set_start(position_in_partition pos) { _start = std::move(pos); }
    void set_end(position_in_partition pos) { _end = std::move(pos); }
    const position_in_partition& start() const& { return _start; }
    position_in_partition&& start() && { return std::move(_start); }
    const position_in_partition& end() const& { return _end; }
    position_in_partition&& end() && { return std::move(_end); }
    bool contains(const schema& s, position_in_partition_view pos) const;
    bool overlaps(const schema& s, position_in_partition_view start, position_in_partition_view end) const;
    // Returns true iff this range contains all keys contained by position_range(start, end).
    bool contains(const schema& s, position_in_partition_view start, position_in_partition_view end) const;
    bool is_all_clustered_rows(const schema&) const;
};

class clustering_interval_set;

inline
bool position_range::contains(const schema& s, position_in_partition_view pos) const {
    position_in_partition::less_compare less(s);
    return !less(pos, _start) && less(pos, _end);
}

inline
bool position_range::contains(const schema& s, position_in_partition_view start, position_in_partition_view end) const {
    position_in_partition::less_compare less(s);
    return !less(start, _start) && !less(_end, end);
}

inline
bool position_range::overlaps(const schema& s, position_in_partition_view start, position_in_partition_view end) const {
    position_in_partition::less_compare less(s);
    return !less(end, _start) && less(start, _end);
}

inline
bool position_range::is_all_clustered_rows(const schema& s) const {
    return _start.is_before_all_clustered_rows(s) && _end.is_after_all_clustered_rows(s);
}

// Assumes that the bounds of `r` are of 'clustered' type
// and that `r` is non-empty (the left bound is smaller than the right bound).
//
// If `r` does not contain any keys, returns nullopt.
std::optional<query::clustering_range> position_range_to_clustering_range(const position_range& r, const schema&);

template <> struct fmt::formatter<position_range> : fmt::formatter<string_view> {
    auto format(const position_range& range, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{{}, {}}}", range.start(), range.end());
    }
};
