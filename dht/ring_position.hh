/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "keys.hh"
#include "dht/token.hh"
#include "dht/decorated_key.hh"

namespace dht {

//
// Represents position in the ring of partitions, where partitions are ordered
// according to decorated_key ordering (first by token, then by key value).
// Intended to be used for defining partition ranges.
//
// The 'key' part is optional. When it's absent, this object represents a position
// which is either before or after all keys sharing given token. That's determined
// by relation_to_keys().
//
// For example for the following data:
//
//   tokens: |    t1   | t2 |
//           +----+----+----+
//   keys:   | k1 | k2 | k3 |
//
// The ordering is:
//
//   ring_position(t1, token_bound::start) < ring_position(k1)
//   ring_position(k1)                     < ring_position(k2)
//   ring_position(k1)                     == decorated_key(k1)
//   ring_position(k2)                     == decorated_key(k2)
//   ring_position(k2)                     < ring_position(t1, token_bound::end)
//   ring_position(k2)                     < ring_position(k3)
//   ring_position(t1, token_bound::end)   < ring_position(t2, token_bound::start)
//
// Maps to org.apache.cassandra.db.RowPosition and its derivatives in Origin.
//
class ring_position {
public:
    enum class token_bound : int8_t { start = -1, end = 1 };
private:
    friend class ring_position_comparator;
    friend class ring_position_ext;
    dht::token _token;
    token_bound _token_bound{}; // valid when !_key
    std::optional<partition_key> _key;
public:
    static ring_position min() noexcept {
        return { minimum_token(), token_bound::start };
    }

    static ring_position max() noexcept {
        return { maximum_token(), token_bound::end };
    }

    bool is_min() const noexcept {
        return _token.is_minimum();
    }

    bool is_max() const noexcept {
        return _token.is_maximum();
    }

    static ring_position starting_at(dht::token token) {
        return { std::move(token), token_bound::start };
    }

    static ring_position ending_at(dht::token token) {
        return { std::move(token), token_bound::end };
    }

    ring_position(dht::token token, token_bound bound)
        : _token(std::move(token))
        , _token_bound(bound)
    { }

    ring_position(dht::token token, partition_key key)
        : _token(std::move(token))
        , _key(std::make_optional(std::move(key)))
    { }

    ring_position(dht::token token, token_bound bound, std::optional<partition_key> key)
        : _token(std::move(token))
        , _token_bound(bound)
        , _key(std::move(key))
    { }

    ring_position(const dht::decorated_key& dk)
        : _token(dk._token)
        , _key(std::make_optional(dk._key))
    { }

    ring_position(dht::decorated_key&& dk)
        : _token(std::move(dk._token))
        , _key(std::make_optional(std::move(dk._key)))
    { }

    const dht::token& token() const noexcept {
        return _token;
    }

    // Valid when !has_key()
    token_bound bound() const {
        return _token_bound;
    }

    // Returns -1 if smaller than keys with the same token, +1 if greater.
    int relation_to_keys() const {
        return _key ? 0 : static_cast<int>(_token_bound);
    }

    const std::optional<partition_key>& key() const {
        return _key;
    }

    bool has_key() const {
        return bool(_key);
    }

    // Call only when has_key()
    dht::decorated_key as_decorated_key() const {
        return { _token, *_key };
    }

    bool equal(const schema&, const ring_position&) const;

    // Trichotomic comparator defining a total ordering on ring_position objects
    std::strong_ordering tri_compare(const schema&, const ring_position&) const;

    // "less" comparator corresponding to tri_compare()
    bool less_compare(const schema&, const ring_position&) const;
};

// Non-owning version of ring_position and ring_position_ext.
//
// Unlike ring_position, it can express positions which are right after and right before the keys.
// ring_position still can not because it is sent between nodes and such a position
// would not be (yet) properly interpreted by old nodes. That's why any ring_position
// can be converted to ring_position_view, but not the other way.
//
// It is possible to express a partition_range using a pair of two ring_position_views v1 and v2,
// where v1 = ring_position_view::for_range_start(r) and v2 = ring_position_view::for_range_end(r).
// Such range includes all keys k such that v1 <= k < v2, with order defined by ring_position_comparator.
//
class ring_position_view {
    friend std::strong_ordering ring_position_tri_compare(const schema& s, ring_position_view lh, ring_position_view rh);
    friend class ring_position_comparator;
    friend class ring_position_comparator_for_sstables;
    friend class ring_position_ext;

    // Order is lexicographical on (_token, _key) tuples, where _key part may be missing, and
    // _weight affecting order between tuples if one is a prefix of the other (including being equal).
    // A positive weight puts the position after all strictly prefixed by it, while a non-positive
    // weight puts it before them. If tuples are equal, the order is further determined by _weight.
    //
    // For example {_token=t1, _key=nullptr, _weight=1} is ordered after {_token=t1, _key=k1, _weight=0},
    // but {_token=t1, _key=nullptr, _weight=-1} is ordered before it.
    //
    const dht::token* _token; // always not nullptr
    const partition_key* _key; // Can be nullptr
    int8_t _weight;
private:
    ring_position_view() noexcept : _token(nullptr), _key(nullptr), _weight(0) { }
    explicit operator bool() const noexcept { return bool(_token); }
public:
    using token_bound = ring_position::token_bound;
    struct after_key_tag {};
    using after_key = bool_class<after_key_tag>;

    static ring_position_view min() noexcept {
        static auto min_token = minimum_token();
        return { min_token, nullptr, -1 };
    }

    static ring_position_view max() noexcept {
        static auto max_token = maximum_token();
        return { max_token, nullptr, 1 };
    }

    bool is_min() const noexcept {
        return _token->is_minimum();
    }

    bool is_max() const noexcept {
        return _token->is_maximum();
    }

    static ring_position_view for_range_start(const partition_range& r) {
        return r.start() ? ring_position_view(r.start()->value(), after_key(!r.start()->is_inclusive())) : min();
    }

    static ring_position_view for_range_end(const partition_range& r) {
        return r.end() ? ring_position_view(r.end()->value(), after_key(r.end()->is_inclusive())) : max();
    }

    static ring_position_view for_after_key(const dht::decorated_key& dk) {
        return ring_position_view(dk, after_key::yes);
    }

    static ring_position_view for_after_key(dht::ring_position_view view) {
        return ring_position_view(after_key_tag(), view);
    }

    static ring_position_view starting_at(const dht::token& t) {
        return ring_position_view(t, token_bound::start);
    }

    static ring_position_view ending_at(const dht::token& t) {
        return ring_position_view(t, token_bound::end);
    }

    ring_position_view(const dht::ring_position& pos, after_key after = after_key::no)
        : _token(&pos.token())
        , _key(pos.has_key() ? &*pos.key() : nullptr)
        , _weight(pos.has_key() ? bool(after) : pos.relation_to_keys())
    { }

    ring_position_view(const ring_position_view& pos) = default;
    ring_position_view& operator=(const ring_position_view& other) = default;

    ring_position_view(after_key_tag, const ring_position_view& v)
        : _token(v._token)
        , _key(v._key)
        , _weight(v._key ? 1 : v._weight)
    { }

    ring_position_view(const dht::decorated_key& key, after_key after_key = after_key::no)
        : _token(&key.token())
        , _key(&key.key())
        , _weight(bool(after_key))
    { }

    ring_position_view(const dht::token& token, const partition_key* key, int8_t weight)
        : _token(&token)
        , _key(key)
        , _weight(weight)
    { }

    explicit ring_position_view(const dht::token& token, token_bound bound = token_bound::start)
        : _token(&token)
        , _key(nullptr)
        , _weight(static_cast<std::underlying_type_t<token_bound>>(bound))
    { }

    const dht::token& token() const noexcept { return *_token; }
    const partition_key* key() const { return _key; }

    // Only when key() == nullptr
    token_bound get_token_bound() const { return token_bound(_weight); }
    // Only when key() != nullptr
    after_key is_after_key() const { return after_key(_weight == 1); }

    friend fmt::formatter<ring_position_view>;
    friend class optimized_optional<ring_position_view>;
};

using ring_position_view_opt = optimized_optional<ring_position_view>;

//
// Represents position in the ring of partitions, where partitions are ordered
// according to decorated_key ordering (first by token, then by key value).
// Intended to be used for defining partition ranges.
//
// Unlike ring_position, it can express positions which are right after and right before the keys.
// ring_position still can not because it is sent between nodes and such a position
// would not be (yet) properly interpreted by old nodes. That's why any ring_position
// can be converted to ring_position_ext, but not the other way.
//
// It is possible to express a partition_range using a pair of two ring_position_exts v1 and v2,
// where v1 = ring_position_ext::for_range_start(r) and v2 = ring_position_ext::for_range_end(r).
// Such range includes all keys k such that v1 <= k < v2, with order defined by ring_position_comparator.
//
class ring_position_ext {
    // Order is lexicographical on (_token, _key) tuples, where _key part may be missing, and
    // _weight affecting order between tuples if one is a prefix of the other (including being equal).
    // A positive weight puts the position after all strictly prefixed by it, while a non-positive
    // weight puts it before them. If tuples are equal, the order is further determined by _weight.
    //
    // For example {_token=t1, _key=nullptr, _weight=1} is ordered after {_token=t1, _key=k1, _weight=0},
    // but {_token=t1, _key=nullptr, _weight=-1} is ordered before it.
    //
    dht::token _token;
    std::optional<partition_key> _key;
    int8_t _weight;
public:
    using token_bound = ring_position::token_bound;
    struct after_key_tag {};
    using after_key = bool_class<after_key_tag>;

    static ring_position_ext min() noexcept {
        return { minimum_token(), std::nullopt, -1 };
    }

    static ring_position_ext max() noexcept {
        return { maximum_token(), std::nullopt, 1 };
    }

    bool is_min() const noexcept {
        return _token.is_minimum();
    }

    bool is_max() const noexcept {
        return _token.is_maximum();
    }

    static ring_position_ext for_range_start(const partition_range& r) {
        return r.start() ? ring_position_ext(r.start()->value(), after_key(!r.start()->is_inclusive())) : min();
    }

    static ring_position_ext for_range_end(const partition_range& r) {
        return r.end() ? ring_position_ext(r.end()->value(), after_key(r.end()->is_inclusive())) : max();
    }

    static ring_position_ext for_after_key(const dht::decorated_key& dk) {
        return ring_position_ext(dk, after_key::yes);
    }

    static ring_position_ext for_after_key(dht::ring_position_ext view) {
        return ring_position_ext(after_key_tag(), view);
    }

    static ring_position_ext starting_at(const dht::token& t) {
        return ring_position_ext(t, token_bound::start);
    }

    static ring_position_ext ending_at(const dht::token& t) {
        return ring_position_ext(t, token_bound::end);
    }

    ring_position_ext(const dht::ring_position& pos, after_key after = after_key::no)
        : _token(pos.token())
        , _key(pos.key())
        , _weight(pos.has_key() ? bool(after) : pos.relation_to_keys())
    { }

    ring_position_ext(const ring_position_ext& pos) = default;
    ring_position_ext& operator=(const ring_position_ext& other) = default;

    ring_position_ext(ring_position_view v)
        : _token(*v._token)
        , _key(v._key ? std::make_optional(*v._key) : std::nullopt)
        , _weight(v._weight)
    { }

    ring_position_ext(after_key_tag, const ring_position_ext& v)
        : _token(v._token)
        , _key(v._key)
        , _weight(v._key ? 1 : v._weight)
    { }

    ring_position_ext(const dht::decorated_key& key, after_key after_key = after_key::no)
        : _token(key.token())
        , _key(key.key())
        , _weight(bool(after_key))
    { }

    ring_position_ext(dht::token token, std::optional<partition_key> key, int8_t weight) noexcept
        : _token(std::move(token))
        , _key(std::move(key))
        , _weight(weight)
    { }

    ring_position_ext(ring_position&& pos) noexcept
        : _token(std::move(pos._token))
        , _key(std::move(pos._key))
        , _weight(pos.relation_to_keys())
    { }

    explicit ring_position_ext(const dht::token& token, token_bound bound = token_bound::start)
        : _token(token)
        , _key(std::nullopt)
        , _weight(static_cast<std::underlying_type_t<token_bound>>(bound))
    { }

    const dht::token& token() const noexcept { return _token; }
    const std::optional<partition_key>& key() const { return _key; }
    int8_t weight() const { return _weight; }

    // Only when key() == std::nullopt
    token_bound get_token_bound() const { return token_bound(_weight); }

    // Only when key() != std::nullopt
    after_key is_after_key() const { return after_key(_weight == 1); }

    operator ring_position_view() const { return { _token, _key ? &*_key : nullptr, _weight }; }
};

std::strong_ordering ring_position_tri_compare(const schema& s, ring_position_view lh, ring_position_view rh);

template <typename T>
requires std::is_convertible<T, ring_position_view>::value
ring_position_view ring_position_view_to_compare(const T& val) {
    return val;
}

// Trichotomic comparator for ring order
struct ring_position_comparator {
    const schema& s;
    ring_position_comparator(const schema& s_) : s(s_) {}

    std::strong_ordering operator()(ring_position_view lh, ring_position_view rh) const {
        return ring_position_tri_compare(s, lh, rh);
    }

    template <typename T>
    std::strong_ordering operator()(const T& lh, ring_position_view rh) const {
        return ring_position_tri_compare(s, ring_position_view_to_compare(lh), rh);
    }

    template <typename T>
    std::strong_ordering operator()(ring_position_view lh, const T& rh) const {
        return ring_position_tri_compare(s, lh, ring_position_view_to_compare(rh));
    }

    template <typename T1, typename T2>
    std::strong_ordering operator()(const T1& lh, const T2& rh) const {
        return ring_position_tri_compare(s, ring_position_view_to_compare(lh), ring_position_view_to_compare(rh));
    }
};

struct ring_position_comparator_for_sstables {
    const schema& s;
    ring_position_comparator_for_sstables(const schema& s_) : s(s_) {}
    std::strong_ordering operator()(ring_position_view, sstables::decorated_key_view) const;
    std::strong_ordering operator()(sstables::decorated_key_view, ring_position_view) const;
};

// "less" comparator giving the same order as ring_position_comparator
struct ring_position_less_comparator {
    ring_position_comparator tri;

    ring_position_less_comparator(const schema& s) : tri(s) {}

    template<typename T, typename U>
    bool operator()(const T& lh, const U& rh) const {
        return tri(lh, rh) < 0;
    }
};

// Wraps ring_position or ring_position_view so either is compatible with old-style C++: default
// constructor, stateless comparators, yada yada.
// The motivations for supporting both types are to make containers self-sufficient by not relying
// on callers to keep ring position alive, allow lookup on containers that don't support different
// key types, and also avoiding unnecessary copies.
class compatible_ring_position_or_view {
    schema_ptr _schema;
    lw_shared_ptr<dht::ring_position> _rp;
    dht::ring_position_view_opt _rpv; // optional only for default ctor, nothing more
public:
    compatible_ring_position_or_view() = default;
    explicit compatible_ring_position_or_view(schema_ptr s, dht::ring_position rp)
        : _schema(std::move(s)), _rp(make_lw_shared<dht::ring_position>(std::move(rp))), _rpv(dht::ring_position_view(*_rp)) {
    }
    explicit compatible_ring_position_or_view(const schema& s, dht::ring_position_view rpv)
        : _schema(s.shared_from_this()), _rpv(rpv) {
    }
    const dht::ring_position_view& position() const {
        return *_rpv;
    }
    std::strong_ordering operator<=>(const compatible_ring_position_or_view& other) const {
        return dht::ring_position_tri_compare(*_schema, position(), other.position());
    }
    bool operator==(const compatible_ring_position_or_view& other) const {
        return *this <=> other == 0;
    }
};

class partition_ranges_view {
    const dht::partition_range* _data = nullptr;
    size_t _size = 0;

public:
    partition_ranges_view() = default;
    partition_ranges_view(const dht::partition_range& range) : _data(&range), _size(1) {}
    partition_ranges_view(const dht::partition_range_vector& ranges) : _data(ranges.data()), _size(ranges.size()) {}
    bool empty() const { return _size == 0; }
    size_t size() const { return _size; }
    const dht::partition_range& front() const { return *_data; }
    const dht::partition_range& back() const { return *(_data + _size - 1); }
    const dht::partition_range* begin() const { return _data; }
    const dht::partition_range* end() const { return _data + _size; }
};

} // namespace dht

template<>
struct fmt::formatter<dht::ring_position_view> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const dht::ring_position_view&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<dht::ring_position_ext> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const dht::ring_position_ext& pos, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", (dht::ring_position_view)pos);
    }
};

template<>
struct fmt::formatter<dht::ring_position> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const dht::ring_position& pos, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<dht::partition_ranges_view> : fmt::formatter<string_view> {
    auto format(const dht::partition_ranges_view&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
