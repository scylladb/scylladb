/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// This file provides translation routines for ring positions and clustering positions
// from Scylla's native in-memory structures to BTI's byte-comparable encoding.
//
// This translation is performed whenever a new decorated key or clustering block
// are added to a BTI index, and whenever a BTI index is queried for a range of positions.
//
// For a description of the encoding, see
// https://github.com/apache/cassandra/blob/fad1f7457032544ab6a7b40c5d38ecb8b25899bb/src/java/org/apache/cassandra/utils/bytecomparable/ByteComparable.md#multi-component-sequences-partition-or-clustering-keys-tuples-bounds-and-nulls

#pragma once

#include "mutation/position_in_partition.hh"
#include "types/comparable_bytes.hh"
#include "common.hh"
#include "dht/ring_position.hh"
#include <generator>
#include "comparable_bytes_iterator.hh"

namespace sstables {
    struct clustering_info;
} // namespace sstables

namespace sstables::trie {

// FIXME: this doesn't deserve to be a template.
// The actual data structure is the same for both partition keys and clustering keys.
// There's no need to codegen two separate functions for them.
// Unfortunately p.components(representation).begin() returns an internal type
// of compound_type. So this isn't easy to detemplatize.
template <allow_prefixes AllowPrefixes>
std::generator<const_bytes> lazy_comparable_bytes_from_compound(const compound_type<AllowPrefixes>& p, managed_bytes_view representation) {
    auto t_it = p.types().begin();
    const auto t_end = p.types().end();
    auto c_it = p.components(representation).begin();
    const auto c_end = p.components(representation).end();

    while (c_it != c_end) {
        SCYLLA_ASSERT(t_it != t_end);

        constexpr std::byte col_separator = std::byte(0x40);
        co_yield std::array<std::byte, 1>{col_separator};

        auto cb = comparable_bytes(**t_it, *c_it);
        auto mbv = cb.as_managed_bytes_view();
        for (bytes_view bv : fragment_range(mbv)) {
            co_yield std::as_bytes(std::span(bv));
        }
        ++c_it;
        ++t_it;
    }
}

// Translates a ring position (during an index query)
// or a decorated key (during an index write)
// to the BTI encoding.
//
// An important part of this class is the lazy evaluation
// initially, only the token is encoded, and the value of
// the key column is encoded only when needed.
//
// Most of the time, the token is enough to uniquely identify
// the partition in a given SSTable and the value of the key
// column isn't needed for anything, and the encoding might
// be quite costly in the worst case. So we make an effort to avoid it.
class lazy_comparable_bytes_from_ring_position {
    dht::token _token;
    std::optional<partition_key> _pk;
    int _weight;
    utils::small_vector<bytes, 1> _frags;
    const schema& _s;
    std::optional<std::generator<const_bytes>> _gen;
    std::optional<decltype(_gen->begin())> _gen_it;
    bool _finished = false;
private:
    void init_first_fragment();

public:
    lazy_comparable_bytes_from_ring_position(const schema& s, dht::ring_position_view);
    lazy_comparable_bytes_from_ring_position(const schema& s, dht::decorated_key);
    lazy_comparable_bytes_from_ring_position(lazy_comparable_bytes_from_ring_position&&) noexcept = delete;
    lazy_comparable_bytes_from_ring_position& operator=(lazy_comparable_bytes_from_ring_position&&) noexcept = delete;
    void advance();
    class iterator {
        using difference_type = std::ptrdiff_t;
        using value_type = std::span<std::byte>;

        lazy_comparable_bytes_from_ring_position& _owner;
        size_t _i = 0;
        std::span<std::byte> _frag;
    public:
        iterator(lazy_comparable_bytes_from_ring_position& o)
            : _owner(o)
            , _frag(std::as_writable_bytes(std::span(_owner._frags[_i])))
        {}
        std::span<std::byte>&& operator*() {
            return std::move(_frag);
        }
        iterator& operator++();
        void operator++(int) {
            ++*this;
        }
        bool operator==(const std::default_sentinel_t&) const {
            return _i == _owner._frags.size();
        }
    };
    void trim(const size_t n);
    iterator begin() { return iterator(*this); }
    std::default_sentinel_t end() { return std::default_sentinel; }
};
static_assert(comparable_bytes_iterator<lazy_comparable_bytes_from_ring_position::iterator>);

// Like lazy_comparable_bytes_from_ring_position, but for clustering positions.
// The difference is small enough that maybe the two classes should be merged.
//
// The value of the lazy encoding for clustering positions is debatable.
// It would be more obviously positive if the underlying comparable_bytes
// translation was also lazy. But it isn't, so we perform the encoding
// column by column.
struct lazy_comparable_bytes_from_clustering_position {
    std::optional<clustering_key_prefix> _ckp;
    std::byte _terminator;
    const schema& _s;
    utils::small_vector<bytes, 3> _frags;
    std::optional<std::generator<const_bytes>> _gen;
    std::optional<decltype(_gen->begin())> _gen_it;
    bool _finished = false;

    lazy_comparable_bytes_from_clustering_position(const schema& s, position_in_partition_view pipv);
    lazy_comparable_bytes_from_clustering_position(const schema& s, sstables::clustering_info);
    lazy_comparable_bytes_from_clustering_position(lazy_comparable_bytes_from_clustering_position&&) = delete;
    void advance();
    struct iterator {
        using difference_type = std::ptrdiff_t;
        using value_type = std::span<std::byte>;

        lazy_comparable_bytes_from_clustering_position& _owner;
        size_t _i = 0;
        std::span<std::byte> _frag;

        iterator(lazy_comparable_bytes_from_clustering_position& o)
            : _owner(o)
            , _frag(std::as_writable_bytes(std::span(_owner._frags[_i])))
        {}
        std::span<std::byte>&& operator*() {
            return std::move(_frag);
        }
        iterator& operator++();
        void operator++(int) {
            ++*this;
        }
        bool operator==(const std::default_sentinel_t&) const {
            return _i == _owner._frags.size();
        }
    };
    void trim(const size_t n);
    iterator begin() { return iterator(*this); }
    std::default_sentinel_t end() { return std::default_sentinel; }
};
static_assert(comparable_bytes_iterator<lazy_comparable_bytes_from_clustering_position::iterator>);

template <comparable_bytes_iterator T>
using cbi_span_type = std::remove_cvref_t<decltype(*std::declval<T>())>;
// Finds the first byte in `b` which differentiates it from `a`,
// (Might be one-past the end of `b` if `b` is a prefix of `a`).
// and returns its index and a pointer to it.
// `b` mustn't be a prefix of `a`.
// 
// If `a` and `b` were contiguous ranges of bytes, the equivalent would be:
// ```
// auto [it_a, it_b] = std::ranges::mismatch(a, b);
// return {it_b - b.begin(), it_b};
// ```
template <comparable_bytes_iterator T>
std::pair<size_t, typename cbi_span_type<T>::pointer> lcb_mismatch(T&& a_it, T&& b_it) {
    using span_type = cbi_span_type<T>;
    // a_sp is a suffix of the span pointed to by a_it,
    // b_sp is a suffix of the span pointed to by b_it.
    // Incrementing the iterators invalidates the corresponding spans.
    auto a_sp = span_type();
    auto b_sp = span_type();
    // Prefix bytes visited so far.
    size_t i = 0;

    if (b_it == std::default_sentinel) {
        goto b_exhausted;
    } 
    b_sp = *b_it;
    if (a_it == std::default_sentinel) {
        goto a_exhausted;
    }
    a_sp = *a_it;
    while (true) {
        if (a_sp.empty()) {
            ++a_it;
            if (a_it == std::default_sentinel) {
                goto a_exhausted;
            }
            a_sp = *a_it;
            expensive_log("lcb_mismatch: a_sp={}", fmt_hex(a_sp));
        }
        if (b_sp.empty()) {
            ++b_it;
            if (b_it == std::default_sentinel) {
                goto b_exhausted;
            }
            b_sp = *b_it;
            expensive_log("lcb_mismatch: b_sp={}", fmt_hex(b_sp));
        }
        size_t mismatch_idx = std::ranges::mismatch(a_sp, b_sp).in2 - b_sp.begin();
        expensive_log("lcb_mismatch: mismatch_idx={}, a[0]={}, b[0]={}", mismatch_idx, *a_sp.begin(), *b_sp.begin());
        i += mismatch_idx;
        if (mismatch_idx < a_sp.size() && mismatch_idx < b_sp.size()) {
            return {i, &b_sp[mismatch_idx]};
        }
        a_sp = a_sp.subspan(mismatch_idx);
        b_sp = b_sp.subspan(mismatch_idx);
    }
a_exhausted:
    while (b_sp.empty()) {
        ++b_it;
        if (b_it == std::default_sentinel) {
            break;
        }
        b_sp = *b_it;
    }
b_exhausted:
    // Note: b_sp might be empty here, if b_it is exhausted.
    // That's fine, in this case the pointer will be one-past the last fragment of b_it.
    return {i, &b_sp[0]};
}

} // sstables::trie
