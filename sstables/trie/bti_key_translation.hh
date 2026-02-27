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
#include "comparable_bytes_iterator.hh"

namespace sstables {
    struct clustering_info;
} // namespace sstables

namespace sstables::trie {

std::byte bound_weight_to_terminator(bound_weight b);

std::byte bound_weight_to_terminator(bound_weight b);

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
    const schema& _s;
    std::array<std::byte, 9> _token_buf;
    bound_weight _weight;
    // Length of this BTI encoding, in bytes.
    // Used to implement `trim()`.
    //
    // Starts at something meaningless but greater than `_token_buf.size()`,
    // is set to the full encoding size when `_pk` is converted to `comparable_bytes`,
    // might be later reduced by trim().
    unsigned _size = -1;
    // Starts as `partition_key`, potentially is converted to `comparable_bytes`
    // later if it turns out that the token isn't enough.
    std::variant<partition_key, comparable_bytes> _pk;
private:
    void init_first_fragment(dht::token);
public:
    lazy_comparable_bytes_from_ring_position(const schema& s, dht::ring_position_view);
    lazy_comparable_bytes_from_ring_position(const schema& s, dht::decorated_key);
    lazy_comparable_bytes_from_ring_position(lazy_comparable_bytes_from_ring_position&&) noexcept = delete;
    lazy_comparable_bytes_from_ring_position& operator=(lazy_comparable_bytes_from_ring_position&&) noexcept = delete;
    struct iterator {
        using difference_type = std::ptrdiff_t;
        using value_type = std::span<std::byte>;

        lazy_comparable_bytes_from_ring_position& _owner;
        // Note: the reason we keep `_current` in a separate member
        // is because `operator*` returns a reference to the span.
        // (Because the caller mutates it while it's reading the iterator,
        // for convenience.)
        std::span<std::byte> _current;
        // When `_remaining` is nullopt, it means that we haven't considered
        // "_owner._pk" yet. If `_current` runs out, we will
        // initialize `_remaining` from "_owner._pk".
        std::optional<managed_bytes_mutable_view> _remaining;

        iterator(lazy_comparable_bytes_from_ring_position& o)
            : _owner(o)
        {
            _current = std::as_writable_bytes(std::span(_owner._token_buf));
            // Note that `_size` is meaningless if `trim()` wasn't called yet,
            // but it's guaranteed to be greater than `_token_buf.size()`,
            // so this branch won't be taken.
            if (_owner._size <= _current.size()) {
                _current = _current.first(_owner._size);
                _remaining.emplace();
            }
        }
        std::span<std::byte>&& operator*() {
            return std::move(_current);
        }
        iterator& operator++() {
            if (!_remaining) {
                if (auto raw_pk = std::get_if<partition_key>(&_owner._pk)) {
                    // The lazy BTI translation happens here.
                    _owner._pk = comparable_bytes_from_compound(*_owner._s.partition_key_type(), raw_pk->representation(), bound_weight_to_terminator(_owner._weight));
                    _owner._size = std::get<comparable_bytes>(_owner._pk).size() + _owner._token_buf.size();
                }
                auto& cb = std::get<comparable_bytes>(_owner._pk);
                _remaining = managed_bytes_mutable_view(cb.as_managed_bytes_mutable_view()).prefix(_owner._size - _owner._token_buf.size());
            }
            _current = std::as_writable_bytes(std::span(_remaining->current_fragment()));
            if (!_remaining->empty()) {
                _remaining->remove_current();
            }
            return *this;
        }
        void operator++(int) {
            ++*this;
        }
        bool operator==(const std::default_sentinel_t&) const {
            return _current.empty() && _remaining.has_value() && _remaining->empty();
        }
    };
    // Trims to the target size.
    // The target size must be smaller than the number of bytes observed by an iterator.
    // Invalidates iterators.
    void trim(const size_t n);
    iterator begin() { return iterator(*this); }
    std::default_sentinel_t end() { return std::default_sentinel; }
};
static_assert(comparable_bytes_iterator<lazy_comparable_bytes_from_ring_position::iterator>);

// Eagerly translates a clustering position to the BTI encoding.
struct lazy_comparable_bytes_from_clustering_position {
    comparable_bytes _encoded;
    // Initially equal to _encoded.size(),
    // but might be reduced by trim().
    unsigned _size;
    lazy_comparable_bytes_from_clustering_position(const schema& s, position_in_partition_view pipv);
    lazy_comparable_bytes_from_clustering_position(const schema& s, const sstables::clustering_info&);
    lazy_comparable_bytes_from_clustering_position(lazy_comparable_bytes_from_clustering_position&&) = delete;
    struct iterator {
        using difference_type = std::ptrdiff_t;
        using value_type = std::span<std::byte>;

        lazy_comparable_bytes_from_clustering_position& _owner;
        // Note: the reason we keep `_current` in a separate member
        // is because `operator*` returns a reference to the span.
        // (Because the caller mutates it while it's reading the iterator,
        // for convenience.)
        managed_bytes_mutable_view _remaining;
        std::span<std::byte> _current;

        iterator(lazy_comparable_bytes_from_clustering_position& o)
            : _owner(o)
            , _remaining(managed_bytes_mutable_view(_owner._encoded.as_managed_bytes_mutable_view()).prefix(_owner._size))
        {
            _current = std::as_writable_bytes(std::span(_remaining.current_fragment()));
            if (!_remaining.empty()) {
                _remaining.remove_current();
            }
        }
        std::span<std::byte>&& operator*() {
            return std::move(_current);
        }
        iterator& operator++() {
            _current = std::as_writable_bytes(std::span(_remaining.current_fragment()));
            if (!_remaining.empty()) {
                _remaining.remove_current();
            }
            return *this;
        }
        void operator++(int) {
            ++*this;
        }
        bool operator==(const std::default_sentinel_t&) const {
            return _current.empty() && _remaining.empty();
        }
    };
    // Trims to the target size.
    // The target size must be smaller than the number of bytes observed by an iterator.
    // Invalidates iterators.
    void trim(unsigned n);
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
