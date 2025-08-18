/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "bti_key_translation.hh"
#include "sstables/mx/types.hh"

namespace sstables::trie {

static bytes_view bytespan_to_bytesview(std::span<const std::byte> s) {
    return {reinterpret_cast<const bytes::value_type*>(s.data()), s.size()};
}

lazy_comparable_bytes_from_ring_position::lazy_comparable_bytes_from_ring_position(const schema& s, dht::ring_position_view rpv)
    : _token(rpv.token())
    , _pk(rpv.key() ? std::optional<partition_key>(*rpv.key()) : std::optional<partition_key>())
    // Weight <1 for maximum_token is semantically equivalent to weight 1,
    // and `init_first_fragment` assumes that it's 1.
    , _weight(_token.is_maximum() ? 1 : rpv.weight())
    , _s(s)
{
    init_first_fragment();
}

lazy_comparable_bytes_from_ring_position::lazy_comparable_bytes_from_ring_position(const schema& s, dht::decorated_key dk)
    : _token(std::move(dk._token))
    , _pk(std::move(dk._key))
    , _weight(0)
    , _s(s)
{
    init_first_fragment();
}

void lazy_comparable_bytes_from_ring_position::init_first_fragment() {
    _frags.emplace_back(bytes::initialized_later(), 9);
    auto p = _frags[0].data();
    // 0x40 is the key field separator as defined by the BTI byte-comparable encoding.
    // See also https://github.com/apache/cassandra/blob/fad1f7457032544ab6a7b40c5d38ecb8b25899bb/src/java/org/apache/cassandra/utils/bytecomparable/ByteComparable.md#multi-component-sequences-partition-or-clustering-keys-tuples-bounds-and-nulls
    p[0] = 0x40;
    auto token = _token.is_maximum() ? std::numeric_limits<uint64_t>::max() : _token.unbias();
    seastar::write_be<uint64_t>(reinterpret_cast<char*>(&p[1]), token);
}

void lazy_comparable_bytes_from_ring_position::advance() {
    if (_finished) {
        return;
    }
    if (_pk) {
        if (!_gen_it.has_value()) {
            _gen.emplace(lazy_comparable_bytes_from_compound(*_s.partition_key_type(), _pk->representation()));
            _gen_it = _gen->begin();
        }
        if (*_gen_it != std::default_sentinel) {
            _frags.emplace_back(bytespan_to_bytesview(**_gen_it));
            ++*_gen_it;
            return;
        }
    }
    std::byte terminator;
    // 0x20, 0x38, 0x60 are terminators defined by the BTI byte-comparable encoding.
    // See also https://github.com/apache/cassandra/blob/fad1f7457032544ab6a7b40c5d38ecb8b25899bb/src/java/org/apache/cassandra/utils/bytecomparable/ByteComparable.md#multi-component-sequences-partition-or-clustering-keys-tuples-bounds-and-nulls
    if (_weight < 0) {
        terminator = std::byte(0x20);
    } else if (_weight == 0) {
        terminator = std::byte(0x38);
    } else {
        terminator = std::byte(0x60);
    }
    _frags.push_back(bytes{bytes::value_type(terminator)});
    _finished = true;
}

auto lazy_comparable_bytes_from_ring_position::iterator::operator++() -> iterator& {
    ++_i;
    if (_i == _owner._frags.size()) {
        if (_owner._finished) {
            return *this;
        }
        _owner.advance();
    }
    _frag = std::as_writable_bytes(std::span(_owner._frags[_i]));
    return *this;
}

void lazy_comparable_bytes_from_ring_position::trim(const size_t n) {
    size_t remaining = n;
    for (size_t i = 0; i < _frags.size(); ++i) {
        if (_frags[i].size() >= remaining) {
            _frags[i].resize(remaining);
            _frags.resize(i + 1);
            break;
        }
        remaining -= _frags[i].size();
    }
    _finished = true;
}

static std::byte bound_weight_to_terminator(bound_weight b) {
    // 0x20, 0x38, 0x60 are terminators defined by the BTI byte-comparable encoding.
    // See also https://github.com/apache/cassandra/blob/fad1f7457032544ab6a7b40c5d38ecb8b25899bb/src/java/org/apache/cassandra/utils/bytecomparable/ByteComparable.md#multi-component-sequences-partition-or-clustering-keys-tuples-bounds-and-nulls
    switch (b) {
    case bound_weight::after_all_prefixed:
        return std::byte(0x60);
    case bound_weight::before_all_prefixed:
        return std::byte(0x20);
    case bound_weight::equal:
        return std::byte(0x38);
    }
}

[[maybe_unused]]
static bound_weight convert_bound_to_bound_weight(sstables::bound_kind_m b) {
    switch (b) {
    case sstables::bound_kind_m::excl_end:
    case sstables::bound_kind_m::incl_start:
    case sstables::bound_kind_m::excl_end_incl_start:
        return bound_weight::before_all_prefixed;
    case sstables::bound_kind_m::clustering:
    case sstables::bound_kind_m::static_clustering:
        return bound_weight::equal;
    case sstables::bound_kind_m::excl_start:
    case sstables::bound_kind_m::incl_end_excl_start:
    case sstables::bound_kind_m::incl_end:
        return bound_weight::after_all_prefixed;
    default:
        abort();
    }
}

lazy_comparable_bytes_from_clustering_position::lazy_comparable_bytes_from_clustering_position(const schema& s, position_in_partition_view pipv)
    : _ckp(pipv.has_key() ? std::optional<clustering_key_prefix>(pipv.key()) : std::optional<clustering_key_prefix>())
    , _s(s) {
    if (pipv.region() < partition_region::clustered) [[unlikely]] {
        _terminator = bound_weight_to_terminator(bound_weight::before_all_prefixed);
        // This isn't defined by the BTI format docs, it's just something smaller than
        // any actual BTI-encoded clustering position.
        // It could be reasonably used during reads, but it should never be written.
        _terminator = std::byte(0x00);
    } else if (pipv.region() > partition_region::clustered) [[unlikely]] {
        _terminator = bound_weight_to_terminator(bound_weight::after_all_prefixed);
        // This isn't defined by the BTI format docs, it's just something greater than
        // any actual BTI-encoded clustering position.
        // It could be reasonably used during reads, but it should never be written.
        _terminator = std::byte(0xff);
    } else {
        _terminator = bound_weight_to_terminator(pipv.get_bound_weight());
    }
    advance();
}

lazy_comparable_bytes_from_clustering_position::lazy_comparable_bytes_from_clustering_position(const schema& s, sstables::clustering_info ci)
    : _ckp(std::move(ci.clustering))
    , _terminator(bound_weight_to_terminator(convert_bound_to_bound_weight(ci.kind)))
    , _s(s) {
    advance();
}

void lazy_comparable_bytes_from_clustering_position::advance() {
    if (!_gen_it && _ckp) {
        _gen = lazy_comparable_bytes_from_compound(*_s.clustering_key_type(), _ckp->representation());
        _gen_it = _gen->begin();
    }
    if (_gen_it && *_gen_it != std::default_sentinel) {
        _frags.emplace_back(bytespan_to_bytesview(**_gen_it));
        ++*_gen_it;
        return;
    }
    _frags.push_back(bytes{bytes::value_type(_terminator)});
    _finished = true;
}

auto lazy_comparable_bytes_from_clustering_position::iterator::operator++() -> iterator& {
    ++_i;
    if (_i == _owner._frags.size()) {
        if (_owner._finished) {
            return *this;
        }
        _owner.advance();
    }
    _frag = std::as_writable_bytes(std::span(_owner._frags[_i]));
    return *this;
}

void lazy_comparable_bytes_from_clustering_position::trim(const size_t n) {
    size_t remaining = n;
    for (size_t i = 0; i < _frags.size(); ++i) {
        if (_frags[i].size() >= remaining) {
            _frags[i].resize(remaining);
            _frags.resize(i + 1);
            break;
        }
        remaining -= _frags[i].size();
    }
    _finished = true;
}


} // namespace sstables::trie
