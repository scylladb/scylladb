/*
 * Copyright (C) 2015 ScyllaDB
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
#include "bytes.hh"
#include "schema.hh"
#include "core/future.hh"
#include "database_fwd.hh"
#include "keys.hh"
#include "compound_compat.hh"
#include "dht/i_partitioner.hh"

namespace sstables {

class key_view {
    bytes_view _bytes;
public:
    explicit key_view(bytes_view b) : _bytes(b) {}
    key_view() : _bytes() {}

    std::vector<bytes_view> explode(const schema& s) const {
        return composite_view(_bytes, s.partition_key_size() > 1).explode();
    }

    partition_key to_partition_key(const schema& s) const {
        return partition_key::from_exploded_view(explode(s));
    }

    bool operator==(const key_view& k) const { return k._bytes == _bytes; }
    bool operator!=(const key_view& k) const { return !(k == *this); }

    bool empty() { return _bytes.empty(); }

    explicit operator bytes_view() const {
        return _bytes;
    }

    int tri_compare(key_view other) const {
        return compare_unsigned(_bytes, other._bytes);
    }

    int tri_compare(const schema& s, partition_key_view other) const {
        auto lf = other.legacy_form(s);
        return lexicographical_tri_compare(
            _bytes.begin(), _bytes.end(), lf.begin(), lf.end(),
            [] (uint8_t b1, uint8_t b2) { return (int)b1 - b2; });
    }
};

// Our internal representation differs slightly (in the way it serializes) from Origin.
// In order to be able to achieve read and write compatibility for sstables - so they can
// be imported and exported - we need to always convert a key to this representation.
class key {
public:
    enum class kind {
        before_all_keys,
        regular,
        after_all_keys,
    };
private:
    kind _kind;
    bytes _bytes;

    static bool is_compound(const schema& s) {
        return s.partition_key_size() > 1;
    }
public:
    key(bytes&& b) : _kind(kind::regular), _bytes(std::move(b)) {}
    key(kind k) : _kind(k) {}
    static key from_bytes(bytes b) {
        return key(std::move(b));
    }
    template <typename RangeOfSerializedComponents>
    static key make_key(const schema& s, RangeOfSerializedComponents&& values) {
        return key(composite::serialize_value(std::forward<decltype(values)>(values), is_compound(s)).release_bytes());
    }
    static key from_deeply_exploded(const schema& s, const std::vector<data_value>& v) {
        return make_key(s, v);
    }
    static key from_exploded(const schema& s, std::vector<bytes>& v) {
        return make_key(s, v);
    }
    static key from_exploded(const schema& s, std::vector<bytes>&& v) {
        return make_key(s, std::move(v));
    }
    // Unfortunately, the _bytes field for the partition_key are not public. We can't move.
    static key from_partition_key(const schema& s, partition_key_view pk) {
        return make_key(s, pk);
    }
    partition_key to_partition_key(const schema& s) const {
        return partition_key::from_exploded_view(explode(s));
    }

    std::vector<bytes_view> explode(const schema& s) const {
        return composite_view(_bytes, is_compound(s)).explode();
    }

    int32_t tri_compare(key_view k) const {
        if (_kind == kind::before_all_keys) {
            return -1;
        }
        if (_kind == kind::after_all_keys) {
            return 1;
        }
        return key_view(_bytes).tri_compare(k);
    }
    operator key_view() const {
        return key_view(_bytes);
    }
    explicit operator bytes_view() const {
        return _bytes;
    }
    const bytes& get_bytes() const {
        return _bytes;
    }
    friend key minimum_key();
    friend key maximum_key();
};

inline key minimum_key() {
    return key(key::kind::before_all_keys);
};

inline key maximum_key() {
    return key(key::kind::after_all_keys);
};

class decorated_key_view {
    dht::token_view _token;
    key_view _partition_key;
public:
    decorated_key_view(dht::token_view token, key_view partition_key) noexcept
        : _token(token), _partition_key(partition_key) { }

    dht::token_view token() const {
        return _token;
    }

    key_view key() const {
        return _partition_key;
    }
};

}
