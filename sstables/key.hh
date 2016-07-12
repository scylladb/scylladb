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

namespace sstables {

class key_view {
    composite_view _c;
public:
    key_view(bytes_view b) : _c(composite_view(b)) {}
    key_view(composite_view c) : _c(c) {}
    key_view() : _c() {}

    std::vector<bytes> explode(const schema& s) const {
        if (s.partition_key_size() == 1) {
            return { to_bytes(static_cast<bytes_view>(_c)) };
        }
        return _c.explode();
    }

    bool operator==(const key_view& k) const { return k._c == _c; }
    bool operator!=(const key_view& k) const { return !(k == *this); }

    bool empty() { return _c.empty(); }

    explicit operator bytes_view() const {
        return static_cast<bytes_view>(_c);
    }

    int tri_compare(key_view other) const {
        return compare_unsigned(static_cast<bytes_view>(_c), static_cast<bytes_view>(other));
    }

    int tri_compare(const schema& s, partition_key_view other) const {
        auto lf = other.legacy_form(s);
        auto bytes = static_cast<bytes_view>(_c);
        return lexicographical_tri_compare(
            bytes.begin(), bytes.end(), lf.begin(), lf.end(),
            [] (uint8_t b1, uint8_t b2) { return (int)b1 - b2; });
    }
};

// Our internal representation differs slightly (in the way it serializes) from Origin.
// In order to be able to achieve read and write compatibility for sstables - so they can
// be imported and exported - we need to always convert a key to this representation.
class key {
    enum class kind {
        before_all_keys,
        regular,
        after_all_keys,
    };
    kind _kind;
    composite _c;
public:
    key(bytes&& c) : _kind(kind::regular), _c(composite(std::move(c))) {}
    key(composite&& c) : _kind(kind::regular), _c(std::move(c)) {}
    key(kind k) : _kind(k) {}
    static key from_bytes(bytes b) {
        return key(composite(std::move(b)));
    }
    template <typename RangeOfSerializedComponents>
    static key make_key(const schema& s, RangeOfSerializedComponents&& values) {
        bool is_compound = s.partition_key_type()->types().size() > 1;
        return key(composite(composite::serialize_value(std::forward<decltype(values)>(values), is_compound), is_compound));
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
        return partition_key::from_exploded(s, explode(s));
    }

    std::vector<bytes> explode(const schema& s) const {
        return key_view(_c).explode(s);
    }

    int32_t tri_compare(key_view k) const {
        if (_kind == kind::before_all_keys) {
            return -1;
        }
        if (_kind == kind::after_all_keys) {
            return 1;
        }
        return key_view(_c).tri_compare(k);
    }
    operator key_view() const {
        return key_view(_c);
    }
    explicit operator bytes_view() const {
        return static_cast<bytes_view>(_c);
    }
    const bytes& get_bytes() const {
        return _c.get_bytes();
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

}
