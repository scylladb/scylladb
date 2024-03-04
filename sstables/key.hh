/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "bytes.hh"
#include "schema/schema_fwd.hh"
#include <seastar/core/future.hh>
#include "replica/database_fwd.hh"
#include "keys.hh"
#include "compound_compat.hh"
#include "dht/token.hh"

namespace sstables {

class key_view {
    managed_bytes_view _bytes;
public:
    explicit key_view(managed_bytes_view b) : _bytes(b) {}
    explicit key_view(bytes_view b) : _bytes(b) {}
    key_view() : _bytes() {}

    template <std::invocable<bytes_view> Func>
    std::invoke_result_t<Func, bytes_view> with_linearized(Func&& func) const {
        return ::with_linearized(_bytes, func);
    }

    partition_key to_partition_key(const schema& s) const {
        return with_linearized([&] (bytes_view v) {
            return partition_key::from_exploded_view(composite_view(v, s.partition_key_size() > 1).explode());
        });
    }

    bool operator==(const key_view& k) const = default;

    bool empty() const { return _bytes.empty(); }

    std::strong_ordering tri_compare(key_view other) const {
        return compare_unsigned(_bytes, other._bytes);
    }

    std::strong_ordering tri_compare(const schema& s, partition_key_view other) const {
        return with_linearized([&] (bytes_view v) {
            auto lf = other.legacy_form(s);
            return std::lexicographical_compare_three_way(
                    v.begin(), v.end(), lf.begin(), lf.end(),
                    [](uint8_t b1, uint8_t b2) { return  b1 <=> b2; });
        });
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

    std::strong_ordering tri_compare(key_view k) const {
        if (_kind == kind::before_all_keys) {
            return std::strong_ordering::less;
        }
        if (_kind == kind::after_all_keys) {
            return std::strong_ordering::greater;
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
    dht::token _token;
    key_view _partition_key;
public:
    decorated_key_view(dht::token token, key_view partition_key) noexcept
        : _token(token), _partition_key(partition_key) { }

    dht::token token() const {
        return _token;
    }

    key_view key() const {
        return _partition_key;
    }
};

}
