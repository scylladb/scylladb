
/*
 * Copyright 2015 Cloudius Systems
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

#include <utils/serialization.hh>
#include <iterator>
#include "serialization_format.hh"

#include "bytes.hh"
#include "schema.hh"
#include "keys.hh"
#include "sstables/key.hh"
#include "../types.hh"

#include "utils/data_input.hh"

namespace sstables {

class internal_serializer {
    using t_type = std::vector<data_type>;
    using it_type = t_type::iterator;

    t_type _t;
    it_type _it;

public:
    internal_serializer(t_type types) : _t(types),  _it(_t.begin()) {}

    inline void reset() {
        _it = _t.begin();
    }

    inline void advance() {
        assert(_it != _t.end());
        _it++;
    }

    inline size_t serialized_size(const data_value& value) {
        return value.serialized_size();
    }

    inline void serialize(const data_value& value, bytes::iterator& out) {
        value.serialize(out);
    }
};

class sstable_serializer {
public:
    inline void reset() {}
    inline void advance() {}
    inline size_t serialized_size(bytes_view value) {
        return value.size();
    }

    inline void serialize(bytes_view value, bytes::iterator& out) {
        out = std::copy_n(value.begin(), value.size(), out);
    }
};

// The iterator has to provide successive elements that are from one of the
// type above ( so we know how to serialize them)
template <typename Iterator, typename Serializer>
inline
bytes from_components(Iterator begin, Iterator end, Serializer&& serializer, bool composite = true) {
    size_t len = 0;

    for (auto c = begin; c != end; ++c) {
        auto& component = *c;

        len += uint8_t(composite) * sizeof(uint16_t);
        len += serializer.serialized_size(component);
        len += uint8_t(composite);
        serializer.advance();
    }

    bytes b(bytes::initialized_later(), len);
    auto bi = b.begin();

    serializer.reset();
    for (auto c = begin; c != end; ++c) {
        auto& component = *c;

        auto sz = serializer.serialized_size(component);
        if (sz > std::numeric_limits<uint16_t>::max()) {
            throw runtime_exception(sprint("Cannot serialize component: value too big (%ld bytes)", sz));
        }

        if (composite) {
            write<uint16_t>(bi, sz);
        }

        serializer.serialize(component, bi);

        if (composite) {
            // Range tombstones are not keys. For collections, only frozen
            // values can be keys. Therefore, for as long as it is safe to
            // assume that this code will be used to create representation of
            // keys, it is safe to assume the trailing byte is always zero.
            write<uint8_t>(bi, uint8_t(0));
        }
        serializer.advance();
    }
    return b;
}

key key::from_deeply_exploded(const schema& s, const std::vector<data_value>& v) {
    auto &pt = s.partition_key_type()->types();
    bool composite = pt.size() > 1;
    return from_components(v.begin(), v.end(), internal_serializer(pt), composite);
}

key key::from_exploded(const schema& s, const std::vector<bytes>& v) {
    auto &pt = s.partition_key_type()->types();
    bool composite = pt.size() > 1;
    return from_components(v.begin(), v.end(), sstable_serializer(), composite);
}

key key::from_exploded(const schema& s, std::vector<bytes>&& v) {
    if (s.partition_key_type()->types().size() == 1) {
        return key(std::move(v[0]));
    }
    return from_components(v.begin(), v.end(), sstable_serializer());
}

key key::from_partition_key(const schema& s, const partition_key& pk) {
    auto &pt = s.partition_key_type()->types();
    bool composite = pt.size() > 1;
    return from_components(pk.begin(s), pk.end(s), sstable_serializer(), composite);
}

partition_key
key::to_partition_key(const schema& s) {
    return partition_key::from_exploded(s, explode(s));
}

template <typename ClusteringElement>
composite composite::from_clustering_element(const schema& s, const ClusteringElement& ce) {
    return from_components(ce.begin(s), ce.end(s), sstable_serializer());
}

template composite composite::from_clustering_element(const schema& s, const clustering_key_prefix& ck);

composite composite::from_exploded(const std::vector<bytes_view>& v, composite_marker m) {
    if (v.size() == 0) {
        return bytes(size_t(1), bytes::value_type(m));
    }
    auto b = from_components(v.begin(), v.end(), sstable_serializer());
    b.back() = bytes::value_type(m);
    return composite(std::move(b));
}

composite composite::static_prefix(const schema& s) {
    static bytes static_marker(size_t(2), bytes::value_type(0xff));

    std::vector<bytes_view> sv(s.clustering_key_size());
    return static_marker + from_components(sv.begin(), sv.end(), sstable_serializer());
}

inline
std::vector<bytes> explode_composite(bytes_view _bytes) {
    std::vector<bytes> ret;

    data_input in(_bytes);
    while (in.has_next()) {
        auto b = in.read_view_to_blob<uint16_t>();
        ret.push_back(to_bytes(b));
        auto marker = in.read<uint8_t>();
        // The components will be separated by a null byte, but the last one has special significance.
        if (in.has_next() && (marker != 0)) {
            throw runtime_exception(sprint("non-zero component divider found (%d) mid", marker));
        }
    }
    return ret;
}

std::vector<bytes> key::explode(const schema& s) const {

    if (s.partition_key_size() == 1) {
        return { _bytes };
    }

    return explode_composite(bytes_view(_bytes));
}

std::vector<bytes> key_view::explode(const schema& s) const {
    if (s.partition_key_size() == 1) {
        return { to_bytes(_bytes) };
    }

    return explode_composite(_bytes);
}

std::vector<bytes> composite_view::explode() const {
    return explode_composite(_bytes);
}

int key_view::tri_compare(const schema& s, partition_key_view other) const {
    auto lf = other.legacy_form(s);
    return lexicographical_tri_compare(
        _bytes.begin(), _bytes.end(), lf.begin(), lf.end(),
        [] (uint8_t b1, uint8_t b2) { return (int)b1 - b2; });
}

}
