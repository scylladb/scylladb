#include <util/serialization.hh>
#include <iterator>
#include "serialization_format.hh"

#include "bytes.hh"
#include "schema.hh"
#include "keys.hh"
#include "sstables/key.hh"
#include "../types.hh"

#include "utils/data_input.hh"

namespace sstables {

inline size_t serialized_size(data_type& t, const boost::any& value) {
    return t->serialized_size(value);
}

inline void serialize(data_type& t, const boost::any& value, bytes::iterator& out) {
    t->serialize(value, out);
}

inline size_t serialized_size(data_type& t, const bytes& value) {
    return value.size();
}

inline void serialize(data_type& t, const bytes& value, bytes::iterator& out) {
    out = std::copy_n(value.begin(), value.size(), out);
}

inline size_t serialized_size(data_type& t, const bytes_view& value) {
    return value.size();
}

inline void serialize(data_type& t, const bytes_view& value, bytes::iterator& out) {
    out = std::copy_n(value.begin(), value.size(), out);
}

// The iterator has to provide successive elements that are from one of the
// type above ( so we know how to serialize them)
template <typename Iterator, typename Type>
inline
bytes from_components(Iterator begin, Iterator end, Type types, bool always_composite = false) {
    bool composite = types.size() > 1 || always_composite;

    size_t len = 0;
    auto i = types.begin();
    for (auto c = begin; c != end; ++c) {
        auto& component = *c;

        assert(i != types.end());
        auto& type = *i++;

        if (composite) {
            len += sizeof(uint16_t);
        }
        len += serialized_size(type, component);

        if (composite) {
            len += 1;
        }
    }

    bytes b(bytes::initialized_later(), len);
    auto bi = b.begin();
    i = types.begin();

    for (auto c = begin; c != end; ++c) {
        auto& component = *c;

        assert(i != types.end());
        auto& type = *i++;

        auto sz = serialized_size(type, component);
        if (sz > std::numeric_limits<uint16_t>::max()) {
            throw runtime_exception(sprint("Cannot serialize component: value too big (%ld bytes)", sz));
        }

        if (composite) {
            write<uint16_t>(bi, sz);
        }

        serialize(type, component, bi);

        if (composite) {
            // Range tombstones are not keys. For collections, only frozen
            // values can be keys. Therefore, for as long as it is safe to
            // assume that this code will be used to create representation of
            // keys, it is safe to assume the trailing byte is always zero.
            write<uint8_t>(bi, uint8_t(0));
        }
    }
    return b;
}

template <typename Iterator>
inline
key from_components(const schema& s, Iterator begin, Iterator end) {
    bytes&& b = from_components(begin, end, s.partition_key_type()->types(), false);
    return key::from_bytes(std::move(b));
}

key key::from_deeply_exploded(const schema& s, const std::vector<boost::any>& v) {
    return from_components(s, v.begin(), v.end());
}

key key::from_exploded(const schema& s, const std::vector<bytes>& v) {
    return from_components(s, v.begin(), v.end());
}

key key::from_exploded(const schema& s, std::vector<bytes>&& v) {
    if (s.partition_key_type()->types().size() == 1) {
        return key(std::move(v[0]));
    }
    return from_components(s, v.begin(), v.end());
}

key key::from_partition_key(const schema& s, const partition_key& pk) {
    return from_components(s, pk.begin(s), pk.end(s));
}

composite composite::from_clustering_key(const schema& s, const clustering_key& ck) {
    return from_components(ck.begin(s), ck.end(s), s.clustering_key_type()->types(), true);
}

composite composite::from_exploded(const std::vector<bytes_view>& v) {
    return from_components(v.begin(), v.end(), std::vector<data_type>(v.size(), bytes_type), true);
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
}
