#include <util/serialization.hh>
#include <iterator>
#include "serialization_format.hh"

#include "bytes.hh"
#include "schema.hh"
#include "keys.hh"
#include "sstables/key.hh"
#include "../types.hh"

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
template <typename Iterator>
inline
key from_components(const schema& s, Iterator begin, Iterator end) {
    auto types = s.partition_key_type->types();

    bool composite = types.size() > 1;

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
    return key::from_bytes(std::move(b));
}

key key::from_deeply_exploded(const schema& s, const std::vector<boost::any>& v) {
    return from_components(s, v.begin(), v.end());
}

key key::from_exploded(const schema& s, const std::vector<bytes>& v) {
    return from_components(s, v.begin(), v.end());
}

key key::from_exploded(const schema& s, std::vector<bytes>&& v) {
    if (s.partition_key_type->types().size() == 1) {
        return key(std::move(v[0]));
    }
    return from_components(s, v.begin(), v.end());
}

key key::from_partition_key(const schema& s, const partition_key& pk) {
    return from_components(s, pk.begin(s), pk.end(s));
}
}
