/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "keys/keys.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "utils/fragment_range.hh"

// Wire format matches the old generated single-vector<bytes> serializer;
// size now comes from one components() walk instead of two explode() calls.

namespace ser {

namespace impl {

struct compound_key_layout {
    size_type count = 0;
    uint64_t size = sizeof(size_type) /* frame size */ + sizeof(size_type) /* component count */;
};

// Frame size and component count, from a single walk over the key's components.
template <typename Key>
compound_key_layout measure_compound_key(const Key& key) {
    compound_key_layout l;
    for (managed_bytes_view c : key.components()) {
        ++l.count;
        l.size += sizeof(size_type) + c.size_bytes();
    }
    // Same limit and wording as get_sizeof(), which this replaces.
    if (l.size > std::numeric_limits<size_type>::max()) {
        throw std::runtime_error("Object is too big for get_sizeof");
    }
    return l;
}

template <typename Output, typename Key>
void write_compound_key(Output& out, const Key& key) {
    auto l = measure_compound_key(key);
    if constexpr (std::is_same_v<Output, seastar::measuring_output_stream>) {
        // Filler bytes only; l.size already has the real total.
        serialize(out, size_type(0));
        serialize(out, l.count);
        out.write(nullptr, l.size - 2 * sizeof(size_type));
    } else {
        serialize(out, size_type(l.size));
        serialize(out, l.count);
        for (managed_bytes_view c : key.components()) {
            safe_serialize_as_uint32(out, c.size_bytes());
            for (bytes_view frag : fragment_range(c)) {
                out.write(reinterpret_cast<const char*>(frag.data()), frag.size());
            }
        }
    }
}

template <typename Key, typename Input>
Key read_compound_key(Input& buf) {
    return seastar::with_serialized_stream(buf, [] (auto& buf) {
        size_type size = deserialize(buf, std::type_identity<size_type>());
        auto in = buf.read_substream(size - sizeof(size_type));
        auto components = deserialize(in, std::type_identity<std::vector<bytes>>());
        return Key(std::move(components));
    });
}

template <typename Input>
void skip_compound_key(Input& buf) {
    seastar::with_serialized_stream(buf, [] (auto& buf) {
        size_type size = deserialize(buf, std::type_identity<size_type>());
        buf.skip(size - sizeof(size_type));
    });
}

} // namespace impl

template <>
struct serializer<clustering_key_prefix> {
    template <typename Output>
    static void write(Output& buf, const clustering_key_prefix& v) {
        impl::write_compound_key(buf, v);
    }

    template <typename Input>
    static clustering_key_prefix read(Input& buf) {
        return impl::read_compound_key<clustering_key_prefix>(buf);
    }

    template <typename Input>
    static void skip(Input& buf) {
        impl::skip_compound_key(buf);
    }
};

template <>
struct serializer<const clustering_key_prefix> : public serializer<clustering_key_prefix>
{};

template <>
struct serializer<partition_key> {
    template <typename Output>
    static void write(Output& buf, const partition_key& v) {
        impl::write_compound_key(buf, v);
    }

    template <typename Input>
    static partition_key read(Input& buf) {
        return impl::read_compound_key<partition_key>(buf);
    }

    template <typename Input>
    static void skip(Input& buf) {
        impl::skip_compound_key(buf);
    }
};

template <>
struct serializer<const partition_key> : public serializer<partition_key>
{};

} // namespace ser
