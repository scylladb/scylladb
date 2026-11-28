/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "keys/keys.hh"
#include "serializer.hh"
#include "utils/fragment_range.hh"

namespace ser {

// Serializes a partition_key as a framed std::vector<bytes> of its components (the
// layout the IDL generates for a type with a single std::vector<bytes> member),
// writing the components straight from the key's representation.
template <>
struct serializer<partition_key> {
    template <typename Output>
    static void write(Output& out, const partition_key& key) {
        using compound = compound_type<allow_prefixes::no>;
        const managed_bytes& rep = key.representation();
        size_t count = 0;
        size_t data_size = 0;
        for (auto it = compound::begin(rep), e = compound::end(rep); it != e; ++it) {
            ++count;
            data_size += (*it).size_bytes();
        }
        safe_serialize_as_uint32(out, sizeof(size_type) + sizeof(uint32_t) + count * sizeof(uint32_t) + data_size);
        safe_serialize_as_uint32(out, count);
        for (auto it = compound::begin(rep), e = compound::end(rep); it != e; ++it) {
            managed_bytes_view c = *it;
            safe_serialize_as_uint32(out, c.size_bytes());
            for (bytes_view frag : fragment_range(c)) {
                out.write(reinterpret_cast<const char*>(frag.data()), frag.size());
            }
        }
    }

    template <typename Input>
    static partition_key read(Input& buf) {
        return seastar::with_serialized_stream(buf, [] (auto& buf) {
            size_type size = deserialize(buf, std::type_identity<size_type>());
            auto in = buf.read_substream(size - sizeof(size_type));
            return partition_key(deserialize(in, std::type_identity<std::vector<bytes>>()));
        });
    }

    template <typename Input>
    static void skip(Input& buf) {
        seastar::with_serialized_stream(buf, [] (auto& buf) {
            size_type size = deserialize(buf, std::type_identity<size_type>());
            buf.skip(size - sizeof(size_type));
        });
    }
};

template <>
struct serializer<const partition_key> : public serializer<partition_key> {};

}
