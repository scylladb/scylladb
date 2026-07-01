/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <cstring>

#include <seastar/net/byteorder.hh>

#include "dht/token.hh"
#include "keys/keys.hh"
#include "sstables/key.hh"
#include "utils/hashers.hh"

namespace replica::logstor {

inline constexpr size_t key_hash_size = 20;
using key_hash = std::array<uint8_t, key_hash_size>;

// Hashes bytes that are already in the external key encoding.
// This is safe without a schema because the caller has already chosen the byte layout.
inline key_hash compute_key_hash(managed_bytes_view key) {
    auto digest = sha256_hasher::calculate(key);
    key_hash h;
    std::copy_n(digest.begin(), h.size(), h.begin());
    return h;
}

// partition_key_view uses Scylla's internal key representation. For multi-component
// keys we must re-encode it into the legacy/SSTable form before hashing, otherwise
// the derived token would not match the logstor partitioner and SSTable-key path.
inline key_hash compute_key_hash(const schema& s, partition_key_view key) {
    sha256_hasher hasher;

    if (s.partition_key_size() == 1) {
        for (bytes_view frag : fragment_range(*key.begin())) {
            hasher.update(reinterpret_cast<const char*>(frag.data()), frag.size());
        }
    } else {
        static constexpr char end_of_component = 0;
        for (managed_bytes_view component : key.components()) {
            auto size = net::hton(static_cast<uint16_t>(component.size_bytes()));
            hasher.update(reinterpret_cast<const char*>(&size), sizeof(size));
            for (bytes_view frag : fragment_range(component)) {
                hasher.update(reinterpret_cast<const char*>(frag.data()), frag.size());
            }
            hasher.update(&end_of_component, sizeof(end_of_component));
        }
    }

    auto digest = hasher.finalize();
    key_hash h;
    std::copy_n(digest.begin(), h.size(), h.begin());
    return h;
}

// sstables::key_view already stores bytes in the external key encoding, so hashing
// the raw bytes is correct even for multi-component keys.
inline key_hash compute_key_hash(const sstables::key_view& key) {
    return key.with_linearized([] (bytes_view v) {
        return compute_key_hash(managed_bytes_view(v));
    });
}

inline dht::token token_from_key_hash(const key_hash& hash) {
    return dht::token(net::ntoh(read_unaligned<int64_t>(hash.data())));
}

}
