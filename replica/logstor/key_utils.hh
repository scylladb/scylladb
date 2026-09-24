/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <compare>
#include <cstdint>

#include "keys/keys.hh"
#include "utils/managed_bytes.hh"

namespace replica::logstor {

// The hash of a partition key as stored in the primary index, see primary_index_key.
//
// It is the 128-bit XXH3 digest of the key's internal representation. The hash never leaves
// memory: nothing on disk carries it, and recovery recomputes it from the partition keys in
// the record headers. That is what allows key_hash_seed to be drawn anew on every boot.
struct key_hash {
    uint64_t low64;
    uint64_t high64;

    bool operator==(const key_hash& other) const noexcept = default;
    auto operator<=>(const key_hash& other) const noexcept = default;
};

// Process-wide seed for compute_key_hash(), drawn at random when the process starts.
extern const uint64_t key_hash_seed;

key_hash compute_key_hash(managed_bytes_view key);

// The hash is over the internal representation of the key, so it needs no schema. Within a
// table that representation identifies the key exactly, which is all the index needs.
inline key_hash compute_key_hash(const partition_key& key) {
    return compute_key_hash(key.representation());
}

}
