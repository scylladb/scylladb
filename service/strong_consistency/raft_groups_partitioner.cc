/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/strong_consistency/raft_groups_partitioner.hh"
#include "dht/token.hh"
#include "schema/schema.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include "keys/keys.hh"
#include "utils/xx_hasher.hh"

namespace service::strong_consistency {

const sstring raft_groups_partitioner::classname = "com.scylladb.dht.RaftGroupsPartitioner";

const sstring raft_groups_partitioner::name() const {
    return classname;
}

dht::token raft_groups_partitioner::token_for_shard(unsigned shard, uint64_t group_id_bits) {
    if (shard > max_shard) {
        return dht::minimum_token();
    }

    int64_t token_value = (static_cast<int64_t>(shard) << shard_shift) | static_cast<int64_t>(group_id_bits & group_id_mask);
    return dht::token(token_value);
}

unsigned raft_groups_partitioner::shard_of(dht::token token) {
    uint64_t token_bits = static_cast<uint64_t>(token.raw());
    return static_cast<unsigned>(token_bits >> shard_shift);
}

static dht::token compute_token(bytes_view shard_bytes, bytes_view group_id_bytes) {
    // Extract shard from first column (int32)
    if (shard_bytes.size() != sizeof(int32_t)) {
        return dht::minimum_token();
    }
    int32_t shard_value = net::ntoh(read_unaligned<int32_t>(shard_bytes.begin()));

    if (shard_value < 0 || shard_value > raft_groups_partitioner::max_shard) {
        // Invalid shard, return minimum token
        return dht::minimum_token();
    }

    // We hash group_id which is a timeuuid (16 bytes) to get (effectively) unique 48 bits for each id.
    xx_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(group_id_bytes.data()), group_id_bytes.size());
    uint64_t group_id_bits = hasher.finalize_uint64();

    auto token = raft_groups_partitioner::token_for_shard(shard_value, group_id_bits);
    return token;
}

dht::token raft_groups_partitioner::get_token(const schema& s, partition_key_view key) const {
    auto exploded_key = key.explode(s);
    if (exploded_key.size() < 2) {
        return dht::minimum_token();
    }
    // First column is shard (int), second is group_id (timeuuid)
    return compute_token(exploded_key[0], exploded_key[1]);
}

dht::token raft_groups_partitioner::get_token(const sstables::key_view& key) const {
    // For key_view, we need to deserialize the composite partition key.
    // The format is: [length1][bytes1][length2][bytes2]...
    // where lengths are 2-byte big-endian values.
    return key.with_linearized([&](bytes_view v) {
        if (v.size() < 2) {
            return dht::minimum_token();
        }

        // Read first component (shard)
        uint16_t len1 = (static_cast<uint16_t>(v[0]) << 8) | static_cast<uint16_t>(v[1]);
        if (v.size() < 2 + len1 + 1 + 2) {  // len1 + data + end-of-component + len2
            return dht::minimum_token();
        }
        bytes_view shard_bytes = v.substr(2, len1);

        // Skip end-of-component byte (0x00)
        size_t offset = 2 + len1 + 1;

        // Read second component (group_id)
        uint16_t len2 = (static_cast<uint16_t>(v[offset]) << 8) | static_cast<uint16_t>(v[offset + 1]);
        if (v.size() < offset + 2 + len2) {
            return dht::minimum_token();
        }
        bytes_view group_id_bytes = v.substr(offset + 2, len2);

        return compute_token(shard_bytes, group_id_bytes);
    });
}

using registry = class_registrator<dht::i_partitioner, raft_groups_partitioner>;
static registry registrator(raft_groups_partitioner::classname);
static registry registrator_short_name("RaftGroupsPartitioner");

} // namespace service::strong_consistency
