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
#include "keys/compound_compat.hh"
#include "utils/xx_hasher.hh"
#include "utils/log.hh"

namespace service::strong_consistency {

extern logging::logger rgslog;

const sstring raft_groups_partitioner::classname = "com.scylladb.dht.RaftGroupsPartitioner";

const sstring raft_groups_partitioner::name() const {
    return classname;
}

dht::token raft_groups_partitioner::token_for_shard(uint16_t shard, uint64_t group_id_bits) {
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
    // Extract shard from first column (smallint)
    if (shard_bytes.size() != sizeof(uint16_t)) {
        return dht::minimum_token();
    }
    uint16_t shard_value = net::ntoh(read_unaligned<uint16_t>(shard_bytes.begin()));

    if (shard_value < 0 || shard_value > raft_groups_partitioner::max_shard) {
        // Invalid shard, return minimum token
        return dht::minimum_token();
    }

    // We hash group_id which is a timeuuid (16 bytes) to get (effectively) unique 48 bits for each id.
    xx_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(group_id_bytes.data()), group_id_bytes.size());
    uint64_t group_id_bits = hasher.finalize_uint64();

    auto token = raft_groups_partitioner::token_for_shard(shard_value, group_id_bits);
    rgslog.trace("compute_token: shard={}, token={}", shard_value, token);
    return token;
}

dht::token raft_groups_partitioner::get_token(const schema& s, partition_key_view key) const {
    auto exploded_key = key.explode(s);
    if (exploded_key.size() < 2) {
        return dht::minimum_token();
    }
    // First column is shard (smallint), second is group_id (timeuuid)
    return compute_token(exploded_key[0], exploded_key[1]);
}

dht::token raft_groups_partitioner::get_token(const sstables::key_view& key) const {
    return key.with_linearized([&](bytes_view v) {
        try {
            auto components = composite_view(v, true).explode();
            if (components.size() < 2) {
                return dht::minimum_token();
            }
            return compute_token(components[0], components[1]);
        } catch (...) {
            return dht::minimum_token();
        }
    });
}

using registry = class_registrator<dht::i_partitioner, raft_groups_partitioner>;
static registry registrator(raft_groups_partitioner::classname);
static registry registrator_short_name("RaftGroupsPartitioner");

} // namespace service::strong_consistency
