/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "dht/i_partitioner.hh"

class schema;

namespace sstables {

class key_view;

}

namespace service::strong_consistency {

/// A partitioner for Raft metadata tables for strongly consistent tables (raft_groups,
/// raft_groups_snapshots, raft_groups_snapshot_config).
///
/// These tables have partition keys of the form (shard, group_id).
/// The partitioner creates tokens that will be assigned to the shard specified
/// in the partition key's first column.
///
/// Token encoding:
///   [shard: 16 bits][group_id: 48 bits]
///
/// To skip converting between signed and unsigned tokens (biasing), the top bit
/// is always 0, so we can effectively use only 15 bits for the shard.
/// This correlates with the limit enforced by the Raft tables' schema which uses
/// a signed int (smallint) for the shard column, where allowing only positive
/// shards allows up to 32767 (1 << 15 - 1) shards.
///
/// The lower 48 bits is a hash of the group_id timeuuid.
///
/// This encoding is shard-count independent - the shard can be extracted by simple
/// bit shifting regardless of how many shards exist in the cluster.
struct raft_groups_partitioner final : public dht::i_partitioner {
    static constexpr unsigned shard_bits = 16;
    static constexpr unsigned shard_shift = 64 - shard_bits;
    static constexpr uint16_t max_shard = std::numeric_limits<int16_t>::max();
    static constexpr uint64_t group_id_mask = (uint64_t(1) << shard_shift) - 1;
    static const sstring classname;

    raft_groups_partitioner() = default;
    virtual const sstring name() const override;
    virtual dht::token get_token(const schema& s, partition_key_view key) const override;
    virtual dht::token get_token(const sstables::key_view& key) const override;

    static dht::token token_for_shard(uint16_t shard, uint64_t group_id_bits);
    static unsigned shard_of(dht::token token);
};

} // namespace service::strong_consistency
