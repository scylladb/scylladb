/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "dht/i_partitioner.hh"
#include "dht/token-sharding.hh"

class schema;

namespace sstables {

class key_view;

}

namespace dht {

/// A partitioner mainly for Raft metadata tables for strongly consistent tables
/// (raft_groups, raft_groups_snapshots, raft_groups_snapshot_config).
///
/// These tables have partition keys with the shard as the first column.
/// The partitioner creates tokens that will be assigned to the shard specified
/// in the partition key's first column.
///
/// Token encoding:
///   [shard: 16 bits][hash: 48 bits]
///
/// To skip converting between signed and unsigned tokens (biasing), the top bit
/// is always 0, so we can effectively use only 15 bits for the shard.
/// This correlates with the limit enforced by the Raft tables' schema which uses
/// a signed int (smallint) for the shard column, where allowing only positive
/// shards allows up to 32767 (1 << 15 - 1) shards.
///
/// The lower 48 bits is a hash of the entire partition key.
///
/// This encoding is shard-count independent - the shard can be extracted by simple
/// bit shifting regardless of how many shards exist in the cluster.
struct fixed_shard_partitioner final : public dht::i_partitioner {
    static constexpr unsigned shard_bits = 16;
    static constexpr unsigned shard_shift = 64 - shard_bits;
    static constexpr uint16_t max_shard = std::numeric_limits<int16_t>::max();
    static constexpr uint64_t hash_mask = (uint64_t(1) << shard_shift) - 1;
    static const sstring classname;

    fixed_shard_partitioner() = default;
    virtual const sstring name() const override;
    virtual dht::token get_token(const schema& s, partition_key_view key) const override;
    virtual dht::token get_token(const sstables::key_view& key) const override;

    static dht::token token_for_shard(uint16_t shard, uint64_t hash_bits);
    static unsigned shard_of(dht::token token);
};

/// A sharder for Raft metadata tables for strongly consistent tables (raft_groups,
/// raft_groups_snapshots, raft_groups_snapshot_config).
///
/// These tables store raft group state for all tablets of strongly consistent tables.
/// The sharder allows specifying the shard where the metadata should be located by
/// including the shard id in the partition key.
///
/// The shard is encoded in the token by fixed_shard_partitioner. The sharder extracts
/// the shard by decoding the token bits used for shard encoding.
///
/// We inherit from static_sharder because that's what we use for system tables.
class fixed_shard_sharder : public dht::static_sharder {
public:
    /// Singleton instance for the raft tablet sharder.
    static fixed_shard_sharder& instance();

    fixed_shard_sharder();
    virtual ~fixed_shard_sharder() = default;

    /// Returns the shard for a token by extracting it from the token's high bits.
    /// This overrides static_sharder::shard_of to use the bit-based encoding.
    virtual unsigned shard_of(const dht::token& t) const override;

    virtual std::optional<unsigned> try_get_shard_for_reads(const dht::token& t) const override;
    virtual dht::shard_replica_set shard_for_writes(const dht::token& t, std::optional<dht::write_replica_set_selector> sel) const override;
    virtual dht::token token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans = 1) const override;
    virtual dht::token token_for_next_shard_for_reads(const dht::token& t, shard_id shard, unsigned spans = 1) const override;
    virtual std::optional<dht::shard_and_token> next_shard(const dht::token& t) const override;
    virtual std::optional<dht::shard_and_token> next_shard_for_reads(const dht::token& t) const override;
};

} // namespace dht
