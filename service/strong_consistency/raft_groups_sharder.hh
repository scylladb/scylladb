/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "dht/token-sharding.hh"

namespace service::strong_consistency {

/// A sharder for Raft metadata tables for strongly consistent tables (raft_groups,
/// raft_groups_snapshots, raft_groups_snapshot_config).
///
/// These tables store raft group state for all tablets of strongly consistent tables.
/// The sharder allows specifying the shard where the metadata should be located by
/// including the shard id in the partition key.
///
/// The shard is encoded in the token by raft_groups_partitioner. The sharder extracts
/// the shard by decoding the token bits used for shard encoding.
///
/// We inherit from static_sharder because that's what we use for system tables.
class raft_groups_sharder : public dht::static_sharder {
public:
    /// Singleton instance for the raft tablet sharder.
    static raft_groups_sharder& instance();

    raft_groups_sharder();
    virtual ~raft_groups_sharder() = default;

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

} // namespace service::strong_consistency
