/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/strong_consistency/raft_groups_sharder.hh"

#include "service/strong_consistency/raft_groups_partitioner.hh"

namespace service::strong_consistency {

raft_groups_sharder& raft_groups_sharder::instance() {
    static thread_local raft_groups_sharder sharder;
    return sharder;
}

raft_groups_sharder::raft_groups_sharder()
    : static_sharder(smp::count, 0)
{
}

unsigned raft_groups_sharder::shard_of(const dht::token& t) const {
    if (t.is_minimum()) {
        return dht::token::shard_of_minimum_token();
    }
    if (t.is_maximum()) {
        return shard_count() - 1;
    }
    auto shard = raft_groups_partitioner::shard_of(t);
    return shard;
}

std::optional<unsigned> raft_groups_sharder::try_get_shard_for_reads(const dht::token& t) const {
    const auto shard = shard_of(t);
    if (shard >= shard_count()) {
        return std::nullopt;
    }
    return shard;
}

dht::shard_replica_set raft_groups_sharder::shard_for_writes(const dht::token& t, std::optional<dht::write_replica_set_selector>) const {
    // We don't support migrations of the data in raft tables for strongly consistent tables.
    // When migrating a strongly consistent tablet, we'll need to move its metadata
    // explicitly to the new shard along with its raft group data.
    auto shard = try_get_shard_for_reads(t);
    if (!shard) {
        return {};
    }
    return { *shard };
}

dht::token raft_groups_sharder::token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans) const {
    return token_for_next_shard_for_reads(t, shard, spans);
}

dht::token raft_groups_sharder::token_for_next_shard_for_reads(const dht::token& t, shard_id shard, unsigned spans) const {
    // With the raft_groups_partitioner, there's only one token range per shard, so spans > 1 always overflows.
    if (spans > 1 || shard >= shard_count() || t.is_maximum()) {
        return dht::maximum_token();
    }

    int64_t token_value = t.is_minimum() ? 0 : t.raw();
    int64_t start = static_cast<int64_t>(shard) << raft_groups_partitioner::shard_shift;
    if (token_value < start) {
        return dht::token(start);
    }
    return dht::maximum_token();
}

std::optional<dht::shard_and_token> raft_groups_sharder::next_shard(const dht::token& t) const {
    auto shard = try_get_shard_for_reads(t);
    if (!shard || *shard + 1 >= shard_count()) {
        return std::nullopt;
    }
    auto next_shard = *shard + 1;
    auto next_token = token_for_next_shard_for_reads(t, next_shard);
    if (next_token.is_maximum()) {
        return std::nullopt;
    }
    return dht::shard_and_token{next_shard, next_token};
}

std::optional<dht::shard_and_token> raft_groups_sharder::next_shard_for_reads(const dht::token& t) const {
    return next_shard(t);
}

} // namespace service::strong_consistency
