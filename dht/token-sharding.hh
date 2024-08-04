/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include "dht/token.hh"
#include <seastar/core/smp.hh>
#include <boost/container/static_vector.hpp>

namespace dht {

inline sstring cpu_sharding_algorithm_name() {
    return "biased-token-round-robin";
}

std::vector<uint64_t> init_zero_based_shard_start(unsigned shards, unsigned sharding_ignore_msb_bits);

unsigned shard_of(unsigned shard_count, unsigned sharding_ignore_msb_bits, const token& t);

token token_for_next_shard(const std::vector<uint64_t>& shard_start, unsigned shard_count, unsigned sharding_ignore_msb_bits, const token& t, shard_id shard, unsigned spans);

struct shard_and_token {
    shard_id shard;
    token token;
};

/// Represents a set of shards that own a given token on a single host.
/// It can be either of: none, single shard (no tablet migration), or two shards (during intra-node tablet migration).
using shard_replica_set = boost::container::static_vector<unsigned, 2>;

/// Describes which replica set should be used during tablet migration.
enum class write_replica_set_selector {
    previous, both, next
};

/**
 * Describes mapping between token space of a given table and owning shards on the local node.
 * The mapping reflected by this instance is constant for the lifetime of this sharder object.
 * It is not guaranteed to be the same for different sharder instances, even within a single process lifetime.
 *
 * For vnode-based replication strategies, the mapping is constant for the lifetime of the local process even
 * across different sharder instances.
 */
class sharder {
protected:
    unsigned _shard_count;
    unsigned _sharding_ignore_msb_bits;
public:
    sharder(unsigned shard_count = smp::count, unsigned sharding_ignore_msb_bits = 0);
    virtual ~sharder() = default;

    /**
     * Returns the shard that handles a particular token for reads.
     * Use shard_for_writes() to determine the set of shards that should receive writes.
     *
     * When there is no tablet migration, the function is equivalent to the lambda:
     *
     *   [] (const token& t) {
     *      auto shards = shard_for_writes();
     *      SCYLLA_ASSERT(shards.size() <= 1);
     *      return shards.empty() ? 0 : shards[0];
     *   }
     *
     */
    virtual unsigned shard_for_reads(const token& t) const = 0;

    /**
     * Returns the set of shards which should receive a write to token t.
     * During intra-node tablet migration, local writes need to be replicated to both the old and new shard.
     * You should keep the effective_replication_map_ptr used to obtain this sharder alive around performing
     * the writes so that topology coordinator can wait for all writes using this particular topology version.
     *
     * Unlike shard_for_reads(), returns an empty vector (instead of 0) if the token is not owned by this node.
     */
    virtual shard_replica_set shard_for_writes(const token& t, std::optional<write_replica_set_selector> sel = std::nullopt) const = 0;

    /**
     * Gets the first token greater than `t` that is in shard `shard` for reads, and is a shard boundary (its first token).
     *
     * It holds that:
     *
     *   shard_for_reads(token_for_next_shard(t, shard)) == shard
     *
     * If the `spans` parameter is greater than zero, the result is the same as if the function
     * is called `spans` times, each time applied to its return value, but efficiently. This allows
     * selecting ranges that include multiple round trips around the 0..smp::count-1 shard span:
     *
     *     token_for_next_shard(t, shard, spans) == token_for_next_shard(token_for_next_shard(t, shard, 1), spans - 1)
     *
     * On overflow, maximum_token() is returned.
     */
    virtual token token_for_next_shard_for_reads(const token& t, shard_id shard, unsigned spans = 1) const = 0;

    /**
     * Finds the next token greater than t which is owned by a different shard than the owner of t
     * and returns that token and its owning shard. If no such token exists, returns nullopt.
     *
     * The next shard may not necessarily be a successor of the shard owning t.
     *
     * The returned shard is the shard for reads:
     *
     *     shard_for_reads(next_shard(t)->token) == next_shard(t)->shard
     */
    virtual std::optional<shard_and_token> next_shard_for_reads(const token& t) const = 0;

    /**
     * @return number of shards configured for this partitioner
     */
    unsigned shard_count() const {
        return _shard_count;
    }

    unsigned sharding_ignore_msb() const {
        return _sharding_ignore_msb_bits;
    }
};

/**
 * A sharder for vnode-based tables.
 * Shard assignment for a given token is constant for a given sharder configuration (shard count and ignore MSB bits).
 * Doesn't change on topology changes.
 */
class static_sharder : public sharder {
    std::vector<uint64_t> _shard_start;
public:
    static_sharder(unsigned shard_count = smp::count, unsigned sharding_ignore_msb_bits = 0);

    virtual unsigned shard_of(const token& t) const;
    virtual std::optional<shard_and_token> next_shard(const token& t) const;
    virtual token token_for_next_shard(const token& t, shard_id shard, unsigned spans = 1) const;

    virtual unsigned shard_for_reads(const token& t) const override;
    virtual shard_replica_set shard_for_writes(const token& t, std::optional<write_replica_set_selector> sel) const override;
    virtual token token_for_next_shard_for_reads(const token& t, shard_id shard, unsigned spans = 1) const override;
    virtual std::optional<shard_and_token> next_shard_for_reads(const token& t) const override;

    bool operator==(const static_sharder& o) const {
        return _shard_count == o._shard_count && _sharding_ignore_msb_bits == o._sharding_ignore_msb_bits;
    }
};

/*
 * Finds the first token in token range (`start`, `end`] that belongs to shard shard_idx.
 *
 * If there is no token that belongs to shard shard_idx in this range,
 * `end` is returned.
 *
 * The first token means the one that appears first on the ring when going
 * from `start` to `end`.
 * 'first token' is not always the smallest.
 * For example, if in vnode (100, 10] only tokens 110 and 1 belong to
 * shard shard_idx then token 110 is the first because it appears first
 * when going from 100 to 10 on the ring.
 */
dht::token find_first_token_for_shard(
        const dht::static_sharder& sharder, dht::token start, dht::token end, size_t shard_idx);

} //namespace dht

template<>
struct fmt::formatter<dht::static_sharder> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const dht::static_sharder& sharder, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "sharder[shard_count={}, ignore_msb_bits={}]",
                              sharder.shard_count(), sharder.sharding_ignore_msb());
    }
};

template<>
struct fmt::formatter<dht::shard_replica_set> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const dht::shard_replica_set& rs, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{{}}}", fmt::join(rs, ", "));
    }
};
