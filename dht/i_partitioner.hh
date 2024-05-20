/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/optimized_optional.hh>
#include "keys.hh"
#include <memory>
#include <utility>
#include <byteswap.h>
#include "dht/token.hh"
#include "dht/token-sharding.hh"
#include "dht/decorated_key.hh"
#include "dht/ring_position.hh"
#include "utils/maybe_yield.hh"

namespace dht {

class i_partitioner {
public:
    using ptr_type = std::unique_ptr<i_partitioner>;

    i_partitioner() = default;
    virtual ~i_partitioner() {}

    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    decorated_key decorate_key(const schema& s, const partition_key& key) const {
        return { get_token(s, key), key };
    }

    /**
     * Transform key to object representation of the on-disk format.
     *
     * @param key the raw, client-facing key
     * @return decorated version of key
     */
    decorated_key decorate_key(const schema& s, partition_key&& key) const {
        auto token = get_token(s, key);
        return { std::move(token), std::move(key) };
    }

    /**
     * @return a token that can be used to route a given key
     * (This is NOT a method to create a token from its string representation;
     * for that, use tokenFactory.fromString.)
     */
    virtual token get_token(const schema& s, partition_key_view key) const = 0;
    virtual token get_token(const sstables::key_view& key) const = 0;

    // FIXME: token.tokenFactory
    //virtual token.tokenFactory gettokenFactory() = 0;

    /**
     * @return name of partitioner.
     */
    virtual const sstring name() const = 0;

    bool operator==(const i_partitioner& o) const {
        return name() == o.name();
    }
};

// Returns the owning shard number for vnode-based replication strategies.
// For the general case, use the sharder obtained from table's effective replication map.
//
//   table& tbl;
//   auto erm = tbl.get_effective_replication_map();
//   auto& sharder = erm->get_sharder();
//
unsigned static_shard_of(const schema&, const token&);

inline decorated_key decorate_key(const schema& s, const partition_key& key) {
    return s.get_partitioner().decorate_key(s, key);
}
inline decorated_key decorate_key(const schema& s, partition_key&& key) {
    return s.get_partitioner().decorate_key(s, std::move(key));
}

inline token get_token(const schema& s, partition_key_view key) {
    return s.get_partitioner().get_token(s, key);
}

dht::partition_range to_partition_range(dht::token_range);
dht::partition_range_vector to_partition_ranges(const dht::token_range_vector& ranges, utils::can_yield can_yield = utils::can_yield::no);

// Each shard gets a sorted, disjoint vector of ranges
std::map<unsigned, dht::partition_range_vector>
split_range_to_shards(dht::partition_range pr, const schema& s, const sharder& sharder);

// Intersect a partition_range with a shard and return the resulting sub-ranges, in sorted order
future<utils::chunked_vector<partition_range>> split_range_to_single_shard(const schema& s,
    const static_sharder& sharder, const dht::partition_range& pr, shard_id shard);

std::unique_ptr<dht::i_partitioner> make_partitioner(sstring name);

// Returns a sorted and deoverlapped list of ranges that are
// the result of subtracting all ranges from ranges_to_subtract.
// ranges_to_subtract must be sorted and deoverlapped.
future<dht::partition_range_vector> subtract_ranges(const schema& schema, const dht::partition_range_vector& ranges, dht::partition_range_vector ranges_to_subtract);

// Returns a token_range vector split based on the given number of most-significant bits
dht::token_range_vector split_token_range_msb(unsigned most_significant_bits);

// Returns the first token included by a partition range.
// May return tokens for which is_minimum() or is_maximum() is true.
dht::token first_token(const dht::partition_range&);

// Returns true iff a given partition range is wholly owned by a single shard.
// If so, returns that shard. Otherwise, return std::nullopt.
// During tablet migration, uses the view on shard ownership for reads.
std::optional<shard_id> is_single_shard(const dht::sharder&, const schema&, const dht::partition_range&);

} // dht

template <> struct fmt::formatter<dht::i_partitioner> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const dht::i_partitioner& p, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{partitioner name = {}}}", p.name());
    }
};
