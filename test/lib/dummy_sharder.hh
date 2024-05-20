/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/range/adaptor/map.hpp>
#include "dht/token.hh"
#include "dht/token-sharding.hh"

// Shards tokens such that tokens are owned by shards in a round-robin manner.
class dummy_sharder : public dht::static_sharder {
    std::vector<dht::token> _tokens;

public:
    // We need a container input that enforces token order by design.
    // In addition client code will often map tokens to something, e.g. mutation
    // they originate from or shards, etc. So, for convenience we allow any
    // ordered associative container (std::map) that has dht::token as keys.
    // Values will be ignored.
    template <typename T>
    dummy_sharder(const dht::static_sharder& sharding, const std::map<dht::token, T>& something_by_token)
        : dht::static_sharder(sharding)
        , _tokens(boost::copy_range<std::vector<dht::token>>(something_by_token | boost::adaptors::map_keys)) {
    }

    virtual unsigned shard_of(const dht::token& t) const override;
    virtual dht::token token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans = 1) const override;
};
