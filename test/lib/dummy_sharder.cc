/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/algorithm/find.hpp>
#include "test/lib/dummy_sharder.hh"

unsigned dummy_sharder::shard_of(const dht::token& t) const {
    auto it = boost::find(_tokens, t);
    // Unknown tokens are assigned to shard 0
    return it == _tokens.end() ? 0 : std::distance(_tokens.begin(), it) % sharder::shard_count();
}

dht::token dummy_sharder::token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans) const {
    // Find the first token that belongs to `shard` and is larger than `t`
    auto it = std::find_if(_tokens.begin(), _tokens.end(), [this, &t, shard] (const dht::token& shard_token) {
        return shard_token > t && shard_of(shard_token) == shard;
    });

    if (it == _tokens.end()) {
        return dht::maximum_token();
    }

    --spans;

    while (spans) {
        if (std::distance(it, _tokens.end()) <= sharder::shard_count()) {
            return dht::maximum_token();
        }
        it += sharder::shard_count();
        --spans;
    }

    return *it;
}
