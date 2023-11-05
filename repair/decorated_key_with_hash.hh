/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "utils/xx_hasher.hh"
#include "repair/hash.hh"
#include "dht/decorated_key.hh"

class decorated_key_with_hash {
public:
    dht::decorated_key dk;
    repair_hash hash;
    decorated_key_with_hash(const schema& s, dht::decorated_key key, uint64_t seed)
        : dk(key) {
        xx_hasher h(seed);
        feed_hash(h, dk.key(), s);
        hash = repair_hash(h.finalize_uint64());
    }
};

