/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cdc_partitioner.hh"
#include "dht/token.hh"
#include "schema/schema.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include "cdc/generation.hh"
#include "keys.hh"

namespace cdc {

const sstring cdc_partitioner::classname = "com.scylladb.dht.CDCPartitioner";

const sstring cdc_partitioner::name() const {
    return classname;
}

static dht::token to_token(int64_t value) {
    return dht::token(value);
}

static dht::token to_token(bytes_view key) {
    // Key should be 16 B long, of which first 8 B are used for token calculation
    if (key.size() != 2*sizeof(int64_t)) {
        return dht::minimum_token();
    }
    return to_token(stream_id::token_from_bytes(key));
}

dht::token
cdc_partitioner::get_token(const sstables::key_view& key) const {
    return key.with_linearized([&] (bytes_view v) {
        return to_token(v);
    });
}

dht::token
cdc_partitioner::get_token(const schema& s, partition_key_view key) const {
    auto exploded_key = key.explode(s);
    return to_token(exploded_key[0]);
}

using registry = class_registrator<dht::i_partitioner, cdc_partitioner>;
static registry registrator(cdc::cdc_partitioner::classname);
static registry registrator_short_name("CDCPartitioner");

}
