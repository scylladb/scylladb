/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/smp.hh>

#include "dht/decorated_key.hh"
#include "seastarx.hh"

struct local_shard_only_tag { };
using local_shard_only = bool_class<local_shard_only_tag>;

namespace tests {

struct key_size {
    size_t min;
    size_t max;
};

// Generate n partition keys for the given schema.
//
// Returned keys are unique (their token too), ordered and never empty.
// Parameters:
// * n - number of keys
// * s - schema of the keys, used also to obtain the sharder
// * shard - only generate keys for this shard (if engaged)
// * size - the min and max size of the key in bytes, if disengaged default
//          limits (1-128) are used. If you want exactly sized keys, use
//          ascii or bytes types only as the key types.
std::vector<dht::decorated_key> generate_partition_keys(size_t n, schema_ptr s, std::optional<shard_id> shard = this_shard_id(), std::optional<key_size> size = {});
std::vector<dht::decorated_key> generate_partition_keys(size_t n, schema_ptr s, local_shard_only lso, std::optional<key_size> size = {});
// Overload for a single key
dht::decorated_key generate_partition_key(schema_ptr s, std::optional<shard_id> shard = this_shard_id(), std::optional<key_size> size = {});
dht::decorated_key generate_partition_key(schema_ptr s, local_shard_only lso, std::optional<key_size> size = {});

// Generate n clustering keys
//
// Returned keys are unique, ordered and never empty.
// Parameters are the same as that of generate_partition_keys().
// If allow_prefixes is true, prefix keys may be generated too.
std::vector<clustering_key> generate_clustering_keys(size_t n, schema_ptr s, bool allow_prefixes = false, std::optional<key_size> size = {});
// Overload for a single key
clustering_key generate_clustering_key(schema_ptr s, bool allow_prefix = false, std::optional<key_size> size = {});

// Double to unsigned long conversion
int64_t d2t(double d);

} // namespace tests
