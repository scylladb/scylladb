/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


/* This module contains classes and functions used to manage CDC generations:
 * sets of CDC stream identifiers used by the cluster to choose partition keys for CDC log writes.
 * Each CDC generation begins operating at a specific time point, called the generation's timestamp.
 * The generation is used by all nodes in the cluster to pick CDC streams until superseded by a new generation.
 *
 * Functions from this module are used by the node joining procedure to introduce new CDC generations to the cluster
 * (which is necessary due to new tokens being inserted into the token ring), or during rolling upgrade
 * if CDC is enabled for the first time.
 */

#pragma once

#include <vector>
#include <unordered_set>
#include <seastar/util/noncopyable_function.hh>

#include "replica/database_fwd.hh"
#include "db_clock.hh"
#include "dht/token.hh"
#include "locator/token_metadata.hh"
#include "utils/chunked_vector.hh"
#include "cdc/generation_id.hh"

namespace seastar {
    class abort_source;
} // namespace seastar

namespace db {
    class config;
    class system_distributed_keyspace;
} // namespace db

namespace gms {
    class inet_address;
    class gossiper;
} // namespace gms

namespace cdc {

api::timestamp_clock::duration get_generation_leeway();

class stream_id final {
    bytes _value;
public:
    static constexpr uint8_t version_1 = 1;

    stream_id() = default;
    stream_id(bytes);
    stream_id(dht::token, size_t);

    bool is_set() const;
    auto operator<=>(const stream_id&) const noexcept = default;

    uint8_t version() const;
    size_t index() const;
    const bytes& to_bytes() const;
    dht::token token() const;

    partition_key to_partition_key(const schema& log_schema) const;
    static int64_t token_from_bytes(bytes_view);
};

/* Describes a mapping of tokens to CDC streams in a token range.
 *
 * The range ends with `token_range_end`. A vector of `token_range_description`s defines the ranges entirely
 * (the end of the `i`th range is the beginning of the `i+1 % size()`th range). Ranges are left-opened, right-closed.
 *
 * Tokens in the range ending with `token_range_end` are mapped to streams in the `streams` vector as follows:
 * token `T` is mapped to `streams[j]` if and only if the used partitioner maps `T` to the `j`th shard,
 * assuming that the partitioner is configured for `streams.size()` shards and (partitioner's) `sharding_ignore_msb`
 * equals to the given `sharding_ignore_msb`.
*/
struct token_range_description {
    dht::token token_range_end;
    std::vector<stream_id> streams;
    uint8_t sharding_ignore_msb;

    bool operator==(const token_range_description&) const;
};


/* Describes a mapping of tokens to CDC streams in a whole token ring.
 *
 * Division of the ring to token ranges is defined in terms of `token_range_end`s
 * in the `_entries` vector. See the comment above `token_range_description` for explanation.
 */
class topology_description {
    utils::chunked_vector<token_range_description> _entries;
public:
    topology_description(utils::chunked_vector<token_range_description> entries);
    bool operator==(const topology_description&) const;

    const utils::chunked_vector<token_range_description>& entries() const&;
    utils::chunked_vector<token_range_description>&& entries() &&;
};

/**
 * The set of streams for a single topology version/generation
 * I.e. the stream ids at a given time. 
 */ 
class streams_version {
public:
    utils::chunked_vector<stream_id> streams;
    db_clock::time_point timestamp;

    streams_version(utils::chunked_vector<stream_id> s, db_clock::time_point ts)
        : streams(std::move(s))
        , timestamp(ts)
    {}
};

class no_generation_data_exception : public std::runtime_error {
public:
    no_generation_data_exception(cdc::generation_id generation_ts)
        : std::runtime_error(format("could not find generation data for timestamp {}", generation_ts))
    {}
};

/* Should be called when we're restarting and we noticed that we didn't save any streams timestamp in our local tables,
 * which means that we're probably upgrading from a non-CDC/old CDC version (another reason could be
 * that there's a bug, or the user messed with our local tables).
 *
 * It checks whether we should be the node to propose the first generation of CDC streams.
 * The chosen condition is arbitrary, it only tries to make sure that no two nodes propose a generation of streams
 * when upgrading, and nothing bad happens if they for some reason do (it's mostly an optimization).
 */
bool should_propose_first_generation(const locator::host_id& me, const gms::gossiper&);

/*
 * Checks if the CDC generation is optimal, which is true if its `topology_description` is consistent
 * with `token_metadata`.
*/
bool is_cdc_generation_optimal(const cdc::topology_description& gen, const locator::token_metadata& tm);

/*
 * Generate a set of CDC stream identifiers such that for each shard
 * and vnode pair there exists a stream whose token falls into this vnode
 * and is owned by this shard. It is sometimes not possible to generate
 * a CDC stream identifier for some (vnode, shard) pair because not all
 * shards have to own tokens in a vnode. Small vnode can be totally owned
 * by a single shard. In such case, a stream identifier that maps to
 * end of the vnode is generated.
 *
 * Then build a cdc::topology_description which maps tokens to generated
 * stream identifiers, such that if token T is owned by shard S in vnode V,
 * it gets mapped to the stream identifier generated for (S, V).
 *
 * Run in seastar::async context.
 */
cdc::topology_description make_new_generation_description(
    const std::unordered_set<dht::token>& bootstrap_tokens,
    const noncopyable_function<std::pair<size_t, uint8_t> (dht::token)>& get_sharding_info,
    const locator::token_metadata_ptr);

db_clock::time_point new_generation_timestamp(bool add_delay, std::chrono::milliseconds ring_delay);

// Translates the CDC generation data given by a `cdc::topology_description` into a vector of mutations,
// using `mutation_size_threshold` to decide on the mutation sizes. The partition key of each mutation
// is given by `gen_uuid`. The timestamp of each cell in each mutation is given by `mutation_timestamp`.
//
// Works only for the CDC_GENERATIONS_V2 schema (in system_distributed keyspace).
future<utils::chunked_vector<mutation>> get_cdc_generation_mutations_v2(
    schema_ptr, utils::UUID gen_uuid, const cdc::topology_description&,
    size_t mutation_size_threshold, api::timestamp_type mutation_timestamp);

// The partition key of all rows in the single-partition CDC_GENERATIONS_V3 schema (in system keyspace).
static constexpr auto CDC_GENERATIONS_V3_KEY = "cdc_generations";

// Translates the CDC generation data given by a `cdc::topology_description` into a vector of mutations,
// using `mutation_size_threshold` to decide on the mutation sizes. The first clustering key column is
// given by `gen_uuid`. The timestamp of each cell in each mutation is given by `mutation_timestamp`.
//
// Works only for the CDC_GENERATIONS_V3 schema (in system keyspace).
future<utils::chunked_vector<mutation>> get_cdc_generation_mutations_v3(
    schema_ptr, utils::UUID gen_uuid, const cdc::topology_description&,
    size_t mutation_size_threshold, api::timestamp_type mutation_timestamp);

} // namespace cdc

#if FMT_VERSION < 100000
// fmt v10 introduced formatter for std::exception
template <>
struct fmt::formatter<cdc::no_generation_data_exception> : fmt::formatter<string_view> {
    auto format(const cdc::no_generation_data_exception& e, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", e.what());
    }
};
#endif
