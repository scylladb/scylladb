/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/on_internal_error.hh>

#include "dht/fixed_shard.hh"
#include "dht/token.hh"
#include "schema/schema.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include "keys/keys.hh"
#include "keys/compound_compat.hh"
#include "utils/murmur_hash.hh"
#include "utils/log.hh"

namespace dht {

static logging::logger fslog("fixed_shard");

const sstring fixed_shard_partitioner::classname = "com.scylladb.dht.FixedShardPartitioner";

const sstring fixed_shard_partitioner::name() const {
    return classname;
}

dht::token fixed_shard_partitioner::token_for_shard(uint16_t shard, uint64_t hash_bits) {
    int64_t token_value = (static_cast<int64_t>(shard) << shard_shift) | static_cast<int64_t>(hash_bits & hash_mask);
    return dht::token(token_value);
}

unsigned fixed_shard_partitioner::shard_of(dht::token token) {
    uint64_t token_bits = static_cast<uint64_t>(token.raw());
    return static_cast<unsigned>(token_bits >> shard_shift);
}

// Called with the bytes of the first partition key component, representing the shard.
static uint16_t compute_shard(managed_bytes_view mb) {
    if (mb.size() != sizeof(uint16_t)) {
        on_internal_error(fslog, format("Invalid shard value size: expected {}, got {}", sizeof(uint16_t), mb.size()));
    }

    // No need to linearize, 2 bytes are represented as a single fragment
    auto shard_bytes = mb.current_fragment();
    uint16_t shard_value = net::ntoh(read_unaligned<uint16_t>(shard_bytes.begin()));

    if (shard_value > fixed_shard_partitioner::max_shard) {
        on_internal_error(fslog, format("Shard value {} exceeds maximum allowed shard {}", shard_value, fixed_shard_partitioner::max_shard));
    }

    return shard_value;
}

dht::token fixed_shard_partitioner::get_token(const schema& s, partition_key_view key) const {
    uint16_t shard_value = compute_shard(*key.begin());
    std::array<uint64_t, 2> hash;
    auto&& legacy = key.legacy_form(s);
    utils::murmur_hash::hash3_x64_128(legacy.begin(), legacy.size(), 0, hash);
    auto token = fixed_shard_partitioner::token_for_shard(shard_value, hash[0]);
    fslog.trace("get_token: shard={}, token={}", shard_value, token);
    return token;
}

dht::token fixed_shard_partitioner::get_token(const sstables::key_view& key) const {
    return key.with_linearized([&](bytes_view v) {
        auto comp = composite_view(v, true);
        uint16_t shard_value = compute_shard(comp.begin()->first);
        std::array<uint64_t, 2> hash;
        utils::murmur_hash::hash3_x64_128(v, 0, hash);
        auto token = fixed_shard_partitioner::token_for_shard(shard_value, hash[0]);
        fslog.trace("get_token: shard={}, token={}", shard_value, token);
        return token;
    });
}

using registry = class_registrator<dht::i_partitioner, fixed_shard_partitioner>;
static registry registrator(fixed_shard_partitioner::classname);
static registry registrator_short_name("FixedShardPartitioner");

fixed_shard_sharder& fixed_shard_sharder::instance() {
    static thread_local fixed_shard_sharder sharder;
    return sharder;
}

fixed_shard_sharder::fixed_shard_sharder()
    : static_sharder(smp::count, 0)
{
}

unsigned fixed_shard_sharder::shard_of(const dht::token& t) const {
    if (t.is_minimum()) {
        return dht::token::shard_of_minimum_token();
    }
    if (t.is_maximum()) {
        return shard_count() - 1;
    }
    auto shard = fixed_shard_partitioner::shard_of(t);
    fslog.trace("shard_of({}) = {}", t, std::min(shard, shard_count() - 1));
    return std::min(shard, shard_count() - 1);
}

std::optional<unsigned> fixed_shard_sharder::try_get_shard_for_reads(const dht::token& t) const {
    return shard_of(t);
}

dht::shard_replica_set fixed_shard_sharder::shard_for_writes(const dht::token& t, std::optional<dht::write_replica_set_selector>) const {
    // We don't support migrations of the data in raft tables for strongly consistent tables.
    // When migrating a strongly consistent tablet, we'll need to move its metadata
    // explicitly to the new shard along with its raft group data.
    auto shard = try_get_shard_for_reads(t);
    if (!shard) {
        return {};
    }
    return { *shard };
}

dht::token fixed_shard_sharder::token_for_next_shard(const dht::token& t, shard_id shard, unsigned spans) const {
    return token_for_next_shard_for_reads(t, shard, spans);
}

dht::token fixed_shard_sharder::token_for_next_shard_for_reads(const dht::token& t, shard_id shard, unsigned spans) const {
    // With the fixed_shard_partitioner, there's only one token range per shard, so spans > 1 always overflows.
    if (spans > 1 || shard >= shard_count() || t.is_maximum()) {
        return dht::maximum_token();
    }

    int64_t token_value = t.is_minimum() ? 0 : t.raw();
    int64_t start = static_cast<int64_t>(shard) << fixed_shard_partitioner::shard_shift;
    if (token_value < start) {
        return dht::token(start);
    }
    return dht::maximum_token();
}

std::optional<dht::shard_and_token> fixed_shard_sharder::next_shard(const dht::token& t) const {
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

std::optional<dht::shard_and_token> fixed_shard_sharder::next_shard_for_reads(const dht::token& t) const {
    return next_shard(t);
}

} // namespace dht
