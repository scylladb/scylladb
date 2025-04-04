/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include "compress.hh"
#include "schema/schema_fwd.hh"
#include "utils/updateable_value.hh"
#include <span>

namespace db {
class config;
} // namespace db

struct sstable_compressor_factory_impl;
class raw_dict;

struct sstable_compressor_factory {
    virtual ~sstable_compressor_factory() {}
    virtual future<compressor_ptr> make_compressor_for_writing(schema_ptr) = 0;
    virtual future<compressor_ptr> make_compressor_for_reading(sstables::compression&) = 0;
    virtual future<> set_recommended_dict(table_id, std::span<const std::byte> dict) = 0;
};

// Note: I couldn't make this an inner class of default_sstable_compressor_factory,
// because then the compiler gives weird complains about default member initializers in line
// ```
// default_sstable_compressor_factory(config = config{});
// ```
// apparently due to some compiler bug related default initializers.
struct default_sstable_compressor_factory_config {
    using self = default_sstable_compressor_factory_config;
    static std::vector<unsigned> get_default_shard_to_numa_node_mapping();
    bool register_metrics = false;
    utils::updateable_value<bool> enable_writing_dictionaries{true};
    utils::updateable_value<float> memory_fraction_starting_at_which_we_stop_writing_dicts{1};
    std::vector<unsigned> numa_config{get_default_shard_to_numa_node_mapping()};

    static default_sstable_compressor_factory_config from_db_config(
        const db::config&,
        std::span<const unsigned> numa_config = get_default_shard_to_numa_node_mapping());
};

// Constructs compressors and decompressors for SSTables,
// making sure that the expensive identical parts (dictionaries) are shared
// between all shards within the same NUMA group.
//
// To make coordination work without resorting to std::mutex and such, dicts have owner shards,
// decided by a content hash of the dictionary.
// All requests for a given dict ID go through the owner of this ID and return a foreign shared pointer
// to that dict.
//
// (Note: this centralization shouldn't pose a performance problem because a dict is only requested once
// per an opening of an SSTable).
struct default_sstable_compressor_factory : peering_sharded_service<default_sstable_compressor_factory>, sstable_compressor_factory {
    using impl = sstable_compressor_factory_impl;
public:
    using self = default_sstable_compressor_factory;
    using config = default_sstable_compressor_factory_config;
private:
    config _cfg;
    // Maps NUMA node ID to the array of shards on that node.
    std::vector<std::vector<shard_id>> _numa_groups;
    // Holds dictionaries owned by this shard.
    std::unique_ptr<sstable_compressor_factory_impl> _impl;
    // All recommended dictionary updates are serialized by a single "leader shard".
    // We do this to avoid dealing with concurrent updates altogether.
    semaphore _recommendation_setting_sem{1};
    constexpr static shard_id _leader_shard = 0;

private:
    using sha256_type = std::array<std::byte, 32>;
    unsigned local_numa_id();
    shard_id get_dict_owner(unsigned numa_id, const sha256_type& sha);
    future<foreign_ptr<lw_shared_ptr<const raw_dict>>> get_recommended_dict(table_id t);
    future<> set_recommended_dict_local(table_id, std::span<const std::byte> dict);
public:
    default_sstable_compressor_factory(config = config{});
    ~default_sstable_compressor_factory();

    future<compressor_ptr> make_compressor_for_writing(schema_ptr) override;
    future<compressor_ptr> make_compressor_for_reading(sstables::compression&) override;
    future<> set_recommended_dict(table_id, std::span<const std::byte> dict) override;
};

std::unique_ptr<sstable_compressor_factory> make_sstable_compressor_factory_for_tests_in_thread();
