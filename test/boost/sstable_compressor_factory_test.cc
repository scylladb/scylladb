/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#undef SEASTAR_TESTING_MAIN
#include <fmt/ranges.h>
#include <seastar/util/defer.hh>
#include <seastar/testing/thread_test_case.hh>
#include "sstables/sstable_compressor_factory.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"

BOOST_AUTO_TEST_SUITE(sstable_compressor_factory_test)

// 1. Create a random message.
// 2. Set this random message as the recommended dict.
// 3. On all shards, create compressors.
// 4. Check that they are using the recommended dict (i.e. that the original message compresses perfectly).
// 5. Check that the used dictionaries are owned by shards on the same NUMA node.
// 6. Check that the number of dictionary copies is equal to number of NUMA nodes.
// 7. Repeat this a few times for both lz4 and zstd.
void test_one_numa_topology(std::span<unsigned> shard_to_numa_mapping) {
    testlog.info("Testing NUMA topology {}", shard_to_numa_mapping);

    // Create a compressor factory.
    SCYLLA_ASSERT(shard_to_numa_mapping.size() == smp::count);
    auto config = default_sstable_compressor_factory::config{
        .numa_config = std::vector(shard_to_numa_mapping.begin(), shard_to_numa_mapping.end()),
    };
    sharded<default_sstable_compressor_factory> sstable_compressor_factory;
    sstable_compressor_factory.start(std::cref(config)).get();
    auto stop_compressor_factory = defer([&sstable_compressor_factory] { sstable_compressor_factory.stop().get(); });

    // The factory keeps recommended dicts (i.e. dicts for writing) per table ID.
    auto table = table_id::create_random_id();

    // Retry a few times just to check that it works more than once.
    for (int retry = 0; retry < 3; ++retry) {
        // Generate a random (and hence uhcompressible without a dict) message.
        auto message = tests::random::get_sstring(4096);
        auto dict_view = std::as_bytes(std::span(message));
        // Set the message as the dict to make the message perfectly compressible.
        sstable_compressor_factory.local().set_recommended_dict(table, dict_view).get();

        // We'll put the owners here to check that the number of owners matches the number of NUMA nodes.
        std::vector<unsigned> compressor_numa_nodes(smp::count);
        std::vector<unsigned> decompressor_numa_nodes(smp::count);

        // Try for both algorithms, just in case there are some differences in how dictionary
        // distribution over shards is implemented between them.
        for (const auto algo : {compressor::algorithm::lz4_with_dicts, compressor::algorithm::zstd_with_dicts}) {
            sstable_compressor_factory.invoke_on_all(coroutine::lambda([&] (default_sstable_compressor_factory& local) -> seastar::future<> {
                // Validate that the dictionaries work as intended,
                // and check that their owner is as expected.

                auto params = compression_parameters(algo);

                auto compressor = co_await local.make_compressor_for_writing_for_tests(params, table);
                auto decompressor = co_await local.make_compressor_for_reading_for_tests(params, dict_view);

                auto our_numa_node = shard_to_numa_mapping[this_shard_id()];
                auto compressor_numa_node = shard_to_numa_mapping[compressor->get_dict_owner_for_test().value()];
                auto decompressor_numa_node = shard_to_numa_mapping[decompressor->get_dict_owner_for_test().value()];

                // Check that the dictionary used by this shard lies on the same NUMA node.
                // This is important to avoid cross-node memory accesses on the hot path.
                BOOST_CHECK_EQUAL(our_numa_node, compressor_numa_node);
                BOOST_CHECK_EQUAL(our_numa_node, decompressor_numa_node);

                compressor_numa_nodes[this_shard_id()] = compressor_numa_node;
                decompressor_numa_nodes[this_shard_id()] = compressor_numa_node;

                auto output_max_size = compressor->compress_max_size(message.size());
                auto compressed = std::vector<char>(output_max_size);
                auto compressed_size = compressor->compress(
                    reinterpret_cast<const char*>(message.data()), message.size(),
                    reinterpret_cast<char*>(compressed.data()), compressed.size());
                BOOST_REQUIRE_GE(compressed_size, 0);
                compressed.resize(compressed_size);

                // Validate that the recommeded dict was actually used.
                BOOST_CHECK(compressed.size() < message.size() / 10);

                auto decompressed = std::vector<char>(message.size());
                auto decompressed_size = decompressor->uncompress(
                    reinterpret_cast<const char*>(compressed.data()), compressed.size(),
                    reinterpret_cast<char*>(decompressed.data()), decompressed.size());
                BOOST_REQUIRE_GE(decompressed_size, 0);
                decompressed.resize(decompressed_size);

                // Validate that the roundtrip through compressor and decompressor
                // resulted in the original message.
                BOOST_CHECK_EQUAL_COLLECTIONS(message.begin(), message.end(), decompressed.begin(), decompressed.end());
            })).get();
        }

        // Check that the number of owners (and hence, copies) is equal to the number
        // of NUMA nodes.
        // This isn't that important, but we don't want to duplicate dictionaries
        // within a NUMA node unnecessarily.
        BOOST_CHECK_EQUAL(
            std::set(compressor_numa_nodes.begin(), compressor_numa_nodes.end()).size(),
            std::set(shard_to_numa_mapping.begin(), shard_to_numa_mapping.end()).size()
        );
        BOOST_CHECK_EQUAL(
            std::set(decompressor_numa_nodes.begin(), decompressor_numa_nodes.end()).size(),
            std::set(shard_to_numa_mapping.begin(), shard_to_numa_mapping.end()).size()
        );
    }
}

SEASTAR_THREAD_TEST_CASE(test_numa_awareness) {
    {
        std::vector<unsigned> one_numa_node(smp::count);
        test_one_numa_topology(one_numa_node);
    }
    {
        std::vector<unsigned> two_numa_nodes(smp::count);
        for (size_t i = 0; i < two_numa_nodes.size(); ++i) {
            two_numa_nodes[i] = i % 2;
        }
        test_one_numa_topology(two_numa_nodes);
    }
    {
        std::vector<unsigned> n_numa_nodes(smp::count);
        for (size_t i = 0; i < n_numa_nodes.size(); ++i) {
            n_numa_nodes[i] = i;
        }
        test_one_numa_topology(n_numa_nodes);
    }
}

BOOST_AUTO_TEST_SUITE_END()
