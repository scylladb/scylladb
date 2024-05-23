/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/test_case.hh>

#include "test/lib/eventually.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"

#include "types/types.hh"
#include "utils/bloom_filter.hh"
#include "utils/i_filter.hh"
#include "utils/large_bitset.hh"

SEASTAR_TEST_CASE(test_sstable_reclaim_memory_from_components_and_reload_reclaimed_components) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto schema_ptr = ss.schema();
        auto sst = env.make_sstable(schema_ptr);

        // create a bloom filter
        auto sst_test = sstables::test(sst);
        sst_test.create_bloom_filter(100);
        sst_test.write_filter();
        auto total_reclaimable_memory = sst_test.total_reclaimable_memory_size();

        // Test sstable::reclaim_memory_from_components() :
        BOOST_REQUIRE_EQUAL(sst_test.reclaim_memory_from_components(), total_reclaimable_memory);
        // No more memory to reclaim in the sstable
        BOOST_REQUIRE_EQUAL(sst_test.total_reclaimable_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst->filter_memory_size(), 0);

        // Test sstable::reload_reclaimed_components() :
        // Reloading should load the bloom filter back into memory
        sst_test.reload_reclaimed_components();
        // SSTable should have reclaimable memory from the bloom filter
        BOOST_REQUIRE_EQUAL(sst_test.total_reclaimable_memory_size(), total_reclaimable_memory);
        BOOST_REQUIRE_EQUAL(sst->filter_memory_size(), total_reclaimable_memory);
    });
}

std::pair<shared_sstable, size_t> create_sstable_with_bloom_filter(test_env& env, test_env_sstables_manager& sst_mgr, schema_ptr sptr, uint64_t estimated_partitions) {
    auto sst = env.make_sstable(sptr);
    sstables::test(sst).create_bloom_filter(estimated_partitions);
    sstables::test(sst).write_filter();
    auto sst_bf_memory = sst->filter_memory_size();
    sst_mgr.increment_total_reclaimable_memory_and_maybe_reclaim(sst.get());
    return {sst, sst_bf_memory};
}

SEASTAR_TEST_CASE(test_sstable_manager_auto_reclaim_and_reload_of_bloom_filter) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto schema_ptr = ss.schema();

        auto& sst_mgr = env.manager();

        // Verify nothing it reclaimed when under threshold
        auto [sst1, sst1_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 70);
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), sst1_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), 0);

        auto [sst2, sst2_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 20);
        // Confirm reclaim was still not triggered
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), sst1_bf_memory);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), 0);

        // Verify manager reclaims from the largest sst when the total usage crosses thresold.
        auto [sst3, sst3_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 50);
        // sst1 has the most reclaimable memory
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory);

        // Reclaim should also work on the latest sst being added
        auto [sst4, sst4_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 100);
        // sst4 should have been reclaimed
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst4->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory + sst4_bf_memory);

        // Test auto reload - disposing sst3 should trigger reload of the
        // smallest filter in the reclaimed list, which is sst1's bloom filter.
        shared_sstable::dispose(sst3.release().release());
        REQUIRE_EVENTUALLY_EQUAL(sst1->filter_memory_size(), sst1_bf_memory);
        // only sst4's bloom filter memory should be reported as reclaimed
        REQUIRE_EVENTUALLY_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst4_bf_memory);
        // sst2 and sst4 remain the same
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst4->filter_memory_size(), 0);
    }, {
        // limit available memory to the sstables_manager to test reclaiming.
        // this will set the reclaim threshold to 100 bytes.
        .available_memory = 1000
    });
}

// Reproducer for https://github.com/scylladb/scylladb/issues/18398.
SEASTAR_TEST_CASE(test_reclaimed_bloom_filter_deletion_from_disk) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto s = ss.schema();
        auto pks = ss.make_pkeys(1);

        auto mut1 = mutation(s, pks[0]);
        mut1.partition().apply_insert(*s, ss.make_ckey(0), ss.new_timestamp());
        auto sst = make_sstable_containing(env.make_sstable(s), {std::move(mut1)});
        auto sst_test = sstables::test(sst);

        const auto filter_path = (env.tempdir().path() / sst_test.filename(component_type::Filter)).native();
        // confirm that the filter exists in disk
        BOOST_REQUIRE(file_exists(filter_path).get());

        // reclaim filter from memory and unlink the sst
        sst_test.reclaim_memory_from_components();
        sst->unlink().get();

        // verify the filter doesn't exist in disk anymore
        BOOST_REQUIRE(!file_exists(filter_path).get());
    });
}

SEASTAR_TEST_CASE(test_bloom_filter_reclaim_during_reload) {
    return test_env::do_with_async([](test_env& env) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        simple_schema ss;
        auto schema_ptr = ss.schema();

        auto& sst_mgr = env.manager();

        auto [sst1, sst1_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 100);
        // there is sufficient memory for sst1's filter
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), sst1_bf_memory);

        auto [sst2, sst2_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 60);
        // total memory used by the bloom filters has crossed the threshold, so sst1's
        // filter, which occupies the most memory, will be discarded from memory.
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory);

        // enable injector that delays reloading a filter
        utils::get_local_injector().enable("reload_reclaimed_components/pause", true);

        // dispose sst2 to trigger reload of sst1's bloom filter
        shared_sstable::dispose(sst2.release().release());
        // _total_reclaimable_memory will be updated when the reload begins; wait for it.
        REQUIRE_EVENTUALLY_EQUAL(sst_mgr.get_total_reclaimable_memory(), sst1_bf_memory);

        // now that the reload is midway and paused, create new sst to verify that its
        // filter gets evicted immediately as the memory that became available is reserved
        // for sst1's filter reload.
        auto [sst3, sst3_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 80);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), 0);
        // confirm sst1 is not reloaded yet
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory + sst3_bf_memory);

        // resume reloading sst1 filter
        utils::get_local_injector().receive_message("reload_reclaimed_components/pause");
        REQUIRE_EVENTUALLY_EQUAL(sst1->filter_memory_size(), sst1_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_reclaimable_memory(), sst1_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst3_bf_memory);

        utils::get_local_injector().disable("reload_reclaimed_components/pause");
    }, {
        // limit available memory to the sstables_manager to test reclaiming.
        // this will set the reclaim threshold to 100 bytes.
        .available_memory = 1000
    });
}

// Test correctness of folding method
SEASTAR_TEST_CASE(test_bloom_filter_folding) {
    return test_env::do_with_async([](test_env& env) {
        const int hashes = 5;

        for (auto [orig_num_bits, new_num_bits] : {
                 // test downsizing to a value aligned up to 64
                 std::pair{6400, 1600},
                 // test downsizing to a value not aligned up to 64
                 {6400, 800}}) {

            // Create a filter
            large_bitset orig_bitset(orig_num_bits);
            auto orig_filter = std::make_unique<utils::filter::murmur3_bloom_filter>(hashes, std::move(orig_bitset), utils::filter_format::m_format);

            // Create a smaller filter that will be used to verify the folding result
            large_bitset expected_bitset(new_num_bits);
            auto expected_filter = std::make_unique<utils::filter::murmur3_bloom_filter>(hashes, std::move(expected_bitset), utils::filter_format::m_format);

            // update both filters with the same keys
            for (int i = 0; i < 50; i++) {
                auto key = int32_type->decompose(i);
                orig_filter->add(key);
                expected_filter->add(key);
            }

            // fold the original filter into new_num_bits
            orig_filter->fold(new_num_bits);

            // verify the folded orig_filter against expected_filter
            BOOST_REQUIRE_EQUAL(orig_filter->memory_size(), expected_filter->memory_size());
            BOOST_REQUIRE_EQUAL(orig_filter->bits().size(), expected_filter->bits().size());
            BOOST_REQUIRE(orig_filter->bits().get_storage() == expected_filter->bits().get_storage());
        }
    });
}

// Test bloom filter loading from i_filter interface to verify that the fold size is computed properly
SEASTAR_TEST_CASE(test_bloom_filter_fold_size_computation) {
    return test_env::do_with_async([](test_env& env) {
        double max_false_pos_prob = 0.1;
        auto estimated_partition_count = 6000;
        auto actual_partition_count = 800;

        // Create a filter with estimated partition
        utils::filter_ptr orig_filter = utils::i_filter::get_filter(estimated_partition_count, max_false_pos_prob, utils::filter_format::m_format);
        auto orig_bf = static_cast<utils::filter::bloom_filter*>(orig_filter.get());
        auto orig_size = orig_bf->bits().memory_size();
        auto orig_num_bits = orig_bf->bits().size();
        BOOST_REQUIRE_EQUAL(orig_size, utils::i_filter::get_filter_size(estimated_partition_count, max_false_pos_prob));

        // update filters with keys
        for (int i = 0; i < actual_partition_count; i++) {
            auto key = int32_type->decompose(i);
            orig_filter->add(key);
        }

        // try fold the original filter into actual_partition_count
        utils::i_filter::maybe_fold_filter(orig_filter, actual_partition_count, max_false_pos_prob);

        auto expected_folded_size = utils::i_filter::get_filter_size(actual_partition_count, max_false_pos_prob);
        auto actual_folded_size = orig_bf->bits().memory_size();
        auto folded_num_bits = orig_bf->bits().size();

        // verify that the filter has been folded down
        BOOST_REQUIRE(expected_folded_size <= actual_folded_size && actual_folded_size < orig_size);
        // verify the filter is folded to a bitmap whose size is aligned to 64 and is a factor of original size
        BOOST_REQUIRE_EQUAL(folded_num_bits % 64, 0);
        BOOST_REQUIRE_EQUAL(orig_num_bits % folded_num_bits, 0);
    });
}
