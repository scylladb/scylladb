/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>

#include "test/lib/eventually.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"

#include "db/config.hh"
#include "readers/from_mutations.hh"
#include "utils/bloom_filter.hh"
#include "utils/error_injection.hh"

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

void dispose_and_stop_tracking_bf_memory(shared_sstable&& sst, test_env_sstables_manager& mgr) {
    mgr.remove_sst_from_reclaimed(sst.get());
    shared_sstable::dispose(sst.release().release());
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
        // sst1 has the most reclaimable memory, so its filter should be reclaimed
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst1->filter_memory_size(); }, 0);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory);

        // Reclaim should also work on the latest sst being added
        auto [sst4, sst4_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 100);
        // sst4 should have been reclaimed
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst4->filter_memory_size(); }, 0);
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory + sst4_bf_memory);

        // Test auto reload - disposing sst3 should trigger reload of the
        // smallest filter in the reclaimed list, which is sst1's bloom filter.
        dispose_and_stop_tracking_bf_memory(std::move(sst3), sst_mgr);
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst1->filter_memory_size(); }, sst1_bf_memory);
        // only sst4's bloom filter memory should be reported as reclaimed
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst_mgr.get_total_memory_reclaimed(); }, sst4_bf_memory);
        // sst2 and sst4 remain the same
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst4->filter_memory_size(), 0);
    }, {
        // limit available memory to the sstables_manager to test reclaiming.
        // this will set the reclaim threshold to 100 bytes.
        .available_memory = 500
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
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst1->filter_memory_size(); }, 0);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory);

        // enable injector that delays reloading a filter
        utils::get_local_injector().enable("reload_reclaimed_components/pause", true);

        // dispose sst2 to trigger reload of sst1's bloom filter
        dispose_and_stop_tracking_bf_memory(std::move(sst2), sst_mgr);
        // _total_reclaimable_memory will be updated when the reload begins; wait for it.
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst_mgr.get_total_reclaimable_memory(); }, sst1_bf_memory);

        // now that the reload is midway and paused, create new sst;
        // it will not be reclaimed immediately as another reload is in progress
        auto [sst3, sst3_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 80);
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst3->filter_memory_size(); }, sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_reclaimable_memory(), sst1_bf_memory + sst3_bf_memory);
        // verify sst1 is not actually reloaded yet
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory);

        // Resume reloading sst1 filter - it will eventually be reclaimed again
        utils::get_local_injector().receive_message("reload_reclaimed_components/pause");
        // Eventually only sst3's bloom filter will be in memory
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst_mgr.get_total_reclaimable_memory(); }, sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory);
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);

        utils::get_local_injector().disable("reload_reclaimed_components/pause");
    }, {
        // limit available memory to the sstables_manager to test reclaiming.
        // this will set the reclaim threshold to 100 bytes.
        .available_memory = 500
    });
}

static void bloom_filters_require_equal(const utils::filter_ptr &f1, const utils::filter_ptr &f2) {
    auto filter1 = static_cast<utils::filter::bloom_filter*>(f1.get());
    auto filter2 = static_cast<utils::filter::bloom_filter*>(f2.get());
    BOOST_REQUIRE_EQUAL(filter1->memory_size(), filter2->memory_size());
    BOOST_REQUIRE_EQUAL(filter1->bits().size(), filter2->bits().size());
    BOOST_REQUIRE(filter1->bits().get_storage() == filter2->bits().get_storage());
}

SEASTAR_TEST_CASE(test_bloom_filters_with_bad_partition_estimate) {
    return test_env::do_with_async([](test_env& env) {
        simple_schema ss;
        auto schema = ss.schema();
        const auto actual_partition_count = 100;

        // Create a bloom filter with optimal size for the given partition count.
        utils::filter_ptr optimal_filter = utils::i_filter::get_filter(actual_partition_count, schema->bloom_filter_fp_chance(), utils::filter_format::m_format);

        // Generate mutations for the table and add the keys to the bloom filter
        utils::chunked_vector<mutation> mutations;
        auto pks = ss.make_pkeys(actual_partition_count);
        mutations.reserve(actual_partition_count);
        for (auto pk : pks) {
            auto mut = mutation(schema, pk);
            mut.partition().apply_insert(*schema, ss.make_ckey(1), ss.new_timestamp());
            mutations.push_back(std::move(mut));
            // add to optimal filter, so that we can verify the generated bloom filters against it
            optimal_filter->add(key::from_partition_key(*schema.get(), pk.key()).get_bytes());
        }

        for (auto estimated_partition_count : {
                 actual_partition_count / 2, // too low estimate
                 actual_partition_count * 2, // too large estimate
             }) {
            // create sstable with the estimated partition count
            auto sst = make_sstable_easy(env, make_mutation_reader_from_mutations(schema, env.make_reader_permit(), mutations),
                                         env.manager().configure_writer(), sstables::get_highest_sstable_version(), estimated_partition_count);

            // Verify that the filter was rebuilt into the optimal size
            bloom_filters_require_equal(sstables::test(sst).get_filter(), optimal_filter);
        }
    });
};

SEASTAR_TEST_CASE(test_bloom_filter_reload_after_unlink) {
    return test_env::do_with_async([] (test_env& env) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        simple_schema ss;
        auto schema = ss.schema();

        auto mut = mutation(schema, ss.make_pkey(1));
        mut.partition().apply_insert(*schema, ss.make_ckey(1), ss.new_timestamp());

        // bloom filter will be reclaimed automatically due to low memory
        auto sst = make_sstable_containing(env.make_sstable(schema), {mut});
        auto& sst_mgr = env.manager();
        BOOST_REQUIRE_EQUAL(sst->filter_memory_size(), 0);

        // manager's reclaimed set has the sst now
        auto& reclaimed_set = sst_mgr.get_reclaimed_set();
        BOOST_REQUIRE_EQUAL(reclaimed_set.size(), 1);
        BOOST_REQUIRE_EQUAL(fmt::to_string(reclaimed_set.begin()->get_filename()), fmt::to_string(sst->get_filename()));

        // hold a copy of shared sst object in async thread to test reload after unlink
        utils::get_local_injector().enable("test_bloom_filter_reload_after_unlink");
        auto async_sst_holder = seastar::async([sst] {
            // do nothing just hold a copy of sst and wait for message signalling test completion
            utils::get_local_injector().inject("test_bloom_filter_reload_after_unlink", [] (auto& handler) {
                auto ret = handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::seconds{5});
                return ret;
            }).get();
        });

        // unlink the sst and release the object
        sst->unlink().get();
        sst.release();

        // verify that the sstable manager's reclaimed set and the reclaim metrics
        // do not track unlinked sst1 at this point.
        BOOST_REQUIRE_EQUAL(sst_mgr.get_reclaimed_set().size(), 0);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), 0);

        // message async thread to complete waiting and thus release its copy of sst, triggering deactivation
        utils::get_local_injector().receive_message("test_bloom_filter_reload_after_unlink");
        async_sst_holder.get();

        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst_mgr.get_active_list().size(); }, 0);
    }, {
        // set available memory = 0 to force reclaim the bloom filter
        .available_memory = 0
    });
};

SEASTAR_TEST_CASE(test_bloom_filter_reclaim_after_unlink) {
    return test_env::do_with_async([] (test_env& env) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        fmt::print("Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n");
        return;
#endif
        simple_schema ss;
        auto schema = ss.schema();

        utils::chunked_vector<mutation> mutations;
        for (int i = 0; i < 10; i++) {
            auto mut = mutation(schema, ss.make_pkey(i));
            mut.partition().apply_insert(*schema, ss.make_ckey(1), ss.new_timestamp());
            mutations.push_back(std::move(mut));
        }

        // create one sst; there is sufficient memory for the bloom filter, so it won't be reclaimed
        auto sst1 = make_sstable_containing(env.make_sstable(schema), mutations);
        auto& sst_mgr = env.manager();
        auto sst1_filename = sst1->get_filename();
        BOOST_REQUIRE(sst1->filter_memory_size() != 0);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), 0);

        // hold a copy of shared sst object in async thread to test reclaim after unlink
        utils::get_local_injector().enable("test_bloom_filter_reload_after_unlink");
        auto async_sst_holder = seastar::async([sst1] {
            // do nothing just hold a copy of sst and wait for message signalling test completion
            utils::get_local_injector().inject("test_bloom_filter_reload_after_unlink", [] (auto& handler) {
                auto ret = handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::seconds{5});
                return ret;
            }).get();
        });

        // unlink sst1 - this will also reclaim the memory from sst1's bloom filter.
        sst1->unlink().get();
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);
        // verify that the sstable manager's reclaimed set and the reclaim metrics
        // do not track unlinked sst1 at this point.
        BOOST_REQUIRE_EQUAL(sst_mgr.get_reclaimed_set().size(), 0);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), 0);
        // release the sst1's reference from this thread
        sst1.release();
        // despite being unlinked and released, the sstable manager's _active list will still
        // hold sst1 as the async thread still has a reference.
        auto& active_list = sst_mgr.get_active_list();
        BOOST_REQUIRE_EQUAL(active_list.size(), 1);
        BOOST_REQUIRE_EQUAL(fmt::to_string(active_list.front().get_filename()), fmt::to_string(sst1_filename));

        // create another sst and unlink it to trigger reload of components.
        // the reload should not attempt to load sst'1 bloom filter into memory depsite its presence in the _active list.
        auto sst2 = make_sstable_containing(env.make_sstable(schema), {mutations[0]});
        sst2->unlink().get();
        sst2.release();

        // message async thread to complete waiting and thus release its copy of sst, triggering deactivation
        utils::get_local_injector().receive_message("test_bloom_filter_reload_after_unlink");
        async_sst_holder.get();

        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return active_list.size(); }, 0);
    }, {
        // set available memory = 0 to force reclaim the bloom filter
        .available_memory = 100
    });
};

SEASTAR_TEST_CASE(test_components_memory_reclaim_threshold_liveupdateness) {
    return test_env::do_with_async([] (test_env& env) {
        simple_schema ss;
        auto schema_ptr = ss.schema();
        auto& sst_mgr = env.manager();
        BOOST_REQUIRE_EQUAL(env.db_config().components_memory_reclaim_threshold(), 0.2);

        // create a few sstables and verify their bloom filters are still in memory
        auto [sst1, sst1_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 70);
        auto [sst2, sst2_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 50);
        auto [sst3, sst3_bf_memory] = create_sstable_with_bloom_filter(env, sst_mgr, schema_ptr, 20);
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), sst1_bf_memory);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), 0);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_reclaimable_memory(), sst1_bf_memory + sst2_bf_memory + sst3_bf_memory);

        // reduce the threshold to 0.1 and verify that sst1's bloom filter, which occupies most memory, gets evicted
        env.db_config().components_memory_reclaim_threshold.set(0.1);
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst1->filter_memory_size(); }, 0);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_memory_reclaimed(), sst1_bf_memory);
        // the other two ssts are untouched
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_reclaimable_memory(), sst2_bf_memory + sst3_bf_memory);

        // reduce the threshold to 0 and verify that no bloom filter is in memory
        env.db_config().components_memory_reclaim_threshold.set(0);
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst_mgr.get_total_memory_reclaimed(); }, sst1_bf_memory + sst2_bf_memory + sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_reclaimable_memory(), 0);
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), 0);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), 0);

        // increase threshold back 0.1 and expect sst2 and sst3's bloom filter to be reloaded
        env.db_config().components_memory_reclaim_threshold.set(0.1);
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst3->filter_memory_size(); }, sst3_bf_memory);
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst2->filter_memory_size(); }, sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_reclaimable_memory(), sst2_bf_memory + sst3_bf_memory);
        // sst1's bloom filter is not reloaded yet due to lack of available memory
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst_mgr.get_total_memory_reclaimed(); }, sst1_bf_memory);
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), 0);

        // increase threshold back to 0.2 and expect sst1 to be reloaded
        env.db_config().components_memory_reclaim_threshold.set(0.2);
        REQUIRE_EVENTUALLY_EQUAL<size_t>([&] { return sst_mgr.get_total_memory_reclaimed(); }, 0);
        BOOST_REQUIRE_EQUAL(sst1->filter_memory_size(), sst1_bf_memory);
        BOOST_REQUIRE_EQUAL(sst2->filter_memory_size(), sst2_bf_memory);
        BOOST_REQUIRE_EQUAL(sst3->filter_memory_size(), sst3_bf_memory);
        BOOST_REQUIRE_EQUAL(sst_mgr.get_total_reclaimable_memory(), sst1_bf_memory + sst2_bf_memory + sst3_bf_memory);
    }, {
        // limit available memory to the sstables_manager to test reclaiming.
        // this will set the reclaim threshold to 200 bytes.
        .available_memory = 1000
    });
}
