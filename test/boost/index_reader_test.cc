/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>

#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/make_random_string.hh"

#include "readers/from_mutations_v2.hh"

using namespace sstables;

SEASTAR_TEST_CASE(test_abort_during_index_read) {
    return test_env::do_with_async([](test_env& env) {
        simple_schema ss;
        auto schema_ptr = ss.schema();
        auto mut = mutation(schema_ptr, ss.make_pkey());
        auto mut_reader = make_mutation_reader_from_mutations_v2(schema_ptr, env.make_reader_permit(), std::move(mut));
        auto sst = make_sstable_easy(env, std::move(mut_reader), env.manager().configure_writer());

        struct dummy_index_consumer {
            dummy_index_consumer() {}
            void consume_entry(parsed_partition_index_entry&& e) {
                // should be aborted before reaching here
                BOOST_ERROR("index_consume_entry_context was not aborted");
            }
        } consumer;

        auto index_file = sst->index_file();
        auto index_file_size = sst->index_size();
        index_consume_entry_context<dummy_index_consumer> consumer_ctx(
            *sst, env.make_reader_permit(), consumer, trust_promoted_index::no,
            make_file_input_stream(index_file, 0, index_file_size), 0, index_file_size,
            sst->get_column_translation(),
            sst->manager().get_abort_source());

        // request abort before starting to consume index, so that the consumer throws an exception as soon as it starts
        env.request_abort();
        BOOST_CHECK_THROW(consumer_ctx.consume_input().get(), seastar::abort_requested_exception);
        consumer_ctx.close().get();
    });
}

SEASTAR_TEST_CASE(test_promoted_index_parsing_page_crossing_and_retries) {
    return test_env::do_with_async([](test_env& env) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
        testlog.info("Skipped because error injection is not enabled");
#else
        simple_schema ss;
        auto s = ss.schema();

        auto pk = ss.make_pkey();
        auto mut = mutation(s, pk);

        // enough to have same index block whose clustering key is split across pages
        std::vector<clustering_key> keys;
        const auto n_keys = 100;
        auto key_size = cached_file::page_size / 3; // guarantees that index blocks are not congruent with page size.
        keys.reserve(n_keys);
        for (int i = 0; i < n_keys; ++i) {
            keys.push_back(ss.make_ckey(make_random_string(key_size)));
            ss.add_row(mut, keys[i], "v");
        }

        clustering_key::less_compare less(*s);
        std::sort(keys.begin(), keys.end(), less);

        env.manager().set_promoted_index_block_size(1); // force entry for each row
        auto mut_reader = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), std::move(mut));
        auto sst = make_sstable_easy(env, std::move(mut_reader), env.manager().configure_writer());

        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();
        tracing::trace_state_ptr trace = nullptr;

        auto index = std::make_unique<index_reader>(sst, permit, trace, use_caching::yes, true);
        auto close_index = deferred_close(*index);

        index->advance_to(dht::ring_position_view(pk)).get();
        index->read_partition_data().get();

        auto cur = dynamic_cast<mc::bsearch_clustered_cursor*>(index->current_clustered_cursor());
        BOOST_REQUIRE(cur);

        std::optional<cached_file::offset_type> prev_offset;
        int crossed_page = 0;

        utils::get_local_injector().enable("cached_promoted_index_parsing_invalidate_buf_across_page", false);

        for (int i = 0; i < n_keys - 1; ++i) {
            auto block_offset = cur->promoted_index().get_block_only_offset(i, trace).get()->offset;
            auto next_block_offset = cur->promoted_index().get_block_only_offset(i + 1, trace).get()->offset;

            auto start_page = block_offset / cached_file::page_size;
            auto end_page = (next_block_offset - 1) / cached_file::page_size;
            if (start_page != end_page) {
                auto pos = position_in_partition::for_key(keys[i]);
                position_in_partition::equal_compare eq(*s);

                testlog.info("Crossed page at block {}, offset [{}, {})", i, block_offset, next_block_offset);
                crossed_page++;

                auto* block = cur->promoted_index().get_block(i, trace).get();

                testlog.debug("key   : {}", pos);
                testlog.debug("start : {}", *block->start);
                testlog.debug("end   : {}", *block->end);

                BOOST_REQUIRE(eq(*block->start, pos));
                BOOST_REQUIRE(eq(*block->end, pos));
                if (prev_offset) {
                    BOOST_REQUIRE_LT(*prev_offset, block->data_file_offset);
                }

                cur->promoted_index().clear();

                utils::get_local_injector().enable("cached_promoted_index_bad_alloc_parsing_across_page", true);
                block = cur->promoted_index().get_block(i, trace).get();

                testlog.debug("start : {}", *block->start);
                testlog.debug("end   : {}", *block->end);
                BOOST_REQUIRE(eq(*block->start, pos));
                BOOST_REQUIRE(eq(*block->end, pos));
                if (prev_offset) {
                    BOOST_REQUIRE_LT(*prev_offset, block->data_file_offset);
                }

                prev_offset = block->data_file_offset;
            }
        }

        BOOST_REQUIRE_GE(crossed_page, 6); // If not, increase n_keys
#endif
    });
}

SEASTAR_TEST_CASE(test_no_data_file_read_on_missing_clustering_keys_with_dense_index) {
    return test_env::do_with_async([](test_env& env) {
        simple_schema ss;
        auto s = ss.schema();

        auto pk = ss.make_pkey();

        // enough to have same index block whose clustering key is split across pages
        clustering_key::less_compare less(*s);
        std::set<clustering_key, clustering_key::less_compare> keys(less); // present in data file
        std::set<clustering_key, clustering_key::less_compare> missing_keys(less);
        const auto n_keys = 13;
        auto key_size = 32; // whatever

        while (keys.size() < n_keys) {
            auto key = ss.make_ckey(make_random_string(key_size));
            keys.emplace(key);
        }

        while (missing_keys.size() < n_keys) {
            auto key = ss.make_ckey(make_random_string(key_size));
            if (!keys.contains(key)) {
                missing_keys.emplace(key);
            }
        }

        // Make sure there are missing keys before and after all present keys
        // to stress edge cases.
        {
            auto first_key = *keys.begin();
            auto last_key = *std::prev(keys.end());
            missing_keys.emplace(first_key);
            missing_keys.emplace(last_key);
            keys.erase(first_key);
            keys.erase(last_key);
        }

        auto last_key = *std::prev(keys.end());

        auto mut = mutation(s, pk);
        for (auto key : keys) {
            ss.add_row(mut, key, "vvvv");
        }

        env.manager().set_promoted_index_block_size(1); // force entry for each row
        auto mut_reader = make_mutation_reader_from_mutations_v2(s, env.make_reader_permit(), std::move(mut));
        auto sst = make_sstable_easy(env, std::move(mut_reader), env.manager().configure_writer());

        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();
        tracing::trace_state_ptr trace = nullptr;

        auto index = std::make_unique<index_reader>(sst, permit, trace, use_caching::yes, true);
        auto close_index = deferred_close(*index);

        index->advance_to(dht::ring_position_view(pk)).get();
        index->read_partition_data().get();

        auto index2 = std::make_unique<index_reader>(sst, permit, trace, use_caching::yes, true);
        auto close_index2 = deferred_close(*index2);
        index2->advance_to(dht::ring_position_view(pk)).get();
        index2->advance_to_next_partition().get();
        auto next_partition_offset = index2->data_file_positions().start;

        for (auto key : missing_keys) {
            auto cur = dynamic_cast<mc::bsearch_clustered_cursor*>(index->current_clustered_cursor());
            BOOST_REQUIRE(cur);

            testlog.info("Testing missing key {}", key);
            auto skip_lb = cur->advance_to(position_in_partition::before_key(key)).get();
            auto skip_ub = cur->probe_upper_bound(position_in_partition::after_key(*s, key)).get();
            uint64_t lb_offset = index->get_data_file_position();
            uint64_t ub_offset = next_partition_offset;
            if (skip_lb) {
                lb_offset = skip_lb->offset;
            }
            if (skip_ub) {
                ub_offset = *skip_ub;
            }
            BOOST_REQUIRE_EQUAL(lb_offset, ub_offset);
            index->reset_clustered_cursor().get();
        }

        position_in_partition::equal_compare eq(*s);

        // Check that for each present key, single-row reads will read a single block
        for (auto key : keys) {
            auto cur = dynamic_cast<mc::bsearch_clustered_cursor*>(index->current_clustered_cursor());
            BOOST_REQUIRE(cur);

            testlog.info("Testing present key {}", key);
            auto skip_lb = cur->advance_to(position_in_partition::before_key(key)).get();
            auto skip_ub = cur->probe_upper_bound(position_in_partition::after_key(*s, key)).get();
            uint64_t lb_offset = index->get_data_file_position();
            uint64_t ub_offset = next_partition_offset;
            if (skip_lb) {
                lb_offset = skip_lb->offset;
            }
            if (skip_ub) {
                ub_offset = *skip_ub;
            }

            // All blocks have the same width, take any of them.
            auto expected_width = cur->promoted_index().get_block(0, trace).get()->width;
            BOOST_REQUIRE_EQUAL(ub_offset - lb_offset, expected_width);

            index->reset_clustered_cursor().get();
        }
    });
}
