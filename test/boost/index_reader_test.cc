/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/test_case.hh>

#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/make_random_string.hh"

#include "readers/from_mutations_v2.hh"

using namespace sstables;


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
