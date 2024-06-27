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
            std::make_optional(get_clustering_values_fixed_lengths(sst->get_serialization_header())),
            sst->manager().get_abort_source());

        // request abort before starting to consume index, so that the consumer throws an exception as soon as it starts
        env.request_abort();
        BOOST_CHECK_THROW(consumer_ctx.consume_input().get(), seastar::abort_requested_exception);
        consumer_ctx.close().get();
    });
}
