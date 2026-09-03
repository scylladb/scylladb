/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Covers join_table_results(), which lines an external search's answers up with the rows a base-table
// read returned. What it produces is handed back out by position, so the walk has to visit exactly
// the rows the result set is built from - which is what most of these cases pin.

#include <boost/test/unit_test.hpp>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include <seastar/testing/thread_test_case.hh>

#include "cql3/statements/external_search/external_search_provider.hh"
#include "dht/i_partitioner.hh"
#include "partition_slice_builder.hh"
#include "query/query-result-set.hh"
#include "query/query-result-writer.hh"
#include "readers/from_mutations.hh"
#include "readers/mutation_source.hh"
#include "replica/querier.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"

using namespace cql3::statements;
using primary_keys = vector_search::vector_store_client::primary_keys;

namespace {

schema_ptr make_schema(bool with_clustering_key) {
    auto builder = schema_builder(this_smp_shard_count(), "ks", "cf")
                           .with_column("pk", int32_type, column_kind::partition_key)
                           .with_column("s", int32_type, column_kind::static_column)
                           .with_column("v", int32_type, column_kind::regular_column);
    if (with_clustering_key) {
        builder.with_column("ck", int32_type, column_kind::clustering_key);
    }
    return builder.build();
}

partition_key pkey(const schema& s, int32_t v) {
    return partition_key::from_single_value(s, int32_type->decompose(v));
}

clustering_key ckey(const schema& s, int32_t v) {
    return clustering_key::from_single_value(s, int32_type->decompose(v));
}

mutation_source make_source(utils::chunked_vector<mutation> mutations) {
    return mutation_source([mutations = std::move(mutations)](schema_ptr s, reader_permit permit, const dht::partition_range&,
                                   const query::partition_slice& slice, tracing::trace_state_ptr, streamed_mutation::forwarding fwd,
                                   mutation_reader::forwarding) {
        return make_mutation_reader_from_mutations(s, std::move(permit), mutations, slice, fwd);
    });
}

/// The rows of `mutations`, read the way an external search reads them: the primary key of every row
/// is sent back, since that is what the answer is matched on.
query::result read_rows(schema_ptr s, reader_permit permit, utils::chunked_vector<mutation> mutations, const query::partition_slice& slice) {
    auto source = make_source(std::move(mutations));
    auto builder = query::result::builder(slice, query::result_options{query::result_request::only_result, query::digest_algorithm::none},
            query::result_memory_accounter{query::result_memory_limiter::unlimited_result_size}, query::max_tombstones);
    auto querier = replica::querier(source, s, std::move(permit), query::full_partition_range, slice, {}, tombstone_gc_state::no_gc());
    auto close_querier = deferred_close(querier);
    querier.consume_page(query_result_builder(*s, builder), std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint32_t>::max(),
                   gc_clock::now())
            .get();
    return builder.build();
}

query::partition_slice make_slice(const schema& s) {
    return partition_slice_builder(s)
            .with_option<query::partition_slice::option::send_partition_key>()
            .with_option<query::partition_slice::option::send_clustering_key>()
            .build();
}

/// How many rows the result set is built from - the number of joined rows the walk has to produce.
/// query::result_set walks the result with the same rule the CQL result-set builder does, static-only
/// partitions included, so it is an independent count of what the provider will be asked to fill.
size_t emitted_rows(schema_ptr s, const query::partition_slice& slice, const query::result& rows) {
    return query::result_set::from_raw_result(s, slice, rows).rows().size();
}

/// The joined rows of `table_results`.
std::vector<joined_row> join(schema_ptr s, const query::partition_slice& slice, const query::result& table_results,
        const primary_keys& external_results) {
    return join_table_results(table_results, slice, *s, external_results);
}

/// The answer each joined row was given, in the order the rows are emitted.
std::vector<std::optional<size_t>> answers_of(const std::vector<joined_row>& rows) {
    auto answers = std::vector<std::optional<size_t>>{};
    answers.reserve(rows.size());
    for (const auto& row : rows) {
        answers.push_back(row.answer);
    }
    return answers;
}

} // anonymous namespace

// query::result_set emits one row for a partition holding nothing but a static row, and the walk
// mirrors that rule, so this pins one against the other. Constructed with a full slice, which the
// search's own read never uses - it asks for a singular clustering range, and such a partition does
// not come back from that.
SEASTAR_THREAD_TEST_CASE(test_static_only_partition_gets_a_joined_row) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_schema(true);
    auto slice = make_slice(*s);

    auto with_rows = mutation(s, pkey(*s, 1));
    with_rows.set_clustered_cell(ckey(*s, 10), "v", data_value(100), api::new_timestamp());
    auto static_only = mutation(s, pkey(*s, 2));
    static_only.set_static_cell("s", data_value(7), api::new_timestamp());

    auto rows = read_rows(s, semaphore.make_permit(), {with_rows, static_only}, slice);
    // Nothing the index says matches either row; all that is under test is how many joined rows the
    // walk produces, and that it is one per emitted row.
    auto joined = join(s, slice, rows, primary_keys{});

    BOOST_REQUIRE_EQUAL(joined.size(), emitted_rows(s, slice, rows));
    BOOST_REQUIRE_EQUAL(joined.size(), 2u);
}

// The answers stay lined up across a result mixing partitions of several rows, none, and one.
SEASTAR_THREAD_TEST_CASE(test_answers_stay_aligned_with_the_rows) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_schema(true);
    auto slice = make_slice(*s);

    auto two_rows = mutation(s, pkey(*s, 1));
    two_rows.set_clustered_cell(ckey(*s, 10), "v", data_value(100), api::new_timestamp());
    two_rows.set_clustered_cell(ckey(*s, 20), "v", data_value(200), api::new_timestamp());
    auto static_only = mutation(s, pkey(*s, 2));
    static_only.set_static_cell("s", data_value(7), api::new_timestamp());
    auto one_row = mutation(s, pkey(*s, 3));
    one_row.set_clustered_cell(ckey(*s, 30), "v", data_value(300), api::new_timestamp());

    auto mutations = utils::chunked_vector<mutation>{two_rows, static_only, one_row};
    auto rows = read_rows(s, semaphore.make_permit(), mutations, slice);
    BOOST_REQUIRE_EQUAL(emitted_rows(s, slice, rows), 4u);

    // In the order the rows come back, which is the order the index's answer is merged into.
    auto ordered = std::vector<mutation>{two_rows, static_only, one_row};
    std::ranges::sort(ordered, [&](const mutation& a, const mutation& b) { return a.decorated_key().less_compare(*s, b.decorated_key()); });

    // Every row the index named is given its own answer; the static-only row is given none, and the
    // rows after it are unmoved by that.
    auto results = primary_keys{};
    auto expected = std::vector<std::optional<size_t>>{};
    for (const auto& m : ordered) {
        if (m.decorated_key().key().equal(*s, static_only.decorated_key().key())) {
            expected.push_back(std::nullopt); // no row of its own for the index to have named
            continue;
        }
        for (const auto& cr : m.partition().clustered_rows()) {
            expected.push_back(results.size());
            results.push_back({m.decorated_key(), cr.key(), 0.5f + results.size()});
        }
    }
    BOOST_REQUIRE_EQUAL(results.size(), 3u);

    auto joined = join(s, slice, rows, results);
    BOOST_REQUIRE_EQUAL(joined.size(), 4u);
    BOOST_REQUIRE(answers_of(joined) == expected);
}

// The index may still know a key whose row has since been deleted. Its answer is stepped over, and
// the rows after it keep their own.
SEASTAR_THREAD_TEST_CASE(test_stale_key_is_stepped_over) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_schema(true);
    auto slice = make_slice(*s);

    auto m = mutation(s, pkey(*s, 1));
    m.set_clustered_cell(ckey(*s, 10), "v", data_value(100), api::new_timestamp());
    m.set_clustered_cell(ckey(*s, 20), "v", data_value(200), api::new_timestamp());
    auto rows = read_rows(s, semaphore.make_permit(), {m}, slice);

    auto results = primary_keys{
            {m.decorated_key(), ckey(*s, 10), 0.5f},
            {m.decorated_key(), ckey(*s, 15), 0.25f}, // gone from the base table
            {m.decorated_key(), ckey(*s, 20), 0.75f},
    };
    auto expected = std::vector<std::optional<size_t>>{0, 2};
    BOOST_REQUIRE(answers_of(join(s, slice, rows, results)) == expected);
}

// A row the answer says nothing about is given none.
SEASTAR_THREAD_TEST_CASE(test_row_without_an_answer_gets_none) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_schema(true);
    auto slice = make_slice(*s);

    auto m = mutation(s, pkey(*s, 1));
    m.set_clustered_cell(ckey(*s, 10), "v", data_value(100), api::new_timestamp());
    m.set_clustered_cell(ckey(*s, 20), "v", data_value(200), api::new_timestamp());
    auto rows = read_rows(s, semaphore.make_permit(), {m}, slice);

    auto expected = std::vector<std::optional<size_t>>{0, std::nullopt};
    BOOST_REQUIRE(answers_of(join(s, slice, rows, primary_keys{{m.decorated_key(), ckey(*s, 10), 0.5f}})) == expected);
}

// A table with no clustering columns is matched on the partition key alone.
SEASTAR_THREAD_TEST_CASE(test_matching_without_a_clustering_key) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_schema(false);
    auto slice = make_slice(*s);

    auto first = mutation(s, pkey(*s, 1));
    first.set_clustered_cell(clustering_key::make_empty(), "v", data_value(100), api::new_timestamp());
    auto second = mutation(s, pkey(*s, 2));
    second.set_clustered_cell(clustering_key::make_empty(), "v", data_value(200), api::new_timestamp());

    // This test reads one range, so the rows come back in token order and the answers are built in
    // that order to match. Production reads a range per key and merges them in the index's order.
    auto ordered = std::vector<mutation>{first, second};
    std::ranges::sort(ordered, [&](const mutation& a, const mutation& b) { return a.decorated_key().less_compare(*s, b.decorated_key()); });
    auto rows = read_rows(s, semaphore.make_permit(), {first, second}, slice);

    auto results = primary_keys{
            {ordered[0].decorated_key(), clustering_key_prefix::make_empty(), 0.5f},
            {ordered[1].decorated_key(), clustering_key_prefix::make_empty(), 0.75f},
    };
    auto expected = std::vector<std::optional<size_t>>{0, 1};
    BOOST_REQUIRE(answers_of(join(s, slice, rows, results)) == expected);
}

// The shape production reads with: one singular clustering range per key the index named. A key whose
// row is gone brings its partition back with no rows at all - not as a static-only partition - so the
// rule mirrored above cannot fire on these reads.
SEASTAR_THREAD_TEST_CASE(test_a_singular_range_read_yields_no_static_only_partition) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_schema(true);

    auto m = mutation(s, pkey(*s, 1));
    m.set_static_cell("s", data_value(7), api::new_timestamp());
    m.set_clustered_cell(ckey(*s, 10), "v", data_value(100), api::new_timestamp());

    // Asks for a row that is not there, as the per-key read does for a key the index has outgrown.
    auto slice = partition_slice_builder(*s)
                         .with_option<query::partition_slice::option::send_partition_key>()
                         .with_option<query::partition_slice::option::send_clustering_key>()
                         .with_range(query::clustering_range::make_singular(ckey(*s, 20)))
                         .build();

    auto rows = read_rows(s, semaphore.make_permit(), {m}, slice);

    BOOST_REQUIRE_EQUAL(emitted_rows(s, slice, rows), 0u);
    BOOST_REQUIRE(join(s, slice, rows, primary_keys{}).empty());
}
