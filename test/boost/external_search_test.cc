/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Unit tests for what the ANN() and BM25() searches share, in cql3/statements/external_search.
//
// The queries themselves are covered by test/cqlpy/test_invalid_ann_queries.py,
// test/cqlpy/test_fulltext_index.py and the mock suites beside them, but they cannot cover all of
// unevaluated_equality(): `never` shows up as a rejection at prepare and `unknown` as one at
// execution, while `always` shows up as nothing at all - deferring the comparison instead reaches
// the same verdict, so no query can tell the two apart. It is also the answer that has to be
// right: `never` and `unknown` at worst report an error at the wrong time, but an `always` that
// should not have been given accepts a query whose selected term is not the one searched with.
//
// join_table_results() is tested here because a CQL test can only observe the outcome of a
// mismatch, a score or a fragment on the wrong row, and not which step of the matching went wrong.
// drop_unscored_rows() and similarities_of() are tested beside it: they decide which joined rows
// have a score to report, and read it.

#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/thread_test_case.hh>

#include "cql3/column_identifier.hh"
#include "cql3/column_specification.hh"
#include "cql3/expr/expression.hh"
#include "cql3/functions/native_scalar_function.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_search_provider.hh"
#include "dht/i_partitioner.hh"
#include "partition_slice_builder.hh"
#include "query/query-result-set.hh"
#include "query/query-result-writer.hh"
#include "readers/from_mutations.hh"
#include "readers/mutation_source.hh"
#include "replica/querier.hh"
#include "schema/schema_builder.hh"
#include "test/lib/expr_test_utils.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "types/types.hh"
#include "types/vector.hh"

#include <seastar/core/shared_ptr.hh>

using namespace cql3;
using namespace cql3::expr;
using namespace cql3::expr::test_utils;

using cql3::statements::external_search::drop_unscored_rows;
using cql3::statements::external_search::equality;
using cql3::statements::external_search::external_search_provider;
using cql3::statements::external_search::external_values;
using cql3::statements::external_search::join_table_results;
using cql3::statements::external_search::joined_row;
using cql3::statements::external_search::similarities_of;
using cql3::statements::external_search::unevaluated_equality;

using primary_keys = vector_search::vector_store_client::primary_keys;

BOOST_AUTO_TEST_SUITE(external_search_test)

namespace {

/// A bind marker standing for a query value. Each call synthesises its own receiver, the way
/// preparing each occurrence of one marker does - which is what makes two of them for one variable
/// compare unequal structurally, while the value they stand for is the same.
expression marker(int32_t bind_index, data_type type) {
    return bind_variable {
        .bind_index = bind_index,
        .receiver = make_lw_shared<column_specification>("ks", "tab",
                ::make_shared<column_identifier>("?", true), std::move(type)),
    };
}

/// A query vector of literals, as prepare folds one: into the value itself.
expression folded_vector(std::vector<constant> elements) {
    return make_vector_const(elements, float_type);
}

} // anonymous namespace

BOOST_AUTO_TEST_CASE(unevaluated_equality_literals) {
    // A folded query vector arrives as the value itself, so a pair of them is decided either way.
    BOOST_REQUIRE(unevaluated_equality(folded_vector({make_float_const(0.1f), make_float_const(0.2f)}),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.2f)})) == equality::always);
    BOOST_REQUIRE(unevaluated_equality(folded_vector({make_float_const(0.1f), make_float_const(0.2f)}),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.3f)})) == equality::never);
    BOOST_REQUIRE(unevaluated_equality(folded_vector({make_float_const(0.1f)}),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.2f)})) == equality::never);

    // A BM25() search term is one value rather than a vector of them, and is decided the same way.
    BOOST_REQUIRE(unevaluated_equality(make_text_const("hello"), make_text_const("hello")) == equality::always);
    BOOST_REQUIRE(unevaluated_equality(make_text_const("hello"), make_text_const("world")) == equality::never);
}

BOOST_AUTO_TEST_CASE(unevaluated_equality_bind_markers) {
    // One variable written twice is two nodes with two receivers and one value. This is what
    // expression::operator== gets wrong, because it compares the receivers.
    BOOST_REQUIRE(unevaluated_equality(marker(0, utf8_type), marker(0, utf8_type)) == equality::always);
    // Two variables may still be given one value, and a marker may be given what a literal holds.
    BOOST_REQUIRE(unevaluated_equality(marker(0, utf8_type), marker(1, utf8_type)) == equality::unknown);
    BOOST_REQUIRE(unevaluated_equality(marker(0, utf8_type), make_text_const("hello")) == equality::unknown);
    BOOST_REQUIRE(unevaluated_equality(make_text_const("hello"), marker(0, utf8_type)) == equality::unknown);

    // A whole query vector bound at once is the same case.
    const auto vector_type = data_type(vector_type_impl::get_instance(float_type, 2));
    BOOST_REQUIRE(unevaluated_equality(marker(0, vector_type), marker(0, vector_type)) == equality::always);
    BOOST_REQUIRE(unevaluated_equality(marker(0, vector_type),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.2f)})) == equality::unknown);
}

BOOST_AUTO_TEST_CASE(unevaluated_equality_leaves_the_rest_to_execution) {
    // A marker among the elements of a query vector leaves it unfolded, and the two are then
    // compared once they have been evaluated rather than reasoned about here.
    auto partly_bound = [] () {
        return expression(make_vector_constructor({make_float_const(0.1f), marker(1, float_type)},
                                                  float_type, 2));
    };
    BOOST_REQUIRE(unevaluated_equality(partly_bound(), partly_bound()) == equality::unknown);
    BOOST_REQUIRE(unevaluated_equality(partly_bound(),
                                       folded_vector({make_float_const(0.1f), make_float_const(0.2f)})) == equality::unknown);

    // So is a search term written as a function call or as a cast: two of those may well compute
    // one value, and proving when they do is not worth the code.
    auto fn = functions::make_native_scalar_function<true>(
            "external_search_test_fn", utf8_type, std::vector<data_type>{utf8_type},
            [] (std::span<const bytes_opt> args) -> bytes_opt { return args[0]; });
    auto call = [&] () {
        return expression(function_call{.func = fn, .args = {marker(0, utf8_type)}});
    };
    BOOST_REQUIRE(unevaluated_equality(call(), call()) == equality::unknown);

    auto to_text = [] () {
        return expression(cast{.style = cast::cast_style::c, .arg = marker(0, utf8_type), .type = utf8_type});
    };
    BOOST_REQUIRE(unevaluated_equality(to_text(), to_text()) == equality::unknown);
}

namespace {

// The join and the provider are read off a real query::result, built the way
// test/boost/mutation_query_test.cc builds one.

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
/// is sent back, since that is what an external result is matched on.
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

/// How many rows the result set is built from, counted by query::result_set, which walks the result
/// with the same rule the CQL result-set builder does - an independent count of the joined rows.
size_t emitted_rows(schema_ptr s, const query::partition_slice& slice, const query::result& rows) {
    return query::result_set::from_raw_result(s, slice, rows).rows().size();
}

float score_of(std::span<const cql3::raw_value> values, size_t row) {
    const auto& value = values[row];
    BOOST_REQUIRE(!value.is_null());
    return value.view().deserialize<float>(*float_type);
}

/// The joined rows of `table_results`, matched to `external_results` and with no columns read out of them.
std::vector<joined_row> join(schema_ptr s, const query::partition_slice& slice, const query::result& table_results,
        const primary_keys& external_results) {
    return join_table_results(table_results, slice, *s, *cql3::selection::selection::wildcard(s), &external_results, {});
}

int32_t int_of(const managed_bytes_opt& value) {
    BOOST_REQUIRE(value);
    return value_cast<int32_t>(int32_type->deserialize(managed_bytes_view(*value)));
}

/// An external result as similarities_of() sees it: it reads only the score, the join having settled
/// which result belongs to which row, so the keys need not be real.
vector_search::primary_key scored(float similarity) {
    return {dht::decorated_key{dht::token(), partition_key::make_empty()}, clustering_key_prefix::make_empty(), similarity};
}

/// The external result each joined row was matched to, in the order the rows are emitted.
std::vector<std::optional<size_t>> external_results_of(const std::vector<joined_row>& rows) {
    auto matched = std::vector<std::optional<size_t>>{};
    matched.reserve(rows.size());
    for (const auto& row : rows) {
        matched.push_back(row.external_result);
    }
    return matched;
}

} // anonymous namespace

// The matching stays lined up across a result mixing partitions of several rows, none, and one.
SEASTAR_THREAD_TEST_CASE(test_external_results_stay_aligned_with_the_rows) {
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

    // In the order the rows come back, which is the order the external results are merged into.
    auto ordered = std::vector<mutation>{two_rows, static_only, one_row};
    std::ranges::sort(ordered, [&](const mutation& a, const mutation& b) { return a.decorated_key().less_compare(*s, b.decorated_key()); });

    // Every row the index named is matched to its own result; the static-only row to none, and the
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
    BOOST_REQUIRE(external_results_of(joined) == expected);
}

// The index may still know a key whose row has since been deleted. Its result is stepped over, and
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
    BOOST_REQUIRE(external_results_of(join(s, slice, rows, results)) == expected);
}

// A row no external result names is matched to none.
SEASTAR_THREAD_TEST_CASE(test_row_without_an_external_result_gets_none) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_schema(true);
    auto slice = make_slice(*s);

    auto m = mutation(s, pkey(*s, 1));
    m.set_clustered_cell(ckey(*s, 10), "v", data_value(100), api::new_timestamp());
    m.set_clustered_cell(ckey(*s, 20), "v", data_value(200), api::new_timestamp());
    auto rows = read_rows(s, semaphore.make_permit(), {m}, slice);

    auto expected = std::vector<std::optional<size_t>>{0, std::nullopt};
    BOOST_REQUIRE(external_results_of(join(s, slice, rows, primary_keys{{m.decorated_key(), ckey(*s, 10), 0.5f}})) == expected);
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

    // This test reads one range, so the rows come back in token order and the results are built in
    // that order to match. Production reads a range per key and merges them in the index's order.
    auto ordered = std::vector<mutation>{first, second};
    std::ranges::sort(ordered, [&](const mutation& a, const mutation& b) { return a.decorated_key().less_compare(*s, b.decorated_key()); });
    auto rows = read_rows(s, semaphore.make_permit(), {first, second}, slice);

    auto results = primary_keys{
            {ordered[0].decorated_key(), clustering_key_prefix::make_empty(), 0.5f},
            {ordered[1].decorated_key(), clustering_key_prefix::make_empty(), 0.75f},
    };
    auto expected = std::vector<std::optional<size_t>>{0, 1};
    BOOST_REQUIRE(external_results_of(join(s, slice, rows, results)) == expected);
}

// What the index said about a row becomes that row's value, or drops it: a row it no longer names has
// no relevance to report, and neither does one it scored with something that is not a number.
BOOST_AUTO_TEST_CASE(test_similarities_are_read_off_the_joined_rows) {
    auto results = primary_keys{scored(0.5f), scored(std::numeric_limits<float>::quiet_NaN()), scored(0.75f)};
    auto rows = std::vector<joined_row>{{0}, {1}, {std::nullopt}, {2}};

    drop_unscored_rows(rows, results);
    auto similarities = similarities_of(rows, results);

    BOOST_REQUIRE_EQUAL(similarities.size(), 4u);
    BOOST_REQUIRE(!rows[0].dropped);
    BOOST_REQUIRE(rows[1].dropped); // not a number
    BOOST_REQUIRE(rows[2].dropped); // no external result
    BOOST_REQUIRE(!rows[3].dropped);
    BOOST_REQUIRE_EQUAL(score_of(similarities, 0), 0.5f);
    BOOST_REQUIRE_EQUAL(score_of(similarities, 3), 0.75f);
    BOOST_REQUIRE(similarities[1].is_null());
    BOOST_REQUIRE(similarities[2].is_null());
}

// drop_unscored_rows() only sets the flag: a row dropped before it is called stays dropped, and one
// with a score is left alone. Whether a dropped row's value is read is the provider's business.
BOOST_AUTO_TEST_CASE(test_a_row_already_dropped_stays_dropped) {
    auto results = primary_keys{scored(0.5f), scored(0.75f)};
    auto rows = std::vector<joined_row>{{.external_result = 0, .dropped = true}, {.external_result = 1}};

    drop_unscored_rows(rows, results);

    BOOST_REQUIRE(rows[0].dropped);
    BOOST_REQUIRE(!rows[1].dropped);
}

// A column of any kind can be read out of every row, for a follow-up request that needs what the
// index does not store. A key column is read from the key, the rest from the row's cells.
SEASTAR_THREAD_TEST_CASE(test_columns_are_read_out_of_every_row) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_schema(true);
    auto slice = make_slice(*s);
    auto selection = cql3::selection::selection::wildcard(s);
    auto columns = std::vector<const column_definition*>{
            s->get_column_definition("pk"),
            s->get_column_definition("ck"),
            s->get_column_definition("s"),
            s->get_column_definition("v"),
    };

    auto m = mutation(s, pkey(*s, 1));
    m.set_static_cell("s", data_value(7), api::new_timestamp());
    m.set_clustered_cell(ckey(*s, 10), "v", data_value(100), api::new_timestamp());
    m.set_clustered_cell(ckey(*s, 20), "v", data_value(200), api::new_timestamp());
    auto rows = read_rows(s, semaphore.make_permit(), {m}, slice);

    auto joined = join_table_results(rows, slice, *s, *selection, nullptr, columns);
    BOOST_REQUIRE_EQUAL(joined.size(), 2u);
    BOOST_REQUIRE_EQUAL(joined[0].columns.size(), 4u);
    BOOST_REQUIRE_EQUAL(int_of(joined[0].columns[0]), 1);
    BOOST_REQUIRE_EQUAL(int_of(joined[0].columns[1]), 10);
    BOOST_REQUIRE_EQUAL(int_of(joined[0].columns[2]), 7);
    BOOST_REQUIRE_EQUAL(int_of(joined[0].columns[3]), 100);
    BOOST_REQUIRE_EQUAL(int_of(joined[1].columns[1]), 20);
    BOOST_REQUIRE_EQUAL(int_of(joined[1].columns[3]), 200);

    // The pseudo-row of a partition holding nothing but a static row has no clustering key and no
    // cells of its own, and reads as absent rather than as another row's value.
    auto static_only = mutation(s, pkey(*s, 2));
    static_only.set_static_cell("s", data_value(9), api::new_timestamp());
    auto static_rows = read_rows(s, semaphore.make_permit(), {static_only}, slice);

    auto static_joined = join_table_results(static_rows, slice, *s, *selection, nullptr, columns);
    BOOST_REQUIRE_EQUAL(static_joined.size(), 1u);
    BOOST_REQUIRE_EQUAL(int_of(static_joined[0].columns[0]), 2);
    BOOST_REQUIRE(!static_joined[0].columns[1]);
    BOOST_REQUIRE_EQUAL(int_of(static_joined[0].columns[2]), 9);
    BOOST_REQUIRE(!static_joined[0].columns[3]);
}

// The provider's position moves for every row it is offered, dropped ones included - otherwise the
// rows after a dropped one would read their neighbour's value.
BOOST_AUTO_TEST_CASE(test_provider_advances_past_a_dropped_row) {
    auto values = std::vector<cql3::raw_value>{
            cql3::raw_value::make_null(),
            cql3::raw_value::make_value(float_type->decompose(0.75f)),
    };
    auto rows = std::vector<joined_row>{{.external_result = std::nullopt, .dropped = true}, {.external_result = 1}};
    auto provider = external_search_provider({external_values{.temporary_index = 1, .values = std::move(values)}}, rows);

    auto temporaries = std::vector<cql3::raw_value>{cql3::raw_value::make_null(), cql3::raw_value::make_null()};
    BOOST_REQUIRE(!provider.try_fill(temporaries));
    BOOST_REQUIRE(provider.try_fill(temporaries));
    BOOST_REQUIRE_EQUAL(temporaries[1].view().deserialize<float>(*float_type), 0.75f);
}

BOOST_AUTO_TEST_SUITE_END()
