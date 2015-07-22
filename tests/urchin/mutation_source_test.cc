/*
 * Copyright 2015 Cloudius Systems
 */

#include "schema_builder.hh"
#include "mutation_reader_assertions.hh"
#include "mutation_source_test.hh"

// partitions must be sorted by decorated key
static void require_no_token_duplicates(const std::vector<mutation>& partitions) {
    std::experimental::optional<dht::token> last_token;
    for (auto&& p : partitions) {
        const dht::decorated_key& key = p.decorated_key();
        if (last_token && key.token() == *last_token) {
            BOOST_FAIL("token duplicate detected");
        }
        last_token = key.token();
    }
}

static void test_range_queries(populate_fn populate) {
    BOOST_MESSAGE("Testing range queries");

    auto s = schema_builder("ks", "cf")
        .with_column("key", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type)
        .build();

    auto make_partition_mutation = [s] (bytes key) -> mutation {
        mutation m(partition_key::from_single_value(*s, key), s);
        m.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v1"), 1);
        return m;
    };

    int partition_count = 300;

    std::vector<mutation> partitions;
    for (int i = 0; i < partition_count; ++i) {
        partitions.emplace_back(
            make_partition_mutation(to_bytes(sprint("key_%d", i))));
    }

    std::sort(partitions.begin(), partitions.end(), mutation_decorated_key_less_comparator());
    require_no_token_duplicates(partitions);

    dht::decorated_key key_before_all = partitions.front().decorated_key();
    partitions.erase(partitions.begin());

    dht::decorated_key key_after_all = partitions.back().decorated_key();
    partitions.pop_back();

    auto ds = populate(s, partitions);

    auto test_slice = [&] (query::range<dht::ring_position> r) {
        BOOST_MESSAGE(sprint("Testing range %s", r));
        assert_that(ds(r))
            .produces(slice(partitions, r))
            .produces_end_of_stream();
    };

    auto inclusive_token_range = [&] (size_t start, size_t end) {
        return query::partition_range::make(
            {partitions[start].token(), true},
            {partitions[end].token(), true});
    };

    test_slice(query::partition_range::make(
        {key_before_all, true}, {partitions.front().decorated_key(), true}));

    test_slice(query::partition_range::make(
        {key_before_all, false}, {partitions.front().decorated_key(), true}));

    test_slice(query::partition_range::make(
        {key_before_all, false}, {partitions.front().decorated_key(), false}));

    test_slice(query::partition_range::make(
        {key_before_all.token(), true}, {partitions.front().token(), true}));

    test_slice(query::partition_range::make(
        {key_before_all.token(), false}, {partitions.front().token(), true}));

    test_slice(query::partition_range::make(
        {key_before_all.token(), false}, {partitions.front().token(), false}));

    test_slice(query::partition_range::make(
        {partitions.back().decorated_key(), true}, {key_after_all, true}));

    test_slice(query::partition_range::make(
        {partitions.back().decorated_key(), true}, {key_after_all, false}));

    test_slice(query::partition_range::make(
        {partitions.back().decorated_key(), false}, {key_after_all, false}));

    test_slice(query::partition_range::make(
        {partitions.back().token(), true}, {key_after_all.token(), true}));

    test_slice(query::partition_range::make(
        {partitions.back().token(), true}, {key_after_all.token(), false}));

    test_slice(query::partition_range::make(
        {partitions.back().token(), false}, {key_after_all.token(), false}));

    test_slice(query::partition_range::make(
        {partitions[0].decorated_key(), false},
        {partitions[1].decorated_key(), true}));

    test_slice(query::partition_range::make(
        {partitions[0].decorated_key(), true},
        {partitions[1].decorated_key(), false}));

    test_slice(query::partition_range::make(
        {partitions[1].decorated_key(), true},
        {partitions[3].decorated_key(), false}));

    test_slice(query::partition_range::make(
        {partitions[1].decorated_key(), false},
        {partitions[3].decorated_key(), true}));

    test_slice(query::partition_range::make_ending_with(
        {partitions[3].decorated_key(), true}));

    test_slice(query::partition_range::make_starting_with(
        {partitions[partitions.size() - 4].decorated_key(), true}));

    test_slice(inclusive_token_range(0, 0));
    test_slice(inclusive_token_range(1, 1));
    test_slice(inclusive_token_range(2, 4));
    test_slice(inclusive_token_range(127, 128));
    test_slice(inclusive_token_range(128, 128));
    test_slice(inclusive_token_range(128, 129));
    test_slice(inclusive_token_range(127, 129));
    test_slice(inclusive_token_range(partitions.size() - 1, partitions.size() - 1));

    test_slice(inclusive_token_range(0, partitions.size() - 1));
    test_slice(inclusive_token_range(0, partitions.size() - 2));
    test_slice(inclusive_token_range(0, partitions.size() - 3));
    test_slice(inclusive_token_range(0, partitions.size() - 128));

    test_slice(inclusive_token_range(1, partitions.size() - 1));
    test_slice(inclusive_token_range(2, partitions.size() - 1));
    test_slice(inclusive_token_range(3, partitions.size() - 1));
    test_slice(inclusive_token_range(128, partitions.size() - 1));
}

void run_mutation_source_tests(populate_fn populate) {
    test_range_queries(populate);
}
