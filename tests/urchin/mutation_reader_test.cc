/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "tests/urchin/mutation_assertions.hh"

#include "mutation_reader.hh"
#include "core/do_with.hh"
#include "schema_builder.hh"

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

static future<> require_produces_next(mutation_reader& reader, mutation m) {
    return reader().then([m = std::move(m)] (mutation_opt&& mo) {
        BOOST_REQUIRE(bool(mo));
        assert_that(*mo).is_equal_to(m);
    });
}

static future<> require_end_of_stream(mutation_reader& reader) {
    return reader().then([] (mutation_opt&& mo) {
        if (bool(mo)) {
            BOOST_FAIL(sprint("Expected end of stream, got %s", *mo));
        }
    });
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_the_same_row) {
    auto s = make_schema();

    mutation m1(partition_key::from_single_value(*s, "key1"), s);
    m1.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v1"), 1);

    mutation m2(partition_key::from_single_value(*s, "key1"), s);
    m2.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v2"), 2);

    return do_with(make_combined_reader({make_reader_returning(m1), make_reader_returning(m2)}), [m2] (auto& cr) {
        return require_produces_next(cr, m2).then([&cr] {
            return require_end_of_stream(cr);
        });
    });
}

SEASTAR_TEST_CASE(test_combining_two_non_overlapping_readers) {
    auto s = make_schema();

    mutation m1(partition_key::from_single_value(*s, "keyB"), s);
    m1.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v1"), 1);

    mutation m2(partition_key::from_single_value(*s, "keyA"), s);
    m2.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v2"), 2);

    return do_with(make_combined_reader({make_reader_returning(m1), make_reader_returning(m2)}), [m2, m1] (auto& cr) {
        return require_produces_next(cr, m2).then([&cr, m1] {
            return require_produces_next(cr, m1);
        }).then([&cr] {
            return require_end_of_stream(cr);
        });
    });
}

SEASTAR_TEST_CASE(test_combining_two_partially_overlapping_readers) {
    auto s = make_schema();

    mutation m1(partition_key::from_single_value(*s, "keyA"), s);
    m1.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v1"), 1);

    mutation m2(partition_key::from_single_value(*s, "keyB"), s);
    m2.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v2"), 1);

    mutation m3(partition_key::from_single_value(*s, "keyC"), s);
    m3.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v3"), 1);

    return do_with(make_combined_reader({make_reader_returning_many({m1, m2}), make_reader_returning_many({m2, m3})}), [=] (auto& cr) {
        return require_produces_next(cr, m1).then([&cr, m2] {
            return require_produces_next(cr, m2);
        }).then([&cr, m3] {
            return require_produces_next(cr, m3);
        }).then([&cr] {
            return require_end_of_stream(cr);
        });
    });
}

SEASTAR_TEST_CASE(test_combining_one_reader_with_many_partitions) {
    auto s = make_schema();

    mutation m1(partition_key::from_single_value(*s, "keyA"), s);
    m1.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v1"), 1);

    mutation m2(partition_key::from_single_value(*s, "keyB"), s);
    m2.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v2"), 1);

    mutation m3(partition_key::from_single_value(*s, "keyC"), s);
    m3.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v3"), 1);

    return do_with(make_combined_reader({make_reader_returning_many({m1, m2, m3})}), [=] (auto& cr) {
        return require_produces_next(cr, m1).then([&cr, m2] {
            return require_produces_next(cr, m2);
        }).then([&cr, m3] {
            return require_produces_next(cr, m3);
        }).then([&cr] {
            return require_end_of_stream(cr);
        });
    });
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_one_reader_empty) {
    auto s = make_schema();
    mutation m1(partition_key::from_single_value(*s, "key1"), s);
    m1.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v1"), 1);

    return do_with(make_combined_reader({make_reader_returning(m1), make_empty_reader()}), [m1] (auto& cr) {
        return require_produces_next(cr, m1).then([&cr] {
            return require_end_of_stream(cr);
        });
    });
}

SEASTAR_TEST_CASE(test_combining_two_empty_readers) {
    return do_with(make_combined_reader({make_empty_reader(), make_empty_reader()}), [] (auto& cr) {
        return require_end_of_stream(cr);
    });
}

SEASTAR_TEST_CASE(test_combining_one_empty_reader) {
    return do_with(make_combined_reader({make_empty_reader()}), [] (auto& cr) {
        return require_end_of_stream(cr);
    });
}
