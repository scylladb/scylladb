/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "frozen_mutation.hh"
#include "schema_builder.hh"
#include "tests/urchin/mutation_assertions.hh"

static schema_builder new_table() {
    return { "some_keyspace", "some_table" };
}

static api::timestamp_type new_timestamp() {
    static api::timestamp_type t = 0;
    return t++;
};

static tombstone new_tombstone() {
    return { new_timestamp(), gc_clock::now() };
};

BOOST_AUTO_TEST_CASE(test_writing_and_reading) {
    schema_ptr s = new_table()
        .with_column("pk_col", bytes_type, column_kind::partition_key)
        .with_column("ck_col_1", bytes_type, column_kind::clustering_key)
        .with_column("ck_col_2", bytes_type, column_kind::clustering_key)
        .with_column("regular_col_1", bytes_type)
        .with_column("regular_col_2", bytes_type)
        .with_column("static_col_1", bytes_type, column_kind::static_column)
        .with_column("static_col_2", bytes_type, column_kind::static_column)
        .build();

    partition_key key = partition_key::from_single_value(*s, bytes("key"));
    clustering_key ck1 = clustering_key::from_deeply_exploded(*s, {bytes("ck1_0"), bytes("ck1_1")});
    clustering_key ck2 = clustering_key::from_deeply_exploded(*s, {bytes("ck2_0"), bytes("ck2_1")});
    auto ttl = gc_clock::duration(1);

    auto test_freezing = [] (const mutation& m) {
        assert_that(freeze(m).unfreeze(m.schema())).is_equal_to(m);
    };

    mutation m(key, s);
    m.partition().apply(new_tombstone());

    test_freezing(m);

    m.partition().apply_delete(s, ck2, new_tombstone());

    test_freezing(m);

    m.partition().apply_row_tombstone(*s, clustering_key_prefix::from_deeply_exploded(*s, {bytes("ck2_0")}), new_tombstone());

    test_freezing(m);

    m.set_clustered_cell(ck1, "regular_col_1", bytes("regular_col_value"), new_timestamp(), ttl);

    test_freezing(m);

    m.set_clustered_cell(ck1, "regular_col_2", bytes("regular_col_value"), new_timestamp(), ttl);

    test_freezing(m);

    m.partition().apply_insert(*s, ck2, new_timestamp());

    test_freezing(m);

    m.set_clustered_cell(ck2, "regular_col_1", bytes("ck2_regular_col_1_value"), new_timestamp());

    test_freezing(m);

    m.set_static_cell("static_col_1", bytes("static_col_value"), new_timestamp(), ttl);

    test_freezing(m);

    m.set_static_cell("static_col_2", bytes("static_col_value"), new_timestamp());

    test_freezing(m);
}
