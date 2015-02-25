/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/included/unit_test.hpp>
#include "core/sstring.hh"
#include "database.hh"

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static boost::any make_atomic_cell(bytes value) {
    return atomic_cell{0, atomic_cell::live{ttl_opt{}, std::move(value)}};
};

BOOST_AUTO_TEST_CASE(test_mutation_is_applied) {
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, utf8_type));

    column_family cf(s);

    column_definition& r1_col = *s->get_column_definition("r1");
    partition_key key = to_bytes("key1");
    clustering_key c_key = s->clustering_key_type->decompose_value({int32_type->decompose(2)});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(3)));
    cf.apply(std::move(m));

    row& row = cf.find_or_create_row(key, c_key);
    auto& cell = boost::any_cast<const atomic_cell&>(row[r1_col.id]);
    BOOST_REQUIRE(cell.is_live());
    BOOST_REQUIRE(int32_type->equal(cell.as_live().value, int32_type->decompose(3)));
}

BOOST_AUTO_TEST_CASE(test_row_tombstone_updates) {
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, utf8_type));

    column_family cf(s);

    partition_key key = to_bytes("key1");

    clustering_key c_key1 = s->clustering_key_type->decompose_value(
        {int32_type->decompose(1)}
    );

    clustering_key c_key2 = s->clustering_key_type->decompose_value(
        {int32_type->decompose(2)}
    );

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(key, s);
    m.p.apply_row_tombstone(s, c_key1, tombstone(1, ttl));
    m.p.apply_row_tombstone(s, c_key2, tombstone(0, ttl));

    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, c_key1), tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, c_key2), tombstone(0, ttl));

    m.p.apply_row_tombstone(s, c_key2, tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, c_key2), tombstone(1, ttl));
}
