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
    auto s = make_lw_shared<schema>(some_keyspace, some_column_family,
        std::vector<schema::column>({{"p1", utf8_type}}),
        std::vector<schema::column>({{"c1", int32_type}}),
        std::vector<schema::column>({{"r1", int32_type}}),
        utf8_type
    );

    column_family cf(s);

    column_definition& r1_col = *s->get_column_definition("r1");
    partition_key key = to_bytes("key1");
    clustering_key c_key = s->clustering_key_type->decompose_value({int32_type->decompose(2)});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(3)));
    cf.apply(m);

    row& row = cf.find_or_create_row(key, c_key);
    auto& cell = boost::any_cast<const atomic_cell&>(row[r1_col.id]);
    BOOST_REQUIRE(cell.is_live());
    BOOST_REQUIRE(int32_type->equal(cell.as_live().value, int32_type->decompose(3)));
}
