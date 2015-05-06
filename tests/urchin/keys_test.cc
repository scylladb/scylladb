/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "keys.hh"
#include "schema.hh"
#include "types.hh"

BOOST_AUTO_TEST_CASE(test_key_is_prefixed_by) {
    schema s({}, "", "", {{"c1", bytes_type}}, {{"c2", bytes_type}, {"c3", bytes_type}, {"c4", bytes_type}}, {}, {}, utf8_type);

    auto key = clustering_key::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")});

    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a")})));
    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("b")})));
    BOOST_REQUIRE(key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")})));

    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes()})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("b"), bytes("c")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("a"), bytes("c"), bytes("b")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("abc")})));
    BOOST_REQUIRE(!key.is_prefixed_by(s, clustering_key_prefix::from_exploded(s, {bytes("ab")})));
}

BOOST_AUTO_TEST_CASE(test_key_component_iterator) {
    schema s({}, "", "",
        {
            {"c1", bytes_type}
        }, {
            {"c2", bytes_type}, {"c3", bytes_type}, {"c4", bytes_type}
        },
        {}, {}, utf8_type);

    auto key = clustering_key::from_exploded(s, {bytes("a"), bytes("b"), bytes("c")});

    auto i = key.begin(s);
    auto end = key.end(s);

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("a")));
    ++i;

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("b")));
    ++i;

    BOOST_REQUIRE(i != end);
    BOOST_REQUIRE(*i == bytes_view(bytes("c")));
    ++i;

    BOOST_REQUIRE(i == end);
}

BOOST_AUTO_TEST_CASE(test_legacy_ordering_for_non_composite_key) {
    schema s({}, "", "", {{"c1", bytes_type}}, {}, {}, {}, utf8_type);

    auto to_key = [&s] (sstring value) {
        return partition_key::from_single_value(s, to_bytes(value));
    };

    auto cmp = [&s] (const partition_key& k1, const partition_key& k2) {
        return k1.legacy_tri_compare(s, k2);
    };

    BOOST_REQUIRE(cmp(to_key("A"), to_key("B"))  < 0);
    BOOST_REQUIRE(cmp(to_key("AA"), to_key("B")) < 0);
    BOOST_REQUIRE(cmp(to_key("B"), to_key("AB")) > 0);
    BOOST_REQUIRE(cmp(to_key("B"), to_key("A"))  > 0);
    BOOST_REQUIRE(cmp(to_key("A"), to_key("A")) == 0);
}

BOOST_AUTO_TEST_CASE(test_legacy_ordering_for_composite_keys) {
    schema s({}, "", "", {{"c1", bytes_type}, {"c2", bytes_type}}, {}, {}, {}, utf8_type);

    auto to_key = [&s] (sstring v1, sstring v2) {
        return partition_key::from_exploded(s, std::vector<bytes>{to_bytes(v1), to_bytes(v2)});
    };

    auto cmp = [&s] (const partition_key& k1, const partition_key& k2) {
        return k1.legacy_tri_compare(s, k2);
    };

    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("A", "B")) == 0);
    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("A", "C")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", "B"), to_key("B", "B")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", "C"), to_key("B", "B")) < 0);
    BOOST_REQUIRE(cmp(to_key("B", "A"), to_key("A", "A")) > 0);

    BOOST_REQUIRE(cmp(to_key("AA", "B"), to_key("B", "B")) > 0);
    BOOST_REQUIRE(cmp(to_key("A", "AA"), to_key("A", "A")) > 0);

    BOOST_REQUIRE(cmp(to_key("", "A"), to_key("A", "A")) < 0);
    BOOST_REQUIRE(cmp(to_key("A", ""), to_key("A", "A")) < 0);
}

BOOST_AUTO_TEST_CASE(test_conversions_between_view_and_wrapper) {
    schema s({}, "", "", {{"c1", bytes_type}}, {}, {}, {}, utf8_type);

    auto key = partition_key::from_deeply_exploded(s, {bytes("value")});
    partition_key_view key_view = key;

    BOOST_REQUIRE(key_view.equal(s, key));
    BOOST_REQUIRE(key.equal(s, key_view));

    partition_key key2 = key_view;

    BOOST_REQUIRE(key2.equal(s, key));
    BOOST_REQUIRE(key.equal(s, key2));

    BOOST_REQUIRE(*key.begin(s) == bytes("value"));
}
