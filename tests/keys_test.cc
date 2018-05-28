/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <boost/range/algorithm/copy.hpp>
#include "keys.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "types.hh"

#include "idl/keys.dist.hh"
#include "serializer_impl.hh"
#include "idl/keys.dist.impl.hh"

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

    auto key = partition_key::from_deeply_exploded(s, {data_value(bytes("value"))});
    partition_key_view key_view = key;

    BOOST_REQUIRE(key_view.equal(s, key));
    BOOST_REQUIRE(key.equal(s, key_view));

    partition_key key2 = key_view;

    BOOST_REQUIRE(key2.equal(s, key));
    BOOST_REQUIRE(key.equal(s, key2));

    BOOST_REQUIRE(*key.begin(s) == bytes("value"));
}

template<typename T>
inline
T reserialize(const T& v) {
    auto buf = ser::serialize_to_buffer<bytes>(v);
    auto in = ser::as_input_stream(buf);
    return ser::deserialize(in, boost::type<T>());
}

BOOST_AUTO_TEST_CASE(test_serialization) {
    auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type)
            .build();

    auto pk_value = bytes("value");
    partition_key key(std::vector<bytes>({pk_value}));

    BOOST_REQUIRE(key.equal(*s, reserialize(key)));
}


BOOST_AUTO_TEST_CASE(test_from_nodetool_style_string) {
    auto s1 = make_lw_shared<schema>(
    schema({}, "", "",
        {
            {"c1", utf8_type}
        }, {
            {"c2", bytes_type}, {"c3", bytes_type}, {"c4", bytes_type}
        },
        {}, {}, utf8_type));

    auto pk_value = bytes("value");
    partition_key key1(std::vector<bytes>({pk_value}));

    auto key2 = partition_key::from_nodetool_style_string(s1, "value");
    BOOST_REQUIRE(key1.equal(*s1, key2));

    auto s2 = make_lw_shared<schema>(
    schema({}, "", "",
        {
            {"c1", utf8_type}, {"c2", utf8_type}
        }, {
            {"c3", bytes_type}, {"c4", bytes_type}
        },
        {}, {}, utf8_type));

    auto pk_value1 = bytes("value1");
    auto pk_value2 = bytes("value2");
    partition_key key3(std::vector<bytes>({pk_value1, pk_value2}));

    auto key4 = partition_key::from_nodetool_style_string(s2, "value1:value2");
    BOOST_REQUIRE(key3.equal(*s1, key4));
}
