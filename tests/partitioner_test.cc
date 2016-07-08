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

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include "dht/byte_ordered_partitioner.hh"
#include "schema.hh"
#include "types.hh"
#include "schema_builder.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

static dht::token token_from_long(uint64_t value) {
    auto t = net::hton(value);
    bytes b(bytes::initialized_later(), 8);
    std::copy_n(reinterpret_cast<int8_t*>(&t), 8, b.begin());
    return { dht::token::kind::key, std::move(b) };
}

BOOST_AUTO_TEST_CASE(test_decorated_key_is_compatible_with_origin) {
    auto s = schema_builder("ks", "cf")
        .with_column("c1", int32_type, column_kind::partition_key)
        .with_column("c2", int32_type, column_kind::partition_key)
        .with_column("v", int32_type)
        .build();

    dht::murmur3_partitioner partitioner;
    auto key = partition_key::from_deeply_exploded(*s, {143, 234});
    auto dk = partitioner.decorate_key(*s, key);

    // Expected value was taken from Origin
    BOOST_REQUIRE_EQUAL(dk._token, token_from_long(4958784316840156970));
    BOOST_REQUIRE(dk._key.equal(*s, key));
}

BOOST_AUTO_TEST_CASE(test_token_wraparound_1) {
    auto t1 = token_from_long(0x7000'0000'0000'0000);
    auto t2 = token_from_long(0xa000'0000'0000'0000);
    dht::murmur3_partitioner partitioner;
    BOOST_REQUIRE(t1 > t2);
    // Even without knowing what the midpoint is, it needs to be inside the
    // wrapped range, i.e., between t1 and inf, OR between -inf and t2
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 || midpoint < t2);
    // We can also calculate the actual value the midpoint should have:
    BOOST_REQUIRE_EQUAL(midpoint, token_from_long(0x8800'0000'0000'0000));
}

BOOST_AUTO_TEST_CASE(test_token_wraparound_2) {
    auto t1 = token_from_long(0x6000'0000'0000'0000);
    auto t2 = token_from_long(0x9000'0000'0000'0000);
    dht::murmur3_partitioner partitioner;
    BOOST_REQUIRE(t1 > t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 || midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, token_from_long(0x7800'0000'0000'0000));
}

BOOST_AUTO_TEST_CASE(test_ring_position_is_comparable_with_decorated_key) {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", int32_type)
        .build();

    std::vector<dht::decorated_key> keys = {
        dht::global_partitioner().decorate_key(*s,
            partition_key::from_single_value(*s, "key1")),
        dht::global_partitioner().decorate_key(*s,
            partition_key::from_single_value(*s, "key2")),
    };

    std::sort(keys.begin(), keys.end(), dht::decorated_key::less_comparator(s));

    auto& k1 = keys[0];
    auto& k2 = keys[1];

    BOOST_REQUIRE(k1._token != k2._token); // The rest of the test assumes that.

    BOOST_REQUIRE(k1.tri_compare(*s, dht::ring_position::starting_at(k1._token)) > 0);
    BOOST_REQUIRE(k1.tri_compare(*s, dht::ring_position::ending_at(k1._token)) < 0);
    BOOST_REQUIRE(k1.tri_compare(*s, dht::ring_position(k1)) == 0);

    BOOST_REQUIRE(k1.tri_compare(*s, dht::ring_position::starting_at(k2._token)) < 0);
    BOOST_REQUIRE(k1.tri_compare(*s, dht::ring_position::ending_at(k2._token)) < 0);
    BOOST_REQUIRE(k1.tri_compare(*s, dht::ring_position(k2)) < 0);

    BOOST_REQUIRE(k2.tri_compare(*s, dht::ring_position::starting_at(k1._token)) > 0);
    BOOST_REQUIRE(k2.tri_compare(*s, dht::ring_position::ending_at(k1._token)) > 0);
    BOOST_REQUIRE(k2.tri_compare(*s, dht::ring_position(k1)) > 0);
}

BOOST_AUTO_TEST_CASE(test_token_no_wraparound_1) {
    auto t1 = token_from_long(0x5000'0000'0000'0000);
    auto t2 = token_from_long(0x7000'0000'0000'0000);
    dht::murmur3_partitioner partitioner;
    BOOST_REQUIRE(t1 < t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 && midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, token_from_long(0x6000'0000'0000'0000));
}

BOOST_AUTO_TEST_CASE(test_bop_token_nowraparound_1) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("03");
    auto t2 = partitioner.from_sstring("09");
    BOOST_REQUIRE(t1 < t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 && midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring("06"));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}


BOOST_AUTO_TEST_CASE(test_bop_token_nowraparound_2) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("20000000000000000000000000000003");
    auto t2 = partitioner.from_sstring("A0000000000000000000000000000009");
    BOOST_REQUIRE(t1 < t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 && midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring("60000000000000000000000000000006"));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_nowraparound_3) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("2000000000000000000000000000000320000000000000000000000000000003");
    auto t2 = partitioner.from_sstring("A0000000000000000000000000000009A0000000000000000000000000000009");
    BOOST_REQUIRE(t1 < t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 && midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring("6000000000000000000000000000000660000000000000000000000000000006"));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_nowraparound_4) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("");
    auto t2 = partitioner.from_sstring("2000");
    BOOST_REQUIRE(t1 < t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 && midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring("1000"));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_nowraparound_5) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("00");
    auto t2 = partitioner.from_sstring("2000");
    BOOST_REQUIRE(t1 < t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 && midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring("1000"));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_nowraparound_6) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring(sstring());
    auto t2 = partitioner.from_sstring(sstring());
    BOOST_REQUIRE(t1 <= t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint >= t1 && midpoint <= t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring(""));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_wraparound_1) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("00000000000000000000000000000009");
    auto t2 = partitioner.from_sstring("00000000000000000000000000000003");
    BOOST_REQUIRE(t1 > t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 || midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring("80000000000000000000000000000006"));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_wraparound_2) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("A0000000000000000000000000000009");
    auto t2 = partitioner.from_sstring("20000000000000000000000000000003");
    BOOST_REQUIRE(t1 > t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 || midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring("E0000000000000000000000000000006"));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_wraparound_3) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("A000000000000000000000000000000900000000000000000000000000000009");
    auto t2 = partitioner.from_sstring("2000000000000000000000000000000300000000000000000000000000000003");
    BOOST_REQUIRE(t1 > t2);
    auto midpoint = partitioner.midpoint(t1, t2);
    BOOST_REQUIRE(midpoint > t1 || midpoint < t2);
    BOOST_REQUIRE_EQUAL(midpoint, partitioner.from_sstring("E000000000000000000000000000000600000000000000000000000000000006"));
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_describe_ownership) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("10000000000000000000000000000000");
    auto t2 = partitioner.from_sstring("30000000000000000000000000000000");
    auto t3 = partitioner.from_sstring("A0000000000000000000000000000000");
    auto t4 = partitioner.from_sstring("F0000000000000000000000000000000");
    auto sorted_tokens = std::vector<dht::token>{t1, t2, t3, t4};
    auto own_map = partitioner.describe_ownership(sorted_tokens);
    BOOST_REQUIRE_EQUAL(own_map[t1], 0.1250);
    BOOST_REQUIRE_EQUAL(own_map[t2], 0.1250);
    BOOST_REQUIRE_EQUAL(own_map[t3], 0.4375);
    BOOST_REQUIRE_EQUAL(own_map[t4], 0.3125);
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_order) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("123456");
    auto t2 = partitioner.from_sstring("12345678");
    BOOST_REQUIRE(t1 < t2);

    t1 = partitioner.from_sstring("22");
    t2 = partitioner.from_sstring("12345678");
    BOOST_REQUIRE(t1 > t2);

    t1 = partitioner.from_sstring("123456");
    t2 = partitioner.from_sstring("123457");
    BOOST_REQUIRE(t1 < t2);

    t1 = partitioner.from_sstring("123456");
    t2 = partitioner.from_sstring("A23456");
    BOOST_REQUIRE(t1 < t2);


    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_midpoint1) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("010000");
    auto t2 = partitioner.from_sstring("20");
    auto mid = partitioner.midpoint(t1, t2);
    // The length of the midpoint token is supposed to be 3 bytes, filled with one zero byte
    auto mid_expected = partitioner.from_sstring("008010");

    BOOST_REQUIRE(t1 < t2);
    BOOST_REQUIRE(mid_expected._data.size() == 3);
    BOOST_REQUIRE(mid._data.size() == 3);
    BOOST_REQUIRE(mid == mid_expected);

    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_midpoint2) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("020001");
    auto t2 = partitioner.from_sstring("60");
    auto mid = partitioner.midpoint(t1, t2);
    // The length of the midpoint token is supposed to be 3 bytes, filled with one zero byte
    auto mid_expected = partitioner.from_sstring("01003080");

    BOOST_REQUIRE(t1 < t2);
    BOOST_REQUIRE(mid == mid_expected);

    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}

BOOST_AUTO_TEST_CASE(test_bop_token_midpoint3) {
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.ByteOrderedPartitioner"));
    dht::byte_ordered_partitioner partitioner;
    auto t1 = partitioner.from_sstring("20");
    auto t2 = partitioner.from_sstring("81");
    auto mid = partitioner.midpoint(t1, t2);
    auto mid_expected = partitioner.from_sstring("5080");

    BOOST_REQUIRE(t1 < t2);
    BOOST_REQUIRE(mid == mid_expected);

    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));
}
