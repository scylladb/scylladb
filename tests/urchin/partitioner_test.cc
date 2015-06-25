/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include "schema.hh"
#include "types.hh"

static dht::token token_from_long(uint64_t value) {
    auto t = net::hton(value);
    bytes b(bytes::initialized_later(), 8);
    std::copy_n(reinterpret_cast<int8_t*>(&t), 8, b.begin());
    return { dht::token::kind::key, std::move(b) };
}

BOOST_AUTO_TEST_CASE(test_decorated_key_is_compatible_with_origin) {
    schema s({}, "", "",
        // partition key
        {{"c1", int32_type}, {"c2", int32_type}},
        // clustering key
        {},
        // regular columns
        {
            {"v", int32_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type
    );

    dht::murmur3_partitioner partitioner;
    auto key = partition_key::from_deeply_exploded(s, {143, 234});
    auto dk = partitioner.decorate_key(s, key);

    // Expected value was taken from Origin
    BOOST_REQUIRE_EQUAL(dk._token, token_from_long(4958784316840156970));
    BOOST_REQUIRE(dk._key.equal(s, key));
}

BOOST_AUTO_TEST_CASE(test_token_wraparound_1) {
    auto t1 = token_from_long(0x7000'0000'0000'0000);
    auto t2 = token_from_long(0xa000'0000'0000'0000);
    dht::murmur3_partitioner partitioner;
    BOOST_REQUIRE(t1 > t2);
    BOOST_REQUIRE_EQUAL(partitioner.midpoint(t1, t2), token_from_long(0x0800'0000'0000'0000));
}

BOOST_AUTO_TEST_CASE(test_token_wraparound_2) {
    auto t1 = token_from_long(0x6000'0000'0000'0000);
    auto t2 = token_from_long(0x9000'0000'0000'0000);
    dht::murmur3_partitioner partitioner;
    BOOST_REQUIRE(t1 > t2);
    BOOST_REQUIRE_EQUAL(partitioner.midpoint(t1, t2), token_from_long(0xf800'0000'0000'0000));
}
