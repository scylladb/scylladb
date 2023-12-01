/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE sstable-generation

#include <string>
#include <boost/test/unit_test.hpp>

#include "sstables/generation_type.hh"

using namespace std::literals;

namespace sstables {
std::ostream& boost_test_print_type(std::ostream& os, const generation_type& gen) {
    fmt::print(os, "{}", gen);
    return os;
}
}

BOOST_AUTO_TEST_CASE(from_string_uuid_good) {
    // the id comes from https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-New-SSTable-Identifiers.html
    const auto id = "3fw2_0tj4_46w3k2cpidnirvjy7k"s;
    const uint64_t msb = 0x6636ac00da8411ec;
    const uint64_t lsb = 0x9abaf56e1443def0;
    const auto uuid = utils::UUID(msb, lsb);
    const auto gen = sstables::generation_type::from_string(id);

    BOOST_REQUIRE(bool(gen));
    BOOST_REQUIRE(gen.is_uuid_based());
    BOOST_CHECK_EQUAL(gen.as_uuid(), uuid);
    BOOST_CHECK_EQUAL(id, fmt::to_string(gen));
}

BOOST_AUTO_TEST_CASE(from_string_int_good) {
    const auto id = "42";
    const auto gen = sstables::generation_type::from_string(id);

    BOOST_REQUIRE(bool(gen));
    BOOST_REQUIRE(!gen.is_uuid_based());
    BOOST_CHECK_EQUAL(gen.as_int(), 42);
    BOOST_CHECK_EQUAL(id, fmt::to_string(gen));
}

BOOST_AUTO_TEST_CASE(invalid_identifier) {
  const auto invalid_id = sstables::generation_type{};
  BOOST_CHECK_NO_THROW(fmt::to_string(invalid_id));
  BOOST_CHECK(!invalid_id);
}


BOOST_AUTO_TEST_CASE(from_string_bad) {
    const auto bad_uuids = {
      "3fw _0tj4_46w3k2cpidnirvjy7k"s,
      "3fw2_0tj4_46w3k2cpidnirvjy7 "s,
      "3fw2_0tj__46w3k2cpidnirvjy7k"s,
      "3fw2_0tj$_46w3k2cpidnirvjy7k"s,
      "3fw2_0tj4_46w3k2cpidnirvjy7"s,
      "3fw2_0tj4_46w3k2cpidnirvjy7kkkk"s,
      "3fw2_0tj4"s,
      "3fw2_0tj4_46w3k2cpidnirvjy7k_and_more"s,
      "bonjour"s,
      "0x42"s,
      ""s,
    };
    for (auto& bad_uuid : bad_uuids) {
        BOOST_CHECK_THROW(sstables::generation_type::from_string(bad_uuid), std::logic_error);
    }
}

BOOST_AUTO_TEST_CASE(compare) {
    // an integer-based identifiers should be always greater than an invalid one
    // so we can find the uuid-based identifier as before -- the invalid id is
    // provided as the minimal identifier
    BOOST_CHECK_GT(sstables::generation_type(42), sstables::generation_type{});
    BOOST_CHECK_LT(sstables::generation_type{}, sstables::generation_type(42));

    const auto uuid = "3fw2_0tj4_46w3k2cpidnirvjy7k"s;
    const auto id_uuid = sstables::generation_type::from_string(uuid);
    // an integer-based identifier should be always greater than a uuid-based one,
    // so we can find the uuid-based identifier as before
    BOOST_CHECK_GT(sstables::generation_type(42), id_uuid);
    BOOST_CHECK_GT(sstables::generation_type(1), id_uuid);
    BOOST_CHECK_LT(id_uuid, sstables::generation_type(1));
    BOOST_CHECK_GT(sstables::generation_type(42), sstables::generation_type(41));
    BOOST_CHECK_LT(sstables::generation_type(41), sstables::generation_type(42));
    BOOST_CHECK_EQUAL(sstables::generation_type(42), sstables::generation_type(42));
    // if two identifiers are compared, we consider them as timeuuid, which means:
    //
    // 1. they can be used as key in an associative container
    // 2. the newer timeuuids are order after the older ones

    // two UUIDs with the same timestamps but different clock seq and node
    BOOST_CHECK_NE(sstables::generation_type::from_string("3fw2_0tj4_46w3k2cpidnirvjy7k"),
                   sstables::generation_type::from_string("3fw2_0tj4_46w3k2cpidnirvjy7z"));
    // the return value from_string() of the same string representation should
    // be equal
    BOOST_CHECK_EQUAL(sstables::generation_type::from_string(uuid),
                      sstables::generation_type::from_string(uuid));
    // two UUIDs with different timestamps but the same clock seq and node,
    // their timestamps are:
    // - 2023-11-24 23:41:56
    // - 2023-11-24 23:41:57
    BOOST_CHECK_LT(sstables::generation_type::from_string("3gbc_17lw_3tlpc2fe8ko69yav6u"),
                   sstables::generation_type::from_string("3gbc_17lx_59opc2fe8ko69yav6u"));
    // all invalid identifiers should be equal
    BOOST_CHECK_EQUAL(sstables::generation_type{}, sstables::generation_type{});
    BOOST_CHECK_NE(sstables::generation_type{},
                   sstables::generation_type::from_string(uuid));
}
