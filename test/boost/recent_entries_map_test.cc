/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/core/manual_clock.hh>
#include <chrono>

#include "utils/recent_entries_map.hh"
#include "utils/UUID.hh"

// Utility struct to ensure that utils::recent_entries_map
// prevents unnecessary copies of entries.
struct non_copyable_value {
    const std::string _value;

    non_copyable_value(const std::string& value)
    : _value(value) { }

    non_copyable_value(const non_copyable_value&) = delete;
    non_copyable_value& operator=(const non_copyable_value&) = delete;
};

bool operator==(const non_copyable_value& l, const non_copyable_value& r) {
    return l._value == r._value;
}

bool operator==(const non_copyable_value&l, const std::string& r) {
    return l._value == r;
}

std::ostream& operator<<(std::ostream& os, const non_copyable_value& e) {
    os << e._value;
    return os;
}

SEASTAR_THREAD_TEST_CASE(test_entry_insertion) {
    utils::recent_entries_map<utils::UUID, non_copyable_value> map;

    const utils::UUID uuid1(0, 1);
    const utils::UUID uuid2(0, 2);

    const std::string foo("foo");
    const std::string bar("bar");
    const std::string foobar("foobar");

    // create the `uuid1` -> `foo` mapping
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid1, foo), foo);
    BOOST_CHECK_EQUAL(map.size(), 1);

    // the key `uuid1` is already present in the map. return `foo`
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid1, bar), foo);
    BOOST_CHECK_EQUAL(map.size(), 1);

    // create the `uuid2` -> `bar` mapping
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid2, bar), bar);
    BOOST_CHECK_EQUAL(map.size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_entry_removal_by_key) {
    utils::recent_entries_map<utils::UUID, non_copyable_value> map;

    const utils::UUID uuid1(0, 1);

    const std::string foo("foo");

    // noting to remove.
    map.remove_recent_entry(uuid1);
    BOOST_CHECK_EQUAL(map.size(), 0);

    // create the `uuid1` -> `foo` mapping
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid1, foo), foo);
    BOOST_CHECK_EQUAL(map.size(), 1);

    // remove entry for the `uuid1` key
    map.remove_recent_entry(uuid1);
    BOOST_CHECK_EQUAL(map.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_remove_least_recent_entries) {
    using namespace std::chrono_literals;

    utils::recent_entries_map<utils::UUID, non_copyable_value, seastar::manual_clock> map;

    const utils::UUID uuid1(0, 1);
    const utils::UUID uuid2(0, 2);
    const utils::UUID uuid3(0, 3);

    const std::string foo("foo");
    const std::string bar("bar");
    const std::string foobar("foobar");

    BOOST_CHECK_EQUAL(map.size(), 0);

    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid1, foo), foo);
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid2, bar), bar);
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid3, foobar), foobar);

    BOOST_CHECK_EQUAL(map.size(), 3);

    // nothing to remove
    map.remove_least_recent_entries(30min);
    BOOST_CHECK_EQUAL(map.size(), 3);

    // advance time on 1h
    seastar::manual_clock::advance(1h);

    // remove all 3 entries
    map.remove_least_recent_entries(30min);
    BOOST_CHECK_EQUAL(map.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_update_and_remove_least_recent_entries) {
    using namespace std::chrono_literals;

    utils::recent_entries_map<utils::UUID, non_copyable_value, seastar::manual_clock> map;

    const utils::UUID uuid1(0, 1);
    const utils::UUID uuid2(0, 2);
    const utils::UUID uuid3(0, 3);

    const std::string foo("foo");
    const std::string bar("bar");
    const std::string foobar("foobar");

    // insert the uuid1 -> foo mapping
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid1, foo), foo);
    BOOST_CHECK_EQUAL(map.size(), 1);

    seastar::manual_clock::advance(5min);

    // insert the uuid2 -> bar mapping
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid2, bar), bar);
    BOOST_CHECK_EQUAL(map.size(), 2);

    seastar::manual_clock::advance(5min);

    // insert the uuid3 -> foobar mapping
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid3, foobar), foobar);
    BOOST_CHECK_EQUAL(map.size(), 3);

    seastar::manual_clock::advance(5min);
    // uuid3 5  min old
    // uuid2 10 min old
    // uuid1 15 min old

    // get the uuid2 and update internal timepoint
    BOOST_CHECK_EQUAL(map.try_get_recent_entry(uuid2, bar), bar);
    BOOST_CHECK_EQUAL(map.size(), 3);
    // uuid2 0  min old
    // uuid3 5  min old
    // uuid1 15 min old

    seastar::manual_clock::advance(5min);
    // uuid2 5  min old
    // uuid3 10 min old
    // uuid1 20 min old

    // get the uuid1 key and try to pass new key
    BOOST_CHECK_NE(map.try_get_recent_entry(uuid1, foo), bar);
    BOOST_CHECK_EQUAL(map.size(), 3);
    // uuid1 0  min old
    // uuid2 5  min old
    // uuid3 10 min old

    // nothing to remove
    map.remove_least_recent_entries(30min);
    BOOST_CHECK_EQUAL(map.size(), 3);

    seastar::manual_clock::advance(5min);
    // uuid1 5  min old
    // uuid2 10 min old
    // uuid3 15 min old

    // remove the entry with the uuid3 key
    map.remove_least_recent_entries(15min);
    BOOST_CHECK_EQUAL(map.size(), 2);

    // nothing to remove
    map.remove_least_recent_entries(11min);
    BOOST_CHECK_EQUAL(map.size(), 2);

    // remove the entry with the uuid2 key
    map.remove_least_recent_entries(7min);
    BOOST_CHECK_EQUAL(map.size(), 1);

    seastar::manual_clock::advance(55min);
    // uuid1 60 min old

    // remove the entry with the uuid2 key
    map.remove_least_recent_entries(10min);
    BOOST_CHECK_EQUAL(map.size(), 0);
}