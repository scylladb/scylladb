/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE utils
#include <boost/test/unit_test.hpp>
#include <fmt/core.h>
#include <ranges>

#include "utils/lru_string_map.hh"

class mapped_object {
    int _v;
public:
    mapped_object() = delete;
    explicit mapped_object(int v) : _v(v) {};
    mapped_object clone() const { return mapped_object(_v); }
    // Movable
    mapped_object(mapped_object&&) = default;
    mapped_object& operator=(mapped_object&&) = default;

    // "copyable", but shouldn't be used
    mapped_object(const mapped_object&) {
        BOOST_FAIL("Copying mapped_object is forbidden");
    };
    mapped_object& operator=(const mapped_object&) {
        BOOST_FAIL("Copying mapped_object is forbidden");
        return *this;
    };

    bool operator==(const mapped_object& other) const {
        return _v == other._v;
    }
    friend std::ostream& operator<< (std::ostream& os, const mapped_object& obj){
        return os << "mapped_object(" << obj._v << ")";
    }
};

std::vector<std::pair<std::string, mapped_object>> make_entries() {
    std::vector<std::pair<std::string, mapped_object>> v;
    v.emplace_back("", mapped_object(0));
    v.emplace_back("a", mapped_object(1));
    v.emplace_back("ab", mapped_object(2));
    v.emplace_back("abc", mapped_object(3));
    v.emplace_back("abcd", mapped_object(4));
    v.emplace_back("abcde", mapped_object(5));
    v.emplace_back("abcdef", mapped_object(6));
    v.emplace_back("abcdefg", mapped_object(7));
    v.emplace_back("abcdefgh", mapped_object(8));
    return v;
}
static std::vector<std::pair<std::string, mapped_object>> entries = make_entries();

auto forward_i(size_t begin=0, size_t end=entries.size()) {
    BOOST_REQUIRE_MESSAGE((begin <= end), fmt::format("Invalid test setup: range {} -> {}", begin, end));
    BOOST_REQUIRE_MESSAGE((end <= entries.size()), fmt::format("Invalid test setup: range end {} > {}", end, entries.size()));
    return std::views::iota(begin, end);
}
auto reverse_i(size_t begin=entries.size(), size_t end=0) {
    BOOST_REQUIRE_MESSAGE((begin >= end), fmt::format("Invalid test setup: index range {} <- {}", end, begin));
    BOOST_REQUIRE_MESSAGE((begin <= entries.size()), fmt::format("Invalid test setup: range begin {} > {}", begin, entries.size()));
    return std::views::iota(end, begin) | std::views::reverse;
}

template<typename Value>
class test_lru_map : public lru_string_map<Value> {
public:
    size_t get_size_with_sanity_check() const {
        BOOST_REQUIRE(this->sanity_check());
        for (auto& key : this->_lru_key_list) {
            BOOST_REQUIRE(this->_map.contains(key.str));
        }
        return this->size();
    }
};
using lru_map = test_lru_map<mapped_object>;

template<typename Map>
void check_size(const Map& map, size_t expected) {
    BOOST_REQUIRE_EQUAL(map.get_size_with_sanity_check(), expected);
}

void fill_map(lru_map& map) {
    for (auto i : forward_i()) {
        map.insert_or_assign(entries[i].first, entries[i].second.clone());
        check_size(map, i + 1);
    }
}

template <typename Range>
void entries_exist(lru_map& map, Range&& i_range) {
    auto size_before = map.get_size_with_sanity_check();
    for (auto i : i_range) {
        auto v = map.find(entries[i].first);
        check_size(map, size_before);
        BOOST_REQUIRE(v.has_value());
        BOOST_REQUIRE_EQUAL(v->get(), entries[i].second);
    }
}

template <typename Range>
void entries_not_exist(lru_map& map, Range&& i_range) {
    auto size_before = map.get_size_with_sanity_check();
    for (auto i : i_range) {
        auto v = map.find(entries[i].first);
        check_size(map, size_before);
        BOOST_REQUIRE(!v.has_value());
    }
}

// Basic tests for lru_string_map capabilities to store and retrieve entries
BOOST_AUTO_TEST_CASE(test_lru_string_map_insert_and_find) {
    lru_map map;
    // empty map
    check_size(map, 0);
    entries_not_exist(map, forward_i());
    // filled map
    fill_map(map);
    entries_exist(map, forward_i());
    entries_exist(map, reverse_i());
}

// Test verify that entries are removed in LRU order.
// It tests that finding existing entries moves them at the end of the LRU list.
// Removing from empty map is also tested.
BOOST_AUTO_TEST_CASE(test_lru_string_map_pop) {
    lru_map map;
    for (bool reverse_order : {false, true}) {
        for (bool touch_existing : {false, true}) {
            // empty map
            BOOST_REQUIRE(!map.pop());
            check_size(map, 0);
            // filled map
            fill_map(map);
            if (reverse_order) { // touch all keys in reverse order
                entries_exist(map, reverse_i());
            }
            for (auto i : forward_i()) {
                auto rev_i = entries.size() - i - 1;
                BOOST_REQUIRE(map.pop());
                check_size(map, rev_i);
                if (reverse_order) {
                    entries_not_exist(map, reverse_i(entries.size(), rev_i));
                    if (touch_existing) { // touch existing keys, but in the same order
                        entries_exist(map, reverse_i(rev_i));
                    }
                } else {
                    entries_not_exist(map, forward_i(0, i+1));
                    if (touch_existing) { // touch existing keys, but in the same order
                        entries_exist(map, forward_i(i+1));
                    }
                }
            }
        }
    }
}

// Test wiping all entries in the map
BOOST_AUTO_TEST_CASE(test_lru_string_map_clear) {
    lru_map map;
    fill_map(map);
    BOOST_REQUIRE_EQUAL(map.clear(), entries.size());
    check_size(map, 0);
    entries_not_exist(map, forward_i());
}

class non_copyable_mapped_object : public mapped_object {
    explicit non_copyable_mapped_object(mapped_object&& o) : mapped_object(std::move(o)) {};
public:
    explicit non_copyable_mapped_object(int v) : mapped_object(v) {};
    non_copyable_mapped_object clone() const { return non_copyable_mapped_object(mapped_object::clone()); }
    // Movable
    non_copyable_mapped_object(non_copyable_mapped_object&&) = default;
    non_copyable_mapped_object& operator=(non_copyable_mapped_object&&) = default;

    // Not copyable
    non_copyable_mapped_object(const non_copyable_mapped_object&) = delete;
    non_copyable_mapped_object& operator=(const non_copyable_mapped_object&) = delete;

    void set(int v) {
        *this = non_copyable_mapped_object(v);
    }
};

// Basic compilation test for non-copyable mapped type
BOOST_AUTO_TEST_CASE(test_lru_string_map_non_copyable_objects) {
    test_lru_map<non_copyable_mapped_object> map;
    map.insert_or_assign(std::string("test"), non_copyable_mapped_object(17));
    BOOST_REQUIRE_EQUAL(map.find("test").value(), non_copyable_mapped_object(17));
}

// Test that find returns reference to object and it can be modified
BOOST_AUTO_TEST_CASE(test_lru_string_map_find_reference) {
    test_lru_map<non_copyable_mapped_object> map;
    map.insert_or_assign(std::string("test"), non_copyable_mapped_object(17));
    map.find("test").value().get().set(42);
    BOOST_REQUIRE_EQUAL(map.find("test").value(), non_copyable_mapped_object(42));
    check_size(map, 1);
}
