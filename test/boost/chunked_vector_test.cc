/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <boost/test/included/unit_test.hpp>
#include <deque>
#include <random>
#include "utils/chunked_vector.hh"
#include "utils/amortized_reserve.hh"

#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/equal.hpp>
#include <boost/range/algorithm/reverse.hpp>
#include <boost/range/irange.hpp>

using disk_array = utils::chunked_vector<uint64_t, 1024>;


using deque = std::deque<int>;

BOOST_AUTO_TEST_CASE(test_random_walk) {
    auto rand = std::default_random_engine();
    auto op_gen = std::uniform_int_distribution<unsigned>(0, 9);
    auto nr_dist = std::geometric_distribution<size_t>(0.7);
    deque d;
    disk_array c;
    for (auto i = 0; i != 1000000; ++i) {
        auto op = op_gen(rand);
        switch (op) {
        case 0: {
            auto n = rand();
            c.push_back(n);
            d.push_back(n);
            break;
        }
        case 1: {
            auto nr_pushes = nr_dist(rand);
            for (auto i : boost::irange(size_t(0), nr_pushes)) {
                (void)i;
                auto n = rand();
                c.push_back(n);
                d.push_back(n);
            }
            break;
        }
        case 2: {
            if (!d.empty()) {
                auto n = d.back();
                auto m = c.back();
                BOOST_REQUIRE_EQUAL(n, m);
                c.pop_back();
                d.pop_back();
            }
            break;
        }
        case 3: {
            c.reserve(nr_dist(rand));
            break;
        }
        case 4: {
            boost::sort(c);
            boost::sort(d);
            break;
        }
        case 5: {
            if (!d.empty()) {
                auto u = std::uniform_int_distribution<size_t>(0, d.size() - 1);
                auto idx = u(rand);
                auto m = c[idx];
                auto n = c[idx];
                BOOST_REQUIRE_EQUAL(m, n);
            }
            break;
        }
        case 6: {
            c.clear();
            d.clear();
            break;
        }
        case 7: {
            boost::reverse(c);
            boost::reverse(d);
            break;
        }
        case 8: {
            c.clear();
            d.clear();
            break;
        }
        case 9: {
            auto nr = nr_dist(rand);
            c.resize(nr);
            d.resize(nr);
            break;
        }
        default:
            abort();
        }
        BOOST_REQUIRE_EQUAL(c.size(), d.size());
        BOOST_REQUIRE(boost::equal(c, d));
    }
}

class exception_safety_checker {
    uint64_t _live_objects = 0;
    uint64_t _countdown = std::numeric_limits<uint64_t>::max();
public:
    bool ok() const {
        return !_live_objects;
    }
    void set_countdown(unsigned x) {
        _countdown = x;
    }
    void add_live_object() {
        if (!_countdown--) { // auto-clears
            throw "ouch";
        }
        ++_live_objects;
    }
    void del_live_object() {
        --_live_objects;
    }
};

class exception_safe_class {
    exception_safety_checker& _esc;
public:
    explicit exception_safe_class(exception_safety_checker& esc) : _esc(esc) {
        _esc.add_live_object();
    }
    exception_safe_class(const exception_safe_class& x) : _esc(x._esc) {
        _esc.add_live_object();
    }
    exception_safe_class(exception_safe_class&&) = default;
    ~exception_safe_class() {
        _esc.del_live_object();
    }
    exception_safe_class& operator=(const exception_safe_class& x) {
        if (this != &x) {
            auto tmp = x;
            this->~exception_safe_class();
            *this = std::move(tmp);
        }
        return *this;
    }
    exception_safe_class& operator=(exception_safe_class&&) = default;
};

BOOST_AUTO_TEST_CASE(tests_constructor_exception_safety) {
    auto checker = exception_safety_checker();
    auto v = std::vector<exception_safe_class>(100, exception_safe_class(checker));
    checker.set_countdown(5);
    try {
        auto u = utils::chunked_vector<exception_safe_class>(v.begin(), v.end());
        BOOST_REQUIRE(false);
    } catch (...) {
        v.clear();
        BOOST_REQUIRE(checker.ok());
    }
}

BOOST_AUTO_TEST_CASE(tests_reserve_partial) {
    auto rand = std::default_random_engine();
    auto size_dist = std::uniform_int_distribution<unsigned>(1, 1 << 12);

    for (int i = 0; i < 100; ++i) {
        utils::chunked_vector<uint8_t> v;
        const auto orig_size = size_dist(rand);
        auto size = orig_size;
        while (size) {
            size = v.reserve_partial(size);
        }
        BOOST_REQUIRE_EQUAL(v.capacity(), orig_size);
    }
}

// Tests the case of make_room() invoked with last_chunk_capacity_deficit but _size not in
// the last reserved chunk.
BOOST_AUTO_TEST_CASE(test_shrinking_and_expansion_involving_chunk_boundary) {
    using vector_type = utils::chunked_vector<std::unique_ptr<uint64_t>>;
    vector_type v;

    // Fill two chunks
    v.reserve(vector_type::max_chunk_capacity() * 3 / 2);
    for (uint64_t i = 0; i < vector_type::max_chunk_capacity() * 3 / 2; ++i) {
        v.emplace_back(std::make_unique<uint64_t>(i));
    }

    // Make the last chunk smaller than max size to trigger the last_chunk_capacity_deficit path in make_room()
    v.shrink_to_fit();

    // Leave the last chunk reserved but empty
    for (uint64_t i = 0; i < vector_type::max_chunk_capacity(); ++i) {
        v.pop_back();
    }

    // Try to reserve more than the currently reserved capacity and trigger last_chunk_capacity_deficit path
    // with _size not in the last chunk. Should not sigsegv.
    v.reserve(vector_type::max_chunk_capacity() * 4);

    for (uint64_t i = 0; i < vector_type::max_chunk_capacity() * 2; ++i) {
        v.emplace_back(std::make_unique<uint64_t>(i));
    }
}

BOOST_AUTO_TEST_CASE(test_amoritzed_reserve) {
    utils::chunked_vector<int> v;

    v.reserve(10);
    amortized_reserve(v, 1);
    BOOST_REQUIRE_EQUAL(v.capacity(), 10);
    BOOST_REQUIRE_EQUAL(v.size(), 0);

    v = {};
    amortized_reserve(v, 1);
    BOOST_REQUIRE_EQUAL(v.capacity(), 1);
    BOOST_REQUIRE_EQUAL(v.size(), 0);

    v = {};
    amortized_reserve(v, 1);
    BOOST_REQUIRE_EQUAL(v.capacity(), 1);
    amortized_reserve(v, 2);
    BOOST_REQUIRE_EQUAL(v.capacity(), 2);
    amortized_reserve(v, 3);
    BOOST_REQUIRE_EQUAL(v.capacity(), 4);
    amortized_reserve(v, 4);
    BOOST_REQUIRE_EQUAL(v.capacity(), 4);
    amortized_reserve(v, 5);
    BOOST_REQUIRE_EQUAL(v.capacity(), 8);
    amortized_reserve(v, 6);
    BOOST_REQUIRE_EQUAL(v.capacity(), 8);
    amortized_reserve(v, 7);
    BOOST_REQUIRE_EQUAL(v.capacity(), 8);
    amortized_reserve(v, 7);
    BOOST_REQUIRE_EQUAL(v.capacity(), 8);
    amortized_reserve(v, 1);
    BOOST_REQUIRE_EQUAL(v.capacity(), 8);
}
