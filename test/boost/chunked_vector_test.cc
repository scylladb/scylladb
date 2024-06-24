/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <stdexcept>
#include <optional>
#include <variant>
#include <fmt/format.h>

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
    int64_t _live_objects = 0;
    int64_t _countdown = std::numeric_limits<int64_t>::max();
public:
    bool ok() const {
        return !_live_objects;
    }
    int64_t live_objects() const {
        return _live_objects;
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
    void add_live_object_noexcept() noexcept {
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
    exception_safe_class(exception_safe_class&& x) noexcept : _esc(x._esc) {
        _esc.add_live_object_noexcept();
    }
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
        utils::chunked_vector<uint8_t, 512> v;
        const auto size = size_dist(rand);
        while (v.capacity() != size) {
            v.reserve_partial(size);
        }
        BOOST_REQUIRE_EQUAL(v.capacity(), size);
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

struct push_back_item {
    std::unique_ptr<int> p;
    push_back_item() = default;
    push_back_item(int v) : p(std::make_unique<int>(v)) {}
    // Note: the copy constructor adds 1 to the copied-from value
    // so it can be checked by test, in constrast to the move constructor.
    push_back_item(const push_back_item& x) : push_back_item(x.value() + 1) {}
    push_back_item(push_back_item&& x) noexcept : p(std::exchange(x.p, nullptr)) {}

    int value() const noexcept { return *p; }
};

template <class VectorType>
static void do_test_push_back_using_existing_element(std::function<void (VectorType&, const push_back_item&)> do_push_back, size_t count = 1000) {
    VectorType v;
    v.push_back(0);
    for (size_t i = 0; i < count; i++) {
        do_push_back(v, v.back());
    }
    for (size_t i = 0; i < count; i++) {
        BOOST_REQUIRE_EQUAL(v[i].value(), i);
    }
}

// Reproducer for https://github.com/scylladb/scylladb/issues/18072
// Test that we can push or emplace_back that copies another element
// that exists in the chunked_vector by reference.
// When reallocation occurs, the vector implementation must
// make sure that the new element is constructed first, before
// the reference to the existing element is invalidated.
// See also https://lists.isocpp.org/std-proposals/2024/03/9467.php
BOOST_AUTO_TEST_CASE(test_push_back_using_existing_element) {
    do_test_push_back_using_existing_element<std::vector<push_back_item>>([] (std::vector<push_back_item>& v, const push_back_item& x) { v.push_back(x); });
    do_test_push_back_using_existing_element<std::vector<push_back_item>>([] (std::vector<push_back_item>& v, const push_back_item& x) { v.emplace_back(x); });

    // Choose `max_contiguous_allocation` to exercise all cases in chunked_vector::reserve_and_emplace_back:
    // - Initial allocation (based on chunked_vector::min_chunk_capacity())
    // - Then the chunk is doubled, until it reaches half the max_chunk_size
    // - Then the chunk is reallocated to the max_chunk_size
    // - From then on, new chunks are allocated using max_chunk_size
    constexpr size_t max_contiguous_allocation = 4 * utils::chunked_vector<push_back_item>::min_chunk_capacity() * sizeof(push_back_item);
    using chunked_vector_type = utils::chunked_vector<push_back_item, max_contiguous_allocation>;

    do_test_push_back_using_existing_element<chunked_vector_type>([] (chunked_vector_type& v, const push_back_item& x) { v.push_back(x); },
            chunked_vector_type::max_chunk_capacity() + 2);
    do_test_push_back_using_existing_element<chunked_vector_type>([] (chunked_vector_type& v, const push_back_item& x) { v.emplace_back(x); },
            chunked_vector_type::max_chunk_capacity() + 2);
}

BOOST_AUTO_TEST_CASE(tests_insertion_exception_safety) {
    constexpr size_t chunk_size = 512;
    using chunked_vector = utils::chunked_vector<exception_safe_class, chunk_size>;
    constexpr size_t max_chunk_capacity = chunked_vector::max_chunk_capacity();

    // FIXME: convert to seastar test infstrature and use test::random
    // for reproducibility
    std::random_device r;
    auto seed = r();
    BOOST_TEST_MESSAGE(fmt::format("random-seed={}", seed));
    auto rand = std::default_random_engine(seed);
    auto size_dist = std::uniform_int_distribution<size_t>(1, 4 * max_chunk_capacity);

    auto checker = exception_safety_checker();
    auto count = size_dist(rand);
    BOOST_TEST_MESSAGE(fmt::format("count={}", count));
    checker.set_countdown(count - 1);
    try {
        chunked_vector v;
        for (size_t i = 0; i < count; i++) {
            v.emplace_back(checker);
        }
        BOOST_REQUIRE(false);
    } catch (...) {
        BOOST_REQUIRE_EQUAL(checker.live_objects(), 0);
    }
}

BOOST_AUTO_TEST_CASE(tests_insertion_exception_safety_with_reserve) {
    constexpr size_t chunk_size = 512;
    using chunked_vector = utils::chunked_vector<exception_safe_class, chunk_size>;
    constexpr size_t max_chunk_capacity = chunked_vector::max_chunk_capacity();

    // FIXME: convert to seastar test infstrature and use test::random
    // for reproducibility
    std::random_device r;
    auto seed = r();
    BOOST_TEST_MESSAGE(fmt::format("random-seed={}", seed));
    auto rand = std::default_random_engine(seed);
    auto size_dist = std::uniform_int_distribution<size_t>(1, 4 * max_chunk_capacity);
    auto count = size_dist(rand);
    BOOST_TEST_MESSAGE(fmt::format("count={}", count));
    auto checker = exception_safety_checker();
    checker.set_countdown(count - 1);
    try {
        chunked_vector v;
        auto reserve_count = size_dist(rand);
        BOOST_TEST_MESSAGE(fmt::format("reserve_count={}", reserve_count));
        v.reserve(reserve_count);
        for (size_t i = 0; i < count; i++) {
            v.emplace_back(checker);
        }
        BOOST_REQUIRE(false);
    } catch (...) {
        BOOST_REQUIRE_EQUAL(checker.live_objects(), 0);
    }
}

// Reproduces https://github.com/scylladb/scylladb/issues/18635
BOOST_AUTO_TEST_CASE(tests_fill_constructor_exception_safety) {
    constexpr size_t chunk_size = 512;
    using chunked_vector = utils::chunked_vector<exception_safe_class, chunk_size>;
    constexpr size_t max_chunk_capacity = chunked_vector::max_chunk_capacity();

    // FIXME: convert to seastar test infstrature and use test::random
    // for reproducibility
    std::random_device r;
    auto seed = r();
    BOOST_TEST_MESSAGE(fmt::format("random-seed={}", seed));
    auto rand = std::default_random_engine(seed);
    auto size_dist = std::uniform_int_distribution<size_t>(1, 4 * max_chunk_capacity);
    auto count = size_dist(rand);
    BOOST_TEST_MESSAGE(fmt::format("count={}", count));
    auto checker = exception_safety_checker();
    auto filler = std::optional<exception_safe_class>(checker);
    checker.set_countdown(count - 1);
    try {
        chunked_vector v(count, *filler);
        BOOST_REQUIRE(false);
    } catch (...) {
        filler.reset();
        BOOST_REQUIRE_EQUAL(checker.live_objects(), 0);
    }
}

BOOST_AUTO_TEST_CASE(tests_copy_constructor_exception_safety) {
    constexpr size_t chunk_size = 512;
    using chunked_vector = utils::chunked_vector<exception_safe_class, chunk_size>;
    constexpr size_t max_chunk_capacity = chunked_vector::max_chunk_capacity();

    // FIXME: convert to seastar test infstrature and use test::random
    // for reproducibility
    std::random_device r;
    auto seed = r();
    BOOST_TEST_MESSAGE(fmt::format("random-seed={}", seed));
    auto rand = std::default_random_engine(seed);
    auto size_dist = std::uniform_int_distribution<size_t>(1, 4 * max_chunk_capacity);
    auto count = size_dist(rand);
    BOOST_TEST_MESSAGE(fmt::format("count={}", count));
    auto checker = exception_safety_checker();
    chunked_vector src(count, exception_safe_class(checker));

    checker.set_countdown(count - 1);
    try {
        chunked_vector v(src);
        BOOST_REQUIRE(false);
    } catch (...) {
        src.clear();
        BOOST_REQUIRE_EQUAL(checker.live_objects(), 0);
    }
}

BOOST_AUTO_TEST_CASE(test_initializer_list_ctor) {
    using value_t = std::variant<int, double, std::string>;
    constexpr size_t chunk_size = 2;
    auto vec = utils::chunked_vector<value_t, chunk_size>({1, "two", 3.0});
    auto expected = std::vector<value_t>({1, "two", 3.0});
    BOOST_REQUIRE_EQUAL(vec.size(), expected.size());
    auto vit = vec.begin();
    for (auto it = expected.begin(); it != expected.end(); ++it, ++vit) {
        BOOST_REQUIRE(*it == *vit);
    }
    BOOST_REQUIRE(vit == vec.end());
}

BOOST_AUTO_TEST_CASE(test_value_default_init_ctor) {
    int n = 17;
    auto vec = utils::chunked_vector<std::string, 8>(n);
    BOOST_REQUIRE_EQUAL(vec.size(), n);
    for (auto it = vec.begin(); it != vec.end(); ++it) {
        BOOST_REQUIRE(it->empty());
    }
}

BOOST_AUTO_TEST_CASE(test_value_init_ctor) {
    double v = 3.14;
    int n = 17;
    auto vec = utils::chunked_vector<double, 8>(n, v);
    BOOST_REQUIRE_EQUAL(vec.size(), n);
    for (auto it = vec.begin(); it != vec.end(); ++it) {
        BOOST_REQUIRE_EQUAL(*it, v);
    }
}
