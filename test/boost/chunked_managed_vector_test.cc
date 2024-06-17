/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"

#include <deque>
#include <random>
#include "utils/lsa/chunked_managed_vector.hh"
#include "utils/managed_ref.hh"
#include "test/lib/log.hh"

#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/equal.hpp>
#include <boost/range/algorithm/reverse.hpp>
#include <boost/range/irange.hpp>

using namespace logalloc;

using disk_array = lsa::chunked_managed_vector<uint64_t>;

using deque = std::deque<int>;

SEASTAR_TEST_CASE(test_random_walk) {
  region region;
  allocating_section as;
  with_allocator(region.allocator(), [&] {
    auto rand = std::default_random_engine();
    auto op_gen = std::uniform_int_distribution<unsigned>(0, 9);
    auto nr_dist = std::geometric_distribution<size_t>(0.7);
    deque d;
    disk_array c;
    for (auto i = 0; i != 1000000; ++i) {
        if (i % 10000 == 0) {
            region.full_compaction();
        }
        as(region, [&] {
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
        });
        BOOST_REQUIRE_EQUAL(c.size(), d.size());
        BOOST_REQUIRE(boost::equal(c, d));
    }
  });
  return make_ready_future<>();
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
    exception_safety_checker* _esc;
public:
    explicit exception_safe_class(exception_safety_checker& esc) : _esc(&esc) {
        _esc->add_live_object();
    }
    exception_safe_class(const exception_safe_class& x) : _esc(x._esc) {
        _esc->add_live_object();
    }
    exception_safe_class(exception_safe_class&&) = default;
    ~exception_safe_class() {
        _esc->del_live_object();
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

SEASTAR_TEST_CASE(tests_constructor_exception_safety) {
  region region;
  allocating_section as;
  with_allocator(region.allocator(), [&] {
   as(region, [&] {
    auto checker = exception_safety_checker();
    auto v = std::vector<exception_safe_class>(100, exception_safe_class(checker));
    checker.set_countdown(5);
    try {
        auto u = lsa::chunked_managed_vector<exception_safe_class>(v.begin(), v.end());
        BOOST_REQUIRE(false);
    } catch (...) {
        v.clear();
        BOOST_REQUIRE(checker.ok());
    }
   });
  });
  return make_ready_future<>();
}

SEASTAR_TEST_CASE(tests_reserve_partial) {
  region region;
  allocating_section as;
  with_allocator(region.allocator(), [&] {
   as(region, [&] {
    auto rand = std::default_random_engine();
    // use twice the max_chunk_capacity() as upper limit to test if
    // reserve_partial() can reserve capacity across multiple chunks.
    auto max_test_size = lsa::chunked_managed_vector<uint8_t>::max_chunk_capacity() * 2;
    auto size_dist = std::uniform_int_distribution<unsigned>(1, max_test_size);

    for (int i = 0; i < 100; ++i) {
        lsa::chunked_managed_vector<uint8_t> v;
        const auto size = size_dist(rand);
        while (v.capacity() != size) {
            v.reserve_partial(size);
        }
        BOOST_REQUIRE_EQUAL(v.capacity(), size);
    }
   });
  });
  return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_clear_and_release) {
    region region;
    allocating_section as;

    with_allocator(region.allocator(), [&] {
        lsa::chunked_managed_vector<managed_ref<uint64_t>> v;

        for (uint64_t i = 1; i < 4000; ++i) {
            as(region, [&] {
                v.emplace_back(make_managed<uint64_t>(i));
            });
        }

        v.clear_and_release();
    });

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_chunk_reserve) {
    region region;
    allocating_section as;

    for (auto conf :
            { // std::make_pair(reserve size, push count)
                std::make_pair(0u, 4000u),
                std::make_pair(100u, 4000u),
                std::make_pair(200u, 4000u),
                std::make_pair(1000u, 4000u),
                std::make_pair(2000u, 4000u),
                std::make_pair(3000u, 4000u),
                std::make_pair(5000u, 4000u),
                std::make_pair(500u, 8000u),
                std::make_pair(1000u, 8000u),
                std::make_pair(2000u, 8000u),
                std::make_pair(8000u, 500u),
            })
    {
        with_allocator(region.allocator(), [&] {
            auto [reserve_size, push_count] = conf;
            testlog.info("Testing reserve({}), {}x emplace_back()", reserve_size, push_count);
            lsa::chunked_managed_vector<managed_ref<uint64_t>> v;
            v.reserve(reserve_size);
            uint64_t seed = rand();
            for (uint64_t i = 0; i < push_count; ++i) {
                as(region, [&] {
                    v.emplace_back(make_managed<uint64_t>(seed + i));
                    BOOST_REQUIRE(**v.begin() == seed);
                });
            }
            auto v_it = v.begin();
            for (uint64_t i = 0; i < push_count; ++i) {
                BOOST_REQUIRE(**v_it++ == seed + i);
            }
            v.clear_and_release();
        });
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_correctness_when_crossing_chunk_boundary) {
    region region;
    allocating_section as;
    with_allocator(region.allocator(), [&] {
        as(region, [&] {
            size_t max_chunk_size = lsa::chunked_managed_vector<int>::max_chunk_capacity();

            lsa::chunked_managed_vector<size_t> v;
            for (size_t i = 0; i < (max_chunk_size + 1); i++) {
                v.push_back(i);
            }
            BOOST_REQUIRE(v.back() == max_chunk_size);
            BOOST_REQUIRE(v.at(max_chunk_size) == max_chunk_size);
            v.pop_back();
            BOOST_REQUIRE(v.back() == max_chunk_size - 1);
            BOOST_REQUIRE(v.at(max_chunk_size - 1) == max_chunk_size - 1);
        });
    });
    return make_ready_future<>();
}

// Tests the case of make_room() invoked with last_chunk_capacity_deficit but _size not in
// the last reserved chunk.
SEASTAR_TEST_CASE(test_shrinking_and_expansion_involving_chunk_boundary) {
    region region;
    allocating_section as;

    with_allocator(region.allocator(), [&] {
        lsa::chunked_managed_vector<managed_ref<uint64_t>> v;

        // Fill two chunks
        v.reserve(2000);
        for (uint64_t i = 0; i < 2000; ++i) {
            as(region, [&] {
                v.emplace_back(make_managed<uint64_t>(i));
            });
        }

        // Make the last chunk smaller than max size to trigger the last_chunk_capacity_deficit path in make_room()
        v.shrink_to_fit();

        // Leave the last chunk reserved but empty
        for (uint64_t i = 0; i < 1000; ++i) {
            v.pop_back();
        }

        // Try to reserve more than the currently reserved capacity and trigger last_chunk_capacity_deficit path
        // with _size not in the last chunk. Should not sigsegv.
        v.reserve(8000);

        for (uint64_t i = 0; i < 2000; ++i) {
            as(region, [&] {
                v.emplace_back(make_managed<uint64_t>(i));
            });
        }

        v.clear_and_release();
    });

    return make_ready_future<>();
}

struct push_back_item {
    std::unique_ptr<int> p;
    push_back_item() = default;
    push_back_item(int v) : p(std::make_unique<int>(v)) {}
    push_back_item(const push_back_item& x) : push_back_item(x.value() + 1) {}
    push_back_item(push_back_item&& x) noexcept : p(std::exchange(x.p, nullptr)) {}

    int value() const noexcept { return *p; }
};

template <class VectorType>
static void do_test_push_back_using_existing_element(std::function<void (VectorType&, const managed_ref<push_back_item>&)> do_push_back) {
    region region;
    allocating_section as;

    with_allocator(region.allocator(), [&] {
        VectorType v;
        as(region, [&] {
            v.push_back(make_managed<push_back_item>(0));
            for (int i = 0; i < 1000; i++) {
                do_push_back(v, v.back());
            }
        });
        for (int i = 0; i < 1000; i++) {
            BOOST_REQUIRE_EQUAL(v[i]->value(), i);
        }
    });
}

SEASTAR_TEST_CASE(test_push_back_using_existing_element) {
    using chunked_managed_vector_type = lsa::chunked_managed_vector<managed_ref<push_back_item>>;
    do_test_push_back_using_existing_element<chunked_managed_vector_type>([] (chunked_managed_vector_type& v, const managed_ref<push_back_item>& x) { v.push_back(make_managed<push_back_item>(*x)); });
    do_test_push_back_using_existing_element<chunked_managed_vector_type>([] (chunked_managed_vector_type& v, const managed_ref<push_back_item>& x) { v.emplace_back(make_managed<push_back_item>(*x)); });
    return make_ready_future<>();
}
