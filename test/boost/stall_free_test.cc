/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <compare>
#include <random>
#include <set>
#include <unordered_set>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/random_utils.hh"

#include "utils/stall_free.hh"
#include "utils/small_vector.hh"
#include "utils/chunked_vector.hh"

SEASTAR_THREAD_TEST_CASE(test_merge1) {
    std::list<int> l1{1, 2, 5, 8};
    std::list<int> l2{3};
    std::list<int> expected{1,2,3,5,8};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge2) {
    std::list<int> l1{1};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{1,3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge3) {
    std::list<int> l1{};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge4) {
    std::list<int> l1{1};
    std::list<int> l2{};
    std::list<int> expected{1};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_string) {
    sstring s0 = "hello";
    utils::clear_gently(s0).get();
    BOOST_CHECK(s0.empty());

    std::string s1 = "hello";
    utils::clear_gently(s1).get();
    BOOST_CHECK(s1.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_unique_ptr) {
    std::unique_ptr<int> p = std::make_unique<int>(0);

    // The unique_ptr is expected to be reset by clear_gently
    utils::clear_gently(p).get();
    BOOST_REQUIRE(!p);
}

template <typename T>
struct clear_gently_tracker {
    std::unique_ptr<T> v;
    std::function<void (T)> on_clear;
    clear_gently_tracker() noexcept
        : on_clear([] (T) { BOOST_FAIL("clear_gently called on default-constructed clear_gently_tracker"); })
    {}
    clear_gently_tracker(T i, std::function<void (T)> f) : v(std::make_unique<T>(std::move(i))), on_clear(std::move(f)) {}
    clear_gently_tracker(clear_gently_tracker&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) {}
    clear_gently_tracker& operator=(clear_gently_tracker&& x) noexcept {
        if (&x != this) {
            std::swap(v, x.v);
            std::swap(on_clear, x.on_clear);
        }
        return *this;
    }
    bool operator==(const clear_gently_tracker& o) const noexcept {
        return ptr() == o.ptr();
    }
    std::strong_ordering operator<=>(const clear_gently_tracker& o) const noexcept {
        return uintptr_t(ptr()) <=> uintptr_t(o.ptr());
    }
    future<> clear_gently() noexcept {
        on_clear(*v);
        v.reset();
        return make_ready_future<>();
    }
    operator bool() const noexcept {
        return bool(v);
    }
    const T* ptr() const noexcept {
        return v.get();
    }
};

namespace std {
    template <typename T>
    struct hash<clear_gently_tracker<T>> {
        size_t operator()(const clear_gently_tracker<T>& t) const noexcept {
            return std::hash<uintptr_t>()(uintptr_t(t.ptr()));
        }
    };
} // namespace std

// const objects should be cleared gently directly
// (only when they are held as elements in containers and the container is cleared gently)
SEASTAR_THREAD_TEST_CASE(test_no_clear_gently_const_object) {
    int cleared_gently = 0;
    const auto obj = clear_gently_tracker<int>(0, [&cleared_gently] (int) {
        cleared_gently++;
    });

    BOOST_REQUIRE_EQUAL(utils::HasClearGentlyMethod<decltype(obj)>, false);
    BOOST_REQUIRE_EQUAL(utils::internal::HasClearGentlyImpl<decltype(obj)>, false);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_non_trivial_unique_ptr) {
    int cleared_gently = 0;
    std::unique_ptr<clear_gently_tracker<int>> p = std::make_unique<clear_gently_tracker<int>>(0, [&cleared_gently] (int) {
        cleared_gently++;
    });

    // The unique_ptr is expected to be reset by clear_gently
    // and the wrapped object to be cleared gently before destroyed
    utils::clear_gently(p).get();
    BOOST_REQUIRE(!p);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test re-clearing the already-reset unique_ptr, which should be a no-op
    cleared_gently = 0;
    p.reset();
    utils::clear_gently(p).get();
    BOOST_CHECK(!p);
    BOOST_REQUIRE_EQUAL(cleared_gently, 0);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_unique_ptr_const_payload) {
    int cleared_gently = 0;
    auto p = std::make_unique<const clear_gently_tracker<int>>(0, [&cleared_gently] (int) {
        cleared_gently++;
    });

    // The unique_ptr is expected to be reset by clear_gently
    // and the wrapped object to be cleared gently before destroyed
    utils::clear_gently(p).get();
    BOOST_REQUIRE(!p);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test re-clearing the already-reset unique_ptr, which should be a no-op
    cleared_gently = 0;
    p.reset();
    utils::clear_gently(p).get();
    BOOST_CHECK(!p);
    BOOST_REQUIRE_EQUAL(cleared_gently, 0);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_vector_of_unique_ptrs) {
    int cleared_gently = 0;
    std::vector<std::unique_ptr<clear_gently_tracker<int>>> v;
    v.emplace_back(std::make_unique<clear_gently_tracker<int>>(0, [&cleared_gently] (int) {
        cleared_gently++;
    }));
    v.emplace_back(nullptr);

    // The vector is expected to be reset by clear_gently
    // and the contained objects to be cleared gently before destroyed
    utils::clear_gently(v).get();
    BOOST_REQUIRE_EQUAL(v.size(), 0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_foreign_unique_ptr) {
    int cleared_gently = 0;
    auto make_foreign_ptr = [&cleared_gently] () {
        return smp::submit_to((this_shard_id() + 1) % smp::count, [&cleared_gently] {
            auto p = std::make_unique<clear_gently_tracker<int>>(0, [&cleared_gently, owner_shard = this_shard_id()] (int) {
                BOOST_REQUIRE_EQUAL(owner_shard, this_shard_id());
                cleared_gently++;
            });
            return make_foreign<std::unique_ptr<clear_gently_tracker<int>>>(std::move(p));
        }).get();
    };
    foreign_ptr<std::unique_ptr<clear_gently_tracker<int>>> p0 = make_foreign_ptr();

    // The foreign unique_ptr is expected to be reset by clear_gently
    // and the wrapped object to be cleared gently before destroyed
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test re-clearing the already-reset foreign unique_ptr, which should be a no-op
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_foreign_unique_ptr_const_payload) {
    int cleared_gently = 0;
    auto make_foreign_ptr = [&cleared_gently] () {
        return smp::submit_to((this_shard_id() + 1) % smp::count, [&cleared_gently] {
            auto p = std::make_unique<const clear_gently_tracker<int>>(0, [&cleared_gently, owner_shard = this_shard_id()] (int) {
                BOOST_REQUIRE_EQUAL(owner_shard, this_shard_id());
                cleared_gently++;
            });
            return make_foreign<std::unique_ptr<const clear_gently_tracker<int>>>(std::move(p));
        }).get();
    };
    foreign_ptr<std::unique_ptr<const clear_gently_tracker<int>>> p0 = make_foreign_ptr();

    // The foreign unique_ptr is expected to be reset by clear_gently
    // and the wrapped object to be cleared gently before destroyed
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test re-clearing the already-reset foreign unique_ptr, which should be a no-op
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_foreign_shared_ptr) {
    int cleared_gently = 0;
    auto make_foreign_ptr = [&cleared_gently] () {
        return smp::submit_to((this_shard_id() + 1) % smp::count, [&cleared_gently] {
            auto p = make_lw_shared<clear_gently_tracker<int>>(0, [&cleared_gently, owner_shard = this_shard_id()] (int) {
                BOOST_REQUIRE_EQUAL(owner_shard, this_shard_id());
                cleared_gently++;
            });
            return make_foreign<lw_shared_ptr<clear_gently_tracker<int>>>(std::move(p));
        }).get();
    };
    std::array<foreign_ptr<lw_shared_ptr<clear_gently_tracker<int>>>, 2> ptrs;
    auto& p0 = ptrs[0];
    p0 = make_foreign_ptr();

    // The foreign shared ptr is expected to be reset by clear_gently
    // and the shared object to be cleared gently before destroyed
    // since its use_count is 1
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test re-clearing the already-reset foreign shared ptr, which should be a no-op
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test clearing of foreign shared ptrs when the use_count is greater than 1
    // Both are expected to be reset by clear_gently, but the shared object
    // is expected to be cleared gently only once, before destroyed,
    // when its use_count reaches 1
    p0 = make_foreign_ptr();
    ptrs[1] = p0.copy().get();

    size_t i = tests::random::get_int<size_t>(0, 1);
    utils::clear_gently(ptrs[i]).get();
    BOOST_REQUIRE(!ptrs[i]);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
    utils::clear_gently(ptrs[i ^ 1]).get();
    BOOST_REQUIRE(!ptrs[i ^ 1]);
    BOOST_REQUIRE_EQUAL(cleared_gently, 2);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_foreign_shared_ptr_const_payload) {
    int cleared_gently = 0;
    auto make_foreign_ptr = [&cleared_gently] () {
        return smp::submit_to((this_shard_id() + 1) % smp::count, [&cleared_gently] {
            auto p = make_lw_shared<const clear_gently_tracker<int>>(0, [&cleared_gently, owner_shard = this_shard_id()] (int) {
                BOOST_REQUIRE_EQUAL(owner_shard, this_shard_id());
                cleared_gently++;
            });
            return make_foreign<lw_shared_ptr<const clear_gently_tracker<int>>>(std::move(p));
        }).get();
    };
    std::array<foreign_ptr<lw_shared_ptr<const clear_gently_tracker<int>>>, 2> ptrs;
    auto& p0 = ptrs[0];
    p0 = make_foreign_ptr();

    // The foreign shared ptr is expected to be reset by clear_gently
    // and the shared object to be cleared gently before destroyed
    // since its use_count is 1
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test re-clearing the already-reset foreign shared ptr, which should be a no-op
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test clearing of foreign shared ptrs when the use_count is greater than 1
    // Both are expected to be reset by clear_gently, but the shared object
    // is expected to be cleared gently only once, before destroyed,
    // when its use_count reaches 1
    p0 = make_foreign_ptr();
    ptrs[1] = p0.copy().get();

    size_t i = tests::random::get_int<size_t>(0, 1);
    utils::clear_gently(ptrs[i]).get();
    BOOST_REQUIRE(!ptrs[i]);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
    utils::clear_gently(ptrs[i ^ 1]).get();
    BOOST_REQUIRE(!ptrs[i ^ 1]);
    BOOST_REQUIRE_EQUAL(cleared_gently, 2);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_shared_ptr) {
    int cleared_gently = 0;
    auto make_shared_ptr = [&cleared_gently] () {
        return make_lw_shared<clear_gently_tracker<int>>(cleared_gently, [&cleared_gently, owner_shard = this_shard_id()] (int) {
            cleared_gently++;
        });
    };
    std::array<lw_shared_ptr<clear_gently_tracker<int>>, 2> ptrs;
    auto& p0 = ptrs[0];
    p0 = make_shared_ptr();

    // The shared ptr is expected to be reset by clear_gently
    // and the shared object to be cleared gently before destroyed
    // since its use_count is 1
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test re-clearing the already-reset shared ptr, which should be a no-op
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test clearing of shared ptrs when the use_count is greater than 1
    // Both are expected to be reset by clear_gently, but the shared object
    // is expected to be cleared gently only once, before destroyed,
    // when its use_count reaches 1
    p0 = make_shared_ptr();
    ptrs[1] = p0;

    size_t i = tests::random::get_int<size_t>(0, 1);
    utils::clear_gently(ptrs[i]).get();
    BOOST_REQUIRE(!ptrs[i]);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
    utils::clear_gently(ptrs[i ^ 1]).get();
    BOOST_REQUIRE(!ptrs[i ^ 1]);
    BOOST_REQUIRE_EQUAL(cleared_gently, 2);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_shared_ptr_const_payload) {
    int cleared_gently = 0;
    auto make_shared_ptr = [&cleared_gently] () {
        return make_lw_shared<const clear_gently_tracker<int>>(cleared_gently, [&cleared_gently, owner_shard = this_shard_id()] (int) {
            cleared_gently++;
        });
    };
    std::array<lw_shared_ptr<const clear_gently_tracker<int>>, 2> ptrs;
    auto& p0 = ptrs[0];
    p0 = make_shared_ptr();

    // The shared ptr is expected to be reset by clear_gently
    // and the shared object to be cleared gently before destroyed
    // since its use_count is 1
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test re-clearing the already-reset shared ptr, which should be a no-op
    utils::clear_gently(p0).get();
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    // Test clearing of shared ptrs when the use_count is greater than 1
    // Both are expected to be reset by clear_gently, but the shared object
    // is expected to be cleared gently only once, before destroyed,
    // when its use_count reaches 1
    p0 = make_shared_ptr();
    ptrs[1] = p0;

    size_t i = tests::random::get_int<size_t>(0, 1);
    utils::clear_gently(ptrs[i]).get();
    BOOST_REQUIRE(!ptrs[i]);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
    utils::clear_gently(ptrs[i ^ 1]).get();
    BOOST_REQUIRE(!ptrs[i ^ 1]);
    BOOST_REQUIRE_EQUAL(cleared_gently, 2);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_array) {
    std::array<int, 3> a = {0, 1, 2};
    std::array<int, 3> ref = a;

    utils::clear_gently(a).get();
    // a is expected to remain unchanged
    BOOST_REQUIRE(a == ref);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_non_trivial_array) {
    constexpr int count = 3;
    std::array<std::unique_ptr<clear_gently_tracker<int>>, count> a;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        a[i] = std::make_unique<clear_gently_tracker<int>>(i, [&cleared_gently] (int) {
            cleared_gently++;
        });
    }

    BOOST_REQUIRE(std::ranges::all_of(a, std::mem_fn(&clear_gently_tracker<int>::operator bool)));

    utils::clear_gently(a).get();
    BOOST_REQUIRE_EQUAL(cleared_gently, count);

    BOOST_REQUIRE(std::ranges::none_of(a, std::mem_fn(&std::unique_ptr<clear_gently_tracker<int>>::operator bool)));
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_array_const_payload) {
    constexpr int count = 3;
    int cleared_gently = 0;
    auto tracker = [&cleared_gently] (int) { cleared_gently++; };
    std::array<const clear_gently_tracker<int>, count> a = {
        clear_gently_tracker<int>(0, tracker),
        clear_gently_tracker<int>(1, tracker),
        clear_gently_tracker<int>(2, tracker)
    };

    BOOST_REQUIRE(std::ranges::all_of(a, std::mem_fn(&clear_gently_tracker<int>::operator bool)));

    utils::clear_gently(a).get();
    BOOST_REQUIRE_EQUAL(cleared_gently, count);

    BOOST_REQUIRE(std::ranges::none_of(a, std::mem_fn(&clear_gently_tracker<int>::operator bool)));
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_vector) {
    constexpr int count = 100;
    std::vector<int> v;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(i);
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_small_vector) {
    utils::small_vector<int, 1> v;
    constexpr int count = 10;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(i);
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_vector) {
    std::vector<int> res;
    struct X {
        int v;
        std::vector<int>& r;
        X(int i, std::vector<int>& res) : v(i), r(res) {}
        X(X&& x) noexcept : v(x.v), r(x.r) {}
        future<> clear_gently() noexcept {
            r.push_back(v);
            return make_ready_future<>();
        }
    };
    // Make sure std::vector<X> is not considered as TriviallyClearableSequence
    // although struct X is trivially destructible - since it also
    // has a clear_gently method, that must be called.
    static_assert(std::is_trivially_destructible_v<X>);
    std::vector<X> v;
    constexpr int count = 100;

    res.reserve(count);
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i, res));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());

    // verify that the items were cleared in reverse order
    for (int i = 0; i < count; i++) {
        BOOST_REQUIRE_EQUAL(res[i], 99 - i);
    }
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_small_vector) {
    std::vector<int> res;
    utils::small_vector<clear_gently_tracker<int>, 1> v;
    constexpr int count = 100;

    res.reserve(count);
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(clear_gently_tracker<int>(i, [&res] (int i) {
            res.emplace_back(i);
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());

    // verify that the items were cleared in reverse order
    for (int i = 0; i < count; i++) {
        BOOST_REQUIRE_EQUAL(res[i], 99 - i);
    }
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_chunked_vector) {
    std::vector<int> res;
    utils::chunked_vector<clear_gently_tracker<int>> v;
    constexpr int count = 100;

    res.reserve(count);
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(clear_gently_tracker<int>(i, [&res] (int i) {
            res.emplace_back(i);
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());

    // verify that the items were cleared in reverse order
    for (int i = 0; i < count; i++) {
        BOOST_REQUIRE_EQUAL(res[i], 99 - i);
    }
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_list) {
    constexpr int count = 100;
    std::list<clear_gently_tracker<int>> v;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        v.emplace_back(clear_gently_tracker<int>(i, [&cleared_gently] (int) {
            cleared_gently++;
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_deque) {
    constexpr int count = 100;
    std::deque<clear_gently_tracker<int>> v;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        v.emplace_back(clear_gently_tracker<int>(i, [&cleared_gently] (int) {
            cleared_gently++;
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_unordered_map) {
    std::unordered_map<int, sstring> c;
    constexpr int count = 100;

    for (int i = 0; i < count; i++) {
        c.insert(std::pair<int, sstring>(i, format("{}", i)));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_vector) {
    constexpr int top_count = 10;
    constexpr int count = 10;
    std::vector<std::vector<clear_gently_tracker<std::pair<int, int>>>> c;
    int cleared_gently = 0;

    c.reserve(top_count);
    for (int i = 0; i < top_count; i++) {
        std::vector<clear_gently_tracker<std::pair<int, int>>> v;
        v.reserve(count);
        for (int j = 0; j < count; j++) {
            v.emplace_back(clear_gently_tracker<std::pair<int, int>>({i, j}, [&cleared_gently] (std::pair<int, int>) {
                cleared_gently++;
            }));
        }
        c.emplace_back(std::move(v));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, top_count * count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_object) {
    constexpr int count = 100;
    std::vector<clear_gently_tracker<int>> v;
    int cleared_gently = 0;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(clear_gently_tracker<int>(i, [&cleared_gently] (int) {
            cleared_gently++;
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_map_object) {
    constexpr int count = 100;
    std::map<int, clear_gently_tracker<int>> v;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        v.insert(std::pair<int, clear_gently_tracker<int>>(i, clear_gently_tracker<int>(i, [&cleared_gently] (int) {
            cleared_gently++;
        })));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_unordered_map_object) {
    constexpr int count = 100;
    std::unordered_map<int, clear_gently_tracker<int>> v;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        v.insert(std::pair<int, clear_gently_tracker<int>>(i, clear_gently_tracker<int>(i, [&cleared_gently] (int) {
            cleared_gently++;
        })));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_unordered_map_const_payload) {
    std::unordered_map<int, const clear_gently_tracker<int>> c;
    constexpr int count = 100;
    int cleared_gently = 0;
    auto tracker = [&cleared_gently] (int) { cleared_gently++; };

    for (int i = 0; i < count; i++) {
        c.emplace(i, clear_gently_tracker<int>(i, tracker));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_set_object) {
    constexpr int count = 100;
    std::set<clear_gently_tracker<int>> s;
    int cleared_gently = 0;
    auto tracker = [&cleared_gently] (int) { cleared_gently++; };

    for (int i = 0; i < count; i++) {
        s.insert(clear_gently_tracker<int>(i, tracker));
    }
    BOOST_REQUIRE_EQUAL(s.size(), count);

    utils::clear_gently(s).get();
    BOOST_CHECK(s.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_unordered_set_object) {
    constexpr int count = 100;
    std::unordered_set<clear_gently_tracker<int>> s;
    int cleared_gently = 0;
    auto tracker = [&cleared_gently] (int) { cleared_gently++; };

    for (int i = 0; i < count; i++) {
        s.insert(clear_gently_tracker<int>(i, tracker));
    }
    BOOST_REQUIRE_EQUAL(s.size(), count);

    utils::clear_gently(s).get();
    BOOST_CHECK(s.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_unordered_map) {
    constexpr int top_count = 10;
    constexpr int count = 10;
    std::unordered_map<int, std::vector<clear_gently_tracker<std::pair<int, int>>>> c;
    int cleared_gently = 0;

    for (int i = 0; i < top_count; i++) {
        std::vector<clear_gently_tracker<std::pair<int, int>>> v;
        v.reserve(count);
        for (int j = 0; j < count; j++) {
            v.emplace_back(clear_gently_tracker<std::pair<int, int>>({i, j}, [&cleared_gently] (std::pair<int, int>) {
                cleared_gently++;
            }));
        }
        c.insert(std::pair<int, std::vector<clear_gently_tracker<std::pair<int, int>>>>(i, std::move(v)));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, top_count * count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_container) {
    constexpr int top_count = 10;
    constexpr int count = 10;
    std::list<std::unordered_map<int, clear_gently_tracker<std::pair<int, int>>>> c;
    int cleared_gently = 0;

    for (int i = 0; i < top_count; i++) {
        std::unordered_map<int, clear_gently_tracker<std::pair<int, int>>> m;
        for (int j = 0; j < count; j++) {
            m.insert(std::pair<int, clear_gently_tracker<std::pair<int, int>>>(j, clear_gently_tracker<std::pair<int, int>>({i, j}, [&cleared_gently] (std::pair<int, int>) {
                cleared_gently++;
            })));
        }
        c.push_back(std::move(m));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, top_count * count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_multi_nesting) {
    struct V {
        std::vector<clear_gently_tracker<std::tuple<int, int, int>>> v;
        V(int i, int j, int count, std::function<void (std::tuple<int, int, int>)> f) {
            v.reserve(count);
            for (int k = 0; k < count; k++) {
                v.emplace_back(clear_gently_tracker<std::tuple<int, int, int>>({i, j, k}, f));
            }
        }
        future<> clear_gently() {
            return utils::clear_gently(v);
        }
    };
    constexpr int top_count = 10;
    constexpr int mid_count = 10;
    constexpr int count = 10;
    std::vector<std::map<int, V>> c;
    int cleared_gently = 0;

    for (int i = 0; i < top_count; i++) {
        std::map<int, V> m;
        for (int j = 0; j < mid_count; j++) {
            m.insert(std::pair<int, V>(j, V(i, j, count, [&cleared_gently] (std::tuple<int, int, int>) {
                cleared_gently++;
            })));
        }
        c.push_back(std::move(m));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, top_count * mid_count * count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_optional) {
    int cleared_gently = 0;
    std::optional<clear_gently_tracker<int>> opt = std::make_optional<clear_gently_tracker<int>>(0, [&cleared_gently] (int) {
        cleared_gently++;
    });

    BOOST_CHECK(opt);
    utils::clear_gently(opt).get();
    BOOST_CHECK(opt);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    cleared_gently = 0;
    opt.reset();
    utils::clear_gently(opt).get();
    BOOST_REQUIRE_EQUAL(cleared_gently, 0);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_optional_const_payload) {
    int cleared_gently = 0;
    std::optional<const clear_gently_tracker<int>> opt = std::make_optional<const clear_gently_tracker<int>>(0, [&cleared_gently] (int) {
        cleared_gently++;
    });

    BOOST_CHECK(opt);
    utils::clear_gently(opt).get();
    BOOST_CHECK(opt);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    cleared_gently = 0;
    opt.reset();
    utils::clear_gently(opt).get();
    BOOST_REQUIRE_EQUAL(cleared_gently, 0);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_vector_of_optionals) {
    int cleared_gently = 0;
    std::vector<std::optional<clear_gently_tracker<int>>> v;
    v.emplace_back(std::make_optional<clear_gently_tracker<int>>(0, [&cleared_gently] (int) {
        cleared_gently++;
    }));
    v.emplace_back(std::nullopt);

    utils::clear_gently(v).get();
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_optimized_optional) {
    int cleared_gently = 0;
    seastar::optimized_optional<clear_gently_tracker<int>> opt(clear_gently_tracker<int>(0, [&cleared_gently] (int) {
        cleared_gently++;
    }));

    BOOST_CHECK(opt);
    utils::clear_gently(opt).get();
    BOOST_CHECK(!opt);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    cleared_gently = 0;
    utils::clear_gently(opt).get();
    BOOST_REQUIRE_EQUAL(cleared_gently, 0);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_vector_of_optimized_optionals) {
    int cleared_gently = 0;
    std::vector<seastar::optimized_optional<clear_gently_tracker<int>>> v;
    v.emplace_back(clear_gently_tracker<int>(0, [&cleared_gently] (int) {
        cleared_gently++;
    }));
    v.emplace_back(std::nullopt);

    utils::clear_gently(v).get();
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
}

SEASTAR_THREAD_TEST_CASE(test_reserve_gently_with_chunked_vector) {
    auto rand = std::default_random_engine();
    auto size_dist = std::uniform_int_distribution<unsigned>(1, 1 << 12);

    for (int i = 0; i < 100; ++i) {
        utils::chunked_vector<uint8_t, 512> v;
        const auto size = size_dist(rand);
        utils::reserve_gently(v, size).get();
        BOOST_REQUIRE_EQUAL(v.capacity(), size);
    }
}
