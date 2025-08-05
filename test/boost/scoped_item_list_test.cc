/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// Unit tests for utils::scoped_item_list<T>

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"

#include <seastar/core/future.hh>
#include <seastar/core/preempt.hh>
#include <seastar/util/later.hh>
#include <vector>
#include <unordered_set>

#include <seastar/core/coroutine.hh>

#include "utils/scoped_item_list.hh"

// A basic test for running a bunch of emplace() and then listing the items
// with for_each_gently(). Also has a basic check of empty() and size().
SEASTAR_TEST_CASE(test_scoped_item_basic) {
    utils::scoped_item_list<int> list;
    BOOST_CHECK(list.empty());
    BOOST_CHECK_EQUAL(list.size(), 0);
    int N = 42;
    // Add N items to the list, each with a different value.
    // We save all the handles we get back from the list.emplace() calls, so
    // that the items all remain in the list. In a separate test below we'll
    // check how if we let go of handles, the items go away.
    std::vector<utils::scoped_item_list<int>::handle> handles;
    for (int i = 0; i < N; i++) {
        handles.emplace_back(list.emplace(i));
    }
    BOOST_CHECK_EQUAL(list.size(), N);
    BOOST_CHECK(!list.empty());
    std::unordered_set<int> seen;
    co_await list.for_each_gently([&seen](int item) {
        seen.insert(item);
    });
    // Verify that all N items were listed. Although in the current
    // implementation the items are listed in the order they were added, this
    // isn't guaranteed in the documentation, so we just check that all N
    // items were seen.
    BOOST_CHECK_EQUAL(seen.size(), N);
    for (int i = 0; i < N; i++) {
        BOOST_CHECK(seen.find(i) != seen.end());
    }
}

// Now check that if we let go of the handles, the items are removed from
// the list. In this first test we just let go of each handle received from
// emplace(), so each item is removed from the list right after it is added.
SEASTAR_TEST_CASE(test_scoped_item_remove_1) {
    utils::scoped_item_list<int> list;
    int N = 42;
    for (int i = 0; i < N; i++) {
        // Note that we don't save the handle returned by emplace(), so the
        // item is added by emplace() and then immediately removed when the
        // handle goes out of scope.
        (void)list.emplace(i);
    }
    // Now there should be no items in the list.
    BOOST_CHECK(list.empty());
    BOOST_CHECK_EQUAL(list.size(), 0);
    co_await list.for_each_gently([](int item) {
        BOOST_FAIL("for_each_gently() should not be called, since the list is empty");
    });
}

// Another simple test for removing items from the list, but this time we
// keep the handles, so the items remain in the list until the end of the
// test, and then we let go of the handles, so the items are removed.
SEASTAR_TEST_CASE(test_scoped_item_remove_2) {
    utils::scoped_item_list<int> list;
    int N = 42;
    std::vector<utils::scoped_item_list<int>::handle> handles;
    for (int i = 0; i < N; i++) {
        // Keep the handles in a vector, so the items remain in the list.
        handles.emplace_back(list.emplace(i));
    }
    // At this point the list should still contain N items.
    BOOST_CHECK_EQUAL(list.size(), N);
    // Now let go of all the handles. The list should become empty.
    handles.clear();
    BOOST_CHECK(list.empty());
    BOOST_CHECK_EQUAL(list.size(), 0);
    co_await list.for_each_gently([](int item) {
        BOOST_FAIL("for_each_gently() should not be called, since the list is empty");
    });
}

// Another test for removing items from the list, but this time we
// let go half the handles and keep the other half, so the list at the
// end contains only the items that were not removed.
SEASTAR_TEST_CASE(test_scoped_item_remove_3) {
    utils::scoped_item_list<int> list;
    int N = 42;
    std::vector<utils::scoped_item_list<int>::handle> handles_even, handles_odd;
    for (int i = 0; i < N; i++) {
        auto h = list.emplace(i);
        // Keep the even and odd handles in separate vectors, so we can clear
        // half of them easily.
        if (i % 2 == 0) {
            // Note that this test also exercises the move constructor of
            // scoped_item_list<T>::handle.
            handles_even.emplace_back(std::move(h));
        } else {
            handles_odd.emplace_back(std::move(h));
        }
    }
    // At this point the list should still contain N items.
    BOOST_CHECK_EQUAL(list.size(), N);
    // Now let go of all the even handles. The list should now have fewer
    // items (only the odd ones).
    handles_even.clear();
    BOOST_CHECK(list.size() == handles_odd.size());
    co_await list.for_each_gently([](int item) {
        BOOST_CHECK(item % 2 == 1); // should only see odd items
    });
    // Now let go of the odd handles. The list should become empty.
    handles_odd.clear();
    BOOST_CHECK(list.empty());
        co_await list.for_each_gently([](int item) {
        BOOST_FAIL("for_each_gently() should not be called, since the list is empty");
    });
}

future<std::unordered_set<int>> get_items(utils::scoped_item_list<int>& list) {
    std::unordered_set<int> ret;
    co_await list.for_each_gently([&ret](int& item) {
        ret.insert(item);
    });
    co_return ret;
}

// variant of the above simple get_items() that works with a type T that
// can't be hashed or sorted, so let's return a vector. To ignore order
// in comparison, use std::is_permutation().
template <typename T>
future<std::vector<T>> get_items(utils::scoped_item_list<T>& list) {
    std::vector<T> ret;
    co_await list.for_each_gently([&ret](T& item) {
        ret.emplace_back(item);
    });
    co_return ret;
}

// The previous test used the handle's move constructor, but here we want
// to test it more rigorously. When a handle is moved, the item remains
// in the list, but its lifetime is now controlled by the new handle.
SEASTAR_TEST_CASE(test_scoped_item_handle_move) {
    utils::scoped_item_list<int> list;
    using handle = utils::scoped_item_list<int>::handle;
    handle h1 = list.emplace(1);
    handle h2 = list.emplace(2);
    handle h3 = list.emplace(3);
    BOOST_CHECK(list.size() == 3);
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1, 2, 3}));
    // Move the handle h2 to newh2. The item list doesn't change because
    // the handle h2 is no longer on the linked list, but newh2 is.
    {
        handle newh2 = std::move(h2);
        BOOST_CHECK(list.size() == 3);
        BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1, 2, 3}));
    }
    // We closed the block, so newh2 holding item "2" went out of scope,
    // so the item 2 should be gone from the list. This proves that after
    // the move, the item is really held by newh2, not h2 (h2 is still in
    // scope, but should no longer be used).
    BOOST_CHECK(list.size() == 2);
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1, 3}));
}

// Test that the content of item can be read or written through the handle,
// using the get() method or the "*" operator on the handle.
// This feature will probably not be needed in most use cases, but
// scoped_item_list allows it, so we test it here.
SEASTAR_TEST_CASE(test_scoped_item_handle_read_write) {
    utils::scoped_item_list<int> list;
    auto h = list.emplace(42);
    BOOST_CHECK(h.get() == 42);
    h.get() = 43; // write through the handle
    BOOST_CHECK(h.get() == 43);
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({43}));

    // Check that the handle's operator* does the same as get().
    BOOST_CHECK(*h == 43);
    *h = 44;
    BOOST_CHECK(*h == 44);
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({44}));
}

// The previous tests all inserted all items before removing some (by
// dropping handles) and listing the items. In this test we interleave
// item addition, removal and listing. However, this test is still
// sequential - it doesn't do the modifications in the *middle* of the
// for_each_gently() iteration. That is tested below.
SEASTAR_TEST_CASE(test_scoped_item_interleave) {
    utils::scoped_item_list<int> list;
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({}));
    auto h1 = list.emplace(1);
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1}));
    {
        auto h2 = list.emplace(2);
        BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1, 2}));
    }
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1}));
    auto h3 = list.emplace(3);
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1, 3}));
}

template<class ContainerA, class ContainerB>
bool is_permutation(ContainerA&& a, ContainerB&& b) {
    return std::is_permutation(std::begin(a), std::end(a), std::begin(b), std::end(b));
}

// The previous test all used scoped_item_list<int>. In this test we try a
// a scoped_item_list<T> with a more elaborate T.
SEASTAR_TEST_CASE(test_scoped_item_elaborate_type) {
    struct T {
        int a;
        double b;
        std::string c;
        T(int a, std::string c)
            : a(a), b(a), c(std::move(c)) {}
        T(T&& other) noexcept = default;
        T(const T&) = default;
        bool operator==(const T& other) const {
            return a == other.a && b == other.b && c == other.c;
        }
    };
    utils::scoped_item_list<T> list;
    (void)list.emplace(1, "one"); // item 1 will be immediately removed
    auto h2 = list.emplace(2, "two");
    auto h3 = list.emplace(3, "three");
    auto h4 = list.emplace(4, "four");
    {
        // test for move constructor with a non-trivial T
        utils::scoped_item_list<T>::handle newh3 = std::move(h3);
        // now newh3 will go out of scope, so the item 3 will be removed
    }
    BOOST_CHECK(is_permutation(co_await get_items(list), std::vector<T>({ T(2, "two"), T(4, "four") })));
}

// for_each_gently() allows to iterate over the items in the list while
// avoiding stalls by possibly preempting between items. It has special code
// to handle the case when the iteration is preempted the code that gets to
// run modifies the list, possibly removing the last item listed, the next
// item to be listed, or even everything in the list. This following tests,
// test_scoped_item_for_each_gently_concurrent_*, are several attempts to
// exercise this case.
// Note that we do not actually achieve preemption in these tests - I wrote
// such tests but they are really hard to get write, and behave differently
// on debug builds (where ready continuations can be reordered). Instead,
// we do the concurrent modification **inside** the function passed to
// for_each_gently(), so that the modification happens at the end of a
// function call instead of at a preemption point between two function calls.
// This is not exactly the same, but it is close enough to test the code
// that handles the case when the list is modified while iterating over it.
SEASTAR_TEST_CASE(test_scoped_item_for_each_gently_concurrent_1) {
    // We'll create a list of three items, and remove the *second* item
    // during one of the three possible preemption points (after the first
    // item, after the second item, or after the third item). We check that
    // the iteration doesn't crash, and yields the right list.
    for (int when = 1; when <= 3; when++) {
        utils::scoped_item_list<int> list;
        auto h1 = list.emplace(1);
        auto h2 = list.emplace(2);
        auto h3 = list.emplace(3);
        BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1, 2, 3}));
        std::unordered_set<int> items;
        co_await list.for_each_gently([&items, &h2, when](int& item) mutable {
            items.insert(item);
            if (item == when) {
                // remove h2. There is no release() method, so we use a move
                // after which the moved object goes out of scope
                {
                    auto h = std::move(h2);
                }
            }
        });
        // If when < 2, we removed item 2 before it was outputted to "items"
        // so items contains only items 1 and 3. But if when >= 2, we
        // removed item 2 after it already reached "items", so items contains
        // items 1, 2 and 3.
        if (when < 2) {
            BOOST_CHECK(items == std::unordered_set<int>({1, 3}));
        } else {
            BOOST_CHECK(items == std::unordered_set<int>({1, 2, 3}));
        }
        // Starting another iteration now, without deleting stuff in the
        // middle, will see again the same items, without item 2 which we
        // deleted in the previous iteration.
        BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1, 3}));
    }
}
SEASTAR_TEST_CASE(test_scoped_item_for_each_gently_concurrent_2) {
    // Another attempt to "preempt" (again, not really preempt but just modify
    // the list) in the middle of for_each_gently(),
    // this time we remove *all* the items in the list after just one
    // item is output, and expect the iteration to end without finding
    // any more items (and, obviosly, without crashing).
    utils::scoped_item_list<int> list;
    std::vector<utils::scoped_item_list<int>::handle> handles;
    handles.push_back(list.emplace(1));
    handles.push_back(list.emplace(2));
    handles.push_back(list.emplace(3));
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({1, 2, 3}));
    std::unordered_set<int> items;
    co_await list.for_each_gently([&items, &handles](int& item) mutable {
        items.insert(item);
        // remove all items from the list
        handles.clear();
    });
    // We cleared the list after seeing just one item, so we expect "items"
    // to have just one item. But if we look at the list again now, it should
    // be completely empty.
    BOOST_CHECK(items.size() == 1);
    BOOST_CHECK(co_await get_items(list) == std::unordered_set<int>({}));
}

// TODO: a handle's destructor may need to fix the iterators of more than one
// gentle_iterator, so we need to test that. I don't know yet how to do this.
