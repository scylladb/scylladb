/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <fmt/core.h>
#include <functional>
#include <iterator>
#include <new>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include "collection_stress.hh"
#include "btree_validation.hh"
#include "tree_test_key.hh"
#include "utils/intrusive_btree.hh"

using namespace intrusive_b;
using namespace seastar;

class test_key : public tree_test_key_base {
    member_hook b_hook;

public:
    struct tri_compare {
        test_key_tri_compare _cmp;
        template <typename A, typename B>
        std::strong_ordering operator()(const A& a, const B& b) const noexcept { return _cmp(a, b); }
    };
    using test_tree = tree<test_key, &test_key::b_hook, tri_compare, 4, 5, key_search::both, with_debug::yes>;
    // `rows_test_tree` has the node size and the linear threshold of rows_entry::container_type.
    using rows_test_tree = tree<test_key, &test_key::b_hook, tri_compare, 12, 20, key_search::both, with_debug::yes>;
    using rows_test_validator = validator<test_key, &test_key::b_hook, tri_compare, 12, 20>;
    test_key(int nr, int cookie) noexcept : tree_test_key_base(nr, cookie) {}
    test_key(int nr) noexcept : tree_test_key_base(nr) {}
    test_key(const test_key& o) : tree_test_key_base(o, tree_test_key_base::force_copy_tag{}) {}
    test_key(test_key&&) = delete;
    bool linked() const noexcept { return b_hook.attached(); }
    const void* hook_node() const noexcept { return b_hook.node(); }
};

using test_tree = test_key::test_tree;
auto key_deleter = [] (test_key* key) noexcept { delete key; };
test_key::tri_compare cmp;

BOOST_AUTO_TEST_CASE(test_ops_empty_tree) {
    /* Sanity checks for no nullptr dereferences */
    test_tree t;
    t.erase(1, cmp);
    t.find(1, cmp);
}

BOOST_AUTO_TEST_CASE(test_plain_key_pointer) {
    test_tree t;

    t.insert(std::make_unique<test_key>(1), cmp);
    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_double_insert) {
    /* No assertions should happen in ~tree */
    test_tree t;
    auto i = t.insert(std::make_unique<test_key>(1), cmp);
    BOOST_REQUIRE(i.second);
    i = t.insert(std::make_unique<test_key>(1), cmp);
    BOOST_REQUIRE(!i.second);
    t.erase_and_dispose(1, cmp, key_deleter);
}

BOOST_AUTO_TEST_CASE(test_cookie_find) {
    test_tree t;
    t.insert(std::make_unique<test_key>(1, 132), cmp);

    auto i = t.find(1, cmp);
    BOOST_REQUIRE(i->cookie() == 132);
    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_double_erase) {
    test_tree t;
    t.insert(std::make_unique<test_key>(1), cmp);
    t.insert(std::make_unique<test_key>(2), cmp);
    auto i = t.erase_and_dispose(1, cmp, key_deleter);
    BOOST_REQUIRE(*i == 2);
    i = t.erase(1, cmp);
    BOOST_REQUIRE(i == t.end());
    i = t.erase_and_dispose(2, cmp, key_deleter);
    BOOST_REQUIRE(i == t.end());
    t.erase(2, cmp);
}

BOOST_AUTO_TEST_CASE(test_remove_corner_case) {
    /* Sanity check for erasure to be precise */
    test_tree t;
    t.insert(std::make_unique<test_key>(1), cmp);
    t.insert(std::make_unique<test_key>(2), cmp);
    t.insert(std::make_unique<test_key>(3), cmp);
    t.erase_and_dispose(1, cmp, key_deleter);
    t.erase_and_dispose(3, cmp, key_deleter);
    auto f = t.find(2, cmp);
    BOOST_REQUIRE(*f == 2);
    t.erase_and_dispose(2, cmp, key_deleter);
}

BOOST_AUTO_TEST_CASE(test_end_iterator) {
    /* Check std::prev(end()) */
    test_tree t;
    t.insert(std::make_unique<test_key>(1), cmp);
    auto i = std::prev(t.end());
    BOOST_REQUIRE(*i == 1);
    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_next_to_end_iterator) {
    /* Same, but with "artificial" end iterator */
    test_tree t;
    auto i = t.insert(std::make_unique<test_key>(1), cmp).first;
    i++;
    BOOST_REQUIRE(i == t.end());
    i--;
    BOOST_REQUIRE(*i == 1);
    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_clear) {
    /* Quick check for tree::clear */
    test_tree t;

    for (int i = 0; i < 32; i++) {
        t.insert(std::make_unique<test_key>(i), cmp);
    }

    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_post_clear) {
    /* Check that tree is work-able after clear */
    test_tree t;

    t.insert(std::make_unique<test_key>(1), cmp);
    t.clear_and_dispose(key_deleter);
    t.insert(std::make_unique<test_key>(2), cmp);
    t.erase_and_dispose(2, cmp, key_deleter);
}

BOOST_AUTO_TEST_CASE(test_iterator_erase) {
    /* Check iterator::erase */
    test_tree t;
    auto it = t.insert(std::make_unique<test_key>(2), cmp);
    t.insert(std::make_unique<test_key>(5), cmp);
    auto in = t.erase_and_dispose(it.first, key_deleter);
    BOOST_REQUIRE(*in == 5);
    BOOST_REQUIRE(*t.find(5, cmp) == 5);
    t.erase_and_dispose(5, cmp, key_deleter);
}

BOOST_AUTO_TEST_CASE(test_iterator_equal) {
    test_tree t;
    auto i1 = t.insert(std::make_unique<test_key>(1), cmp);
    auto i2 = t.insert(std::make_unique<test_key>(2), cmp);
    auto i3 = t.find(1, cmp);
    BOOST_REQUIRE(i1.first == i3);
    BOOST_REQUIRE(i1.first != i2.first);
    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_find_all) {
    test_tree t;
    int nkeys = 16;

    for (int i = 0; i < nkeys; i++) {
        t.insert(std::make_unique<test_key>(2 * i + 1), cmp);
    }

    for (int i = 0; ; i++) {
        auto it = t.find(i, cmp);

        if (i == 2 * nkeys + 1) {
            BOOST_REQUIRE(it == t.end());
            break;
        }

        if (i % 2 == 0) {
            BOOST_REQUIRE(it == t.end());
        } else {
            BOOST_REQUIRE(*it == i);
        }
    }

    t.clear_and_dispose(key_deleter);
}

BOOST_DATA_TEST_CASE(test_lower_bound_sz,
                     boost::unit_test::data::make({1, 3, 16}), nkeys) {
    test_tree t;

    for (int i = 0; i < nkeys; i++) {
        t.insert(std::make_unique<test_key>(2 * i + 1), cmp);
    }

    for (int i = 0; ; i++) {
        bool match;
        auto it = t.lower_bound(i, match, cmp);

        if (it == t.end()) {
            BOOST_REQUIRE(i == 2 * nkeys);
            break;
        } else if (i % 2 == 0) {
            BOOST_REQUIRE(!match && *it == i + 1);
        } else {
            BOOST_REQUIRE(match && *it == i);
        }
    }

    for (int i = 3; ; i += 2) {
        auto it = t.begin();
        if (it == t.end()) {
            break;
        }

        t.erase_and_dispose(it, key_deleter);

        bool match;
        it = t.lower_bound(0, match, cmp);
        if (it == t.end()) {
            BOOST_REQUIRE(i == 2 * nkeys + 1);
        } else {
            BOOST_REQUIRE(!match && *it == i);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_upper_bound_all) {
    test_tree t;
    int nkeys = 16;

    for (int i = 0; i < nkeys; i++) {
        t.insert(std::make_unique<test_key>(2 * i + 1), cmp);
    }

    for (int i = 0; ; i++) {
        auto it = t.upper_bound(i, cmp);

        if (it == t.end()) {
            BOOST_REQUIRE(i == 2 * nkeys - 1);
            break;
        } else if (i % 2 == 0) {
            BOOST_REQUIRE(*it == i + 1);
        } else {
            BOOST_REQUIRE(*it == i + 2);
        }
    }

    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_insert_iterator_index) {
    /* Check insertion iterator ++ and duplicate key */
    test_tree t;
    t.insert(std::make_unique<test_key>(1), cmp);
    t.insert(std::make_unique<test_key>(3), cmp);
    auto i = t.insert(std::make_unique<test_key>(2), cmp).first;
    i++;
    BOOST_REQUIRE(*i == 3);
    auto i2 = t.insert(std::make_unique<test_key>(2), cmp); /* 2nd insert finds the previous */
    BOOST_REQUIRE(!i2.second);
    i2.first++;
    BOOST_REQUIRE(*(i2.first) == 3);
    t.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_insert_before) {
    int size = 16;

    for (int num_keys = 0; num_keys < size; num_keys++) {
        test_tree tree;

        for (int i = 0; i < num_keys; i++) {
            tree.insert(std::make_unique<test_key>(2 * i + 1), cmp);
        }

        auto bi = tree.begin();

        for (int i = 0; i <= num_keys; i++) {
            if (i != 0) {
                bi++;
            }

            auto ni = tree.insert_before(bi, std::make_unique<test_key>(2 * i));
            BOOST_REQUIRE(*(ni++) == 2 * i);
            if (bi != tree.end()) {
                BOOST_REQUIRE(*ni == 2 * i + 1);
            }
            BOOST_REQUIRE(ni == bi);
        }

        tree.clear_and_dispose(key_deleter);
    }
}

BOOST_AUTO_TEST_CASE(test_iterators) {
    test_tree t;

    for (auto i = t.rbegin(); i != t.rend(); i++) {
        BOOST_REQUIRE(false);
    }
    for (auto i = t.begin(); i != t.end(); i++) {
        BOOST_REQUIRE(false);
    }

    t.insert(std::make_unique<test_key>(7), cmp);
    t.insert(std::make_unique<test_key>(9), cmp);

    {
        auto i = t.begin();
        BOOST_REQUIRE(*(i++) == 7);
        BOOST_REQUIRE(*(i++) == 9);
        BOOST_REQUIRE(i == t.end());
    }

    {
        auto i = t.rbegin();
        BOOST_REQUIRE(*(i++) == 9);
        BOOST_REQUIRE(*(i++) == 7);
        BOOST_REQUIRE(i == t.rend());
    }

    t.clear_and_dispose(key_deleter);
}

/*
 * Special test that makes sure "self-iterator" works OK.
 * See comment near the btree::iterator(T* d) constructor
 * for details.
 */
BOOST_AUTO_TEST_CASE(test_data_self_iterator) {
    test_tree t;

    auto i = t.insert(std::make_unique<test_key>(1, 42), cmp);
    BOOST_REQUIRE(i.second);

    test_key* d = &(*i.first);
    BOOST_REQUIRE(d->cookie() == 42);

    test_tree::iterator di(d);
    BOOST_REQUIRE(di->cookie() == 42);

    t.erase_and_dispose(di, key_deleter);
    BOOST_REQUIRE(t.find(1, cmp) == t.end());
}

BOOST_DATA_TEST_CASE(test_singular_tree_ptr_sz,
                     boost::unit_test::data::make({1, 2, 10}), sz) {
    test_tree t;

    for (int i = 0; i < sz; i++) {
        t.insert(std::make_unique<test_key>(i), cmp);
    }

    for (int i = 0; i < sz; i++) {
        auto it = t.begin();
        if (i == sz - 1) {
            BOOST_REQUIRE(it.tree_if_singular() == &t);
        } else {
            BOOST_REQUIRE(it.tree_if_singular() == nullptr);
        }
        t.erase_and_dispose(it, key_deleter);
    }
}

BOOST_AUTO_TEST_CASE(test_range_erase) {
    int size = 32;

    for (int f = 0; f < size; f++) {
        for (int t = f; t <= size; t++) {
            test_tree tree;

            for (int i = 0; i < size; i++) {
                tree.insert(std::make_unique<test_key>(i), cmp);
            }

            auto iter_at = [&tree] (int at) -> typename test_tree::iterator {
                auto it = tree.begin();
                for (int i = 0; i < at; i++, it++) ;
                return it;
            };

            auto n = tree.erase_and_dispose(iter_at(f), iter_at(t), key_deleter);

            auto r = tree.begin();
            for (int i = 0; i < size; i++) {
                if (!(i >= f && i < t)) {
                    if (i == t) {
                        BOOST_REQUIRE(*n == i);
                    }
                    BOOST_REQUIRE(*(r++) == i);
                }
            }
            if (t == size) {
                BOOST_REQUIRE(n == tree.end());
            }
            BOOST_REQUIRE(r == tree.end());

            tree.clear_and_dispose(key_deleter);
        }
    }
}

BOOST_DATA_TEST_CASE(test_clone_n,
                     boost::unit_test::data::make({1, 3, 32}), n) {
    /* Quick check for tree::clone_from */
    test_tree t;

    for (int i = 0; i < n; i++) {
        t.insert(std::make_unique<test_key>(i), cmp);
    }

    test_tree ct;
    auto cloner = [] (test_key* key) -> test_key* { return new test_key(*key); };
    ct.clone_from(t, cloner, key_deleter);

    auto cit = ct.begin();
    for (auto it = t.begin(); it != t.end(); it++) {
        BOOST_REQUIRE(*it == *cit);
        cit++;
    }
    BOOST_REQUIRE(cit == ct.end());

    t.clear_and_dispose(key_deleter);
    ct.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_insert_before_hint) {
    test_tree t;

    for (int num_keys = 0; num_keys <= 16; num_keys++) {
        for (int hint_i = 0; hint_i <= num_keys; hint_i++) {
            for (int i = 0; i < num_keys; i++) {
                t.insert(std::make_unique<test_key>(2 * i + 1), cmp);
            }

            auto hint = t.begin();
            for (int i = 0; i < hint_i; i++) {
                hint++;
            }

            for (int i = 0; i < 2 * num_keys + 1; i++) {
                auto npi = t.insert_before_hint(hint, std::make_unique<test_key>(i), cmp);
                auto ni = npi.first;
                BOOST_REQUIRE(*ni == i);
                if (hint_i * 2 + 1 == i) {
                    BOOST_REQUIRE(!npi.second);
                    BOOST_REQUIRE(ni == hint);
                }
                if (npi.second) {
                    BOOST_REQUIRE(i % 2 == 0);
                    ni++;
                    if (i == 2 * num_keys) {
                        BOOST_REQUIRE(ni == t.end());
                    } else {
                        BOOST_REQUIRE(*ni == i + 1);
                    }
                } else {
                    BOOST_REQUIRE(i % 2 == 1);
                }
            }

            t.clear_and_dispose(key_deleter);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_tree_size) {
    test_tree t;

    for (int i = 0; i < 23; i++) {
        t.insert(std::make_unique<test_key>(i), cmp);
        BOOST_REQUIRE(t.calculate_size() == size_t(i + 1));
    }

    t.clear_and_dispose(key_deleter);
}

// Calls `fn` with the keys from 0 to `count - 1` in ascending, descending and shuffled order.
static void for_each_key_order(int count, std::function<void(const std::vector<int>&)> fn) {
    std::vector<int> keys(count);
    std::iota(keys.begin(), keys.end(), 0);
    fn(keys);
    std::ranges::reverse(keys);
    fn(keys);
    std::ranges::shuffle(keys, std::mt19937(42));
    fn(keys);
}

// Checks that an insert into a full leaf allocates no node when the leaf can push a key to a
// sibling instead of splitting. Ascending or descending keys fill the rightmost or the leftmost
// leaf again and again, and that leaf pushes keys to its sibling until the sibling is full too.
// Shuffled keys make the leaves inside the tree push.
SEASTAR_THREAD_TEST_CASE(test_insert_push_allocates_no_node) {
    using rows_test_tree = test_key::rows_test_tree;

    for_each_key_order(10000, [] (const std::vector<int>& keys) {
        logalloc::region mem;
        with_allocator(mem.allocator(), [&] {
            rows_test_tree t;
            auto clear_tree = defer([&] () noexcept { t.clear_and_dispose(key_deleter); });
            // The stats count no leaf while the root is inline or linear.
            unsigned long leaves = 0;
            for (int k : keys) {
                const auto before = logalloc::shard_tracker().statistics().num_allocations;
                auto ti = t.insert(std::make_unique<test_key>(k), cmp);
                BOOST_REQUIRE(ti.second);
                const auto allocations = logalloc::shard_tracker().statistics().num_allocations - before;
                if (allocations == 0) {
                    continue;
                }
                // The keys come from the standard allocator, so the region allocates only nodes.
                // An insert into a tree that has leaves needs a node only when it splits a leaf,
                // so an insert that allocates must add a leaf.
                const auto leaves_before = std::exchange(leaves, t.get_stats().leaves);
                if (leaves_before > 0) {
                    BOOST_REQUIRE_GT(leaves, leaves_before);
                }
            }
            BOOST_REQUIRE_GT(leaves, 0);
        });
    });
}

// Returns the node of each key of the tree, in the order of the keys.
static std::vector<const void*> key_nodes(const test_key::rows_test_tree& t) {
    std::vector<const void*> nodes;
    for (const auto& k : t) {
        nodes.push_back(k.hook_node());
    }
    return nodes;
}

// Checks that an insert that fails to allocate a node leaves the tree unchanged and does not
// link the key, so that a retry of the same insert succeeds. Each insert runs again and again,
// with an allocation failure at each allocation point in turn.
SEASTAR_THREAD_TEST_CASE(test_insert_exception_safety) {
    using rows_test_tree = test_key::rows_test_tree;
    using rows_test_validator = test_key::rows_test_validator;

    // `key_ref` is a key pointer that does not own the key, so that the key outlives a failed insert.
    struct key_ref {
        test_key* k;
        test_key& operator*() const noexcept { return *k; }
        test_key* release() noexcept { return k; }
    };

    for_each_key_order(2000, [] (const std::vector<int>& keys) {
        logalloc::region mem;
        with_allocator(mem.allocator(), [&] {
            rows_test_tree t;
            rows_test_validator tv;
            auto clear_tree = defer([&] () noexcept { t.clear_and_dispose(key_deleter); });
            size_t size = 0;
            [[maybe_unused]] size_t allocating_inserts = 0;
            for (int k : keys) {
                auto* key = new test_key(k);
                bool inserted = false;
                // The tree owns a linked key, so the test deletes the key only when the tree
                // did not link the key.
                auto delete_key = defer([&] () noexcept {
                    if (!key->linked()) {
                        delete key;
                    }
                });
                const auto nodes_before = key_nodes(t);
                [[maybe_unused]] size_t failures = 0;
                [[maybe_unused]] size_t allocations = 0;
                memory::with_allocation_failures([&] {
                    // with_allocation_failures() runs the function again when an allocation
                    // failed and the function did not throw, and the key is then in the tree.
                    if (key->linked()) {
                        return;
                    }
                    const auto allocations_before = logalloc::shard_tracker().statistics().num_allocations;
                    try {
                        inserted = t.insert(key_ref{key}, cmp).second;
                    } catch (const std::bad_alloc&) {
                        failures++;
                        // The injector stops after the first failure, so these checks can allocate.
                        BOOST_REQUIRE(!key->linked());
                        BOOST_REQUIRE_EQUAL(t.calculate_size(), size);
                        BOOST_REQUIRE(tv.validate(t));
                        // The tree keeps a single key in the inline root, and an insert into such
                        // a tree moves that key to a new linear root before the allocation that
                        // can fail. So the test compares the nodes of the keys only when the tree
                        // does not hold a single key.
                        if (size != 1) {
                            BOOST_REQUIRE(key_nodes(t) == nodes_before);
                        }
                        throw;
                    }
                    allocations = logalloc::shard_tracker().statistics().num_allocations - allocations_before;
                });
                BOOST_REQUIRE(inserted);
                BOOST_REQUIRE(tv.validate(t));
                size++;
#ifdef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
                // Every insert fails once at the allocation point at the start of tree::insert().
                // An insert that allocates a node must also fail at the allocation of that node.
                if (allocations > 0) {
                    BOOST_REQUIRE_GE(failures, 2);
                    allocating_inserts++;
                }
#endif
            }
            BOOST_REQUIRE_EQUAL(t.calculate_size(), keys.size());
#ifdef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
            BOOST_REQUIRE_GT(allocating_inserts, 0);
#endif
        });
    });
}

// Checks an insert that takes the key from another tree with a key_grabber, also when a full
// leaf pushes a key to a sibling. key_grabber::release() erases the key from the source tree
// through the hook of the key, so the insert must release the key before it links the key.
BOOST_AUTO_TEST_CASE(test_insert_with_key_grabber) {
    using rows_test_tree = test_key::rows_test_tree;
    using rows_test_validator = test_key::rows_test_validator;

    for_each_key_order(1000, [] (const std::vector<int>& keys) {
        rows_test_tree src;
        rows_test_tree dst;
        rows_test_validator tv;
        auto clear_trees = defer([&] () noexcept {
            src.clear_and_dispose(key_deleter);
            dst.clear_and_dispose(key_deleter);
        });
        for (int k : keys) {
            src.insert(std::make_unique<test_key>(k), cmp);
        }
        size_t moved = 0;
        for (int k : keys) {
            auto it = src.find(k, cmp);
            const auto next = std::next(it);
            auto ti = dst.insert(rows_test_tree::key_grabber(it), cmp);
            moved++;
            BOOST_REQUIRE(ti.second);
            BOOST_REQUIRE(*ti.first == k);
            BOOST_REQUIRE(it == next);
            BOOST_REQUIRE_EQUAL(dst.calculate_size(), moved);
            BOOST_REQUIRE_EQUAL(src.calculate_size(), keys.size() - moved);
            BOOST_REQUIRE(tv.validate(dst));
            BOOST_REQUIRE(tv.validate(src));
        }
    });
}

BOOST_AUTO_TEST_CASE(test_swap_between_trees_1) {
    test_tree t1, t2;

    auto i1 = t2.insert(std::make_unique<test_key>(1), cmp).first;
    auto i1n = t1.insert(test_tree::key_grabber(i1), cmp).first;

    BOOST_REQUIRE(i1n++ == t1.begin());
    BOOST_REQUIRE(i1n == t1.end());
    BOOST_REQUIRE(t2.begin() == t2.end());

    t1.clear_and_dispose(key_deleter);
}

BOOST_AUTO_TEST_CASE(test_swap_between_trees) {
    test_tree t1, t2;

    auto i2 = t1.insert(std::make_unique<test_key>(2), cmp).first;
    auto i1 = t2.insert(std::make_unique<test_key>(1), cmp).first;
    auto i3 = t2.insert(std::make_unique<test_key>(3), cmp).first;
    auto i1n = t1.insert_before(i2, test_tree::key_grabber(i1));

    BOOST_REQUIRE(i1n == t1.begin());
    BOOST_REQUIRE(*(i1n++) == 1);
    BOOST_REQUIRE(i1n++ == i2);
    BOOST_REQUIRE(i1n == t1.end());

    BOOST_REQUIRE(i3++ == t2.begin());
    BOOST_REQUIRE(i3 == t2.end());

    t1.clear_and_dispose(key_deleter);
    t2.clear_and_dispose(key_deleter);
}

BOOST_DATA_TEST_CASE(test_unlink_leftmost_n,
                     boost::unit_test::data::make({0, 1, 3, 32}), n) {
    fmt::print("CHK {}\n", n);
    test_tree t;

    for (int i = 0; i < n; i++) {
        t.insert(std::make_unique<test_key>(i), cmp);
    }

    int rover = 0;
    test_key* k;
    while ((k = t.unlink_leftmost_without_rebalance()) != nullptr) {
        BOOST_REQUIRE(int(*k) == rover++);
        delete k;
    }
}

BOOST_DATA_TEST_CASE(stress_test, boost::unit_test::data::make({std::tuple(4132, 9), std::tuple(27, 312)}), count, iter) {
    constexpr int TEST_NODE_SIZE = 8;
    constexpr int TEST_LINEAR_THRESH = 21;

    class test_key : public tree_test_key_base {
    public:
        member_hook _hook;
        test_key(int nr) noexcept : tree_test_key_base(nr) {}
        test_key(const test_key&) = delete;
        test_key(test_key&&) = delete;
    };

    using test_tree = tree<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESH, key_search::both, with_debug::yes>;
    using test_validator = validator<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESH>;
    using test_iterator_checker = iterator_checker<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESH>;

    test_key_tri_compare cmp;
    auto t = std::make_unique<test_tree>();
    std::map<int, unsigned long> oracle;
    test_validator tv;
    auto* itc = new test_iterator_checker(tv, *t);

    stress_config cfg;
    cfg.count = count;
    cfg.iters = iter;
    cfg.keys = "rand";
    cfg.verb = false;
    auto itv = 0;

    stress_collection(cfg,
        /* insert */ [&] (int key) {
            auto ir = t->insert(std::make_unique<test_key>(key), cmp);
            SCYLLA_ASSERT(ir.second);
            oracle[key] = key;

            if (itv++ % 7 == 0) {
                if (!itc->step()) {
                    delete itc;
                    itc = new test_iterator_checker(tv, *t);
                }
            }
        },
        /* erase */ [&] (int key) {
            test_key k(key);
            auto deleter = [] (test_key* k) noexcept { delete k; };

            if (itc->here(k)) {
                delete itc;
                itc = nullptr;
            }

            t->erase_and_dispose(key, cmp, deleter);
            oracle.erase(key);

            if (itc == nullptr) {
                itc = new test_iterator_checker(tv, *t);
            }

            if (itv++ % 5 == 0) {
                if (!itc->step()) {
                    delete itc;
                    itc = new test_iterator_checker(tv, *t);
                }
            }
        },
        /* validate */ [&] {
            if (cfg.verb) {
                fmt::print("Validating\n");
                tv.print_tree(*t, '|');
            }
            tv.validate(*t);
        },
        /* step */ [&] (stress_step step) {
            if (step == stress_step::before_erase) {
                auto sz = t->calculate_size();
                if (sz != (size_t)cfg.count) {
                    fmt::print("Size {} != count {}\n", sz, cfg.count);
                    throw "size";
                }

                auto ti = t->begin();
                for (auto oe : oracle) {
                    if ((unsigned long)*ti != oe.second) {
                        fmt::print("Data mismatch {} vs {}\n", oe.second, *ti);
                        throw "oracle";
                    }
                    ti++;
                }
            }
        }
    );

    delete itc;
}

static future<> test_exception_safety_of_clone(unsigned nr_keys) {
    return seastar::async([nr_keys] {
        test_tree t;

        for (unsigned i = 0; i < nr_keys; i++) {
            t.insert(std::make_unique<test_key>(i), cmp);
        }

        test_tree ct;
        unsigned nr_cloned_keys = 0;

        auto cloner = [&] (test_key* key) -> test_key* {
            auto* k = new test_key(*key);
            nr_cloned_keys++;
            return k;
        };
        auto key_deleter = [&] (test_key* key) noexcept {
            nr_cloned_keys--;
            delete key;
        };

        memory::with_allocation_failures([&] {
            ct.clone_from(t, cloner, key_deleter);
        });

        BOOST_REQUIRE(std::equal(t.begin(), t.end(), ct.begin(), ct.end()));
        // Check that no keys left cloned but not rolled-back on exception
        BOOST_REQUIRE_EQUAL(nr_cloned_keys, ct.calculate_size());

        t.clear_and_dispose(key_deleter);
        ct.clear_and_dispose(key_deleter);
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_clone_linear) {
    return test_exception_safety_of_clone(3);
}

SEASTAR_TEST_CASE(test_exception_safety_of_clone_large) {
    return test_exception_safety_of_clone(2534);
}

void stress_compaction_test(int count, int iter) {
    constexpr int TEST_NODE_SIZE = 7;
    constexpr int TEST_LINEAR_THRESHOLD = 19;

    class test_key : public tree_test_key_base {
    public:
        member_hook _hook;
        test_key(int nr) noexcept : tree_test_key_base(nr) {}
        test_key(const test_key&) = delete;
        test_key(test_key&& o) noexcept : tree_test_key_base(std::move(o)), _hook(std::move(o._hook)) {}
    };

    using test_tree = tree<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESHOLD, key_search::both, with_debug::yes>;
    using test_validator = validator<test_key, &test_key::_hook, test_key_tri_compare, TEST_NODE_SIZE, TEST_LINEAR_THRESHOLD>;

    stress_config cfg;
    cfg.count = count;
    cfg.iters = iter;
    cfg.verb = false;

    tree_pointer<test_tree> t;
    test_validator tv;

    stress_compact_collection(cfg,
        /* insert */ [&] (int key) {
            auto k = alloc_strategy_unique_ptr<test_key>(current_allocator().construct<test_key>(key));
            auto ti = t->insert(std::move(k), test_key_tri_compare{});
            SCYLLA_ASSERT(ti.second);
        },
        /* erase */ [&] (int key) {
            auto deleter = current_deleter<test_key>();
            t->erase_and_dispose(test_key(key), test_key_tri_compare{}, deleter);
        },
        /* validate */ [&] {
            if (cfg.verb) {
                fmt::print("Validating:\n");
                tv.print_tree(*t, '|');
            }
            tv.validate(*t);
        },
        /* clear */ [&] {
            t->clear_and_dispose(current_deleter<test_key>());
        }
    );
}

SEASTAR_THREAD_TEST_CASE(stress_compaction_test_large) {
    stress_compaction_test(10000, 13);
}

SEASTAR_THREAD_TEST_CASE(stress_compaction_test_small) {
    stress_compaction_test(17, 3);
}
