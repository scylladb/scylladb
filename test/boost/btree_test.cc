/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_MODULE btree

#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include "test/unit/tree_test_key.hh"
#include "utils/intrusive_btree.hh"

using namespace intrusive_b;

class test_key : public tree_test_key_base {
    member_hook b_hook;

public:
    struct tri_compare {
        test_key_tri_compare _cmp;
        template <typename A, typename B>
        std::strong_ordering operator()(const A& a, const B& b) const noexcept { return _cmp(a, b) <=> 0; }
    };
    using test_tree = tree<test_key, &test_key::b_hook, tri_compare, 4, 5, key_search::both, with_debug::yes>;
    test_key(int nr, int cookie) noexcept : tree_test_key_base(nr, cookie) {}
    test_key(int nr) noexcept : tree_test_key_base(nr) {}
    test_key(const test_key& o) noexcept : tree_test_key_base(o, tree_test_key_base::force_copy_tag{}) {}
    test_key(test_key&&) = delete;
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

    test_key k(1);
    t.insert(k, cmp);
    t.clear();
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
    auto in = it.first.erase_and_dispose(key_deleter);
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

static void test_lower_bound_sz(int nkeys) {
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

        it.erase_and_dispose(key_deleter);

        bool match;
        it = t.lower_bound(0, match, cmp);
        if (it == t.end()) {
            BOOST_REQUIRE(i == 2 * nkeys + 1);
        } else {
            BOOST_REQUIRE(!match && *it == i);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_lower_bound_all) {
    test_lower_bound_sz(1);
    test_lower_bound_sz(3);
    test_lower_bound_sz(16);
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

    di.erase_and_dispose(key_deleter);
    BOOST_REQUIRE(t.find(1, cmp) == t.end());
}

static void test_singular_tree_ptr_sz(int sz) {
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
        it.erase_and_dispose(key_deleter);
    }
}

BOOST_AUTO_TEST_CASE(test_singular_tree_ptr) {
    test_singular_tree_ptr_sz(1);
    test_singular_tree_ptr_sz(2);
    test_singular_tree_ptr_sz(10);
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

static void test_clone_n(int n) {
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

BOOST_AUTO_TEST_CASE(test_clone) {
    test_clone_n(1);
    test_clone_n(3);
    test_clone_n(32);
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

static void test_unlink_leftmost_n(int n) {
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

BOOST_AUTO_TEST_CASE(test_unlink_leftmost_without_rebalance) {
    test_unlink_leftmost_n(0);
    test_unlink_leftmost_n(1);
    test_unlink_leftmost_n(3);
    test_unlink_leftmost_n(32);
}
