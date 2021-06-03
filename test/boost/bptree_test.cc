
/*
 * Copyright (C) 2020-present ScyllaDB
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

#define BOOST_TEST_MODULE bptree

#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include "utils/bptree.hh"
#include "test/unit/tree_test_key.hh"

struct int_compare {
    bool operator()(const int& a, const int& b) const noexcept { return a < b; }
};

using namespace bplus;
using test_key = tree_test_key_base;
using test_tree = tree<int, unsigned long, int_compare, 4, key_search::both, with_debug::yes>;

BOOST_AUTO_TEST_CASE(test_ops_empty_tree) {
    /* Sanity checks for no nullptr dereferences */
    test_tree t(int_compare{});
    t.erase(1);
    t.find(1);
}

BOOST_AUTO_TEST_CASE(test_double_insert) {
    /* No assertions should happen in ~tree */
    test_tree t(int_compare{});
    auto i = t.emplace(1, 1);
    BOOST_REQUIRE(i.second);
    i = t.emplace(1, 1);
    BOOST_REQUIRE(!i.second);
    t.erase(1);
}

BOOST_AUTO_TEST_CASE(test_cookie_find) {
    struct int_to_key_compare {
        bool operator()(const test_key& a, const int& b) const noexcept { return (int)a < b; }
        bool operator()(const int& a, const test_key& b) const noexcept { return a < (int)b; }
        bool operator()(const test_key& a, const test_key& b) const noexcept {
            test_key_compare cmp;
            return cmp(a, b);
        }
    };

    using test_tree = tree<test_key, int, int_to_key_compare, 4, key_search::both, with_debug::yes>;

    test_tree t(int_to_key_compare{});
    t.emplace(test_key{1}, 132);

    auto i = t.find(1);
    BOOST_REQUIRE(*i == 132);
}

BOOST_AUTO_TEST_CASE(test_double_erase) {
    test_tree t(int_compare{});
    t.emplace(1, 1);
    t.emplace(2, 2);
    auto i = t.erase(1);
    BOOST_REQUIRE(*i == 2);
    i = t.erase(1);
    BOOST_REQUIRE(i == t.end());
    i = t.erase(2);
    BOOST_REQUIRE(i == t.end());
    t.erase(2);
}

BOOST_AUTO_TEST_CASE(test_remove_corner_case) {
    /* Sanity check for erasure to be precise */
    test_tree t(int_compare{});
    t.emplace(1, 1);
    t.emplace(2, 123);
    t.emplace(3, 3);
    t.erase(1);
    t.erase(3);
    auto f = t.find(2);
    BOOST_REQUIRE(*f == 123);
    t.erase(2);
}

BOOST_AUTO_TEST_CASE(test_end_iterator) {
    /* Check std::prev(end()) */
    test_tree t(int_compare{});
    t.emplace(1, 123);
    auto i = std::prev(t.end());
    BOOST_REQUIRE(*i = 123);
    t.erase(1);
}

BOOST_AUTO_TEST_CASE(test_next_to_end_iterator) {
    /* Same, but with "artificial" end iterator */
    test_tree t(int_compare{});
    auto i = t.emplace(1, 123).first;
    i++;
    BOOST_REQUIRE(i == t.end());
    i--;
    BOOST_REQUIRE(*i = 123);
    t.erase(1);
}

BOOST_AUTO_TEST_CASE(test_clear) {
    /* Quick check for tree::clear */
    test_tree t(int_compare{});

    for (int i = 0; i < 32; i++) {
        t.emplace(i, i);
    }

    t.clear();
}

BOOST_AUTO_TEST_CASE(test_post_clear) {
    /* Check that tree is work-able after clear */
    test_tree t(int_compare{});

    t.emplace(1, 1);
    t.clear();
    t.emplace(2, 2);
    t.erase(2);
}

BOOST_AUTO_TEST_CASE(test_iterator_erase) {
    /* Check iterator::erase */
    test_tree t(int_compare{});
    auto it = t.emplace(2, 2);
    t.emplace(1, 321);
    it.first.erase(int_compare{});
    BOOST_REQUIRE(*t.find(1) == 321);
    t.erase(1);
}

BOOST_AUTO_TEST_CASE(test_iterator_equal) {
    test_tree t(int_compare{});
    auto i1 = t.emplace(1, 1);
    auto i2 = t.emplace(2, 2);
    auto i3 = t.find(1);
    BOOST_REQUIRE(i1.first == i3);
    BOOST_REQUIRE(i1.first != i2.first);
}

BOOST_AUTO_TEST_CASE(test_lower_bound) {
    test_tree t(int_compare{});
    t.emplace(1, 11);
    t.emplace(3, 13);

    bool match;
    BOOST_REQUIRE(*t.lower_bound(0, match) == 11 && !match);
    BOOST_REQUIRE(*t.lower_bound(1, match) == 11 && match);
    BOOST_REQUIRE(*t.lower_bound(2, match) == 13 && !match);
    BOOST_REQUIRE(*t.lower_bound(3, match) == 13 && match);
    BOOST_REQUIRE(t.lower_bound(4, match) == t.end() && !match);
}

BOOST_AUTO_TEST_CASE(test_upper_bound) {
    test_tree t(int_compare{});
    t.emplace(1, 11);
    t.emplace(3, 13);

    BOOST_REQUIRE(*t.upper_bound(0) == 11);
    BOOST_REQUIRE(*t.upper_bound(1) == 13);
    BOOST_REQUIRE(*t.upper_bound(2) == 13);
    BOOST_REQUIRE(t.upper_bound(3) == t.end());
    BOOST_REQUIRE(t.upper_bound(4) == t.end());
}

BOOST_AUTO_TEST_CASE(test_insert_iterator_index) {
    /* Check insertion iterator ++ and duplicate key */
    test_tree t(int_compare{});
    t.emplace(1, 10);
    t.emplace(3, 13);
    auto i = t.emplace(2, 2).first;
    i++;
    BOOST_REQUIRE(*i == 13);
    auto i2 = t.emplace(2, 2); /* 2nd insert finds the previous */
    BOOST_REQUIRE(!i2.second);
    i2.first++;
    BOOST_REQUIRE(*(i2.first) == 13);
}

BOOST_AUTO_TEST_CASE(test_insert_before) {
    /* Check iterator::insert_before */
    test_tree t(int_compare{});
    auto i3 = t.emplace(3, 13).first;
    auto i2 = i3.emplace_before(2, int_compare{}, 12);
    BOOST_REQUIRE(++i2 == i3);
    BOOST_REQUIRE(*i3 == 13);
    BOOST_REQUIRE(*--i2 == 12);
    BOOST_REQUIRE(*--i3 == 12);
}

BOOST_AUTO_TEST_CASE(test_insert_before_end) {
    /* The same but for end() iterator */
    test_tree t(int_compare{});
    auto i = t.emplace(1, 1).first;
    auto i2 = t.end().emplace_before(2, int_compare{}, 12);
    BOOST_REQUIRE(++i == i2);
    BOOST_REQUIRE(++i2 == t.end());
}

BOOST_AUTO_TEST_CASE(test_insert_before_end_empty) {
    /* The same, but for empty tree */
    test_tree t(int_compare{});
    auto i = t.end().emplace_before(42, int_compare{}, 142);
    BOOST_REQUIRE(i == t.begin());
    t.erase(42);
}

BOOST_AUTO_TEST_CASE(test_iterators) {
    test_tree t(int_compare{});

    for (auto i = t.rbegin(); i != t.rend(); i++) {
        BOOST_REQUIRE(false);
    }
    for (auto i = t.begin(); i != t.end(); i++) {
        BOOST_REQUIRE(false);
    }

    t.emplace(1, 7);
    t.emplace(2, 9);

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
}

/*
 * Special test that makes sure "self-iterator" works OK.
 * See comment near the bptree::iterator(T* d) constructor
 * for details.
 */
class tree_data {
    int _key;
    int _cookie;
public:
    explicit tree_data(int cookie) : _key(-1), _cookie(cookie) {}
    tree_data(int key, int cookie) : _key(key), _cookie(cookie) {}
    int cookie() const { return _cookie; }
    int key() const {
        assert(_key != -1);
        return _key;
    }
};

BOOST_AUTO_TEST_CASE(test_data_self_iterator) {
    using test_tree = tree<int, tree_data, int_compare, 4, key_search::both, with_debug::yes>;

    test_tree t(int_compare{});
    auto i = t.emplace(1, 42);
    BOOST_REQUIRE(i.second);

    tree_data* d = &(*i.first);
    BOOST_REQUIRE(d->cookie() == 42);

    test_tree::iterator di(d);
    BOOST_REQUIRE(di->cookie() == 42);

    di.erase(int_compare{});
    BOOST_REQUIRE(t.find(1) == t.end());
}

BOOST_AUTO_TEST_CASE(test_insert_before_nokey) {
    using test_tree = tree<int, tree_data, int_compare, 4, key_search::both, with_debug::yes>;

    test_tree t(int_compare{});
    auto i = t.emplace(2, 52).first;
    auto ni = i.emplace_before(int_compare{}, 1, 42);
    BOOST_REQUIRE(ni->cookie() == 42);
    ni++;
    BOOST_REQUIRE(ni == i);
}


BOOST_AUTO_TEST_CASE(test_self_iterator_rover) {
    test_tree t(int_compare{});
    auto i = t.emplace(2, 42).first;
    unsigned long* d = &(*i);
    test_tree::iterator di(d);

    i = di.emplace_before(1, int_compare{}, 31);
    BOOST_REQUIRE(*i == 31);
    BOOST_REQUIRE(*(++i) == 42);
    BOOST_REQUIRE(++i == t.end());
    BOOST_REQUIRE(++di == t.end());
}

BOOST_AUTO_TEST_CASE(test_erase_range) {
    /* Quick check for tree::erase(from, to) */
    test_tree t(int_compare{});

    for (int i = 0; i < 32; i++) {
        t.emplace(i, i);
    }

    auto b = t.find(8);
    auto e = t.find(25);
    t.erase(b, e);

    BOOST_REQUIRE(*t.find(7) == 7);
    BOOST_REQUIRE(t.find(8) == t.end());
    BOOST_REQUIRE(t.find(24) == t.end());
    BOOST_REQUIRE(*t.find(25) == 25);
}

static bool key_simplified = false;

static void check_conversions() {
    BOOST_REQUIRE(key_simplified);
}

BOOST_AUTO_TEST_CASE(test_avx_search) {
    struct int64_compare {
        bool operator()(const int64_t& a, const int64_t& b) const noexcept { return a < b; }
        int64_t simplify_key(int64_t k) const noexcept {
            key_simplified = true;
            return k;
        }
    };

    using test_tree = tree<int64_t, unsigned long, int64_compare, 4, key_search::linear>;

    test_tree t(int64_compare{});

    t.emplace(std::numeric_limits<int64_t>::min() + 1, 321);
    t.emplace(0, 213);
    t.emplace(std::numeric_limits<int64_t>::max(), 123);

    auto i = t.find(std::numeric_limits<int64_t>::min() + 1);
    BOOST_REQUIRE(*i++ == 321);
    BOOST_REQUIRE(*i++ == 213);
    BOOST_REQUIRE(*i++ == 123);
    BOOST_REQUIRE(i == t.end());

    i = t.find(0);
    BOOST_REQUIRE(*i == 213);
    i = t.find(std::numeric_limits<int64_t>::max());
    BOOST_REQUIRE(*i == 123);

    t.clear();
    check_conversions();
}
