
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

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <seastar/core/print.hh>
#include <fmt/core.h>
#include <string>

#include "utils/double-decker.hh"
#include "test/lib/random_utils.hh"

class compound_key {
public:
    int key;
    std::string sub_key;

    compound_key(int k, std::string sk) noexcept : key(k), sub_key(sk) {}

    compound_key(const compound_key& other) = delete;
    compound_key(compound_key&& other) noexcept : key(other.key), sub_key(std::move(other.sub_key)) {}

    compound_key& operator=(const compound_key& other) = delete;
    compound_key& operator=(compound_key&& other) noexcept {
        key = other.key;
        sub_key = std::move(other.sub_key);
        return *this;
    }

    std::string format() const {
        return seastar::format("{}.{}", key, sub_key);
    }

    bool operator==(const compound_key& other) const {
        return key == other.key && sub_key == other.sub_key;
    }

    bool operator!=(const compound_key& other) const { return !(*this == other); }

    struct compare {
        int operator()(const int& a, const int& b) const { return a - b; }
        int operator()(const int& a, const compound_key& b) const { return a - b.key; }
        int operator()(const compound_key& a, const int& b) const { return a.key - b; }

        int operator()(const compound_key& a, const compound_key& b) const {
            if (a.key != b.key) {
                return this->operator()(a.key, b.key);
            } else {
                return a.sub_key.compare(b.sub_key);
            }
        }
    };

    struct less_compare {
        compare cmp;

        template <typename A, typename B>
        bool operator()(const A& a, const B& b) const noexcept {
            return cmp(a, b) < 0;
        }
    };
};

class test_data {
    compound_key _key;
    bool _head = false;
    bool _tail = false;
    bool _train = false;

    int *_cookie;
    int *_cookie2;
public:
    bool is_head() const noexcept { return _head; }
    bool is_tail() const noexcept { return _tail; }
    bool with_train() const noexcept { return _train; }
    void set_head(bool v) noexcept { _head = v; }
    void set_tail(bool v) noexcept { _tail = v; }
    void set_train(bool v) noexcept { _train = v; }

    test_data(int key, std::string sub) : _key(key, sub), _cookie(new int(0)), _cookie2(new int(0)) {}

    test_data(const test_data& other) = delete;
    test_data(test_data&& other) noexcept : _key(std::move(other._key)),
            _head(other._head), _tail(other._tail), _train(other._train),
            _cookie(other._cookie), _cookie2(new int(0)) {
        other._cookie = nullptr;
    }

    ~test_data() {
        if (_cookie != nullptr) {
            delete _cookie;
        }
        delete _cookie2;
    }

    bool operator==(const compound_key& k) { return _key == k; }

    test_data& operator=(const test_data& other) = delete;
    test_data& operator=(test_data&& other) = delete;

    std::string format() const { return _key.format(); }

    struct compare {
        compound_key::compare kcmp;
        int operator()(const int& a, const int& b) { return kcmp(a, b); }
        int operator()(const compound_key& a, const int& b) { return kcmp(a.key, b); }
        int operator()(const int& a, const compound_key& b) { return kcmp(a, b.key); }
        int operator()(const compound_key& a, const compound_key& b) { return kcmp(a, b); }
        int operator()(const compound_key& a, const test_data& b) { return kcmp(a, b._key); }
        int operator()(const test_data& a, const compound_key& b) { return kcmp(a._key, b); }
        int operator()(const test_data& a, const test_data& b) { return kcmp(a._key, b._key); }
    };
};

using collection = double_decker<int, test_data, compound_key::less_compare, test_data::compare, 4,
                    bplus::key_search::both, bplus::with_debug::yes>;
using oracle = std::set<compound_key, compound_key::less_compare>;

SEASTAR_THREAD_TEST_CASE(test_lower_bound) {
    collection c(compound_key::less_compare{});
    test_data::compare cmp;

    c.insert(3, test_data(3, "e"), cmp);
    c.insert(5, test_data(5, "i"), cmp);
    c.insert(5, test_data(5, "o"), cmp);

    collection::bound_hint h;

    BOOST_REQUIRE(*c.lower_bound(compound_key(2, "a"), cmp, h) == compound_key(3, "e") && !h.key_match);
    BOOST_REQUIRE(*c.lower_bound(compound_key(3, "a"), cmp, h) == compound_key(3, "e") && h.key_match && !h.key_tail && !h.match);
    BOOST_REQUIRE(*c.lower_bound(compound_key(3, "e"), cmp, h) == compound_key(3, "e") && h.key_match && !h.key_tail && h.match);
    BOOST_REQUIRE(*c.lower_bound(compound_key(3, "o"), cmp, h) == compound_key(5, "i") && h.key_match && h.key_tail && !h.match);
    BOOST_REQUIRE(*c.lower_bound(compound_key(4, "i"), cmp, h) == compound_key(5, "i") && !h.key_match);
    BOOST_REQUIRE(*c.lower_bound(compound_key(5, "a"), cmp, h) == compound_key(5, "i") && h.key_match && !h.key_tail && !h.match);
    BOOST_REQUIRE(*c.lower_bound(compound_key(5, "i"), cmp, h) == compound_key(5, "i") && h.key_match && !h.key_tail && h.match);
    BOOST_REQUIRE(*c.lower_bound(compound_key(5, "l"), cmp, h) == compound_key(5, "o") && h.key_match && !h.key_tail && !h.match);
    BOOST_REQUIRE(*c.lower_bound(compound_key(5, "o"), cmp, h) == compound_key(5, "o") && h.key_match && !h.key_tail && h.match);
    BOOST_REQUIRE(c.lower_bound(compound_key(5, "q"), cmp, h) == c.end() && h.key_match && h.key_tail);
    BOOST_REQUIRE(c.lower_bound(compound_key(6, "q"), cmp, h) == c.end() && !h.key_match);

    c.clear();
}

SEASTAR_THREAD_TEST_CASE(test_upper_bound) {
    collection c(compound_key::less_compare{});
    test_data::compare cmp;

    c.insert(3, test_data(3, "e"), cmp);
    c.insert(5, test_data(5, "i"), cmp);
    c.insert(5, test_data(5, "o"), cmp);

    BOOST_REQUIRE(*c.upper_bound(compound_key(2, "a"), cmp) == compound_key(3, "e"));
    BOOST_REQUIRE(*c.upper_bound(compound_key(3, "a"), cmp) == compound_key(3, "e"));
    BOOST_REQUIRE(*c.upper_bound(compound_key(3, "e"), cmp) == compound_key(5, "i"));
    BOOST_REQUIRE(*c.upper_bound(compound_key(3, "o"), cmp) == compound_key(5, "i"));
    BOOST_REQUIRE(*c.upper_bound(compound_key(4, "i"), cmp) == compound_key(5, "i"));
    BOOST_REQUIRE(*c.upper_bound(compound_key(5, "a"), cmp) == compound_key(5, "i"));
    BOOST_REQUIRE(*c.upper_bound(compound_key(5, "i"), cmp) == compound_key(5, "o"));
    BOOST_REQUIRE(*c.upper_bound(compound_key(5, "l"), cmp) == compound_key(5, "o"));
    BOOST_REQUIRE(c.upper_bound(compound_key(5, "o"), cmp) == c.end());
    BOOST_REQUIRE(c.upper_bound(compound_key(5, "q"), cmp) == c.end());
    BOOST_REQUIRE(c.upper_bound(compound_key(6, "q"), cmp) == c.end());

    c.clear();
}
SEASTAR_THREAD_TEST_CASE(test_self_iterator) {
    collection c(compound_key::less_compare{});
    test_data::compare cmp;

    c.insert(1, std::move(test_data(1, "a")), cmp);
    c.insert(1, std::move(test_data(1, "b")), cmp);
    c.insert(2, std::move(test_data(2, "c")), cmp);
    c.insert(3, std::move(test_data(3, "d")), cmp);
    c.insert(3, std::move(test_data(3, "e")), cmp);

    auto erase_by_ptr = [&] (int key, std::string sub) {
        test_data* d = &*c.find(compound_key(key, sub), cmp);
        collection::iterator di(d);
        di.erase(compound_key::less_compare{});
    };

    erase_by_ptr(1, "b");
    erase_by_ptr(2, "c");
    erase_by_ptr(3, "d");

    auto i = c.begin();
    BOOST_REQUIRE(*i++ == compound_key(1, "a"));
    BOOST_REQUIRE(*i++ == compound_key(3, "e"));
    BOOST_REQUIRE(i == c.end());

    c.clear();
}

SEASTAR_THREAD_TEST_CASE(test_end_iterator) {
    collection c(compound_key::less_compare{});
    test_data::compare cmp;

    c.insert(1, std::move(test_data(1, "a")), cmp);
    auto i = std::prev(c.end());
    BOOST_REQUIRE(*i == compound_key(1, "a"));

    c.clear();
}

void validate_sorted(collection& c) {
    auto i = c.begin();
    if (i == c.end()) {
        return;
    }

    while (1) {
        auto cur = i;
        i++;
        if (i == c.end()) {
            break;
        }
        test_data::compare cmp;
        BOOST_REQUIRE(cmp(*cur, *i) < 0);
    }
}

void compare_with_set(collection& c, oracle& s) {
    test_data::compare cmp;
    /* All keys must be findable */
    for (auto i = s.begin(); i != s.end(); i++) {
        auto j = c.find(*i, cmp);
        BOOST_REQUIRE(j != c.end() && *j == *i);
    }

    /* Both iterators must coinside */
    auto i = c.begin();
    auto j = s.begin();

    while (i != c.end()) {
        BOOST_REQUIRE(*i == *j);
        i++;
        j++;
    }
}

SEASTAR_THREAD_TEST_CASE(test_insert_via_emplace) {
    collection c(compound_key::less_compare{});
    test_data::compare cmp;
    oracle s;
    int nr = 0;

    while (nr < 4000) {
        compound_key k(tests::random::get_int<int>(900), tests::random::get_sstring(4));

        collection::bound_hint h;
        auto i = c.lower_bound(k, cmp, h);

        if (i == c.end() || !h.match) {
            auto it = c.emplace_before(i, k.key, h, k.key, k.sub_key);
            BOOST_REQUIRE(*it == k);
            s.insert(std::move(k));
            nr++;
        }
    }

    compare_with_set(c, s);
    c.clear();
}

SEASTAR_THREAD_TEST_CASE(test_insert_and_erase) {
    collection c(compound_key::less_compare{});
    test_data::compare cmp;
    int nr = 0;

    while (nr < 500) {
        compound_key k(tests::random::get_int<int>(100), tests::random::get_sstring(3));

        if (c.find(k, cmp) == c.end()) {
            auto it = c.insert(k.key, std::move(test_data(k.key, k.sub_key)), cmp);
            BOOST_REQUIRE(*it == k);
            nr++;
        }
    }

    validate_sorted(c);

    while (nr > 0) {
        int n = tests::random::get_int<int>() % nr;

        auto i = c.begin();
        while (n > 0) {
            i++;
            n--;
        }

        i.erase(compound_key::less_compare{});
        nr--;

        validate_sorted(c);
    }
}

SEASTAR_THREAD_TEST_CASE(test_compaction) {
    logalloc::region reg;
    with_allocator(reg.allocator(), [&] {
        collection c(compound_key::less_compare{});
        test_data::compare cmp;
        oracle s;

        {
            logalloc::reclaim_lock rl(reg);

            int nr = 0;

            while (nr < 1500) {
                compound_key k(tests::random::get_int<int>(400), tests::random::get_sstring(3));

                if (c.find(k, cmp) == c.end()) {
                    auto it = c.insert(k.key, std::move(test_data(k.key, k.sub_key)), cmp);
                    BOOST_REQUIRE(*it == k);
                    s.insert(std::move(k));
                    nr++;
                }
            }
        }

        reg.full_compaction();

        compare_with_set(c, s);
        c.clear();
    });
}

SEASTAR_THREAD_TEST_CASE(test_range_erase) {
    std::vector<compound_key> keys;
    test_data::compare cmp;

    keys.emplace_back(1, "a");
    keys.emplace_back(1, "b");
    keys.emplace_back(1, "c");
    keys.emplace_back(1, "d");
    keys.emplace_back(2, "a");
    keys.emplace_back(2, "b");
    keys.emplace_back(2, "c");
    keys.emplace_back(2, "d");
    keys.emplace_back(2, "e");
    keys.emplace_back(3, "a");
    keys.emplace_back(3, "b");
    keys.emplace_back(3, "c");

    for (size_t f = 0; f < keys.size(); f++) {
        for (size_t t = f; t <= keys.size(); t++) {
            collection c(compound_key::less_compare{});

            for (auto&& k : keys) {
                c.insert(k.key, std::move(test_data(k.key, k.sub_key)), cmp);
            }

            auto iter_at = [&c] (size_t at) -> collection::iterator {
                auto it = c.begin();
                for (size_t i = 0; i < at; i++, it++) ;
                return it;
            };

            auto n = c.erase(iter_at(f), iter_at(t));

            auto r = c.begin();
            for (size_t i = 0; i < keys.size(); i++) {
                if (!(i >= f && i < t)) {
                    if (i == t) {
                        BOOST_REQUIRE(*n == keys[i]);
                    }
                    BOOST_REQUIRE(*(r++) == keys[i]);
                }
            }
            if (t == keys.size()) {
                BOOST_REQUIRE(n == c.end());
            }
            BOOST_REQUIRE(r == c.end());
        }
    }
}
