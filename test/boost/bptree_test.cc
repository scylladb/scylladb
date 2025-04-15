
/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include <seastar/testing/thread_test_case.hh>
#include "utils/assert.hh"
#include "utils/bptree.hh"
#include "collection_stress.hh"
#include "bptree_validation.hh"
#include "tree_test_key.hh"

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
        SCYLLA_ASSERT(_key != -1);
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

using test_key = tree_test_key_base;

class test_data {
    int _value;
public:
    test_data() : _value(0) {}
    test_data(test_key& k) : _value((int)k + 10) {}

    operator unsigned long() const { return _value; }
    bool match_key(const test_key& k) const { return _value == (int)k + 10; }
};

template <> struct fmt::formatter<test_data> : fmt::formatter<string_view> {
    auto format(test_data d, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", static_cast<unsigned long>(d));
    }
};

BOOST_AUTO_TEST_CASE(stress_test) {
    constexpr int TEST_NODE_SIZE = 16;
    using test_tree = tree<test_key, test_data, test_key_compare, TEST_NODE_SIZE, key_search::both, with_debug::yes>;
    using test_validator = validator<test_key, test_data, test_key_compare, TEST_NODE_SIZE>;
    using test_iterator_checker = iterator_checker<test_key, test_data, test_key_compare, TEST_NODE_SIZE>;

    auto t = std::make_unique<test_tree>(test_key_compare{});
    std::map<int, unsigned long> oracle;
    test_validator tv;
    auto* itc = new test_iterator_checker(tv, *t);

    stress_config cfg;
    cfg.count = 4132;
    cfg.iters = 9;
    cfg.keys = "rand";
    cfg.verb = false;
    auto rep = 0, itv = 0;

    stress_collection(cfg,
        /* insert */ [&] (int key) {
            test_key k(key);

            if (rep % 2 != 1) {
                auto ir = t->emplace(copy_key(k), k);
                SCYLLA_ASSERT(ir.second);
            } else {
                auto ir = t->lower_bound(k);
                ir.emplace_before(copy_key(k), test_key_compare{}, k);
            }
            oracle[key] = key + 10;

            if (itv++ % 7 == 0) {
                if (!itc->step()) {
                    delete itc;
                    itc = new test_iterator_checker(tv, *t);
                }
            }
        },
        /* erase */ [&] (int key) {
            test_key k(key);

            if (itc->here(k)) {
                delete itc;
                itc = nullptr;
            }

            if (rep % 3 != 2) {
                t->erase(k);
            } else {
                auto ri = t->find(k);
                auto ni = ri;
                ni++;
                auto eni = ri.erase(test_key_compare{});
                SCYLLA_ASSERT(ni == eni);
            }
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
            if (step == stress_step::iteration_finished) {
                rep++;
            }

            if (step == stress_step::before_erase) {
                auto sz = t->size_slow();
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

SEASTAR_THREAD_TEST_CASE(stress_compaction_test) {
    constexpr int TEST_NODE_SIZE = 7;

    using test_key = tree_test_key_base;

    class test_data {
        int _value;
    public:
        test_data() : _value(0) {}
        test_data(test_key& k) : _value((int)k + 10) {}

        operator unsigned long() const { return _value; }
        bool match_key(const test_key& k) const { return _value == (int)k + 10; }
    };
    using test_tree = tree<test_key, test_data, test_key_compare, TEST_NODE_SIZE, key_search::both, with_debug::yes>;
    using test_validator = validator<test_key, test_data, test_key_compare, TEST_NODE_SIZE>;

    stress_config cfg;
    cfg.count = 10000;
    cfg.iters = 13;
    cfg.verb = false;

    tree_pointer<test_tree> t(test_key_compare{});
    test_validator tv;

    stress_compact_collection(cfg,
        /* insert */ [&] (int key) {
            test_key k(key);
            auto ti = t->emplace(copy_key(k), k);
            SCYLLA_ASSERT(ti.second);
        },
        /* erase */ [&] (int key) {
            t->erase(test_key(key));
        },
        /* validate */ [&] {
            if (cfg.verb) {
                fmt::print("Validating:\n");
                tv.print_tree(*t, '|');
            }
            tv.validate(*t);
        },
        /* clear */ [&] {
            t->clear();
        }
    );
}
