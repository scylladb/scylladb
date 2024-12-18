
/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <fmt/core.h>

#include "radix_tree_printer.hh"
#include "collection_stress.hh"
#include "utils/compact-radix-tree.hh"

using namespace compact_radix_tree;
using namespace seastar;

class test_data {
    unsigned long _val;
    unsigned long *_pval;
public:
    test_data(unsigned long val) : _val(val), _pval(new unsigned long(val)) {}
    test_data(const test_data&) = delete;
    test_data(test_data&& o) noexcept : _val(o._val), _pval(std::exchange(o._pval, nullptr)) {}
    ~test_data() {
        if (_pval != nullptr) {
            delete _pval;
        }
    }

    unsigned long value() const {
        return _pval != nullptr ? *_pval : _val + 1000000;
    }
};

std::ostream& operator<<(std::ostream& out, const test_data& d) {
    out << d.value();
    return out;
}

using test_tree = tree<test_data>;

SEASTAR_TEST_CASE(test_exception_safety_of_emplace) {
    return seastar::async([] {
        test_tree tree;

        int next = 0;
        memory::with_allocation_failures([&] {
            while (next < 1024) {
                BOOST_REQUIRE(tree.get(next) == nullptr);
                tree.emplace(next, next);
                next++;
            }
        });

        int count = 0;
        auto it = tree.begin();
        while (it != tree.end()) {
            BOOST_REQUIRE(it.key() == it->value());
            it++;
            count++;
        }

        BOOST_REQUIRE(count == 1024);
    });
}

BOOST_AUTO_TEST_CASE(test_weed_from_tree) {
    test_tree tree;

    for (int i = 0; i < 1000; i++) {
        tree.emplace(i, i);
    }

    auto filter = [] (unsigned idx) noexcept {
        return idx % 2 == 0 || idx % 3 == 0;
    };

    tree.weed([&filter] (unsigned idx, test_data& d) noexcept {
        BOOST_REQUIRE(idx == d.value());
        return filter(idx);
    });

    for (int i = 0; i < 1000; i++) {
        test_data* d = tree.get(i);
        if (filter(i)) {
            BOOST_REQUIRE(d == nullptr);
        } else {
            BOOST_REQUIRE(d != nullptr);
            BOOST_REQUIRE(d->value() == (unsigned long)i);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_lower_bound) {
    test_tree tree;

    for (int i = 0; i < 1000; i++) {
        tree.emplace(i * 2, i * 2 + 1);
    }

    for (int i = 0; ; i++) {
        test_data* d = tree.lower_bound(i);
        if (d == nullptr) {
            BOOST_REQUIRE(i == 1999);
            break;
        }

        if (i % 2 == 0) {
            BOOST_REQUIRE(d->value() == (unsigned long)(i + 1));
        } else {
            BOOST_REQUIRE(d->value() == (unsigned long)(i + 2));
        }
    }
}

BOOST_AUTO_TEST_CASE(test_clear) {
    test_tree tree;

    for (int i = 0; i < 1000; i++) {
        tree.emplace(i * 3, i * 3);
    }

    tree.clear();
    BOOST_REQUIRE(tree.lower_bound(0) == nullptr);
}

static void do_test_clone(size_t sz) {
    test_tree t;

    for (unsigned i = 0; i < sz; i++) {
        t.emplace(i, i);
    }

    test_tree ct;

    ct.clone_from(t, [] (unsigned idx, const test_data& td) {
        BOOST_REQUIRE(idx == td.value());
        return test_data(td.value());
    });

    BOOST_REQUIRE(std::equal(t.begin(), t.end(), ct.begin(), ct.end(),
        [] (const test_data& a, const test_data& b) {
            return a.value() == b.value();
        }));
}

BOOST_AUTO_TEST_CASE(test_clone) {
    do_test_clone(0);
    do_test_clone(2);
    do_test_clone(99);
    do_test_clone(1111);
    do_test_clone(333333);
}

class stress_test_data {
    unsigned long *_data;
    unsigned long _val;
public:
    stress_test_data(unsigned long val) : _data(new unsigned long(val)), _val(val) {}
    stress_test_data(const stress_test_data&) = delete;
    stress_test_data(stress_test_data&& o) noexcept : _data(std::exchange(o._data, nullptr)), _val(o._val) {}
    ~stress_test_data() {
        if (_data != nullptr) {
            delete _data;
        }
    }

    unsigned long value() const {
        return _data == nullptr ? _val + 0x80000000 : *_data;
    }
};

template <> struct fmt::formatter<stress_test_data> : fmt::formatter<string_view> {
    auto format(const stress_test_data& d, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", d.value());
    }
};

BOOST_AUTO_TEST_CASE(stress_test) {
    using test_tree = tree<stress_test_data>;

    auto t = std::make_unique<test_tree>();
    std::map<unsigned, stress_test_data> oracle;

    unsigned col_size = 0;
    enum class validate {
        oracle, iterator, walk, lower_bound,
    };
    validate vld = validate::oracle;

    stress_config cfg;
    cfg.count = 35642;
    cfg.iters = 1;
    cfg.keys = "rand";
    cfg.verb = false;

    for (int i = 0; i < 5; i++) {
        stress_collection(cfg,
            /* insert */ [&] (int key) {
                t->emplace(key, key);
                oracle.emplace(std::make_pair(key, key));
                col_size++;
            },
            /* erase */ [&] (int key) {
                t->erase(key);
                oracle.erase(key);
                col_size--;
            },
            /* validate */ [&] {
                if (cfg.verb) {
                    compact_radix_tree::printer<stress_test_data, unsigned>::show(*t);
                }
                if (vld == validate::oracle) {
                    for (auto&& d : oracle) {
                        stress_test_data* td = t->get(d.first);
                        SCYLLA_ASSERT(td != nullptr);
                        SCYLLA_ASSERT(td->value() == d.second.value());
                    }
                    vld = validate::iterator;
                } else if (vld == validate::iterator) {
                    unsigned nr = 0;
                    auto ti = t->begin();
                    while (ti != t->end()) {
                        SCYLLA_ASSERT(ti->value() == ti.key());
                        nr++;
                        ti++;
                        SCYLLA_ASSERT(nr <= col_size);
                    }
                    SCYLLA_ASSERT(nr == col_size);
                    vld = validate::walk;
                } else if (vld == validate::walk) {
                    unsigned nr = 0;
                    t->walk([&nr, col_size] (unsigned idx, stress_test_data& td) {
                        SCYLLA_ASSERT(idx == td.value());
                        nr++;
                        SCYLLA_ASSERT(nr <= col_size);
                        return true;
                    });
                    SCYLLA_ASSERT(nr == col_size);
                    vld = validate::lower_bound;
                } else if (vld == validate::lower_bound) {
                    unsigned nr = 0;
                    unsigned idx = 0;
                    while (true) {
                        stress_test_data* td = t->lower_bound(idx);
                        if (td == nullptr) {
                            break;
                        }
                        SCYLLA_ASSERT(td->value() >= idx);
                        nr++;
                        idx = td->value() + 1;
                        SCYLLA_ASSERT(nr <= col_size);
                    }
                    SCYLLA_ASSERT(nr == col_size);
                    vld = validate::oracle;
                }
            },
            /* step */ [] (stress_step step) { }
        );

        cfg.count /= 3;
    }

    t->clear();
    oracle.clear();
}

SEASTAR_TEST_CASE(test_exception_safety_of_clone) {
    return seastar::async([] {
        test_tree t;

        for (unsigned i = 0; i < 2345; i++) {
            t.emplace(i, i);
        }

        test_tree ct;

        memory::with_allocation_failures([&] {
            ct.clone_from(t, [] (unsigned idx, const test_data& td) {
                return test_data(td.value());
            });
        });

        BOOST_REQUIRE(std::equal(t.begin(), t.end(), ct.begin(), ct.end(),
                [] (const test_data& a, const test_data& b) {
                    return a.value() == b.value();
                }));
    });
}

class compaction_test_data {
    unsigned long *_data;
    unsigned long _val;
public:
    compaction_test_data(unsigned long val) : _data(new unsigned long(val)), _val(val) {}
    compaction_test_data(const compaction_test_data&) = delete;
    compaction_test_data(compaction_test_data&& o) noexcept : _data(std::exchange(o._data, nullptr)), _val(o._val) {}
    ~compaction_test_data() {
        if (_data != nullptr) {
            delete _data;
        }
    }

    unsigned long value() const {
        return _data == nullptr ? _val + 0x80000000 : *_data;
    }
};

template <> struct fmt::formatter<compaction_test_data> : fmt::formatter<string_view> {
    auto format(const compaction_test_data& d, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", d.value());
    }
};

SEASTAR_THREAD_TEST_CASE(stress_compaction_test) {
    using test_tree = tree<compaction_test_data>;

    tree_pointer<test_tree> t;

    stress_config cfg;
    cfg.count = 132564;
    cfg.iters = 1;
    cfg.keys = "rand";
    cfg.verb = false;

    unsigned col_size = 0;

    for (int i = 0; i < 32; i++) {
        stress_compact_collection(cfg,
            /* insert */ [&] (int key) {
                t->emplace(key, key);
                col_size++;
            },
            /* erase */ [&] (int key) {
                t->erase(key);
                col_size--;
            },
            /* validate */ [&] {
                if (cfg.verb) {
                    compact_radix_tree::printer<compaction_test_data, unsigned>::show(*t);
                }

                unsigned nr = 0;
                auto ti = t->begin();
                while (ti != t->end()) {
                    SCYLLA_ASSERT(ti->value() == ti.key());
                    nr++;
                    ti++;
                }
                SCYLLA_ASSERT(nr == col_size);
            },
            /* clear */ [&] {
                t->clear();
                col_size = 0;
            }
        );

        cfg.count /= 2;
    }
}
