
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

#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <fmt/core.h>

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
