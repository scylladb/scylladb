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

#include <boost/test/unit_test.hpp>
#include <seastar/testing/thread_test_case.hh>
#include <fmt/core.h>

#include "utils/intrusive-array.hh"
#include "utils/logalloc.hh"

class element {
    bool _head = false;
    bool _tail = false;
    bool _train = false;

    long _data;
    int *_cookie;
    int *_cookie2;

public:
    explicit element(long val) : _data(val), _cookie(new int(0)), _cookie2(new int(0)) { }

    element(const element& other) = delete;
    element(element&& other) noexcept : _head(other._head), _tail(other._tail), _train(other._train),
            _data(other._data), _cookie(other._cookie), _cookie2(new int(0)) {
        other._cookie = nullptr;
    }

    ~element() {
        if (_cookie != nullptr) {
            delete _cookie;
        }

        delete _cookie2;
    }

    bool is_head() const noexcept { return _head; }
    void set_head(bool v) noexcept { _head = v; }
    bool is_tail() const noexcept { return _tail; }
    void set_tail(bool v) noexcept { _tail = v; }
    bool with_train() const noexcept { return _train; }
    void set_train(bool v) noexcept { _train = v; }

    bool operator==(long v) const { return v == _data; }
    long operator*() const { return _data; }

    bool bound_check(int idx, int size) {
        return ((idx == 0) == is_head()) && ((idx == size - 1) == is_tail());
    }
};

using test_array = intrusive_array<element>;

static bool size_check(test_array& a, size_t size, unsigned short tlen) {
    return a[size - 1].is_tail() && a.size() == size &&
        size_for_allocation_strategy(a) == (size + tlen) * sizeof(element) &&
        ((tlen != 0) == a[0].with_train()) &&
        ((tlen == 0) || *reinterpret_cast<unsigned short*>(&a[size]) == tlen);
}

void show(const char *pfx, test_array& a, int sz) {
    int i;

    fmt::print("{}", pfx);
    for (i = 0; i < sz; i++) {
        fmt::print("{}{}{}", a[i].is_head() ? 'H' : ' ', *a[i], a[i].is_tail() ? 'T' : ' ');
    }
    if (a[0].with_train()) {
        fmt::print(" ~{}", *reinterpret_cast<unsigned short *>(&a[i]));
    }
    fmt::print("\n");
}

SEASTAR_THREAD_TEST_CASE(test_basic_construct) {
    test_array array(12);

    for (auto i = array.begin(); i != array.end(); i++) {
       BOOST_REQUIRE(*i == 12);
    }
}

test_array* grow(test_array& from, size_t nsize, int npos, long ndat) {
    BOOST_REQUIRE(from.size() + 1 == nsize);
    auto ptr = current_allocator().alloc<test_array>(sizeof(element) * nsize);
    return new (ptr) test_array(from, test_array::grow_tag{npos}, ndat);
}

test_array* shrink(test_array& from, size_t nszie, int spos) {
    BOOST_REQUIRE(from.size() - 1 == nszie);
    auto ptr = current_allocator().alloc<test_array>(sizeof(element) * nszie);
    return new (ptr) test_array(from, test_array::shrink_tag{spos});
}

void grow_shrink_and_check(test_array& cur, int size, int depth) {
    for (int i = 0; i <= size; i++) {
        long nel = size + 12;
        test_array* narr = grow(cur, size + 1, i, nel);
        int idx = 0;

        BOOST_REQUIRE(size_check(*narr, size + 1, 0));

        for (auto ni = narr->begin(); ni != narr->end(); ni++) {
            if (idx == i) {
                BOOST_REQUIRE(*ni == nel);
            } else if (idx < i) {
                BOOST_REQUIRE(*ni == *cur[idx]);
            } else {
                BOOST_REQUIRE(*ni == *cur[idx - 1]);
            }

            BOOST_REQUIRE(ni->bound_check(idx, size + 1));
            idx++;
        }

        if (size < depth) {
            grow_shrink_and_check(*narr, size + 1, depth);
        }

        current_allocator().destroy(narr);
    }

    if (size > 1) {
        for (int i = 0; i < size; i++) {
            test_array* narr = shrink(cur, size - 1, i);
            int idx = 0;

            BOOST_REQUIRE(size_check(*narr, size - 1, 0));

            for (auto ni = narr->begin(); ni != narr->end(); ni++) {
                if (idx == i) {
                    continue;
                } else if (idx < i) {
                    BOOST_REQUIRE(*ni == *cur[idx]);
                } else {
                    BOOST_REQUIRE(*ni == *cur[idx + 1]);
                }

                BOOST_REQUIRE(ni->bound_check(idx, size - 1));
                idx++;
            }

            current_allocator().destroy(narr);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_grow_shrink_construct) {
    test_array array(12);
    grow_shrink_and_check(array, 1, 5);
}

SEASTAR_THREAD_TEST_CASE(test_erase) {
    test_array a1(10);
    test_array *a2 = grow(a1, 2, 1, 20);
    test_array *a3 = grow(*a2, 3, 2, 30);

    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 3; j++) {
            for (int k = 0; k < 2; k++) {
                std::vector<int> x({10, 20, 30, 40});
                test_array *a4 = grow(*a3, 4, 3, 40);

                auto test_fn = [&] (int idx, int sz) {
                    a4->erase(idx);
                    x.erase(x.begin() + idx);
                    BOOST_REQUIRE(size_check(*a4, sz, 4 - sz));
                    for (int a = 0; a < sz; a++) {
                        BOOST_REQUIRE(x[a] == *(*a4)[a]);
                    }
                };

                test_fn(i, 3);
                test_fn(j, 2);
                test_fn(k, 1);

                current_allocator().destroy(a4);
            }
        }
    }

    current_allocator().destroy(a3);
    current_allocator().destroy(a2);
}

SEASTAR_THREAD_TEST_CASE(test_lower_bound) {
    test_array a1(12);
    struct compare {
        int operator()(const element& a, const element& b) const { return *a - *b; }
    };

    test_array *a2 = grow(a1, 2, 1, 14);

    auto i = a2->lower_bound(element(13), compare{});
    BOOST_REQUIRE(*i == 14 && a2->index_of(i) == 1);

    test_array *a3 = grow(*a2, 3, 2, 17);

    bool match;
    BOOST_REQUIRE(*a3->lower_bound(element(11), compare{}, match) == 12 && !match);
    BOOST_REQUIRE(*a3->lower_bound(element(12), compare{}, match) == 12 && match);
    BOOST_REQUIRE(*a3->lower_bound(element(13), compare{}, match) == 14 && !match);
    BOOST_REQUIRE(*a3->lower_bound(element(14), compare{}, match) == 14 && match);
    BOOST_REQUIRE(*a3->lower_bound(element(15), compare{}, match) == 17 && !match);
    BOOST_REQUIRE(*a3->lower_bound(element(16), compare{}, match) == 17 && !match);
    BOOST_REQUIRE(*a3->lower_bound(element(17), compare{}, match) == 17 && match);
    BOOST_REQUIRE(a3->lower_bound(element(18), compare{}, match) == a3->end());

    current_allocator().destroy(a3);
    current_allocator().destroy(a2);
}

SEASTAR_THREAD_TEST_CASE(test_from_element) {
    test_array a1(12);
    test_array *a2 = grow(a1, 2, 1, 14);
    test_array *a3 = grow(*a2, 3, 2, 17);

    element* i = &((*a3)[2]);
    BOOST_REQUIRE(*i == 17);
    int idx;
    test_array& x = test_array::from_element(i, idx);
    BOOST_REQUIRE(&x == a3 && idx == 2);

    current_allocator().destroy(a3);
    current_allocator().destroy(a2);
}
