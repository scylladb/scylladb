/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/included/unit_test.hpp>
#include "core/shared_ptr.hh"

struct expected_exception : public std::exception {};

struct A {
    static bool destroyed;
    A() {
        destroyed = false;
    }
    virtual ~A() {
        destroyed = true;
    }
};

struct B {
    virtual void x() {}
};

bool A::destroyed = false;

BOOST_AUTO_TEST_CASE(explot_dynamic_cast_use_after_free_problem) {
    shared_ptr<A> p = ::make_shared<A>();
    {
        auto p2 = dynamic_pointer_cast<B>(p);
        BOOST_ASSERT(!p2);
    }
    BOOST_ASSERT(!A::destroyed);
}

class C : public enable_shared_from_this<C> {
public:
    shared_ptr<C> dup() { return shared_from_this(); }
    shared_ptr<const C> get() const { return shared_from_this(); }
};

BOOST_AUTO_TEST_CASE(test_const_ptr) {
    shared_ptr<C> a = make_shared<C>();
    shared_ptr<const C> ca = a;
    BOOST_REQUIRE(ca == a);
    shared_ptr<const C> cca = ca->get();
    BOOST_REQUIRE(cca == ca);
}

struct D {};

BOOST_AUTO_TEST_CASE(test_lw_const_ptr_1) {
    auto pd1 = make_lw_shared<const D>(D());
    auto pd2 = make_lw_shared(D());
    lw_shared_ptr<const D> pd3 = pd2;
    BOOST_REQUIRE(pd2 == pd3);
}

struct E : enable_lw_shared_from_this<E> {};

BOOST_AUTO_TEST_CASE(test_lw_const_ptr_2) {
    auto pe1 = make_lw_shared<const E>();
    auto pe2 = make_lw_shared<E>();
    lw_shared_ptr<const E> pe3 = pe2;
    BOOST_REQUIRE(pe2 == pe3);
}

struct F : enable_lw_shared_from_this<F> {
    auto const_method() const {
        return shared_from_this();
    }
};

BOOST_AUTO_TEST_CASE(test_shared_from_this_called_on_const_object) {
    auto ptr = make_lw_shared<F>();
    ptr->const_method();
}

BOOST_AUTO_TEST_CASE(test_exception_thrown_from_constructor_is_propagated) {
    struct X {
        X() {
            throw expected_exception();
        }
    };
    try {
        auto ptr = make_lw_shared<X>();
        BOOST_FAIL("Constructor should have thrown");
    } catch (const expected_exception& e) {
        BOOST_MESSAGE("Expected exception caught");
    }
    try {
        auto ptr = ::make_shared<X>();
        BOOST_FAIL("Constructor should have thrown");
    } catch (const expected_exception& e) {
        BOOST_MESSAGE("Expected exception caught");
    }
}
