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
