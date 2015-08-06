/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "utils/managed_ref.hh"

BOOST_AUTO_TEST_CASE(test_standrard_allocator_respects_alignment) {
    constexpr auto alignment = 16;
    struct alignas(alignment) A {};
    auto m = make_managed<A>();
    auto addr = reinterpret_cast<uintptr_t>(&*m);
    BOOST_REQUIRE((addr & (alignment - 1)) == 0);
}
