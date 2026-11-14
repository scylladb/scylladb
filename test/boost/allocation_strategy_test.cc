/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cstdint>
#include <cstring>
#include <vector>
#include <seastar/testing/thread_test_case.hh>
#include <boost/test/unit_test.hpp>

#include "utils/managed_ref.hh"

BOOST_AUTO_TEST_CASE(test_standrard_allocator_respects_alignment) {
    constexpr auto alignment = 16;
    struct alignas(alignment) A {};
    auto m = make_managed<A>();
    auto addr = reinterpret_cast<uintptr_t>(&*m);
    BOOST_REQUIRE((addr & (alignment - 1)) == 0);
}

SEASTAR_THREAD_TEST_CASE(test_standard_allocator_alignment_and_size) {
    auto& a = standard_allocator();
    std::vector<size_t> sizes;
    for (size_t s = 0; s <= 4096; ++s) {
        sizes.push_back(s);
    }
    sizes.insert(sizes.end(), {8191, 10008, 20008});
    for (auto size : sizes) {
        for (size_t alignment : {1, 2, 4, 8, 16}) {
            void* p = a.alloc(nullptr, size, alignment);
            BOOST_REQUIRE_EQUAL(reinterpret_cast<uintptr_t>(p) % alignment, 0);
            std::memset(p, 0xa5, size);
            auto usable = a.object_memory_size_in_allocator(p);
            BOOST_REQUIRE_GE(usable, size);
            // Above 16 KiB seastar may hand out up to twice the pages asked for, to aligned and plain requests alike.
            if (size > 16384) {
                BOOST_REQUIRE_LT(usable, 2 * size);
            } else if (size >= 32) {
                BOOST_REQUIRE_LE(usable, size + size / 4 + 32);
            }
            a.free(p, size);
        }
    }
}
