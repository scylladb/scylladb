/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <algorithm>

#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "log.hh"

static auto x = [] {
    logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
    return 0;
}();

using namespace logalloc;

SEASTAR_TEST_CASE(test_compaction) {
    return seastar::async([] {
        region reg;
        with_allocator(reg.allocator(), [] {
            std::vector<managed_ref<int>> _allocated;

            // Allocate several segments

            for (int i = 0; i < 32 * 1024 * 4; i++) {
                _allocated.push_back(make_managed<int>());
            }

            // Free 1/3 randomly

            std::random_shuffle(_allocated.begin(), _allocated.end());

            auto it = _allocated.begin();
            size_t nr_freed = _allocated.size() / 3;
            for (size_t i = 0; i < nr_freed; ++i) {
                *it++ = {};
            }

            // Try to reclaim

            size_t target = sizeof(managed<int>) * nr_freed;
            BOOST_REQUIRE(shard_tracker().reclaim(target) >= target);
        });
    });
}


SEASTAR_TEST_CASE(test_compaction_with_multiple_regions) {
    return seastar::async([] {
        region reg1;
        region reg2;

        std::vector<managed_ref<int>> allocated1;
        std::vector<managed_ref<int>> allocated2;

        int count = 32 * 1024 * 4;
        
        with_allocator(reg1.allocator(), [&] {
            for (int i = 0; i < count; i++) {
                allocated1.push_back(make_managed<int>());
            }
        });

        with_allocator(reg2.allocator(), [&] {
            for (int i = 0; i < count; i++) {
                allocated2.push_back(make_managed<int>());
            }
        });

        size_t quarter = shard_tracker().occupancy().total_space() / 4;

        // Can't reclaim anything yet
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) == 0);
        
        // Free 60% from the second pool

        // Shuffle, so that we don't free whole segments back to the pool
        // and there's nothing to reclaim.
        std::random_shuffle(allocated2.begin(), allocated2.end());

        with_allocator(reg2.allocator(), [&] {
            auto it = allocated2.begin();
            for (size_t i = 0; i < (count * 0.6); ++i) {
                *it++ = {};
            }
        });

        BOOST_REQUIRE(shard_tracker().reclaim(quarter) >= quarter);
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) < quarter);

        // Free 60% from the first pool

        std::random_shuffle(allocated1.begin(), allocated1.end());

        with_allocator(reg1.allocator(), [&] {
            auto it = allocated1.begin();
            for (size_t i = 0; i < (count * 0.6); ++i) {
                *it++ = {};
            }
        });

        BOOST_REQUIRE(shard_tracker().reclaim(quarter) >= quarter);
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) < quarter);

        with_allocator(reg2.allocator(), [&] () mutable {
            allocated2.clear();
        });

        with_allocator(reg1.allocator(), [&] () mutable {
            allocated1.clear();
        });
    });
}

SEASTAR_TEST_CASE(test_mixed_type_compaction) {
    return seastar::async([] {
        static bool a_moved = false;
        static bool b_moved = false;
        static bool c_moved = false;

        static bool a_destroyed = false;
        static bool b_destroyed = false;
        static bool c_destroyed = false;

        struct A {
            uint8_t v = 0xca;
            A() = default;
            A(A&&) noexcept {
                a_moved = true;
            }
            ~A() {
                BOOST_REQUIRE(v == 0xca);
                a_destroyed = true;
            }
        };
        struct B {
            uint16_t v = 0xcafe;
            B() = default;
            B(B&&) noexcept {
                b_moved = true;
            }
            ~B() {
                BOOST_REQUIRE(v == 0xcafe);
                b_destroyed = true;
            }
        };
        struct C {
            uint64_t v = 0xcafebabe;
            C() = default;
            C(C&&) noexcept {
                c_moved = true;
            }
            ~C() {
                BOOST_REQUIRE(v == 0xcafebabe);
                c_destroyed = true;
            }
        };

        region reg;
        with_allocator(reg.allocator(), [&] {
            {
                std::vector<int*> objs;

                auto p1 = make_managed<A>();

                int junk_count = 10;

                for (int i = 0; i < junk_count; i++) {
                    objs.push_back(reg.allocator().construct<int>(i));
                }

                auto p2 = make_managed<B>();

                for (int i = 0; i < junk_count; i++) {
                    objs.push_back(reg.allocator().construct<int>(i));
                }

                auto p3 = make_managed<C>();

                for (auto&& p : objs) {
                    reg.allocator().destroy(p);
                }

                reg.full_compaction();

                BOOST_REQUIRE(a_moved);
                BOOST_REQUIRE(b_moved);
                BOOST_REQUIRE(c_moved);

                BOOST_REQUIRE(a_destroyed);
                BOOST_REQUIRE(b_destroyed);
                BOOST_REQUIRE(c_destroyed);

                a_destroyed = false;
                b_destroyed = false;
                c_destroyed = false;
            }

            BOOST_REQUIRE(a_destroyed);
            BOOST_REQUIRE(b_destroyed);
            BOOST_REQUIRE(c_destroyed);
        });
    });
}

SEASTAR_TEST_CASE(test_blob) {
    return seastar::async([] {
        region reg;
        with_allocator(reg.allocator(), [&] {
            auto src = bytes("123456");
            managed_bytes b(src);

            BOOST_REQUIRE(bytes_view(b) == src);

            reg.full_compaction();

            BOOST_REQUIRE(bytes_view(b) == src);
        });
    });
}
