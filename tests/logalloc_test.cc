/*
 * Copyright 2015 Cloudius Systems
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

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <algorithm>

#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>
#include <deque>

#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "log.hh"

[[gnu::unused]]
static auto x = [] {
    logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
    return 0;
}();

using namespace logalloc;

SEASTAR_TEST_CASE(test_compaction) {
    return seastar::async([] {
        region reg;

        with_allocator(reg.allocator(), [&reg] {
            std::vector<managed_ref<int>> _allocated;

            // Allocate several segments

            auto reclaim_counter_1 = reg.reclaim_counter();

            for (int i = 0; i < 32 * 1024 * 4; i++) {
                _allocated.push_back(make_managed<int>());
            }

            // Allocation should not invalidate references
            BOOST_REQUIRE_EQUAL(reg.reclaim_counter(), reclaim_counter_1);

            shard_tracker().reclaim_all_free_segments();

            // Free 1/3 randomly

            std::random_shuffle(_allocated.begin(), _allocated.end());

            auto it = _allocated.begin();
            size_t nr_freed = _allocated.size() / 3;
            for (size_t i = 0; i < nr_freed; ++i) {
                *it++ = {};
            }

            // Freeing should not invalidate references
            BOOST_REQUIRE_EQUAL(reg.reclaim_counter(), reclaim_counter_1);

            // Try to reclaim

            size_t target = sizeof(managed<int>) * nr_freed;
            BOOST_REQUIRE(shard_tracker().reclaim(target) >= target);

            // There must have been some compaction during such reclaim
            BOOST_REQUIRE(reg.reclaim_counter() != reclaim_counter_1);
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

        shard_tracker().reclaim_all_free_segments();

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

SEASTAR_TEST_CASE(test_merging) {
    return seastar::async([] {
        region reg1;
        region reg2;

        reg1.merge(reg2);

        managed_ref<int> r1;

        with_allocator(reg1.allocator(), [&] {
            r1 = make_managed<int>();
        });

        reg2.merge(reg1);

        with_allocator(reg2.allocator(), [&] {
            r1 = {};
        });

        std::vector<managed_ref<int>> refs;

        with_allocator(reg1.allocator(), [&] {
            for (int i = 0; i < 10000; ++i) {
                refs.emplace_back(make_managed<int>());
            }
        });

        reg2.merge(reg1);

        with_allocator(reg2.allocator(), [&] {
            refs.clear();
        });
    });
}

#ifndef DEFAULT_ALLOCATOR
SEASTAR_TEST_CASE(test_region_lock) {
    return seastar::async([] {
        region reg;
        with_allocator(reg.allocator(), [&] {
            std::deque<managed_bytes> refs;

            for (int i = 0; i < 1024 * 10; ++i) {
                refs.push_back(managed_bytes(managed_bytes::initialized_later(), 1024));
            }

            // Evict 30% so that region is compactible, but do it randomly so that
            // segments are not released into the standard allocator without compaction.
            std::random_shuffle(refs.begin(), refs.end());
            for (size_t i = 0; i < refs.size() * 0.3; ++i) {
                refs.pop_back();
            }

            reg.make_evictable([&refs] {
                if (refs.empty()) {
                    return memory::reclaiming_result::reclaimed_nothing;
                }
                refs.pop_back();
                return memory::reclaiming_result::reclaimed_something;
            });

            std::deque<bytes> objects;

            auto counter = reg.reclaim_counter();

            // Verify that with compaction lock we rather run out of memory
            // than compact it
            {
                BOOST_REQUIRE(reg.reclaiming_enabled());

                logalloc::reclaim_lock _(reg);

                BOOST_REQUIRE(!reg.reclaiming_enabled());
                auto used_before = reg.occupancy().used_space();

                try {
                    while (true) {
                        objects.push_back(bytes(bytes::initialized_later(), 1024*1024));
                    }
                } catch (const std::bad_alloc&) {
                    // expected
                }

                BOOST_REQUIRE(reg.reclaim_counter() == counter);
                BOOST_REQUIRE(reg.occupancy().used_space() == used_before); // eviction is also disabled
            }

            BOOST_REQUIRE(reg.reclaiming_enabled());
        });
    });
}

SEASTAR_TEST_CASE(test_large_allocation) {
    return seastar::async([] {
        logalloc::region r_evictable;
        logalloc::region r_non_evictable;

        static constexpr unsigned element_size = 16 * 1024;

        std::deque<managed_bytes> evictable;
        std::deque<managed_bytes> non_evictable;
        try {
            while (true) {
                with_allocator(r_evictable.allocator(), [&] {
                    evictable.push_back(bytes(bytes::initialized_later(),element_size));
                });
                with_allocator(r_non_evictable.allocator(), [&] {
                    non_evictable.push_back(bytes(bytes::initialized_later(),element_size));
                });
            }
        } catch (const std::bad_alloc&) {
            // expected
        }

        std::random_shuffle(evictable.begin(), evictable.end());
        r_evictable.make_evictable([&] {
            return with_allocator(r_evictable.allocator(), [&] {
                if (evictable.empty()) {
                    return memory::reclaiming_result::reclaimed_nothing;
                }
                evictable.pop_front();
                return memory::reclaiming_result::reclaimed_something;
            });
        });

        auto clear_all = [&] {
            with_allocator(r_non_evictable.allocator(), [&] {
                non_evictable.clear();
            });
            with_allocator(r_evictable.allocator(), [&] {
                evictable.clear();
            });
        };

        try {
            auto ptr = std::make_unique<char[]>(evictable.size() * element_size / 4 * 3);
        } catch (const std::bad_alloc&) {
            // This shouldn't have happened, but clear remaining lsa data
            // properly so that humans see bad_alloc instead of some confusing
            // assertion failure caused by destroying evictable and
            // non_evictable without with_allocator().
            clear_all();
            throw;
        }

        clear_all();
    });
}
#endif
