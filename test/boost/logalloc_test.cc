/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <boost/intrusive/parent_from_member.hpp>
#include <algorithm>

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/random.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "utils/assert.hh"
#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "test/lib/log.hh"
#ifndef SEASTAR_DEFAULT_ALLOCATOR
#include "utils/chunked_vector.hh"
#include "utils/logalloc.hh"
#include "utils/lsa/weak_ptr.hh"
#include "test/lib/make_random_string.hh"
#endif
#include "log.hh"

[[gnu::unused]]
static auto x = [] {
    logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
    return 0;
}();

using namespace logalloc;
using namespace std::chrono_literals;

// this test should be first in order to initialize logalloc for others
SEASTAR_TEST_CASE(test_prime_logalloc) {
    return prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory());
}

SEASTAR_TEST_CASE(test_compaction) {
    return seastar::async([] {
        region reg;

        with_allocator(reg.allocator(), [&reg] {
            std::vector<managed_ref<int>> _allocated;

            // Allocate several segments

            auto reclaim_counter_1 = reg.reclaim_counter();

            for (int i = 0; i < 32 * 1024 * 8; i++) {
                _allocated.push_back(make_managed<int>());
            }

            // Allocation should not invalidate references
            BOOST_REQUIRE_EQUAL(reg.reclaim_counter(), reclaim_counter_1);

            shard_tracker().reclaim_all_free_segments();

            // Free 1/3 randomly

            auto& random = seastar::testing::local_random_engine;
            std::shuffle(_allocated.begin(), _allocated.end(), random);

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

SEASTAR_TEST_CASE(test_occupancy) {
    return seastar::async([] {
        region reg;
        auto& alloc = reg.allocator();
        auto* obj1 = alloc.construct<short>(42);
#ifdef SEASTAR_ASAN_ENABLED
        // The descriptor fits in 2 bytes, but the value has to be
        // aligned to 8 bytes and we pad the end so that the next
        // descriptor is aligned.
        BOOST_REQUIRE_EQUAL(reg.occupancy().used_space(), 16);
#else
        BOOST_REQUIRE_EQUAL(reg.occupancy().used_space(), 4);
#endif
        auto* obj2 = alloc.construct<short>(42);

#ifdef SEASTAR_ASAN_ENABLED
        BOOST_REQUIRE_EQUAL(reg.occupancy().used_space(), 32);
#else
        BOOST_REQUIRE_EQUAL(reg.occupancy().used_space(), 8);
#endif

        alloc.destroy(obj1);

#ifdef SEASTAR_ASAN_ENABLED
        BOOST_REQUIRE_EQUAL(reg.occupancy().used_space(), 16);
#else
        BOOST_REQUIRE_EQUAL(reg.occupancy().used_space(), 4);
#endif

        alloc.destroy(obj2);
    });
}


SEASTAR_TEST_CASE(test_compaction_with_multiple_regions) {
    return seastar::async([] {
        region reg1;
        region reg2;

        std::vector<managed_ref<int>> allocated1;
        std::vector<managed_ref<int>> allocated2;

        auto clear_vectors = defer([&] {
            with_allocator(reg1.allocator(), [&] {
                allocated1.clear();
            });
            with_allocator(reg2.allocator(), [&] {
                allocated2.clear();
            });
        });

        int count = 32 * 1024 * 4 * 2;
        
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

        size_t quarter = shard_tracker().region_occupancy().total_space() / 4;

        shard_tracker().reclaim_all_free_segments();

        // Can't reclaim anything yet
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) == 0);
        
        // Free 65% from the second pool

        // Shuffle, so that we don't free whole segments back to the pool
        // and there's nothing to reclaim.
        auto& random = seastar::testing::local_random_engine;
        std::shuffle(allocated2.begin(), allocated2.end(), random);

        with_allocator(reg2.allocator(), [&] {
            auto it = allocated2.begin();
            for (size_t i = 0; i < (count * 0.65); ++i) {
                *it++ = {};
            }
        });

        BOOST_REQUIRE(shard_tracker().reclaim(quarter) >= quarter);
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) < quarter);

        // Free 65% from the first pool

        std::shuffle(allocated1.begin(), allocated1.end(), random);

        with_allocator(reg1.allocator(), [&] {
            auto it = allocated1.begin();
            for (size_t i = 0; i < (count * 0.65); ++i) {
                *it++ = {};
            }
        });

        BOOST_REQUIRE(shard_tracker().reclaim(quarter) >= quarter);
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) < quarter);
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

            BOOST_REQUIRE(managed_bytes_view(b) == bytes_view(src));

            reg.full_compaction();

            BOOST_REQUIRE(managed_bytes_view(b) == bytes_view(src));
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

SEASTAR_THREAD_TEST_CASE(test_region_move) {
    logalloc::region r0;
    logalloc::region r1(std::move(r0)); // simple move
    logalloc::region r2(std::move(r1)); // transitive move
    logalloc::region r3(std::move(r0)); // moving a moved-from region (with disengaged impl)

    logalloc::region r4;
    r4 = std::move(r2); // simple move
    r4 = std::move(r3); // moving a moved-from region (with disengaged impl)
    auto r5 = std::move(r4);
}

#ifndef SEASTAR_DEFAULT_ALLOCATOR
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
            auto& random = seastar::testing::local_random_engine;
            std::shuffle(refs.begin(), refs.end(), random);
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

        std::vector<managed_bytes> evictable;
        std::vector<managed_bytes> non_evictable;

        auto nr_elements = seastar::memory::stats().total_memory() / element_size;
        evictable.reserve(nr_elements / 2);
        non_evictable.reserve(nr_elements / 2);

        try {
            while (true) {
                with_allocator(r_evictable.allocator(), [&] {
                    evictable.push_back(managed_bytes(bytes(bytes::initialized_later(),element_size)));
                });
                with_allocator(r_non_evictable.allocator(), [&] {
                    non_evictable.push_back(managed_bytes(bytes(bytes::initialized_later(),element_size)));
                });
            }
        } catch (const std::bad_alloc&) {
            // expected
        }

        auto& random = seastar::testing::local_random_engine;
        std::shuffle(evictable.begin(), evictable.end(), random);
        r_evictable.make_evictable([&] {
            return with_allocator(r_evictable.allocator(), [&] {
                if (evictable.empty()) {
                    return memory::reclaiming_result::reclaimed_nothing;
                }
                evictable.pop_back();
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
            std::vector<std::unique_ptr<char[]>> ptrs;
            auto to_alloc = evictable.size() * element_size / 4 * 3;
            auto unit = seastar::memory::stats().total_memory() / 32;
            size_t allocated = 0;
            while (allocated < to_alloc) {
                ptrs.push_back(std::make_unique<char[]>(unit));
                allocated += unit;
            }
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

SEASTAR_TEST_CASE(test_zone_reclaiming_preserves_free_size) {
    return seastar::async([] {
        region r;
        with_allocator(r.allocator(), [&] {
            chunked_fifo<managed_bytes> objs;

            auto zone_size = max_zone_segments * segment_size;

            // We need to generate 3 zones, so that at least one zone (not last) can be released fully. The first
            // zone would not due to emergency reserve.
            while (logalloc::shard_tracker().region_occupancy().used_space() < zone_size * 2 + zone_size / 4) {
                objs.emplace_back(managed_bytes(managed_bytes::initialized_later(), 1024));
            }

            testlog.info("non_lsa_used_space = {}", logalloc::shard_tracker().non_lsa_used_space());
            testlog.info("region_occupancy = {}", logalloc::shard_tracker().region_occupancy());

            while (logalloc::shard_tracker().region_occupancy().used_space() >= logalloc::segment_size * 2) {
                objs.pop_front();
            }

            testlog.info("non_lsa_used_space = {}", logalloc::shard_tracker().non_lsa_used_space());
            testlog.info("region_occupancy = {}", logalloc::shard_tracker().region_occupancy());

            auto before = logalloc::shard_tracker().non_lsa_used_space();
            logalloc::shard_tracker().reclaim(logalloc::segment_size);
            auto after = logalloc::shard_tracker().non_lsa_used_space();

            testlog.info("non_lsa_used_space = {}", logalloc::shard_tracker().non_lsa_used_space());
            testlog.info("region_occupancy = {}", logalloc::shard_tracker().region_occupancy());

            BOOST_REQUIRE(after <= before);
        });
    });
}

// Tests the intended usage of hold_reserve.
//
// Sets up a reserve, exhausts memory, opens the reserve,
// checks that this allows us to do multiple additional allocations
// without failing.
SEASTAR_THREAD_TEST_CASE(test_hold_reserve) {
    logalloc::region region;
    logalloc::allocating_section as;

    // We will fill LSA with an intrusive list of small entries.
    // We make it intrusive to avoid any containers which do std allocations,
    // since it could make the test imprecise.
    struct entry {
        using link = boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
        link _link;
        // We are going to fill the entire memory with this.
        // Padding makes the entries bigger to speed up the test.
        std::array<char, 8192> _padding;
    };
    using list = boost::intrusive::list<entry,
        boost::intrusive::member_hook<entry, entry::link, &entry::_link>,
        boost::intrusive::constant_time_size<false>>;

    as.with_reserve(region, [&] {
        with_allocator(region.allocator(), [&] {
            SCYLLA_ASSERT(sizeof(entry) + 128 < current_allocator().preferred_max_contiguous_allocation());
            logalloc::reclaim_lock rl(region);

            // Reserve a segment.
            auto guard = std::make_optional<hold_reserve>(128*1024);

            // Fill the entire available memory with LSA objects.
            list entries;
            auto clean_up = defer([&entries] {
                entries.clear_and_dispose([] (entry *e) {current_allocator().destroy(e);});
            });
            auto alloc_entry = [] () {
                return current_allocator().construct<entry>();
            };
            try {
                while (true) {
                    entries.push_back(*alloc_entry());
                }
            } catch (const std::bad_alloc&) {
                // expected
            }

            // Sanity check. We should be OOM at this point.
            BOOST_REQUIRE_THROW(hold_reserve(128*1024), std::bad_alloc);
            BOOST_REQUIRE_THROW(alloc_entry(), std::bad_alloc);

            // Release the reserve.
            guard.reset();

            // Sanity check.
            BOOST_REQUIRE_NO_THROW(hold_reserve(128*1024));
            BOOST_REQUIRE_NO_THROW(hold_reserve(128*1024));
            BOOST_REQUIRE_NO_THROW(hold_reserve(128*1024));

            // Freeing up a segment should be enough to allocate multiple small entries;
            for (int i = 0; i < 10; ++i) {
                entries.push_back(*alloc_entry());
            }
        });
    });
}

// No point in testing contiguous memory allocation in debug mode
#ifndef SEASTAR_DEFAULT_ALLOCATOR
SEASTAR_THREAD_TEST_CASE(test_can_reclaim_contiguous_memory_with_mixed_allocations) {
    prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();  // if previous test cases muddied the pool

    region evictable;
    region non_evictable;
    std::vector<managed_bytes> evictable_allocs;
    std::vector<managed_bytes> non_evictable_allocs;
    std::vector<std::unique_ptr<char[]>> std_allocs;

    auto& rnd = seastar::testing::local_random_engine;

    auto clean_up = defer([&] () noexcept {
        with_allocator(evictable.allocator(), [&] {
            evictable_allocs.clear();
        });
        with_allocator(non_evictable.allocator(), [&] {
            non_evictable_allocs.clear();
        });
    });


    // Fill up memory with allocations, try to intersperse lsa and std allocations
    size_t lsa_alloc_size = 20000;
    size_t std_alloc_size = 128*1024;
    size_t throw_wrench_every = 4*1024*1024;
    size_t ctr = 0;
    while (true) {
        try {
            with_allocator(evictable.allocator(), [&] {
                evictable_allocs.push_back(managed_bytes(managed_bytes::initialized_later(), lsa_alloc_size));
            });
            with_allocator(non_evictable.allocator(), [&] {
                non_evictable_allocs.push_back(managed_bytes(managed_bytes::initialized_later(), lsa_alloc_size));
            });
            if (++ctr % (throw_wrench_every / (2*lsa_alloc_size)) == 0) {
                // large std allocation to make it harder to allocate contiguous memory
                std_allocs.push_back(std::make_unique<char[]>(std_alloc_size));
            }
        } catch (std::bad_alloc&) {
            break;
        }
    }

    // make the reclaimer work harder
    std::shuffle(evictable_allocs.begin(), evictable_allocs.end(), rnd);

    evictable.make_evictable([&] () -> memory::reclaiming_result {
       if (evictable_allocs.empty()) {
           return memory::reclaiming_result::reclaimed_nothing;
       }
       with_allocator(evictable.allocator(), [&] {
           evictable_allocs.pop_back();
       });
       return memory::reclaiming_result::reclaimed_something;
    });

    // try to allocate 25% of memory using large-ish blocks
    size_t large_alloc_size = 20*1024*1024;
    size_t nr_large_allocs = memory::stats().total_memory() / 4 / large_alloc_size;
    std::vector<std::unique_ptr<char[]>> large_allocs;
    for (size_t i = 0; i < nr_large_allocs; ++i) {
        auto p = new (std::nothrow) char[large_alloc_size];
        BOOST_REQUIRE(p);
        auto up = std::unique_ptr<char[]>(p);
        large_allocs.push_back(std::move(up));
    }
}

SEASTAR_THREAD_TEST_CASE(test_decay_reserves) {
    logalloc::region region;
    std::list<managed_bytes> lru;
    unsigned reclaims = 0;
    logalloc::allocating_section alloc_section;
    auto small_thing = bytes(10'000, int8_t(0));
    auto large_thing = bytes(100'000'000, int8_t(0));

    auto cleanup = defer([&] () noexcept {
        with_allocator(region.allocator(), [&] {
            lru.clear();
        });
    });

    region.make_evictable([&] () -> memory::reclaiming_result {
       if (lru.empty()) {
           return memory::reclaiming_result::reclaimed_nothing;
       }
       with_allocator(region.allocator(), [&] {
           lru.pop_back();
           ++reclaims;
       });
       return memory::reclaiming_result::reclaimed_something;
    });

    // Fill up region with stuff so that allocations fail and the
    // reserve is forced to increase
    while (reclaims == 0) {
        alloc_section(region, [&] {
            with_allocator(region.allocator(), [&] {
                lru.push_front(managed_bytes(small_thing));
            });
        });
    }

    reclaims = 0;

    // Allocate a big chunk to force the reserve to increase,
    // and immediately deallocate it (to keep the lru homogeneous
    // and the test simple)
    alloc_section(region, [&] {
        with_allocator(region.allocator(), [&] {
            auto large_chunk = managed_bytes(large_thing);
            (void)large_chunk; // keep compiler quiet
        });
    });

    // sanity check, we must have reclaimed at least that much
    BOOST_REQUIRE(reclaims >= large_thing.size() / small_thing.size());

    // Run a fake workload, not actually allocating anything,
    // to let the large reserve decay
    for (int i = 0; i < 1'000'000; ++i) {
        alloc_section(region, [&] {
            // nothing
        });
    }

    reclaims = 0;

    // Fill up the reserve behind allocating_section's back,
    // so when we invoke it again we see exactly how much it
    // thinks it needs to reserve.
    with_allocator(region.allocator(), [&] {
        reclaim_lock lock(region);
        while (true) {
            try {
                lru.push_front(managed_bytes(small_thing));
            } catch (std::bad_alloc&) {
                break;
            }
        }
    });

    // Sanity check, everything was under reclaim_lock:
    BOOST_REQUIRE_EQUAL(reclaims, 0);

    // Now run a real workload, and observe how many reclaims are
    // needed. The first few allocations will not need to reclaim
    // anything since the previously large reserves made room for
    // them.
    while (reclaims == 0) {
        alloc_section(region, [&] {
            with_allocator(region.allocator(), [&] {
                lru.push_front(managed_bytes(small_thing));
            });
        });
    }

    auto expected_reserve_size = 128 * 1024 * 10;
    auto slop = 5;
    auto expected_reclaims = expected_reserve_size * slop / small_thing.size();
    BOOST_REQUIRE_LE(reclaims, expected_reclaims);
}

SEASTAR_THREAD_TEST_CASE(background_reclaim) {
    prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();  // if previous test cases muddied the pool

    region evictable;
    std::vector<managed_bytes> evictable_allocs;

    auto& rnd = seastar::testing::local_random_engine;

    auto clean_up = defer([&] () noexcept {
        with_allocator(evictable.allocator(), [&] {
            evictable_allocs.clear();
        });
    });


    // Fill up memory with allocations
    size_t lsa_alloc_size = 300;

    while (true) {
        try {
            with_allocator(evictable.allocator(), [&] {
                evictable_allocs.push_back(managed_bytes(managed_bytes::initialized_later(), lsa_alloc_size));
            });
        } catch (std::bad_alloc&) {
            break;
        }
    }

    // make the reclaimer work harder
    std::shuffle(evictable_allocs.begin(), evictable_allocs.end(), rnd);

    evictable.make_evictable([&] () -> memory::reclaiming_result {
       if (evictable_allocs.empty()) {
           return memory::reclaiming_result::reclaimed_nothing;
       }
       with_allocator(evictable.allocator(), [&] {
           evictable_allocs.pop_back();
       });
       return memory::reclaiming_result::reclaimed_something;
    });

    // Set up the background reclaimer

    auto background_reclaim_scheduling_group = create_scheduling_group("background_reclaim", 100).get();
    auto kill_sched_group = defer([&] () noexcept {
        destroy_scheduling_group(background_reclaim_scheduling_group).get();
    });

    logalloc::tracker::config st_cfg;
    st_cfg.defragment_on_idle = false;
    st_cfg.abort_on_lsa_bad_alloc = false;
    st_cfg.lsa_reclamation_step = 1;
    st_cfg.background_reclaim_sched_group = background_reclaim_scheduling_group;
    logalloc::shard_tracker().configure(st_cfg);

    auto stop_lsa_background_reclaim = defer([&] () noexcept {
        logalloc::shard_tracker().stop().get();
    });

    sleep(500ms).get(); // sleep a little, to give the reclaimer a head start

    std::vector<managed_bytes> std_allocs;
    size_t std_alloc_size = 1000000; // note that managed_bytes fragments these, even in std
    for (int i = 0; i < 50; ++i) {
        auto compacted_pre = logalloc::shard_tracker().statistics().memory_compacted;
        fmt::print("compacted {} items {} (pre)\n", compacted_pre, evictable_allocs.size());
        std_allocs.emplace_back(managed_bytes::initialized_later(), std_alloc_size);
        auto compacted_post = logalloc::shard_tracker().statistics().memory_compacted;
        fmt::print("compacted {} items {} (post)\n", compacted_post, evictable_allocs.size());
        BOOST_REQUIRE_EQUAL(compacted_pre, compacted_post);
    
        // Pretend to do some work. Sleeping would be too easy, as the background reclaim group would use
        // all that time.
        //
        // Use thread_cputime_clock to prevent overcommitted test machines from stealing CPU time
        // and causing test failures.
        auto deadline = thread_cputime_clock::now() + 100ms;
        while (thread_cputime_clock::now() < deadline) {
            thread::maybe_yield();
        }
    }
}

inline
bool is_aligned(void* ptr, size_t alignment) {
    return uintptr_t(ptr) % alignment == 0;
}

static sstring to_sstring(const lsa_buffer& buf) {
    sstring result(sstring::initialized_later(), buf.size());
    std::copy(buf.get(), buf.get() + buf.size(), result.begin());
    return result;
}

SEASTAR_THREAD_TEST_CASE(test_buf_allocation) {
    logalloc::region region;
    size_t buf_size = 4096;
    auto cookie = make_random_string(buf_size);

    lsa_buffer buf = region.alloc_buf(buf_size);
    std::copy(cookie.begin(), cookie.end(), buf.get());

    BOOST_REQUIRE_EQUAL(to_sstring(buf), cookie);
    BOOST_REQUIRE(is_aligned(buf.get(), buf_size));

    {
        auto ptr1 = buf.get();
        region.full_compaction();

        // check that the segment was moved by full_compaction() to exercise the tracking code.
        BOOST_REQUIRE(buf.get() != ptr1);
        BOOST_REQUIRE_EQUAL(to_sstring(buf), cookie);
    }

    lsa_buffer buf2;
    {
        auto ptr1 = buf.get();
        buf2 = std::move(buf);
        BOOST_REQUIRE(!buf);
        BOOST_REQUIRE_EQUAL(buf2.get(), ptr1);
        BOOST_REQUIRE_EQUAL(buf2.size(), buf_size);
    }

    region.full_compaction();

    BOOST_REQUIRE_EQUAL(to_sstring(buf2), cookie);
    BOOST_REQUIRE_EQUAL(buf2.size(), buf_size);

    buf2 = nullptr;
    BOOST_REQUIRE(!buf2);

    region.full_compaction();

    lsa_buffer buf3;
    {
        buf3 = std::move(buf2);
        BOOST_REQUIRE(!buf2);
        BOOST_REQUIRE(!buf3);
    }

    region.full_compaction();

    auto cookie2 = make_random_string(buf_size);
    auto buf4 = region.alloc_buf(buf_size);
    std::copy(cookie2.begin(), cookie2.end(), buf4.get());
    BOOST_REQUIRE(is_aligned(buf4.get(), buf_size));

    buf3 = std::move(buf4);

    region.full_compaction();

    BOOST_REQUIRE(buf3);
    BOOST_REQUIRE_EQUAL(to_sstring(buf3), cookie2);
}

SEASTAR_THREAD_TEST_CASE(test_lsa_buffer_alloc_dealloc_patterns) {
    logalloc::region region;
    size_t buf_size = 128*1024;

    std::vector<sstring> cookies;
    for (int i = 0; i < 7; ++i) {
        cookies.push_back(make_random_string(buf_size));
    }

    auto make_buf = [&] (int idx, size_t size) {
        lsa_buffer buf = region.alloc_buf(size);
        std::copy(cookies[idx].begin(), cookies[idx].begin() + size, buf.get());
        return buf;
    };

    auto chk_buf = [&] (int idx, const lsa_buffer& buf) {
        if (buf) {
            BOOST_REQUIRE_EQUAL(to_sstring(buf), cookies[idx].substr(0, buf.size()));
        }
    };

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf1 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf2 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf3 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf1 = nullptr;
        buf3 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf1 = nullptr;
        buf2 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf2 = nullptr;
        buf3 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);

    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf2 = nullptr;
        buf3 = nullptr;
        buf1 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf3 = nullptr;
        buf2 = nullptr;
        buf1 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 1);
        lsa_buffer buf2 = make_buf(2, 1);
        lsa_buffer buf3 = make_buf(3, 1);
        buf1 = nullptr;
        buf2 = nullptr;
        buf3 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
    }

    {
        lsa_buffer buf1 = make_buf(1, 128*1024);
        lsa_buffer buf2 = make_buf(2, 128*1024);
        lsa_buffer buf3 = make_buf(3, 128*1024);
        buf2 = nullptr;
        lsa_buffer buf4 = make_buf(4, 128*1024);
        buf1 = nullptr;
        lsa_buffer buf5 = make_buf(5, 128*1024);
        buf5 = nullptr;
        lsa_buffer buf6 = make_buf(6, 128*1024);

        region.full_compaction();

        chk_buf(1, buf1);
        chk_buf(2, buf2);
        chk_buf(3, buf3);
        chk_buf(4, buf4);
        chk_buf(5, buf5);
        chk_buf(6, buf6);
    }
}

SEASTAR_THREAD_TEST_CASE(test_weak_ptr) {
    logalloc::region region;

    const int cookie = 172;
    const int cookie2 = 341;

    struct Obj : public lsa::weakly_referencable<Obj> {
        int val;
        Obj(int v) : val(v) {}
    };

    managed_ref<Obj> obj_ptr = with_allocator(region.allocator(), [&] {
        return make_managed<Obj>(cookie);
    });
    auto del_obj_ptr = defer([&] () noexcept {
        with_allocator(region.allocator(), [&] {
            obj_ptr = {};
        });
    });

    managed_ref<Obj> obj2_ptr = with_allocator(region.allocator(), [&] {
        return make_managed<Obj>(cookie2);
    });
    auto del_obj2_ptr = defer([&] () noexcept {
        with_allocator(region.allocator(), [&] {
           obj2_ptr = {};
        });
    });

    lsa::weak_ptr<Obj> obj_wptr = obj_ptr->weak_from_this();

    BOOST_REQUIRE_EQUAL(obj_ptr.get(), obj_wptr.get());
    BOOST_REQUIRE_EQUAL(obj_wptr->val, cookie);
    BOOST_REQUIRE(obj_wptr);

    region.full_compaction();

    BOOST_REQUIRE_EQUAL(obj_ptr.get(), obj_wptr.get());
    BOOST_REQUIRE_EQUAL(obj_wptr->val, cookie);

    auto obj_wptr2 = obj_wptr->weak_from_this();

    BOOST_REQUIRE_EQUAL(obj_ptr.get(), obj_wptr2.get());
    BOOST_REQUIRE_EQUAL(obj_wptr2->val, cookie);
    BOOST_REQUIRE(obj_wptr2);

    auto obj_wptr3 = std::move(obj_wptr2);

    BOOST_REQUIRE_EQUAL(obj_ptr.get(), obj_wptr3.get());
    BOOST_REQUIRE_EQUAL(obj_wptr3->val, cookie);
    BOOST_REQUIRE(obj_wptr3);
    BOOST_REQUIRE(!obj_wptr2);
    BOOST_REQUIRE(obj_wptr2.get() == nullptr);

    obj_wptr3 = obj2_ptr->weak_from_this();
    BOOST_REQUIRE_EQUAL(obj2_ptr.get(), obj_wptr3.get());
    BOOST_REQUIRE_EQUAL(obj_wptr3->val, cookie2);
    BOOST_REQUIRE(obj_wptr3);

    with_allocator(region.allocator(), [&] {
        obj_ptr = {};
    });

    BOOST_REQUIRE(obj_wptr.get() == nullptr);
    BOOST_REQUIRE(!obj_wptr);
}

SEASTAR_THREAD_TEST_CASE(test_buf_alloc_compaction) {
    logalloc::region region;
    size_t buf_size = 128; // much smaller than region_impl::buf_align

    utils::chunked_vector<lsa_buffer> bufs;

    bool reclaimer_run = false;
    region.make_evictable([&] {
        reclaimer_run = true;
        if (bufs.empty()) {
            return memory::reclaiming_result::reclaimed_nothing;
        }
        bufs.pop_back();
        return memory::reclaiming_result::reclaimed_something;
    });

    allocating_section as;
    while (!reclaimer_run) {
        as(region, [&] {
            bufs.emplace_back(region.alloc_buf(buf_size));
        });
    }

    // Allocate a few segments more after eviction starts
    // to make sure we can really make forward progress.
    for (int i = 0; i < 32*100; ++i) {
        as(region, [&] {
            bufs.emplace_back(region.alloc_buf(buf_size));
        });
    }
}

#endif
