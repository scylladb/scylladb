/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <boost/intrusive/parent_from_member.hpp>
#include <algorithm>
#include <chrono>
#include <random>

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <deque>
#include "utils/lsa/weak_ptr.hh"
#include "utils/phased_barrier.hh"

#include "utils/logalloc.hh"
#include "dirty_memory_manager.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "utils/chunked_vector.hh"
#include "test/lib/log.hh"
#include "log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/make_random_string.hh"

[[gnu::unused]]
static auto x = [] {
    logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
    return 0;
}();

using namespace logalloc;
using namespace dirty_memory_manager_logalloc;

SEASTAR_TEST_CASE(test_region_groups) {
    return seastar::async([] {
        region_group just_four;
        region_group all;
        region_group one_and_two("one_and_two", &all);

        auto one = std::make_unique<size_tracked_region>();
        one->listen(&one_and_two);
        auto two = std::make_unique<size_tracked_region>();
        two->listen(&one_and_two);
        auto three = std::make_unique<size_tracked_region>();
        three->listen(&all);
        auto four = std::make_unique<size_tracked_region>();
        four->listen(&just_four);
        auto five = std::make_unique<size_tracked_region>();

        constexpr size_t base_count = 16 * 1024;

        constexpr size_t one_count = 16 * base_count;
        std::vector<managed_ref<int>> one_objs;
        with_allocator(one->allocator(), [&] {
            for (size_t i = 0; i < one_count; i++) {
                one_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(one->occupancy().used_space()), ssize_t(one_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(one->occupancy().total_space()), ssize_t(one->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(one_and_two.memory_used(), one->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(all.memory_used(), one->occupancy().total_space());

        constexpr size_t two_count = 8 * base_count;
        std::vector<managed_ref<int>> two_objs;
        with_allocator(two->allocator(), [&] {
            for (size_t i = 0; i < two_count; i++) {
                two_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(two->occupancy().used_space()), ssize_t(two_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(two->occupancy().total_space()), ssize_t(two->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(one_and_two.memory_used(), one->occupancy().total_space() + two->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used());

        constexpr size_t three_count = 32 * base_count;
        std::vector<managed_ref<int>> three_objs;
        with_allocator(three->allocator(), [&] {
            for (size_t i = 0; i < three_count; i++) {
                three_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().used_space()), ssize_t(three_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().total_space()), ssize_t(three->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used() + three->occupancy().total_space());

        constexpr size_t four_count = 4 * base_count;
        std::vector<managed_ref<int>> four_objs;
        with_allocator(four->allocator(), [&] {
            for (size_t i = 0; i < four_count; i++) {
                four_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(four->occupancy().used_space()), ssize_t(four_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(four->occupancy().total_space()), ssize_t(four->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(just_four.memory_used(), four->occupancy().total_space());

        with_allocator(five->allocator(), [] {
            constexpr size_t five_count = base_count;
            std::vector<managed_ref<int>> five_objs;
            for (size_t i = 0; i < five_count; i++) {
                five_objs.emplace_back(make_managed<int>());
            }
        });

        three->merge(*four);
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().used_space()), ssize_t((three_count  + four_count)* sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().total_space()), ssize_t(three->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used() + three->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(just_four.memory_used(), 0);

        three->merge(*five);
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().used_space()), ssize_t((three_count  + four_count)* sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().total_space()), ssize_t(three->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used() + three->occupancy().total_space());

        with_allocator(two->allocator(), [&] {
            two_objs.clear();
        });
        two.reset();
        BOOST_REQUIRE_EQUAL(one_and_two.memory_used(), one->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used() + three->occupancy().total_space());

        with_allocator(one->allocator(), [&] {
            one_objs.clear();
        });
        one.reset();
        BOOST_REQUIRE_EQUAL(one_and_two.memory_used(), 0);
        BOOST_REQUIRE_EQUAL(all.memory_used(), three->occupancy().total_space());

        with_allocator(three->allocator(), [&] {
            three_objs.clear();
            four_objs.clear();
        });
        three.reset();
        four.reset();
        five.reset();
        BOOST_REQUIRE_EQUAL(all.memory_used(), 0);
    });
}

using namespace std::chrono_literals;

template <typename FutureType>
inline void quiesce(FutureType&& fut) {
    // Unfortunately seastar::thread::yield is not enough here, because the process of releasing
    // a request may be broken into many continuations. While we could just yield many times, the
    // exact amount needed to guarantee execution would be dependent on the internals of the
    // implementation, we want to avoid that.
    with_timeout(lowres_clock::now() + 2s, std::move(fut)).get();
}

// Simple RAII structure that wraps around a region_group
// Not using defer because we usually employ many region groups
struct test_region_group: public region_group {
    test_region_group(region_group* parent, region_group_reclaimer& reclaimer)
        : region_group("test_region_group", parent, reclaimer) {}
    test_region_group(region_group_reclaimer& reclaimer)
        : region_group("test_region_group", nullptr, reclaimer) {}

    ~test_region_group() {
        shutdown().get();
    }
};

struct test_region: public dirty_memory_manager_logalloc::size_tracked_region  {
    test_region() : dirty_memory_manager_logalloc::size_tracked_region() {}
    ~test_region() {
        clear();
    }

    void clear() {
        with_allocator(allocator(), [this] {
            std::vector<managed_bytes>().swap(_alloc);
            std::vector<managed_ref<uint64_t>>().swap(_alloc_simple);
        });
    }
    void alloc(size_t size = logalloc::segment_size) {
        with_allocator(allocator(), [this, size] {
            _alloc.push_back(managed_bytes(bytes(bytes::initialized_later(), size)));
        });
    }

    void alloc_small(size_t nr = 1) {
        with_allocator(allocator(), [this] {
            _alloc_simple.emplace_back(make_managed<uint64_t>());
        });
    }
private:
    std::vector<managed_bytes> _alloc;
    // For small objects we don't want to get caught in basic_sstring's internal buffer. We know
    // which size we need to allocate to avoid that, but that's technically internal representation.
    // Better to use integers if we want something small.
    std::vector<managed_ref<uint64_t>> _alloc_simple;
};

SEASTAR_TEST_CASE(test_region_groups_basic_throttling) {
    return seastar::async([] {
        region_group_reclaimer simple_reclaimer(logalloc::segment_size);

        // singleton hierarchy, only one segment allowed
        test_region_group simple(simple_reclaimer);
        auto simple_region = std::make_unique<test_region>();
        simple_region->listen(&simple);

        // Expectation: after first allocation region will have one segment,
        // memory_used() == throttle_threshold and we are good to go, future
        // is ready immediately.
        //
        // The allocation of the first element won't change the memory usage inside
        // the group and we'll be okay to do that a second time.
        auto fut = simple.run_when_memory_available([&simple_region] { simple_region->alloc_small(); }, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), true);
        BOOST_REQUIRE_EQUAL(simple.memory_used(), logalloc::segment_size);

        fut = simple.run_when_memory_available([&simple_region] { simple_region->alloc_small(); }, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), true);
        BOOST_REQUIRE_EQUAL(simple.memory_used(), logalloc::segment_size);

        auto big_region = std::make_unique<test_region>();
        big_region->listen(&simple);
        // Allocate a big chunk, that will certainly get us over the threshold
        big_region->alloc();

        // We should not be permitted to go forward with a new allocation now...
        testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
        fut = simple.run_when_memory_available([&simple_region] { simple_region->alloc_small(); }, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        BOOST_REQUIRE_GT(simple.memory_used(), logalloc::segment_size);

        testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
        testlog.info("used = {}", simple.memory_used());

        testlog.info("Resetting");

        // But when we remove the big bytes allocator from the region, then we should.
        // Internally, we can't guarantee that just freeing the object will give the segment back,
        // that's up to the internal policies. So to make sure we need to remove the whole region.
        big_region.reset();

        testlog.info("used = {}", simple.memory_used());
        testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
        try {
            quiesce(std::move(fut));
        } catch (...) {
            testlog.info("Aborting: {}", std::current_exception());
            testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
            testlog.info("used = {}", simple.memory_used());
            abort();
        }
        testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
    });
}

SEASTAR_TEST_CASE(test_region_groups_linear_hierarchy_throttling_child_alloc) {
    return seastar::async([] {
        region_group_reclaimer parent_reclaimer(2 * logalloc::segment_size);
        region_group_reclaimer child_reclaimer(logalloc::segment_size);

        test_region_group parent(parent_reclaimer);
        test_region_group child(&parent, child_reclaimer);

        auto child_region = std::make_unique<test_region>();
        child_region->listen(&child);
        auto parent_region = std::make_unique<test_region>();
        parent_region->listen(&parent);

        child_region->alloc();
        BOOST_REQUIRE_GE(parent.memory_used(), logalloc::segment_size);

        auto fut = parent.run_when_memory_available([&parent_region] { parent_region->alloc_small(); }, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), true);
        BOOST_REQUIRE_GE(parent.memory_used(), 2 * logalloc::segment_size);

        // This time child will use all parent's memory. Note that because the child's memory limit
        // is lower than the parent's, for that to happen we need to allocate directly.
        child_region->alloc();
        BOOST_REQUIRE_GE(child.memory_used(), 2 * logalloc::segment_size);

        fut = parent.run_when_memory_available([&parent_region] { parent_region->alloc_small(); }, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        BOOST_REQUIRE_GE(parent.memory_used(), 2 * logalloc::segment_size);

        child_region.reset();
        quiesce(std::move(fut));
    });
}

SEASTAR_TEST_CASE(test_region_groups_linear_hierarchy_throttling_parent_alloc) {
    return seastar::async([] {
        region_group_reclaimer simple_reclaimer(logalloc::segment_size);

        test_region_group parent(simple_reclaimer);
        test_region_group child(&parent, simple_reclaimer);

        auto parent_region = std::make_unique<test_region>();
        parent_region->listen(&parent);

        parent_region->alloc();
        BOOST_REQUIRE_GE(parent.memory_used(), logalloc::segment_size);

        auto fut = child.run_when_memory_available([] {}, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), false);

        parent_region.reset();
        quiesce(std::move(fut));
    });
}

SEASTAR_TEST_CASE(test_region_groups_fifo_order) {
    // tests that requests that are queued for later execution execute in FIFO order
    return seastar::async([] {
        region_group_reclaimer simple_reclaimer(logalloc::segment_size);

        test_region_group rg(simple_reclaimer);

        auto region = std::make_unique<test_region>();
        region->listen(&rg);

        // fill the parent. Try allocating at child level. Should not be allowed.
        region->alloc();
        BOOST_REQUIRE_GE(rg.memory_used(), logalloc::segment_size);

        auto exec_cnt = make_lw_shared<int>(0);
        std::vector<future<>> executions;

        for (auto index = 0; index < 100; ++index) {
            auto fut = rg.run_when_memory_available([exec_cnt, index] {
                BOOST_REQUIRE_EQUAL(index, (*exec_cnt)++);
            }, db::no_timeout);
            BOOST_REQUIRE_EQUAL(fut.available(), false);
            executions.push_back(std::move(fut));
        }

        region.reset();
        quiesce(when_all(executions.begin(), executions.end()));
    });
}

SEASTAR_TEST_CASE(test_region_groups_linear_hierarchy_throttling_moving_restriction) {
    // Hierarchy here is A -> B -> C.
    // We will fill B causing an execution in C to fail. We then fill A and free B.
    //
    // C should still be blocked.
    return seastar::async([] {
        region_group_reclaimer simple_reclaimer(logalloc::segment_size);

        test_region_group root(simple_reclaimer);
        test_region_group inner(&root, simple_reclaimer);
        test_region_group child(&inner, simple_reclaimer);

        auto inner_region = std::make_unique<test_region>();
        inner_region->listen(&inner);
        auto root_region = std::make_unique<test_region>();
        root_region->listen(&root);

        // fill the inner node. Try allocating at child level. Should not be allowed.
        circular_buffer<managed_bytes> big_alloc;
        with_allocator(inner_region->allocator(), [&big_alloc] {
            big_alloc.push_back(managed_bytes(bytes(bytes::initialized_later(), logalloc::segment_size)));
        });
        BOOST_REQUIRE_GE(inner.memory_used(), logalloc::segment_size);

        auto fut = child.run_when_memory_available([] {}, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), false);

        // Now fill the root...
        with_allocator(root_region->allocator(), [&big_alloc] {
            big_alloc.push_back(managed_bytes(bytes(bytes::initialized_later(), logalloc::segment_size)));
        });
        BOOST_REQUIRE_GE(root.memory_used(), logalloc::segment_size);

        // And free the inner node. We will verify that
        // 1) the notifications that the inner node sent the child when it was freed won't
        //    erroneously cause it to execute
        // 2) the child is still able to receive notifications from the root
        with_allocator(inner_region->allocator(), [&big_alloc] {
            big_alloc.pop_front();
        });
        inner_region.reset();

        // Verifying (1)
        // Can't quiesce because we don't want to wait on the futures.
        sleep(10ms).get();
        BOOST_REQUIRE_EQUAL(fut.available(), false);

        // Verifying (2)
        with_allocator(root_region->allocator(), [&big_alloc] {
            big_alloc.pop_front();
        });
        root_region.reset();
        quiesce(std::move(fut));
    });
}

SEASTAR_TEST_CASE(test_region_groups_tree_hierarchy_throttling_leaf_alloc) {
    return seastar::async([] {
        class leaf {
            region_group_reclaimer _leaf_reclaimer;
            test_region_group _rg;
            std::unique_ptr<test_region> _region;
        public:
            leaf(test_region_group& parent)
                : _leaf_reclaimer(logalloc::segment_size)
                , _rg(&parent, _leaf_reclaimer)
                , _region(std::make_unique<test_region>())
                {
                _region->listen(&_rg);
            }

            void alloc(size_t size) {
                _region->alloc(size);
            }

            future<> try_alloc(size_t size) {
                return _rg.run_when_memory_available([this, size] {
                    alloc(size);
                }, db::no_timeout);
            }
            void reset() {
                _region.reset(new test_region());
                _region->listen(&_rg);
            }
        };

        region_group_reclaimer simple_reclaimer(logalloc::segment_size);
        test_region_group parent(simple_reclaimer);

        leaf first_leaf(parent);
        leaf second_leaf(parent);
        leaf third_leaf(parent);

        first_leaf.alloc(logalloc::segment_size);
        second_leaf.alloc(logalloc::segment_size);
        third_leaf.alloc(logalloc::segment_size);

        auto fut_1 = first_leaf.try_alloc(sizeof(uint64_t));
        auto fut_2 = second_leaf.try_alloc(sizeof(uint64_t));
        auto fut_3 = third_leaf.try_alloc(sizeof(uint64_t));

        BOOST_REQUIRE_EQUAL(fut_1.available() || fut_2.available() || fut_3.available(), false);

        // Total memory is still 2 * segment_size, can't proceed
        first_leaf.reset();
        // Can't quiesce because we don't want to wait on the futures.
        sleep(10ms).get();

        BOOST_REQUIRE_EQUAL(fut_1.available() || fut_2.available() || fut_3.available(), false);

        // Now all futures should resolve.
        first_leaf.reset();
        second_leaf.reset();
        third_leaf.reset();
        quiesce(when_all(std::move(fut_1), std::move(fut_2), std::move(fut_3)));
    });
}

// Helper for all async reclaim tests.
class test_async_reclaim_region {
    dirty_memory_manager_logalloc::size_tracked_region _region;
    std::vector<managed_bytes> _alloc;
    size_t _alloc_size;
    // Make sure we don't reclaim the same region more than once. It is supposed to be empty
    // after the first reclaim
    int _reclaim_counter = 0;
    region_group& _rg;
public:
    test_async_reclaim_region(region_group& rg, size_t alloc_size)
            : _region()
            , _alloc_size(alloc_size)
            , _rg(rg)
    {
        _region.listen(&rg);
        with_allocator(_region.allocator(), [this] {
            _alloc.push_back(managed_bytes(bytes(bytes::initialized_later(), this->_alloc_size)));
        });

    }

    ~test_async_reclaim_region() {
        with_allocator(_region.allocator(), [this] {
            std::vector<managed_bytes>().swap(_alloc);
        });
    }

    size_t evict() {
        BOOST_REQUIRE_EQUAL(_reclaim_counter++, 0);
        with_allocator(_region.allocator(), [this] {
            std::vector<managed_bytes>().swap(_alloc);
        });
        _region = dirty_memory_manager_logalloc::size_tracked_region();
        _region.listen(&_rg);
        return this->_alloc_size;
    }
    static test_async_reclaim_region& from_region(dirty_memory_manager_logalloc::size_tracked_region* region_ptr) {
        auto aptr = boost::intrusive::get_parent_from_member(region_ptr, &test_async_reclaim_region::_region);
        return *aptr;
    }
};

class test_reclaimer: public region_group_reclaimer {
    test_reclaimer *_result_accumulator;
    region_group _rg;
    std::vector<size_t> _reclaim_sizes;
    shared_promise<> _unleash_reclaimer;
    seastar::gate _reclaimers_done;
    promise<> _unleashed;
public:
    virtual void start_reclaiming() noexcept override {
        // Future is waited on indirectly in `~test_reclaimer()` (via `_reclaimers_done`).
        (void)with_gate(_reclaimers_done, [this] {
            return _unleash_reclaimer.get_shared_future().then([this] {
                _unleashed.set_value();
                while (this->under_pressure()) {
                    size_t reclaimed = test_async_reclaim_region::from_region(_rg.get_largest_region()).evict();
                    _result_accumulator->_reclaim_sizes.push_back(reclaimed);
                }
            });
        });
    }

    ~test_reclaimer() {
        _reclaimers_done.close().get();
        _rg.shutdown().get();
    }

    std::vector<size_t>& reclaim_sizes() {
        return _reclaim_sizes;
    }

    region_group& rg() {
        return _rg;
    }

    test_reclaimer(size_t threshold) : region_group_reclaimer(threshold), _result_accumulator(this), _rg("test_reclaimer RG", *this) {}
    test_reclaimer(test_reclaimer& parent, size_t threshold) : region_group_reclaimer(threshold), _result_accumulator(&parent), _rg("test_reclaimer RG", &parent._rg, *this) {}

    future<> unleash(future<> after) {
        // Result indirectly forwarded to _unleashed (returned below).
        (void)after.then([this] { _unleash_reclaimer.set_value(); });
        return _unleashed.get_future();
    }
};

SEASTAR_TEST_CASE(test_region_groups_basic_throttling_simple_active_reclaim) {
    return seastar::async([] {
        // allocate a single region to exhaustion, and make sure active reclaim is activated.
        test_reclaimer simple(logalloc::segment_size);
        test_async_reclaim_region simple_region(simple.rg(), logalloc::segment_size);
        // FIXME: discarded future.
        (void)simple.unleash(make_ready_future<>());

        // Can't run this function until we have reclaimed something
        auto fut = simple.rg().run_when_memory_available([] {}, db::no_timeout);

        // Initially not available
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        quiesce(std::move(fut));

        BOOST_REQUIRE_EQUAL(simple.reclaim_sizes().size(), 1);
    });
}

SEASTAR_TEST_CASE(test_region_groups_basic_throttling_active_reclaim_worst_offender) {
    return seastar::async([] {
        // allocate three regions with three different sizes (segment boundary must be used due to
        // LSA granularity).
        //
        // The function can only be executed when all three are freed - which exercises continous
        // reclaim, but they must be freed in descending order of their sizes
        test_reclaimer simple(logalloc::segment_size);

        test_async_reclaim_region small_region(simple.rg(), logalloc::segment_size);
        test_async_reclaim_region medium_region(simple.rg(), 2 * logalloc::segment_size);
        test_async_reclaim_region big_region(simple.rg(), 3 * logalloc::segment_size);
        // FIXME: discarded future.
        (void)simple.unleash(make_ready_future<>());

        // Can't run this function until we have reclaimed
        auto fut = simple.rg().run_when_memory_available([&simple] {
            BOOST_REQUIRE_EQUAL(simple.reclaim_sizes().size(), 3);
        }, db::no_timeout);

        // Initially not available
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        quiesce(std::move(fut));

        // Test if the ordering is the one we have expected
        BOOST_REQUIRE_EQUAL(simple.reclaim_sizes()[2], logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(simple.reclaim_sizes()[1], 2 * logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(simple.reclaim_sizes()[0], 3 * logalloc::segment_size);
    });
}

SEASTAR_TEST_CASE(test_region_groups_basic_throttling_active_reclaim_leaf_offender) {
    return seastar::async([] {
        // allocate a parent region group (A) with two leaf region groups (B and C), so that B has
        // the largest size, then A, then C. Make sure that the freeing happens in descending order.
        // of their sizes regardless of the topology
        test_reclaimer root(logalloc::segment_size);
        test_reclaimer large_leaf(root, logalloc::segment_size);
        test_reclaimer small_leaf(root, logalloc::segment_size);

        test_async_reclaim_region small_region(small_leaf.rg(), logalloc::segment_size);
        test_async_reclaim_region medium_region(root.rg(), 2 * logalloc::segment_size);
        test_async_reclaim_region big_region(large_leaf.rg(), 3 * logalloc::segment_size);
        auto fr = root.unleash(make_ready_future<>());
        auto flf = large_leaf.unleash(std::move(fr));
        // FIXME: discarded future.
        (void)small_leaf.unleash(std::move(flf));

        // Can't run this function until we have reclaimed. Try at the root, and we'll make sure
        // that the leaves are forced correctly.
        auto fut = root.rg().run_when_memory_available([&root] {
            BOOST_REQUIRE_EQUAL(root.reclaim_sizes().size(), 3);
        }, db::no_timeout);

        // Initially not available
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        quiesce(std::move(fut));

        // Test if the ordering is the one we have expected
        BOOST_REQUIRE_EQUAL(root.reclaim_sizes()[2], logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(root.reclaim_sizes()[1], 2 * logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(root.reclaim_sizes()[0], 3 * logalloc::segment_size);
    });
}

SEASTAR_TEST_CASE(test_region_groups_basic_throttling_active_reclaim_ancestor_block) {
    return seastar::async([] {
        // allocate a parent region group (A) with a leaf region group (B)
        // Make sure that active reclaim still works when we block at an ancestor
        test_reclaimer root(logalloc::segment_size);
        test_reclaimer leaf(root, logalloc::segment_size);

        test_async_reclaim_region root_region(root.rg(), logalloc::segment_size);
        auto f = root.unleash(make_ready_future<>());
        // FIXME: discarded future.
        (void)leaf.unleash(std::move(f));

        // Can't run this function until we have reclaimed. Try at the leaf, and we'll make sure
        // that the root reclaims
        auto fut = leaf.rg().run_when_memory_available([&root] {
            BOOST_REQUIRE_EQUAL(root.reclaim_sizes().size(), 1);
        }, db::no_timeout);

        // Initially not available
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        quiesce(std::move(fut));

        BOOST_REQUIRE_EQUAL(root.reclaim_sizes()[0], logalloc::segment_size);
    });
}

SEASTAR_TEST_CASE(test_region_groups_basic_throttling_active_reclaim_big_region_goes_first) {
    return seastar::async([] {
        // allocate a parent region group (A) with a leaf region group (B). B's usage is higher, but
        // due to multiple small regions. Make sure we reclaim from A first.
        test_reclaimer root(logalloc::segment_size);
        test_reclaimer leaf(root, logalloc::segment_size);

        test_async_reclaim_region root_region(root.rg(), 4 * logalloc::segment_size);
        test_async_reclaim_region big_leaf_region(leaf.rg(), 3 * logalloc::segment_size);
        test_async_reclaim_region small_leaf_region(leaf.rg(), 2 * logalloc::segment_size);
        auto f = root.unleash(make_ready_future<>());
        // FIXME: discarded future.
        (void)leaf.unleash(std::move(f));

        auto fut = root.rg().run_when_memory_available([&root] {
            BOOST_REQUIRE_EQUAL(root.reclaim_sizes().size(), 3);
        }, db::no_timeout);

        // Initially not available
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        quiesce(std::move(fut));

        BOOST_REQUIRE_EQUAL(root.reclaim_sizes()[2], 2 * logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(root.reclaim_sizes()[1], 3 * logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(root.reclaim_sizes()[0], 4 * logalloc::segment_size);
    });
}

SEASTAR_TEST_CASE(test_region_groups_basic_throttling_active_reclaim_no_double_reclaim) {
    return seastar::async([] {
        // allocate a parent region group (A) with a leaf region group (B), and let B go over limit.
        // Both A and B try to execute requests, and we need to make sure that doesn't cause B's
        // region eviction function to be called more than once. Node that test_async_reclaim_region
        // will already make sure that we don't have double calls, so all we have to do is to
        // generate a situation in which a double call would happen
        test_reclaimer root(logalloc::segment_size);
        test_reclaimer leaf(root, logalloc::segment_size);

        test_async_reclaim_region leaf_region(leaf.rg(), logalloc::segment_size);
        auto f = root.unleash(make_ready_future<>());
        // FIXME: discarded future.
        (void)leaf.unleash(std::move(f));

        auto fut_root = root.rg().run_when_memory_available([&root] {
            BOOST_REQUIRE_EQUAL(root.reclaim_sizes().size(), 1);
        }, db::no_timeout);

        auto fut_leaf = leaf.rg().run_when_memory_available([&root] {
            BOOST_REQUIRE_EQUAL(root.reclaim_sizes().size(), 1);
        }, db::no_timeout);

        // Initially not available
        BOOST_REQUIRE_EQUAL(fut_root.available(), false);
        BOOST_REQUIRE_EQUAL(fut_leaf.available(), false);
        quiesce(std::move(fut_root));
        quiesce(std::move(fut_leaf));

        BOOST_REQUIRE_EQUAL(root.reclaim_sizes().size(), 1);
        BOOST_REQUIRE_EQUAL(root.reclaim_sizes()[0], logalloc::segment_size);
    });
}

// Reproduces issue #2021
SEASTAR_TEST_CASE(test_no_crash_when_a_lot_of_requests_released_which_change_region_group_size) {
    return seastar::async([test_name = get_name()] {
#ifndef SEASTAR_DEFAULT_ALLOCATOR // Because we need memory::stats().free_memory();
        logging::logger_registry().set_logger_level("lsa", seastar::log_level::debug);

        auto free_space = memory::stats().free_memory();
        size_t threshold = size_t(0.75 * free_space);
        region_group_reclaimer recl(threshold, threshold);
        region_group gr(test_name, recl);
        auto close_gr = defer([&gr] () noexcept { gr.shutdown().get(); });
        size_tracked_region r;
        r.listen(&gr);

        with_allocator(r.allocator(), [&] {
            std::vector<managed_bytes> objs;

            r.make_evictable([&] {
                if (objs.empty()) {
                    return memory::reclaiming_result::reclaimed_nothing;
                }
                with_allocator(r.allocator(), [&] {
                    objs.pop_back();
                });
                return memory::reclaiming_result::reclaimed_something;
            });

            auto fill_to_pressure = [&] {
                while (!recl.under_pressure()) {
                    objs.emplace_back(managed_bytes(managed_bytes::initialized_later(), 1024));
                }
            };

            utils::phased_barrier request_barrier;
            auto wait_for_requests = defer([&] () noexcept { request_barrier.advance_and_await().get(); });

            for (int i = 0; i < 1000000; ++i) {
                fill_to_pressure();
                future<> f = gr.run_when_memory_available([&, op = request_barrier.start()] {
                    // Trigger group size change (Refs issue #2021)
                    gr.update(-10);
                    gr.update(+10);
                }, db::no_timeout);
                BOOST_REQUIRE(!f.available());
            }

            // Release
            while (recl.under_pressure()) {
                objs.pop_back();
            }
        });
#endif
    });
}

SEASTAR_TEST_CASE(test_reclaiming_runs_as_long_as_there_is_soft_pressure) {
    return seastar::async([test_name = get_name()] {
        size_t hard_threshold = logalloc::segment_size * 8;
        size_t soft_threshold = hard_threshold / 2;

        class reclaimer : public region_group_reclaimer {
            bool _reclaim = false;
        protected:
            void start_reclaiming() noexcept override {
                _reclaim = true;
            }

            void stop_reclaiming() noexcept override {
                _reclaim = false;
            }
        public:
            reclaimer(size_t hard_threshold, size_t soft_threshold)
                : region_group_reclaimer(hard_threshold, soft_threshold)
            { }
            bool reclaiming() const { return _reclaim; };
        };

        reclaimer recl(hard_threshold, soft_threshold);
        region_group gr(test_name, recl);
        auto close_gr = defer([&gr] () noexcept { gr.shutdown().get(); });
        size_tracked_region r;
        r.listen(&gr);

        with_allocator(r.allocator(), [&] {
            std::vector<managed_bytes> objs;

            BOOST_REQUIRE(!recl.reclaiming());

            while (!recl.over_soft_limit()) {
                objs.emplace_back(managed_bytes(managed_bytes::initialized_later(), logalloc::segment_size));
            }

            BOOST_REQUIRE(recl.reclaiming());

            while (!recl.under_pressure()) {
                objs.emplace_back(managed_bytes(managed_bytes::initialized_later(), logalloc::segment_size));
            }

            BOOST_REQUIRE(recl.reclaiming());

            while (recl.under_pressure()) {
                objs.pop_back();
            }

            BOOST_REQUIRE(recl.over_soft_limit());
            BOOST_REQUIRE(recl.reclaiming());

            while (recl.over_soft_limit()) {
                objs.pop_back();
            }

            BOOST_REQUIRE(!recl.reclaiming());
        });
    });
}
