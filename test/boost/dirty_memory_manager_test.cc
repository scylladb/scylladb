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

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#ifndef SEASTAR_DEFAULT_ALLOCATOR
#include "utils/phased_barrier.hh"
#endif
#include "utils/logalloc.hh"
#include "replica/dirty_memory_manager.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "test/lib/log.hh"
#include "log.hh"

[[gnu::unused]]
static auto x = [] {
    logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
    return 0;
}();

using namespace logalloc;
using namespace replica::dirty_memory_manager_logalloc;
using namespace replica;

SEASTAR_TEST_CASE(test_region_groups) {
    return seastar::async([] {
        region_group just_four;
        region_group one_and_two("one_and_two");

        auto one = std::make_unique<size_tracked_region>();
        one->listen(&one_and_two);
        auto two = std::make_unique<size_tracked_region>();
        two->listen(&one_and_two);
        auto three = std::make_unique<size_tracked_region>();
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
        BOOST_REQUIRE_EQUAL(one_and_two.unspooled_memory_used(), one->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(one_and_two.real_memory_used(), one->occupancy().total_space());

        constexpr size_t two_count = 8 * base_count;
        std::vector<managed_ref<int>> two_objs;
        with_allocator(two->allocator(), [&] {
            for (size_t i = 0; i < two_count; i++) {
                two_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(two->occupancy().used_space()), ssize_t(two_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(two->occupancy().total_space()), ssize_t(two->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(one_and_two.unspooled_memory_used(), one->occupancy().total_space() + two->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(one_and_two.real_memory_used(), one_and_two.unspooled_memory_used());

        constexpr size_t three_count = 32 * base_count;
        std::vector<managed_ref<int>> three_objs;
        with_allocator(three->allocator(), [&] {
            for (size_t i = 0; i < three_count; i++) {
                three_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().used_space()), ssize_t(three_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().total_space()), ssize_t(three->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(one_and_two.real_memory_used(), one_and_two.unspooled_memory_used());

        constexpr size_t four_count = 4 * base_count;
        std::vector<managed_ref<int>> four_objs;
        with_allocator(four->allocator(), [&] {
            for (size_t i = 0; i < four_count; i++) {
                four_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(four->occupancy().used_space()), ssize_t(four_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(four->occupancy().total_space()), ssize_t(four->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(just_four.unspooled_memory_used(), four->occupancy().total_space());

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
        BOOST_REQUIRE_EQUAL(one_and_two.real_memory_used(), one_and_two.unspooled_memory_used());
        BOOST_REQUIRE_EQUAL(just_four.unspooled_memory_used(), 0);

        three->merge(*five);
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().used_space()), ssize_t((three_count  + four_count)* sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().total_space()), ssize_t(three->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(one_and_two.real_memory_used(), one_and_two.unspooled_memory_used());

        with_allocator(two->allocator(), [&] {
            two_objs.clear();
        });
        two.reset();
        BOOST_REQUIRE_EQUAL(one_and_two.unspooled_memory_used(), one->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(one_and_two.real_memory_used(), one_and_two.unspooled_memory_used());

        with_allocator(one->allocator(), [&] {
            one_objs.clear();
        });
        one.reset();
        BOOST_REQUIRE_EQUAL(one_and_two.unspooled_memory_used(), 0);
        BOOST_REQUIRE_EQUAL(one_and_two.real_memory_used(), 0);

        with_allocator(three->allocator(), [&] {
            three_objs.clear();
            four_objs.clear();
        });
        three.reset();
        four.reset();
        five.reset();
        BOOST_REQUIRE_EQUAL(one_and_two.real_memory_used(), 0);
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
struct raii_region_group: public region_group {
    raii_region_group(reclaim_config cfg)
        : region_group("test_region_group", std::move(cfg)) {}

    ~raii_region_group() {
        shutdown().get();
    }
};

struct test_region: public replica::dirty_memory_manager_logalloc::size_tracked_region  {
    test_region() : replica::dirty_memory_manager_logalloc::size_tracked_region() {}
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
        // singleton hierarchy, only one segment allowed
        raii_region_group simple({ .unspooled_hard_limit = logalloc::segment_size });
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
        BOOST_REQUIRE_EQUAL(simple.unspooled_memory_used(), logalloc::segment_size);

        fut = simple.run_when_memory_available([&simple_region] { simple_region->alloc_small(); }, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), true);
        BOOST_REQUIRE_EQUAL(simple.unspooled_memory_used(), logalloc::segment_size);

        auto big_region = std::make_unique<test_region>();
        big_region->listen(&simple);
        // Allocate a big chunk, that will certainly get us over the threshold
        big_region->alloc();

        // We should not be permitted to go forward with a new allocation now...
        testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
        fut = simple.run_when_memory_available([&simple_region] { simple_region->alloc_small(); }, db::no_timeout);
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        BOOST_REQUIRE_GT(simple.unspooled_memory_used(), logalloc::segment_size);

        testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
        testlog.info("used = {}", simple.unspooled_memory_used());

        testlog.info("Resetting");

        // But when we remove the big bytes allocator from the region, then we should.
        // Internally, we can't guarantee that just freeing the object will give the segment back,
        // that's up to the internal policies. So to make sure we need to remove the whole region.
        big_region.reset();

        testlog.info("used = {}", simple.unspooled_memory_used());
        testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
        try {
            quiesce(std::move(fut));
        } catch (...) {
            testlog.info("Aborting: {}", std::current_exception());
            testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
            testlog.info("used = {}", simple.unspooled_memory_used());
            abort();
        }
        testlog.info("now = {}", lowres_clock::now().time_since_epoch().count());
    });
}

SEASTAR_TEST_CASE(test_region_groups_fifo_order) {
    // tests that requests that are queued for later execution execute in FIFO order
    return seastar::async([] {
        raii_region_group rg({.unspooled_hard_limit = logalloc::segment_size});

        auto region = std::make_unique<test_region>();
        region->listen(&rg);

        // fill the parent. Try allocating at child level. Should not be allowed.
        region->alloc();
        BOOST_REQUIRE_GE(rg.unspooled_memory_used(), logalloc::segment_size);

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

class test_reclaimer {
    test_reclaimer *_result_accumulator;
    region_group _rg;
    std::vector<size_t> _reclaim_sizes;
    shared_promise<> _unleash_reclaimer;
    seastar::gate _reclaimers_done;
    promise<> _unleashed;
public:
    void start_reclaiming() noexcept {
        // Future is waited on indirectly in `~test_reclaimer()` (via `_reclaimers_done`).
        (void)with_gate(_reclaimers_done, [this] {
            return _unleash_reclaimer.get_shared_future().then([this] {
                _unleashed.set_value();
                while (_rg.under_unspooled_pressure()) {
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

    test_reclaimer(size_t threshold)
        : _result_accumulator(this)
        , _rg("test_reclaimer RG", {
            .unspooled_hard_limit = threshold,
            .start_reclaiming = std::bind_front(&test_reclaimer::start_reclaiming, this),
        }) {}

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
        // The function can only be executed when all three are freed - which exercises continuous
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

// Reproduces issue #2021
SEASTAR_TEST_CASE(test_no_crash_when_a_lot_of_requests_released_which_change_region_group_size) {
    return seastar::async([test_name = get_name()] {
#ifndef SEASTAR_DEFAULT_ALLOCATOR // Because we need memory::stats().free_memory();
        logging::logger_registry().set_logger_level("lsa", seastar::log_level::debug);

        auto free_space = memory::stats().free_memory();
        size_t threshold = size_t(0.75 * free_space);
        region_group gr(test_name, {.unspooled_hard_limit = threshold, .unspooled_soft_limit = threshold});
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
                while (!gr.under_unspooled_pressure()) {
                    objs.emplace_back(managed_bytes(managed_bytes::initialized_later(), 1024));
                }
            };

            utils::phased_barrier request_barrier;
            auto wait_for_requests = defer([&] () noexcept { request_barrier.advance_and_await().get(); });

            for (int i = 0; i < 1000000; ++i) {
                fill_to_pressure();
                future<> f = gr.run_when_memory_available([&, op = request_barrier.start()] {
                    // Trigger group size change (Refs issue #2021)
                    gr.update_unspooled(-10);
                    gr.update_unspooled(+10);
                }, db::no_timeout);
                BOOST_REQUIRE(!f.available());
            }

            // Release
            while (gr.under_unspooled_pressure()) {
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

        bool reclaiming = false;
        region_group gr(test_name, {
                .unspooled_hard_limit = hard_threshold,
                .unspooled_soft_limit = soft_threshold,
                .start_reclaiming = [&] () noexcept { reclaiming = true; },
                .stop_reclaiming = [&] () noexcept { reclaiming = false; },
        });
        auto close_gr = defer([&gr] () noexcept { gr.shutdown().get(); });
        size_tracked_region r;
        r.listen(&gr);

        with_allocator(r.allocator(), [&] {
            std::vector<managed_bytes> objs;

            BOOST_REQUIRE(!reclaiming);

            while (!gr.over_unspooled_soft_limit()) {
                objs.emplace_back(managed_bytes(managed_bytes::initialized_later(), logalloc::segment_size));
            }

            BOOST_REQUIRE(reclaiming);

            while (!gr.under_unspooled_pressure()) {
                objs.emplace_back(managed_bytes(managed_bytes::initialized_later(), logalloc::segment_size));
            }

            BOOST_REQUIRE(reclaiming);

            while (gr.under_unspooled_pressure()) {
                objs.pop_back();
            }

            BOOST_REQUIRE(gr.over_unspooled_soft_limit());
            BOOST_REQUIRE(reclaiming);

            while (gr.over_unspooled_soft_limit()) {
                objs.pop_back();
            }

            BOOST_REQUIRE(!reclaiming);
        });
    });
}

class test_region_group : public region_group {
    sstring _name;

public:
    test_region_group(sstring name)
        : region_group(name)
        , _name(std::move(name))
    {}

    const sstring& name() const noexcept {
        return _name;
    }

    bool empty() const noexcept {
        return _regions.empty();
    }

    bool contains(const region* r) const noexcept {
        auto strg = static_cast<const size_tracked_region*>(r);
        for (auto it = _regions.begin(); it != _regions.end(); ++it) {
            if (*it == strg) {
                return true;
            }
        }
        return false;
    }

public:
    virtual void add(region* r) override {
        testlog.debug("test_region_listener [{}:{}]: add region={}", _name, fmt::ptr(this), fmt::ptr(r));

        BOOST_REQUIRE(!contains(r));
        region_group::add(r);
        BOOST_REQUIRE(contains(r));
    }
    virtual void del(region* r) override {
        testlog.debug("test_region_listener [{}:{}]: del region={}", _name, fmt::ptr(this), fmt::ptr(r));

        BOOST_REQUIRE(contains(r));
        region_group::del(r);
        BOOST_REQUIRE(!contains(r));
    }
    virtual void moved(region* old_region, region* new_region) override {
        testlog.debug("test_region_listener [{}:{}]: moved old_region={} new_region={}", _name, fmt::ptr(this), fmt::ptr(old_region), fmt::ptr(new_region));

        BOOST_REQUIRE(contains(old_region));
        BOOST_REQUIRE(!contains(new_region));
        region_group::moved(old_region, new_region);
        BOOST_REQUIRE(!contains(old_region));
        BOOST_REQUIRE(contains(new_region));
    }
    virtual void increase_usage(region* r, ssize_t delta) override {
        testlog.debug("test_region_listener [{}:{}]: increase_usage region={} delta={}", _name, fmt::ptr(this), fmt::ptr(r), delta);

        BOOST_REQUIRE(contains(r));
        region_group::increase_usage(r, delta);
    }
    virtual void decrease_evictable_usage(region* r) override {
        testlog.debug("test_region_listener [{}:{}]: decrease_evictable_usage region={}", _name, fmt::ptr(this), fmt::ptr(r));

        BOOST_REQUIRE(contains(r));
        region_group::decrease_evictable_usage(r);
    }
    virtual void decrease_usage(region* r, ssize_t delta) override {
        testlog.debug("test_region_listener [{}:{}]: decrease_usage region={} delta={}", _name, fmt::ptr(this), fmt::ptr(r), delta);

        BOOST_REQUIRE(contains(r));
        region_group::decrease_usage(r, delta);
    }
};

SEASTAR_THREAD_TEST_CASE(test_size_tracked_region_move) {
    struct managed_object {
        int x;
        static size_t storage_size() noexcept { return sizeof(x); }
    };

    test_region_group rg0("test_size_tracked_region_move.rg0");
    size_tracked_region r0;
    r0.listen(&rg0);
    void* p = r0.allocator().alloc<managed_object>(managed_object::storage_size());
    BOOST_REQUIRE_NE(p, nullptr);

    size_tracked_region r1(std::move(r0));
    r1.allocator().free(std::exchange(p, nullptr));
}

SEASTAR_THREAD_TEST_CASE(test_size_tracked_region_move_assign) {
    struct managed_object {
        int x;
        static size_t storage_size() noexcept { return sizeof(x); }
    };

    test_region_group rg0("test_size_tracked_region_move.rg0");
    size_tracked_region r0;
    r0.listen(&rg0);

    void* p = r0.allocator().alloc<managed_object>(managed_object::storage_size());
    BOOST_REQUIRE_NE(p, nullptr);

    test_region_group rg1("test_size_tracked_region_move.rg1");
    size_tracked_region r1;
    r1.listen(&rg1);

    r1 = std::move(r0);
    r1.allocator().free(std::exchange(p, nullptr));
}
