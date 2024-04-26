/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/future-util.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/test_case.hh>

#include "direct_failure_detector/failure_detector.hh"

#include "test/raft/helpers.hh"

future<> ping_shards() {
    if (smp::count == 1) {
        return seastar::yield();
    }

    return parallel_for_each(boost::irange(0u, smp::count), [] (shard_id s) {
        return smp::submit_to(s, [](){});
    });
}

struct test_pinger: public direct_failure_detector::pinger {
    std::unordered_set<endpoint_id> _responding;
    std::unordered_map<endpoint_id, size_t> _pings;
    bool _block = false;

    virtual future<bool> ping(endpoint_id ep, abort_source& as) override {
        bool ret = false;
        co_await invoke_abortable_on(0, [this, ep, &ret] (abort_source& as) -> future<> {
            ++_pings[ep];
            if (!_block) {
                ret = _responding.contains(ep);
                co_return;
            }

            promise<> p;
            auto f = p.get_future();
            auto sub = as.subscribe([&, p = std::move(p)] () mutable noexcept {
                p.set_value();
            });
            if (!sub) {
                throw abort_requested_exception{};
            }
            co_await std::move(f);
            throw abort_requested_exception{};
        }, as);
        co_return ret;
    }
};

struct test_clock : public direct_failure_detector::clock {
    std::atomic<int64_t> _ticks{0};
    condition_variable _cond;

    future<> tick() {
        ++_ticks;
        _cond.broadcast();
        return ping_shards();
    }

    virtual timepoint_t now() noexcept override {
        return _ticks;
    }

    virtual future<> sleep_until(timepoint_t tp, abort_source& as) override {
        try {
            co_await invoke_abortable_on(0, [this, tp] (abort_source& as) -> future<> {
                bool aborted = false;
                auto sub = as.subscribe([&] () noexcept {
                    aborted = true;
                    _cond.broadcast();
                });
                if (!sub) {
                    throw sleep_aborted{};
                }

                co_await _cond.wait([&] { return aborted || _ticks >= tp; });

                if (_ticks < tp) {
                    throw sleep_aborted{};
                }
            }, as);
        } catch (abort_requested_exception&) {
            throw sleep_aborted{};
        }
    }
};

struct test_listener : public direct_failure_detector::listener {
    using endpoint_id = direct_failure_detector::pinger::endpoint_id;

    std::unordered_set<endpoint_id> _live;
    condition_variable _cond;

    std::unordered_map<endpoint_id, size_t> _notifications;

    virtual future<> mark_alive(endpoint_id ep) override {
        _notifications[ep]++;
        _live.insert(ep);
        _cond.signal();
        co_return;
    }

    virtual future<> mark_dead(endpoint_id ep) override {
        _notifications[ep]++;
        _live.erase(ep);
        _cond.signal();
        co_return;
    }

    bool is_alive(endpoint_id ep) {
        return _live.contains(ep);
    }

    future<> wait_for(endpoint_id ep, bool alive) {
        return _cond.wait([this, alive, ep] { return alive == is_alive(ep); });
    }
};


SEASTAR_TEST_CASE(failure_detector_test) {
    test_pinger pinger;
    test_clock clock;
    sharded<direct_failure_detector::failure_detector> fd;
    co_await fd.start(std::ref(pinger), std::ref(clock), 10, 30);

    test_listener l1, l2;
    auto sub1 = co_await fd.local().register_listener(l1, 95);
    auto sub2 = co_await fd.local().register_listener(l2, 45);

    direct_failure_detector::pinger::endpoint_id ep1{0, 1};
    direct_failure_detector::pinger::endpoint_id ep2{0, 2};

    BOOST_REQUIRE(!l1.is_alive(ep1));
    BOOST_REQUIRE(!l2.is_alive(ep1));

    fd.local().add_endpoint(ep1);
    fd.local().add_endpoint(ep2);

    auto tick = [&clock] (size_t n) -> future<> {
        for (size_t i = 0; i < n; ++i) {
            co_await clock.tick();
        }
    };

    auto tick_until_ping = [&] (direct_failure_detector::pinger::endpoint_id ep) -> future<> {
        auto p = pinger._pings[ep];
        while (pinger._pings[ep] == p) {
            co_await tick(1);
        }
    };

    co_await tick(200);

    BOOST_REQUIRE(!l1.is_alive(ep1));
    BOOST_REQUIRE(!l2.is_alive(ep1));

    pinger._responding.insert(ep1);
    // 10 ticks (ping_period) must be enough for the fd to mark ep1 alive.
    co_await tick(10);
    co_await l1.wait_for(ep1, true);
    co_await l2.wait_for(ep1, true);

    BOOST_REQUIRE(l1.is_alive(ep1));
    BOOST_REQUIRE(l2.is_alive(ep1));
    BOOST_REQUIRE(!l1.is_alive(ep2));
    BOOST_REQUIRE(!l2.is_alive(ep2));

    pinger._responding.insert(ep2);
    co_await tick(10);
    co_await l1.wait_for(ep2, true);
    co_await l2.wait_for(ep2, true);

    BOOST_REQUIRE(l1.is_alive(ep1));
    BOOST_REQUIRE(l2.is_alive(ep1));
    BOOST_REQUIRE(l1.is_alive(ep2));
    BOOST_REQUIRE(l2.is_alive(ep2));

    pinger._responding.erase(ep1);
    pinger._responding.erase(ep2);

    // Wait until the time from last successful ping to ep1 is >= 45 (l2's threshold)
    co_await tick_until_ping(ep1);
    co_await tick(45);

    co_await l2.wait_for(ep1, false);
    co_await l2.wait_for(ep2, false);

    BOOST_REQUIRE(l1.is_alive(ep1));
    BOOST_REQUIRE(l1.is_alive(ep2));
    BOOST_REQUIRE(!l2.is_alive(ep1));
    BOOST_REQUIRE(!l2.is_alive(ep2));

    pinger._responding.insert(ep2);
    co_await tick(10);
    co_await l1.wait_for(ep2, true);
    co_await l2.wait_for(ep2, true);
    BOOST_REQUIRE(l1.is_alive(ep1));
    BOOST_REQUIRE(!l2.is_alive(ep1));

    // >= 55 ticks passed since ep1 stopped responding, wait another 40 for l1's threshold to pass
    co_await tick(40);
    co_await l1.wait_for(ep1, false);
    BOOST_REQUIRE(!l2.is_alive(ep1));

    // ep2 is currently marked alive by l2.
    // Let's periodically make ep2 unresponsive and then responsive, so that ~1 ping is successful
    // during each period of 45 ticks (= l2's threshold).
    // If the time between two successful pings does not exceed the threshold,
    // the fd should not mark the endpoint as dead.
    {
        BOOST_REQUIRE(l2.is_alive(ep2));

        co_await tick_until_ping(ep2);
        auto last_successful_ping = clock.now();
        auto n = l2._notifications[ep2];
        for (int i = 0; i < 100; ++i) {
            pinger._responding.erase(ep2);
            co_await tick(30);
            pinger._responding.insert(ep2);
            co_await tick_until_ping(ep2);
            auto diff = clock.now() - last_successful_ping;
            if (diff < 45) {
                // The time between two successful pings did not exceed threshold.
                // There should have been no notifications in between.
                BOOST_REQUIRE_EQUAL(l2._notifications[ep2], n);
            }

            last_successful_ping = clock.now();
            n = l2._notifications[ep2];
        }
    }

    // Destroy the l2 subscription.
    std::optional<direct_failure_detector::subscription> sub_opt{std::move(sub2)};
    sub_opt.reset();

    // Remove a live and responsive endpoint.
    // l1 should receive a final mark_dead notification. l2 should not, since it's no longer subscribed.
    {
        auto n = l2._notifications[ep2];
        BOOST_REQUIRE(l1.is_alive(ep2));
        BOOST_REQUIRE(l2.is_alive(ep2));
        fd.local().remove_endpoint(ep2);
        co_await l1.wait_for(ep2, false);
        BOOST_REQUIRE(l2.is_alive(ep2));
        BOOST_REQUIRE_EQUAL(l2._notifications[ep2], n);
    }

    // Readd a responsive endpoint.
    // l1 should eventually receive a mark_live notification.
    BOOST_REQUIRE(!l1.is_alive(ep2));
    fd.local().add_endpoint(ep2);
    co_await l1.wait_for(ep2, true);

    // Verify that the fd continues working even if a ping blocks until it is aborted.
    pinger._block = true;
    co_await tick(95);
    co_await l1.wait_for(ep2, false);

    // Destroy the l1 subscription
    sub_opt.emplace(std::move(sub1));
    sub_opt.reset();

    co_await fd.stop();
}
