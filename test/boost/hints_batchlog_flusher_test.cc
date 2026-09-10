/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <chrono>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/closeable.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "service/hints_batchlog_flusher.hh"
#include "utils/updateable_value.hh"

BOOST_AUTO_TEST_SUITE(hints_batchlog_flusher_test)

using namespace service;
using namespace std::chrono_literals;

namespace {

constexpr uint32_t one_minute_in_ms = 60 * 1000;

// A cluster controlled by the test: which nodes exist, which are down, and
// what each flush returns. Every flush is counted.
class fake_cluster : public hints_batchlog_flusher::cluster {
public:
    std::unordered_set<locator::host_id> members;
    std::unordered_set<locator::host_id> down;
    std::unordered_map<locator::host_id, unsigned> flushes;
    std::function<future<gc_clock::time_point>(locator::host_id, abort_source&)> on_flush = [] (locator::host_id, abort_source&) {
        return make_ready_future<gc_clock::time_point>(gc_clock::now());
    };

    std::unordered_set<locator::host_id> nodes() const override { return members; }
    bool is_alive(locator::host_id node) const override { return !down.contains(node); }
    future<gc_clock::time_point> flush(locator::host_id node, abort_source& as) override {
        ++flushes[node];
        return on_flush(node, as);
    }

    unsigned total_flushes() const {
        unsigned n = 0;
        for (const auto& [_, count] : flushes) {
            n += count;
        }
        return n;
    }
};

std::vector<locator::host_id> make_nodes(unsigned count) {
    std::vector<locator::host_id> nodes;
    for (unsigned i = 0; i < count; ++i) {
        nodes.push_back(locator::host_id::create_random_id());
    }
    return nodes;
}

std::unique_ptr<fake_cluster> make_cluster(const std::vector<locator::host_id>& nodes) {
    auto c = std::make_unique<fake_cluster>();
    c->members.insert(nodes.begin(), nodes.end());
    return c;
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_first_round_flushes_every_node_and_returns_earliest_time) {
    auto nodes = make_nodes(3);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    auto stop = deferred_stop(flusher);
    auto base = gc_clock::now();
    std::unordered_map<locator::host_id, gc_clock::time_point> reported;
    for (unsigned i = 0; i < nodes.size(); ++i) {
        reported[nodes[i]] = base + std::chrono::seconds(i);
    }
    cluster.on_flush = [&] (locator::host_id node, abort_source&) { return make_ready_future<gc_clock::time_point>(reported[node]); };

    auto time = co_await flusher.flush_time();
    BOOST_REQUIRE(time.has_value());
    BOOST_REQUIRE(*time == base);
    for (const auto& node : nodes) {
        BOOST_REQUIRE_EQUAL(cluster.flushes[node], 1u);
    }

    // Everything is fresh now, so nothing is flushed and the answer is the same.
    auto again = co_await flusher.flush_time();
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), nodes.size());
    BOOST_REQUIRE(*again == base);
}

SEASTAR_TEST_CASE(test_concurrent_callers_share_one_round) {
    auto nodes = make_nodes(3);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    auto stop = deferred_stop(flusher);
    std::unordered_map<locator::host_id, promise<gc_clock::time_point>> pending;
    cluster.on_flush = [&] (locator::host_id node, abort_source&) { return pending[node].get_future(); };

    std::vector<future<std::optional<gc_clock::time_point>>> callers;
    for (int i = 0; i < 5; ++i) {
        callers.push_back(flusher.flush_time());
    }
    // Every caller is waiting on a flush that has not completed. Only one
    // round is running: each node was asked once.
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), nodes.size());
    for (auto& caller : callers) {
        BOOST_REQUIRE(!caller.available());
    }

    auto flushed_at = gc_clock::now();
    for (auto& [node, p] : pending) {
        p.set_value(flushed_at);
    }
    for (auto& caller : callers) {
        auto time = co_await std::move(caller);
        BOOST_REQUIRE(time.has_value());
        BOOST_REQUIRE(*time == flushed_at);
    }
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), nodes.size());
}

SEASTAR_TEST_CASE(test_only_nodes_without_a_flush_time_are_flushed_again) {
    auto nodes = make_nodes(3);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    auto stop = deferred_stop(flusher);
    auto now = gc_clock::now();
    cluster.on_flush = [&] (locator::host_id, abort_source&) { return make_ready_future<gc_clock::time_point>(now); };
    auto first = co_await flusher.flush_time();
    BOOST_REQUIRE(first.has_value());
    BOOST_REQUIRE(*first == now);

    auto joining = make_nodes(1)[0];
    cluster.members.insert(joining);
    cluster.on_flush = [&] (locator::host_id, abort_source&) { return make_ready_future<gc_clock::time_point>(now - 30s); };
    auto second = co_await flusher.flush_time();
    BOOST_REQUIRE(second.has_value());
    BOOST_REQUIRE(*second == now - 30s);
    BOOST_REQUIRE_EQUAL(cluster.flushes[joining], 1u);
    for (const auto& node : nodes) {
        BOOST_REQUIRE_EQUAL(cluster.flushes[node], 1u);
    }
}

SEASTAR_TEST_CASE(test_replica_clock_skew_does_not_age_the_cache) {
    auto nodes = make_nodes(3);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    auto stop = deferred_stop(flusher);
    auto now = gc_clock::now();
    cluster.on_flush = [&] (locator::host_id node, abort_source&) {
        auto skew = node == nodes[0] ? -2h : (node == nodes[1] ? 2h : 0h);
        return make_ready_future<gc_clock::time_point>(now + skew);
    };

    auto first = co_await flusher.flush_time();
    BOOST_REQUIRE(first.has_value());
    BOOST_REQUIRE(*first == now - 2h);

    auto second = co_await flusher.flush_time();
    BOOST_REQUIRE(second.has_value());
    BOOST_REQUIRE(*second == now - 2h);
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), nodes.size());
}

SEASTAR_TEST_CASE(test_down_node_yields_no_time_without_flushing_anything) {
    auto nodes = make_nodes(3);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    auto stop = deferred_stop(flusher);
    cluster.down.insert(nodes[2]);

    auto time = co_await flusher.flush_time();
    BOOST_REQUIRE(!time.has_value());
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), 0u);

    cluster.down.clear();
    time = co_await flusher.flush_time();
    BOOST_REQUIRE(time.has_value());
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), nodes.size());

    // A node that went down after reporting a fresh flush does not matter.
    cluster.down.insert(nodes[2]);
    auto again = co_await flusher.flush_time();
    BOOST_REQUIRE(again.has_value());
    BOOST_REQUIRE(*again == *time);
}

SEASTAR_TEST_CASE(test_failed_flush_yields_no_time_but_keeps_the_others) {
    auto nodes = make_nodes(3);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    auto stop = deferred_stop(flusher);
    auto failing = nodes[2];
    cluster.on_flush = [&] (locator::host_id node, abort_source&) {
        if (node == failing) {
            return make_exception_future<gc_clock::time_point>(std::runtime_error("no reply"));
        }
        return make_ready_future<gc_clock::time_point>(gc_clock::now());
    };

    auto time = co_await flusher.flush_time();
    BOOST_REQUIRE(!time.has_value());
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), nodes.size());

    // Only the node that failed is asked again.
    cluster.on_flush = [] (locator::host_id, abort_source&) { return make_ready_future<gc_clock::time_point>(gc_clock::now()); };
    time = co_await flusher.flush_time();
    BOOST_REQUIRE(time.has_value());
    BOOST_REQUIRE_EQUAL(cluster.flushes[failing], 2u);
    BOOST_REQUIRE_EQUAL(cluster.flushes[nodes[0]], 1u);
    BOOST_REQUIRE_EQUAL(cluster.flushes[nodes[1]], 1u);
}

SEASTAR_TEST_CASE(test_node_removed_from_the_cluster_is_forgotten) {
    auto nodes = make_nodes(3);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    auto stop = deferred_stop(flusher);
    // The first node reports the earliest, still fresh, time.
    auto leaving = nodes[0];
    auto now = gc_clock::now();
    cluster.on_flush = [&] (locator::host_id node, abort_source&) {
        return make_ready_future<gc_clock::time_point>(node == leaving ? now - 30s : now);
    };
    auto with = co_await flusher.flush_time();
    BOOST_REQUIRE(with.has_value());
    BOOST_REQUIRE(*with == now - 30s);

    cluster.members.erase(leaving);
    auto without = co_await flusher.flush_time();
    BOOST_REQUIRE(without.has_value());
    BOOST_REQUIRE(*without == now);
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), nodes.size());
}

SEASTAR_TEST_CASE(test_zero_cache_time_flushes_on_every_call_until_raised) {
    auto nodes = make_nodes(2);
    utils::updateable_value_source<uint32_t> cache_time_in_ms(0);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(cache_time_in_ms), as);
    auto stop = deferred_stop(flusher);

    co_await flusher.flush_time();
    co_await flusher.flush_time();
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), 2 * nodes.size());

    // The option is live: raising it makes the last flush fresh.
    cache_time_in_ms.set(one_minute_in_ms);
    co_await flusher.flush_time();
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), 2 * nodes.size());
}

SEASTAR_TEST_CASE(test_caller_joining_a_round_that_fails_gets_no_time) {
    auto nodes = make_nodes(2);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    auto stop = deferred_stop(flusher);
    std::unordered_map<locator::host_id, promise<gc_clock::time_point>> pending;
    cluster.on_flush = [&] (locator::host_id node, abort_source&) { return pending[node].get_future(); };

    auto starter = flusher.flush_time();
    auto joiner = flusher.flush_time();
    BOOST_REQUIRE_EQUAL(cluster.total_flushes(), nodes.size());

    pending[nodes[0]].set_value(gc_clock::now());
    pending[nodes[1]].set_exception(std::runtime_error("no reply"));
    auto starter_time = co_await std::move(starter);
    auto joiner_time = co_await std::move(joiner);
    BOOST_REQUIRE(!starter_time.has_value());
    BOOST_REQUIRE(!joiner_time.has_value());
}

SEASTAR_TEST_CASE(test_stop_waits_for_the_round_in_flight) {
    auto nodes = make_nodes(2);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    std::unordered_map<locator::host_id, promise<gc_clock::time_point>> pending;
    cluster.on_flush = [&] (locator::host_id node, abort_source&) { return pending[node].get_future(); };

    auto caller = flusher.flush_time();
    auto stopped = flusher.stop();
    BOOST_REQUIRE(!stopped.available());

    auto flushed_at = gc_clock::now();
    for (auto& [node, p] : pending) {
        p.set_value(flushed_at);
    }
    co_await std::move(stopped);
    auto time = co_await std::move(caller);
    BOOST_REQUIRE(time.has_value());
    BOOST_REQUIRE(*time == flushed_at);
}

SEASTAR_TEST_CASE(test_abort_ends_the_round_before_the_nodes_answer) {
    auto nodes = make_nodes(2);
    auto owned = make_cluster(nodes);
    auto& cluster = *owned;
    abort_source as;
    hints_batchlog_flusher flusher(std::move(owned), utils::updateable_value<uint32_t>(one_minute_in_ms), as);
    // A node that never answers; the abort source is the only way out.
    cluster.on_flush = [] (locator::host_id, abort_source& as) {
        return sleep_abortable(std::chrono::hours(1), as).then([] { return gc_clock::now(); });
    };

    auto caller = flusher.flush_time();
    as.request_abort();
    auto stopped = flusher.stop();
    auto result = co_await coroutine::as_future(std::move(caller));
    BOOST_REQUIRE_THROW(result.get(), abort_requested_exception);
    co_await std::move(stopped);
}

BOOST_AUTO_TEST_SUITE_END()
