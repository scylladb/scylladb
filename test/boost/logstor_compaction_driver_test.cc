/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "replica/logstor/compaction.hh"
#include "replica/logstor/compaction_driver.hh"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <vector>

using namespace replica::logstor;
using namespace std::chrono_literals;

namespace {

constexpr uint64_t segment_size = 128 * 1024;
// A space floor of 32 segments: 4 jobs in parallel, of up to 8 segments each.
constexpr uint64_t segment_count = 1024;

// Rich and poor are far apart, so the gate refuses the poor batch next to the rich one.
const compaction_candidate_score rich{.n_in = 8, .n_out = 2, .live_bytes = 2 * segment_size};
const compaction_candidate_score poor{.n_in = 8, .n_out = 7, .live_bytes = 7 * segment_size};

class fake_group final : public logstor_group {
public:
    fake_group() : logstor_group(segment_size, nullptr) {}

    ::table_id table_id() const noexcept override {
        return {};
    }
    primary_index& logstor_index() noexcept override {
        std::abort();
    }
    const primary_index& logstor_index() const noexcept override {
        std::abort();
    }

protected:
    compaction_manager& logstor_compaction_manager() noexcept override {
        std::abort();
    }
};

struct submitted_job {
    logstor_group* group;
    compaction_batch_limits limits;
    promise<> done;
    bool ended = false;
};

// Ranks from a script and runs each job until the test ends it.
class fake_host final : public compaction_driver::host {
public:
    // The rankings to return, in order, each one best first. Without one, a ranking is empty.
    std::deque<std::vector<compaction_driver::candidate>> rankings;
    // While set, a ranking waits for it.
    std::optional<promise<>> ranking_gate;
    // The limits of each ranking that started.
    std::vector<compaction_batch_limits> ranking_limits;
    std::vector<std::unique_ptr<submitted_job>> jobs;
    std::set<const logstor_group*> running;
    uint64_t available_segments = segment_count;
    utilization_histogram utilization{};
    uint64_t dead_segments = 0;

    bool can_submit_compaction() const noexcept override {
        return true;
    }

    future<compaction_driver::ranking> find_top_compaction_candidates(const compaction_batch_limits& limits, size_t) override {
        ranking_limits.push_back(limits);
        if (ranking_gate) {
            auto gate = ranking_gate->get_future();
            co_await std::move(gate);
        }
        std::vector<compaction_driver::candidate> candidates;
        if (!rankings.empty()) {
            candidates = std::move(rankings.front());
            rankings.pop_front();
        }
        std::erase_if(candidates, [this] (const compaction_driver::candidate& c) { return running.contains(c.group); });
        std::ranges::reverse(candidates);
        co_return compaction_driver::ranking{.candidates = std::move(candidates)};
    }

    bool can_start(const logstor_group* group) const noexcept override {
        return !running.contains(group);
    }

    future<> submit_normal_compaction(logstor_group& group, compaction_batch_limits limits) override {
        running.insert(&group);
        auto& job = *jobs.emplace_back(std::make_unique<submitted_job>(&group, limits, promise<>()));
        co_await job.done.get_future();
        running.erase(&group);
    }

    compaction_driver::shard_sample sample() const noexcept override {
        return compaction_driver::shard_sample{
            .available_segments = available_segments,
            .utilization = utilization,
            .dead_segments = dead_segments,
        };
    }

    void open_ranking_gate() {
        auto gate = std::exchange(ranking_gate, std::nullopt);
        gate->set_value();
    }
};

compaction_driver::candidate candidate_of(fake_group& group, const compaction_candidate_score& score) {
    return compaction_driver::candidate{.group = &group, .score = score};
}

// A threshold of a half, and a minimum at the threshold, so that the curve is flat above the anchor
// and only the free level below the anchor moves the threshold.
compaction_driver::config make_config(utils::updateable_value<double> threshold = utils::updateable_value<double>(0.5)) {
    return compaction_driver::config{
        .space_floor = utils::updateable_value<double>(1.0 / 32),
        .utilization_threshold = threshold,
        .threshold_anchor = utils::updateable_value<double>(0),
        .threshold_ceiling = utils::updateable_value<double>(0),
        .min_utilization_threshold = std::move(threshold),
        .sg = default_scheduling_group(),
    };
}

void start(compaction_driver& driver) {
    driver.start({.segment_count = segment_count, .segment_size = segment_size});
}

// Polls `pred` until it holds or `limit` passes. The limits are well under the 10 s fallback wake
// of the driver, so a driver that depends on the fallback fails.
bool wait_until(std::function<bool()> pred, std::chrono::milliseconds limit = 2s) {
    const auto deadline = lowres_clock::now() + limit;
    while (!pred()) {
        if (lowres_clock::now() >= deadline) {
            return false;
        }
        seastar::sleep(10ms).get();
    }
    return true;
}

void end_job(fake_host& host, size_t i) {
    host.jobs[i]->ended = true;
    host.jobs[i]->done.set_value();
}

void stop(compaction_driver& driver, fake_host& host) {
    driver.request_stop();
    if (host.ranking_gate) {
        host.open_ranking_gate();
    }
    // The jobs that the driver submitted are aborted by the compaction manager, which ends them.
    for (size_t i = 0; i < host.jobs.size(); ++i) {
        if (!host.jobs[i]->ended) {
            end_job(host, i);
        }
    }
    driver.stop().get();
}

} // namespace

// The gate refuses a poor batch next to a rich job. It waits for one job to end, not for all of
// them, and the poor batch starts when nothing better runs.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_refuses_a_marginal_batch) {
    fake_host host;
    fake_group a, b, c;
    host.rankings = {
        {candidate_of(a, rich), candidate_of(c, rich), candidate_of(b, poor)},
        {candidate_of(b, poor)},
        {candidate_of(b, poor)},
    };
    compaction_driver driver(host, make_config());
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return driver.get_stats().batches_refused == 1; }));
    BOOST_REQUIRE_EQUAL(host.jobs.size(), 2u);
    BOOST_REQUIRE_EQUAL(host.jobs[0]->group, &a);
    BOOST_REQUIRE_EQUAL(host.jobs[1]->group, &c);

    // One rich job ends and the driver ranks again, but the other rich job still sets the bar.
    end_job(host, 1);
    BOOST_REQUIRE(wait_until([&] { return driver.get_stats().batches_refused == 2; }));
    BOOST_REQUIRE_EQUAL(host.ranking_limits.size(), 2u);
    BOOST_REQUIRE_EQUAL(host.jobs.size(), 2u);

    end_job(host, 0);
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 3; }));
    BOOST_REQUIRE_EQUAL(host.jobs[2]->group, &b);
    BOOST_REQUIRE_EQUAL(driver.get_stats().batches_refused, 2u);

    stop(driver, host);
}

// The ranking skips a group that is compacting, so a ranking made while the rich job runs has only
// the poor batch. The job in flight still sets the bar.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_bar_survives_a_new_ranking) {
    fake_host host;
    fake_group a, b;
    host.rankings = {
        {candidate_of(a, rich)},
        {candidate_of(b, poor)},
        {candidate_of(b, poor)},
    };
    compaction_driver driver(host, make_config());
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return driver.get_stats().batches_refused == 1; }));
    BOOST_REQUIRE_EQUAL(host.jobs.size(), 1u);

    // With nothing in flight the best candidate always starts, however poor.
    end_job(host, 0);
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 2; }));
    BOOST_REQUIRE_EQUAL(host.jobs[1]->group, &b);

    stop(driver, host);
}

// A tick that sees the backlog change while a ranking runs wakes the driver after an empty
// ranking.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_wakes_on_a_change_during_a_ranking) {
    fake_host host;
    fake_group a;
    host.rankings = {{}, {candidate_of(a, rich)}};
    host.ranking_gate.emplace();
    compaction_driver driver(host, make_config());
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 1; }));
    host.utilization[0] = 5;
    seastar::sleep(2 * compaction_rate_control_period).get();
    host.open_ranking_gate();

    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 1; }));

    stop(driver, host);
}

// A segment that dies in the lowest bucket does not change the backlog estimate, but it can be
// compacted at once, so it wakes the driver.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_wakes_when_a_segment_dies) {
    fake_host host;
    fake_group a;
    host.rankings = {{}, {candidate_of(a, rich)}};
    host.utilization[0] = 1;
    compaction_driver driver(host, make_config());
    start(driver);

    seastar::sleep(2 * compaction_rate_control_period).get();
    BOOST_REQUIRE(host.jobs.empty());

    host.dead_segments = 1;
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 1; }));

    stop(driver, host);
}

// The end of a job, and notify_group_idle(), wake a driver that found nothing to compact.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_wakes_when_a_group_is_idle) {
    fake_host host;
    fake_group a, b, c;
    // Each job is followed by a ranking that comes back empty.
    host.rankings = {{candidate_of(a, rich)}, {}, {candidate_of(b, rich)}, {}, {candidate_of(c, rich)}};
    compaction_driver driver(host, make_config());
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 1; }));
    end_job(host, 0);
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 2; }));
    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 4; }));
    driver.notify_group_idle();
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 3; }));

    stop(driver, host);
}

// A notify_group_idle() while a ranking runs is not lost when the ranking comes back empty.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_wakes_when_a_group_is_idle_during_a_ranking) {
    fake_host host;
    fake_group a;
    host.rankings = {{}, {candidate_of(a, rich)}};
    host.ranking_gate.emplace();
    compaction_driver driver(host, make_config());
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 1; }));
    driver.notify_group_idle();
    host.open_ranking_gate();
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 1; }));

    stop(driver, host);
}

// A job selects its batch by the limits of its ranking, also when a tick changes the limits while
// the ranking runs.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_job_runs_under_its_ranking_limits) {
    fake_host host;
    fake_group a;
    host.available_segments = 500;
    host.rankings = {{candidate_of(a, rich)}};
    host.ranking_gate.emplace();
    compaction_driver driver(host, make_config());
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 1; }));
    // Between the space floor (32) and the anchor (64): the threshold and the minimum batch move.
    host.available_segments = 40;
    seastar::sleep(2 * compaction_rate_control_period).get();
    const auto ranked = host.ranking_limits[0];
    const auto now = driver.automatic_batch_limits();
    BOOST_REQUIRE_NE(ranked.max_utilization, now.max_utilization);
    BOOST_REQUIRE_NE(ranked.min_batch, now.min_batch);
    host.open_ranking_gate();

    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 1; }));
    const auto& used = host.jobs[0]->limits;
    BOOST_REQUIRE_EQUAL(used.batch_cap, ranked.batch_cap);
    BOOST_REQUIRE_EQUAL(used.min_batch, ranked.min_batch);
    BOOST_REQUIRE_EQUAL(used.max_utilization, ranked.max_utilization);

    stop(driver, host);
}

// A change of the space floor changes the parallelism at once. The jobs in flight continue, and a
// new job starts only when fewer jobs than the new parallelism run.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_space_floor_update) {
    fake_host host;
    fake_group a, b, c, d, e, f;
    host.rankings = {
        {candidate_of(a, rich), candidate_of(b, rich), candidate_of(c, rich), candidate_of(d, rich)},
        {candidate_of(e, rich), candidate_of(f, rich)},
    };
    utils::updateable_value_source<double> space_floor(1.0 / 32);
    auto cfg = make_config();
    cfg.space_floor = utils::updateable_value<double>(space_floor);
    compaction_driver driver(host, std::move(cfg));
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 4; }));

    // 16 segments: 2 jobs of up to 8 segments.
    space_floor.set(1.0 / 64);
    end_job(host, 0);
    end_job(host, 1);
    seastar::sleep(50ms).get();
    BOOST_REQUIRE_EQUAL(host.jobs.size(), 4u);

    end_job(host, 2);
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 5; }));
    BOOST_REQUIRE_EQUAL(host.jobs[4]->group, &e);
    seastar::sleep(50ms).get();
    BOOST_REQUIRE_EQUAL(host.jobs.size(), 5u);

    space_floor.set(1.0 / 32);
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 6; }));
    BOOST_REQUIRE_EQUAL(host.jobs[5]->group, &f);

    stop(driver, host);
}

// A smaller space floor can take the slots of the jobs in flight, which leaves the semaphore below
// zero. A batch that the gate refuses then still waits for a job to end, and a stop ends the wait.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_refusal_after_the_space_floor_decreases) {
    fake_host host;
    fake_group a, b, c, d;
    host.rankings = {
        {candidate_of(a, rich), candidate_of(b, rich), candidate_of(c, rich)},
        {candidate_of(d, poor)},
    };
    host.ranking_gate.emplace();
    utils::updateable_value_source<double> space_floor(1.0 / 32);
    auto cfg = make_config();
    cfg.space_floor = utils::updateable_value<double>(space_floor);
    compaction_driver driver(host, std::move(cfg));
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 1; }));
    host.open_ranking_gate();
    host.ranking_gate.emplace();
    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 2; }));
    BOOST_REQUIRE_EQUAL(host.jobs.size(), 3u);

    // 2 jobs: with the slot that the driver holds for the ranking, 2 slots fewer than in use.
    space_floor.set(1.0 / 64);
    host.open_ranking_gate();
    BOOST_REQUIRE(wait_until([&] { return driver.get_stats().batches_refused == 1; }));
    seastar::sleep(50ms).get();
    BOOST_REQUIRE_EQUAL(host.ranking_limits.size(), 2u);

    driver.request_stop();
    for (size_t i = 0; i < host.jobs.size(); ++i) {
        end_job(host, i);
    }
    seastar::with_timeout(lowres_clock::now() + 5s, driver.stop()).get();
}

// A stop that comes while a ranking runs starts no job from that ranking.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_stop_during_a_ranking) {
    fake_host host;
    fake_group a;
    host.rankings = {{candidate_of(a, rich)}};
    host.ranking_gate.emplace();
    compaction_driver driver(host, make_config());
    start(driver);

    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 1; }));
    driver.request_stop();
    host.open_ranking_gate();
    seastar::sleep(50ms).get();
    BOOST_REQUIRE(host.jobs.empty());

    stop(driver, host);
}

// A threshold of 1 disables automatic compaction, and setting a usable threshold enables it.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_disabled_threshold) {
    fake_host host;
    utils::updateable_value_source<double> threshold(1);
    compaction_driver driver(host, make_config(utils::updateable_value<double>(threshold)));
    start(driver);

    seastar::sleep(3 * compaction_rate_control_period).get();
    BOOST_REQUIRE(host.ranking_limits.empty());

    threshold.set(0.5);
    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 1; }));

    stop(driver, host);
}

// A threshold that disables compaction while a ranking runs starts no job from that ranking.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_driver_disabled_during_a_ranking) {
    fake_host host;
    fake_group a, b;
    host.rankings = {{candidate_of(a, rich)}, {candidate_of(b, rich)}};
    host.ranking_gate.emplace();
    utils::updateable_value_source<double> threshold(0.5);
    compaction_driver driver(host, make_config(utils::updateable_value<double>(threshold)));
    start(driver);
    // A job that starts by mistake must not leave the driver running when the test fails.
    auto stop_driver = seastar::defer([&] noexcept { stop(driver, host); });

    BOOST_REQUIRE(wait_until([&] { return host.ranking_limits.size() == 1; }));
    threshold.set(1);
    seastar::sleep(2 * compaction_rate_control_period).get();
    host.open_ranking_gate();
    seastar::sleep(50ms).get();
    BOOST_REQUIRE(host.jobs.empty());

    threshold.set(0.5);
    BOOST_REQUIRE(wait_until([&] { return host.jobs.size() == 1; }));
    BOOST_REQUIRE_EQUAL(host.jobs[0]->group, &b);
}
