/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//
// Unit tests for service::bounded_clock_adjtimex, the LeaseGuard bounded-clock
// backend that disciplines CLOCK_REALTIME (read via ntp_adjtime) into a
// monotonic, uncertainty-bounded timeline.
//
// interval_now() samples the real clocks and delegates the whole discipline to
// advance(mono, sample), which is deterministic and syscall-free. These tests
// drive advance() directly with synthetic (monotonic time, kernel reading)
// sequences, so they can exercise clock steps, unsync gaps, and slew convergence
// without depending on the host clock.
//

#define BOOST_TEST_MODULE bounded_clock_adjtimex
#include <boost/test/unit_test.hpp>

#include "service/raft/bounded_clock_adjtimex.hh"

#include <chrono>
#include <optional>
#include <random>

using namespace std::chrono_literals;
namespace ch = std::chrono;

using steady = ch::steady_clock;
// The clock the discipline reports in. raft::time_bounds is expressed in
// raft::lease_clock so the unit is pinned by the type
// (see raft/bounded_clock.hh).
using leaseclk = raft::lease_clock;
using sample = service::bounded_clock_adjtimex::ntp_sample;

namespace {

// Arbitrary, distinct base points for the two clocks. The monotonic and
// real-time timelines are independent, so the discipline keeps their epochs
// separate.
const steady::time_point mono0{ch::seconds{1'000'000}};
const leaseclk::time_point rt0{ch::seconds{2'000'000}};

// max_correction per call is elapsed >> slew_shift, i.e. elapsed / kSlewFactor.
// The worked numbers in the cases below assume a factor of 16.
constexpr int kSlewFactor = 1 << service::bounded_clock_adjtimex::slew_shift;
static_assert(kSlewFactor == 16);

// The center we report around is exactly the midpoint of [earliest, latest]
// (the interval is symmetric: earliest = center - error, latest = center +
// error). Recovering it lets tests assert the internal monotonic-center
// invariant through the public interface.
leaseclk::time_point center_of(const raft::time_bounds& b) {
    return b.earliest + (b.latest - b.earliest) / 2;
}

// A small driver that owns the clock and the current monotonic instant, so each
// step only needs the elapsed monotonic time and the kernel reading.
struct driver {
    service::bounded_clock_adjtimex clock;
    steady::time_point mono = mono0;

    std::optional<raft::time_bounds> synced(steady::duration dmono, leaseclk::time_point realtime, ch::microseconds maxerror) {
        mono += dmono;
        return clock.advance(mono, sample{realtime, maxerror});
    }

    std::optional<raft::time_bounds> unsynced(steady::duration dmono) {
        mono += dmono;
        return clock.advance(mono, std::nullopt);
    }
};

} // anonymous namespace

// monotonic_now() is independent of the discipline: it reports elapsed time, not
// absolute time, so it must keep advancing while the clock is unsynchronized --
// which is exactly when LeaseGuard needs it, to bound how long ago a leader was
// elected once interval_now() has stopped reporting anything.
//
// This reads the real clock (CLOCK_MONOTONIC_RAW), so the assertions are limited
// to what holds regardless of scheduling: it advances monotonically, and it keeps
// reporting while the discipline is unsynchronized.
BOOST_AUTO_TEST_CASE(monotonic_now_is_independent_of_sync_state) {
    driver d;

    const auto t0 = d.clock.monotonic_now();

    // Un-anchored and unsynchronized: interval_now() has nothing to say, but
    // elapsed time is still observable.
    BOOST_REQUIRE(!d.unsynced(1s));
    const auto t1 = d.clock.monotonic_now();
    BOOST_CHECK(t1 >= t0);

    // Anchored and synchronized.
    BOOST_REQUIRE(d.synced(1s, rt0, 1000us));
    const auto t2 = d.clock.monotonic_now();
    BOOST_CHECK(t2 >= t1);

    // Sync lost again after anchoring: still advancing, still monotonic.
    BOOST_REQUIRE(!d.unsynced(1s));
    BOOST_CHECK(d.clock.monotonic_now() >= t2);
}

// Case 1: the discipline stays un-anchored while unsynchronized, then anchors on
// the first synchronized reading and reports the kernel's own bound verbatim.
BOOST_AUTO_TEST_CASE(anchoring) {
    driver d;

    // No trusted reading yet: nothing to report, and we must not anchor on an
    // unsynchronized clock (otherwise the timeline would start from garbage).
    BOOST_CHECK(!d.unsynced(1s));
    BOOST_CHECK(!d.unsynced(1s));

    // First synchronized reading anchors the center on CLOCK_REALTIME and emits
    // exactly [realtime - maxerror, realtime + maxerror].
    const auto err = 1000us;
    const auto b = d.synced(1s, rt0, err);
    BOOST_REQUIRE(b);
    BOOST_CHECK(b->earliest == rt0 - err);
    BOOST_CHECK(b->latest == rt0 + err);
    BOOST_CHECK(center_of(*b) == rt0);
}

// Case 2: when CLOCK_REALTIME advances in lockstep with the monotonic clock (the
// steady state), the center tracks CLOCK_REALTIME exactly and the reported
// interval is precisely +/- maxerror.
BOOST_AUTO_TEST_CASE(steady_state_tracks_realtime) {
    driver d;
    const auto err = 500us;
    auto rt = rt0;
    d.synced(1s, rt, err); // anchor

    for (int i = 0; i < 10; ++i) {
        rt += 1s;
        const auto b = d.synced(1s, rt, err);
        BOOST_REQUIRE(b);
        BOOST_CHECK(center_of(*b) == rt);
        BOOST_CHECK(b->earliest == rt - err);
        BOOST_CHECK(b->latest == rt + err);
    }
}

// Case 3: a backward step of CLOCK_REALTIME must not move the center backward.
// Instead the error inflates to keep containing the kernel interval, so the
// emitted `latest` grows and the center stays monotonic.
BOOST_AUTO_TEST_CASE(backward_step_is_monotonic_and_contained) {
    driver d;
    const auto err = 1000us;
    auto rt = rt0;
    auto prev = d.synced(1s, rt, err); // anchor
    for (int i = 0; i < 5; ++i) { // reach steady state
        rt += 1s;
        prev = d.synced(1s, rt, err);
    }
    const auto prev_center = center_of(*prev);

    // CLOCK_REALTIME jumps back by 500ms while 1s of monotonic time elapses.
    const auto step = 500ms;
    rt = rt - step;
    const auto b = d.synced(1s, rt, err);
    BOOST_REQUIRE(b);

    // Center did not go backwards (it advanced by mono then slewed back by at
    // most elapsed/kSlewFactor).
    BOOST_CHECK(center_of(*b) >= prev_center);

    // The reported interval still contains the kernel interval [rt-err, rt+err].
    BOOST_CHECK(b->earliest <= rt - err);
    BOOST_CHECK(b->latest >= rt + err);

    // `latest` grew relative to the previous, tightly-bounded reading: the
    // uncertainty absorbed the step.
    BOOST_CHECK(b->latest > prev->latest);
}

// Case 4: a step is absorbed gradually. Each subsequent lockstep call slews the
// center towards CLOCK_REALTIME by at most elapsed/kSlewFactor, so the gap
// shrinks by that cap per call until it closes and the interval self-heals back
// to +/- maxerror.
BOOST_AUTO_TEST_CASE(slew_cap_and_convergence) {
    driver d;
    const auto err = 100us;
    // Use 1600ms elapsed so the cap is exactly 100ms and divisions stay exact.
    const auto dmono = 1600ms;
    const auto cap = ch::duration_cast<leaseclk::duration>(dmono) / kSlewFactor; // 100ms
    auto rt = rt0;
    d.synced(dmono, rt, err); // anchor
    rt += dmono;
    d.synced(dmono, rt, err); // one steady step; center == rt

    // Backward step of 500ms: realtime advances the normal 1600ms but then
    // leaps 500ms back, landing 500ms behind the lockstep position. Initial gap
    // is therefore step - cap after this call's first slew.
    const auto step = 500ms;
    rt += dmono - step;
    auto b = d.synced(dmono, rt, err);
    BOOST_REQUIRE(b);
    // gap = center - realtime (center is ahead after a backward step).
    auto gap = center_of(*b) - rt;
    BOOST_CHECK(gap == ch::duration_cast<leaseclk::duration>(step) - cap);

    // Feed lockstep steps: each closes the gap by exactly one cap.
    for (int i = 0; i < 4; ++i) {
        rt += dmono;
        b = d.synced(dmono, rt, err);
        BOOST_REQUIRE(b);
        const auto new_gap = center_of(*b) - rt;
        BOOST_CHECK(new_gap == gap - cap);
        gap = new_gap;
    }

    // Fully converged: center == realtime again and the interval is tight.
    BOOST_CHECK(center_of(*b) == rt);
    BOOST_CHECK(b->earliest == rt - err);
    BOOST_CHECK(b->latest == rt + err);
}

// Case 5: a forward step of CLOCK_REALTIME -- a leap-second deletion, or the gap
// left by a step that marked the clock unsynchronized -- is slewed like a
// backward one: the center stays monotonic, the error inflates to keep
// containing the kernel interval, and the gap closes by one cap per call.
BOOST_AUTO_TEST_CASE(forward_step_is_slewed) {
    driver d;
    const auto err = 100us;
    // Use 1600ms elapsed so the cap is exactly 100ms and divisions stay exact.
    const auto dmono = 1600ms;
    const auto cap = ch::duration_cast<leaseclk::duration>(dmono) / kSlewFactor; // 100ms
    auto rt = rt0;
    auto prev = d.synced(dmono, rt, err); // anchor
    BOOST_REQUIRE(prev);

    // CLOCK_REALTIME gains a second on top of the normal advance.
    rt += dmono + 1s;
    auto b = d.synced(dmono, rt, err);
    BOOST_REQUIRE(b);
    BOOST_CHECK(center_of(*b) >= center_of(*prev));
    BOOST_CHECK(b->earliest <= rt - err);
    BOOST_CHECK(b->latest >= rt + err);
    auto gap = rt - center_of(*b);
    BOOST_CHECK(gap == ch::duration_cast<leaseclk::duration>(1s) - cap);

    for (int i = 0; i < 9; ++i) {
        rt += dmono;
        b = d.synced(dmono, rt, err);
        BOOST_REQUIRE(b);
        BOOST_CHECK(b->earliest <= rt - err);
        BOOST_CHECK(b->latest >= rt + err);
        const auto new_gap = rt - center_of(*b);
        BOOST_CHECK(new_gap == gap - cap);
        gap = new_gap;
    }
    BOOST_CHECK(gap == leaseclk::duration::zero());
    BOOST_CHECK(b->earliest == rt - err);
    BOOST_CHECK(b->latest == rt + err);
}

// Case 6: across an unsynchronized gap the center keeps advancing on the
// monotonic clock, so on recovery the divergence reflects the size of any
// CLOCK_REALTIME step during the gap -- not the length of the outage.
BOOST_AUTO_TEST_CASE(unsync_gap_recovery_reflects_step_not_outage) {
    // Sub-case A: no step during the gap => seamless recovery, tight interval.
    {
        driver d;
        const auto err = 1000us;
        auto rt = rt0;
        d.synced(1s, rt, err); // anchor; center == rt

        // 3s of unsynchronized time: no readings, but the center advances.
        for (int i = 0; i < 3; ++i) {
            rt += 1s;
            BOOST_CHECK(!d.unsynced(1s));
        }

        // Recover after a further 1s. Total mono elapsed since anchor is 4s and
        // the true clock also advanced 4s (no step), so the center matches
        // CLOCK_REALTIME and the interval is tight again despite the long gap.
        rt += 1s;
        const auto b = d.synced(1s, rt, err);
        BOOST_REQUIRE(b);
        BOOST_CHECK(center_of(*b) == rt);
        BOOST_CHECK(b->earliest == rt - err);
        BOOST_CHECK(b->latest == rt + err);
    }

    // Sub-case B: a 500ms backward step happened during the gap. On recovery the
    // divergence is exactly the step size, not inflated by the 4s outage.
    {
        driver d;
        const auto err = 1000us;
        auto rt = rt0;
        d.synced(1s, rt, err); // anchor; center == rt

        for (int i = 0; i < 3; ++i) {
            rt += 1s;
            BOOST_CHECK(!d.unsynced(1s));
        }

        // 1s more mono elapses, and CLOCK_REALTIME comes back 500ms behind the
        // monotonic timeline: a 500ms backward step.
        rt += 1s;
        const auto stepped_rt = rt - 500ms;
        const auto b = d.synced(1s, stepped_rt, err);
        BOOST_REQUIRE(b);

        // Center is monotonic and lands near the step-free position (pulled back
        // by one slew cap), NOT dragged around by the 4s outage.
        BOOST_CHECK(center_of(*b) >= stepped_rt);
        BOOST_CHECK(b->earliest <= stepped_rt - err);
        BOOST_CHECK(b->latest >= stepped_rt + err);
        // The uncertainty inflation reflects the ~500ms step (minus one slew
        // cap), independent of the 4s outage: on the order of the step, well
        // under a second.
        const auto infl = b->latest - center_of(*b);
        BOOST_CHECK(infl < ch::duration_cast<leaseclk::duration>(1s));
        BOOST_CHECK(infl > ch::duration_cast<leaseclk::duration>(400ms));
    }
}

// Case 7: a divergence that is not a whole number of microseconds must still be
// contained. The slew cap makes such residuals routine: max_correction is
// elapsed / kSlewFactor, so an elapsed of 1ms yields 62.5us, already not
// microsecond-aligned. The divergence is an absolute value, and the reported
// error carries it at nanosecond resolution, which is what keeps the advertised
// interval covering the kernel's.
//
// Worked example (forward step below): the center advances 1ms to rt0 + 1ms, the
// 1ms correction is capped to 1ms / 16 = 62'500ns, leaving a divergence of
// 937'500ns -- 937.5us. The trailing 500ns is the margin this case pins.
BOOST_AUTO_TEST_CASE(sub_microsecond_divergence_is_contained) {
    const auto err = 1000us;

    // Forward step: the shortfall lands on `latest`.
    {
        driver d;
        BOOST_REQUIRE(d.synced(1ms, rt0, err)); // anchor: center == rt0
        // On top of the normal 1ms advance, leap an extra 1ms ahead. The
        // correction (1ms) far exceeds the cap (62.5us), so the center is left
        // short by a fractional-microsecond divergence.
        const auto rt = rt0 + 2ms;
        const auto b = d.synced(1ms, rt, err);
        BOOST_REQUIRE(b);

        // Pin that the residual really is sub-microsecond, so this case exercises
        // the fractional-divergence path.
        const auto divergence = rt - center_of(*b);
        BOOST_REQUIRE(divergence % ch::duration_cast<leaseclk::duration>(1us) != leaseclk::duration::zero());

        BOOST_CHECK(b->latest >= rt + err);
        BOOST_CHECK(b->earliest <= rt - err);
    }

    // Backward step: the mirror image, with the shortfall on `earliest`.
    {
        driver d;
        BOOST_REQUIRE(d.synced(1ms, rt0, err)); // anchor: center == rt0
        // Drop 1ms below where the center advances to, so the center now leads
        // the kernel clock by a fractional-microsecond divergence.
        const auto rt = rt0;
        const auto b = d.synced(1ms, rt, err);
        BOOST_REQUIRE(b);

        const auto divergence = center_of(*b) - rt;
        BOOST_REQUIRE(divergence % ch::duration_cast<leaseclk::duration>(1us) != leaseclk::duration::zero());

        BOOST_CHECK(b->earliest <= rt - err);
        BOOST_CHECK(b->latest >= rt + err);
    }
}

// Case 8: randomized invariant test. For an arbitrary sequence of monotonic
// advances, sync/unsync flips, CLOCK_REALTIME jumps and maxerror values, the
// discipline must always:
//   (a) keep the emitted center monotonically non-decreasing, and
//   (b) contain the kernel interval [realtime - maxerror, realtime + maxerror].
// Both hold by construction regardless of the input, so this is a strong
// property test.
BOOST_AUTO_TEST_CASE(randomized_monotonic_and_contained) {
    std::mt19937_64 rng{0xC10CB0'2026ull};
    std::uniform_int_distribution<int64_t> dmono_us{1'000, 2'000'000};   // 1ms..2s
    std::uniform_int_distribution<int64_t> err_us{100, 50'000};          // 100us..50ms
    std::uniform_int_distribution<int64_t> jump_us{-1'000'000, 1'000'000}; // +/-1s step
    std::bernoulli_distribution unsynced_p{0.15};
    std::bernoulli_distribution jump_p{0.1};

    driver d;
    // A model of "true" CLOCK_REALTIME that mostly advances with the monotonic
    // clock but occasionally steps. Its absolute relation to the center is
    // irrelevant to the invariants below.
    auto rt = rt0;
    // Anchor first so the discipline is initialized.
    const auto anchor = d.synced(1ms, rt, 1000us);
    BOOST_REQUIRE(anchor);
    auto last_center = center_of(*anchor);

    for (int i = 0; i < 5000; ++i) {
        const auto dmono = ch::microseconds{dmono_us(rng)};
        // Advance the modelled true clock by the elapsed time, plus an occasional
        // step.
        rt += ch::duration_cast<leaseclk::duration>(dmono);
        if (jump_p(rng)) {
            rt += ch::microseconds{jump_us(rng)};
        }

        if (unsynced_p(rng)) {
            BOOST_REQUIRE(!d.unsynced(dmono));
            continue;
        }

        const auto err = ch::microseconds{err_us(rng)};
        const auto b = d.synced(dmono, rt, err);
        BOOST_REQUIRE(b);

        // (b) Containment of the kernel interval. Exact: the reported error is
        // computed in lease_clock::duration (nanoseconds), so no rounding may
        // shave anything off either endpoint.
        BOOST_REQUIRE(b->earliest <= rt - err);
        BOOST_REQUIRE(b->latest >= rt + err);
        // earliest strictly precedes latest.
        BOOST_REQUIRE(b->earliest < b->latest);

        // (a) The center is monotonically non-decreasing.
        const auto c = center_of(*b);
        BOOST_REQUIRE(c >= last_center);
        last_center = c;
    }
}
