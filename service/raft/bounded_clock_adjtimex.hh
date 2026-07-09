/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/bounded_clock.hh"

#include <chrono>

namespace service {

// Concrete raft::bounded_clock backend that reads the local clock's error
// bounds from the Linux kernel's NTP discipline via ntp_adjtime(2)/adjtimex(2).
// Requires no external service at runtime beyond an NTP daemon (chrony/ntpd)
// disciplining the kernel clock; the error bound is read with a single
// non-blocking syscall. Returns nullopt while the clock is unsynchronized
// (TIME_ERROR), so callers fall back to the safe path. The daemon must apply
// leap seconds through the kernel or by a step, and all nodes must use the same
// kind of time sources, all leap-smearing or none (see the .cc).
//
// This lives outside the raft library because it is platform-specific env glue,
// like the other concrete raft integrations in service/raft/.
//
// Monotonicity
// ------------
// LeaseGuard compares a time interval recorded in the past (stored in a
// committed log entry's lease_time) against a fresh interval_now() reading. That
// comparison is sound because the emitted timeline is monotonic: the age it
// computes for a recorded stamp is at least the true age, which keeps
// lease-validity checks conservative.
//
// CLOCK_REALTIME is not monotonic. A step made through settimeofday() or
// adjtimex(ADJ_SETOFFSET) marks the clock unsynchronized, so no interval is
// reported until the NTP daemon re-synchronizes it. A kernel leap second steps
// it while it stays synchronized with maxerror unchanged: back one second for
// an insertion, forward for a deletion. This class therefore runs a small clock
// discipline (see the .cc) that derives a monotonic "center" from
// CLOCK_MONOTONIC. The center slews towards CLOCK_REALTIME at a bounded rate in
// either direction, widening the reported uncertainty to cover the
// divergence. The result is monotonic and contains the
// true time while maxerror bounds the kernel clock's error, so both of
// LeaseGuard's comparisons stay conservative across a leap second.
//
// The instance is stateful and holds the discipline state below. It is meant to
// be used per shard (one physical clock per node); interval_now() is not
// thread-safe, matching Seastar's single-threaded-per-shard model.
class bounded_clock_adjtimex final : public raft::bounded_clock {
    // False until the first synchronized reading anchors the discipline.
    bool _initialized = false;
    // CLOCK_MONOTONIC (via steady_clock) at the previous call. Used to advance
    // the center by the true elapsed physical time between calls. CLOCK_MONOTONIC
    // advances monotonically, so this advance is always forward.
    std::chrono::steady_clock::time_point _mono_last{};
    // The monotonic virtual real-time "center" we report around. Only ever moves
    // forward (see the .cc for the proof), which is what makes the emitted
    // timeline safe to compare against previously recorded stamps.
    raft::lease_clock::time_point _center{};

public:
    // Maximum fraction of the elapsed interval by which the center may be slewed
    // towards CLOCK_REALTIME on a single call, expressed as a right shift:
    // max_correction = elapsed >> slew_shift, i.e. ~1/16 (6.25%). It must be
    // strictly less than 1 (shift >= 1) so the center stays monotonic; the value
    // trades convergence speed after a slewed step (~2^slew_shift * step_size of
    // wall-clock time to fully absorb it) against sensitivity to transient
    // real-time jitter.
    static constexpr int slew_shift = 4;

    // A synchronized reading of the kernel clock, as decoded from ntp_adjtime():
    // the CLOCK_REALTIME timestamp and the kernel's maximum-error bound sampled
    // at the same instant. Used as the input to advance(), where std::nullopt
    // represents an unsynchronized clock.
    struct ntp_sample {
        raft::lease_clock::time_point realtime;
        std::chrono::microseconds maxerror;
    };

    std::optional<raft::time_bounds> interval_now() override;

    // Elapsed physical time, read straight from CLOCK_MONOTONIC_RAW. It stays
    // usable while the clock is unsynchronized, which is exactly when LeaseGuard
    // needs it: interval_now() reports nothing in that state. See
    // service/raft/monotonic_raw_clock.hh for the rate bound it relies on.
    raft::mono_clock::time_point monotonic_now() override;

    // The deterministic, syscall-free core of interval_now(): runs the clock
    // discipline for one reading and returns the bounded-time interval (or
    // nullopt while unsynchronized). `mono` is a CLOCK_MONOTONIC sample and
    // `sample` is the kernel reading (std::nullopt == unsynchronized). Exposed
    // so unit tests can drive the discipline with synthetic time sequences;
    // interval_now() is the thin glue that samples the real clocks and calls
    // this.
    std::optional<raft::time_bounds> advance(
            std::chrono::steady_clock::time_point mono,
            std::optional<ntp_sample> sample);
};

} // namespace service
