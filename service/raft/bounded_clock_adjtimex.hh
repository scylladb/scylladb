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
// (STA_UNSYNC / TIME_ERROR), so callers fall back to the safe path.
//
// This lives outside the raft library because it is platform-specific env glue,
// like the other concrete raft integrations in service/raft/.
//
// Monotonicity
// ------------
// LeaseGuard compares a time interval recorded in the past (stored in a
// committed log entry's lease_time) against a fresh interval_now() reading. That
// comparison is only sound if the emitted timeline is monotonic: a reading that
// went *backwards* relative to an already-recorded stamp would make a leaseholder
// under-estimate how much time has elapsed and serve a stale read.
//
// CLOCK_REALTIME is NOT monotonic: NTP corrections, settimeofday(), and leap
// seconds can step it, forwards or backwards. Feeding raw CLOCK_REALTIME to
// LeaseGuard would therefore be unsafe. Instead, this class runs a small clock
// discipline (see the .cc) that derives a *monotonic* "center" from
// CLOCK_MONOTONIC and slews it towards CLOCK_REALTIME at a bounded rate, widening
// the reported uncertainty to cover any divergence. A step never moves the center
// backwards; it only inflates the error, which then shrinks as the center
// re-converges. The result is monotonic AND always contains the true time, so
// both of LeaseGuard's comparisons stay conservative across clock steps.
//
// The instance is stateful and holds the discipline state below. It is meant to
// be used per shard (one physical clock per node); interval_now() is not
// thread-safe, matching Seastar's single-threaded-per-shard model.
class bounded_clock_adjtimex final : public raft::bounded_clock {
    // False until the first synchronized reading anchors the discipline.
    bool _initialized = false;
    // CLOCK_MONOTONIC (via steady_clock) at the previous call. Used to advance
    // the center by the true elapsed physical time between calls. CLOCK_MONOTONIC
    // never steps, so this advance is always forward.
    std::chrono::steady_clock::time_point _mono_last{};
    // The monotonic virtual real-time "center" we report around. Only ever moves
    // forward (see the .cc for the proof), which is what makes the emitted
    // timeline safe to compare against previously recorded stamps.
    std::chrono::system_clock::time_point _center{};

    // Maximum fraction of the elapsed interval by which the center may be slewed
    // towards CLOCK_REALTIME on a single call, expressed as a right shift:
    // max_correction = elapsed >> slew_shift, i.e. ~1/16 (6.25%). It must be
    // strictly less than 1 (shift >= 1) so the center stays monotonic; the value
    // trades convergence speed after a step (~2^slew_shift * step_size of
    // wall-clock time to fully absorb a step) against sensitivity to transient
    // real-time jitter.
    static constexpr int slew_shift = 4;

public:
    // A synchronized reading of the kernel clock, as decoded from ntp_adjtime():
    // the CLOCK_REALTIME timestamp and the kernel's maximum-error bound sampled
    // at the same instant. Used as the input to advance(); an unsynchronized
    // clock is represented by std::nullopt instead.
    struct ntp_sample {
        std::chrono::system_clock::time_point realtime;
        std::chrono::microseconds maxerror;
    };

    std::optional<raft::time_bounds> interval_now() override;

    // The deterministic, syscall-free core of interval_now(): runs the clock
    // discipline for one reading and returns the bounded-time interval (or
    // nullopt while unsynchronized). `mono` is a CLOCK_MONOTONIC sample and
    // `sample` is the kernel reading (std::nullopt == unsynchronized). Exposed
    // so unit tests can drive the discipline with synthetic time sequences
    // instead of live syscalls; interval_now() is the thin glue that samples the
    // real clocks and calls this.
    std::optional<raft::time_bounds> advance(
            std::chrono::steady_clock::time_point mono,
            std::optional<ntp_sample> sample);
};

} // namespace service
