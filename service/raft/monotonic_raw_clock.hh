/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/bounded_clock.hh"

#include <chrono>
#include <ctime>

namespace service {

// raft::bounded_clock::monotonic_now() for the Linux backends.
//
// CLOCK_MONOTONIC_RAW, deliberately, rather than std::chrono::steady_clock.
// steady_clock is CLOCK_MONOTONIC, which is immune to clock *steps* but not to
// NTP *slewing*: a daemon correcting a large offset adjusts the timekeeping rate,
// and the amounts involved are not small -- chrony's default maxslewrate is
// 83333.333 ppm (8.33%), and the kernel's tick adjustment permits roughly +/-10%
// on top of the +/-500 ppm freq clamp.
//
// That would be the wrong clock precisely where this one is used. LeaseGuard
// calls monotonic_now() to bound elapsed time exactly when the wall clock is
// unusable -- and a clock that was unsynchronized and is now reacquiring sync
// with a large offset is the case where CLOCK_MONOTONIC runs fastest, for as
// long as the correction takes. The caller's safety margin cannot be expected to
// absorb 10%.
//
// CLOCK_MONOTONIC_RAW is documented as not subject to NTP adjustments or to
// adjtime(3), so its rate error is that of the raw oscillator plus the kernel's
// boot calibration: order 100-200 ppm. The trade-off is deliberate -- a
// disciplined CLOCK_MONOTONIC has better *typical* rate accuracy, since NTP is
// correcting the crystal's systematic error, but only the raw clock has a
// *bounded worst case*, and a safety property needs the bound.
//
// It is in the vDSO (since Linux 4.12), so this is not a syscall.
inline raft::mono_clock::time_point monotonic_raw_now() noexcept {
    struct timespec ts = {};
    // Cannot fail for a supported clock id with a valid pointer.
    ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return raft::mono_clock::time_point(
            std::chrono::seconds(ts.tv_sec) + std::chrono::nanoseconds(ts.tv_nsec));
}

} // namespace service
