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
// Reads CLOCK_MONOTONIC_RAW, which runs free of NTP adjustments and adjtime(3).
// Its rate error is therefore that of the raw oscillator plus the kernel's boot
// calibration: order 100-200 ppm, bounded at all times.
//
// That bound is the property LeaseGuard needs. monotonic_now() bounds elapsed
// time exactly when the wall clock is unusable, including while a daemon
// reacquires sync and corrects a large offset, and the caller's safety margin is
// sized against 100-200 ppm.
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
