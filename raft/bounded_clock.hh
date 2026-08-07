/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <ratio>
#include <type_traits>

namespace raft {

// The clock LeaseGuard time intervals are expressed in.
//
// This is a distinct clock type rather than std::chrono::system_clock because
// these time points are persisted in the raft log and shipped between nodes,
// and system_clock's period is implementation-defined (nanoseconds on
// libstdc++, microseconds on libc++). The generic serializer writes a
// time_point as the raw count of its duration with no unit tag, so a raw
// system_clock::time_point would encode differently depending on the standard
// library it was built against -- and a reader that decoded a nanosecond count
// as microseconds would see a lease 1000x younger than it is and serve a stale
// local read. Pinning rep and period here makes the wire format a property of
// the type instead of the toolchain.
//
// Nanoseconds is exact for every clock source we have (ClockBound reports a
// timespec, adjtimex reports microseconds), so no source has to decide which
// way to round; int64 nanoseconds spans +/-292 years around the epoch.
class lease_clock final {
public:
    using base = std::chrono::system_clock;
    using rep = int64_t;
    using period = std::nano;
    using duration = std::chrono::duration<rep, period>;
    using time_point = std::chrono::time_point<lease_clock, duration>;

    static constexpr bool is_steady = base::is_steady;

    static time_point now() noexcept {
        return time_point(std::chrono::duration_cast<duration>(base::now().time_since_epoch()));
    }
};

// The wire format of a lease interval. Changing either of these changes how
// every persisted log entry's lease_time is interpreted, so they are pinned
// here rather than left to the standard library. Note that a serialization
// round-trip test cannot catch a regression in this: both ends of a round trip
// share a standard library, which is exactly the assumption being removed.
static_assert(std::is_same_v<lease_clock::period, std::nano>,
        "raft::lease_clock::period is the on-disk unit of log_entry::lease_time");
static_assert(sizeof(lease_clock::rep) == 8,
        "raft::lease_clock::rep is the on-disk width of log_entry::lease_time");

// A reading of a bounded-uncertainty (physical) clock.
//
// A call to bounded_clock::interval_now() returns [earliest, latest] such that
// the true time was somewhere in this interval for at least a moment between
// the call's invocation and completion. This is the primitive used by
// LeaseGuard (see raft/README.md) to decide whether a time recorded on another
// node is now "more than delta old" or "less than delta old", without relying
// on synchronized wall-clock reads.
//
// Unlike raft::logical_clock (which counts ticks for election/heartbeat
// timeouts), this clock carries real time with an explicit uncertainty window.
struct time_bounds {
    using clock = lease_clock;
    clock::time_point earliest;
    clock::time_point latest;

    using duration = clock::duration;

    // True if this interval is *definitely* more than `delta` old relative to
    // the current-time reading `now`, i.e. even in the most pessimistic case
    // (our recorded upper bound vs. the current lower bound) at least `delta`
    // has elapsed. This is the conservative test a new leader uses to decide a
    // deposed leader's lease has expired before committing.
    bool older_than(duration delta, const time_bounds& now) const {
        return latest + delta < now.earliest;
    }

    // True if this interval is *definitely* less than `delta` old relative to
    // the current-time reading `now`, i.e. even in the most pessimistic case
    // (our recorded lower bound vs. the current upper bound) less than `delta`
    // has elapsed. This is the conservative test a leaseholder uses to decide
    // its lease is still valid before serving a local read.
    bool younger_than(duration delta, const time_bounds& now) const {
        return earliest + delta > now.latest;
    }
};

// Abstract source of bounded-uncertainty time readings.
//
// interval_now() returns std::nullopt when the clock cannot provide trustworthy
// bounds (e.g. the local clock is not synchronized). Callers must treat nullopt
// as "no lease information available" and fall back to the safe path (quorum
// reads / no lease), never as a zero-width interval.
//
// This library only defines the abstraction and a test backend; concrete
// backends that read a real clock (e.g. service::bounded_clock_adjtimex, which
// reads the Linux kernel's NTP error bounds) live outside the raft library so
// that raft itself stays platform-independent.
class bounded_clock {
public:
    virtual ~bounded_clock() = default;
    virtual std::optional<time_bounds> interval_now() = 0;
};

// Test backend with an explicit, injectable reading. Lets fsm unit tests drive
// deterministic time and clock skew without touching the real clock.
class bounded_clock_mock final : public bounded_clock {
    std::optional<time_bounds> _now;
public:
    bounded_clock_mock() = default;
    explicit bounded_clock_mock(time_bounds now) : _now(now) {}

    // Set the interval returned by the next interval_now() calls.
    void set(time_bounds now) {
        _now = now;
    }

    // Set a symmetric interval [center - error, center + error].
    void set(time_bounds::clock::time_point center, time_bounds::duration error) {
        _now = time_bounds{center - error, center + error};
    }

    // Make the clock report as unsynchronized (interval_now() returns nullopt).
    void set_unsynchronized() {
        _now = std::nullopt;
    }

    std::optional<time_bounds> interval_now() override {
        return _now;
    }
};

} // end of namespace raft
