/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <chrono>
#include <optional>

namespace raft {

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
    std::chrono::system_clock::time_point earliest;
    std::chrono::system_clock::time_point latest;

    using duration = std::chrono::system_clock::duration;

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
    void set(std::chrono::system_clock::time_point center, time_bounds::duration error) {
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
