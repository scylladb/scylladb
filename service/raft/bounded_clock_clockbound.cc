/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/raft/bounded_clock_clockbound.hh"

#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <time.h>
#include <fmt/format.h>

#include "utils/log.hh"

// libclockbound exposes a plain C ABI, but its header has no `extern "C"` guards
// of its own, so we must wrap it to get C linkage. It also declares a struct
// field literally named `errno`, which is a macro in C++ -- neutralize the macro
// just around the include so the field name is a plain identifier (we never read
// that field).
extern "C" {
#pragma push_macro("errno")
#undef errno
#include "clockbound.h"
#pragma pop_macro("errno")
}

static logging::logger logger("bounded_clock_clockbound");

namespace {

// Human-readable name for a ClockBound clock status, for logs and errors.
std::string_view status_name(clockbound_clock_status status) {
    switch (status) {
    case CLOCKBOUND_STA_UNKNOWN:
        return "unknown";
    case CLOCKBOUND_STA_SYNCHRONIZED:
        return "synchronized";
    case CLOCKBOUND_STA_FREE_RUNNING:
        return "free-running";
    case CLOCKBOUND_STA_DISRUPTED:
        return "disrupted";
    }
    return "invalid";
}

// Copy a libclockbound error detail (a fixed-size, not necessarily
// NUL-terminated char buffer) into a std::string_view over that buffer.
std::string_view error_detail(const clockbound_err& err) {
    return std::string_view(err.detail, ::strnlen(err.detail, CLOCKBOUND_ERROR_DETAIL_SIZE));
}

} // anonymous namespace

namespace service {

bounded_clock_clockbound::bounded_clock_clockbound() {
    clockbound_err err = {};
    _ctx = clockbound_open(&err);
    if (_ctx == nullptr) {
        throw std::runtime_error(fmt::format(
                "Failed to open ClockBound context (is the clockbound daemon running?): {}",
                error_detail(err)));
    }

    // Opening the context only proves the shared-memory segment *file* exists --
    // and the daemon leaves that file behind under /var/run/clockbound when it
    // stops, so a successful open does not by itself mean the daemon is healthy.
    // Probe the segment once with a full read to reject one that is present but
    // unreadable/malformed (e.g. an incompatible layout version), which would
    // otherwise fail every interval_now() at runtime. We deliberately do NOT
    // require the clock to be synchronized here: a node may legitimately start
    // before the clock has converged, and an unsynchronized clock is handled
    // safely at runtime (interval_now() returns nullopt so LeaseGuard falls back
    // to quorum reads, and logs a rate-limited warning). Failing to start on a
    // transiently-unsynchronized clock would hurt availability without improving
    // safety.
    //
    // On a read error we must close the context ourselves: the destructor does
    // not run for an object whose constructor throws, so it would otherwise leak.
    clockbound_now_result res = {};
    clockbound_err now_err = {};
    if (clockbound_now(_ctx, &res, &now_err) != nullptr) {
        const auto detail = error_detail(now_err);
        clockbound_close(_ctx, &err);
        _ctx = nullptr;
        throw std::runtime_error(fmt::format(
                "ClockBound read failed at startup (is the clockbound daemon running?): {}", detail));
    }
}

bounded_clock_clockbound::~bounded_clock_clockbound() {
    if (_ctx != nullptr) {
        clockbound_err err = {};
        clockbound_close(_ctx, &err);
    }
}

std::optional<raft::time_bounds> bounded_clock_clockbound::interval_now() {
    clockbound_now_result res = {};
    clockbound_err err = {};

    // A read failure means we cannot trust any bound right now; fall back to the
    // safe path. This is unexpected after the successful startup probe, so warn
    // (rate-limited: interval_now() is on the raft lease hot path).
    if (clockbound_now(_ctx, &res, &err) != nullptr) {
        static thread_local logging::logger::rate_limit rate_limit{std::chrono::seconds(60)};
        logger.log(seastar::log_level::warn, rate_limit,
                "clockbound_now() failed ({}); LeaseGuard cannot use a trusted clock and is "
                "falling back to quorum reads. Is the clockbound daemon still running?",
                error_detail(err));
        return std::nullopt;
    }

    // Trust the bound only when the clock is synchronized. The other statuses
    // (UNKNOWN, FREE_RUNNING, DISRUPTED) mean the bound is either untrustworthy
    // or should not be relied on for leases; returning nullopt degrades to the
    // quorum-read fallback rather than risking a stale reading. In particular a
    // DISRUPTED status (e.g. after a VM live-migration clock jump, detected via
    // the VMClock device) safely suppresses lease reads until synchronization is
    // re-established.
    if (res.clock_status != CLOCKBOUND_STA_SYNCHRONIZED) {
        static thread_local logging::logger::rate_limit rate_limit{std::chrono::seconds(60)};
        logger.log(seastar::log_level::warn, rate_limit,
                "ClockBound clock is not synchronized (status: {}); LeaseGuard is falling back to "
                "quorum reads until it recovers. Check the clockbound daemon and clock synchronization.",
                status_name(res.clock_status));
        return std::nullopt;
    }

    // earliest/latest are absolute CLOCK_REALTIME timestamps, directly
    // comparable to std::chrono::system_clock across nodes (all disciplined to
    // UTC by ClockBound).
    const auto to_time_point = [](const struct timespec& ts) {
        return std::chrono::system_clock::time_point(
                std::chrono::duration_cast<std::chrono::system_clock::duration>(
                        std::chrono::seconds(ts.tv_sec) + std::chrono::nanoseconds(ts.tv_nsec)));
    };
    return raft::time_bounds{to_time_point(res.earliest), to_time_point(res.latest)};
}

} // namespace service
