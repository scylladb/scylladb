/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/bounded_clock.hh"

// Opaque ClockBound context, defined by libclockbound's <clockbound.h>. Forward
// declared here (in the global namespace, matching the C header) so this header
// does not depend on <clockbound.h>.
struct clockbound_ctx;

namespace service {

// Concrete raft::bounded_clock backend backed by the AWS ClockBound daemon,
// using the plain C client library (libclockbound, built from the clock-bound
// git submodule's clock-bound-ffi crate).
//
// ClockBound is a purpose-built bounded-uncertainty clock: a local daemon
// disciplines the clock and publishes, in a shared-memory segment, an error
// bound [earliest, latest] guaranteed to contain true time together with a clock
// status. Reading it (clockbound_now) is the equivalent of
// clock_gettime(CLOCK_REALTIME) plus a shared-memory read -- non-blocking and
// safe to call from the reactor.
//
// Step safety
// -----------
// Unlike the adjtimex backend, this class needs no monotonic discipline of its
// own: ClockBound detects clock steps/disruptions itself (including via the
// VMClock paravirtual device on VMs) and reports them through the clock status.
// interval_now() trusts the bound only when the status is SYNCHRONIZED and
// returns nullopt otherwise, so any disruption safely degrades to the quorum
// fallback rather than a stale reading.
//
// The ClockBound context is NOT thread-safe; this class is meant to be used per
// shard (one physical clock per node) and interval_now() called only from the
// owning shard's reactor thread. Requires the clockbound daemon at runtime --
// the constructor throws if it is not available.
class bounded_clock_clockbound final : public raft::bounded_clock {
    ::clockbound_ctx* _ctx = nullptr;

public:
    // Opens a per-shard ClockBound context. Throws std::runtime_error if the
    // daemon/segment is unavailable.
    bounded_clock_clockbound();
    ~bounded_clock_clockbound();

    bounded_clock_clockbound(const bounded_clock_clockbound&) = delete;
    bounded_clock_clockbound& operator=(const bounded_clock_clockbound&) = delete;

    std::optional<raft::time_bounds> interval_now() override;
};

} // namespace service
