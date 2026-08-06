/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/raft/bounded_clock_adjtimex.hh"

#include <sys/timex.h>

namespace service {

std::optional<raft::time_bounds> bounded_clock_adjtimex::interval_now() {
    // Sample the monotonic clock first, as close as possible to the adjtimex
    // sample below. steady_clock is CLOCK_MONOTONIC on Linux: it never steps and
    // never runs backwards, so it is a trustworthy measure of *elapsed physical
    // time* even when CLOCK_REALTIME is being stepped by NTP. We use it both to
    // advance our monotonic center and (implicitly) to bound how far the center
    // may be corrected per call.
    const auto mono = std::chrono::steady_clock::now();

    // modes == 0 makes this a pure read of the kernel's NTP state; no adjustment
    // is applied. It returns, in one syscall: the current CLOCK_REALTIME value
    // (tx.time), the kernel's conservative maximum error (tx.maxerror), the
    // status flags (tx.status, incl. STA_NANO/STA_UNSYNC), and the clock state
    // (the return value, e.g. TIME_ERROR).
    struct timex tx = {};
    const int state = ntp_adjtime(&tx);

    // The clock is only trustworthy when it is synchronized. ntp_adjtime()
    // returns TIME_ERROR (and sets STA_UNSYNC) once the kernel's estimated
    // maximum error grows past its limit, or when no NTP source is disciplining
    // the clock. In that case we cannot bound the error, so the reading is
    // unusable.
    const bool synced = !(state < 0 || state == TIME_ERROR || (tx.status & STA_UNSYNC));
    if (!synced) {
        return advance(mono, std::nullopt);
    }

    // Decode the CLOCK_REALTIME timestamp that was sampled at the same instant as
    // tx.maxerror, so the error bound applies to exactly this timestamp.
    // tx.time.tv_usec is in nanoseconds when STA_NANO is set, otherwise
    // microseconds; misinterpreting this would corrupt the timestamp by up to
    // ~1000x, so it must be honored.
    const auto subsecond = (tx.status & STA_NANO)
            ? std::chrono::nanoseconds(tx.time.tv_usec)
            : std::chrono::nanoseconds(std::chrono::microseconds(tx.time.tv_usec));
    const auto realtime = std::chrono::system_clock::time_point(
            std::chrono::duration_cast<std::chrono::system_clock::duration>(
                    std::chrono::seconds(tx.time.tv_sec) + subsecond));

    return advance(mono, ntp_sample{realtime, std::chrono::microseconds(tx.maxerror)});
}

std::optional<raft::time_bounds> bounded_clock_adjtimex::advance(
        std::chrono::steady_clock::time_point mono,
        std::optional<ntp_sample> sample) {
    // First reading: we have no prior monotonic sample to advance from, so we
    // simply anchor the center on the kernel clock and report the kernel's own
    // bound. We only anchor on a synchronized clock; while unsynchronized we
    // report nullopt and stay un-anchored so the very first trusted reading
    // defines the start of our monotonic timeline.
    if (!_initialized) {
        if (!sample) {
            return std::nullopt;
        }
        _center = sample->realtime;
        _mono_last = mono;
        _initialized = true;
        return raft::time_bounds{_center - sample->maxerror, _center + sample->maxerror};
    }

    // Step 1: advance the center by the elapsed *physical* time since the last
    // call, measured on the monotonic clock. This is always a forward move
    // (elapsed >= 0 because CLOCK_MONOTONIC never goes backwards), and it is the
    // only thing that ties our virtual clock's rate to real elapsed time. We
    // keep advancing the center even while unsynchronized, so that our monotonic
    // timeline remains continuous across an unsync gap: when the clock recovers,
    // the divergence we see is the size of any real-time step, not the length of
    // the outage.
    const auto elapsed = mono - _mono_last;
    _mono_last = mono;
    _center += std::chrono::duration_cast<std::chrono::system_clock::duration>(elapsed);

    // While unsynchronized we cannot bound the error, so emit nothing (the caller
    // falls back to the safe path). The center state has already been advanced
    // above so recovery is seamless.
    if (!sample) {
        return std::nullopt;
    }
    const auto realtime = sample->realtime;

    // Step 2: slew the center towards the kernel's CLOCK_REALTIME, but by no more
    // than `elapsed >> slew_shift` this call. Because the correction magnitude is
    // strictly smaller than `elapsed`, the net change to the center this call is
    //     elapsed + correction  >=  elapsed - (elapsed >> slew_shift)  >=  0,
    // so the center is guaranteed to be MONOTONICALLY NON-DECREASING. This is the
    // crucial safety property: a reading can never come out smaller than one we
    // already emitted (and that a leaseholder may already have recorded in a log
    // entry's lease_time), regardless of how CLOCK_REALTIME jumps.
    //
    // In steady state the required correction (frequency error * elapsed, well
    // under 0.1%) is far below the cap, so it is applied in full and the center
    // tracks CLOCK_REALTIME tightly. Only a genuine step exceeds the cap, and it
    // is then absorbed gradually over the following calls.
    const auto correction = realtime - _center;
    const auto max_correction =
            std::chrono::duration_cast<std::chrono::system_clock::duration>(elapsed) / (1 << slew_shift);
    if (correction > max_correction) {
        _center += max_correction;
    } else if (correction < -max_correction) {
        _center -= max_correction;
    } else {
        _center += correction;
    }

    // Step 3: report [center - error, center + error] where the error widens to
    // cover any residual divergence between our (monotonic, possibly not-yet-
    // converged) center and the kernel clock:
    //
    //     error = maxerror + |realtime - center|
    //
    // This guarantees the reported interval always contains the kernel's interval
    //     [realtime - maxerror, realtime + maxerror]
    // and therefore the true time (assuming maxerror is a sound bound):
    //     center - error = center - maxerror - |realtime - center| <= realtime - maxerror
    //     center + error = center + maxerror + |realtime - center| >= realtime + maxerror
    //
    // Combined with the monotonic center, this makes BOTH LeaseGuard comparisons
    // conservative across a clock step:
    //   - A backward step (CLOCK_REALTIME drops by X) does not move the center;
    //     instead |realtime - center| ~= X inflates the error, so `latest`
    //     *increases*. younger_than() (a leaseholder deciding its lease is still
    //     valid) becomes harder, so the lease expires no later than it should --
    //     no stale reads.
    //   - A forward step likewise inflates the error, lowering `earliest`, so
    //     older_than() (a new leader deciding a deposed lease has expired)
    //     becomes harder -- it defers committing longer. Safe, at a small
    //     availability cost.
    // As the slew closes the gap, |realtime - center| -> 0 and the interval
    // tightens back to +/- maxerror; the clock is self-healing.
    //
    // Residual assumption: this handles *steps* in the CLOCK_REALTIME value, but
    // soundness still relies on maxerror being a correct bound on
    // |true_time - CLOCK_REALTIME| between NTP updates. Running the NTP daemon in
    // slew-only mode (no stepping after startup) keeps that bound trustworthy and
    // avoids large error-inflation events; a fully rigorous bound would require a
    // dedicated service such as AWS clockbound. Slew-only is thus recommended for
    // accuracy/availability, but is no longer required for *safety* against steps.
    const auto divergence = realtime > _center ? (realtime - _center) : (_center - realtime);
    const auto error = sample->maxerror
            + std::chrono::duration_cast<std::chrono::microseconds>(divergence);
    return raft::time_bounds{_center - error, _center + error};
}


} // namespace service
