/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <type_traits>

#include <seastar/core/circular_buffer_fixed_capacity.hh>
#include <seastar/core/lowres_clock.hh>

#include "utils/rate.hh"

namespace utils {

/// Moving average of the rate at which a quantity accumulates, e.g. bytes
/// flushed or compacted per second, over a sliding time window.
///
/// The quantity is accumulated with add() and the rate is read with rate().
/// A snapshot of the running total is kept every window / Samples from the
/// start, and only the last Samples + 1 snapshots are kept, so memory is fixed
/// no matter how often add() is called. rate() returns what was added since
/// the oldest snapshot together with the time since it: idle periods count,
/// and until the window has filled, the rate covers the time since the start.
///
/// Averages built with the same window and start keep their snapshots at the
/// same times, so rates read from them at the same time cover the same interval
/// and can be combined.
///
/// No timer is needed: snapshots are recorded lazily. add() first records the
/// snapshots whose times passed since the last one recorded. Nothing was added in
/// between, so each holds the current total, what a timer would have recorded.
/// rate() computes them the same way without storing them, so it stays const.
///
/// \tparam Unit what the quantity counts, e.g. units::bytes
/// \tparam T the type of the quantity, e.g. uint64_t for bytes
/// \tparam Clock the clock the time points come from
/// \tparam Samples the number of sample intervals the window is divided into
template <typename Unit, typename T = uint64_t, typename Clock = seastar::lowres_clock, size_t Samples = 30>
requires std::is_arithmetic_v<T> && (Samples > 0)
class moving_average_rate {
public:
    using clock_type = Clock;
    using duration = typename Clock::duration;
    using time_point = typename Clock::time_point;
private:
    struct snapshot {
        time_point time;
        T total;
    };
    // Samples intervals are delimited by Samples + 1 snapshots.
    static constexpr size_t max_snapshots = Samples + 1;
    seastar::circular_buffer_fixed_capacity<snapshot, std::bit_ceil(max_snapshots)> _snapshots;
    duration _sample_interval;
    T _total = T();

    void push(snapshot s) noexcept {
        if (_snapshots.size() == max_snapshots) {
            _snapshots.pop_front();
        }
        _snapshots.push_back(s);
    }

    // The last snapshot recorded. circular_buffer_fixed_capacity has no const back().
    const snapshot& last_snapshot() const noexcept {
        return *(_snapshots.cend() - 1);
    }

    // The snapshots not recorded yet at `now`: one per sample interval that
    // ended since the last recorded one.
    using count_type = typename duration::rep;
    count_type unrecorded(time_point now) const noexcept {
        auto last = last_snapshot().time;
        return now > last ? (now - last) / _sample_interval : 0;
    }

    // Records the snapshots not recorded yet at `now`.
    void catch_up(time_point now) noexcept {
        auto last = last_snapshot().time;
        auto missing = unrecorded(now);
        // Only the last max_snapshots are kept; the ones before would be dropped.
        count_type first = missing > count_type(max_snapshots) ? missing - count_type(max_snapshots) + 1 : 1;
        for (count_type i = first; i <= missing; ++i) {
            push(snapshot{last + i * _sample_interval, _total});
        }
    }
public:
    /// Averages over `window`, starting at `start`.
    moving_average_rate(duration window, time_point start)
        : _sample_interval(window / Samples) {
        if (_sample_interval <= duration::zero()) {
            throw std::invalid_argument("moving_average_rate: the window must hold at least one clock tick per sample");
        }
        push(snapshot{start, _total});
    }

    void add(T value, time_point now = Clock::now()) noexcept {
        catch_up(now);
        _total += value;
    }

    /// What was added over the window ending at `now`, or since the start
    /// while the window hasn't filled.
    utils::rate<Unit> rate(time_point now = Clock::now()) const noexcept {
        // The snapshots not recorded yet would all hold the current total.
        // Count how many of the recorded ones they would push out; if they
        // would push out all of them, nothing was added within the window, and
        // the oldest snapshot is one of them.
        auto last = last_snapshot().time;
        count_type size = _snapshots.size();
        count_type pushed_out = std::max<count_type>(size + unrecorded(now) - count_type(max_snapshots), 0);
        auto oldest = pushed_out < size
                ? *(_snapshots.cbegin() + pushed_out)
                : snapshot{last + (pushed_out - size + 1) * _sample_interval, _total};
        if (now <= oldest.time) {
            return {};
        }
        return utils::rate<Unit>(double(_total - oldest.total), now - oldest.time);
    }

    /// Everything added so far.
    T total() const noexcept {
        return _total;
    }

    duration window() const noexcept {
        return _sample_interval * Samples;
    }
};

} // namespace utils
