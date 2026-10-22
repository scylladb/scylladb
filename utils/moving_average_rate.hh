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
/// No timer is needed: add() first records the snapshots that fell due since
/// the last add(). Nothing was added in between, so those snapshots all hold
/// the current total, and the result is the same as if they had been recorded
/// on time. rate() accounts for the ones due since the last add() the same
/// way, without recording them.
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

    // Records the snapshots that fell due since the last one, every
    // _sample_interval after it, up to `now`.
    void catch_up(time_point now) noexcept {
        auto last = (_snapshots.cend() - 1)->time;
        if (now - last < _sample_interval) {
            return;
        }
        using count_type = typename duration::rep;
        count_type due = (now - last) / _sample_interval;
        // Only the last max_snapshots matter, the older ones would be dropped.
        count_type first = due > count_type(max_snapshots) ? due - count_type(max_snapshots) + 1 : 1;
        for (count_type i = first; i <= due; ++i) {
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
        // Snapshots that fell due since the last add() aren't recorded, but
        // they would all hold the current total. Count how many of the
        // recorded ones they would push out; if they would push out all of
        // them, nothing was added within the window, and the oldest snapshot
        // is one of them.
        auto last = (_snapshots.cend() - 1)->time;
        using count_type = typename duration::rep;
        count_type due = now > last ? (now - last) / _sample_interval : 0;
        count_type size = _snapshots.size();
        count_type pushed_out = std::max<count_type>(size + due - count_type(max_snapshots), 0);
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
