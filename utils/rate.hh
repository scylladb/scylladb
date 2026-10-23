/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <fmt/format.h>

#include "utils/on_internal_error.hh"

namespace utils {

/// Tags naming what a rate counts.
namespace units {
struct bytes;
struct reads;
struct sstables;
}

/// How much of something accumulated over an interval of time, e.g. the bytes
/// flushed over the last 30 minutes.
///
/// A rate keeps the amount and the interval rather than only their quotient, so
/// the period it covers is known, and amounts are only combined when they cover
/// the same one. Unit is a tag naming what the amount counts, e.g. units::bytes,
/// so rates of different things can't be mixed up.
template <typename Unit>
class rate {
public:
    using unit = Unit;
    using interval_type = std::chrono::duration<double>;
private:
    double _amount = 0;
    interval_type _interval = interval_type::zero();
public:
    constexpr rate() noexcept = default;
    constexpr rate(double amount, interval_type interval) noexcept
        : _amount(amount)
        , _interval(interval) {
    }

    /// How much accumulated over the interval, in Unit.
    constexpr double amount() const noexcept {
        return _amount;
    }

    constexpr interval_type interval() const noexcept {
        return _interval;
    }

    /// Units per second over the interval, or 0 over an empty interval.
    constexpr double per_second() const noexcept {
        return _interval > interval_type::zero() ? _amount / _interval.count() : 0;
    }

    /// The amount of this rate per unit of `other`'s amount, e.g. sstables read per read,
    /// or 0 when `other`'s amount is 0. Both must cover the same interval, otherwise
    /// their amounts don't relate to each other.
    template <typename OtherUnit>
    double per(const rate<OtherUnit>& other) const {
        if (_interval != other.interval()) {
            on_internal_error(fmt::format("rate: dividing an amount over {}s by one over {}s",
                    _interval.count(), other.interval().count()));
        }
        return other.amount() != 0 ? _amount / other.amount() : 0;
    }
};

} // namespace utils
