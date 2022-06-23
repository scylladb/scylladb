/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <cstddef>
#include <chrono>
#include <limits>
#include <concepts>
#include <vector>
#include <optional>
#include <random>

#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/util/bool_class.hh>

#include "utils/chunked_vector.hh"
#include "db/per_partition_rate_limit_info.hh"

// A data structure used to implement per-partition rate limiting. It accounts
// operations and enforces limits when it is detected that the operation rate
// is too high.

namespace db {

class rate_limiter_base {
public:
    static constexpr size_t op_count_bits = 20;
    static constexpr size_t time_window_bits = 12;

private:
    struct metrics {
        uint64_t allocations_on_empty = 0;
        uint64_t successful_lookups = 0;
        uint64_t failed_allocations = 0;
        uint64_t probe_count = 0;
    };

    // Represents a piece of the hashmap storage.
    struct entry {
    public:
        // The partition key token of the operation which allocated this entry.
        uint64_t token = 0;

        // The label of the operation which allocated this entry.
        // Labels are used to differentiate operations which should be counted
        // separately, e.g. reads and writes to the same table or writes
        // to two different tables.
        uint32_t label = 0;

        // The number of operations counted for given token/label.
        // It is virtually decremented on each window change, so the real
        // operation count is actually `op_count - _current_bucket`.
        // If the number drops to zero or below, the entry is considered
        // "expired" and may be overwritten by another operation.
        uint32_t op_count : op_count_bits = 0;

        // ID of the time window in which the entry was allocated.
        uint32_t time_window : time_window_bits = 0;
    };

    struct time_window_entry {
        // How many entries are there active within this time window?
        uint32_t entries_active = 0;

        // By how much should the counter should be decreased within
        // this time window?
        uint32_t lossy_counting_decrease = 0;
    };

public:
    struct can_proceed_tag{};
    using can_proceed = seastar::bool_class<can_proceed_tag>;

    // Identifies a type of operation which is counted separately from other
    // operations. For example, reads and writes for given table should have
    // separate labels.
    struct label {
    private:
        // The current ID used to identify the label in the rate limiter.
        // It is assigned on first use.
        uint32_t _label = 0;

        friend class rate_limiter_base;
    };

private:
    uint32_t _current_bucket = 0;
    uint32_t _current_ops_in_bucket = 0;
    uint32_t _current_entries_in_time_window = 0;

    uint32_t _next_label = 1;
    uint32_t _current_time_window = 0;

    const uint32_t _salt;

    utils::chunked_vector<entry> _entries;
    std::vector<time_window_entry> _time_window_history;

    metrics _metrics;
    seastar::metrics::metric_groups _metric_group;

private:
    entry* get_entry(uint32_t label, uint64_t token) noexcept;
    size_t compute_hash(uint32_t label, uint64_t token) noexcept;

    void entry_refresh(entry& b) noexcept;
    bool entry_is_empty(const entry& b) noexcept;

    void register_metrics();

protected:
    void on_timer() noexcept;

public:
    rate_limiter_base();

    rate_limiter_base(const rate_limiter_base&) = delete;
    rate_limiter_base(rate_limiter_base&&) = delete;

    rate_limiter_base& operator=(const rate_limiter_base&) = delete;
    rate_limiter_base& operator=(rate_limiter_base&&) = delete;

    // (For testing purposes only)
    // Increments the counter for given (label, token) and returns
    // the new value of the counter.
    uint64_t increase_and_get_counter(label& l, uint64_t token) noexcept;

    // Increments the counter for given (label, token).
    // If the counter indicates that the partition is over the limit,
    // returns can_proceed::no with some probability.
    //
    // The `random_variable` parameter should be a value from range [0, 1).
    // It is used as the source of randomness - the function chooses a threshold
    // and accepts if and only if `random_variable` is below it.
    //
    // The probability is calculated in such a way that statistically
    // only `limit` operations per second are admitted.
    can_proceed account_operation(label& l, uint64_t token, uint64_t limit,
            const db::per_partition_rate_limit::info& rate_limit_info) noexcept;
};

template<typename ClockType>
class generic_rate_limiter : public rate_limiter_base {
private:
    seastar::timer<ClockType> _timer;

public:
    generic_rate_limiter()
            : rate_limiter_base() {

        // Rate limiting is more accurate when the rate limiter timers
        // on all nodes are synchronized. Assume that the nodes' clocks
        // are synchronized and schedule the first tick on the beginning
        // of the closest second.

        const auto period = std::chrono::seconds(1);
        const auto now = std::chrono::system_clock::now();
        const auto initial_delay = period - now.time_since_epoch() % period;

        _timer.set_callback([this] { on_timer(); });
        _timer.arm(ClockType::now() + initial_delay, period);
    }
};

extern template class generic_rate_limiter<seastar::lowres_clock>;
using rate_limiter = generic_rate_limiter<seastar::lowres_clock>;

}
