/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <cmath>
#include <numbers>
#include <array>
#include <random>
#include <variant>
#include <chrono>

#include <seastar/core/metrics.hh>

#include "utils/small_vector.hh"
#include "utils/murmur_hash.hh"
#include "db/rate_limiter.hh"

// The rate limiter keeps a hashmap of counters differentiated by operation type
// (e.g. read or write) and the partition token. On each operation,
// the corresponding counter is increased by 1.
//
// The counters are decremented via two mechanisms:
//
// 1. Every `time_window_duration`, all counters are halved.
// 2. Within a time window, on every `bucket_size` operations all counters
//    are decremented by 1.
//
// The mechanism 1) makes sure that we do not forget about very frequent
// operations too quick and makes it possible to reject in a probabilistic
// manner (this is described in more detail in design notes).
//
// The mechanism 2) protects the internal hashmap from being flooded with
// counters with low values. This causes the rate limiter to underestimate
// the counter values by the current number of "buckets" within this
// time window. This strategy is also known as "lossy counting".
//
// Both mechanisms 1) and 2) are implemented in a lazy manner.

namespace db {

static constexpr size_t hash_bits = 16;
static constexpr size_t entry_count = 1 << hash_bits;
static constexpr size_t bucket_size = 10000;


void rate_limiter_base::on_timer() noexcept {
    _time_window_history.pop_back();
    _time_window_history.insert(_time_window_history.begin(), time_window_entry {
        .entries_active = _current_entries_in_time_window,
        .lossy_counting_decrease = _current_bucket,
    });

    _current_bucket = 0;
    _current_ops_in_bucket = 0;
    _current_entries_in_time_window = 0;

    _current_time_window = (_current_time_window + 1) % (1 << time_window_bits);

    // Because time window ids are 12 bit numbers and we increase the current
    // time window number by 1 every second, it wraps around every 4096
    // seconds (more than an hour). Because of this, some very old entry
    // updated last 4096 seconds may accidentally become valid again.
    //
    // In order to prevent this, we make sure to update the entries
    // more frequently. We do this by refreshing all the entries within half
    // of the wraparound period (2048 seconds).
    //
    // Instead of clearing everything at once, we divide this operation
    // into many small steps and perform them during time window change.
    //
    // All of this should make sure that each entry's time window is not
    // older than 2048 seconds from the current generation.

    constexpr size_t period = 1 << (time_window_bits - 1);
    constexpr size_t entries_per_step = entry_count / period;

    const size_t begin = _current_time_window * entries_per_step;
    for (size_t i = 0; i < entries_per_step; i++) {
        entry_refresh(_entries[(begin + i) % entry_count]);
    }
}

rate_limiter_base::entry* rate_limiter_base::get_entry(uint32_t label, uint64_t token) noexcept {
    // We need to either find the existing entry for this (label, token) combination
    // or otherwise find an invalid entry which we can initialize and use.
    //
    // We start by looking at the entry corresponding to the computed hash,
    // if it's occupied by another (label, token) try other entries using
    // the quadratic probing strategy.
    //
    // We limit ourselves to 32 attempts - if no suitable entry is found
    // then we return nullptr and admit the operation unconditionally.

    // Because we use quadratic probing and entries can be deleted (lazily),
    // a situation can occur where an entry A suddenly becomes inaccessible
    // because another entry B which is earlier on the probe chain is deleted.
    // One of the following will happen:
    //
    // 1. Either we will allocate a new entry over B and A becomes accessible
    //    again,
    // 2. Or we will allocate a new entry for the same operation/partition as A
    //    and A will eventually expire.
    //
    // In the worst case, A might be a "hot" entry and be actively rate limited
    // and the described situation will cause a large number of operations
    // to be admitted. Fortunately, this will move the entry earlier in the
    // probe chain, so this situation will happen a limited number of times (if
    // any at all) for a single "hot" entry.

    size_t hash = compute_hash(label, token);

    static constexpr size_t max_probes = 32;
    for (size_t i = 0; i < max_probes; i++) {
        // Quadratic probing - every iteration jumps further than the previous one
        hash = (hash + i) % entry_count;
        entry& b = _entries[hash];
        ++_metrics.probe_count;

        entry_refresh(b);

        if (entry_is_empty(b)) {
            ++_metrics.allocations_on_empty;
            b.token = token;
            b.label = label;
            b.op_count = _current_bucket;
            return &b;
        } else if (b.token == token && b.label == label) {
            ++_metrics.successful_lookups;
            return &b;
        }
    }

    ++_metrics.failed_allocations;
    return nullptr;
}

size_t rate_limiter_base::compute_hash(uint32_t label, uint64_t token) noexcept {
    // The map key is a tuple (token, key) + salt
    // The key is hashed with murmur hash for good hash quality

    static constexpr size_t key_length = sizeof(token) + sizeof(label) + sizeof(_salt);

    std::array<uint8_t, key_length> key;
    uint8_t* ptr = key.data();
    memcpy(ptr, &token, sizeof(token));
    ptr += sizeof(token);
    memcpy(ptr, &label, sizeof(label));
    ptr += sizeof(label);
    memcpy(ptr, &_salt, sizeof(_salt));

    std::array<uint64_t, 2> out;
    utils::murmur_hash::hash3_x64_128(key.data(), key_length, 0, out);
    return out[0];
}

void rate_limiter_base::entry_refresh(rate_limiter_base::entry& b) noexcept {
    uint32_t window_delta = _current_time_window - b.time_window;

    if (window_delta == 0) {
        // The entry is fresh, it was allocated in this time window
        return;
    }

    if (window_delta < _time_window_history.size()) {
        // The entry is not that old so we have to apply the effects
        // of lossy counting and halving on time window switch
        --_time_window_history[window_delta - 1].entries_active;
        while (window_delta > 0) {
            if (b.op_count > _time_window_history[window_delta - 1].lossy_counting_decrease) {
                b.op_count -= _time_window_history[window_delta - 1].lossy_counting_decrease;
            } else {
                b.op_count = 0;
            }
            b.op_count /= 2;

            --window_delta;
        }
    } else {
        // The entry is very old and the op_count can be safely decreased to zero
        b.op_count = 0;
    }

    ++_current_entries_in_time_window;
    b.time_window = _current_time_window;
}

bool rate_limiter_base::entry_is_empty(const rate_limiter_base::entry& b) noexcept {
    return b.op_count <= _current_bucket;
}

void rate_limiter_base::register_metrics() {
    namespace sm = seastar::metrics;

    _metric_group.add_group("per_partition_rate_limiter", {
        // TODO: Most of the following metrics are pretty low-level and not useful for users,
        // perhaps they should be hidden behind a configuration flag

        sm::make_counter("allocations", _metrics.allocations_on_empty,
                sm::description("Number of times a entry was allocated over an empty/expired entry.")),

        sm::make_counter("successful_lookups", _metrics.successful_lookups,
                sm::description("Number of times a lookup returned an already allocated entry.")),

        sm::make_counter("failed_allocations", _metrics.failed_allocations,
                sm::description("Number of times the rate limiter gave up trying to allocate.")),

        sm::make_counter("probe_count", _metrics.probe_count,
                sm::description("Number of probes made during lookups.")),

        sm::make_gauge("load_factor", [&] {
                    uint32_t occupied_entry_count = _current_entries_in_time_window;
                    for (const auto& twe : _time_window_history) {
                        occupied_entry_count += twe.entries_active;
                    }
                    return double(occupied_entry_count) / double(entry_count);
                },
                sm::description("Current load factor of the hash table (upper bound, may be overestimated).")),
    });
}

rate_limiter_base::rate_limiter_base()
        : _salt(std::random_device{}())
        , _entries(entry_count)
        , _time_window_history(op_count_bits - 1) {
    
    register_metrics();
}

uint64_t rate_limiter_base::increase_and_get_counter(label& l, uint64_t token) noexcept {
    // Assign a label if not done yet
    if (l._label == 0) {
        l._label = _next_label++;
    }

    entry* b = get_entry(l._label, token);
    if (!b) {
        // We failed to allocate a entry for this partition. This means that
        // we won't track hit count for this partition during this time window.
        // Assume that it's OK to admit the operation.
        return 0;
    }

    // Protect from wrap-around
    b->op_count = std::min<uint32_t>((1 << op_count_bits) - 1, b->op_count + 1);
    ++_current_ops_in_bucket;
    if (_current_ops_in_bucket >= bucket_size) {
        // Every `bucket_size` operations, virtually decrement all entries
        // by one. We implement it by always subtracting the `_current_bucket`
        // when comparing the count in the entry with the limit.
        ++_current_bucket;
        _current_ops_in_bucket -= bucket_size;
    }

    return b->op_count - _current_bucket;
}


rate_limiter_base::can_proceed rate_limiter_base::account_operation(
        label& l, uint64_t token, uint64_t limit,
        const db::per_partition_rate_limit::info& rate_limit_info) noexcept {

    if (std::holds_alternative<std::monostate>(rate_limit_info)) {
        // Rate limiting turned off
        return can_proceed::yes;
    }

    const uint64_t count = increase_and_get_counter(l, token);

    if (auto* info = std::get_if<db::per_partition_rate_limit::account_and_enforce>(&rate_limit_info)) {
        // On each time window change we halve the entry counts, therefore
        // a partition with X ops/s will stabilize at 2X hits at the end
        // of each time window.
        if (count <= 2 * limit) {
            return can_proceed::yes;
        } else {
            // As mentioned before, assuming a fixed operation rate, the operation
            // count in a entry will oscillate between X at the beginning of the
            // time window and 2X at the end. In order to only accept `limit`
            // operations within a time window, we need to reject with probability
            // P_c(x), where P_c(x) is a function such that, integrated over [X, 2X]
            // will be equal to `limit`. `P_c(x) = limit / (x * ln 2)` satisfies
            // this criterion.
            //
            // All replicas get the same value for the random variable X, with an
            // expectation that all replicas' counters oscillate between the same
            // values. Because of that, most of the time replicas will agree
            // and either all accept or reject.
            if (info->get_random_variable_as_double() * double(count) * std::numbers::ln2 < double(limit)) {
                return can_proceed::yes;
            } else {
                return can_proceed::no;
            }
        }
    } else {
        return can_proceed::yes;
    }
}

template class generic_rate_limiter<seastar::lowres_clock>;

}
