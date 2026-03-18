/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>

#include <seastar/core/bitops.hh>
#include <seastar/core/circular_buffer_fixed_capacity.hh>

namespace utils {

/// Tracks the rolling maximum over the last `window_size` samples
/// using in amortized cost O(1) per sample. Current_max()
/// returns an upper bound that is a power of two, at most 2x the
/// true maximum) for efficiency.
class rolling_max_tracker {
    // With the sample clamp to 1, log2ceil produces values
    // in [0, 63] for 64-bit size_t, so at most 64 entries.
    seastar::circular_buffer_fixed_capacity<std::pair<uint64_t, unsigned>, 64> _buf;
    uint64_t _seq = 0;
    size_t _window_size;

public:
    explicit rolling_max_tracker(size_t window_size) noexcept
        : _window_size(window_size) {
    }

    void add_sample(size_t value) noexcept {
        // Clamp to 1 to avoid undefined log2ceil(0)
        auto v = seastar::log2ceil(std::max(value, size_t(1)));
        // Maintain the monotonic (decreasing) property:
        // remove all entries from the back that are <= the new value,
        // since they can never be the maximum while this entry is in the window.
        while (!_buf.empty() && _buf.back().second <= v) {
            _buf.pop_back();
        }
        _buf.emplace_back(_seq, v);
        ++_seq;
        // Remove entries that have fallen out of the window from the front.
        while (_buf.front().first + _window_size < _seq) {
            _buf.pop_front();
        }
    }

    size_t current_max() const noexcept {
        return _buf.empty() ? 0 : size_t(1) << _buf.cbegin()->second;
    }
};

} // namespace utils
