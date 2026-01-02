/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "dht/i_partitioner_fwd.hh"

using namespace seastar;

namespace compaction {

class owned_ranges {
public:
    using owned_ranges_ptr = lw_shared_ptr<const dht::token_range_vector>;

private:
    owned_ranges_ptr _ranges;
    std::optional<uint64_t> _hash;

    owned_ranges(owned_ranges_ptr ranges, std::optional<uint64_t> hash) noexcept
        : _ranges(std::move(ranges)), _hash(hash)
    {}

    static uint64_t calculate_hash(const dht::token_range_vector& ranges) noexcept;
    static future<uint64_t> calculate_hash_gently(const dht::token_range_vector& ranges) noexcept;

    friend owned_ranges make_owned_ranges(dht::token_range_vector&& ranges) noexcept;
    friend future<owned_ranges> make_owned_ranges_gently(dht::token_range_vector&& ranges) noexcept;

public:
    owned_ranges() = default;

    owned_ranges clone() const;
    future<owned_ranges> clone_gently() const;

    const owned_ranges_ptr& ranges() const noexcept {
        return _ranges;
    }

    void set_ranges(owned_ranges_ptr ranges) noexcept {
        _ranges = std::move(ranges);
    }

    const std::optional<uint64_t>& hash() const noexcept {
        return _hash;
    }

    void set_hash(uint64_t hash) noexcept {
        _hash = hash;
    }
};

owned_ranges make_owned_ranges(dht::token_range_vector&& ranges) noexcept;
future<owned_ranges> make_owned_ranges_gently(dht::token_range_vector&& ranges) noexcept;

} // namespace compaction
