/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <atomic>

namespace utils {

/*
 * The neat_id class is purely a debugging thing -- when reading
 * the logs with object IDs in it it's more handy to look at those
 * consisting * of 1-3 digits, rather than 16 hex-digits of a printed
 * pointer.
 *
 * Embed with [[no_unique_address]] tag for memory efficiency
 */
template <bool Debug>
struct neat_id {
    unsigned int operator()() const noexcept { return reinterpret_cast<uintptr_t>(this); }
};

template <>
struct neat_id<true> {
    unsigned int _id;
    static unsigned int _next() noexcept {
        static std::atomic<unsigned int> rover {1};
        return rover.fetch_add(1);
    }

    neat_id() noexcept : _id(_next()) {}
    unsigned int operator()() const noexcept { return _id; }
};

} // namespace
