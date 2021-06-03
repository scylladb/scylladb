/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
