/*
 * Copyright (C) 2017 ScyllaDB
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

// FIXME: FNV-1a is quite slow, consider something faster, CityHash seems to be
// a good choice.

template<unsigned>
struct fnv1a_constants { };

template<>
struct fnv1a_constants<8> {
    enum : uint64_t {
        offset = 0xcbf29ce484222325ull,
        prime = 0x100000001b3ull,
    };
};

class fnv1a_hasher {
    using constants = fnv1a_constants<sizeof(size_t)>;
    size_t _hash = constants::offset;
public:
    void update(const char* ptr, size_t length) {
        auto end = ptr + length;
        while (ptr != end) {
            _hash ^= *ptr;
            _hash *= constants::prime;
            ++ptr;
        }
    }

    size_t finalize() const {
        return _hash;
    }
};
