/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "bytes.hh"
#include "utils/serialization.hh"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#include <xxhash.h>
#pragma GCC diagnostic pop

#include <array>

class xx_hasher {
    static constexpr size_t digest_size = 16;
    XXH64_state_t _state;

public:
    explicit xx_hasher(uint64_t seed = 0) noexcept {
        XXH64_reset(&_state, seed);
    }

    void update(const char* ptr, size_t length) noexcept {
        XXH64_update(&_state, ptr, length);
    }

    bytes finalize() {
        bytes digest{bytes::initialized_later(), digest_size};
        serialize_to(digest.begin());
        return digest;
    }

    std::array<uint8_t, digest_size> finalize_array() {
        std::array<uint8_t, digest_size> digest;
        serialize_to(digest.begin());
        return digest;
    }

    uint64_t finalize_uint64() {
        return XXH64_digest(&_state);
    }

private:
    template<typename OutIterator>
    void serialize_to(OutIterator&& out) {
        serialize_int64(out, 0);
        serialize_int64(out, finalize_uint64());
    }
};

// Used to specialize templates in order to fix a bug
// in handling null values: #4567
class legacy_xx_hasher_without_null_digest : public xx_hasher {
public:
    explicit legacy_xx_hasher_without_null_digest(uint64_t seed = 0) noexcept : xx_hasher(seed) {}
};
