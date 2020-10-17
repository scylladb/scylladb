/*
 * Leverage SIMD for fast UTF-8 validation with range base algorithm.
 * Details at https://github.com/cyb70289/utf8/.
 *
 * Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
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

#include <cstdint>
#include "bytes.hh"

namespace utils {

namespace utf8 {

namespace internal {

struct partial_validation_results {
    bool error;
    size_t unvalidated_tail;
    size_t bytes_needed_for_tail;
};

partial_validation_results validate_partial(const uint8_t* data, size_t len);

}


bool validate(const uint8_t *data, size_t len);

inline bool validate(bytes_view string) {
    const uint8_t *data = reinterpret_cast<const uint8_t*>(string.data());
    size_t len = string.size();

    return validate(data, len);
}

// If data represents a correct UTF-8 string, return std::nullopt,
// otherwise return a position of first error byte.
std::optional<size_t> validate_with_error_position(const uint8_t *data, size_t len);

inline std::optional<size_t> validate_with_error_position(bytes_view string) {
    const uint8_t *data = reinterpret_cast<const uint8_t*>(string.data());
    size_t len = string.size();

    return validate_with_error_position(data, len);	
}

} // namespace utf8

} // namespace utils
