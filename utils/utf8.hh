/*
 * Leverage SIMD for fast UTF-8 validation with range base algorithm.
 * Details at https://github.com/cyb70289/utf8/.
 *
 * Copyright (c) 2018, Arm Limited and affiliates. All rights reserved.
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include "bytes.hh"
#include "fragment_range.hh"

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

inline std::optional<size_t> validate_with_error_position_fragmented(single_fragmented_view fv) {
    return validate_with_error_position(fv.current_fragment());
}

std::optional<size_t> validate_with_error_position_fragmented(FragmentedView auto fv) {
    uint8_t partial_codepoint[4];
    size_t partial_filled = 0;
    size_t partial_more_needed = 0;
    size_t bytes_validated = 0;
    for (bytes_view frag : fragment_range(fv)) {
        auto data = reinterpret_cast<const uint8_t*>(frag.data());
        auto len = frag.size();
        if (partial_more_needed) {
            // Tiny loop (often zero iterations), don't call memcpy
            while (partial_more_needed && len) {
                partial_codepoint[partial_filled++] = *data++;
                --partial_more_needed;
                --len;
            }
            if (!partial_more_needed) {
                // We accumulated a codepoint that straddled two or more fragments,
                // validate it now.
                auto pvr = internal::validate_partial(partial_codepoint, partial_filled);
                if (pvr.error) {
                    return {bytes_validated};
                }
                bytes_validated += partial_filled;
                partial_filled = partial_more_needed = 0;
            }
            if (!len) {
                continue;
            }
        }
        auto pvr = internal::validate_partial(data, len);
        if (pvr.error) {
            return bytes_validated + *validate_with_error_position(data, len);
        }
        bytes_validated += len - pvr.unvalidated_tail;
        data += len - pvr.unvalidated_tail;
        len = pvr.unvalidated_tail;
        while (len) {
            partial_codepoint[partial_filled++] = *data++;
            --len;
        }
        partial_more_needed = pvr.bytes_needed_for_tail;
    }
    if (partial_more_needed) {
        return bytes_validated;
    }
    return std::nullopt;
}

} // namespace utf8

} // namespace utils
