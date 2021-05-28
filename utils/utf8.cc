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

/*
 * http://www.unicode.org/versions/Unicode6.0.0/ch03.pdf - page 94
 *
 * Table 3-7. Well-Formed UTF-8 Byte Sequences
 *
 * +--------------------+------------+-------------+------------+-------------+
 * | Code Points        | First Byte | Second Byte | Third Byte | Fourth Byte |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0000..U+007F     | 00..7F     |             |            |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0080..U+07FF     | C2..DF     | 80..BF      |            |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+0800..U+0FFF     | E0         | A0..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+1000..U+CFFF     | E1..EC     | 80..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+D000..U+D7FF     | ED         | 80..9F      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+E000..U+FFFF     | EE..EF     | 80..BF      | 80..BF     |             |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+10000..U+3FFFF   | F0         | 90..BF      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+40000..U+FFFFF   | F1..F3     | 80..BF      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 * | U+100000..U+10FFFF | F4         | 80..8F      | 80..BF     | 80..BF      |
 * +--------------------+------------+-------------+------------+-------------+
 */

#include "utf8.hh"

namespace utils {

namespace utf8 {

using namespace internal;

struct codepoint_status {
    size_t bytes_validated;
    bool error;
    uint8_t more_bytes_needed;
};

static
codepoint_status
inline
evaluate_codepoint(const uint8_t* data, size_t len) {
    const uint8_t byte1 = data[0];
    static const uint8_t len_from_first_nibble[16] = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 4 };
    auto codepoint_len = len_from_first_nibble[byte1 >> 4];
    if (codepoint_len > len) {
        return codepoint_status{.more_bytes_needed = uint8_t(codepoint_len - len)};
    } else {
        if (byte1 <= 0x7F) {
            // 00..7F
            return codepoint_status{.bytes_validated = codepoint_len};
        } else if (len >= 2 && byte1 >= 0xC2 && byte1 <= 0xDF &&
                (int8_t)data[1] <= (int8_t)0xBF) {
            // C2..DF, 80..BF
            return codepoint_status{.bytes_validated = codepoint_len};
        } else if (len >= 3) {
            const uint8_t byte2 = data[1];

            // Is byte2, byte3 between 0x80 ~ 0xBF
            const int byte2_ok = (int8_t)byte2 <= (int8_t)0xBF;
            const int byte3_ok = (int8_t)data[2] <= (int8_t)0xBF;

            if (byte2_ok && byte3_ok &&
                     // E0, A0..BF, 80..BF
                    ((byte1 == 0xE0 && byte2 >= 0xA0) ||
                     // E1..EC, 80..BF, 80..BF
                     (byte1 >= 0xE1 && byte1 <= 0xEC) ||
                     // ED, 80..9F, 80..BF
                     (byte1 == 0xED && byte2 <= 0x9F) ||
                     // EE..EF, 80..BF, 80..BF
                     (byte1 >= 0xEE && byte1 <= 0xEF))) {
                return codepoint_status{.bytes_validated = codepoint_len};
            } else if (len >= 4) {
                // Is byte4 between 0x80 ~ 0xBF
                const int byte4_ok = (int8_t)data[3] <= (int8_t)0xBF;

                if (byte2_ok && byte3_ok && byte4_ok &&
                         // F0, 90..BF, 80..BF, 80..BF
                        ((byte1 == 0xF0 && byte2 >= 0x90) ||
                         // F1..F3, 80..BF, 80..BF, 80..BF
                         (byte1 >= 0xF1 && byte1 <= 0xF3) ||
                         // F4, 80..8F, 80..BF, 80..BF
                         (byte1 == 0xF4 && byte2 <= 0x8F))) {
                    return codepoint_status{.bytes_validated = codepoint_len};
                } else {
                    return codepoint_status{.error = true};
                }
            } else {
                return codepoint_status{.error = true};
            }
        } else {
            return codepoint_status{.error = true};
        }
    }
}

// 3x faster than boost utf_to_utf
static inline std::optional<size_t> validate_naive(const uint8_t *data, size_t len) {
    size_t pos = 0;

    while (len) {
        auto cs = evaluate_codepoint(data, len);
        pos += cs.bytes_validated;
        data += cs.bytes_validated;
        len -= cs.bytes_validated;
        if (cs.error || cs.more_bytes_needed) {
            return pos;
        }
    }

    return std::nullopt;
}

static
partial_validation_results
validate_partial_naive(const uint8_t *data, size_t len) {
    while (len) {
        auto cs = evaluate_codepoint(data, len);
        data += cs.bytes_validated;
        len -= cs.bytes_validated;
        if (cs.error) {
            return partial_validation_results{.error = true};
        }
        if (cs.more_bytes_needed) {
            return partial_validation_results{.unvalidated_tail = len, .bytes_needed_for_tail = cs.more_bytes_needed};
        }
    }
    return partial_validation_results{};
}

} // namespace utf8

} // namespace utils

#if defined(__aarch64__)
#include <arm_neon.h>

namespace utils {

namespace utf8 {

// Map high nibble of "First Byte" to legal character length minus 1
// 0x00 ~ 0xBF --> 0
// 0xC0 ~ 0xDF --> 1
// 0xE0 ~ 0xEF --> 2
// 0xF0 ~ 0xFF --> 3
alignas(16) static const uint8_t s_first_len_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3,
};

// Map "First Byte" to 8-th item of range table (0xC2 ~ 0xF4)
alignas(16) static const uint8_t s_first_range_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8,
};

// Range table, map range index to min and max values
// Index 0    : 00 ~ 7F (First Byte, ascii)
// Index 1,2,3: 80 ~ BF (Second, Third, Fourth Byte)
// Index 4    : A0 ~ BF (Second Byte after E0)
// Index 5    : 80 ~ 9F (Second Byte after ED)
// Index 6    : 90 ~ BF (Second Byte after F0)
// Index 7    : 80 ~ 8F (Second Byte after F4)
// Index 8    : C2 ~ F4 (First Byte, non ascii)
// Index 9~15 : illegal: u >= 255 && u <= 0
alignas(16) static const uint8_t s_range_min_tbl[] = {
    0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80,
    0xC2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
};
alignas(16) static const uint8_t s_range_max_tbl[] = {
    0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F,
    0xF4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
};

// This table is for fast handling four special First Bytes(E0,ED,F0,F4), after
// which the Second Byte are not 80~BF. It contains "range index adjustment".
// - The idea is to minus byte with E0, use the result(0~31) as the index to
//   lookup the "range index adjustment". Then add the adjustment to original
//   range index to get the correct range.
// - Range index adjustment
//   +------------+---------------+------------------+----------------+
//   | First Byte | original range| range adjustment | adjusted range |
//   +------------+---------------+------------------+----------------+
//   | E0         | 2             | 2                | 4              |
//   +------------+---------------+------------------+----------------+
//   | ED         | 2             | 3                | 5              |
//   +------------+---------------+------------------+----------------+
//   | F0         | 3             | 3                | 6              |
//   +------------+---------------+------------------+----------------+
//   | F4         | 4             | 4                | 8              |
//   +------------+---------------+------------------+----------------+
// - Below is a uint8x16x2 table, data is interleaved in NEON register. So I'm
//   putting it vertically. 1st column is for E0~EF, 2nd column for F0~FF.
alignas(16) static const uint8_t s_range_adjust_tbl[] = {
    /* index -> 0~15  16~31 <- index */
    /*  E0 -> */ 2,     3, /* <- F0  */
                 0,     0,
                 0,     0,
                 0,     0,
                 0,     4, /* <- F4  */
                 0,     0,
                 0,     0,
                 0,     0,
                 0,     0,
                 0,     0,
                 0,     0,
                 0,     0,
                 0,     0,
    /*  ED -> */ 3,     0,
                 0,     0,
                 0,     0,
};

// 2x ~ 4x faster than naive method
partial_validation_results
internal::validate_partial(const uint8_t *data, size_t len) {
    if (len >= 16) {
        uint8x16_t prev_input = vdupq_n_u8(0);
        uint8x16_t prev_first_len = vdupq_n_u8(0);

        // Cached tables
        const uint8x16_t first_len_tbl = vld1q_u8(s_first_len_tbl);
        const uint8x16_t first_range_tbl = vld1q_u8(s_first_range_tbl);
        const uint8x16_t range_min_tbl = vld1q_u8(s_range_min_tbl);
        const uint8x16_t range_max_tbl = vld1q_u8(s_range_max_tbl);
        const uint8x16x2_t range_adjust_tbl = vld2q_u8(s_range_adjust_tbl);

        // Cached values
        const uint8x16_t const_1 = vdupq_n_u8(1);
        const uint8x16_t const_2 = vdupq_n_u8(2);
        const uint8x16_t const_e0 = vdupq_n_u8(0xE0);

        uint8x16_t error = vdupq_n_u8(0);

        while (len >= 16) {
            const uint8x16_t input = vld1q_u8(data);

            // high_nibbles = input >> 4
            const uint8x16_t high_nibbles = vshrq_n_u8(input, 4);

            // first_len = legal character length minus 1
            // 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF
            // first_len = first_len_tbl[high_nibbles]
            const uint8x16_t first_len =
                vqtbl1q_u8(first_len_tbl, high_nibbles);

            // First Byte: set range index to 8 for bytes within 0xC0 ~ 0xFF
            // range = first_range_tbl[high_nibbles]
            uint8x16_t range = vqtbl1q_u8(first_range_tbl, high_nibbles);

            // Second Byte: set range index to first_len
            // 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF
            // range |= (first_len, prev_first_len) << 1 byte
            range =
                vorrq_u8(range, vextq_u8(prev_first_len, first_len, 15));

            // Third Byte: set range index to saturate_sub(first_len, 1)
            // 0 for 00~7F, 0 for C0~DF, 1 for E0~EF, 2 for F0~FF
            uint8x16_t tmp1, tmp2;
            // tmp1 = saturate_sub(first_len, 1)
            tmp1 = vqsubq_u8(first_len, const_1);
            // tmp2 = saturate_sub(prev_first_len, 1)
            tmp2 = vqsubq_u8(prev_first_len, const_1);
            // range |= (tmp1, tmp2) << 2 bytes
            range = vorrq_u8(range, vextq_u8(tmp2, tmp1, 14));

            // Fourth Byte: set range index to saturate_sub(first_len, 2)
            // 0 for 00~7F, 0 for C0~DF, 0 for E0~EF, 1 for F0~FF
            // tmp1 = saturate_sub(first_len, 2)
            tmp1 = vqsubq_u8(first_len, const_2);
            // tmp2 = saturate_sub(prev_first_len, 2)
            tmp2 = vqsubq_u8(prev_first_len, const_2);
            // range |= (tmp1, tmp2) << 3 bytes
            range = vorrq_u8(range, vextq_u8(tmp2, tmp1, 13));

            // Now we have below range indices caluclated
            // Correct cases:
            // - 8 for C0~FF
            // - 3 for 1st byte after F0~FF
            // - 2 for 1st byte after E0~EF or 2nd byte after F0~FF
            // - 1 for 1st byte after C0~DF or 2nd byte after E0~EF or
            //         3rd byte after F0~FF
            // - 0 for others
            // Error cases:
            //   9,10,11 if non ascii First Byte overlaps
            //   E.g., F1 80 C2 90 --> 8 3 10 2, where 10 indicates error

            // Adjust Second Byte range for special First Bytes(E0,ED,F0,F4)
            // See s_range_adjust_tbl[] definition for details
            // Overlaps lead to index 9~15, which are illegal in range table
            uint8x16_t shift1 = vextq_u8(prev_input, input, 15);
            uint8x16_t pos = vsubq_u8(shift1, const_e0);
            range = vaddq_u8(range, vqtbl2q_u8(range_adjust_tbl, pos));

            // Load min and max values per calculated range index
            uint8x16_t minv = vqtbl1q_u8(range_min_tbl, range);
            uint8x16_t maxv = vqtbl1q_u8(range_max_tbl, range);

            // Check value range
            error = vorrq_u8(error, vcltq_u8(input, minv));
            error = vorrq_u8(error, vcgtq_u8(input, maxv));

            prev_input = input;
            prev_first_len = first_len;

            data += 16;
            len -= 16;
        }

        // Delay error check till loop ends
        if (vmaxvq_u8(error)) {
            return partial_validation_results{.error = true};
        }

        // Find previous token (not 80~BF)
        uint32_t token4;
        vst1q_lane_u32(&token4, vreinterpretq_u32_u8(prev_input), 3);

        const int8_t *token = (const int8_t *)&token4;
        int lookahead = 0;
        if (token[3] > (int8_t)0xBF) {
            lookahead = 1;
        } else if (token[2] > (int8_t)0xBF) {
            lookahead = 2;
        } else if (token[1] > (int8_t)0xBF) {
            lookahead = 3;
        }
        data -= lookahead;
        len += lookahead;
    }

    // Continue with remaining bytes with naive method
    return validate_partial_naive(data, len);
}

} // namespace utf8

} // namespace utils

#elif defined(__x86_64__)
#include <smmintrin.h>

namespace utils {

namespace utf8 {

// Map high nibble of "First Byte" to legal character length minus 1
// 0x00 ~ 0xBF --> 0
// 0xC0 ~ 0xDF --> 1
// 0xE0 ~ 0xEF --> 2
// 0xF0 ~ 0xFF --> 3
alignas(16) static const int8_t s_first_len_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3,
};

// Map "First Byte" to 8-th item of range table (0xC2 ~ 0xF4)
alignas(16) static const int8_t s_first_range_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8,
};

// Range table, map range index to min and max values
// Index 0    : 00 ~ 7F (First Byte, ascii)
// Index 1,2,3: 80 ~ BF (Second, Third, Fourth Byte)
// Index 4    : A0 ~ BF (Second Byte after E0)
// Index 5    : 80 ~ 9F (Second Byte after ED)
// Index 6    : 90 ~ BF (Second Byte after F0)
// Index 7    : 80 ~ 8F (Second Byte after F4)
// Index 8    : C2 ~ F4 (First Byte, non ascii)
// Index 9~15 : illegal: i >= 127 && i <= -128
alignas(16) static const int8_t s_range_min_tbl[] = {
    '\x00', '\x80', '\x80', '\x80', '\xA0', '\x80', '\x90', '\x80',
    '\xC2', '\x7F', '\x7F', '\x7F', '\x7F', '\x7F', '\x7F', '\x7F',
};
alignas(16) static const int8_t s_range_max_tbl[] = {
    '\x7F', '\xBF', '\xBF', '\xBF', '\xBF', '\x9F', '\xBF', '\x8F',
    '\xF4', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80', '\x80',
};

// Tables for fast handling of four special First Bytes(E0,ED,F0,F4), after
// which the Second Byte are not 80~BF. It contains "range index adjustment".
// +------------+---------------+------------------+----------------+
// | First Byte | original range| range adjustment | adjusted range |
// +------------+---------------+------------------+----------------+
// | E0         | 2             | 2                | 4              |
// +------------+---------------+------------------+----------------+
// | ED         | 2             | 3                | 5              |
// +------------+---------------+------------------+----------------+
// | F0         | 3             | 3                | 6              |
// +------------+---------------+------------------+----------------+
// | F4         | 4             | 4                | 8              |
// +------------+---------------+------------------+----------------+
// index1 -> E0, index14 -> ED
alignas(16) static const int8_t s_df_ee_tbl[] = {
    0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0,
};
// index1 -> F0, index5 -> F4
alignas(16) static const int8_t s_ef_fe_tbl[] = {
    0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
};

// 5x faster than naive method
partial_validation_results
internal::validate_partial(const uint8_t *data, size_t len) {
    if (len >= 16) {
        __m128i prev_input = _mm_set1_epi8(0);
        __m128i prev_first_len = _mm_set1_epi8(0);

        // Cached tables
        const __m128i first_len_tbl = _mm_load_si128((const __m128i *)s_first_len_tbl);
        const __m128i first_range_tbl = _mm_load_si128((const __m128i *)s_first_range_tbl);
        const __m128i range_min_tbl = _mm_load_si128((const __m128i *)s_range_min_tbl);
        const __m128i range_max_tbl = _mm_load_si128((const __m128i *)s_range_max_tbl);
        const __m128i df_ee_tbl = _mm_load_si128((const __m128i *)s_df_ee_tbl);
        const __m128i ef_fe_tbl = _mm_load_si128((const __m128i *)s_ef_fe_tbl);

        __m128i error = _mm_set1_epi8(0);

        while (len >= 16) {
            const __m128i input = _mm_lddqu_si128((const __m128i *)data);

            // high_nibbles = input >> 4
            const __m128i high_nibbles =
                _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));

            // first_len = legal character length minus 1
            // 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF
            // first_len = first_len_tbl[high_nibbles]
            __m128i first_len = _mm_shuffle_epi8(first_len_tbl, high_nibbles);

            // First Byte: set range index to 8 for bytes within 0xC0 ~ 0xFF
            // range = first_range_tbl[high_nibbles]
            __m128i range = _mm_shuffle_epi8(first_range_tbl, high_nibbles);

            // Second Byte: set range index to first_len
            // 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF
            // range |= (first_len, prev_first_len) << 1 byte
            range = _mm_or_si128(
                    range, _mm_alignr_epi8(first_len, prev_first_len, 15));

            // Third Byte: set range index to saturate_sub(first_len, 1)
            // 0 for 00~7F, 0 for C0~DF, 1 for E0~EF, 2 for F0~FF
            __m128i tmp1, tmp2;
            // tmp1 = saturate_sub(first_len, 1)
            tmp1 = _mm_subs_epu8(first_len, _mm_set1_epi8(1));
            // tmp2 = saturate_sub(prev_first_len, 1)
            tmp2 = _mm_subs_epu8(prev_first_len, _mm_set1_epi8(1));
            // range |= (tmp1, tmp2) << 2 bytes
            range = _mm_or_si128(range, _mm_alignr_epi8(tmp1, tmp2, 14));

            // Fourth Byte: set range index to saturate_sub(first_len, 2)
            // 0 for 00~7F, 0 for C0~DF, 0 for E0~EF, 1 for F0~FF
            // tmp1 = saturate_sub(first_len, 2)
            tmp1 = _mm_subs_epu8(first_len, _mm_set1_epi8(2));
            // tmp2 = saturate_sub(prev_first_len, 2)
            tmp2 = _mm_subs_epu8(prev_first_len, _mm_set1_epi8(2));
            // range |= (tmp1, tmp2) << 3 bytes
            range = _mm_or_si128(range, _mm_alignr_epi8(tmp1, tmp2, 13));

            // Now we have below range indices caluclated
            // Correct cases:
            // - 8 for C0~FF
            // - 3 for 1st byte after F0~FF
            // - 2 for 1st byte after E0~EF or 2nd byte after F0~FF
            // - 1 for 1st byte after C0~DF or 2nd byte after E0~EF or
            //         3rd byte after F0~FF
            // - 0 for others
            // Error cases:
            //   9,10,11 if non ascii First Byte overlaps
            //   E.g., F1 80 C2 90 --> 8 3 10 2, where 10 indicates error

            // Adjust Second Byte range for special First Bytes(E0,ED,F0,F4)
            // Overlaps lead to index 9~15, which are illegal in range table
            __m128i shift1, pos, range2;
            // shift1 = (input, prev_input) << 1 byte
            shift1 = _mm_alignr_epi8(input, prev_input, 15);
            pos = _mm_sub_epi8(shift1, _mm_set1_epi8(0xEF));
            // shift1:  | EF  F0 ... FE | FF  00  ... ...  DE | DF  E0 ... EE |
            // pos:     | 0   1      15 | 16  17           239| 240 241    255|
            // pos-240: | 0   0      0  | 0   0            0  | 0   1      15 |
            // pos+112: | 112 113    127|       >= 128        |     >= 128    |
            tmp1 = _mm_subs_epu8(pos, _mm_set1_epi8(char(240)));
            range2 = _mm_shuffle_epi8(df_ee_tbl, tmp1);
            tmp2 = _mm_adds_epu8(pos, _mm_set1_epi8(112));
            range2 = _mm_add_epi8(range2, _mm_shuffle_epi8(ef_fe_tbl, tmp2));

            range = _mm_add_epi8(range, range2);

            // Load min and max values per calculated range index
            __m128i minv = _mm_shuffle_epi8(range_min_tbl, range);
            __m128i maxv = _mm_shuffle_epi8(range_max_tbl, range);

            // Check value range
            error = _mm_or_si128(error, _mm_cmplt_epi8(input, minv));
            error = _mm_or_si128(error, _mm_cmpgt_epi8(input, maxv));

            prev_input = input;
            prev_first_len = first_len;

            data += 16;
            len -= 16;
        }

        // Reduce error vector, error_reduced = 0xFFFF if error == 0
        int error_reduced =
            _mm_movemask_epi8(_mm_cmpeq_epi8(error, _mm_set1_epi8(0)));
        if (error_reduced != 0xFFFF) {
            return partial_validation_results{.error = true};
        }

        // Find previous token (not 80~BF)
        int32_t token4 = _mm_extract_epi32(prev_input, 3);
        const int8_t *token = (const int8_t *)&token4;
        int lookahead = 0;
        if (token[3] > (int8_t)0xBF) {
            lookahead = 1;
        } else if (token[2] > (int8_t)0xBF) {
            lookahead = 2;
        } else if (token[1] > (int8_t)0xBF) {
            lookahead = 3;
        }
        data -= lookahead;
        len += lookahead;
    }

    // Continue with remaining bytes with naive method
    return validate_partial_naive(data, len);
}

} // namespace utf8

} // namespace utils

#else

namespace utils {

namespace utf8 {

namespace internal {

// No SIMD implementation for this arch, fallback to naive method
partial_validation_results
validate_partial(const uint8_t *data, size_t len) {
    return validate_partial_naive(data, len);
}

}

} // namespace utf8

} // namespace utils

#endif

namespace utils {

namespace utf8 {

bool validate(const uint8_t* data, size_t len) {
    auto pvr = validate_partial(data, len);
    return !pvr.error && !pvr.unvalidated_tail;
}

std::optional<size_t> validate_with_error_position(const uint8_t *data, size_t len) {
    // First pass - validate data (using optimized code)
    if (validate(data, len)) {
        return std::nullopt;
    }
    // Second pass - data is invalid. Find the error position using naive method
    return validate_naive(data, len);
}

} // namespace utf8

} // namespace utils
