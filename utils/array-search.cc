/*
 * Copyright (C) 2020 ScyllaDB
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


#include "array-search.hh"
#ifdef __x86_64__
#include <x86intrin.h>
#define arch_target(name) [[gnu::target(name)]]
#else
#define arch_target(name)
#endif

namespace utils {

arch_target("default") int array_search_gt_impl(int64_t val, const int64_t* array, const int capacity, const int size) {
    int i;

    for (i = 0; i < size; i++) {
        if (val < array[i])
            break;
    }

    return i;
}

#ifdef __x86_64__

/*
 * The AVX2 version doesn't take @size argument into account and expects
 * all the elements above it to be less than any possible value.
 *
 * To make it work without this requirement we'd need to:
 *  - limit the loop iterations to size instead of capacity
 *  - explicitly set to 1 all the mask's bits for elements >= size
 * both do make things up to 50% slower.
 */

arch_target("avx2") int array_search_gt_impl(int64_t val, const int64_t* array, const int capacity, const int size) {
    int cnt = 0;

    // 0. Load key into 256-bit ymm
    __m256i k = _mm256_set1_epi64x(val);
    for (int i = 0; i < capacity; i += 4) {
        // 4. Count the number of 1-s, each gt match gives 8 bits
        cnt += _mm_popcnt_u32(
                    // 3. Pack result into 4 bytes -- 1 byte from each comparison
                    _mm256_movemask_epi8(
                        // 2. Compare array[i] > key, 4 elements in one go
                        _mm256_cmpgt_epi64(
                            // 1. Load next 4 elements into ymm
                            _mm256_lddqu_si256((__m256i*)&array[i]), k
                        )
                    )
                ) / 8;
    }

    /*
     * 5. We need the index of the first gt value. Unused elements are < k
     *    for sure, so count from the tail of the used part.
     *
     *   <grumble>
     *    We might have done it the other way -- store the maximum in unused,
     *    check for key >= array[i] in the above loop and just return the cnt,
     *    but ...  AVX2 instructions set doesn't have the PCMPGE
     *
     *    SSE* set (predecessor) has cmpge, but eats 2 keys in one go
     *    AVX-512 (successor) has it back, and even eats 8 keys, but is
     *    not widely available
     *   </grumble>
     */
    return size - cnt;
}

#endif

int array_search_gt(int64_t val, const int64_t* array, const int capacity, const int size) {
    return array_search_gt_impl(val, array, capacity, size);
}

}
