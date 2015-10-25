/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <list>
#include <map>
#include <vector>
#include <algorithm>
#include <iterator>

namespace sstables {

class downsampling {
public:
    /**
     * The base (down)sampling level determines the granularity at which we can down/upsample.
     *
     * A higher number allows us to approximate more closely the ideal sampling.  (It could also mean we do a lot of
     * expensive almost-no-op resamplings from N to N-1, but the thresholds in IndexSummaryManager prevent that.)
     *
     * BSL must be a power of two in order to have good sampling patterns. This cannot be changed without rebuilding
     * all index summaries at full sampling; for now we treat it as a constant.
     */
    static constexpr int BASE_SAMPLING_LEVEL = 128;

    static thread_local std::array<std::vector<int>, BASE_SAMPLING_LEVEL> _sample_pattern_cache;

    static thread_local std::array<std::vector<int>, BASE_SAMPLING_LEVEL> _original_index_cache;

    /**
     * Gets a list L of starting indices for downsampling rounds: the first round should start with the offset
     * given by L[0], the second by the offset in L[1], etc.
     *
     * @param sampling_level the base sampling level
     *
     * @return A list of `sampling_level` unique indices between 0 and `sampling_level`
     */
    static const std::vector<int>& get_sampling_pattern(int sampling_level) {
        assert(sampling_level > 0 && sampling_level <= BASE_SAMPLING_LEVEL);
        auto& entry = _sample_pattern_cache[sampling_level-1];
        if (!entry.empty()) {
            return entry;
        }

        if (sampling_level <= 1) {
            assert(_sample_pattern_cache[0].empty());
            _sample_pattern_cache[0].push_back(0);
            return _sample_pattern_cache[0];
        }

        std::vector<int> odds;
        std::vector<int> evens;
        odds.resize(sampling_level / 2);
        evens.resize(sampling_level / 2);
        for (int i = 1; i < sampling_level; i += 2) {
            odds[i/2] = i;
        }
        for (int i = 0; i < sampling_level; i += 2) {
            evens[i/2] = i;
        }

        // especially for latter rounds, it's important that we spread out the start points, so we'll
        // make a recursive call to get an ordering for this list of start points
        const std::vector<int>& ordering = get_sampling_pattern(sampling_level/2);
        std::vector<int> start_indices;
        start_indices.reserve(sampling_level);

        for (auto index : ordering) {
            start_indices.push_back(odds[index]);
        }
        for (auto index : ordering) {
            start_indices.push_back(evens[index]);
        }

        _sample_pattern_cache[sampling_level-1] = std::move(start_indices);
        return _sample_pattern_cache[sampling_level-1];
    }

    /**
     * Returns a list that can be used to translate current index summary indexes to their original index before
     * downsampling.  (This repeats every `sampling_level`, so that's how many entries we return.)
     *
     * For example, if [7, 15] is returned, the current index summary entry at index 0 was originally
     * at index 7, and the current index 1 was originally at index 15.
     *
     * @param sampling_level the current sampling level for the index summary
     *
     * @return a list of original indexes for current summary entries
     */
    static const std::vector<int>& get_original_indexes(int sampling_level) {
        assert(sampling_level > 0 && sampling_level <= BASE_SAMPLING_LEVEL);
        auto& entry = _original_index_cache[sampling_level-1];
        if (!entry.empty()) {
            return entry;
        }

        const std::vector<int>& pattern = get_sampling_pattern(BASE_SAMPLING_LEVEL);
        std::vector<int> original_indexes;

        auto pattern_end = pattern.begin() + (BASE_SAMPLING_LEVEL - sampling_level);
        for (int j = 0; j < BASE_SAMPLING_LEVEL; j++) {
            auto it = std::find(pattern.begin(), pattern_end, j);
            if (it == pattern_end) {
                // add j to original_indexes if not found in pattern.
                original_indexes.push_back(j);
            }
        }

        _original_index_cache[sampling_level-1] = std::move(original_indexes);
        return _original_index_cache[sampling_level-1];
    }

    /**
     * Calculates the effective index interval after the entry at `index` in an IndexSummary.  In other words, this
     * returns the number of partitions in the primary on-disk index before the next partition that has an entry in
     * the index summary.  If sampling_level == BASE_SAMPLING_LEVEL, this will be equal to the index interval.
     * @param index an index into an IndexSummary
     * @param sampling_level the current sampling level for that IndexSummary
     * @param min_index_interval the min index interval (effective index interval at full sampling)
     * @return the number of partitions before the next index summary entry, inclusive on one end
     */
    static int get_effective_index_interval_after_index(int index, int sampling_level, int min_index_interval) {
        assert(index >= -1);
        const std::vector<int>& original_indexes = get_original_indexes(sampling_level);
        if (index == -1) {
            return original_indexes[0] * min_index_interval;
        }

        index %= sampling_level;
        if (size_t(index) == original_indexes.size() - 1) {
            // account for partitions after the "last" entry as well as partitions before the "first" entry
            return ((BASE_SAMPLING_LEVEL - original_indexes[index]) + original_indexes[0]) * min_index_interval;
        } else {
            return (original_indexes[index + 1] - original_indexes[index]) * min_index_interval;
        }
    }
#if 0
    public static int[] getStartPoints(int currentSamplingLevel, int newSamplingLevel)
    {
        List<Integer> allStartPoints = getSamplingPattern(BASE_SAMPLING_LEVEL);

        // calculate starting indexes for sampling rounds
        int initialRound = BASE_SAMPLING_LEVEL - currentSamplingLevel;
        int numRounds = Math.abs(currentSamplingLevel - newSamplingLevel);
        int[] startPoints = new int[numRounds];
        for (int i = 0; i < numRounds; ++i)
        {
            int start = allStartPoints.get(initialRound + i);

            // our "ideal" start points will be affected by the removal of items in earlier rounds, so go through all
            // earlier rounds, and if we see an index that comes before our ideal start point, decrement the start point
            int adjustment = 0;
            for (int j = 0; j < initialRound; ++j)
            {
                if (allStartPoints.get(j) < start)
                    adjustment++;
            }
            startPoints[i] = start - adjustment;
        }
        return startPoints;
    }
#endif
};

}
