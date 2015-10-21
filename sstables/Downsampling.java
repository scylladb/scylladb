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
package org.apache.cassandra.io.sstable;

import java.util.*;

public class Downsampling
{
    /**
     * The base (down)sampling level determines the granularity at which we can down/upsample.
     *
     * A higher number allows us to approximate more closely the ideal sampling.  (It could also mean we do a lot of
     * expensive almost-no-op resamplings from N to N-1, but the thresholds in IndexSummaryManager prevent that.)
     *
     * BSL must be a power of two in order to have good sampling patterns. This cannot be changed without rebuilding
     * all index summaries at full sampling; for now we treat it as a constant.
     */
    public static final int BASE_SAMPLING_LEVEL = 128;

    private static final Map<Integer, List<Integer>> samplePatternCache = new HashMap<>();

    private static final Map<Integer, List<Integer>> originalIndexCache = new HashMap<>();

    /**
     * Gets a list L of starting indices for downsampling rounds: the first round should start with the offset
     * given by L[0], the second by the offset in L[1], etc.
     *
     * @param samplingLevel the base sampling level
     *
     * @return A list of `samplingLevel` unique indices between 0 and `samplingLevel`
     */
    public static List<Integer> getSamplingPattern(int samplingLevel)
    {
        List<Integer> pattern = samplePatternCache.get(samplingLevel);
        if (pattern != null)
            return pattern;

        if (samplingLevel <= 1)
            return Arrays.asList(0);

        int[] odds = new int[samplingLevel / 2];
        int[] evens = new int[samplingLevel / 2];
        for (int i = 1; i < samplingLevel; i += 2)
            odds[i/2] = i;
        for (int i = 0; i < samplingLevel; i += 2)
            evens[i/2] = i;

        // especially for latter rounds, it's important that we spread out the start points, so we'll
        // make a recursive call to get an ordering for this list of start points
        List<Integer> ordering = getSamplingPattern(samplingLevel/2);
        List<Integer> startIndices = new ArrayList<>(samplingLevel);

        for (Integer index : ordering)
            startIndices.add(odds[index]);
        for (Integer index : ordering)
            startIndices.add(evens[index]);

        samplePatternCache.put(samplingLevel, startIndices);
        return startIndices;
    }

    /**
     * Returns a list that can be used to translate current index summary indexes to their original index before
     * downsampling.  (This repeats every `samplingLevel`, so that's how many entries we return.)
     *
     * For example, if [0, 64] is returned, the current index summary entry at index 0 was originally
     * at index 0, and the current index 1 was originally at index 64.
     *
     * @param samplingLevel the current sampling level for the index summary
     *
     * @return a list of original indexes for current summary entries
     */
    public static List<Integer> getOriginalIndexes(int samplingLevel)
    {
        List<Integer> originalIndexes = originalIndexCache.get(samplingLevel);
        if (originalIndexes != null)
            return originalIndexes;

        List<Integer> pattern = getSamplingPattern(BASE_SAMPLING_LEVEL).subList(0, BASE_SAMPLING_LEVEL - samplingLevel);
        originalIndexes = new ArrayList<>(samplingLevel);
        for (int j = 0; j < BASE_SAMPLING_LEVEL; j++)
        {
            if (!pattern.contains(j))
                originalIndexes.add(j);
        }

        originalIndexCache.put(samplingLevel, originalIndexes);
        return originalIndexes;
    }

    /**
     * Calculates the effective index interval after the entry at `index` in an IndexSummary.  In other words, this
     * returns the number of partitions in the primary on-disk index before the next partition that has an entry in
     * the index summary.  If samplingLevel == BASE_SAMPLING_LEVEL, this will be equal to the index interval.
     * @param index an index into an IndexSummary
     * @param samplingLevel the current sampling level for that IndexSummary
     * @param minIndexInterval the min index interval (effective index interval at full sampling)
     * @return the number of partitions before the next index summary entry, inclusive on one end
     */
    public static int getEffectiveIndexIntervalAfterIndex(int index, int samplingLevel, int minIndexInterval)
    {
        assert index >= 0;
        index %= samplingLevel;
        List<Integer> originalIndexes = getOriginalIndexes(samplingLevel);
        int nextEntryOriginalIndex = (index == originalIndexes.size() - 1) ? BASE_SAMPLING_LEVEL : originalIndexes.get(index + 1);
        return (nextEntryOriginalIndex - originalIndexes.get(index)) * minIndexInterval;
    }

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
}
