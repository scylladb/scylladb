/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include "sstables.hh"
#include "compaction.hh"
#include "range.hh"
#include "log.hh"

class leveled_manifest {
    logging::logger logger;

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    static constexpr int MAX_COMPACTING_L0 = 32;
    /**
     * If we go this many rounds without compacting
     * in the highest level, we start bringing in sstables from
     * that level into lower level compactions
     */
    static constexpr int NO_COMPACTION_LIMIT = 25;

    schema_ptr _schema;
    std::vector<std::list<sstables::shared_sstable>> _generations;
#if 0
    private final RowPosition[] lastCompactedKeys;
#endif
    uint64_t _max_sstable_size_in_bytes;
#if 0
    private final SizeTieredCompactionStrategyOptions options;
    private final int [] compactionCounter;
#endif

public:
    leveled_manifest(column_family& cfs, int max_sstable_size_in_MB)
        : logger("LeveledManifest")
        , _schema(cfs.schema())
        , _max_sstable_size_in_bytes(max_sstable_size_in_MB * 1024 * 1024)
    {
        // allocate enough generations for a PB of data, with a 1-MB sstable size.  (Note that if maxSSTableSize is
        // updated, we will still have sstables of the older, potentially smaller size.  So don't make this
        // dependent on maxSSTableSize.)
        uint64_t n = 9; // log10(1000^3)
        _generations.resize(n);
#if 0
        lastCompactedKeys = new RowPosition[n];
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<>();
            lastCompactedKeys[i] = cfs.partitioner.getMinimumToken().minKeyBound();
        }
        compactionCounter = new int[n];
#endif
    }

#if 0
    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, List<SSTableReader> sstables)
    {
        return create(cfs, maxSSTableSize, sstables, new SizeTieredCompactionStrategyOptions());
    }
#endif

    static leveled_manifest create(column_family& cfs, std::vector<sstables::shared_sstable>& sstables, int max_sstable_size_in_mb) {
        leveled_manifest manifest = leveled_manifest(cfs, max_sstable_size_in_mb);

        // ensure all SSTables are in the manifest
        for (auto& sstable : sstables) {
            manifest.add(sstable);
        }

        for (auto i = 1U; i < manifest._generations.size(); i++) {
            manifest.repair_overlapping_sstables(i);
        }

        return manifest;
    }

    void add(sstables::shared_sstable& sstable) {
        uint32_t level = sstable->get_sstable_level();

        if (level >= _generations.size()) {
            throw std::runtime_error(sprint("Invalid level %u out of %ld", level, (_generations.size() - 1)));
        }
#if 0
        logDistribution();
#endif
        if (can_add_sstable(sstable)) {
            // adding the sstable does not cause overlap in the level

            logger.debug("Adding {} to L{}", sstable->get_filename(), level);

            _generations[level].push_back(sstable);
        } else {
            // this can happen if:
            // * a compaction has promoted an overlapping sstable to the given level, or
            //   was also supposed to add an sstable at the given level.
            // * we are moving sstables from unrepaired to repaired and the sstable
            //   would cause overlap
            //
            // The add(..):ed sstable will be sent to level 0
#if 0
            try
            {
                reader.descriptor.getMetadataSerializer().mutateLevel(reader.descriptor, 0);
                reader.reloadSSTableMetadata();
            }
            catch (IOException e)
            {
                logger.error("Could not change sstable level - adding it at level 0 anyway, we will find it at restart.", e);
            }
#endif
            _generations[0].push_back(sstable);
        }
    }

#if 0
    public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        assert !removed.isEmpty(); // use add() instead of promote when adding new sstables
        logDistribution();
        if (logger.isDebugEnabled())
            logger.debug("Replacing [{}]", toString(removed));

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        int minLevel = Integer.MAX_VALUE;

        for (SSTableReader sstable : removed)
        {
            int thisLevel = remove(sstable);
            minLevel = Math.min(minLevel, thisLevel);
        }

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (added.isEmpty())
            return;

        if (logger.isDebugEnabled())
            logger.debug("Adding [{}]", toString(added));

        for (SSTableReader ssTableReader : added)
            add(ssTableReader);
        lastCompactedKeys[minLevel] = SSTableReader.sstableOrdering.max(added).last;
    }
#endif

    void repair_overlapping_sstables(int level) {
        const sstables::sstable *previous = nullptr;
        const schema& s = *_schema;

        _generations[level].sort([&s] (auto& i, auto& j) {
            return i->compare_by_first_key(s, *j) < 0;
        });

        std::vector<sstables::shared_sstable> out_of_order_sstables;

        for (auto& current : _generations[level]) {
            auto current_first = current->get_first_decorated_key(s);

            if (previous != nullptr && current_first.tri_compare(s, previous->get_last_decorated_key(s)) <= 0) {

                logger.warn("At level {}, {} [{}, {}] overlaps {} [{}, {}].  This could be caused by a bug in Cassandra 1.1.0 .. 1.1.3 " \
                    "or due to the fact that you have dropped sstables from another node into the data directory. " \
                    "Sending back to L0. If you didn't drop in sstables, and have not yet run scrub, you should do so since you may also " \
                    "have rows out-of-order within an sstable",
                    level, previous->get_filename(), previous->get_first_partition_key(s), previous->get_last_partition_key(s),
                    current->get_filename(), current->get_first_partition_key(s), current->get_last_partition_key(s));

                out_of_order_sstables.push_back(current);
            } else {
                previous = &*current;
            }
        }

        if (!out_of_order_sstables.empty()) {
            for (auto& sstable : out_of_order_sstables) {
                send_back_to_L0(sstable);
            }
        }
    }

    /**
     * Checks if adding the sstable creates an overlap in the level
     * @param sstable the sstable to add
     * @return true if it is safe to add the sstable in the level.
     */
    bool can_add_sstable(sstables::shared_sstable& sstable) {
        uint32_t level = sstable->get_sstable_level();
        const schema& s = *_schema;

        if (level == 0) {
            return true;
        }

        auto copy_level = _generations[level];
        copy_level.push_back(sstable);
        copy_level.sort([&s] (auto& i, auto& j) {
            return i->compare_by_first_key(s, *j) < 0;
        });

        const sstables::sstable *previous = nullptr;
        for (auto& current : copy_level) {
            if (previous != nullptr) {
                auto current_first = current->get_first_decorated_key(s);
                auto previous_last = previous->get_last_decorated_key(s);

                if (current_first.tri_compare(s, previous_last) <= 0) {
                    return false;
                }
            }
            previous = &*current;
        }

        return true;
    }

    void send_back_to_L0(sstables::shared_sstable& sstable) {
        remove(sstable);
#if 0
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
            sstable.reloadSSTableMetadata();
            add(sstable);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not reload sstable meta data", e);
        }
#else
        _generations[0].push_back(sstable);
#endif
    }

#if 0
    private String toString(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
        {
            builder.append(sstable.descriptor.cfname)
                   .append('-')
                   .append(sstable.descriptor.generation)
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }
#endif

    static uint64_t max_bytes_for_level(int level, uint64_t max_sstable_size_in_bytes) {
        if (level == 0) {
            return 4L * max_sstable_size_in_bytes;
        }
        double bytes = pow(10, level) * max_sstable_size_in_bytes;
        if (bytes > std::numeric_limits<int64_t>::max()) {
            throw std::runtime_error(sprint("At most %ld bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute %f", 
                std::numeric_limits<int64_t>::max(), bytes));
        }
        uint64_t bytes_u64 = bytes;
        return bytes_u64;
    }

    uint64_t max_bytes_for_level(int level) {
        return max_bytes_for_level(level, _max_sstable_size_in_bytes);
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    sstables::compaction_descriptor get_compaction_candidates() {
#if 0
        // during bootstrap we only do size tiering in L0 to make sure
        // the streamed files can be placed in their original levels
        if (StorageService.instance.isBootstrapMode())
        {
            List<SSTableReader> mostInteresting = getSSTablesForSTCS(getLevel(0));
            if (!mostInteresting.isEmpty())
            {
                logger.info("Bootstrapping - doing STCS in L0");
                return new CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE);
            }
            return null;
        }
#endif
        // LevelDB gives each level a score of how much data it contains vs its ideal amount, and
        // compacts the level with the highest score. But this falls apart spectacularly once you
        // get behind.  Consider this set of levels:
        // L0: 988 [ideal: 4]
        // L1: 117 [ideal: 10]
        // L2: 12  [ideal: 100]
        //
        // The problem is that L0 has a much higher score (almost 250) than L1 (11), so what we'll
        // do is compact a batch of MAX_COMPACTING_L0 sstables with all 117 L1 sstables, and put the
        // result (say, 120 sstables) in L1. Then we'll compact the next batch of MAX_COMPACTING_L0,
        // and so forth.  So we spend most of our i/o rewriting the L1 data with each batch.
        //
        // If we could just do *all* L0 a single time with L1, that would be ideal.  But we can't
        // -- see the javadoc for MAX_COMPACTING_L0.
        //
        // LevelDB's way around this is to simply block writes if L0 compaction falls behind.
        // We don't have that luxury.
        //
        // So instead, we
        // 1) force compacting higher levels first, which minimizes the i/o needed to compact
        //    optimially which gives us a long term win, and
        // 2) if L0 falls behind, we will size-tiered compact it to reduce read overhead until
        //    we can catch up on the higher levels.
        //
        // This isn't a magic wand -- if you are consistently writing too fast for LCS to keep
        // up, you're still screwed.  But if instead you have intermittent bursts of activity,
        // it can help a lot.
        for (auto i = _generations.size() - 1; i > 0; i--) {
            auto& sstables = get_level(i);
            if (sstables.empty()) {
                continue; // mostly this just avoids polluting the debug log with zero scores
            }
#if 0
            // we want to calculate score excluding compacting ones
            Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
            Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getDataTracker().getCompacting());
#endif
            double score = (double) get_total_bytes(sstables) / (double) max_bytes_for_level(i);

            logger.debug("Compaction score for level {} is {}", i, score);

            if (score > 1.001) {
#if 0
                // before proceeding with a higher level, let's see if L0 is far enough behind to warrant STCS
                if (!DatabaseDescriptor.getDisableSTCSInL0() && getLevel(0).size() > MAX_COMPACTING_L0)
                {
                    List<SSTableReader> mostInteresting = getSSTablesForSTCS(getLevel(0));
                    if (!mostInteresting.isEmpty())
                    {
                        logger.debug("L0 is too far behind, performing size-tiering there first");
                        return new CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE);
                    }
                }
#endif
                // L0 is fine, proceed with this level
                auto candidates = get_candidates_for(i);
                if (!candidates.empty()) {
                    int next_level = get_next_level(candidates);
#if 0
                    candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                    if (logger.isDebugEnabled())
                        logger.debug("Compaction candidates for L{} are {}", i, toString(candidates));
#endif
                    return sstables::compaction_descriptor(std::move(candidates), next_level, _max_sstable_size_in_bytes);
                }
                else {
                    logger.debug("No compaction candidates for L{}", i);
                }
            }
        }

        // Higher levels are happy, time for a standard, non-STCS L0 compaction
        if (get_level(0).empty()) {
            return sstables::compaction_descriptor();
        }
        auto candidates = get_candidates_for(0);
        if (candidates.empty()) {
            return sstables::compaction_descriptor();
        }
        auto next_level = get_next_level(candidates);
        return sstables::compaction_descriptor(std::move(candidates), next_level, _max_sstable_size_in_bytes);
    }

#if 0
    private List<SSTableReader> getSSTablesForSTCS(Collection<SSTableReader> sstables)
    {
        Iterable<SSTableReader> candidates = cfs.getDataTracker().getUncompactingSSTables(sstables);
        List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                    options.bucketHigh,
                                                                                    options.bucketLow,
                                                                                    options.minSSTableSize);
        return SizeTieredCompactionStrategy.mostInterestingBucket(buckets, 4, 32);
    }
#endif

    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
#if 0
    private Collection<SSTableReader> getOverlappingStarvedSSTables(int targetLevel, Collection<SSTableReader> candidates)
    {
        Set<SSTableReader> withStarvedCandidate = new HashSet<>(candidates);

        for (int i = generations.length - 1; i > 0; i--)
            compactionCounter[i]++;
        compactionCounter[targetLevel] = 0;
        if (logger.isDebugEnabled())
        {
            for (int j = 0; j < compactionCounter.length; j++)
                logger.debug("CompactionCounter: {}: {}", j, compactionCounter[j]);
        }

        for (int i = generations.length - 1; i > 0; i--)
        {
            if (getLevelSize(i) > 0)
            {
                if (compactionCounter[i] > NO_COMPACTION_LIMIT)
                {
                    // we try to find an sstable that is fully contained within  the boundaries we are compacting;
                    // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
                    // this means that we will not create overlap in L2 if we add an sstable
                    // contained within 0 -> 33 to the compaction
                    RowPosition max = null;
                    RowPosition min = null;
                    for (SSTableReader candidate : candidates)
                    {
                        if (min == null || candidate.first.compareTo(min) < 0)
                            min = candidate.first;
                        if (max == null || candidate.last.compareTo(max) > 0)
                            max = candidate.last;
                    }
                    Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
                    Range<RowPosition> boundaries = new Range<>(min, max);
                    for (SSTableReader sstable : getLevel(i))
                    {
                        Range<RowPosition> r = new Range<RowPosition>(sstable.first, sstable.last);
                        if (boundaries.contains(r) && !compacting.contains(sstable))
                        {
                            logger.info("Adding high-level (L{}) {} to candidates", sstable.getSSTableLevel(), sstable);
                            withStarvedCandidate.add(sstable);
                            return withStarvedCandidate;
                        }
                    }
                }
                return candidates;
            }
        }

        return candidates;
    }
#endif

    size_t get_level_size(uint32_t level) {
#if 0
        if (i >= generations.length)
            throw new ArrayIndexOutOfBoundsException("Maximum valid generation is " + (generations.length - 1));
#endif
        return get_level(level).size();
    }

#if 0
    public synchronized int[] getAllLevelSize()
    {
        int[] counts = new int[generations.length];
        for (int i = 0; i < counts.length; i++)
            counts[i] = getLevel(i).size();
        return counts;
    }

    private void logDistribution()
    {
        if (logger.isDebugEnabled())
        {
            for (int i = 0; i < generations.length; i++)
            {
                if (!getLevel(i).isEmpty())
                {
                    logger.debug("L{} contains {} SSTables ({} bytes) in {}",
                                 i, getLevel(i).size(), SSTableReader.getTotalBytes(getLevel(i)), this);
                }
            }
        }
    }
#endif

    uint32_t remove(sstables::shared_sstable& sstable) {
        uint32_t level = sstable->get_sstable_level();
        if (level >= _generations.size()) {
            throw std::runtime_error("Invalid level");
        }
        _generations[level].remove(sstable);
        return level;
    }

    std::vector<sstables::shared_sstable>
    overlapping(std::vector<sstables::shared_sstable>& candidates, std::list<sstables::shared_sstable>& others) {
        const schema& s = *_schema;
        assert(!candidates.empty());
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        auto it = candidates.begin();
        auto& first_sstable = *it;
        it++;
        dht::token first = first_sstable->get_first_decorated_key(s)._token;
        dht::token last = first_sstable->get_last_decorated_key(s)._token;
        while (it != candidates.end()) {
            auto& candidate_sstable = *it;
            it++;
            dht::token first_candidate = candidate_sstable->get_first_decorated_key(s)._token;
            dht::token last_candidate = candidate_sstable->get_last_decorated_key(s)._token;

            first = first <= first_candidate? first : first_candidate;
            last = last >= last_candidate ? last : last_candidate;
        }
        return overlapping(first, last, others);
    }

    std::vector<sstables::shared_sstable>
    overlapping(sstables::shared_sstable& sstable, std::list<sstables::shared_sstable>& others) {
        const schema& s = *_schema;
        return overlapping(sstable->get_first_decorated_key(s)._token, sstable->get_last_decorated_key(s)._token, others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
   std::vector<sstables::shared_sstable>
   overlapping(dht::token start, dht::token end, std::list<sstables::shared_sstable>& sstables) {
        const schema& s = *_schema;
        assert(start <= end);

        std::vector<sstables::shared_sstable> overlapped;
        auto range = ::range<dht::token>::make(start, end);

        for (auto& candidate : sstables) {
            auto candidate_range = ::range<dht::token>::make(candidate->get_first_decorated_key(s)._token, candidate->get_last_decorated_key(s)._token);

            if (range.overlaps(candidate_range, dht::token_comparator())) {
                overlapped.push_back(candidate);
            }
        }
        return overlapped;
    }

#if 0
    private static final Predicate<SSTableReader> suspectP = new Predicate<SSTableReader>()
    {
        public boolean apply(SSTableReader candidate)
        {
            return candidate.isMarkedSuspect();
        }
    };
#endif
    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are blacklisted
     * for prior failure), will return an empty list.  Never returns null.
     */
    std::vector<sstables::shared_sstable> get_candidates_for(int level) {
        const schema& s = *_schema;
        assert(!get_level(level).empty());

        logger.debug("Choosing candidates for L{}", level);
#if 0
        final Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
#endif
        if (level == 0) {
#if 0
            Set<SSTableReader> compactingL0 = ImmutableSet.copyOf(Iterables.filter(getLevel(0), Predicates.in(compacting)));

            RowPosition lastCompactingKey = null;
            RowPosition firstCompactingKey = null;
            for (SSTableReader candidate : compactingL0)
            {
                if (firstCompactingKey == null || candidate.first.compareTo(firstCompactingKey) < 0)
                    firstCompactingKey = candidate.first;
                if (lastCompactingKey == null || candidate.last.compareTo(lastCompactingKey) > 0)
                    lastCompactingKey = candidate.last;
            }
#endif

            // L0 is the dumping ground for new sstables which thus may overlap each other.
            //
            // We treat L0 compactions specially:
            // 1a. add sstables to the candidate set until we have at least maxSSTableSizeInMB
            // 1b. prefer choosing older sstables as candidates, to newer ones
            // 1c. any L0 sstables that overlap a candidate, will also become candidates
            // 2. At most MAX_COMPACTING_L0 sstables from L0 will be compacted at once
            // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
            //    and the result of the compaction will stay in L0 instead of being promoted (see promote())
            //
            // Note that we ignore suspect-ness of L1 sstables here, since if an L1 sstable is suspect we're
            // basically screwed, since we expect all or most L0 sstables to overlap with each L1 sstable.
            // So if an L1 sstable is suspect we can't do much besides try anyway and hope for the best.
            std::vector<sstables::shared_sstable> candidates;
            std::list<sstables::shared_sstable> remaining = get_level(0);
#if 0
            Iterables.addAll(remaining, Iterables.filter(getLevel(0), Predicates.not(suspectP)));
#endif
            for (auto& sstable : age_sorted_sstables(remaining)) {
                auto it = std::find(candidates.begin(), candidates.end(), sstable);
                if (it != candidates.end()) {
                    continue;
                }

                auto overlappedL0 = overlapping(sstable, remaining);
                it = std::find(overlappedL0.begin(), overlappedL0.end(), sstable);
                if (it == overlappedL0.end()) {
                    overlappedL0.push_back(sstable);
                }

#if 0
                if (!Sets.intersection(overlappedL0, compactingL0).isEmpty())
                    continue;
#endif

                for (auto& new_candidate : overlappedL0) {
#if 0
                    if (firstCompactingKey == null || lastCompactingKey == null || overlapping(firstCompactingKey.getToken(), lastCompactingKey.getToken(), Arrays.asList(newCandidate)).size() == 0)
                        candidates.add(newCandidate);
#else
                    candidates.push_back(new_candidate);
#endif
                    remaining.remove(new_candidate);
                }

                if (candidates.size() > MAX_COMPACTING_L0) {
                    // limit to only the MAX_COMPACTING_L0 oldest candidates
                    auto age_sorted_candidates = age_sorted_sstables(candidates);
                    // create a sub list of age_sorted_candidates by resizing it.
                    age_sorted_candidates.resize(MAX_COMPACTING_L0);
                    candidates = std::move(age_sorted_candidates);
                    break;
                }
            }

            // leave everything in L0 if we didn't end up with a full sstable's worth of data
            if (get_total_bytes(candidates) > _max_sstable_size_in_bytes) {
                // add sstables from L1 that overlap candidates
                // if the overlapping ones are already busy in a compaction, leave it out.
                // TODO try to find a set of L0 sstables that only overlaps with non-busy L1 sstables
                auto l1overlapping = overlapping(candidates, get_level(1));
                for (auto candidate : l1overlapping) {
                    auto it = std::find(candidates.begin(), candidates.end(), candidate);
                    if (it != candidates.end()) {
                        continue;
                    }
                    candidates.push_back(candidate);
                }
            }
            if (candidates.size() < 2) {
                return {};
            } else {
                return candidates;
            }
        }

        // for non-L0 compactions, pick up where we left off last time
        get_level(level).sort([&s] (auto& i, auto& j) {
            return i->compare_by_first_key(s, *j) < 0;
        });
        int start = 0; // handles case where the prior compaction touched the very last range
#if 0
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader sstable = getLevel(level).get(i);
            if (sstable.first.compareTo(lastCompactedKeys[level]) > 0)
            {
                start = i;
                break;
            }
        }
#endif
        // look for a non-suspect keyspace to compact with, starting with where we left off last time,
        // and wrapping back to the beginning of the generation if necessary
        for (auto i = 0U; i < get_level(level).size(); i++) {
            // get an iterator to the element of position pos from the list get_level(level).
            auto pos = (start + i) % get_level(level).size();
            auto it = get_level(level).begin();
            std::advance(it, pos);

            auto sstable = *it;
            auto candidates = overlapping(sstable, get_level(level + 1));
            candidates.push_back(sstable);
#if 0
            if (Iterables.any(candidates, suspectP))
                continue;
            if (Sets.intersection(candidates, compacting).isEmpty())
                return candidates;
#endif
            return candidates;
        }

        // all the sstables were suspect or overlapped with something suspect
        return {};
    }

    std::list<sstables::shared_sstable> age_sorted_sstables(std::list<sstables::shared_sstable>& candidates) {
        auto age_sorted_candidates = candidates;

        age_sorted_candidates.sort([] (auto& i, auto& j) {
            return i->compare_by_max_timestamp(*j) > 0;
        });

        return age_sorted_candidates;
    }

    std::vector<sstables::shared_sstable> age_sorted_sstables(std::vector<sstables::shared_sstable>& candidates) {
        auto age_sorted_candidates = candidates;

        std::sort(age_sorted_candidates.begin(), age_sorted_candidates.end(), [] (auto& i, auto& j) {
            return i->compare_by_max_timestamp(*j) > 0;
        });

        return age_sorted_candidates;
    }
#if 0
    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public int getLevelCount()
    {
        for (int i = generations.length - 1; i >= 0; i--)
        {
            if (getLevel(i).size() > 0)
                return i;
        }
        return 0;
    }

    public synchronized SortedSet<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator)
    {
        return ImmutableSortedSet.copyOf(comparator, getLevel(level));
    }
#endif
    std::list<sstables::shared_sstable>& get_level(uint32_t level) {
        if (level >= _generations.size()) {
            throw std::runtime_error("Invalid level");
        }
        return _generations[level];
    }
#if 0
    public synchronized int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[generations.length];

        for (int i = generations.length - 1; i >= 0; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            estimated[i] = Math.max(0L, SSTableReader.getTotalBytes(sstables) - maxBytesForLevel(i)) / maxSSTableSizeInBytes;
            tasks += estimated[i];
        }

        logger.debug("Estimating {} compactions to do for {}.{}",
                     Arrays.toString(estimated), cfs.keyspace.getName(), cfs.name);
        return Ints.checkedCast(tasks);
    }
#endif
    int get_next_level(const std::vector<sstables::shared_sstable>& sstables) {
        int maximum_level = std::numeric_limits<int>::min();
        int minimum_level = std::numeric_limits<int>::max();
        auto total_bytes = get_total_bytes(sstables);

        for (auto& sstable : sstables) {
            int sstable_level = sstable->get_sstable_level();
            maximum_level = std::max(sstable_level, maximum_level);
            minimum_level = std::min(sstable_level, minimum_level);
        }

        int new_level;
        if (minimum_level == 0 && minimum_level == maximum_level && total_bytes < _max_sstable_size_in_bytes) {
            new_level = 0;
        } else {
            new_level = minimum_level == maximum_level ? maximum_level + 1 : maximum_level;
            assert(new_level > 0);
        }
        return new_level;
    }

    template <typename T>
    static uint64_t get_total_bytes(const T& sstables) {
        uint64_t sum = 0;
        for (auto& sstable : sstables) {
            sum += sstable->data_size();
        }
        return sum;
    }
};
