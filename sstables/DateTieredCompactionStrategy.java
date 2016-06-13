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
package org.apache.cassandra.db.compaction;

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.CFPropDefs;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class DateTieredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(DateTieredCompactionStrategy.class);

    private final DateTieredCompactionStrategyOptions options;
    protected volatile int estimatedRemainingTasks;
    private final Set<SSTableReader> sstables = new HashSet<>();

    public DateTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.options = new DateTieredCompactionStrategyOptions(options);
        if (!options.containsKey(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.containsKey(AbstractCompactionStrategy.TOMBSTONE_THRESHOLD_OPTION))
        {
            disableTombstoneCompactions = true;
            logger.debug("Disabling tombstone compactions for DTCS");
        }
        else
            logger.debug("Enabling tombstone compactions for DTCS");

    }

    @Override
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        while (true)
        {
            List<SSTableReader> latestBucket = getNextBackgroundSSTables(gcBefore);

            if (latestBucket.isEmpty())
                return null;

            if (cfs.getDataTracker().markCompacting(latestBucket))
                return new CompactionTask(cfs, latestBucket, gcBefore, false);
        }
    }

    /**
     *
     * @param gcBefore
     * @return
     */
    private List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        if (!isEnabled() || cfs.getSSTables().isEmpty())
            return Collections.emptyList();

        Set<SSTableReader> uncompacting = Sets.intersection(sstables, cfs.getUncompactingSSTables());

        // Find fully expired SSTables. Those will be included no matter what.
        Set<SSTableReader> expired = CompactionController.getFullyExpiredSSTables(cfs, uncompacting, cfs.getOverlappingSSTables(uncompacting), gcBefore);
        Set<SSTableReader> candidates = Sets.newHashSet(filterSuspectSSTables(uncompacting));

        List<SSTableReader> compactionCandidates = new ArrayList<>(getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore));
        if (!expired.isEmpty())
        {
            logger.debug("Including expired sstables: {}", expired);
            compactionCandidates.addAll(expired);
        }
        return compactionCandidates;
    }

    private List<SSTableReader> getNextNonExpiredSSTables(Iterable<SSTableReader> nonExpiringSSTables, final int gcBefore)
    {
        int base = cfs.getMinimumCompactionThreshold();
        long now = getNow();
        List<SSTableReader> mostInteresting = getCompactionCandidates(nonExpiringSSTables, now, base);
        if (mostInteresting != null)
        {
            return mostInteresting;
        }

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = Lists.newArrayList();
        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        return Collections.singletonList(Collections.min(sstablesWithTombstones, new SSTableReader.SizeComparator()));
    }

    private List<SSTableReader> getCompactionCandidates(Iterable<SSTableReader> candidateSSTables, long now, int base)
    {
        Iterable<SSTableReader> candidates = filterOldSSTables(Lists.newArrayList(candidateSSTables), options.maxSSTableAge, now);

        List<List<SSTableReader>> buckets = getBuckets(createSSTableAndMinTimestampPairs(candidates), options.baseTime, base, now);
        logger.debug("Compaction buckets are {}", buckets);
        updateEstimatedCompactionsByTasks(buckets);
        List<SSTableReader> mostInteresting = newestBucket(buckets,
                                                           cfs.getMinimumCompactionThreshold(),
                                                           cfs.getMaximumCompactionThreshold(),
                                                           now,
                                                           options.baseTime);
        if (!mostInteresting.isEmpty())
            return mostInteresting;
        return null;
    }

    /**
     * Gets the timestamp that DateTieredCompactionStrategy considers to be the "current time".
     * @return the maximum timestamp across all SSTables.
     * @throws java.util.NoSuchElementException if there are no SSTables.
     */
    private long getNow()
    {
        return Collections.max(cfs.getSSTables(), new Comparator<SSTableReader>()
        {
            public int compare(SSTableReader o1, SSTableReader o2)
            {
                return Long.compare(o1.getMaxTimestamp(), o2.getMaxTimestamp());
            }
        }).getMaxTimestamp();
    }

    /**
     * Removes all sstables with max timestamp older than maxSSTableAge.
     * @param sstables all sstables to consider
     * @param maxSSTableAge the age in milliseconds when an SSTable stops participating in compactions
     * @param now current time. SSTables with max timestamp less than (now - maxSSTableAge) are filtered.
     * @return a list of sstables with the oldest sstables excluded
     */
    @VisibleForTesting
    static Iterable<SSTableReader> filterOldSSTables(List<SSTableReader> sstables, long maxSSTableAge, long now)
    {
        if (maxSSTableAge == 0)
            return sstables;
        final long cutoff = now - maxSSTableAge;
        return Iterables.filter(sstables, new Predicate<SSTableReader>()
        {
            @Override
            public boolean apply(SSTableReader sstable)
            {
                return sstable.getMaxTimestamp() >= cutoff;
            }
        });
    }

    /**
     *
     * @param sstables
     * @return
     */
    public static List<Pair<SSTableReader, Long>> createSSTableAndMinTimestampPairs(Iterable<SSTableReader> sstables)
    {
        List<Pair<SSTableReader, Long>> sstableMinTimestampPairs = Lists.newArrayListWithCapacity(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            sstableMinTimestampPairs.add(Pair.create(sstable, sstable.getMinTimestamp()));
        return sstableMinTimestampPairs;
    }
    @Override
    public void addSSTable(SSTableReader sstable)
    {
        sstables.add(sstable);
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }
    /**
     * A target time span used for bucketing SSTables based on timestamps.
     */
    private static class Target
    {
        // How big a range of timestamps fit inside the target.
        public final long size;
        // A timestamp t hits the target iff t / size == divPosition.
        public final long divPosition;

        public Target(long size, long divPosition)
        {
            this.size = size;
            this.divPosition = divPosition;
        }

        /**
         * Compares the target to a timestamp.
         * @param timestamp the timestamp to compare.
         * @return a negative integer, zero, or a positive integer as the target lies before, covering, or after than the timestamp.
         */
        public int compareToTimestamp(long timestamp)
        {
            return Long.compare(divPosition, timestamp / size);
        }

        /**
         * Tells if the timestamp hits the target.
         * @param timestamp the timestamp to test.
         * @return <code>true</code> iff timestamp / size == divPosition.
         */
        public boolean onTarget(long timestamp)
        {
            return compareToTimestamp(timestamp) == 0;
        }

        /**
         * Gets the next target, which represents an earlier time span.
         * @param base The number of contiguous targets that will have the same size. Targets following those will be <code>base</code> times as big.
         * @return
         */
        public Target nextTarget(int base)
        {
            if (divPosition % base > 0)
                return new Target(size, divPosition - 1);
            else
                return new Target(size * base, divPosition / base - 1);
        }
    }


    /**
     * Group files with similar min timestamp into buckets. Files with recent min timestamps are grouped together into
     * buckets designated to short timespans while files with older timestamps are grouped into buckets representing
     * longer timespans.
     * @param files pairs consisting of a file and its min timestamp
     * @param timeUnit
     * @param base
     * @param now
     * @return a list of buckets of files. The list is ordered such that the files with newest timestamps come first.
     *         Each bucket is also a list of files ordered from newest to oldest.
     */
    @VisibleForTesting
    static <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files, long timeUnit, int base, long now)
    {
        // Sort files by age. Newest first.
        final List<Pair<T, Long>> sortedFiles = Lists.newArrayList(files);
        Collections.sort(sortedFiles, Collections.reverseOrder(new Comparator<Pair<T, Long>>()
        {
            public int compare(Pair<T, Long> p1, Pair<T, Long> p2)
            {
                return p1.right.compareTo(p2.right);
            }
        }));

        List<List<T>> buckets = Lists.newArrayList();
        Target target = getInitialTarget(now, timeUnit);
        PeekingIterator<Pair<T, Long>> it = Iterators.peekingIterator(sortedFiles.iterator());

        outerLoop:
        while (it.hasNext())
        {
            while (!target.onTarget(it.peek().right))
            {
                // If the file is too new for the target, skip it.
                if (target.compareToTimestamp(it.peek().right) < 0)
                {
                    it.next();

                    if (!it.hasNext())
                        break outerLoop;
                }
                else // If the file is too old for the target, switch targets.
                    target = target.nextTarget(base);
            }

            List<T> bucket = Lists.newArrayList();
            while (target.onTarget(it.peek().right))
            {
                bucket.add(it.next().left);

                if (!it.hasNext())
                    break;
            }
            buckets.add(bucket);
        }

        return buckets;
    }

    @VisibleForTesting
    static Target getInitialTarget(long now, long timeUnit)
    {
        return new Target(timeUnit, now / timeUnit);
    }


    private void updateEstimatedCompactionsByTasks(List<List<SSTableReader>> tasks)
    {
        int n = 0;
        for (List<SSTableReader> bucket : tasks)
        {
            if (bucket.size() >= cfs.getMinimumCompactionThreshold())
                n += Math.ceil((double)bucket.size() / cfs.getMaximumCompactionThreshold());
        }
        estimatedRemainingTasks = n;
    }


    /**
     * @param buckets list of buckets, sorted from newest to oldest, from which to return the newest bucket within thresholds.
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @return a bucket (list) of sstables to compact.
     */
    @VisibleForTesting
    static List<SSTableReader> newestBucket(List<List<SSTableReader>> buckets, int minThreshold, int maxThreshold, long now, long baseTime)
    {
        // If the "incoming window" has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.
        Target incomingWindow = getInitialTarget(now, baseTime);
        for (List<SSTableReader> bucket : buckets)
        {
            if (bucket.size() >= minThreshold ||
                    (bucket.size() >= 2 && !incomingWindow.onTarget(bucket.get(0).getMinTimestamp())))
                return trimToThreshold(bucket, maxThreshold);
        }
        return Collections.emptyList();
    }

    /**
     * @param bucket list of sstables, ordered from newest to oldest by getMinTimestamp().
     * @param maxThreshold maximum number of sstables in a single compaction task.
     * @return A bucket trimmed to the <code>maxThreshold</code> newest sstables.
     */
    @VisibleForTesting
    static List<SSTableReader> trimToThreshold(List<SSTableReader> bucket, int maxThreshold)
    {
        // Trim the oldest sstables off the end to meet the maxThreshold
        return bucket.subList(0, Math.min(bucket.size(), maxThreshold));
    }

    @Override
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore)
    {
        Iterable<SSTableReader> sstables = cfs.markAllCompacting();
        if (sstables == null)
            return null;

        return Arrays.<AbstractCompactionTask>asList(new CompactionTask(cfs, sstables, gcBefore, false));
    }

    @Override
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        if (!cfs.getDataTracker().markCompacting(sstables))
        {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new CompactionTask(cfs, sstables, gcBefore, false).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }


    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = DateTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD);
        uncheckedOptions.remove(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD);

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("DateTieredCompactionStrategy[%s/%s]",
                cfs.getMinimumCompactionThreshold(),
                cfs.getMaximumCompactionThreshold());
    }
}
