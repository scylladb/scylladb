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
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * A combination of a top-level (or row) tombstone and range tombstones describing the deletions
 * within a {@link ColumnFamily} (or row).
 */
public class DeletionInfo implements IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new DeletionInfo(0, 0));

    /**
     * This represents a deletion of the entire row.  We can't represent this within the RangeTombstoneList, so it's
     * kept separately.  This also slightly optimizes the common case of a full row deletion.
     */
    private DeletionTime topLevel;

    /**
     * A list of range tombstones within the row.  This is left as null if there are no range tombstones
     * (to save an allocation (since it's a common case).
     */
    private RangeTombstoneList ranges;

    /**
     * Creates a DeletionInfo with only a top-level (row) tombstone.
     * @param markedForDeleteAt the time after which the entire row should be considered deleted
     * @param localDeletionTime what time the deletion write was applied locally (for purposes of
     *                          purging the tombstone after gc_grace_seconds).
     */
    public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        this(new DeletionTime(markedForDeleteAt, localDeletionTime == Integer.MIN_VALUE ? Integer.MAX_VALUE : localDeletionTime));
    }

    public DeletionInfo(DeletionTime topLevel)
    {
        this(topLevel, null);
    }

    public DeletionInfo(Composite start, Composite end, Comparator<Composite> comparator, long markedForDeleteAt, int localDeletionTime)
    {
        this(DeletionTime.LIVE, new RangeTombstoneList(comparator, 1));
        ranges.add(start, end, markedForDeleteAt, localDeletionTime);
    }

    public DeletionInfo(RangeTombstone rangeTombstone, Comparator<Composite> comparator)
    {
        this(rangeTombstone.min, rangeTombstone.max, comparator, rangeTombstone.data.markedForDeleteAt, rangeTombstone.data.localDeletionTime);
    }

    private DeletionInfo(DeletionTime topLevel, RangeTombstoneList ranges)
    {
        this.topLevel = topLevel;
        this.ranges = ranges;
    }

    /**
     * Returns a new DeletionInfo that has no top-level tombstone or any range tombstones.
     */
    public static DeletionInfo live()
    {
        return new DeletionInfo(DeletionTime.LIVE);
    }

    public DeletionInfo copy()
    {
        return new DeletionInfo(topLevel, ranges == null ? null : ranges.copy());
    }

    public DeletionInfo copy(AbstractAllocator allocator)
    {

        RangeTombstoneList rangesCopy = null;
        if (ranges != null)
             rangesCopy = ranges.copy(allocator);

        return new DeletionInfo(topLevel, rangesCopy);
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return topLevel.isLive() && (ranges == null || ranges.isEmpty());
    }

    /**
     * Return whether a given cell is deleted by the container having this deletion info.
     *
     * @param cell the cell to check.
     * @return true if the cell is deleted, false otherwise
     */
    public boolean isDeleted(Cell cell)
    {
        // We do rely on this test: if topLevel.markedForDeleteAt is MIN_VALUE, we should not
        // consider the column deleted even if timestamp=MIN_VALUE, otherwise this break QueryFilter.isRelevant
        if (isLive())
            return false;

        if (cell.timestamp() <= topLevel.markedForDeleteAt)
            return true;

        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        if (!topLevel.isLive() && cell instanceof CounterCell)
            return true;

        return ranges != null && ranges.isDeleted(cell);
    }

    /**
     * Returns a new {@link InOrderTester} in forward order.
     */
    public InOrderTester inOrderTester()
    {
        return inOrderTester(false);
    }

    /**
     * Returns a new {@link InOrderTester} given the order in which
     * columns will be passed to it.
     */
    public InOrderTester inOrderTester(boolean reversed)
    {
        return new InOrderTester(reversed);
    }

    /**
     * Purge every tombstones that are older than {@code gcbefore}.
     *
     * @param gcBefore timestamp (in seconds) before which tombstones should be purged
     */
    public void purge(int gcBefore)
    {
        topLevel = topLevel.localDeletionTime < gcBefore ? DeletionTime.LIVE : topLevel;

        if (ranges != null)
        {
            ranges.purge(gcBefore);
            if (ranges.isEmpty())
                ranges = null;
        }
    }

    /**
     * Evaluates difference between this deletion info and superset for read repair
     *
     * @return the difference between the two, or LIVE if no difference
     */
    public DeletionInfo diff(DeletionInfo superset)
    {
        RangeTombstoneList rangeDiff = superset.ranges == null || superset.ranges.isEmpty()
                                     ? null
                                     : ranges == null ? superset.ranges : ranges.diff(superset.ranges);

        return topLevel.markedForDeleteAt != superset.topLevel.markedForDeleteAt || rangeDiff != null
             ? new DeletionInfo(superset.topLevel, rangeDiff)
             : DeletionInfo.live();
    }


    /**
     * Digests deletion info. Used to trigger read repair on mismatch.
     */
    public void updateDigest(MessageDigest digest)
    {
        if (topLevel.markedForDeleteAt != Long.MIN_VALUE)
            digest.update(ByteBufferUtil.bytes(topLevel.markedForDeleteAt));

        if (ranges != null)
            ranges.updateDigest(digest);
    }

    /**
     * Returns true if {@code purge} would remove the top-level tombstone or any of the range
     * tombstones, false otherwise.
     * @param gcBefore timestamp (in seconds) before which tombstones should be purged
     */
    public boolean hasPurgeableTombstones(int gcBefore)
    {
        if (topLevel.localDeletionTime < gcBefore)
            return true;

        return ranges != null && ranges.hasPurgeableTombstones(gcBefore);
    }

    /**
     * Potentially replaces the top-level tombstone with another, keeping whichever has the higher markedForDeleteAt
     * timestamp.
     * @param newInfo
     */
    public void add(DeletionTime newInfo)
    {
        if (topLevel.markedForDeleteAt < newInfo.markedForDeleteAt)
            topLevel = newInfo;
    }

    public void add(RangeTombstone tombstone, Comparator<Composite> comparator)
    {
        if (ranges == null)
            ranges = new RangeTombstoneList(comparator, 1);

        ranges.add(tombstone);
    }

    /**
     * Combines another DeletionInfo with this one and returns the result.  Whichever top-level tombstone
     * has the higher markedForDeleteAt timestamp will be kept, along with its localDeletionTime.  The
     * range tombstones will be combined.
     *
     * @return this object.
     */
    public DeletionInfo add(DeletionInfo newInfo)
    {
        add(newInfo.topLevel);

        if (ranges == null)
            ranges = newInfo.ranges == null ? null : newInfo.ranges.copy();
        else if (newInfo.ranges != null)
            ranges.addAll(newInfo.ranges);

        return this;
    }

    /**
     * Returns the minimum timestamp in any of the range tombstones or the top-level tombstone.
     */
    public long minTimestamp()
    {
        return ranges == null
             ? topLevel.markedForDeleteAt
             : Math.min(topLevel.markedForDeleteAt, ranges.minMarkedAt());
    }

    /**
     * Returns the maximum timestamp in any of the range tombstones or the top-level tombstone.
     */
    public long maxTimestamp()
    {
        return ranges == null
             ? topLevel.markedForDeleteAt
             : Math.max(topLevel.markedForDeleteAt, ranges.maxMarkedAt());
    }

    /**
     * Returns the top-level (or "row") tombstone.
     */
    public DeletionTime getTopLevelDeletion()
    {
        return topLevel;
    }

    // Use sparingly, not the most efficient thing
    public Iterator<RangeTombstone> rangeIterator()
    {
        return ranges == null ? Iterators.<RangeTombstone>emptyIterator() : ranges.iterator();
    }

    public Iterator<RangeTombstone> rangeIterator(Composite start, Composite finish)
    {
        return ranges == null ? Iterators.<RangeTombstone>emptyIterator() : ranges.iterator(start, finish);
    }

    public RangeTombstone rangeCovering(Composite name)
    {
        return ranges == null ? null : ranges.search(name);
    }

    public int dataSize()
    {
        int size = TypeSizes.NATIVE.sizeof(topLevel.markedForDeleteAt);
        return size + (ranges == null ? 0 : ranges.dataSize());
    }

    public boolean hasRanges()
    {
        return ranges != null && !ranges.isEmpty();
    }

    public int rangeCount()
    {
        return hasRanges() ? ranges.size() : 0;
    }

    /**
     * Whether this deletion info may modify the provided one if added to it.
     */
    public boolean mayModify(DeletionInfo delInfo)
    {
        return topLevel.compareTo(delInfo.topLevel) > 0 || hasRanges();
    }

    @Override
    public String toString()
    {
        if (ranges == null || ranges.isEmpty())
            return String.format("{%s}", topLevel);
        else
            return String.format("{%s, ranges=%s}", topLevel, rangesAsString());
    }

    private String rangesAsString()
    {
        assert !ranges.isEmpty();
        StringBuilder sb = new StringBuilder();
        CType type = (CType)ranges.comparator();
        assert type != null;
        Iterator<RangeTombstone> iter = rangeIterator();
        while (iter.hasNext())
        {
            RangeTombstone i = iter.next();
            sb.append("[");
            sb.append(type.getString(i.min)).append("-");
            sb.append(type.getString(i.max)).append(", ");
            sb.append(i.data);
            sb.append("]");
        }
        return sb.toString();
    }

    // Updates all the timestamp of the deletion contained in this DeletionInfo to be {@code timestamp}.
    public void updateAllTimestamp(long timestamp)
    {
        if (topLevel.markedForDeleteAt != Long.MIN_VALUE)
            topLevel = new DeletionTime(timestamp, topLevel.localDeletionTime);

        if (ranges != null)
            ranges.updateAllTimestamp(timestamp);
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionInfo))
            return false;
        DeletionInfo that = (DeletionInfo)o;
        return topLevel.equals(that.topLevel) && Objects.equal(ranges, that.ranges);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(topLevel, ranges);
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + topLevel.unsharedHeapSize() + (ranges == null ? 0 : ranges.unsharedHeapSize());
    }

    public static class Serializer implements IVersionedSerializer<DeletionInfo>
    {
        private final RangeTombstoneList.Serializer rtlSerializer;

        public Serializer(CType type)
        {
            this.rtlSerializer = new RangeTombstoneList.Serializer(type);
        }

        public void serialize(DeletionInfo info, DataOutputPlus out, int version) throws IOException
        {
            DeletionTime.serializer.serialize(info.topLevel, out);
            rtlSerializer.serialize(info.ranges, out, version);
        }

        public DeletionInfo deserialize(DataInput in, int version) throws IOException
        {
            DeletionTime topLevel = DeletionTime.serializer.deserialize(in);
            RangeTombstoneList ranges = rtlSerializer.deserialize(in, version);
            return new DeletionInfo(topLevel, ranges);
        }

        public long serializedSize(DeletionInfo info, TypeSizes typeSizes, int version)
        {
            long size = DeletionTime.serializer.serializedSize(info.topLevel, typeSizes);
            return size + rtlSerializer.serializedSize(info.ranges, typeSizes, version);
        }

        public long serializedSize(DeletionInfo info, int version)
        {
            return serializedSize(info, TypeSizes.NATIVE, version);
        }
    }

    /**
     * This object allow testing whether a given column (name/timestamp) is deleted
     * or not by this DeletionInfo, assuming that the columns given to this
     * object are passed in forward or reversed comparator sorted order.
     *
     * This is more efficient that calling DeletionInfo.isDeleted() repeatedly
     * in that case.
     */
    public class InOrderTester
    {
        /*
         * Note that because because range tombstone are added to this DeletionInfo while we iterate,
         * `ranges` may be null initially and we need to wait for the first range to create the tester (once
         * created the test will pick up new tombstones however). We are guaranteed that a range tombstone
         * will be added *before* we test any column that it may delete, so this is ok.
         */
        private RangeTombstoneList.InOrderTester tester;
        private final boolean reversed;

        private InOrderTester(boolean reversed)
        {
            this.reversed = reversed;
        }

        public boolean isDeleted(Cell cell)
        {
            if (cell.timestamp() <= topLevel.markedForDeleteAt)
                return true;

            // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
            if (!topLevel.isLive() && cell instanceof CounterCell)
                return true;

            /*
             * We don't optimize the reversed case for now because RangeTombstoneList
             * is always in forward sorted order.
             */
            if (reversed)
                 return DeletionInfo.this.isDeleted(cell);

            // Maybe create the tester if we hadn't yet and we now have some ranges (see above).
            if (tester == null && ranges != null)
                tester = ranges.inOrderTester();

            return tester != null && tester.isDeleted(cell);
        }
    }
}
