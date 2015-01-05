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
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.Interval;

public class RangeTombstone extends Interval<Composite, DeletionTime> implements OnDiskAtom
{
    public RangeTombstone(Composite start, Composite stop, long markedForDeleteAt, int localDeletionTime)
    {
        this(start, stop, new DeletionTime(markedForDeleteAt, localDeletionTime));
    }

    public RangeTombstone(Composite start, Composite stop, DeletionTime delTime)
    {
        super(start, stop, delTime);
    }

    public Composite name()
    {
        return min;
    }

    public int getLocalDeletionTime()
    {
        return data.localDeletionTime;
    }

    public long timestamp()
    {
        return data.markedForDeleteAt;
    }

    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        metadata.comparator.validate(min);
        metadata.comparator.validate(max);
    }

    public void updateDigest(MessageDigest digest)
    {
        digest.update(min.toByteBuffer().duplicate());
        digest.update(max.toByteBuffer().duplicate());

        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            buffer.writeLong(data.markedForDeleteAt);
            digest.update(buffer.getData(), 0, buffer.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * This tombstone supersedes another one if it is more recent and cover a
     * bigger range than rt.
     */
    public boolean supersedes(RangeTombstone rt, Comparator<Composite> comparator)
    {
        if (rt.data.markedForDeleteAt > data.markedForDeleteAt)
            return false;

        return comparator.compare(min, rt.min) <= 0 && comparator.compare(max, rt.max) >= 0;
    }

    public boolean includes(Comparator<Composite> comparator, Composite name)
    {
        return comparator.compare(name, min) >= 0 && comparator.compare(name, max) <= 0;
    }

    public static class Tracker
    {
        private final Comparator<Composite> comparator;
        private final Deque<RangeTombstone> ranges = new ArrayDeque<RangeTombstone>();
        private final SortedSet<RangeTombstone> maxOrderingSet = new TreeSet<RangeTombstone>(new Comparator<RangeTombstone>()
        {
            public int compare(RangeTombstone t1, RangeTombstone t2)
            {
                return comparator.compare(t1.max, t2.max);
            }
        });
        public final Set<RangeTombstone> expired = new HashSet<RangeTombstone>();
        private int atomCount;

        public Tracker(Comparator<Composite> comparator)
        {
            this.comparator = comparator;
        }

        /**
         * Compute RangeTombstone that are needed at the beginning of an index
         * block starting with {@code firstColumn}.
         * Returns the total serialized size of said tombstones and write them
         * to {@code out} it if isn't null.
         */
        public long writeOpenedMarker(OnDiskAtom firstColumn, DataOutputPlus out, OnDiskAtom.Serializer atomSerializer) throws IOException
        {
            long size = 0;
            if (ranges.isEmpty())
                return size;

            /*
             * Compute the marker that needs to be written at the beginning of
             * this block. We need to write one if it the more recent
             * (opened) tombstone for at least some part of its range.
             */
            List<RangeTombstone> toWrite = new LinkedList<RangeTombstone>();
            outer:
            for (RangeTombstone tombstone : ranges)
            {
                // If ever the first column is outside the range, skip it (in
                // case update() hasn't been called yet)
                if (comparator.compare(firstColumn.name(), tombstone.max) > 0)
                    continue;

                if (expired.contains(tombstone))
                    continue;

                RangeTombstone updated = new RangeTombstone(firstColumn.name(), tombstone.max, tombstone.data);

                Iterator<RangeTombstone> iter = toWrite.iterator();
                while (iter.hasNext())
                {
                    RangeTombstone other = iter.next();
                    if (other.supersedes(updated, comparator))
                        break outer;
                    if (updated.supersedes(other, comparator))
                        iter.remove();
                }
                toWrite.add(tombstone);
            }

            for (RangeTombstone tombstone : toWrite)
            {
                size += atomSerializer.serializedSizeForSSTable(tombstone);
                atomCount++;
                if (out != null)
                    atomSerializer.serializeForSSTable(tombstone, out);
            }
            return size;
        }

        public int writtenAtom()
        {
            return atomCount;
        }

        /**
         * Update this tracker given an {@code atom}.
         * If column is a Cell, check if any tracked range is useless and
         * can be removed. If it is a RangeTombstone, add it to this tracker.
         */
        public void update(OnDiskAtom atom, boolean isExpired)
        {
            if (atom instanceof RangeTombstone)
            {
                RangeTombstone t = (RangeTombstone)atom;
                // This could be a repeated marker already. If so, we already have a range in which it is
                // fully included. While keeping both would be ok functionaly, we could end up with a lot of
                // useless marker after a few compaction, so avoid this.
                for (RangeTombstone tombstone : maxOrderingSet.tailSet(t))
                {
                    // We only care about tombstone have the same max than t
                    if (comparator.compare(t.max, tombstone.max) > 0)
                        break;

                    // Since it is assume tombstones are passed to this method in growing min order, it's enough to
                    // check for the data to know is the current tombstone is included in a previous one
                    if (tombstone.data.equals(t.data))
                        return;
                }
                ranges.addLast(t);
                maxOrderingSet.add(t);
                if (isExpired)
                    expired.add(t);
            }
            else
            {
                assert atom instanceof Cell;
                Iterator<RangeTombstone> iter = maxOrderingSet.iterator();
                while (iter.hasNext())
                {
                    RangeTombstone tombstone = iter.next();
                    if (comparator.compare(atom.name(), tombstone.max) > 0)
                    {
                        // That tombstone is now useless
                        iter.remove();
                        ranges.remove(tombstone);
                    }
                    else
                    {
                        // Since we're iterating by growing end bound, if the current range
                        // includes the column, so does all the next ones
                        return;
                    }
                }
            }
        }

        public boolean isDeleted(Cell cell)
        {
            for (RangeTombstone tombstone : ranges)
            {
                if (comparator.compare(cell.name(), tombstone.min) >= 0
                    && comparator.compare(cell.name(), tombstone.max) <= 0
                    && tombstone.timestamp() >= cell.timestamp())
                {
                    return true;
                }
            }
            return false;
        }
    }

    public static class Serializer implements ISSTableSerializer<RangeTombstone>
    {
        private final CType type;

        public Serializer(CType type)
        {
            this.type = type;
        }

        public void serializeForSSTable(RangeTombstone t, DataOutputPlus out) throws IOException
        {
            type.serializer().serialize(t.min, out);
            out.writeByte(ColumnSerializer.RANGE_TOMBSTONE_MASK);
            type.serializer().serialize(t.max, out);
            DeletionTime.serializer.serialize(t.data, out);
        }

        public RangeTombstone deserializeFromSSTable(DataInput in, Version version) throws IOException
        {
            Composite min = type.serializer().deserialize(in);

            int b = in.readUnsignedByte();
            assert (b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0;
            return deserializeBody(in, min, version);
        }

        public RangeTombstone deserializeBody(DataInput in, Composite min, Version version) throws IOException
        {
            Composite max = type.serializer().deserialize(in);
            DeletionTime dt = DeletionTime.serializer.deserialize(in);
            return new RangeTombstone(min, max, dt);
        }

        public void skipBody(DataInput in, Version version) throws IOException
        {
            type.serializer().skip(in);
            DeletionTime.serializer.skip(in);
        }

        public long serializedSizeForSSTable(RangeTombstone t)
        {
            TypeSizes typeSizes = TypeSizes.NATIVE;
            return type.serializer().serializedSize(t.min, typeSizes)
                 + 1 // serialization flag
                 + type.serializer().serializedSize(t.max, typeSizes)
                 + DeletionTime.serializer.serializedSize(t.data, typeSizes);
        }
    }
}
