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
package org.apache.cassandra.db.commitlog;

import java.io.DataInput;
import java.io.IOException;
import java.util.Comparator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ReplayPosition implements Comparable<ReplayPosition>
{
    public static final ReplayPositionSerializer serializer = new ReplayPositionSerializer();

    // NONE is used for SSTables that are streamed from other nodes and thus have no relationship
    // with our local commitlog. The values satisfy the critera that
    //  - no real commitlog segment will have the given id
    //  - it will sort before any real replayposition, so it will be effectively ignored by getReplayPosition
    public static final ReplayPosition NONE = new ReplayPosition(-1, 0);

    /**
     * Convenience method to compute the replay position for a group of SSTables.
     * @param sstables
     * @return the most recent (highest) replay position
     */
    public static ReplayPosition getReplayPosition(Iterable<? extends SSTableReader> sstables)
    {
        if (Iterables.isEmpty(sstables))
            return NONE;

        Function<SSTableReader, ReplayPosition> f = new Function<SSTableReader, ReplayPosition>()
        {
            public ReplayPosition apply(SSTableReader sstable)
            {
                return sstable.getReplayPosition();
            }
        };
        Ordering<ReplayPosition> ordering = Ordering.from(ReplayPosition.comparator);
        return ordering.max(Iterables.transform(sstables, f));
    }


    public final long segment;
    public final int position;

    public static final Comparator<ReplayPosition> comparator = new Comparator<ReplayPosition>()
    {
        public int compare(ReplayPosition o1, ReplayPosition o2)
        {
            if (o1.segment != o2.segment)
                return Long.valueOf(o1.segment).compareTo(o2.segment);

            return Integer.valueOf(o1.position).compareTo(o2.position);
        }
    };

    public ReplayPosition(long segment, int position)
    {
        this.segment = segment;
        assert position >= 0;
        this.position = position;
    }

    public int compareTo(ReplayPosition other)
    {
        return comparator.compare(this, other);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplayPosition that = (ReplayPosition) o;

        if (position != that.position) return false;
        return segment == that.segment;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (segment ^ (segment >>> 32));
        result = 31 * result + position;
        return result;
    }

    @Override
    public String toString()
    {
        return "ReplayPosition(" +
               "segmentId=" + segment +
               ", position=" + position +
               ')';
    }

    public ReplayPosition clone()
    {
        return new ReplayPosition(segment, position);
    }

    public static class ReplayPositionSerializer implements ISerializer<ReplayPosition>
    {
        public void serialize(ReplayPosition rp, DataOutputPlus out) throws IOException
        {
            out.writeLong(rp.segment);
            out.writeInt(rp.position);
        }

        public ReplayPosition deserialize(DataInput in) throws IOException
        {
            return new ReplayPosition(in.readLong(), in.readInt());
        }

        public long serializedSize(ReplayPosition rp, TypeSizes typeSizes)
        {
            return typeSizes.sizeof(rp.segment) + typeSizes.sizeof(rp.position);
        }
    }
}
