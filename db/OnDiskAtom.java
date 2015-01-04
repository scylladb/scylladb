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

import java.io.*;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;

public interface OnDiskAtom
{
    public Composite name();

    /**
     * For a standard column, this is the same as timestamp().
     * For a super column, this is the min/max column timestamp of the sub columns.
     */
    public long timestamp();
    public int getLocalDeletionTime(); // for tombstone GC, so int is sufficient granularity

    public void validateFields(CFMetaData metadata) throws MarshalException;
    public void updateDigest(MessageDigest digest);

    public static class Serializer implements ISSTableSerializer<OnDiskAtom>
    {
        private final CellNameType type;

        public Serializer(CellNameType type)
        {
            this.type = type;
        }

        public void serializeForSSTable(OnDiskAtom atom, DataOutputPlus out) throws IOException
        {
            if (atom instanceof Cell)
            {
                type.columnSerializer().serialize((Cell)atom, out);
            }
            else
            {
                assert atom instanceof RangeTombstone;
                type.rangeTombstoneSerializer().serializeForSSTable((RangeTombstone)atom, out);
            }
        }

        public OnDiskAtom deserializeFromSSTable(DataInput in, Version version) throws IOException
        {
            return deserializeFromSSTable(in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
        }

        public OnDiskAtom deserializeFromSSTable(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Version version) throws IOException
        {
            Composite name = type.serializer().deserialize(in);
            if (name.isEmpty())
            {
                // SSTableWriter.END_OF_ROW
                return null;
            }

            int b = in.readUnsignedByte();
            if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
                return type.rangeTombstoneSerializer().deserializeBody(in, name, version);
            else
                return type.columnSerializer().deserializeColumnBody(in, (CellName)name, b, flag, expireBefore);
        }

        public long serializedSizeForSSTable(OnDiskAtom atom)
        {
            if (atom instanceof Cell)
            {
                return type.columnSerializer().serializedSize((Cell)atom, TypeSizes.NATIVE);
            }
            else
            {
                assert atom instanceof RangeTombstone;
                return type.rangeTombstoneSerializer().serializedSizeForSSTable((RangeTombstone)atom);
            }
        }
    }
}
