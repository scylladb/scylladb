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
package org.apache.cassandra.db.composites;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

/**
 * A type for a Composite.
 *
 * There is essentially 2 types of Composite and such of CType:
 *   1. the "simple" ones, see SimpleCType.
 *   2. the "truly-composite" ones, see CompositeCType.
 *
 * API-wise, a CType is simply a collection of AbstractType with a few utility
 * methods.
 */
public interface CType extends Comparator<Composite>
{
    /**
     * Returns whether this is a "truly-composite" underneath.
     */
    public boolean isCompound();

    /**
     * The number of subtypes for this CType.
     */
    public int size();

    int compare(Composite o1, Composite o2);

    /**
     * Gets a subtype of this CType.
     */
    public AbstractType<?> subtype(int i);

    /**
     * A builder of Composite.
     */
    public CBuilder builder();

    /**
     * Convenience method to build composites from their component.
     *
     * The arguments can be either ByteBuffer or actual objects of the type
     * corresponding to their position.
     */
    public Composite make(Object... components);

    /**
     * Validates a composite.
     */
    public void validate(Composite name);

    /**
     * Converts a composite to a user-readable string.
     */
    public String getString(Composite c);

    /**
     * See AbstractType#isCompatibleWith.
     */
    public boolean isCompatibleWith(CType previous);

    /**
     * Returns a new CType that is equivalent to this CType but with
     * one of the subtype replaced by the provided new type.
     */
    public CType setSubtype(int position, AbstractType<?> newType);

    /**
     * Deserialize a Composite from a ByteBuffer.
     *
     * This is meant for thrift to convert the fully serialized buffer we
     * get from the clients to composites.
     */
    public Composite fromByteBuffer(ByteBuffer bb);

    /**
     * Returns a AbstractType corresponding to this CType for thrift sake.
     *
     * If the CType is a "simple" one, this just return the wrapped type, otherwise
     * it returns the corresponding org.apache.cassandra.db.marshal.CompositeType.
     *
     * This is only meant to be use for backward compatibility (particularly for
     * thrift) but it's not meant to be used internally.
     */
    public AbstractType<?> asAbstractType();


    /**********************************************************/

    /*
     * Follows a number of per-CType instances for the Comparator and Serializer used throughout
     * the code. The reason we need this is that we want the per-CType/per-CellNameType Composite/CellName
     * serializers, which means the following instances have to depend on the type too.
     */

    public Comparator<Composite> reverseComparator();
    public Comparator<IndexInfo> indexComparator();
    public Comparator<IndexInfo> indexReverseComparator();

    public Serializer serializer();

    public IVersionedSerializer<ColumnSlice> sliceSerializer();
    public IVersionedSerializer<SliceQueryFilter> sliceQueryFilterSerializer();
    public DeletionInfo.Serializer deletionInfoSerializer();
    public RangeTombstone.Serializer rangeTombstoneSerializer();

    public interface Serializer extends ISerializer<Composite>
    {
        public void skip(DataInput in) throws IOException;
    }
}
