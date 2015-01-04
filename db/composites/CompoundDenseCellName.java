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

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.ObjectSizes;

public class CompoundDenseCellName extends CompoundComposite implements CellName
{

    private static final long HEAP_SIZE = ObjectSizes.measure(new CompoundDenseCellName(new ByteBuffer[0]));

    // Not meant to be used directly, you should use the CellNameType method instead
    CompoundDenseCellName(ByteBuffer[] elements)
    {
        super(elements, elements.length, false);
    }

    CompoundDenseCellName(ByteBuffer[] elements, int size)
    {
        super(elements, size, false);
    }

    public int clusteringSize()
    {
        return size;
    }

    public ColumnIdentifier cql3ColumnName(CFMetaData metadata)
    {
        return null;
    }

    public ByteBuffer collectionElement()
    {
        return null;
    }

    public boolean isCollectionCell()
    {
        return false;
    }

    public boolean isSameCQL3RowAs(CellNameType type, CellName other)
    {
        // Dense cell imply one cell by CQL row so no other cell will be the same row.
        return type.compare(this, other) == 0;
    }

    @Override
    public long unsharedHeapSize()
    {
        return HEAP_SIZE + ObjectSizes.sizeOnHeapOf(elements);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return HEAP_SIZE + ObjectSizes.sizeOnHeapExcludingData(elements);
    }

    public CellName copy(CFMetaData cfm, AbstractAllocator allocator)
    {
        return new CompoundDenseCellName(elementsCopy(allocator));
    }

}
