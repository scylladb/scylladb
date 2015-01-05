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
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * A "truly-composite" Composite.
 */
public class CompoundComposite extends AbstractComposite
{
    private static final long HEAP_SIZE = ObjectSizes.measure(new CompoundComposite(null, 0, false));

    // We could use a List, but we'll create such object *a lot* and using a array+size is not
    // all that harder, so we save the List object allocation.
    final ByteBuffer[] elements;
    final int size;
    final boolean isStatic;

    CompoundComposite(ByteBuffer[] elements, int size, boolean isStatic)
    {
        this.elements = elements;
        this.size = size;
        this.isStatic = isStatic;
    }

    public int size()
    {
        return size;
    }

    public ByteBuffer get(int i)
    {
        // Note: most consumer should validate that i is within bounds. However, for backward compatibility
        // reasons, composite dense tables can have names that don't have all their component of the clustering
        // columns, which may end up here with i > size(). For those calls, it's actually simpler to return null
        // than to force the caller to special case.
        return i >= size() ? null : elements[i];
    }

    @Override
    public boolean isStatic()
    {
        return isStatic;
    }

    protected ByteBuffer[] elementsCopy(AbstractAllocator allocator)
    {
        ByteBuffer[] elementsCopy = new ByteBuffer[size];
        for (int i = 0; i < size; i++)
            elementsCopy[i] = allocator.clone(elements[i]);
        return elementsCopy;
    }

    public long unsharedHeapSize()
    {
        return HEAP_SIZE + ObjectSizes.sizeOnHeapOf(elements);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return HEAP_SIZE + ObjectSizes.sizeOnHeapExcludingData(elements);
    }

    public Composite copy(CFMetaData cfm, AbstractAllocator allocator)
    {
        return new CompoundComposite(elementsCopy(allocator), size, isStatic);
    }
}
