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
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.AbstractAllocator;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public abstract class Composites
{
    private Composites() {}

    public static final Composite EMPTY = new EmptyComposite();

    /**
     * Converts the specified <code>Composites</code> into <code>ByteBuffer</code>s.
     *
     * @param composites the composites to convert.
     * @return the <code>ByteBuffer</code>s corresponding to the specified <code>Composites</code>.
     */
    public static List<ByteBuffer> toByteBuffers(List<Composite> composites)
    {
        return Lists.transform(composites, new Function<Composite, ByteBuffer>()
        {
            public ByteBuffer apply(Composite composite)
            {
                return composite.toByteBuffer();
            }
        });
    }

    static final CBuilder EMPTY_BUILDER = new CBuilder()
    {
        public int remainingCount() { return 0; }

        public CBuilder add(ByteBuffer value) { throw new IllegalStateException(); }
        public CBuilder add(Object value) { throw new IllegalStateException(); }

        public Composite build() { return EMPTY; }
        public Composite buildWith(ByteBuffer value) { throw new IllegalStateException(); }
        public Composite buildWith(List<ByteBuffer> values) { throw new IllegalStateException(); }
    };

    private static class EmptyComposite implements Composite
    {
        public boolean isEmpty()
        {
            return true;
        }

        public int size()
        {
            return 0;
        }

        public ByteBuffer get(int i)
        {
            if (i > 0)
                throw new IndexOutOfBoundsException();

            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        public EOC eoc()
        {
            return EOC.NONE;
        }

        public Composite start()
        {
            // Note that SimpleCType/AbstractSimpleCellNameType compare method
            // indirectly rely on the fact that EMPTY == EMPTY.start() == EMPTY.end()
            // (or more precisely on the fact that the EOC is NONE for all of those).
            return this;
        }

        public Composite end()
        {
            // Note that SimpleCType/AbstractSimpleCellNameType compare method
            // indirectly rely on the fact that EMPTY == EMPTY.start() == EMPTY.end()
            // (or more precisely on the fact that the EOC is NONE for all of those).
            return this;
        }

        public Composite withEOC(EOC newEoc)
        {
            // Note that SimpleCType/AbstractSimpleCellNameType compare method
            // indirectly rely on the fact that EMPTY == EMPTY.start() == EMPTY.end()
            // (or more precisely on the fact that the EOC is NONE for all of those).
            return this;
        }

        public ColumnSlice slice()
        {
            return ColumnSlice.ALL_COLUMNS;
        }

        public ByteBuffer toByteBuffer()
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        public boolean isStatic()
        {
            return false;
        }

        public int dataSize()
        {
            return 0;
        }

        public long unsharedHeapSize()
        {
            return 0;
        }

        public boolean isPrefixOf(CType type, Composite c)
        {
            return true;
        }

        public Composite copy(CFMetaData cfm, AbstractAllocator allocator)
        {
            return this;
        }
    }
}
