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

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * A composite value.
 *
 * This can be though as a list of ByteBuffer, except that this also include an
 * 'end-of-component' flag, that allow precise selection of composite ranges.
 *
 * We also make a difference between "true" composites and the "simple" ones. The
 * non-truly composite will have a size() == 1 but differs from true composites with
 * size() == 1 in the way they are stored. Most code shouldn't have to care about the
 * difference.
 */
public interface Composite extends IMeasurableMemory
{
    public enum EOC
    {
        START(-1), NONE(-1), END(1);

        // If composite p has this EOC and is a strict prefix of composite c, then this
        // the result of the comparison of p and c. Basically, p sorts before c unless
        // it's EOC is END.
        public final int prefixComparisonResult;

        private EOC(int prefixComparisonResult)
        {
            this.prefixComparisonResult = prefixComparisonResult;
        }

        public static EOC from(int eoc)
        {
            return eoc == 0 ? NONE : (eoc < 0 ? START : END);
        }
    }

    public int size();
    public boolean isEmpty();
    public ByteBuffer get(int i);

    public EOC eoc();
    public Composite withEOC(EOC eoc);
    public Composite start();
    public Composite end();
    public ColumnSlice slice();

    public boolean isStatic();

    public boolean isPrefixOf(CType type, Composite other);

    public ByteBuffer toByteBuffer();

    public int dataSize();
    public Composite copy(CFMetaData cfm, AbstractAllocator allocator);
}
