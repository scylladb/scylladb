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
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Wraps another Composite and adds an EOC byte to track whether this is a slice start or end.
 */
public class BoundedComposite extends AbstractComposite
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BoundedComposite(null, false));

    private final Composite wrapped;
    private final boolean isStart;

    private BoundedComposite(Composite wrapped, boolean isStart)
    {
        this.wrapped = wrapped;
        this.isStart = isStart;
    }

    static Composite startOf(Composite c)
    {
        return new BoundedComposite(c, true);
    }

    static Composite endOf(Composite c)
    {
        return new BoundedComposite(c, false);
    }

    public int size()
    {
        return wrapped.size();
    }

    public boolean isStatic()
    {
        return wrapped.isStatic();
    }

    public ByteBuffer get(int i)
    {
        return wrapped.get(i);
    }

    @Override
    public EOC eoc()
    {
        return isStart ? EOC.START : EOC.END;
    }

    @Override
    public Composite withEOC(EOC eoc)
    {
        switch (eoc)
        {
            case START:
                return isStart ? this : startOf(wrapped);
            case END:
                return isStart ? endOf(wrapped) : this;
            default:
                return wrapped;
        }
    }

    @Override
    public ByteBuffer toByteBuffer()
    {
        ByteBuffer bb = wrapped.toByteBuffer();
        bb.put(bb.remaining() - 1, (byte)(isStart ? -1 : 1));
        return bb;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + wrapped.unsharedHeapSize();
    }

    public Composite copy(CFMetaData cfm, AbstractAllocator allocator)
    {
        return new BoundedComposite(wrapped.copy(cfm, allocator), isStart);
    }
}
