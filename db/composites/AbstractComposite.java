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

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractComposite implements Composite
{
    public boolean isEmpty()
    {
        return size() == 0;
    }

    public boolean isStatic()
    {
        return false;
    }

    public EOC eoc()
    {
        return EOC.NONE;
    }

    public Composite start()
    {
        return withEOC(EOC.START);
    }

    public Composite end()
    {
        return withEOC(EOC.END);
    }

    public Composite withEOC(EOC newEoc)
    {
        // Note: CompositeBound overwrite this so we assume the EOC of this is NONE
        switch (newEoc)
        {
            case START:
                return BoundedComposite.startOf(this);
            case END:
                return BoundedComposite.endOf(this);
            default:
                return this;
        }
    }

    public ColumnSlice slice()
    {
        return new ColumnSlice(start(), end());
    }

    public ByteBuffer toByteBuffer()
    {
        // This is the legacy format of composites.
        // See org.apache.cassandra.db.marshal.CompositeType for details.
        ByteBuffer result = ByteBuffer.allocate(dataSize() + 3 * size() + (isStatic() ? 2 : 0));
        if (isStatic())
            ByteBufferUtil.writeShortLength(result, CompositeType.STATIC_MARKER);

        for (int i = 0; i < size(); i++)
        {
            ByteBuffer bb = get(i);
            ByteBufferUtil.writeShortLength(result, bb.remaining());
            result.put(bb.duplicate());
            result.put((byte)0);
        }
        result.flip();
        return result;
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
            size += get(i).remaining();
        return size;
    }

    public boolean isPrefixOf(CType type, Composite c)
    {
        if (size() > c.size() || isStatic() != c.isStatic())
            return false;

        for (int i = 0; i < size(); i++)
        {
            if (type.subtype(i).compare(get(i), c.get(i)) != 0)
                return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if(!(o instanceof Composite))
            return false;

        Composite c = (Composite)o;
        if (size() != c.size() || isStatic() != c.isStatic())
            return false;

        for (int i = 0; i < size(); i++)
        {
            if (!get(i).equals(c.get(i)))
                return false;
        }
        return eoc() == c.eoc();
    }

    @Override
    public int hashCode()
    {
        int h = 31;
        for (int i = 0; i < size(); i++)
            h += get(i).hashCode();
        return h + eoc().hashCode() + (isStatic() ? 1 : 0);
    }
}
