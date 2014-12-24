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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.selection.Selection.ResultSetBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.ByteBufferUtil;

final class WritetimeOrTTLSelector extends Selector
{
    private final String columnName;
    private final int idx;
    private final boolean isWritetime;
    private ByteBuffer current;

    public static Factory newFactory(final String columnName, final int idx, final boolean isWritetime)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return String.format("%s(%s)", isWritetime ? "writetime" : "ttl", columnName);
            }

            protected AbstractType<?> getReturnType()
            {
                return isWritetime ? LongType.instance : Int32Type.instance;
            }

            public Selector newInstance()
            {
                return new WritetimeOrTTLSelector(columnName, idx, isWritetime);
            }

            public boolean isWritetimeSelectorFactory()
            {
                return isWritetime;
            }

            public boolean isTTLSelectorFactory()
            {
                return !isWritetime;
            }
        };
    }

    public void addInput(int protocolVersion, ResultSetBuilder rs)
    {
        if (isWritetime)
        {
            long ts = rs.timestamps[idx];
            current = ts != Long.MIN_VALUE ? ByteBufferUtil.bytes(ts) : null;
        }
        else
        {
            int ttl = rs.ttls[idx];
            current = ttl > 0 ? ByteBufferUtil.bytes(ttl) : null;
        }
    }

    public ByteBuffer getOutput(int protocolVersion)
    {
        return current;
    }

    public void reset()
    {
        current = null;
    }

    public AbstractType<?> getType()
    {
        return isWritetime ? LongType.instance : Int32Type.instance;
    }

    @Override
    public String toString()
    {
        return columnName;
    }

    private WritetimeOrTTLSelector(String columnName, int idx, boolean isWritetime)
    {
        this.columnName = columnName;
        this.idx = idx;
        this.isWritetime = isWritetime;
    }

}
