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
import org.apache.cassandra.exceptions.InvalidRequestException;

public final class SimpleSelector extends Selector
{
    private final String columnName;
    private final int idx;
    private final AbstractType<?> type;
    private ByteBuffer current;

    public static Factory newFactory(final String columnName, final int idx, final AbstractType<?> type)
    {
        return new Factory()
        {
            @Override
            protected String getColumnName()
            {
                return columnName;
            }

            @Override
            protected AbstractType<?> getReturnType()
            {
                return type;
            }

            @Override
            public Selector newInstance()
            {
                return new SimpleSelector(columnName, idx, type);
            }
        };
    }

    @Override
    public void addInput(int protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
    {
        current = rs.current.get(idx);
    }

    @Override
    public ByteBuffer getOutput(int protocolVersion) throws InvalidRequestException
    {
        return current;
    }

    @Override
    public void reset()
    {
        current = null;
    }

    @Override
    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return columnName;
    }

    private SimpleSelector(String columnName, int idx, AbstractType<?> type)
    {
        this.columnName = columnName;
        this.idx = idx;
        this.type = type;
    }
}