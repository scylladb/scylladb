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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;

final class FieldSelector extends Selector
{
    private final UserType type;
    private final int field;
    private final Selector selected;

    public static Factory newFactory(final UserType type, final int field, final Selector.Factory factory)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return String.format("%s.%s",
                                     factory.getColumnName(),
                                     UTF8Type.instance.getString(type.fieldName(field)));
            }

            protected AbstractType<?> getReturnType()
            {
                return type.fieldType(field);
            }

            public Selector newInstance() throws InvalidRequestException
            {
                return new FieldSelector(type, field, factory.newInstance());
            }

            public boolean isAggregateSelectorFactory()
            {
                return factory.isAggregateSelectorFactory();
            }
        };
    }

    public boolean isAggregate()
    {
        return false;
    }

    public void addInput(int protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
    {
        selected.addInput(protocolVersion, rs);
    }

    public ByteBuffer getOutput(int protocolVersion) throws InvalidRequestException
    {
        ByteBuffer value = selected.getOutput(protocolVersion);
        if (value == null)
            return null;
        ByteBuffer[] buffers = type.split(value);
        return field < buffers.length ? buffers[field] : null;
    }

    public AbstractType<?> getType()
    {
        return type.fieldType(field);
    }

    public void reset()
    {
        selected.reset();
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s", selected, UTF8Type.instance.getString(type.fieldName(field)));
    }

    private FieldSelector(UserType type, int field, Selector selected)
    {
        this.type = type;
        this.field = field;
        this.selected = selected;
    }
}