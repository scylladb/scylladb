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

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;

public abstract class CellNames
{
    private CellNames() {}

    public static CellNameType fromAbstractType(AbstractType<?> type, boolean isDense)
    {
        if (isDense)
        {
            if (type instanceof CompositeType)
            {
                return new CompoundDenseCellNameType(((CompositeType)type).types);
            }
            else
            {
                return new SimpleDenseCellNameType(type);
            }
        }
        else
        {
            if (type instanceof CompositeType)
            {
                List<AbstractType<?>> types = ((CompositeType)type).types;
                if (types.get(types.size() - 1) instanceof ColumnToCollectionType)
                {
                    // We don't allow collection for super columns, so the "name" type *must* be UTF8
                    assert types.get(types.size() - 2) instanceof UTF8Type;
                    return new CompoundSparseCellNameType.WithCollection(types.subList(0, types.size() - 2), (ColumnToCollectionType)types.get(types.size() - 1));
                }
                else
                {
                    AbstractType<?> nameType = types.get(types.size() - 1);
                    return new CompoundSparseCellNameType(types.subList(0, types.size() - 1), nameType);
                }
            }
            else
            {
                return new SimpleSparseCellNameType(type);
            }
        }
    }

    // Mainly for tests and a few cases where we know what we need and didn't wanted to pass the type around.
    // Avoid in general, prefer the CellNameType methods.
    public static CellName simpleDense(ByteBuffer bb)
    {
        assert bb.hasRemaining();
        return new SimpleDenseCellName(bb);
    }

    public static CellName simpleSparse(ColumnIdentifier identifier)
    {
        return new SimpleSparseCellName(identifier);
    }

    // Mainly for tests and a few cases where we know what we need and didn't wanted to pass the type around
    // Avoid in general, prefer the CellNameType methods.
    public static CellName compositeDense(ByteBuffer... bbs)
    {
        return new CompoundDenseCellName(bbs);
    }

    public static CellName compositeSparse(ByteBuffer[] bbs, ColumnIdentifier identifier, boolean isStatic)
    {
        return new CompoundSparseCellName(bbs, identifier, isStatic);
    }

    public static CellName compositeSparseWithCollection(ByteBuffer[] bbs, ByteBuffer collectionElement, ColumnIdentifier identifier, boolean isStatic)
    {
        return new CompoundSparseCellName.WithCollection(bbs, identifier, collectionElement, isStatic);
    }

    public static String getColumnsString(CellNameType type, Iterable<Cell> columns)
    {
        StringBuilder builder = new StringBuilder();
        for (Cell cell : columns)
            builder.append(cell.getString(type)).append(",");
        return builder.toString();
    }
}
