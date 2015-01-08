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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.transport.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;

import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The abstract validator that is the base for maps, sets and lists (both frozen and non-frozen).
 *
 * Please note that this comparator shouldn't be used "manually" (through thrift for instance).
 */
public abstract class CollectionType<T> extends AbstractType<T>
{
    private static final Logger logger = LoggerFactory.getLogger(CollectionType.class);

    public static final int MAX_ELEMENTS = 65535;

    public enum Kind
    {
        MAP
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return isKey ? Maps.keySpecOf(collection) : Maps.valueSpecOf(collection);
            }
        },
        SET
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return Sets.valueSpecOf(collection);
            }
        },
        LIST
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return Lists.valueSpecOf(collection);
            }
        };

        public abstract ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey);
    }

    public final Kind kind;

    protected CollectionType(Kind kind)
    {
        this.kind = kind;
    }

    public abstract AbstractType<?> nameComparator();
    public abstract AbstractType<?> valueComparator();

    @Override
    public abstract CollectionSerializer<T> getSerializer();

    public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
    {
        return kind.makeCollectionReceiver(collection, isKey);
    }

    public String getString(ByteBuffer bytes)
    {
        return BytesType.instance.getString(bytes);
    }

    public ByteBuffer fromString(String source)
    {
        try
        {
            return ByteBufferUtil.hexToBytes(source);
        }
        catch (NumberFormatException e)
        {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    public boolean isCollection()
    {
        return true;
    }

    @Override
    public void validateCellValue(ByteBuffer cellValue) throws MarshalException
    {
        if (isMultiCell())
            valueComparator().validate(cellValue);
        else
            super.validateCellValue(cellValue);
    }

    /**
     * Checks if this collection is Map.
     * @return <code>true</code> if this collection is a Map, <code>false</code> otherwise.
     */
    public boolean isMap()
    {
        return kind == Kind.MAP;
    }

    public List<Cell> enforceLimit(List<Cell> cells, int version)
    {
        assert isMultiCell();

        if (version >= Server.VERSION_3 || cells.size() <= MAX_ELEMENTS)
            return cells;

        logger.error("Detected collection with {} elements, more than the {} limit. Only the first {} elements will be returned to the client. "
                   + "Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.", cells.size(), MAX_ELEMENTS, MAX_ELEMENTS);
        return cells.subList(0, MAX_ELEMENTS);
    }

    public abstract List<ByteBuffer> serializedValues(List<Cell> cells);

    public ByteBuffer serializeForNativeProtocol(List<Cell> cells, int version)
    {
        assert isMultiCell();
        cells = enforceLimit(cells, version);
        List<ByteBuffer> values = serializedValues(cells);
        return CollectionSerializer.pack(values, cells.size(), version);
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        if (this == previous)
            return true;

        if (!getClass().equals(previous.getClass()))
            return false;

        CollectionType tprev = (CollectionType) previous;
        if (this.isMultiCell() != tprev.isMultiCell())
            return false;

        // subclasses should handle compatibility checks for frozen collections
        if (!this.isMultiCell())
            return isCompatibleWithFrozen(tprev);

        if (!this.nameComparator().isCompatibleWith(tprev.nameComparator()))
            return false;

        // the value comparator is only used for Cell values, so sorting doesn't matter
        return this.valueComparator().isValueCompatibleWith(tprev.valueComparator());
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> previous)
    {
        // for multi-cell collections, compatibility and value-compatibility are the same
        if (this.isMultiCell())
            return isCompatibleWith(previous);

        if (this == previous)
            return true;

        if (!getClass().equals(previous.getClass()))
            return false;

        CollectionType tprev = (CollectionType) previous;
        if (this.isMultiCell() != tprev.isMultiCell())
            return false;

        // subclasses should handle compatibility checks for frozen collections
        return isValueCompatibleWithFrozen(tprev);
    }

    /** A version of isCompatibleWith() to deal with non-multicell (frozen) collections */
    protected abstract boolean isCompatibleWithFrozen(CollectionType<?> previous);

    /** A version of isValueCompatibleWith() to deal with non-multicell (frozen) collections */
    protected abstract boolean isValueCompatibleWithFrozen(CollectionType<?> previous);

    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Collection(this);
    }

    @Override
    public String toString()
    {
        return this.toString(false);
    }
}
