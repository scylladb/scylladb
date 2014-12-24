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
package org.apache.cassandra.cql3;

import java.util.List;
import java.util.Locale;
import java.nio.ByteBuffer;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.cql3.selection.SimpleSelector;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Represents an identifer for a CQL column definition.
 * TODO : should support light-weight mode without text representation for when not interned
 */
public class ColumnIdentifier extends org.apache.cassandra.cql3.selection.Selectable implements IMeasurableMemory
{
    public final ByteBuffer bytes;
    private final String text;

    private static final long EMPTY_SIZE = ObjectSizes.measure(new ColumnIdentifier("", true));

    public ColumnIdentifier(String rawText, boolean keepCase)
    {
        this.text = keepCase ? rawText : rawText.toLowerCase(Locale.US);
        this.bytes = ByteBufferUtil.bytes(this.text);
    }

    public ColumnIdentifier(ByteBuffer bytes, AbstractType<?> type)
    {
        this.bytes = bytes;
        this.text = type.getString(bytes);
    }

    public ColumnIdentifier(ByteBuffer bytes, String text)
    {
        this.bytes = bytes;
        this.text = text;
    }

    @Override
    public final int hashCode()
    {
        return bytes.hashCode();
    }

    @Override
    public final boolean equals(Object o)
    {
        // Note: it's worth checking for reference equality since we intern those
        // in SparseCellNameType
        if (this == o)
            return true;

        if(!(o instanceof ColumnIdentifier))
            return false;
        ColumnIdentifier that = (ColumnIdentifier)o;
        return bytes.equals(that.bytes);
    }

    @Override
    public String toString()
    {
        return text;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapOf(bytes)
             + ObjectSizes.sizeOf(text);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapExcludingData(bytes)
             + ObjectSizes.sizeOf(text);
    }

    public ColumnIdentifier clone(AbstractAllocator allocator)
    {
        return new ColumnIdentifier(allocator.clone(bytes), text);
    }

    public Selector.Factory newSelectorFactory(CFMetaData cfm, List<ColumnDefinition> defs) throws InvalidRequestException
    {
        ColumnDefinition def = cfm.getColumnDefinition(this);
        if (def == null)
            throw new InvalidRequestException(String.format("Undefined name %s in selection clause", this));

        return SimpleSelector.newFactory(def.name.toString(), addAndGetIndex(def, defs), def.type);
    }

    /**
     * Because Thrift-created tables may have a non-text comparator, we cannot determine the proper 'key' until
     * we know the comparator. ColumnIdentifier.Raw is a placeholder that can be converted to a real ColumnIdentifier
     * once the comparator is known with prepare(). This should only be used with identifiers that are actual
     * column names. See CASSANDRA-8178 for more background.
     */
    public static class Raw implements Selectable.Raw
    {
        private final String rawText;
        private final String text;

        public Raw(String rawText, boolean keepCase)
        {
            this.rawText = rawText;
            this.text =  keepCase ? rawText : rawText.toLowerCase(Locale.US);
        }

        public ColumnIdentifier prepare(CFMetaData cfm)
        {
            AbstractType<?> comparator = cfm.comparator.asAbstractType();
            if (cfm.getIsDense() || comparator instanceof CompositeType || comparator instanceof UTF8Type)
                return new ColumnIdentifier(text, true);

            // We have a Thrift-created table with a non-text comparator.  We need to parse column names with the comparator
            // to get the correct ByteBuffer representation.  However, this doesn't apply to key aliases, so we need to
            // make a special check for those and treat them normally.  See CASSANDRA-8178.
            ByteBuffer bufferName = ByteBufferUtil.bytes(text);
            for (ColumnDefinition def : cfm.partitionKeyColumns())
            {
                if (def.name.bytes.equals(bufferName))
                    return new ColumnIdentifier(text, true);
            }
            return new ColumnIdentifier(comparator.fromString(rawText), text);
        }

        public boolean processesSelection()
        {
            return false;
        }

        @Override
        public final int hashCode()
        {
            return text.hashCode();
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof ColumnIdentifier.Raw))
                return false;
            ColumnIdentifier.Raw that = (ColumnIdentifier.Raw)o;
            return text.equals(that.text);
        }

        @Override
        public String toString()
        {
            return text;
        }
    }
}
