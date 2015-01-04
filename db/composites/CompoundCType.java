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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;

/**
 * A truly-composite CType.
 */
public class CompoundCType extends AbstractCType
{
    final List<AbstractType<?>> types;

    // It's up to the caller to pass a list that is effectively immutable
    public CompoundCType(List<AbstractType<?>> types)
    {
        super(isByteOrderComparable(types));
        this.types = types;
    }

    public boolean isCompound()
    {
        return true;
    }

    public int size()
    {
        return types.size();
    }

    public AbstractType<?> subtype(int i)
    {
        return types.get(i);
    }

    public Composite fromByteBuffer(ByteBuffer bytes)
    {
        if (!bytes.hasRemaining())
            return Composites.EMPTY;

        ByteBuffer[] elements = new ByteBuffer[size()];
        int idx = bytes.position(), i = 0;
        byte eoc = 0;

        boolean isStatic = false;
        if (CompositeType.isStaticName(bytes))
        {
            isStatic = true;
            idx += 2;
        }

        while (idx < bytes.limit())
        {
            checkRemaining(bytes, idx, 2);
            int length = bytes.getShort(idx) & 0xFFFF;
            idx += 2;

            checkRemaining(bytes, idx, length + 1);
            elements[i++] = sliceBytes(bytes, idx, length);
            idx += length;
            eoc = bytes.get(idx++);
        }
        return new CompoundComposite(elements, i, isStatic).withEOC(Composite.EOC.from(eoc));
    }

    public CBuilder builder()
    {
        return new CompoundCBuilder(this);
    }

    public CompoundCType setSubtype(int position, AbstractType<?> newType)
    {
        List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(types);
        newTypes.set(position, newType);
        return new CompoundCType(newTypes);
    }

    public AbstractType<?> asAbstractType()
    {
        return CompositeType.getInstance(types);
    }

    public static class CompoundCBuilder implements CBuilder
    {
        private final CType type;
        private final ByteBuffer[] values;
        private int size;
        private boolean built;

        public CompoundCBuilder(CType type)
        {
            this.type = type;
            this.values = new ByteBuffer[type.size()];
        }

        public int remainingCount()
        {
            return values.length - size;
        }

        public CBuilder add(ByteBuffer value)
        {
            if (isDone())
                throw new IllegalStateException();
            values[size++] = value;
            return this;
        }

        public CBuilder add(Object value)
        {
            return add(((AbstractType)type.subtype(size)).decompose(value));
        }

        private boolean isDone()
        {
            return remainingCount() == 0 || built;
        }

        public Composite build()
        {
            if (size == 0)
                return Composites.EMPTY;

            // We don't allow to add more element to a builder that has been built so
            // that we don't have to copy values.
            built = true;

            // If the builder is full and we're building a dense cell name, then we can
            // directly allocate the CellName object as it's complete.
            if (size == values.length && type instanceof CellNameType && ((CellNameType)type).isDense())
                return new CompoundDenseCellName(values);
            return new CompoundComposite(values, size, false);
        }

        public Composite buildWith(ByteBuffer value)
        {
            ByteBuffer[] newValues = Arrays.copyOf(values, values.length);
            newValues[size] = value;
            // Same as above
            if (size+1 == newValues.length && type instanceof CellNameType && ((CellNameType)type).isDense())
                return new CompoundDenseCellName(newValues);

            return new CompoundComposite(newValues, size+1, false);
        }

        public Composite buildWith(List<ByteBuffer> newValues)
        {
            ByteBuffer[] buffers = Arrays.copyOf(values, values.length);
            int newSize = size;
            for (ByteBuffer value : newValues)
                buffers[newSize++] = value;

            if (newSize == buffers.length && type instanceof CellNameType && ((CellNameType)type).isDense())
                return new CompoundDenseCellName(buffers);

            return new CompoundComposite(buffers, newSize, false);
        }
    }
}
