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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;

public abstract class AbstractCompoundCellNameType extends AbstractCellNameType
{
    protected final CompoundCType clusteringType;
    protected final CompoundCType fullType;

    protected final int clusteringSize;
    protected final int fullSize;

    protected AbstractCompoundCellNameType(CompoundCType clusteringType, CompoundCType fullType)
    {
        super(isByteOrderComparable(fullType.types));
        this.clusteringType = clusteringType;
        this.fullType = fullType;

        this.clusteringSize = clusteringType.size();
        this.fullSize = fullType.size();
    }

    public int clusteringPrefixSize()
    {
        return clusteringSize;
    }

    public boolean isCompound()
    {
        return true;
    }

    public int size()
    {
        return fullSize;
    }

    public AbstractType<?> subtype(int i)
    {
        return fullType.subtype(i);
    }

    public CBuilder prefixBuilder()
    {
        return clusteringType.builder();
    }

    public CBuilder builder()
    {
        return new CompoundCType.CompoundCBuilder(this);
    }

    @Override
    public Composite fromByteBuffer(ByteBuffer bytes)
    {
        if (!bytes.hasRemaining())
            return Composites.EMPTY;

        ByteBuffer[] elements = new ByteBuffer[fullSize];
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

        return makeWith(elements, i, Composite.EOC.from(eoc), isStatic);
    }

    public AbstractType<?> asAbstractType()
    {
        return CompositeType.getInstance(fullType.types);
    }

    public Deserializer newDeserializer(DataInput in)
    {
        return new CompositeDeserializer(this, in);
    }

    protected CellName makeCellName(ByteBuffer[] components)
    {
        return (CellName)makeWith(components, components.length, Composite.EOC.NONE, false);
    }

    protected abstract Composite makeWith(ByteBuffer[] components, int size, Composite.EOC eoc, boolean isStatic);
    protected abstract Composite copyAndMakeWith(ByteBuffer[] components, int size, Composite.EOC eoc, boolean isStatic);

    private static class CompositeDeserializer implements CellNameType.Deserializer
    {
        private static byte[] EMPTY = new byte[0];

        private final AbstractCompoundCellNameType type;
        private final DataInput in;

        private byte[] nextFull;
        private int nextIdx;

        private final ByteBuffer[] nextComponents;
        private int nextSize;
        private Composite.EOC nextEOC;
        private boolean nextIsStatic;

        public CompositeDeserializer(AbstractCompoundCellNameType type, DataInput in)
        {
            this.type = type;
            this.in = in;
            this.nextComponents = new ByteBuffer[type.size()];
        }

        public boolean hasNext() throws IOException
        {
            if (nextFull == null)
                maybeReadNext();
            return nextFull != EMPTY;
        }

        public boolean hasUnprocessed() throws IOException
        {
            return nextFull != null;
        }

        public int compareNextTo(Composite composite) throws IOException
        {
            maybeReadNext();

            if (composite.isEmpty())
                return nextFull == EMPTY ? 0 : 1;

            if (nextFull == EMPTY)
                return -1;

            if (nextIsStatic != composite.isStatic())
                return nextIsStatic ? -1 : 1;

            ByteBuffer previous = null;
            for (int i = 0; i < composite.size(); i++)
            {
                if (!hasComponent(i))
                    return nextEOC == Composite.EOC.END ? 1 : -1;

                AbstractType<?> comparator = type.subtype(i);
                ByteBuffer value1 = nextComponents[i];
                ByteBuffer value2 = composite.get(i);

                int cmp = comparator.compareCollectionMembers(value1, value2, previous);
                if (cmp != 0)
                    return cmp;

                previous = value1;
            }

            // If we have more component than composite
            if (!allComponentsDeserialized() || composite.size() < nextSize)
                return composite.eoc() == Composite.EOC.END ? -1 : 1;

            // same size, check eoc
            if (nextEOC != composite.eoc())
            {
                switch (nextEOC)
                {
                    case START: return -1;
                    case END:   return 1;
                    case NONE:  return composite.eoc() == Composite.EOC.START ? 1 : -1;
                }
            }

            return 0;
        }

        private boolean hasComponent(int i)
        {
            while (i >= nextSize && deserializeOne())
                continue;

            return i < nextSize;
        }

        private int readShort()
        {
            return ((nextFull[nextIdx++] & 0xFF) << 8) | (nextFull[nextIdx++] & 0xFF);
        }

        private int peekShort()
        {
            return ((nextFull[nextIdx] & 0xFF) << 8) | (nextFull[nextIdx+1] & 0xFF);
        }

        private boolean deserializeOne()
        {
            if (allComponentsDeserialized())
                return false;

            int length = readShort();
            ByteBuffer component = ByteBuffer.wrap(nextFull, nextIdx, length);
            nextIdx += length;
            nextComponents[nextSize++] = component;
            nextEOC = Composite.EOC.from(nextFull[nextIdx++]);
            return true;
        }

        private void deserializeAll()
        {
            while (deserializeOne())
                continue;
        }

        private boolean allComponentsDeserialized()
        {
            return nextIdx >= nextFull.length;
        }

        private void maybeReadNext() throws IOException
        {
            if (nextFull != null)
                return;

            nextIdx = 0;
            nextSize = 0;

            int length = in.readShort() & 0xFFFF;
            // Note that empty is ok because it marks the end of row
            if (length == 0)
            {
                nextFull = EMPTY;
                return;
            }

            nextFull = new byte[length];
            in.readFully(nextFull);

            // Is is a static?
            nextIsStatic = false;
            if (peekShort() == CompositeType.STATIC_MARKER)
            {
                nextIsStatic = true;
                readShort(); // Skip the static marker
            }
        }

        public Composite readNext() throws IOException
        {
            maybeReadNext();
            if (nextFull == EMPTY)
                return Composites.EMPTY;

            deserializeAll();
            Composite c = type.copyAndMakeWith(nextComponents, nextSize, nextEOC, nextIsStatic);
            nextFull = null;
            return c;
        }

        public void skipNext() throws IOException
        {
            maybeReadNext();
            nextFull = null;
        }
    }
}
