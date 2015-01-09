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
import java.util.*;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.ListSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListType<T> extends CollectionType<List<T>>
{
    private static final Logger logger = LoggerFactory.getLogger(ListType.class);

    // interning instances
    private static final Map<AbstractType<?>, ListType> instances = new HashMap<>();
    private static final Map<AbstractType<?>, ListType> frozenInstances = new HashMap<>();

    private final AbstractType<T> elements;
    public final ListSerializer<T> serializer;
    private final boolean isMultiCell;

    public static ListType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("ListType takes exactly 1 type parameter");

        return getInstance(l.get(0), true);
    }

    public static synchronized <T> ListType<T> getInstance(AbstractType<T> elements, boolean isMultiCell)
    {
        Map<AbstractType<?>, ListType> internMap = isMultiCell ? instances : frozenInstances;
        ListType<T> t = internMap.get(elements);
        if (t == null)
        {
            t = new ListType<T>(elements, isMultiCell);
            internMap.put(elements, t);
        }
        return t;
    }

    private ListType(AbstractType<T> elements, boolean isMultiCell)
    {
        super(Kind.LIST);
        this.elements = elements;
        this.serializer = ListSerializer.getInstance(elements.getSerializer());
        this.isMultiCell = isMultiCell;
    }

    public AbstractType<T> getElementsType()
    {
        return elements;
    }

    public AbstractType<UUID> nameComparator()
    {
        return TimeUUIDType.instance;
    }

    public AbstractType<T> valueComparator()
    {
        return elements;
    }

    public ListSerializer<T> getSerializer()
    {
        return serializer;
    }

    @Override
    public AbstractType<?> freeze()
    {
        if (isMultiCell)
            return getInstance(this.elements, false);
        else
            return this;
    }

    @Override
    public boolean isMultiCell()
    {
        return isMultiCell;
    }

    @Override
    public boolean isCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        return this.elements.isCompatibleWith(((ListType) previous).elements);
    }

    @Override
    public boolean isValueCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        return this.elements.isValueCompatibleWithInternal(((ListType) previous).elements);
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return compareListOrSet(elements, o1, o2);
    }

    static int compareListOrSet(AbstractType<?> elementsComparator, ByteBuffer o1, ByteBuffer o2)
    {
        // Note that this is only used if the collection is frozen
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();

        int size1 = CollectionSerializer.readCollectionSize(bb1, 3);
        int size2 = CollectionSerializer.readCollectionSize(bb2, 3);

        for (int i = 0; i < Math.min(size1, size2); i++)
        {
            ByteBuffer v1 = CollectionSerializer.readValue(bb1, 3);
            ByteBuffer v2 = CollectionSerializer.readValue(bb2, 3);
            int cmp = elementsComparator.compare(v1, v2);
            if (cmp != 0)
                return cmp;
        }

        return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append("(");
        sb.append(getClass().getName());
        sb.append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements), ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(")");
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(List<Cell> cells)
    {
        assert isMultiCell;
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(cells.size());
        for (Cell c : cells)
            bbs.add(c.value());
        return bbs;
    }
}
