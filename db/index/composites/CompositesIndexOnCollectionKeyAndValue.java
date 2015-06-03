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
package org.apache.cassandra.db.index.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.*;

/**
 * Index on the element and value of cells participating in a collection.
 *
 * The row keys for this index are a composite of the collection element
 * and value of indexed columns.
 */
public class CompositesIndexOnCollectionKeyAndValue extends CompositesIndexIncludingCollectionKey
{
    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        CollectionType colType = (CollectionType)columnDef.type;
        return CompositeType.getInstance(colType.nameComparator(), colType.valueComparator());
    }

    @Override
    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Cell cell)
    {
        final ByteBuffer key = cell.name().get(columnDef.position() + 1);
        final ByteBuffer value = cell.value();
        return CompositeType.build(key, value);
    }

    @Override
    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        Cell cell = extractTargetCell(entry, data);
        if (cellIsDead(cell, now))
            return true;
        ByteBuffer indexCollectionValue = extractCollectionValue(entry);
        ByteBuffer targetCollectionValue = cell.value();
        AbstractType<?> valueComparator = ((CollectionType)columnDef.type).valueComparator();
        return valueComparator.compare(indexCollectionValue, targetCollectionValue) != 0;
    }

    private Cell extractTargetCell(IndexedEntry entry, ColumnFamily data)
    {
        ByteBuffer collectionKey = extractCollectionKey(entry);
        CellName name = data.getComparator().create(entry.indexedEntryPrefix, columnDef, collectionKey);
        return data.getColumn(name);
    }

    private ByteBuffer extractCollectionKey(IndexedEntry entry)
    {
        return extractIndexKeyComponent(entry, 0);
    }

    private ByteBuffer extractIndexKeyComponent(IndexedEntry entry, int component)
    {
        return CompositeType.extractComponent(entry.indexValue.getKey(), component);
    }

    private ByteBuffer extractCollectionValue(IndexedEntry entry)
    {
        return extractIndexKeyComponent(entry, 1);
    }

    private boolean cellIsDead(Cell cell, long now)
    {
        return cell == null || !cell.isLive(now);
    }
}
