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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.CompoundDenseCellNameType;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;

/**
 * Index the value of a collection cell.
 *
 * This is a lot like an index on REGULAR, except that we also need to make
 * the collection key part of the index entry so that:
 *   1) we don't have to scan the whole collection at query time to know the
 *   entry is stale and if it still satisfies the query.
 *   2) if a collection has multiple time the same value, we need one entry
 *   for each so that if we delete one of the value only we only delete the
 *   entry corresponding to that value.
 */
public class CompositesIndexOnCollectionValue extends CompositesIndex
{
    public static CellNameType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int prefixSize = columnDef.position();
        List<AbstractType<?>> types = new ArrayList<>(prefixSize + 2);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < prefixSize; i++)
            types.add(baseMetadata.comparator.subtype(i));
        types.add(((CollectionType)columnDef.type).nameComparator()); // collection key
        return new CompoundDenseCellNameType(types);
    }

    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        return ((CollectionType)columnDef.type).valueComparator();
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Cell cell)
    {
        return cell.value();
    }

    protected Composite makeIndexColumnPrefix(ByteBuffer rowKey, Composite cellName)
    {
        CBuilder builder = getIndexComparator().prefixBuilder();
        builder.add(rowKey);
        for (int i = 0; i < Math.min(columnDef.position(), cellName.size()); i++)
            builder.add(cellName.get(i));

        // When indexing, cellName is a full name including the collection
        // key. When searching, restricted clustering columns are included
        // but the collection key is not. In this case, don't try to add an
        // element to the builder for it, as it will just end up null and
        // error out when retrieving cells from the index cf (CASSANDRA-7525)
        if (cellName.size() >= columnDef.position() + 1)
            builder.add(cellName.get(columnDef.position() + 1));
        return builder.build();
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Cell indexEntry)
    {
        int prefixSize = columnDef.position();
        CellName name = indexEntry.name();
        CBuilder builder = baseCfs.getComparator().builder();
        for (int i = 0; i < prefixSize; i++)
            builder.add(name.get(i + 1));
        return new IndexedEntry(indexedValue, name, indexEntry.timestamp(), name.get(0), builder.build(), name.get(prefixSize + 1));
    }

    @Override
    public boolean supportsOperator(Operator operator)
    {
        return operator == Operator.CONTAINS && !(columnDef.type instanceof SetType);
    }

    @Override
    public boolean indexes(CellName name)
    {
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return name.size() > columnDef.position()
            && comp.compare(name.get(columnDef.position()), columnDef.name.bytes) == 0;
    }

    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        CellName name = data.getComparator().create(entry.indexedEntryPrefix, columnDef, entry.indexedEntryCollectionKey);
        Cell cell = data.getColumn(name);
        return cell == null || !cell.isLive(now) || ((CollectionType) columnDef.type).valueComparator().compare(entry.indexValue.getKey(), cell.value()) != 0;
    }
}
