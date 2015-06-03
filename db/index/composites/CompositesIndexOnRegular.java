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
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;

/**
 * Index on a REGULAR column definition on a composite type.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name : v
 * where ck_i are the cluster keys, c_name the last component of the cell
 * composite name (or second to last if collections are in use, but this
 * has no impact) and v the cell value.
 *
 * Such a cell is indexed if c_name == columnDef.name, and it will generate
 * (makeIndexColumnName()) an index entry whose:
 *   - row key will be the value v (getIndexedValue()).
 *   - cell name will
 *       rk ck_0 ... ck_n
 *     where rk is the row key of the initial cell. I.e. the index entry store
 *     all the information require to locate back the indexed cell.
 */
public class CompositesIndexOnRegular extends CompositesIndex
{
    public static CellNameType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int prefixSize = columnDef.position();
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(prefixSize + 1);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < prefixSize; i++)
            types.add(baseMetadata.comparator.subtype(i));
        return new CompoundDenseCellNameType(types);
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
        return builder.build();
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Cell indexEntry)
    {
        CBuilder builder = baseCfs.getComparator().builder();
        for (int i = 0; i < columnDef.position(); i++)
            builder.add(indexEntry.name().get(i + 1));
        return new IndexedEntry(indexedValue, indexEntry.name(), indexEntry.timestamp(), indexEntry.name().get(0), builder.build());
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
        CellName name = data.getComparator().create(entry.indexedEntryPrefix, columnDef);
        Cell cell = data.getColumn(name);
        return cell == null || !cell.isLive(now) || columnDef.type.compare(entry.indexValue.getKey(), cell.value()) != 0;
    }
}
