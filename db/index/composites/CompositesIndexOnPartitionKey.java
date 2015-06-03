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
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Index on a PARTITION_KEY column definition.
 *
 * This suppose a composite row key:
 *   rk = rk_0 ... rk_n
 *
 * The corresponding index entry will be:
 *   - index row key will be rk_i (where i == columnDef.componentIndex)
 *   - cell name will be: rk ck
 *     where rk is the fully partition key and ck the clustering keys of the
 *     original cell names (thus excluding the last column name as we want to refer to
 *     the whole CQL3 row, not just the cell itself)
 *
 * Note that contrarily to other type of index, we repeat the indexed value in
 * the index cell name (we use the whole partition key). The reason is that we
 * want to order the index cell name by partitioner first, and skipping a part
 * of the row key would change the order.
 */
public class CompositesIndexOnPartitionKey extends CompositesIndex
{
    public static CellNameType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int ckCount = baseMetadata.clusteringColumns().size();
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(ckCount + 1);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < ckCount; i++)
            types.add(baseMetadata.comparator.subtype(i));
        return new CompoundDenseCellNameType(types);
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Cell cell)
    {
        CompositeType keyComparator = (CompositeType)baseCfs.metadata.getKeyValidator();
        ByteBuffer[] components = keyComparator.split(rowKey);
        return components[columnDef.position()];
    }

    protected Composite makeIndexColumnPrefix(ByteBuffer rowKey, Composite columnName)
    {
        int count = Math.min(baseCfs.metadata.clusteringColumns().size(), columnName.size());
        CBuilder builder = getIndexComparator().prefixBuilder();
        builder.add(rowKey);
        for (int i = 0; i < count; i++)
            builder.add(columnName.get(i));
        return builder.build();
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Cell indexEntry)
    {
        int ckCount = baseCfs.metadata.clusteringColumns().size();
        CBuilder builder = baseCfs.getComparator().builder();
        for (int i = 0; i < ckCount; i++)
            builder.add(indexEntry.name().get(i + 1));

        return new IndexedEntry(indexedValue, indexEntry.name(), indexEntry.timestamp(), indexEntry.name().get(0), builder.build());
    }

    @Override
    public boolean indexes(CellName name)
    {
        // Since a partition key is always full, we always index it
        return true;
    }

    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        return data.hasOnlyTombstones(now);
    }

    @Override
    public void delete(ByteBuffer rowKey, Cell cell, OpOrder.Group opGroup)
    {
        // We only know that one column of the CQL row has been updated/deleted, but we don't know if the
        // full row has been deleted so we should not do anything. If it ends up that the whole row has
        // been deleted, it will be eventually cleaned up on read because the entry will be detected stale.
    }
}
