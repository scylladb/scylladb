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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Base class for secondary indexes where composites are involved.
 */
public abstract class CompositesIndex extends AbstractSimplePerColumnSecondaryIndex
{
    private volatile CellNameType indexComparator;

    protected CellNameType getIndexComparator()
    {
        // Yes, this is racy, but doing this more than once is not a big deal, we just want to avoid doing it every time
        // More seriously, we should fix that whole SecondaryIndex API so this can be a final and avoid all that non-sense.
        if (indexComparator == null)
        {
            assert columnDef != null;
            indexComparator = getIndexComparator(baseCfs.metadata, columnDef);
        }
        return indexComparator;
    }

    public static CompositesIndex create(ColumnDefinition cfDef)
    {
        if (cfDef.type.isCollection() && cfDef.type.isMultiCell())
        {
            switch (((CollectionType)cfDef.type).kind)
            {
                case LIST:
                    return new CompositesIndexOnCollectionValue();
                case SET:
                    return new CompositesIndexOnCollectionKey();
                case MAP:
                    if (cfDef.hasIndexOption(SecondaryIndex.INDEX_KEYS_OPTION_NAME))
                        return new CompositesIndexOnCollectionKey();
                    else if (cfDef.hasIndexOption(SecondaryIndex.INDEX_ENTRIES_OPTION_NAME))
                        return new CompositesIndexOnCollectionKeyAndValue();
                    else
                        return new CompositesIndexOnCollectionValue();
            }
        }

        switch (cfDef.kind)
        {
            case CLUSTERING_COLUMN:
                return new CompositesIndexOnClusteringKey();
            case REGULAR:
                return new CompositesIndexOnRegular();
            case PARTITION_KEY:
                return new CompositesIndexOnPartitionKey();
            //case COMPACT_VALUE:
            //    return new CompositesIndexOnCompactValue();
        }
        throw new AssertionError();
    }

    // Check SecondaryIndex.getIndexComparator if you want to know why this is static
    public static CellNameType getIndexComparator(CFMetaData baseMetadata, ColumnDefinition cfDef)
    {
        if (cfDef.type.isCollection() && cfDef.type.isMultiCell())
        {
            switch (((CollectionType)cfDef.type).kind)
            {
                case LIST:
                    return CompositesIndexOnCollectionValue.buildIndexComparator(baseMetadata, cfDef);
                case SET:
                    return CompositesIndexOnCollectionKey.buildIndexComparator(baseMetadata, cfDef);
                case MAP:
                    if (cfDef.hasIndexOption(SecondaryIndex.INDEX_KEYS_OPTION_NAME))
                        return CompositesIndexOnCollectionKey.buildIndexComparator(baseMetadata, cfDef);
                    else if (cfDef.hasIndexOption(SecondaryIndex.INDEX_ENTRIES_OPTION_NAME))
                        return CompositesIndexOnCollectionKeyAndValue.buildIndexComparator(baseMetadata, cfDef);
                    else
                        return CompositesIndexOnCollectionValue.buildIndexComparator(baseMetadata, cfDef);
            }
        }

        switch (cfDef.kind)
        {
            case CLUSTERING_COLUMN:
                return CompositesIndexOnClusteringKey.buildIndexComparator(baseMetadata, cfDef);
            case REGULAR:
                return CompositesIndexOnRegular.buildIndexComparator(baseMetadata, cfDef);
            case PARTITION_KEY:
                return CompositesIndexOnPartitionKey.buildIndexComparator(baseMetadata, cfDef);
            //case COMPACT_VALUE:
            //    return CompositesIndexOnCompactValue.buildIndexComparator(baseMetadata, cfDef);
        }
        throw new AssertionError();
    }

    protected CellName makeIndexColumnName(ByteBuffer rowKey, Cell cell)
    {
        return getIndexComparator().create(makeIndexColumnPrefix(rowKey, cell.name()), null);
    }

    protected abstract Composite makeIndexColumnPrefix(ByteBuffer rowKey, Composite columnName);

    public abstract IndexedEntry decodeEntry(DecoratedKey indexedValue, Cell indexEntry);

    public abstract boolean isStale(IndexedEntry entry, ColumnFamily data, long now);

    public void delete(IndexedEntry entry, OpOrder.Group opGroup)
    {
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        ColumnFamily cfi = ArrayBackedSortedColumns.factory.create(indexCfs.metadata);
        cfi.addTombstone(entry.indexEntry, localDeletionTime, entry.timestamp);
        indexCfs.apply(entry.indexValue, cfi, SecondaryIndexManager.nullUpdater, opGroup, null);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", entry.indexValue, cfi);
    }

    protected AbstractType<?> getExpressionComparator()
    {
        return baseCfs.metadata.getColumnDefinitionComparator(columnDef);
    }

    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new CompositesSearcher(baseCfs.indexManager, columns);
    }

    public void validateOptions() throws ConfigurationException
    {
        ColumnDefinition columnDef = columnDefs.iterator().next();
        Map<String, String> options = new HashMap<String, String>(columnDef.getIndexOptions());

        // We used to have an option called "prefix_size" so skip it silently for backward compatibility sake.
        options.remove("prefix_size");

        if (columnDef.type.isCollection())
        {
            options.remove(SecondaryIndex.INDEX_VALUES_OPTION_NAME);
            options.remove(SecondaryIndex.INDEX_KEYS_OPTION_NAME);
            options.remove(SecondaryIndex.INDEX_ENTRIES_OPTION_NAME);
        }

        if (!options.isEmpty())
            throw new ConfigurationException("Unknown options provided for COMPOSITES index: " + options.keySet());
    }

    public static class IndexedEntry
    {
        public final DecoratedKey indexValue;
        public final CellName indexEntry;
        public final long timestamp;

        public final ByteBuffer indexedKey;
        public final Composite indexedEntryPrefix;
        public final ByteBuffer indexedEntryCollectionKey; // may be null

        public IndexedEntry(DecoratedKey indexValue, CellName indexEntry, long timestamp, ByteBuffer indexedKey, Composite indexedEntryPrefix)
        {
            this(indexValue, indexEntry, timestamp, indexedKey, indexedEntryPrefix, null);
        }

        public IndexedEntry(DecoratedKey indexValue,
                            CellName indexEntry,
                            long timestamp,
                            ByteBuffer indexedKey,
                            Composite indexedEntryPrefix,
                            ByteBuffer indexedEntryCollectionKey)
        {
            this.indexValue = indexValue;
            this.indexEntry = indexEntry;
            this.timestamp = timestamp;
            this.indexedKey = indexedKey;
            this.indexedEntryPrefix = indexedEntryPrefix;
            this.indexedEntryCollectionKey = indexedEntryCollectionKey;
        }
    }
}
