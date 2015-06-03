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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CompositesSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(CompositesSearcher.class);

    public CompositesSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        super(indexManager, columns);
    }

    @Override
    public List<Row> search(ExtendedFilter filter)
    {
        assert filter.getClause() != null && !filter.getClause().isEmpty();
        final IndexExpression primary = highestSelectivityPredicate(filter.getClause(), true);
        final CompositesIndex index = (CompositesIndex)indexManager.getIndexForColumn(primary.column);
        // TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try* to delete stale entries, without blocking if there's no room
        // as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room being made
        try (OpOrder.Group writeOp = baseCfs.keyspace.writeOrder.start(); OpOrder.Group baseOp = baseCfs.readOrdering.start(); OpOrder.Group indexOp = index.getIndexCfs().readOrdering.start())
        {
            return baseCfs.filter(getIndexedIterator(writeOp, filter, primary, index), filter);
        }
    }

    private Composite makePrefix(CompositesIndex index, ByteBuffer key, ExtendedFilter filter, boolean isStart)
    {
        if (key.remaining() == 0)
            return Composites.EMPTY;

        Composite prefix;
        IDiskAtomFilter columnFilter = filter.columnFilter(key);
        if (columnFilter instanceof SliceQueryFilter)
        {
            SliceQueryFilter sqf = (SliceQueryFilter)columnFilter;
            Composite columnName = isStart ? sqf.start() : sqf.finish();
            prefix = columnName.isEmpty() ? index.getIndexComparator().make(key) : index.makeIndexColumnPrefix(key, columnName);
        }
        else
        {
            prefix = index.getIndexComparator().make(key);
        }
        return isStart ? prefix.start() : prefix.end();
    }

    private ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final OpOrder.Group writeOp, final ExtendedFilter filter, final IndexExpression primary, final CompositesIndex index)
    {
        // Start with the most-restrictive indexed clause, then apply remaining clauses
        // to each row matching that clause.
        // TODO: allow merge join instead of just one index + loop
        assert index != null;
        assert index.getIndexCfs() != null;
        final DecoratedKey indexKey = index.getIndexKeyFor(primary.value);

        if (logger.isDebugEnabled())
            logger.debug("Most-selective indexed predicate is {}", index.expressionString(primary));

        /*
         * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
         * the indexed row unfortunately (which will be inefficient), because we have not way to intuit the smallest
         * possible key having a given token. A fix would be to actually store the token along the key in the
         * indexed row.
         */
        final AbstractBounds<RowPosition> range = filter.dataRange.keyRange();
        ByteBuffer startKey = range.left instanceof DecoratedKey ? ((DecoratedKey)range.left).getKey() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        ByteBuffer endKey = range.right instanceof DecoratedKey ? ((DecoratedKey)range.right).getKey() : ByteBufferUtil.EMPTY_BYTE_BUFFER;

        final CellNameType baseComparator = baseCfs.getComparator();
        final CellNameType indexComparator = index.getIndexCfs().getComparator();

        final Composite startPrefix = makePrefix(index, startKey, filter, true);
        final Composite endPrefix = makePrefix(index, endKey, filter, false);

        return new ColumnFamilyStore.AbstractScanIterator()
        {
            private Composite lastSeenPrefix = startPrefix;
            private Deque<Cell> indexCells;
            private int columnsRead = Integer.MAX_VALUE;
            private int limit = filter.currentLimit();
            private int columnsCount = 0;

            // We have to fetch at least two rows to avoid breaking paging if the first row doesn't satisfy all clauses
            private int indexCellsPerQuery = Math.max(2, Math.min(filter.maxColumns(), filter.maxRows()));

            public boolean needsFiltering()
            {
                return false;
            }

            private Row makeReturn(DecoratedKey key, ColumnFamily data)
            {
                if (data == null)
                    return endOfData();

                assert key != null;
                return new Row(key, data);
            }

            protected Row computeNext()
            {
                /*
                 * Our internal index code is wired toward internal rows. So we need to accumulate all results for a given
                 * row before returning from this method. Which unfortunately means that this method has to do what
                 * CFS.filter does for KeysIndex.
                 */
                DecoratedKey currentKey = null;
                ColumnFamily data = null;
                Composite previousPrefix = null;

                while (true)
                {
                    // Did we get more columns that needed to respect the user limit?
                    // (but we still need to return what has been fetched already)
                    if (columnsCount >= limit)
                        return makeReturn(currentKey, data);

                    if (indexCells == null || indexCells.isEmpty())
                    {
                        if (columnsRead < indexCellsPerQuery)
                        {
                            logger.trace("Read only {} (< {}) last page through, must be done", columnsRead, indexCellsPerQuery);
                            return makeReturn(currentKey, data);
                        }

                        if (logger.isTraceEnabled())
                            logger.trace("Scanning index {} starting with {}",
                                         index.expressionString(primary), indexComparator.getString(startPrefix));

                        QueryFilter indexFilter = QueryFilter.getSliceFilter(indexKey,
                                                                             index.getIndexCfs().name,
                                                                             lastSeenPrefix,
                                                                             endPrefix,
                                                                             false,
                                                                             indexCellsPerQuery,
                                                                             filter.timestamp);
                        ColumnFamily indexRow = index.getIndexCfs().getColumnFamily(indexFilter);
                        if (indexRow == null || !indexRow.hasColumns())
                            return makeReturn(currentKey, data);

                        Collection<Cell> sortedCells = indexRow.getSortedColumns();
                        columnsRead = sortedCells.size();
                        indexCells = new ArrayDeque<>(sortedCells);
                        Cell firstCell = sortedCells.iterator().next();

                        // Paging is racy, so it is possible the first column of a page is not the last seen one.
                        if (lastSeenPrefix != startPrefix && lastSeenPrefix.equals(firstCell.name()))
                        {
                            // skip the row we already saw w/ the last page of results
                            indexCells.poll();
                            logger.trace("Skipping {}", indexComparator.getString(firstCell.name()));
                        }
                    }

                    while (!indexCells.isEmpty() && columnsCount <= limit)
                    {
                        Cell cell = indexCells.poll();
                        lastSeenPrefix = cell.name();
                        if (!cell.isLive(filter.timestamp))
                        {
                            logger.trace("skipping {}", cell.name());
                            continue;
                        }

                        CompositesIndex.IndexedEntry entry = index.decodeEntry(indexKey, cell);
                        DecoratedKey dk = baseCfs.partitioner.decorateKey(entry.indexedKey);

                        // Are we done for this row?
                        if (currentKey == null)
                        {
                            currentKey = dk;
                        }
                        else if (!currentKey.equals(dk))
                        {
                            DecoratedKey previousKey = currentKey;
                            currentKey = dk;
                            previousPrefix = null;

                            // We're done with the previous row, return it if it had data, continue otherwise
                            indexCells.addFirst(cell);
                            if (data == null)
                                continue;
                            else
                                return makeReturn(previousKey, data);
                        }

                        if (!range.contains(dk))
                        {
                            // Either we're not yet in the range cause the range is start excluding, or we're
                            // past it.
                            if (!range.right.isMinimum() && range.right.compareTo(dk) < 0)
                            {
                                logger.trace("Reached end of assigned scan range");
                                return endOfData();
                            }
                            else
                            {
                                logger.debug("Skipping entry {} before assigned scan range", dk.getToken());
                                continue;
                            }
                        }

                        // Check if this entry cannot be a hit due to the original cell filter
                        Composite start = entry.indexedEntryPrefix;
                        if (!filter.columnFilter(dk.getKey()).maySelectPrefix(baseComparator, start))
                            continue;

                        // If we've record the previous prefix, it means we're dealing with an index on the collection value. In
                        // that case, we can have multiple index prefix for the same CQL3 row. In that case, we want to only add
                        // the CQL3 row once (because requesting the data multiple time would be inefficient but more importantly
                        // because we shouldn't count the columns multiple times with the lastCounted() call at the end of this
                        // method).
                        if (previousPrefix != null && previousPrefix.equals(start))
                            continue;
                        else
                            previousPrefix = null;

                        if (logger.isTraceEnabled())
                            logger.trace("Adding index hit to current row for {}", indexComparator.getString(cell.name()));

                        // We always query the whole CQL3 row. In the case where the original filter was a name filter this might be
                        // slightly wasteful, but this probably doesn't matter in practice and it simplify things.
                        ColumnSlice dataSlice = new ColumnSlice(start, entry.indexedEntryPrefix.end());
                        // If the table has static columns, we must fetch them too as they may need to be returned too.
                        // Note that this is potentially wasteful for 2 reasons:
                        //  1) we will retrieve the static parts for each indexed row, even if we have more than one row in
                        //     the same partition. If we were to group data queries to rows on the same slice, which would
                        //     speed up things in general, we would also optimize here since we would fetch static columns only
                        //     once for each group.
                        //  2) at this point we don't know if the user asked for static columns or not, so we might be fetching
                        //     them for nothing. We would however need to ship the list of "CQL3 columns selected" with getRangeSlice
                        //     to be able to know that.
                        // TODO: we should improve both point above
                        ColumnSlice[] slices = baseCfs.metadata.hasStaticColumns()
                                             ? new ColumnSlice[]{ baseCfs.metadata.comparator.staticPrefix().slice(), dataSlice }
                                             : new ColumnSlice[]{ dataSlice };
                        SliceQueryFilter dataFilter = new SliceQueryFilter(slices, false, Integer.MAX_VALUE, baseCfs.metadata.clusteringColumns().size());
                        ColumnFamily newData = baseCfs.getColumnFamily(new QueryFilter(dk, baseCfs.name, dataFilter, filter.timestamp));
                        if (newData == null || index.isStale(entry, newData, filter.timestamp))
                        {
                            index.delete(entry, writeOp);
                            continue;
                        }

                        assert newData != null : "An entry with no data should have been considered stale";

                        // We know the entry is not stale and so the entry satisfy the primary clause. So whether
                        // or not the data satisfies the other clauses, there will be no point to re-check the
                        // same CQL3 row if we run into another collection value entry for this row.
                        if (entry.indexedEntryCollectionKey != null)
                            previousPrefix = start;

                        if (!filter.isSatisfiedBy(dk, newData, entry.indexedEntryPrefix, entry.indexedEntryCollectionKey))
                            continue;

                        if (data == null)
                            data = ArrayBackedSortedColumns.factory.create(baseCfs.metadata);
                        data.addAll(newData);
                        columnsCount += dataFilter.lastCounted();
                    }
                 }
             }

            public void close() throws IOException {}
        };
    }
}
