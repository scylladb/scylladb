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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.utils.Pair;

/**
 * Processed CAS conditions and update on potentially multiple rows of the same partition.
 */
public class CQL3CasRequest implements CASRequest
{
    private final CFMetaData cfm;
    private final ByteBuffer key;
    private final long now;
    private final boolean isBatch;

    // We index RowCondition by the prefix of the row they applied to for 2 reasons:
    //   1) this allows to keep things sorted to build the ColumnSlice array below
    //   2) this allows to detect when contradictory conditions are set (not exists with some other conditions on the same row)
    private final SortedMap<Composite, RowCondition> conditions;

    private final List<RowUpdate> updates = new ArrayList<>();

    public CQL3CasRequest(CFMetaData cfm, ByteBuffer key, boolean isBatch)
    {
        this.cfm = cfm;
        // When checking if conditions apply, we want to use a fixed reference time for a whole request to check
        // for expired cells. Note that this is unrelated to the cell timestamp.
        this.now = System.currentTimeMillis();
        this.key = key;
        this.conditions = new TreeMap<>(cfm.comparator);
        this.isBatch = isBatch;
    }

    public void addRowUpdate(Composite prefix, ModificationStatement stmt, QueryOptions options, long timestamp)
    {
        updates.add(new RowUpdate(prefix, stmt, options, timestamp));
    }

    public void addNotExist(Composite prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix, new NotExistCondition(prefix, now));
        if (previous != null && !(previous instanceof NotExistCondition))
        {
            // these should be prevented by the parser, but it doesn't hurt to check
            if (previous instanceof ExistCondition)
                throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
            else
                throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
    }

    public void addExist(Composite prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix, new ExistCondition(prefix, now));
        // this should be prevented by the parser, but it doesn't hurt to check
        if (previous instanceof NotExistCondition)
            throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
    }

    public void addConditions(Composite prefix, Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
    {
        RowCondition condition = conditions.get(prefix);
        if (condition == null)
        {
            condition = new ColumnsConditions(prefix, now);
            conditions.put(prefix, condition);
        }
        else if (!(condition instanceof ColumnsConditions))
        {
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
        ((ColumnsConditions)condition).addConditions(conds, options);
    }

    public IDiskAtomFilter readFilter()
    {
        assert !conditions.isEmpty();
        ColumnSlice[] slices = new ColumnSlice[conditions.size()];
        int i = 0;
        // We always read CQL rows entirely as on CAS failure we want to be able to distinguish between "row exists
        // but all values for which there were conditions are null" and "row doesn't exists", and we can't rely on the
        // row marker for that (see #6623)
        for (Composite prefix : conditions.keySet())
            slices[i++] = prefix.slice();

        int toGroup = cfm.comparator.isDense() ? -1 : cfm.clusteringColumns().size();
        slices = ColumnSlice.deoverlapSlices(slices, cfm.comparator);
        assert ColumnSlice.validateSlices(slices, cfm.comparator, false);
        return new SliceQueryFilter(slices, false, slices.length, toGroup);
    }

    public boolean appliesTo(ColumnFamily current) throws InvalidRequestException
    {
        for (RowCondition condition : conditions.values())
        {
            if (!condition.appliesTo(current))
                return false;
        }
        return true;
    }

    public ColumnFamily makeUpdates(ColumnFamily current) throws InvalidRequestException
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfm);
        for (RowUpdate upd : updates)
            upd.applyUpdates(current, cf);

        if (isBatch)
            BatchStatement.verifyBatchSize(Collections.singleton(cf));

        return cf;
    }

    /**
     * Due to some operation on lists, we can't generate the update that a given Modification statement does before
     * we get the values read by the initial read of Paxos. A RowUpdate thus just store the relevant information
     * (include the statement iself) to generate those updates. We'll have multiple RowUpdate for a Batch, otherwise
     * we'll have only one.
     */
    private class RowUpdate
    {
        private final Composite rowPrefix;
        private final ModificationStatement stmt;
        private final QueryOptions options;
        private final long timestamp;

        private RowUpdate(Composite rowPrefix, ModificationStatement stmt, QueryOptions options, long timestamp)
        {
            this.rowPrefix = rowPrefix;
            this.stmt = stmt;
            this.options = options;
            this.timestamp = timestamp;
        }

        public void applyUpdates(ColumnFamily current, ColumnFamily updates) throws InvalidRequestException
        {
            Map<ByteBuffer, CQL3Row> map = null;
            if (stmt.requiresRead())
            {
                // Uses the "current" values read by Paxos for lists operation that requires a read
                Iterator<CQL3Row> iter = cfm.comparator.CQL3RowBuilder(cfm, now).group(current.iterator(new ColumnSlice[]{ rowPrefix.slice() }));
                if (iter.hasNext())
                {
                    map = Collections.singletonMap(key, iter.next());
                    assert !iter.hasNext() : "We shoudn't be updating more than one CQL row per-ModificationStatement";
                }
            }

            UpdateParameters params = new UpdateParameters(cfm, options, timestamp, stmt.getTimeToLive(options), map);
            stmt.addUpdateForKey(updates, key, rowPrefix, params);
        }
    }

    private static abstract class RowCondition
    {
        public final Composite rowPrefix;
        protected final long now;

        protected RowCondition(Composite rowPrefix, long now)
        {
            this.rowPrefix = rowPrefix;
            this.now = now;
        }

        public abstract boolean appliesTo(ColumnFamily current) throws InvalidRequestException;
    }

    private static class NotExistCondition extends RowCondition
    {
        private NotExistCondition(Composite rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return true;

            Iterator<Cell> iter = current.iterator(new ColumnSlice[]{ rowPrefix.slice() });
            while (iter.hasNext())
                if (iter.next().isLive(now))
                    return false;
            return true;
        }
    }

    private static class ExistCondition extends RowCondition
    {
        private ExistCondition(Composite rowPrefix, long now)
        {
            super (rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return false;

            Iterator<Cell> iter = current.iterator(new ColumnSlice[]{ rowPrefix.slice() });
            while (iter.hasNext())
                if (iter.next().isLive(now))
                    return true;
            return false;
        }
    }

    private static class ColumnsConditions extends RowCondition
    {
        private final Multimap<Pair<ColumnIdentifier, ByteBuffer>, ColumnCondition.Bound> conditions = HashMultimap.create();

        private ColumnsConditions(Composite rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public void addConditions(Collection<ColumnCondition> conds, QueryOptions options) throws InvalidRequestException
        {
            for (ColumnCondition condition : conds)
            {
                ColumnCondition.Bound current = condition.bind(options);
                conditions.put(Pair.create(condition.column.name, current.getCollectionElementValue()), current);
            }
        }

        public boolean appliesTo(ColumnFamily current) throws InvalidRequestException
        {
            if (current == null)
                return conditions.isEmpty();

            for (ColumnCondition.Bound condition : conditions.values())
                if (!condition.appliesTo(rowPrefix, current, now))
                    return false;
            return true;
        }
    }
}
