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
package org.apache.cassandra.cql3.restrictions;

import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction.Contains;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Sets of single column restrictions.
 */
final class SingleColumnRestrictions implements Restrictions
{
    /**
     * The comparator used to sort the <code>Restriction</code>s.
     */
    private static final Comparator<ColumnDefinition> COLUMN_DEFINITION_COMPARATOR = new Comparator<ColumnDefinition>()
    {
        @Override
        public int compare(ColumnDefinition column, ColumnDefinition otherColumn)
        {
            int value = Integer.compare(column.position(), otherColumn.position());
            return value != 0 ? value : column.name.bytes.compareTo(otherColumn.name.bytes);
        }
    };

    /**
     * The restrictions per column.
     */
    protected final TreeMap<ColumnDefinition, Restriction> restrictions;

    public SingleColumnRestrictions()
    {
        this(new TreeMap<ColumnDefinition, Restriction>(COLUMN_DEFINITION_COMPARATOR));
    }

    protected SingleColumnRestrictions(TreeMap<ColumnDefinition, Restriction> restrictions)
    {
        this.restrictions = restrictions;
    }

    @Override
    public final void addIndexExpressionTo(List<IndexExpression> expressions,
                                           QueryOptions options) throws InvalidRequestException
    {
        for (Restriction restriction : restrictions.values())
            restriction.addIndexExpressionTo(expressions, options);
    }

    @Override
    public final Set<ColumnDefinition> getColumnDefs()
    {
        return restrictions.keySet();
    }

    /**
     * Returns the restriction associated to the specified column.
     *
     * @param columnDef the column definition
     * @return the restriction associated to the specified column
     */
    public Restriction getRestriction(ColumnDefinition columnDef)
    {
        return restrictions.get(columnDef);
    }

    @Override
    public boolean usesFunction(String ksName, String functionName)
    {
        for (Restriction restriction : restrictions.values())
            if (restriction.usesFunction(ksName, functionName))
                return true;

        return false;
    }

    @Override
    public final boolean isEmpty()
    {
        return getColumnDefs().isEmpty();
    }

    @Override
    public final int size()
    {
        return getColumnDefs().size();
    }

    /**
     * Adds the specified restriction to this set of restrictions.
     *
     * @param restriction the restriction to add
     * @return the new set of restrictions
     * @throws InvalidRequestException if the new restriction cannot be added
     */
    public SingleColumnRestrictions addRestriction(SingleColumnRestriction restriction) throws InvalidRequestException
    {
        TreeMap<ColumnDefinition, Restriction> newRestrictions = new TreeMap<>(this.restrictions);
        return new SingleColumnRestrictions(mergeRestrictions(newRestrictions, restriction));
    }

    private static TreeMap<ColumnDefinition, Restriction> mergeRestrictions(TreeMap<ColumnDefinition, Restriction> restrictions,
                                                                            Restriction restriction)
                                                                            throws InvalidRequestException
    {
        ColumnDefinition def = ((SingleColumnRestriction) restriction).getColumnDef();
        Restriction existing = restrictions.get(def);
        Restriction newRestriction = mergeRestrictions(existing, restriction);
        restrictions.put(def, newRestriction);
        return restrictions;
    }

    @Override
    public final boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        for (Restriction restriction : restrictions.values())
        {
            if (restriction.hasSupportingIndex(indexManager))
                return true;
        }
        return false;
    }

    /**
     * Returns the column after the specified one.
     *
     * @param columnDef the column for which the next one need to be found
     * @return the column after the specified one.
     */
    ColumnDefinition nextColumn(ColumnDefinition columnDef)
    {
        return restrictions.tailMap(columnDef, false).firstKey();
    }

    /**
     * Returns the definition of the last column.
     *
     * @return the definition of the last column.
     */
    ColumnDefinition lastColumn()
    {
        return isEmpty() ? null : this.restrictions.lastKey();
    }

    /**
     * Returns the last restriction.
     *
     * @return the last restriction.
     */
    Restriction lastRestriction()
    {
        return isEmpty() ? null : this.restrictions.lastEntry().getValue();
    }

    /**
     * Merges the two specified restrictions.
     *
     * @param restriction the first restriction
     * @param otherRestriction the second restriction
     * @return the merged restriction
     * @throws InvalidRequestException if the two restrictions cannot be merged
     */
    private static Restriction mergeRestrictions(Restriction restriction,
                                                 Restriction otherRestriction) throws InvalidRequestException
    {
        return restriction == null ? otherRestriction
                                   : restriction.mergeWith(otherRestriction);
    }

    /**
     * Checks if the restrictions contains multiple contains, contains key, or map[key] = value.
     *
     * @return <code>true</code> if the restrictions contains multiple contains, contains key, or ,
     * map[key] = value; <code>false</code> otherwise
     */
    public final boolean hasMultipleContains()
    {
        int numberOfContains = 0;
        for (Restriction restriction : restrictions.values())
        {
            if (restriction.isContains())
            {
                Contains contains = (Contains) restriction;
                numberOfContains += (contains.numberOfValues() + contains.numberOfKeys() + contains.numberOfEntries());
            }
        }
        return numberOfContains > 1;
    }
}