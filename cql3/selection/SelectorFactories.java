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
package org.apache.cassandra.cql3.selection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.selection.Selector.Factory;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A set of <code>Selector</code> factories.
 */
final class SelectorFactories implements Iterable<Selector.Factory>
{
    /**
     * The <code>Selector</code> factories.
     */
    private final List<Selector.Factory> factories;

    /**
     * <code>true</code> if one of the factory creates writetime selectors.
     */
    private boolean containsWritetimeFactory;

    /**
     * <code>true</code> if one of the factory creates TTL selectors.
     */
    private boolean containsTTLFactory;

    /**
     * The number of factories creating aggregates.
     */
    private int numberOfAggregateFactories;

    /**
     * Creates a new <code>SelectorFactories</code> instance and collect the column definitions.
     *
     * @param selectables the <code>Selectable</code>s for which the factories must be created
     * @param cfm the Column Family Definition
     * @param defs the collector parameter for the column definitions
     * @return a new <code>SelectorFactories</code> instance
     * @throws InvalidRequestException if a problem occurs while creating the factories
     */
    public static SelectorFactories createFactoriesAndCollectColumnDefinitions(List<Selectable> selectables,
                                                                               CFMetaData cfm,
                                                                               List<ColumnDefinition> defs)
                                                                               throws InvalidRequestException
    {
        return new SelectorFactories(selectables, cfm, defs);
    }

    private SelectorFactories(List<Selectable> selectables,
                              CFMetaData cfm,
                              List<ColumnDefinition> defs)
                              throws InvalidRequestException
    {
        factories = new ArrayList<>(selectables.size());

        for (Selectable selectable : selectables)
        {
            Factory factory = selectable.newSelectorFactory(cfm, defs);
            containsWritetimeFactory |= factory.isWritetimeSelectorFactory();
            containsTTLFactory |= factory.isTTLSelectorFactory();
            if (factory.isAggregateSelectorFactory())
                ++numberOfAggregateFactories;
            factories.add(factory);
        }
    }

    public boolean usesFunction(String ksName, String functionName)
    {
        for (Factory factory : factories)
            if (factory != null && factory.usesFunction(ksName, functionName))
                return true;
        return false;
    }

    /**
     * Adds a new <code>Selector.Factory</code> for a column that is needed only for ORDER BY purposes.
     * @param def the column that is needed for ordering
     * @param index the index of the column definition in the Selection's list of columns
     */
    public void addSelectorForOrdering(ColumnDefinition def, int index)
    {
        factories.add(SimpleSelector.newFactory(def.name.toString(), index, def.type));
    }

    /**
     * Checks if this <code>SelectorFactories</code> contains only factories for aggregates.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains only factories for aggregates,
     * <code>false</code> otherwise.
     */
    public boolean containsOnlyAggregateFunctions()
    {
        int size = factories.size();
        return  size != 0 && numberOfAggregateFactories == size;
    }

    /**
     * Whether the selector built by this factory does aggregation or not (either directly or in a sub-selector).
     *
     * @return <code>true</code> if the selector built by this factor does aggregation, <code>false</code> otherwise.
     */
    public boolean doesAggregation()
    {
        return numberOfAggregateFactories > 0;
    }

    /**
     * Checks if this <code>SelectorFactories</code> contains at least one factory for writetime selectors.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains at least one factory for writetime
     * selectors, <code>false</code> otherwise.
     */
    public boolean containsWritetimeSelectorFactory()
    {
        return containsWritetimeFactory;
    }

    /**
     * Checks if this <code>SelectorFactories</code> contains at least one factory for TTL selectors.
     *
     * @return <code>true</code> if this <code>SelectorFactories</code> contains at least one factory for TTL
     * selectors, <code>false</code> otherwise.
     */
    public boolean containsTTLSelectorFactory()
    {
        return containsTTLFactory;
    }

    /**
     * Creates a list of new <code>Selector</code> instances.
     * @return a list of new <code>Selector</code> instances.
     */
    public List<Selector> newInstances() throws InvalidRequestException
    {
        List<Selector> selectors = new ArrayList<>(factories.size());
        for (Selector.Factory factory : factories)
        {
            selectors.add(factory.newInstance());
        }
        return selectors;
    }

    public Iterator<Factory> iterator()
    {
        return factories.iterator();
    }

    /**
     * Returns the names of the columns corresponding to the output values of the selector instances created by
     * these factories.
     *
     * @return a list of column names
     */
    public List<String> getColumnNames()
    {
        return Lists.transform(factories, new Function<Selector.Factory, String>()
        {
            public String apply(Selector.Factory factory)
            {
                return factory.getColumnName();
            }
        });
    }
}
