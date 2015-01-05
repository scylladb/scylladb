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
package org.apache.cassandra.db.composites;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.db.composites.Composite.EOC;

import static java.util.Collections.singletonList;

/**
 * Builder that allow to build multiple composites at the same time.
 */
public final class CompositesBuilder
{
    /**
     * The builder used to build the <code>Composite</code>s.
     */
    private final CBuilder builder;

    /**
     * The comparator used to sort the returned <code>Composite</code>s.
     */
    private final Comparator<Composite> comparator;

    /**
     * The elements of the composites
     */
    private final List<List<ByteBuffer>> elementsList = new ArrayList<>();

    /**
     * The number of elements that still can be added.
     */
    private int remaining;

    /**
     * <code>true</code> if the composites have been build, <code>false</code> otherwise.
     */
    private boolean built;

    /**
     * <code>true</code> if the composites contains some <code>null</code> elements.
     */
    private boolean containsNull;

    public CompositesBuilder(CBuilder builder, Comparator<Composite> comparator)
    {
        this.builder = builder;
        this.comparator = comparator;
        this.remaining = builder.remainingCount();
    }

    /**
     * Adds the specified element to all the composites.
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add D will result in the composites:
     * A-B-D and A-C-D.
     * </p>
     *
     * @param value the value of the next element
     * @return this <code>CompositeBuilder</code>
     */
    public CompositesBuilder addElementToAll(ByteBuffer value)
    {
        checkUpdateable();

        if (isEmpty())
            elementsList.add(new ArrayList<ByteBuffer>());

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            if (value == null)
                containsNull = true;

            elementsList.get(i).add(value);
        }
        remaining--;
        return this;
    }

    /**
     * Adds individually each of the specified elements to the end of all of the existing composites.
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add D and E will result in the 4
     * composites: A-B-D, A-B-E, A-C-D and A-C-E.
     * </p>
     *
     * @param values the elements to add
     * @return this <code>CompositeBuilder</code>
     */
    public CompositesBuilder addEachElementToAll(List<ByteBuffer> values)
    {
        checkUpdateable();

        if (isEmpty())
            elementsList.add(new ArrayList<ByteBuffer>());

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            List<ByteBuffer> oldComposite = elementsList.remove(0);

            for (int j = 0, n = values.size(); j < n; j++)
            {
                List<ByteBuffer> newComposite = new ArrayList<>(oldComposite);
                elementsList.add(newComposite);

                ByteBuffer value = values.get(j);

                if (value == null)
                    containsNull = true;

                newComposite.add(values.get(j));
            }
        }

        remaining--;
        return this;
    }


    /**
     * Adds individually each of the specified list of elements to the end of all of the existing composites.
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add [[D, E], [F, G]] will result in the 4
     * composites: A-B-D-E, A-B-F-G, A-C-D-E and A-C-F-G.
     * </p>
     *
     * @param values the elements to add
     * @return this <code>CompositeBuilder</code>
     */
    public CompositesBuilder addAllElementsToAll(List<List<ByteBuffer>> values)
    {
        assert !values.isEmpty();
        checkUpdateable();

        if (isEmpty())
            elementsList.add(new ArrayList<ByteBuffer>());

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            List<ByteBuffer> oldComposite = elementsList.remove(0);

            for (int j = 0, n = values.size(); j < n; j++)
            {
                List<ByteBuffer> newComposite = new ArrayList<>(oldComposite);
                elementsList.add(newComposite);

                List<ByteBuffer> value = values.get(j);

                if (value.contains(null))
                    containsNull = true;

                newComposite.addAll(value);
            }
        }

        remaining -= values.get(0).size();
        return this;
    }

    /**
     * Returns the number of elements that can be added to the composites.
     *
     * @return the number of elements that can be added to the composites.
     */
    public int remainingCount()
    {
        return remaining;
    }

    /**
     * Checks if some elements can still be added to the composites.
     *
     * @return <code>true</code> if it is possible to add more elements to the composites, <code>false</code> otherwise.
     */
    public boolean hasRemaining()
    {
        return remaining > 0;
    }

    /**
     * Checks if this builder is empty.
     *
     * @return <code>true</code> if this builder is empty, <code>false</code> otherwise.
     */
    public boolean isEmpty()
    {
        return elementsList.isEmpty();
    }

    /**
     * Checks if the composites contains null elements.
     *
     * @return <code>true</code> if the composites contains <code>null</code> elements, <code>false</code> otherwise.
     */
    public boolean containsNull()
    {
        return containsNull;
    }

    /**
     * Builds the <code>Composites</code>.
     *
     * @return the composites
     */
    public List<Composite> build()
    {
        return buildWithEOC(EOC.NONE);
    }

    /**
     * Builds the <code>Composites</code> with the specified EOC.
     *
     * @return the composites
     */
    public List<Composite> buildWithEOC(EOC eoc)
    {
        built = true;

        if (elementsList.isEmpty())
            return singletonList(builder.build().withEOC(eoc));

        // Use a Set to sort if needed and eliminate duplicates
        Set<Composite> set = newSet();

        for (int i = 0, m = elementsList.size(); i < m; i++)
        {
            List<ByteBuffer> elements = elementsList.get(i);
            set.add(builder.buildWith(elements).withEOC(eoc));
        }

        return new ArrayList<>(set);
    }

    /**
     * Returns a new <code>Set</code> instance that will be used to eliminate duplicates and sort the results.
     *
     * @return a new <code>Set</code> instance.
     */
    private Set<Composite> newSet()
    {
        return comparator == null ? new LinkedHashSet<Composite>() : new TreeSet<Composite>(comparator);
    }

    private void checkUpdateable()
    {
        if (!hasRemaining() || built)
            throw new IllegalStateException("this CompositesBuilder cannot be updated anymore");
    }
}
