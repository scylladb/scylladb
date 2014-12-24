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

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.index.SecondaryIndex;

final class TermSlice
{
    /**
     * The slice boundaries.
     */
    private final Term[] bounds;

    /**
     * Specifies if a slice boundary is inclusive or not.
     */
    private final boolean[] boundInclusive;

    /**
     * Creates a new <code>TermSlice</code> with the specified boundaries.
     *
     * @param start the lower boundary
     * @param includeStart <code>true</code> if the lower boundary is inclusive
     * @param end the upper boundary
     * @param includeEnd <code>true</code> if the upper boundary is inclusive
     */
    private TermSlice(Term start, boolean includeStart, Term end, boolean includeEnd)
    {
        bounds = new Term[]{start, end};
        boundInclusive = new boolean[]{includeStart, includeEnd};
    }

    /**
     * Creates a new <code>TermSlice</code> with the specified boundary.
     *
     * @param bound the boundary type
     * @param include <code>true</code> if the boundary is inclusive
     * @param term the value
     * @return a new <code>TermSlice</code> instance
     */
    public static TermSlice newInstance(Bound bound, boolean include, Term term)
    {
        return  bound.isStart() ? new TermSlice(term, include, null, false) 
                                : new TermSlice(null, false, term, include);
    }

    /**
     * Returns the boundary value.
     *
     * @param bound the boundary type
     * @return the boundary value
     */
    public Term bound(Bound bound)
    {
        return bounds[bound.idx];
    }

    /**
     * Checks if this slice has a boundary for the specified type.
     *
     * @param b the boundary type
     * @return <code>true</code> if this slice has a boundary for the specified type, <code>false</code> otherwise.
     */
    public boolean hasBound(Bound b)
    {
        return bounds[b.idx] != null;
    }

    /**
     * Checks if this slice boundary is inclusive for the specified type.
     *
     * @param b the boundary type
     * @return <code>true</code> if this slice boundary is inclusive for the specified type,
     * <code>false</code> otherwise.
     */
    public boolean isInclusive(Bound b)
    {
        return bounds[b.idx] == null || boundInclusive[b.idx];
    }

    /**
     * Merges this slice with the specified one.
     *
     * @param otherSlice the slice to merge to
     * @return the new slice resulting from the merge
     */
    public TermSlice merge(TermSlice otherSlice)
    {
        if (hasBound(Bound.START))
        {
            assert !otherSlice.hasBound(Bound.START);

            return new TermSlice(bound(Bound.START), 
                                  isInclusive(Bound.START),
                                  otherSlice.bound(Bound.END),
                                  otherSlice.isInclusive(Bound.END));
        }
        assert !otherSlice.hasBound(Bound.END);

        return new TermSlice(otherSlice.bound(Bound.START), 
                              otherSlice.isInclusive(Bound.START),
                              bound(Bound.END),
                              isInclusive(Bound.END));
    }

    @Override
    public String toString()
    {
        return String.format("(%s %s, %s %s)", boundInclusive[0] ? ">=" : ">",
                             bounds[0],
                             boundInclusive[1] ? "<=" : "<",
                             bounds[1]);
    }

    /**
     * Returns the index operator corresponding to the specified boundary.
     *
     * @param b the boundary type
     * @return the index operator corresponding to the specified boundary
     */
    public Operator getIndexOperator(Bound b)
    {
        if (b.isStart())
            return boundInclusive[b.idx] ? Operator.GTE : Operator.GT;

        return boundInclusive[b.idx] ? Operator.LTE : Operator.LT;
    }

    /**
     * Check if this <code>TermSlice</code> is supported by the specified index.
     *
     * @param index the Secondary index
     * @return <code>true</code> this type of <code>TermSlice</code> is supported by the specified index,
     * <code>false</code> otherwise.
     */
    public boolean isSupportedBy(SecondaryIndex index)
    {
        boolean supported = false;

        if (hasBound(Bound.START))
            supported |= isInclusive(Bound.START) ? index.supportsOperator(Operator.GTE)
                    : index.supportsOperator(Operator.GT);
        if (hasBound(Bound.END))
            supported |= isInclusive(Bound.END) ? index.supportsOperator(Operator.LTE)
                    : index.supportsOperator(Operator.LT);

        return supported;
    }
}
