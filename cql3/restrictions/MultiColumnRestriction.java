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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class MultiColumnRestriction extends AbstractPrimaryKeyRestrictions
{
    protected final CType ctype;

    /**
     * The columns to which the restriction apply.
     */
    protected final List<ColumnDefinition> columnDefs;

    public MultiColumnRestriction(CType ctype, List<ColumnDefinition> columnDefs)
    {
        this.ctype = ctype;
        this.columnDefs = columnDefs;
    }

    @Override
    public boolean isMultiColumn()
    {
        return true;
    }

    @Override
    public Collection<ColumnDefinition> getColumnDefs()
    {
        return columnDefs;
    }

    @Override
    public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
    {
        return Composites.toByteBuffers(valuesAsComposites(options));
    }

    @Override
    public final PrimaryKeyRestrictions mergeWith(Restriction otherRestriction) throws InvalidRequestException
    {
            checkTrue(otherRestriction.isMultiColumn(),
                      "Mixing single column relations and multi column relations on clustering columns is not allowed");
            return doMergeWith((PrimaryKeyRestrictions) otherRestriction);
    }

    protected abstract PrimaryKeyRestrictions doMergeWith(PrimaryKeyRestrictions otherRestriction) throws InvalidRequestException;

    /**
     * Returns the names of the columns that are specified within this <code>Restrictions</code> and the other one
     * as a comma separated <code>String</code>.
     *
     * @param otherRestrictions the other restrictions
     * @return the names of the columns that are specified within this <code>Restrictions</code> and the other one
     * as a comma separated <code>String</code>.
     */
    protected final String getColumnsInCommons(Restrictions otherRestrictions)
    {
        Set<ColumnDefinition> commons = new HashSet<>(getColumnDefs());
        commons.retainAll(otherRestrictions.getColumnDefs());
        StringBuilder builder = new StringBuilder();
        for (ColumnDefinition columnDefinition : commons)
        {
            if (builder.length() != 0)
                builder.append(" ,");
            builder.append(columnDefinition.name);
        }
        return builder.toString();
    }

    @Override
    public final boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        for (ColumnDefinition columnDef : columnDefs)
        {
            SecondaryIndex index = indexManager.getIndexForColumn(columnDef.name.bytes);
            if (index != null && isSupportedBy(index))
                return true;
        }
        return false;
    }

    /**
     * Check if this type of restriction is supported for the specified column by the specified index.
     * @param index the Secondary index
     *
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(SecondaryIndex index);

    public static class EQ  extends MultiColumnRestriction
    {
        protected final Term value;

        public EQ(CType ctype, List<ColumnDefinition> columnDefs, Term value)
        {
            super(ctype, columnDefs);
            this.value = value;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return usesFunction(value, ksName, functionName);
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)", value);
        }

        @Override
        public PrimaryKeyRestrictions doMergeWith(PrimaryKeyRestrictions otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal",
                                 getColumnsInCommons(otherRestriction));
        }

        @Override
        public List<Composite> valuesAsComposites(QueryOptions options) throws InvalidRequestException
        {
            return Collections.singletonList(compositeValue(options));
        }

        @Override
        public List<Composite> boundsAsComposites(Bound bound, QueryOptions options) throws InvalidRequestException
        {
            Composite prefix = compositeValue(options);
            return Collections.singletonList(ctype.size() > prefix.size() && bound.isEnd()
                                             ? prefix.end()
                                             : prefix);
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return index.supportsOperator(Operator.EQ);
        }

        private Composite compositeValue(QueryOptions options) throws InvalidRequestException
        {
            CBuilder builder = ctype.builder();
            Tuples.Value t = ((Tuples.Value) value.bind(options));
            List<ByteBuffer> values = t.getElements();
            for (int i = 0; i < values.size(); i++)
            {
                ByteBuffer component = checkNotNull(values.get(i),
                                                    "Invalid null value in condition for column %s",
                                                    columnDefs.get(i).name);
                builder.add(component);
            }

            return builder.build();
        }

        @Override
        public final void addIndexExpressionTo(List<IndexExpression> expressions,
                                               QueryOptions options) throws InvalidRequestException
        {
            Tuples.Value t = ((Tuples.Value) value.bind(options));
            List<ByteBuffer> values = t.getElements();
            for (int i = 0; i < values.size(); i++)
            {
                ColumnDefinition columnDef = columnDefs.get(i);
                ByteBuffer component = validateIndexedValue(columnDef, values.get(i));
                expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, component));
            }
        }
    }

    public abstract static class IN extends MultiColumnRestriction
    {
        @Override
        public List<Composite> valuesAsComposites(QueryOptions options) throws InvalidRequestException
        {
            CBuilder builder = ctype.builder();
            List<List<ByteBuffer>> splitInValues = splitValues(options);
            // The IN query might not have listed the values in comparator order, so we need to re-sort
            // the bounds lists to make sure the slices works correctly (also, to avoid duplicates).
            TreeSet<Composite> inValues = new TreeSet<>(ctype);
            for (List<ByteBuffer> components : splitInValues)
            {
                for (int i = 0; i < components.size(); i++)
                    checkNotNull(components.get(i), "Invalid null value in condition for column " + columnDefs.get(i).name);

                inValues.add(builder.buildWith(components));
            }
            return new ArrayList<>(inValues);
        }

        @Override
        public List<Composite> boundsAsComposites(Bound bound, QueryOptions options) throws InvalidRequestException
        {
            CBuilder builder = ctype.builder();
            List<List<ByteBuffer>> splitInValues = splitValues(options);
            // The IN query might not have listed the values in comparator order, so we need to re-sort
            // the bounds lists to make sure the slices works correctly (also, to avoid duplicates).
            TreeSet<Composite> inValues = new TreeSet<>(ctype);
            for (List<ByteBuffer> components : splitInValues)
            {
                for (int i = 0; i < components.size(); i++)
                    checkNotNull(components.get(i), "Invalid null value in condition for column %s", columnDefs.get(i).name);

                Composite prefix = builder.buildWith(components);
                inValues.add(bound.isEnd() && builder.remainingCount() - components.size() > 0
                             ? prefix.end()
                             : prefix);
            }
            return new ArrayList<>(inValues);
        }

        @Override
        public void addIndexExpressionTo(List<IndexExpression> expressions,
                                         QueryOptions options) throws InvalidRequestException
        {
            List<List<ByteBuffer>> splitInValues = splitValues(options);
            checkTrue(splitInValues.size() == 1, "IN restrictions are not supported on indexed columns");

            List<ByteBuffer> values = splitInValues.get(0);
            checkTrue(values.size() == 1, "IN restrictions are not supported on indexed columns");

            ColumnDefinition columnDef = columnDefs.get(0);
            ByteBuffer component = validateIndexedValue(columnDef, values.get(0));
            expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, component));
        }

        public IN(CType ctype, List<ColumnDefinition> columnDefs)
        {
            super(ctype, columnDefs);
        }

        @Override
        public boolean isIN()
        {
            return true;
        }

        @Override
        public PrimaryKeyRestrictions doMergeWith(PrimaryKeyRestrictions otherRestrictions) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes a IN",
                                 getColumnsInCommons(otherRestrictions));
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return index.supportsOperator(Operator.IN);
        }

        protected abstract List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException;
    }

    /**
     * An IN restriction that has a set of terms for in values.
     * For example: "SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))" or "WHERE (a, b, c) IN (?, ?)"
     */
    public static class InWithValues extends MultiColumnRestriction.IN
    {
        protected final List<Term> values;

        public InWithValues(CType ctype, List<ColumnDefinition> columnDefs, List<Term> values)
        {
            super(ctype, columnDefs);
            this.values = values;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return usesFunction(values, ksName, functionName);
        }

        @Override
        public String toString()
        {
            return String.format("IN(%s)", values);
        }

        @Override
        protected List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException
        {
            List<List<ByteBuffer>> buffers = new ArrayList<>(values.size());
            for (Term value : values)
            {
                Term.MultiItemTerminal term = (Term.MultiItemTerminal) value.bind(options);
                buffers.add(term.getElements());
            }
            return buffers;
        }
    }

    /**
     * An IN restriction that uses a single marker for a set of IN values that are tuples.
     * For example: "SELECT ... WHERE (a, b, c) IN ?"
     */
    public static class InWithMarker extends MultiColumnRestriction.IN
    {
        protected final AbstractMarker marker;

        public InWithMarker(CType ctype, List<ColumnDefinition> columnDefs, AbstractMarker marker)
        {
            super(ctype, columnDefs);
            this.marker = marker;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "IN ?";
        }

        @Override
        protected List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException
        {
            Tuples.InMarker inMarker = (Tuples.InMarker) marker;
            Tuples.InValue inValue = inMarker.bind(options);
            checkNotNull(inValue, "Invalid null value for IN restriction");
            return inValue.getSplitValues();
        }
    }

    public static class Slice extends MultiColumnRestriction
    {
        private final TermSlice slice;

        public Slice(CType ctype, List<ColumnDefinition> columnDefs, Bound bound, boolean inclusive, Term term)
        {
            this(ctype, columnDefs, TermSlice.newInstance(bound, inclusive, term));
        }

        private Slice(CType ctype, List<ColumnDefinition> columnDefs, TermSlice slice)
        {
            super(ctype, columnDefs);
            this.slice = slice;
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public List<Composite> valuesAsComposites(QueryOptions options) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
        {
            return Composites.toByteBuffers(boundsAsComposites(b, options));
        }

        @Override
        public List<Composite> boundsAsComposites(Bound bound, QueryOptions options) throws InvalidRequestException
        {
            CBuilder builder = ctype.builder();
            Iterator<ColumnDefinition> iter = columnDefs.iterator();
            ColumnDefinition firstName = iter.next();
            // A hack to preserve pre-6875 behavior for tuple-notation slices where the comparator mixes ASCENDING
            // and DESCENDING orders.  This stores the bound for the first component; we will re-use it for all following
            // components, even if they don't match the first component's reversal/non-reversal.  Note that this does *not*
            // guarantee correct query results, it just preserves the previous behavior.
            Bound firstComponentBound = !firstName.isReversedType() ? bound : bound.reverse();

            if (!hasBound(firstComponentBound))
            {
                Composite prefix = builder.build();
                return Collections.singletonList(builder.remainingCount() > 0 && bound.isEnd()
                        ? prefix.end()
                        : prefix);
            }

            List<ByteBuffer> vals = componentBounds(firstComponentBound, options);

            ByteBuffer v = checkNotNull(vals.get(firstName.position()), "Invalid null value in condition for column %s", firstName.name);
            builder.add(v);

            while (iter.hasNext())
            {
                ColumnDefinition def = iter.next();
                if (def.position() >= vals.size())
                    break;

                v = checkNotNull(vals.get(def.position()), "Invalid null value in condition for column %s", def.name);
                builder.add(v);
            }
            Composite.EOC eoc =  eocFor(this, bound, firstComponentBound);
            return Collections.singletonList(builder.build().withEOC(eoc));
        }

        @Override
        public void addIndexExpressionTo(List<IndexExpression> expressions,
                                         QueryOptions options) throws InvalidRequestException
        {
            throw invalidRequest("Slice restrictions are not supported on indexed columns which are part of a multi column relation");
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return slice.isSupportedBy(index);
        }

        private static Composite.EOC eocFor(Restriction r, Bound eocBound, Bound inclusiveBound)
        {
            if (eocBound.isStart())
                return r.isInclusive(inclusiveBound) ? Composite.EOC.NONE : Composite.EOC.END;

            return r.isInclusive(inclusiveBound) ? Composite.EOC.END : Composite.EOC.START;
        }

        @Override
        public boolean hasBound(Bound b)
        {
            return slice.hasBound(b);
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return (slice.hasBound(Bound.START) && usesFunction(slice.bound(Bound.START), ksName, functionName))
                    || (slice.hasBound(Bound.END) && usesFunction(slice.bound(Bound.END), ksName, functionName));
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            return slice.isInclusive(b);
        }

        @Override
        public PrimaryKeyRestrictions doMergeWith(PrimaryKeyRestrictions otherRestriction) throws InvalidRequestException
        {
            checkTrue(otherRestriction.isSlice(),
                      "Column \"%s\" cannot be restricted by both an equality and an inequality relation",
                      getColumnsInCommons(otherRestriction));

            Slice otherSlice = (Slice) otherRestriction;

            checkFalse(hasBound(Bound.START) && otherSlice.hasBound(Bound.START),
                       "More than one restriction was found for the start bound on %s",
                       getColumnsInCommons(otherRestriction));
            checkFalse(hasBound(Bound.END) && otherSlice.hasBound(Bound.END),
                       "More than one restriction was found for the end bound on %s",
                       getColumnsInCommons(otherRestriction));

            List<ColumnDefinition> newColumnDefs = size() >= otherSlice.size() ?  columnDefs : otherSlice.columnDefs;
            return new Slice(ctype,  newColumnDefs, slice.merge(otherSlice.slice));
        }

        @Override
        public String toString()
        {
            return "SLICE" + slice;
        }

        /**
         * Similar to bounds(), but returns one ByteBuffer per-component in the bound instead of a single
         * ByteBuffer to represent the entire bound.
         * @param b the bound type
         * @param options the query options
         * @return one ByteBuffer per-component in the bound
         * @throws InvalidRequestException if the components cannot be retrieved
         */
        private List<ByteBuffer> componentBounds(Bound b, QueryOptions options) throws InvalidRequestException
        {
            Tuples.Value value = (Tuples.Value) slice.bound(b).bind(options);
            return value.getElements();
        }
    }
}
