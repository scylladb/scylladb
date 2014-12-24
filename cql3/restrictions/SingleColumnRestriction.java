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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class SingleColumnRestriction extends AbstractRestriction
{
    /**
     * The definition of the column to which apply the restriction.
     */
    protected final ColumnDefinition columnDef;

    public SingleColumnRestriction(ColumnDefinition columnDef)
    {
        this.columnDef = columnDef;
    }

    /**
     * Returns the definition of the column to which is associated this restriction.
     * @return the definition of the column to which is associated this restriction
     */
    public ColumnDefinition getColumnDef()
    {
        return columnDef;
    }

    @Override
    public void addIndexExpressionTo(List<IndexExpression> expressions,
                                     QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> values = values(options);
        checkTrue(values.size() == 1, "IN restrictions are not supported on indexed columns");

        ByteBuffer value = validateIndexedValue(columnDef, values.get(0));
        expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, value));
    }

    @Override
    public boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        SecondaryIndex index = indexManager.getIndexForColumn(columnDef.name.bytes);
        return index != null && isSupportedBy(index);
    }

    /**
     * Check if this type of restriction is supported by the specified index.
     *
     * @param index the Secondary index
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(SecondaryIndex index);

    public static final class EQ extends SingleColumnRestriction
    {
        private final Term value;

        public EQ(ColumnDefinition columnDef, Term value)
        {
            super(columnDef);
            this.value = value;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return usesFunction(value, ksName, functionName);
        }

        public boolean isEQ()
        {
            return true;
        }

        @Override
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            return Collections.singletonList(value.bindAndGet(options));
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)", value);
        }

        @Override
        public Restriction mergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal", columnDef.name);
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return index.supportsOperator(Operator.EQ);
        }
    }

    public static abstract class IN extends SingleColumnRestriction
    {
        public IN(ColumnDefinition columnDef)
        {
            super(columnDef);
        }

        @Override
        public final boolean isIN()
        {
            return true;
        }

        @Override
        public final Restriction mergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes a IN", columnDef.name);
        }

        @Override
        protected final boolean isSupportedBy(SecondaryIndex index)
        {
            return index.supportsOperator(Operator.IN);
        }
    }

    public static class InWithValues extends IN
    {
        protected final List<Term> values;

        public InWithValues(ColumnDefinition columnDef, List<Term> values)
        {
            super(columnDef);
            this.values = values;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return usesFunction(values, ksName, functionName);
        }

        @Override
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(values.size());
            for (Term value : values)
                buffers.add(value.bindAndGet(options));
            return buffers;
        }

        @Override
        public String toString()
        {
            return String.format("IN(%s)", values);
        }
    }

    public static class InWithMarker extends IN
    {
        protected final AbstractMarker marker;

        public InWithMarker(ColumnDefinition columnDef, AbstractMarker marker)
        {
            super(columnDef);
            this.marker = marker;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return false;
        }

        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            Term.MultiItemTerminal lval = (Term.MultiItemTerminal) marker.bind(options);
            if (lval == null)
                throw new InvalidRequestException("Invalid null value for IN restriction");
            return lval.getElements();
        }

        @Override
        public String toString()
        {
            return "IN ?";
        }
    }

    public static class Slice extends SingleColumnRestriction
    {
        private final TermSlice slice;

        public Slice(ColumnDefinition columnDef, Bound bound, boolean inclusive, Term term)
        {
            super(columnDef);
            slice = TermSlice.newInstance(bound, inclusive, term);
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return (slice.hasBound(Bound.START) && usesFunction(slice.bound(Bound.START), ksName, functionName))
                    || (slice.hasBound(Bound.END) && usesFunction(slice.bound(Bound.END), ksName, functionName));
        }

        public boolean isSlice()
        {
            return true;
        }

        @Override
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBound(Bound b)
        {
            return slice.hasBound(b);
        }

        @Override
        public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
        {
            return Collections.singletonList(slice.bound(b).bindAndGet(options));
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            return slice.isInclusive(b);
        }

        @Override
        public Restriction mergeWith(Restriction otherRestriction)
        throws InvalidRequestException
        {
            checkTrue(otherRestriction.isSlice(),
                      "Column \"%s\" cannot be restricted by both an equality and an inequality relation",
                      columnDef.name);

            SingleColumnRestriction.Slice otherSlice = (SingleColumnRestriction.Slice) otherRestriction;

            checkFalse(hasBound(Bound.START) && otherSlice.hasBound(Bound.START),
                       "More than one restriction was found for the start bound on %s", columnDef.name);

            checkFalse(hasBound(Bound.END) && otherSlice.hasBound(Bound.END),
                       "More than one restriction was found for the end bound on %s", columnDef.name);

            return new Slice(columnDef,  slice.merge(otherSlice.slice));
        }

        @Override
        public void addIndexExpressionTo(List<IndexExpression> expressions,
                                         QueryOptions options) throws InvalidRequestException
        {
            for (Bound b : Bound.values())
            {
                if (hasBound(b))
                {
                    ByteBuffer value = validateIndexedValue(columnDef, slice.bound(b).bindAndGet(options));
                    Operator op = slice.getIndexOperator(b);
                    // If the underlying comparator for name is reversed, we need to reverse the IndexOperator: user operation
                    // always refer to the "forward" sorting even if the clustering order is reversed, but the 2ndary code does
                    // use the underlying comparator as is.
                    op = columnDef.isReversedType() ? op.reverse() : op;
                    expressions.add(new IndexExpression(columnDef.name.bytes, op, value));
                }
            }
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return slice.isSupportedBy(index);
        }

        @Override
        public String toString()
        {
            return String.format("SLICE%s", slice);
        }

        private Slice(ColumnDefinition columnDef, TermSlice slice)
        {
            super(columnDef);
            this.slice = slice;
        }
    }

    // This holds CONTAINS, CONTAINS_KEY, and map[key] = value restrictions because we might want to have any combination of them.
    public static final class Contains extends SingleColumnRestriction
    {
        private List<Term> values = new ArrayList<>(); // for CONTAINS
        private List<Term> keys = new ArrayList<>(); // for CONTAINS_KEY
        private List<Term> entryKeys = new ArrayList<>(); // for map[key] = value
        private List<Term> entryValues = new ArrayList<>(); // for map[key] = value

        public Contains(ColumnDefinition columnDef, Term t, boolean isKey)
        {
            super(columnDef);
            if (isKey)
                keys.add(t);
            else
                values.add(t);
        }

        public Contains(ColumnDefinition columnDef, Term mapKey, Term mapValue)
        {
            super(columnDef);
            entryKeys.add(mapKey);
            entryValues.add(mapValue);
        }

        @Override
        public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
        {
            return bindAndGet(values, options);
        }

        @Override
        public boolean isContains()
        {
            return true;
        }

        @Override
        public Restriction mergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            checkTrue(otherRestriction.isContains(),
                      "Collection column %s can only be restricted by CONTAINS, CONTAINS KEY, or map-entry equality",
                      getColumnDef().name);

            SingleColumnRestriction.Contains newContains = new Contains(getColumnDef());

            copyKeysAndValues(this, newContains);
            copyKeysAndValues((Contains) otherRestriction, newContains);

            return newContains;
        }

        @Override
        public void addIndexExpressionTo(List<IndexExpression> expressions,
                                         QueryOptions options)
                                         throws InvalidRequestException
        {
            addExpressionsFor(expressions, values(options), Operator.CONTAINS);
            addExpressionsFor(expressions, keys(options), Operator.CONTAINS_KEY);
            addExpressionsFor(expressions, entries(options), Operator.EQ);
        }

        private void addExpressionsFor(List<IndexExpression> target, List<ByteBuffer> values,
                                       Operator op) throws InvalidRequestException
        {
            for (ByteBuffer value : values)
            {
                validateIndexedValue(columnDef, value);
                target.add(new IndexExpression(columnDef.name.bytes, op, value));
            }
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            boolean supported = false;

            if (numberOfValues() > 0)
                supported |= index.supportsOperator(Operator.CONTAINS);

            if (numberOfKeys() > 0)
                supported |= index.supportsOperator(Operator.CONTAINS_KEY);

            if (numberOfEntries() > 0)
                supported |= index.supportsOperator(Operator.EQ);

            return supported;
        }

        public int numberOfValues()
        {
            return values.size();
        }

        public int numberOfKeys()
        {
            return keys.size();
        }

        public int numberOfEntries()
        {
            return entryKeys.size();
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return usesFunction(values, ksName, functionName) || usesFunction(keys, ksName, functionName) ||
                   usesFunction(entryKeys, ksName, functionName) || usesFunction(entryValues, ksName, functionName);
        }

        @Override
        public String toString()
        {
            return String.format("CONTAINS(values=%s, keys=%s, entryKeys=%s, entryValues=%s)", values, keys, entryKeys, entryValues);
        }

        @Override
        public boolean hasBound(Bound b)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            throw new UnsupportedOperationException();
        }

        private List<ByteBuffer> keys(QueryOptions options) throws InvalidRequestException
        {
            return bindAndGet(keys, options);
        }

        private List<ByteBuffer> entries(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> entryBuffers = new ArrayList<>(entryKeys.size());
            List<ByteBuffer> keyBuffers = bindAndGet(entryKeys, options);
            List<ByteBuffer> valueBuffers = bindAndGet(entryValues, options);
            for (int i = 0; i < entryKeys.size(); i++)
            {
                if (valueBuffers.get(i) == null)
                    throw new InvalidRequestException("Unsupported null value for map-entry equality");
                entryBuffers.add(CompositeType.build(keyBuffers.get(i), valueBuffers.get(i)));
            }
            return entryBuffers;
        }

        /**
         * Binds the query options to the specified terms and returns the resulting values.
         *
         * @param terms the terms
         * @param options the query options
         * @return the value resulting from binding the query options to the specified terms
         * @throws InvalidRequestException if a problem occurs while binding the query options
         */
        private static List<ByteBuffer> bindAndGet(List<Term> terms, QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(terms.size());
            for (Term value : terms)
                buffers.add(value.bindAndGet(options));
            return buffers;
        }

        /**
         * Copies the keys and value from the first <code>Contains</code> to the second one.
         *
         * @param from the <code>Contains</code> to copy from
         * @param to the <code>Contains</code> to copy to
         */
        private static void copyKeysAndValues(Contains from, Contains to)
        {
            to.values.addAll(from.values);
            to.keys.addAll(from.keys);
            to.entryKeys.addAll(from.entryKeys);
            to.entryValues.addAll(from.entryValues);
        }

        private Contains(ColumnDefinition columnDef)
        {
            super(columnDef);
        }
    }
}
