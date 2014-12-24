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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static helper methods and classes for lists.
 */
public abstract class Lists
{
    private static final Logger logger = LoggerFactory.getLogger(Lists.class);

    private Lists() {}

    public static ColumnSpecification indexSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("idx(" + column.name + ")", true), Int32Type.instance);
    }

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((ListType)column.type).getElementsType());
    }

    public static class Literal implements Term.Raw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            ColumnSpecification valueSpec = Lists.valueSpecOf(receiver);
            List<Term> values = new ArrayList<Term>(elements.size());
            boolean allTerminal = true;
            for (Term.Raw rt : elements)
            {
                Term t = rt.prepare(keyspace, valueSpec);

                if (t.containsBindMarker())
                    throw new InvalidRequestException(String.format("Invalid list literal for %s: bind variables are not supported inside collection literals", receiver.name));

                if (t instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(t);
            }
            DelayedValue value = new DelayedValue(values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof ListType))
                throw new InvalidRequestException(String.format("Invalid list literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));

            ColumnSpecification valueSpec = Lists.valueSpecOf(receiver);
            for (Term.Raw rt : elements)
            {
                if (!rt.testAssignment(keyspace, valueSpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid list literal for %s: value %s is not of type %s", receiver.name, rt, valueSpec.type.asCQL3Type()));
            }
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            if (!(receiver.type instanceof ListType))
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

            // If there is no elements, we can't say it's an exact match (an empty list if fundamentally polymorphic).
            if (elements.isEmpty())
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

            ColumnSpecification valueSpec = Lists.valueSpecOf(receiver);
            return AssignmentTestable.TestResult.testAll(keyspace, valueSpec, elements);
        }

        @Override
        public String toString()
        {
            return elements.toString();
        }
    }

    public static class Value extends Term.MultiItemTerminal implements Term.CollectionTerminal
    {
        public final List<ByteBuffer> elements;

        public Value(List<ByteBuffer> elements)
        {
            this.elements = elements;
        }

        public static Value fromSerialized(ByteBuffer value, ListType type, int version) throws InvalidRequestException
        {
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                List<?> l = (List<?>)type.getSerializer().deserializeForNativeProtocol(value, version);
                List<ByteBuffer> elements = new ArrayList<ByteBuffer>(l.size());
                for (Object element : l)
                    // elements can be null in lists that represent a set of IN values
                    elements.add(element == null ? null : type.getElementsType().decompose(element));
                return new Value(elements);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public ByteBuffer get(QueryOptions options)
        {
            return getWithProtocolVersion(options.getProtocolVersion());
        }

        public ByteBuffer getWithProtocolVersion(int protocolVersion)
        {
            return CollectionSerializer.pack(elements, elements.size(), protocolVersion);
        }

        public boolean equals(ListType lt, Value v)
        {
            if (elements.size() != v.elements.size())
                return false;

            for (int i = 0; i < elements.size(); i++)
                if (lt.getElementsType().compare(elements.get(i), v.elements.get(i)) != 0)
                    return false;

            return true;
        }

        public List<ByteBuffer> getElements()
        {
            return elements;
        }
    }

    /**
     * Basically similar to a Value, but with some non-pure function (that need
     * to be evaluated at execution time) in it.
     *
     * Note: this would also work for a list with bind markers, but we don't support
     * that because 1) it's not excessively useful and 2) we wouldn't have a good
     * column name to return in the ColumnSpecification for those markers (not a
     * blocker per-se but we don't bother due to 1)).
     */
    public static class DelayedValue extends Term.NonTerminal
    {
        private final List<Term> elements;

        public DelayedValue(List<Term> elements)
        {
            this.elements = elements;
        }

        public boolean containsBindMarker()
        {
            // False since we don't support them in collection
            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(elements.size());
            for (Term t : elements)
            {
                ByteBuffer bytes = t.bindAndGet(options);

                if (bytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");

                // We don't support value > 64K because the serialization format encode the length as an unsigned short.
                if (bytes.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("List value is too long. List values are limited to %d bytes but %d bytes value provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    bytes.remaining()));

                buffers.add(bytes);
            }
            return new Value(buffers);
        }
    }

    /**
     * A marker for List values and IN relations
     */
    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof ListType;
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (ListType)receiver.type, options.getProtocolVersion());
        }
    }

    /*
     * For prepend, we need to be able to generate unique but decreasing time
     * UUID, which is a bit challenging. To do that, given a time in milliseconds,
     * we adds a number representing the 100-nanoseconds precision and make sure
     * that within the same millisecond, that number is always decreasing. We
     * do rely on the fact that the user will only provide decreasing
     * milliseconds timestamp for that purpose.
     */
    private static class PrecisionTime
    {
        // Our reference time (1 jan 2010, 00:00:00) in milliseconds.
        private static final long REFERENCE_TIME = 1262304000000L;
        private static final AtomicReference<PrecisionTime> last = new AtomicReference<PrecisionTime>(new PrecisionTime(Long.MAX_VALUE, 0));

        public final long millis;
        public final int nanos;

        PrecisionTime(long millis, int nanos)
        {
            this.millis = millis;
            this.nanos = nanos;
        }

        static PrecisionTime getNext(long millis)
        {
            while (true)
            {
                PrecisionTime current = last.get();

                assert millis <= current.millis;
                PrecisionTime next = millis < current.millis
                    ? new PrecisionTime(millis, 9999)
                    : new PrecisionTime(millis, Math.max(0, current.nanos - 1));

                if (last.compareAndSet(current, next))
                    return next;
            }
        }
    }

    public static class Setter extends Operation
    {
        public Setter(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            if (column.type.isMultiCell())
            {
                // delete + append
                CellName name = cf.getComparator().create(prefix, column);
                cf.addAtom(params.makeTombstoneForOverwrite(name.slice()));
            }
            Appender.doAppend(t, cf, prefix, column, params);
        }
    }

    public static class SetterByIndex extends Operation
    {
        private final Term idx;

        public SetterByIndex(ColumnDefinition column, Term idx, Term t)
        {
            super(column, t);
            this.idx = idx;
        }

        @Override
        public boolean requiresRead()
        {
            return true;
        }

        @Override
        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            super.collectMarkerSpecification(boundNames);
            idx.collectMarkerSpecification(boundNames);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            // we should not get here for frozen lists
            assert column.type.isMultiCell() : "Attempted to set an individual element on a frozen list";

            ByteBuffer index = idx.bindAndGet(params.options);
            ByteBuffer value = t.bindAndGet(params.options);

            if (index == null)
                throw new InvalidRequestException("Invalid null value for list index");

            List<Cell> existingList = params.getPrefetchedList(rowKey, column.name);
            int idx = ByteBufferUtil.toInt(index);
            if (idx < 0 || idx >= existingList.size())
                throw new InvalidRequestException(String.format("List index %d out of bound, list has size %d", idx, existingList.size()));

            CellName elementName = existingList.get(idx).name();
            if (value == null)
            {
                cf.addColumn(params.makeTombstone(elementName));
            }
            else
            {
                // We don't support value > 64K because the serialization format encode the length as an unsigned short.
                if (value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("List value is too long. List values are limited to %d bytes but %d bytes value provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    value.remaining()));

                cf.addColumn(params.makeColumn(elementName, value));
            }
        }
    }

    public static class Appender extends Operation
    {
        public Appender(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to append to a frozen list";
            doAppend(t, cf, prefix, column, params);
        }

        static void doAppend(Term t, ColumnFamily cf, Composite prefix, ColumnDefinition column, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.options);
            Lists.Value listValue = (Lists.Value)value;
            if (column.type.isMultiCell())
            {
                // If we append null, do nothing. Note that for Setter, we've
                // already removed the previous value so we're good here too
                if (value == null)
                    return;

                List<ByteBuffer> toAdd = listValue.elements;
                for (int i = 0; i < toAdd.size(); i++)
                {
                    ByteBuffer uuid = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
                    cf.addColumn(params.makeColumn(cf.getComparator().create(prefix, column, uuid), toAdd.get(i)));
                }
            }
            else
            {
                // for frozen lists, we're overwriting the whole cell value
                CellName name = cf.getComparator().create(prefix, column);
                if (value == null)
                    cf.addAtom(params.makeTombstone(name));
                else
                    cf.addColumn(params.makeColumn(name, listValue.getWithProtocolVersion(Server.CURRENT_VERSION)));
            }
        }
    }

    public static class Prepender extends Operation
    {
        public Prepender(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to prepend to a frozen list";
            Term.Terminal value = t.bind(params.options);
            if (value == null)
                return;

            assert value instanceof Lists.Value;
            long time = PrecisionTime.REFERENCE_TIME - (System.currentTimeMillis() - PrecisionTime.REFERENCE_TIME);

            List<ByteBuffer> toAdd = ((Lists.Value)value).elements;
            for (int i = 0; i < toAdd.size(); i++)
            {
                PrecisionTime pt = PrecisionTime.getNext(time);
                ByteBuffer uuid = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(pt.millis, pt.nanos));
                cf.addColumn(params.makeColumn(cf.getComparator().create(prefix, column, uuid), toAdd.get(i)));
            }
        }
    }

    public static class Discarder extends Operation
    {
        public Discarder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        @Override
        public boolean requiresRead()
        {
            return true;
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to delete from a frozen list";
            List<Cell> existingList = params.getPrefetchedList(rowKey, column.name);
            // We want to call bind before possibly returning to reject queries where the value provided is not a list.
            Term.Terminal value = t.bind(params.options);

            if (existingList.isEmpty())
                return;

            if (value == null)
                return;

            assert value instanceof Lists.Value;

            // Note: below, we will call 'contains' on this toDiscard list for each element of existingList.
            // Meaning that if toDiscard is big, converting it to a HashSet might be more efficient. However,
            // the read-before-write this operation requires limits its usefulness on big lists, so in practice
            // toDiscard will be small and keeping a list will be more efficient.
            List<ByteBuffer> toDiscard = ((Lists.Value)value).elements;
            for (Cell cell : existingList)
            {
                if (toDiscard.contains(cell.value()))
                    cf.addColumn(params.makeTombstone(cell.name()));
            }
        }
    }

    public static class DiscarderByIndex extends Operation
    {
        public DiscarderByIndex(ColumnDefinition column, Term idx)
        {
            super(column, idx);
        }

        @Override
        public boolean requiresRead()
        {
            return true;
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to delete an item by index from a frozen list";
            Term.Terminal index = t.bind(params.options);
            if (index == null)
                throw new InvalidRequestException("Invalid null value for list index");

            assert index instanceof Constants.Value;

            List<Cell> existingList = params.getPrefetchedList(rowKey, column.name);
            int idx = ByteBufferUtil.toInt(((Constants.Value)index).bytes);
            if (idx < 0 || idx >= existingList.size())
                throw new InvalidRequestException(String.format("List index %d out of bound, list has size %d", idx, existingList.size()));

            CellName elementName = existingList.get(idx).name();
            cf.addColumn(params.makeTombstone(elementName));
        }
    }
}
