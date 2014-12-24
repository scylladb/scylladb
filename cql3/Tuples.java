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
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Term.MultiColumnRaw;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;

/**
 * Static helper methods and classes for tuples.
 */
public class Tuples
{
    private static final Logger logger = LoggerFactory.getLogger(Tuples.class);

    private Tuples() {}

    public static ColumnSpecification componentSpecOf(ColumnSpecification column, int component)
    {
        return new ColumnSpecification(column.ksName,
                                       column.cfName,
                                       new ColumnIdentifier(String.format("%s[%d]", column.name, component), true),
                                       ((TupleType)column.type).type(component));
    }

    /**
     * A raw, literal tuple.  When prepared, this will become a Tuples.Value or Tuples.DelayedValue, depending
     * on whether the tuple holds NonTerminals.
     */
    public static class Literal implements Term.MultiColumnRaw
    {
        private final List<Term.Raw> elements;

        public Literal(List<Term.Raw> elements)
        {
            this.elements = elements;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            List<Term> values = new ArrayList<>(elements.size());
            boolean allTerminal = true;
            for (int i = 0; i < elements.size(); i++)
            {
                Term value = elements.get(i).prepare(keyspace, componentSpecOf(receiver, i));
                if (value instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(value);
            }
            DelayedValue value = new DelayedValue((TupleType)receiver.type, values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        public Term prepare(String keyspace, List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            if (elements.size() != receivers.size())
                throw new InvalidRequestException(String.format("Expected %d elements in value tuple, but got %d: %s", receivers.size(), elements.size(), this));

            List<Term> values = new ArrayList<>(elements.size());
            List<AbstractType<?>> types = new ArrayList<>(elements.size());
            boolean allTerminal = true;
            for (int i = 0; i < elements.size(); i++)
            {
                Term t = elements.get(i).prepare(keyspace, receivers.get(i));
                if (t instanceof Term.NonTerminal)
                    allTerminal = false;

                values.add(t);
                types.add(receivers.get(i).type);
            }
            DelayedValue value = new DelayedValue(new TupleType(types), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof TupleType))
                throw new InvalidRequestException(String.format("Invalid tuple type literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));

            TupleType tt = (TupleType)receiver.type;
            for (int i = 0; i < elements.size(); i++)
            {
                if (i >= tt.size())
                    throw new InvalidRequestException(String.format("Invalid tuple literal for %s: too many elements. Type %s expects %d but got %d",
                                                                    receiver.name, tt.asCQL3Type(), tt.size(), elements.size()));

                Term.Raw value = elements.get(i);
                ColumnSpecification spec = componentSpecOf(receiver, i);
                if (!value.testAssignment(keyspace, spec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid tuple literal for %s: component %d is not of type %s", receiver.name, i, spec.type.asCQL3Type()));
            }
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            try
            {
                validateAssignableTo(keyspace, receiver);
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            }
            catch (InvalidRequestException e)
            {
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            }
        }

        @Override
        public String toString()
        {
            return tupleToString(elements);
        }
    }

    /**
     * A tuple of terminal values (e.g (123, 'abc')).
     */
    public static class Value extends Term.MultiItemTerminal
    {
        public final ByteBuffer[] elements;

        public Value(ByteBuffer[] elements)
        {
            this.elements = elements;
        }

        public static Value fromSerialized(ByteBuffer bytes, TupleType type)
        {
            return new Value(type.split(bytes));
        }

        public ByteBuffer get(QueryOptions options)
        {
            return TupleType.buildValue(elements);
        }

        public List<ByteBuffer> getElements()
        {
            return Arrays.asList(elements);
        }
    }

    /**
     * Similar to Value, but contains at least one NonTerminal, such as a non-pure functions or bind marker.
     */
    public static class DelayedValue extends Term.NonTerminal
    {
        public final TupleType type;
        public final List<Term> elements;

        public DelayedValue(TupleType type, List<Term> elements)
        {
            this.type = type;
            this.elements = elements;
        }

        public boolean containsBindMarker()
        {
            for (Term term : elements)
                if (term.containsBindMarker())
                    return true;

            return false;
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            for (Term term : elements)
                term.collectMarkerSpecification(boundNames);
        }

        private ByteBuffer[] bindInternal(QueryOptions options) throws InvalidRequestException
        {
            int version = options.getProtocolVersion();

            ByteBuffer[] buffers = new ByteBuffer[elements.size()];
            for (int i = 0; i < elements.size(); i++)
            {
                buffers[i] = elements.get(i).bindAndGet(options);
                // Inside tuples, we must force the serialization of collections to v3 whatever protocol
                // version is in use since we're going to store directly that serialized value.
                if (version < 3 && type.type(i).isCollection())
                    buffers[i] = ((CollectionType)type.type(i)).getSerializer().reserializeToV3(buffers[i]);
            }
            return buffers;
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            return new Value(bindInternal(options));
        }

        @Override
        public ByteBuffer bindAndGet(QueryOptions options) throws InvalidRequestException
        {
            // We don't "need" that override but it saves us the allocation of a Value object if used
            return TupleType.buildValue(bindInternal(options));
        }

        @Override
        public String toString()
        {
            return tupleToString(elements);
        }
    }

    /**
     * A terminal value for a list of IN values that are tuples. For example: "SELECT ... WHERE (a, b, c) IN ?"
     * This is similar to Lists.Value, but allows us to keep components of the tuples in the list separate.
     */
    public static class InValue extends Term.Terminal
    {
        List<List<ByteBuffer>> elements;

        public InValue(List<List<ByteBuffer>> items)
        {
            this.elements = items;
        }

        public static InValue fromSerialized(ByteBuffer value, ListType type, QueryOptions options) throws InvalidRequestException
        {
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but the deserialization does the validation (so we're fine).
                List<?> l = (List<?>)type.getSerializer().deserializeForNativeProtocol(value, options.getProtocolVersion());

                assert type.getElementsType() instanceof TupleType;
                TupleType tupleType = (TupleType) type.getElementsType();

                // type.split(bytes)
                List<List<ByteBuffer>> elements = new ArrayList<>(l.size());
                for (Object element : l)
                    elements.add(Arrays.asList(tupleType.split(type.getElementsType().decompose(element))));
                return new InValue(elements);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        public ByteBuffer get(QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        public List<List<ByteBuffer>> getSplitValues()
        {
            return elements;
        }
    }

    /**
     * A raw placeholder for a tuple of values for different multiple columns, each of which may have a different type.
     * For example, "SELECT ... WHERE (col1, col2) > ?".
     */
    public static class Raw extends AbstractMarker.Raw implements Term.MultiColumnRaw
    {
        public Raw(int bindIndex)
        {
            super(bindIndex);
        }

        private static ColumnSpecification makeReceiver(List<? extends ColumnSpecification> receivers)
        {
            List<AbstractType<?>> types = new ArrayList<>(receivers.size());
            StringBuilder inName = new StringBuilder("(");
            for (int i = 0; i < receivers.size(); i++)
            {
                ColumnSpecification receiver = receivers.get(i);
                inName.append(receiver.name);
                if (i < receivers.size() - 1)
                    inName.append(",");
                types.add(receiver.type);
            }
            inName.append(')');

            ColumnIdentifier identifier = new ColumnIdentifier(inName.toString(), true);
            TupleType type = new TupleType(types);
            return new ColumnSpecification(receivers.get(0).ksName, receivers.get(0).cfName, identifier, type);
        }

        public AbstractMarker prepare(String keyspace, List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            return new Tuples.Marker(bindIndex, makeReceiver(receivers));
        }

        @Override
        public AbstractMarker prepare(String keyspace, ColumnSpecification receiver)
        {
            throw new AssertionError("Tuples.Raw.prepare() requires a list of receivers");
        }
    }

    /**
     * A raw marker for an IN list of tuples, like "SELECT ... WHERE (a, b, c) IN ?"
     */
    public static class INRaw extends AbstractMarker.Raw implements MultiColumnRaw
    {
        public INRaw(int bindIndex)
        {
            super(bindIndex);
        }

        private static ColumnSpecification makeInReceiver(List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            List<AbstractType<?>> types = new ArrayList<>(receivers.size());
            StringBuilder inName = new StringBuilder("in(");
            for (int i = 0; i < receivers.size(); i++)
            {
                ColumnSpecification receiver = receivers.get(i);
                inName.append(receiver.name);
                if (i < receivers.size() - 1)
                    inName.append(",");

                if (receiver.type.isCollection() && receiver.type.isMultiCell())
                    throw new InvalidRequestException("Non-frozen collection columns do not support IN relations");

                types.add(receiver.type);
            }
            inName.append(')');

            ColumnIdentifier identifier = new ColumnIdentifier(inName.toString(), true);
            TupleType type = new TupleType(types);
            return new ColumnSpecification(receivers.get(0).ksName, receivers.get(0).cfName, identifier, ListType.getInstance(type, false));
        }

        public AbstractMarker prepare(String keyspace, List<? extends ColumnSpecification> receivers) throws InvalidRequestException
        {
            return new InMarker(bindIndex, makeInReceiver(receivers));
        }

        @Override
        public AbstractMarker prepare(String keyspace, ColumnSpecification receiver)
        {
            throw new AssertionError("Tuples.INRaw.prepare() requires a list of receivers");
        }
    }

    /**
     * Represents a marker for a single tuple, like "SELECT ... WHERE (a, b, c) > ?"
     */
    public static class Marker extends AbstractMarker
    {
        public Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (TupleType)receiver.type);
        }
    }

    /**
     * Represents a marker for a set of IN values that are tuples, like "SELECT ... WHERE (a, b, c) IN ?"
     */
    public static class InMarker extends AbstractMarker
    {
        protected InMarker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof ListType;
        }

        public InValue bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : InValue.fromSerialized(value, (ListType)receiver.type, options);
        }
    }

    public static String tupleToString(List<?> items)
    {

        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < items.size(); i++)
        {
            sb.append(items.get(i));
            if (i < items.size() - 1)
                sb.append(", ");
        }
        sb.append(')');
        return sb.toString();
    }
}
