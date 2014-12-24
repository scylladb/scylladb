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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Static helper methods and classes for maps.
 */
public abstract class Maps
{
    private Maps() {}

    public static ColumnSpecification keySpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("key(" + column.name + ")", true), ((MapType)column.type).getKeysType());
    }

    public static ColumnSpecification valueSpecOf(ColumnSpecification column)
    {
        return new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier("value(" + column.name + ")", true), ((MapType)column.type).getValuesType());
    }

    public static class Literal implements Term.Raw
    {
        public final List<Pair<Term.Raw, Term.Raw>> entries;

        public Literal(List<Pair<Term.Raw, Term.Raw>> entries)
        {
            this.entries = entries;
        }

        public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            validateAssignableTo(keyspace, receiver);

            ColumnSpecification keySpec = Maps.keySpecOf(receiver);
            ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
            Map<Term, Term> values = new HashMap<Term, Term>(entries.size());
            boolean allTerminal = true;
            for (Pair<Term.Raw, Term.Raw> entry : entries)
            {
                Term k = entry.left.prepare(keyspace, keySpec);
                Term v = entry.right.prepare(keyspace, valueSpec);

                if (k.containsBindMarker() || v.containsBindMarker())
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: bind variables are not supported inside collection literals", receiver.name));

                if (k instanceof Term.NonTerminal || v instanceof Term.NonTerminal)
                    allTerminal = false;

                values.put(k, v);
            }
            DelayedValue value = new DelayedValue(((MapType)receiver.type).getKeysType(), values);
            return allTerminal ? value.bind(QueryOptions.DEFAULT) : value;
        }

        private void validateAssignableTo(String keyspace, ColumnSpecification receiver) throws InvalidRequestException
        {
            if (!(receiver.type instanceof MapType))
                throw new InvalidRequestException(String.format("Invalid map literal for %s of type %s", receiver.name, receiver.type.asCQL3Type()));

            ColumnSpecification keySpec = Maps.keySpecOf(receiver);
            ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
            for (Pair<Term.Raw, Term.Raw> entry : entries)
            {
                if (!entry.left.testAssignment(keyspace, keySpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: key %s is not of type %s", receiver.name, entry.left, keySpec.type.asCQL3Type()));
                if (!entry.right.testAssignment(keyspace, valueSpec).isAssignable())
                    throw new InvalidRequestException(String.format("Invalid map literal for %s: value %s is not of type %s", receiver.name, entry.right, valueSpec.type.asCQL3Type()));
            }
        }

        public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver)
        {
            if (!(receiver.type instanceof MapType))
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

            // If there is no elements, we can't say it's an exact match (an empty map if fundamentally polymorphic).
            if (entries.isEmpty())
                return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;

            ColumnSpecification keySpec = Maps.keySpecOf(receiver);
            ColumnSpecification valueSpec = Maps.valueSpecOf(receiver);
            // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
            AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;
            for (Pair<Term.Raw, Term.Raw> entry : entries)
            {
                AssignmentTestable.TestResult t1 = entry.left.testAssignment(keyspace, keySpec);
                AssignmentTestable.TestResult t2 = entry.right.testAssignment(keyspace, valueSpec);
                if (t1 == AssignmentTestable.TestResult.NOT_ASSIGNABLE || t2 == AssignmentTestable.TestResult.NOT_ASSIGNABLE)
                    return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
                if (t1 != AssignmentTestable.TestResult.EXACT_MATCH || t2 != AssignmentTestable.TestResult.EXACT_MATCH)
                    res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
            }
            return res;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < entries.size(); i++)
            {
                if (i > 0) sb.append(", ");
                sb.append(entries.get(i).left).append(":").append(entries.get(i).right);
            }
            sb.append("}");
            return sb.toString();
        }
    }

    public static class Value extends Term.Terminal implements Term.CollectionTerminal
    {
        public final Map<ByteBuffer, ByteBuffer> map;

        public Value(Map<ByteBuffer, ByteBuffer> map)
        {
            this.map = map;
        }

        public static Value fromSerialized(ByteBuffer value, MapType type, int version) throws InvalidRequestException
        {
            try
            {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                Map<?, ?> m = (Map<?, ?>)type.getSerializer().deserializeForNativeProtocol(value, version);
                Map<ByteBuffer, ByteBuffer> map = new LinkedHashMap<ByteBuffer, ByteBuffer>(m.size());
                for (Map.Entry<?, ?> entry : m.entrySet())
                    map.put(type.getKeysType().decompose(entry.getKey()), type.getValuesType().decompose(entry.getValue()));
                return new Value(map);
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
            List<ByteBuffer> buffers = new ArrayList<>(2 * map.size());
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet())
            {
                buffers.add(entry.getKey());
                buffers.add(entry.getValue());
            }
            return CollectionSerializer.pack(buffers, map.size(), protocolVersion);
        }

        public boolean equals(MapType mt, Value v)
        {
            if (map.size() != v.map.size())
                return false;

            // We use the fact that we know the maps iteration will both be in comparator order
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> thisIter = map.entrySet().iterator();
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> thatIter = v.map.entrySet().iterator();
            while (thisIter.hasNext())
            {
                Map.Entry<ByteBuffer, ByteBuffer> thisEntry = thisIter.next();
                Map.Entry<ByteBuffer, ByteBuffer> thatEntry = thatIter.next();
                if (mt.getKeysType().compare(thisEntry.getKey(), thatEntry.getKey()) != 0 || mt.getValuesType().compare(thisEntry.getValue(), thatEntry.getValue()) != 0)
                    return false;
            }

            return true;
        }
    }

    // See Lists.DelayedValue
    public static class DelayedValue extends Term.NonTerminal
    {
        private final Comparator<ByteBuffer> comparator;
        private final Map<Term, Term> elements;

        public DelayedValue(Comparator<ByteBuffer> comparator, Map<Term, Term> elements)
        {
            this.comparator = comparator;
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
            Map<ByteBuffer, ByteBuffer> buffers = new TreeMap<ByteBuffer, ByteBuffer>(comparator);
            for (Map.Entry<Term, Term> entry : elements.entrySet())
            {
                // We don't support values > 64K because the serialization format encode the length as an unsigned short.
                ByteBuffer keyBytes = entry.getKey().bindAndGet(options);
                if (keyBytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (keyBytes.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Map key is too long. Map keys are limited to %d bytes but %d bytes keys provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    keyBytes.remaining()));

                ByteBuffer valueBytes = entry.getValue().bindAndGet(options);
                if (valueBytes == null)
                    throw new InvalidRequestException("null is not supported inside collections");
                if (valueBytes.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Map value is too long. Map values are limited to %d bytes but %d bytes value provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    valueBytes.remaining()));

                buffers.put(keyBytes, valueBytes);
            }
            return new Value(buffers);
        }
    }

    public static class Marker extends AbstractMarker
    {
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof MapType;
        }

        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (MapType)receiver.type, options.getProtocolVersion());
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
                // delete + put
                CellName name = cf.getComparator().create(prefix, column);
                cf.addAtom(params.makeTombstoneForOverwrite(name.slice()));
            }
            Putter.doPut(t, cf, prefix, column, params);
        }
    }

    public static class SetterByKey extends Operation
    {
        private final Term k;

        public SetterByKey(ColumnDefinition column, Term k, Term t)
        {
            super(column, t);
            this.k = k;
        }

        @Override
        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            super.collectMarkerSpecification(boundNames);
            k.collectMarkerSpecification(boundNames);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to set a value for a single key on a frozen map";
            ByteBuffer key = k.bindAndGet(params.options);
            ByteBuffer value = t.bindAndGet(params.options);
            if (key == null)
                throw new InvalidRequestException("Invalid null map key");

            CellName cellName = cf.getComparator().create(prefix, column, key);

            if (value == null)
            {
                cf.addColumn(params.makeTombstone(cellName));
            }
            else
            {
                // We don't support value > 64K because the serialization format encode the length as an unsigned short.
                if (value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
                    throw new InvalidRequestException(String.format("Map value is too long. Map values are limited to %d bytes but %d bytes value provided",
                                                                    FBUtilities.MAX_UNSIGNED_SHORT,
                                                                    value.remaining()));

                cf.addColumn(params.makeColumn(cellName, value));
            }
        }
    }

    public static class Putter extends Operation
    {
        public Putter(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to add items to a frozen map";
            doPut(t, cf, prefix, column, params);
        }

        static void doPut(Term t, ColumnFamily cf, Composite prefix, ColumnDefinition column, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.options);
            Maps.Value mapValue = (Maps.Value) value;
            if (column.type.isMultiCell())
            {
                if (value == null)
                    return;

                for (Map.Entry<ByteBuffer, ByteBuffer> entry : mapValue.map.entrySet())
                {
                    CellName cellName = cf.getComparator().create(prefix, column, entry.getKey());
                    cf.addColumn(params.makeColumn(cellName, entry.getValue()));
                }
            }
            else
            {
                // for frozen maps, we're overwriting the whole cell
                CellName cellName = cf.getComparator().create(prefix, column);
                if (value == null)
                    cf.addAtom(params.makeTombstone(cellName));
                else
                    cf.addColumn(params.makeColumn(cellName, mapValue.getWithProtocolVersion(Server.CURRENT_VERSION)));
            }
        }
    }

    public static class DiscarderByKey extends Operation
    {
        public DiscarderByKey(ColumnDefinition column, Term k)
        {
            super(column, k);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to delete a single key in a frozen map";
            Term.Terminal key = t.bind(params.options);
            if (key == null)
                throw new InvalidRequestException("Invalid null map key");
            assert key instanceof Constants.Value;

            CellName cellName = cf.getComparator().create(prefix, column, ((Constants.Value)key).bytes);
            cf.addColumn(params.makeTombstone(cellName));
        }
    }
}
