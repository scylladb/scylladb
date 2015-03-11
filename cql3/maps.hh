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

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_MAPS_HH
#define CQL3_MAPS_HH

#include "cql3/abstract_marker.hh"
#include "cql3/term.hh"
#include "operation.hh"
#include "update_parameters.hh"

namespace cql3 {

#if 0
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
#endif

/**
 * Static helper methods and classes for maps.
 */
class maps {
private:
    maps() = delete;
public:
    static shared_ptr<column_specification> key_spec_of(column_specification& column) {
        return ::make_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(sprint("key(%s)", *column.name), true),
                dynamic_pointer_cast<map_type_impl>(column.type)->get_keys_type());
    }

    static shared_ptr<column_specification> value_spec_of(column_specification& column) {
        return ::make_shared<column_specification>(column.ks_name, column.cf_name,
                ::make_shared<column_identifier>(sprint("value(%s)", *column.name), true),
                dynamic_pointer_cast<map_type_impl>(column.type)->get_values_type());
    }

    class literal : public term::raw {
    public:
        const std::vector<std::pair<::shared_ptr<term::raw>, ::shared_ptr<term::raw>>> entries;

        literal(const std::vector<std::pair<::shared_ptr<term::raw>, ::shared_ptr<term::raw>>>& entries_)
            : entries{entries_}
        { }

        virtual ::shared_ptr<term> prepare(const sstring& keyspace, ::shared_ptr<column_specification> receiver) override {
            throw std::runtime_error("not implemented");
#if 0
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
#endif
        }

#if 0
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
#endif

        virtual assignment_testable::test_result test_assignment(const sstring& keyspace, ::shared_ptr<column_specification> receiver) override {
            throw std::runtime_error("not implemented");
#if 0
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
#endif
        }

        virtual sstring to_string() override {
            sstring result = "{";
            for (size_t i = 0; i < entries.size(); i++) {
                if (i > 0) {
                    result += ", ";
                }
                result += entries[i].first->to_string();
                result += ":";
                result += entries[i].second->to_string();
            }
            result += "}";
            return result;
        }
    };

    class value : public terminal, collection_terminal {
    public:
        std::map<bytes, bytes, serialized_compare> map;

        value(std::map<bytes, bytes, serialized_compare> map)
            : map(std::move(map)) {
        }

        static value from_serialized(bytes value, map_type type, int version) {
            try {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                // FIXME: deserialize_for_native_protocol?!
                auto m = boost::any_cast<map_type_impl::native_type&&>(type->deserialize(value, version));
                std::map<bytes, bytes, serialized_compare> map(type->get_keys_type()->as_less_comparator());
                for (auto&& e : m) {
                    map.emplace(type->get_keys_type()->decompose(e.first),
                                type->get_values_type()->decompose(e.second));
                }
                return { std::move(map) };
            } catch (marshal_exception& e) {
                throw exceptions::invalid_request_exception(e.why());
            }
        }

        virtual bytes_opt get(const query_options& options) override {
            return get_with_protocol_version(options.get_protocol_version());
        }

        virtual bytes get_with_protocol_version(int protocol_version) {
            std::ostringstream out;
            write_collection_size(out, protocol_version, map.size());
            for (auto&& e : map) {
                write_collection_value(out, protocol_version, e.first);
                write_collection_value(out, protocol_version, e.second);
            }
            return out.str();
        }

        bool equals(map_type mt, const value& v) {
            return std::equal(map.begin(), map.end(),
                              v.map.begin(), v.map.end(),
                              [mt] (auto&& e1, auto&& e2) {
                return mt->get_keys_type()->compare(e1.first, e2.first) == 0
                        && mt->get_values_type()->compare(e1.second, e2.second) == 0;
            });
        }

        virtual sstring to_string() const {
            // FIXME:
            abort();
        }
    };

#if 0
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
#endif

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        { }
#if 0
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof MapType;
        }
#endif

        virtual ::shared_ptr<terminal> bind(const query_options& options) override {
            throw std::runtime_error("");
        }
#if 0
        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (MapType)receiver.type, options.getProtocolVersion());
        }
#endif
    };

#if 0
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
#endif

    class setter_by_key : public operation {
        const shared_ptr<term> _k;
    public:
        setter_by_key(column_definition& column, shared_ptr<term> k, shared_ptr<term> t)
            : operation(column, std::move(t)), _k(std::move(k)) {
        }

        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override {
            operation::collect_marker_specification(bound_names);
            _k->collect_marker_specification(bound_names);
        }

        virtual void execute(mutation& m, const clustering_prefix& prefix, const update_parameters& params) override {
            using exceptions::invalid_request_exception;
            assert(column.type->is_multi_cell()); // "Attempted to set a value for a single key on a frozen map"m
            bytes_opt key = _k->bind_and_get(params._options);
            bytes_opt value = _t->bind_and_get(params._options);
            if (!key) {
                throw invalid_request_exception("Invalid null map key");
            }
            if (value && value->size() >= std::numeric_limits<uint16_t>::max()) {
                throw invalid_request_exception(
                        sprint("Map value is too long. Map values are limited to %d bytes but %d bytes value provided",
                               std::numeric_limits<uint16_t>::max(),
                               value->size()));
            }
            auto avalue = value ? params.make_cell(*value) : params.make_dead_cell();
            map_type_impl::mutation update = { { std::move(*key), std::move(avalue) } };
            // should have been verified as map earlier?
            auto ctype = static_pointer_cast<map_type_impl>(column.type);
            auto col_mut = ctype->serialize_mutation_form(std::move(update));
            if (column.is_static()) {
                m.set_static_cell(column, std::move(col_mut));
            } else {
                m.set_clustered_cell(prefix, column, std::move(col_mut));
            }
        }
    };
#if 0
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
#endif
};

}

#endif
