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

#ifndef CQL3_SETS_HH
#define CQL3_SETS_HH

#include "cql3/abstract_marker.hh"
#include "maps.hh"
#include "column_specification.hh"
#include "column_identifier.hh"
#include "to_string.hh"
#include <unordered_set>

namespace cql3 {

#if 0
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Joiner;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
#endif

/**
 * Static helper methods and classes for sets.
 */
class sets {
    sets() = delete;
public:
    static shared_ptr<column_specification> value_spec_of(shared_ptr<column_specification> column) {
        return make_shared<column_specification>(column->ks_name, column->cf_name,
                ::make_shared<column_identifier>(sprint("value(%s)", *column->name), true),
                dynamic_pointer_cast<set_type_impl>(column->type)->get_elements_type());
    }

    class literal : public term::raw {
        std::vector<shared_ptr<term::raw>> _elements;
    public:
        explicit literal(std::vector<shared_ptr<term::raw>> elements)
                : _elements(std::move(elements)) {
        }

        shared_ptr<term> prepare(const sstring& keyspace, shared_ptr<column_specification> receiver) {
            validate_assignable_to(keyspace, receiver);

            // We've parsed empty maps as a set literal to break the ambiguity so
            // handle that case now
            if (_elements.empty() && dynamic_pointer_cast<map_type_impl>(receiver->type)) {
                // use empty_type for comparator, set is empty anyway.
                std::map<bytes, bytes, serialized_compare> m(empty_type->as_less_comparator());
                return ::make_shared<maps::value>(std::move(m));
            }

            auto value_spec = value_spec_of(receiver);
            std::vector<shared_ptr<term>> values;
            values.reserve(_elements.size());
            bool all_terminal = true;
            for (shared_ptr<term::raw> rt : _elements)
            {
                auto t = rt->prepare(keyspace, value_spec);

                if (t->contains_bind_marker()) {
                    throw exceptions::invalid_request_exception(sprint("Invalid set literal for %s: bind variables are not supported inside collection literals", *receiver->name));
                }

                if (dynamic_pointer_cast<non_terminal>(t)) {
                    all_terminal = false;
                }

                values.push_back(std::move(t));
            }
            auto compare = dynamic_pointer_cast<set_type_impl>(receiver->type)->get_elements_type()->as_less_comparator();

            auto value = ::make_shared<delayed_value>(compare, std::move(values));
            if (all_terminal) {
                return value->bind(query_options::DEFAULT);
            } else {
                return value;
            }
        }

        void validate_assignable_to(const sstring& keyspace, shared_ptr<column_specification> receiver) {
            if (!dynamic_pointer_cast<set_type_impl>(receiver->type)) {
                // We've parsed empty maps as a set literal to break the ambiguity so
                // handle that case now
                if (dynamic_pointer_cast<map_type_impl>(receiver->type) && _elements.empty()) {
                    return;
                }

                throw exceptions::invalid_request_exception(sprint("Invalid set literal for %s of type %s", *receiver->name, *receiver->type->as_cql3_type()));
            }

            auto&& value_spec = value_spec_of(receiver);
            for (shared_ptr<term::raw> rt : _elements) {
                if (!is_assignable(rt->test_assignment(keyspace, value_spec))) {
                    throw exceptions::invalid_request_exception(sprint("Invalid set literal for %s: value %s is not of type %s", *receiver->name, *rt, *value_spec->type->as_cql3_type()));
                }
            }
        }

        assignment_testable::test_result
        test_assignment(const sstring& keyspace, shared_ptr<column_specification> receiver) {
            if (!dynamic_pointer_cast<set_type_impl>(receiver->type)) {
                // We've parsed empty maps as a set literal to break the ambiguity so handle that case now
                if (dynamic_pointer_cast<map_type_impl>(receiver->type) && _elements.empty()) {
                    return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
                }

                return assignment_testable::test_result::NOT_ASSIGNABLE;
            }

            // If there is no elements, we can't say it's an exact match (an empty set if fundamentally polymorphic).
            if (_elements.empty()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }

            auto&& value_spec = value_spec_of(receiver);
            // FIXME: make assignment_testable::test_all() accept ranges
            std::vector<shared_ptr<assignment_testable>> to_test(_elements.begin(), _elements.end());
            return assignment_testable::test_all(keyspace, value_spec, to_test);
        }

        virtual sstring to_string() override {
            return "{" + join(", ", _elements) + "}";
        }
    };

    class value : public terminal, collection_terminal {
        std::set<bytes, serialized_compare> _elements;
    public:
        value(std::set<bytes, serialized_compare> elements)
                : _elements(std::move(elements)) {
        }

        static value from_serialized(bytes_view v, set_type type, int version) {
            try {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                // FIXME: deserializeForNativeProtocol?!
                auto s = boost::any_cast<set_type_impl::native_type>(type->deserialize(v, version));
                std::set<bytes, serialized_compare> elements(type->as_less_comparator());
                for (auto&& element : s) {
                    elements.insert(elements.end(), type->get_elements_type()->decompose(element));
                }
                return value(std::move(elements));
            } catch (marshal_exception& e) {
                throw exceptions::invalid_request_exception(e.why());
            }
        }

        virtual bytes_opt get(const query_options& options) override {
            return get_with_protocol_version(options.get_protocol_version());
        }

        virtual bytes get_with_protocol_version(int protocol_version) override {
            return collection_type_impl::pack(_elements.begin(), _elements.end(),
                    _elements.size(), protocol_version);
        }

        bool equals(set_type st, const value& v) {
            if (_elements.size() != v._elements.size()) {
                return false;
            }
            auto&& elements_type = st->get_elements_type();
            return std::equal(_elements.begin(), _elements.end(),
                    v._elements.begin(),
                    [elements_type] (bytes_view v1, bytes_view v2) {
                        return elements_type->equal(v1, v2);
                    });
        }

        virtual sstring to_string() const override {
            sstring result = "{";
            bool first = true;
            for (auto&& e : _elements) {
                if (!first) {
                    result += ", ";
                }
                first = true;
                result += to_hex(e);
            }
            result += "}";
            return result;
        }
    };

    // See Lists.DelayedValue
    class delayed_value : public non_terminal {
        serialized_compare _comparator;
        std::vector<shared_ptr<term>> _elements;
    public:
        delayed_value(serialized_compare comparator, std::vector<shared_ptr<term>> elements)
            : _comparator(std::move(comparator)), _elements(std::move(elements)) {
        }

        virtual bool contains_bind_marker() const override {
            // False since we don't support them in collection
            return false;
        }

        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override {
        }

        virtual shared_ptr<terminal> bind(const query_options& options) {
            std::set<bytes, serialized_compare> buffers(_comparator);
            for (auto&& t : _elements) {
                bytes_opt b = t->bind_and_get(options);

                if (!b) {
                    throw exceptions::invalid_request_exception("null is not supported inside collections");
                }

                // We don't support value > 64K because the serialization format encode the length as an unsigned short.
                if (b->size() > std::numeric_limits<uint16_t>::max()) {
                    throw exceptions::invalid_request_exception(sprint("Set value is too long. Set values are limited to %d bytes but %d bytes value provided",
                            std::numeric_limits<uint16_t>::max(),
                            b->size()));
                }

                buffers.insert(buffers.end(), std::move(*b));
            }
            return ::make_shared<value>(std::move(buffers));
        }
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        { }
#if 0
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof SetType;
        }
#endif

        virtual ::shared_ptr<terminal> bind(const query_options& options) override {
            throw std::runtime_error("");
        }
#if 0
        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (SetType)receiver.type, options.getProtocolVersion());
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
                // delete + add
                CellName name = cf.getComparator().create(prefix, column);
                cf.addAtom(params.makeTombstoneForOverwrite(name.slice()));
            }
            Adder.doAdd(t, cf, prefix, column, params);
        }
    }

    public static class Adder extends Operation
    {
        public Adder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to add items to a frozen set";

            doAdd(t, cf, prefix, column, params);
        }

        static void doAdd(Term t, ColumnFamily cf, Composite prefix, ColumnDefinition column, UpdateParameters params) throws InvalidRequestException
        {
            Term.Terminal value = t.bind(params.options);
            Sets.Value setValue = (Sets.Value)value;
            if (column.type.isMultiCell())
            {
                if (value == null)
                    return;

                Set<ByteBuffer> toAdd = setValue.elements;
                for (ByteBuffer bb : toAdd)
                {
                    CellName cellName = cf.getComparator().create(prefix, column, bb);
                    cf.addColumn(params.makeColumn(cellName, ByteBufferUtil.EMPTY_BYTE_BUFFER));
                }
            }
            else
            {
                // for frozen sets, we're overwriting the whole cell
                CellName cellName = cf.getComparator().create(prefix, column);
                if (value == null)
                    cf.addAtom(params.makeTombstone(cellName));
                else
                    cf.addColumn(params.makeColumn(cellName, ((Value) value).getWithProtocolVersion(Server.CURRENT_VERSION)));
            }
        }
    }

    // Note that this is reused for Map subtraction too (we subtract a set from a map)
    public static class Discarder extends Operation
    {
        public Discarder(ColumnDefinition column, Term t)
        {
            super(column, t);
        }

        public void execute(ByteBuffer rowKey, ColumnFamily cf, Composite prefix, UpdateParameters params) throws InvalidRequestException
        {
            assert column.type.isMultiCell() : "Attempted to remove items from a frozen set";

            Term.Terminal value = t.bind(params.options);
            if (value == null)
                return;

            // This can be either a set or a single element
            Set<ByteBuffer> toDiscard = value instanceof Constants.Value
                                      ? Collections.singleton(((Constants.Value)value).bytes)
                                      : ((Sets.Value)value).elements;

            for (ByteBuffer bb : toDiscard)
            {
                cf.addColumn(params.makeTombstone(cf.getComparator().create(prefix, column, bb)));
            }
        }
    }
#endif
};

}

#endif
