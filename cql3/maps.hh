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
#include "exceptions/exceptions.hh"
#include "cql3/cql3_type.hh"

namespace cql3 {

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
            validate_assignable_to(keyspace, *receiver);

            auto key_spec = maps::key_spec_of(*receiver);
            auto value_spec = maps::value_spec_of(*receiver);
            std::unordered_map<shared_ptr<term>, shared_ptr<term>> values;
            values.reserve(entries.size());
            bool all_terminal = true;
            for (auto&& entry : entries) {
                auto k = entry.first->prepare(keyspace, key_spec);
                auto v = entry.second->prepare(keyspace, value_spec);

                if (k->contains_bind_marker() || v->contains_bind_marker()) {
                    throw exceptions::invalid_request_exception(sprint("Invalid map literal for %s: bind variables are not supported inside collection literals", *receiver->name));
                }

                if (dynamic_pointer_cast<non_terminal>(k) || dynamic_pointer_cast<non_terminal>(v)) {
                    all_terminal = false;
                }

                values.emplace(k, v);
            }
            delayed_value value(static_pointer_cast<map_type_impl>(receiver->type)->get_keys_type()->as_less_comparator(), values);
            if (all_terminal) {
                return value.bind(query_options::DEFAULT);
            } else {
                return make_shared(std::move(value));
            }
        }

    private:
        void validate_assignable_to(const sstring& keyspace, column_specification& receiver) {
            if (!dynamic_pointer_cast<map_type_impl>(receiver.type)) {
                throw exceptions::invalid_request_exception(sprint("Invalid map literal for %s of type %s", *receiver.name, *receiver.type->as_cql3_type()));
            }
            auto&& key_spec = maps::key_spec_of(receiver);
            auto&& value_spec = maps::value_spec_of(receiver);
            for (auto&& entry : entries) {
                if (!is_assignable(entry.first->test_assignment(keyspace, key_spec))) {
                    throw exceptions::invalid_request_exception(sprint("Invalid map literal for %s: key %s is not of type %s", *receiver.name, *entry.first, *key_spec->type->as_cql3_type()));
                }
                if (!is_assignable(entry.second->test_assignment(keyspace, value_spec))) {
                    throw exceptions::invalid_request_exception(sprint("Invalid map literal for %s: value %s is not of type %s", *receiver.name, *entry.second, *value_spec->type->as_cql3_type()));
                }
            }
        }
    public:

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

        virtual sstring to_string() const override {
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

        static value from_serialized(bytes_view value, map_type type, serialization_format sf) {
            try {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but compose does the validation (so we're fine).
                // FIXME: deserialize_for_native_protocol?!
                auto m = boost::any_cast<map_type_impl::native_type>(type->deserialize(value, sf));
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
            return get_with_protocol_version(options.get_serialization_format());
        }

        virtual bytes get_with_protocol_version(serialization_format sf) {
            //FIXME: share code with serialize_partially_deserialized_form
            size_t len = collection_value_len(sf) * map.size() * 2 + collection_size_len(sf);
            for (auto&& e : map) {
                len += e.first.size() + e.second.size();
            }
            bytes b(bytes::initialized_later(), len);
            bytes::iterator out = b.begin();

            write_collection_size(out, map.size(), sf);
            for (auto&& e : map) {
                write_collection_value(out, sf, e.first);
                write_collection_value(out, sf, e.second);
            }
            return b;
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

    // See Lists.DelayedValue
    class delayed_value : public non_terminal {
        serialized_compare _comparator;
        std::unordered_map<shared_ptr<term>, shared_ptr<term>> _elements;
    public:
        delayed_value(serialized_compare comparator,
                      std::unordered_map<shared_ptr<term>, shared_ptr<term>> elements)
                : _comparator(std::move(comparator)), _elements(std::move(elements)) {
        }

        virtual bool contains_bind_marker() const override {
            // False since we don't support them in collection
            return false;
        }

        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override {
        }

        shared_ptr<terminal> bind(const query_options& options) {
            std::map<bytes, bytes, serialized_compare> buffers(_comparator);
            for (auto&& entry : _elements) {
                auto&& key = entry.first;
                auto&& value = entry.second;

                // We don't support values > 64K because the serialization format encode the length as an unsigned short.
                bytes_opt key_bytes = key->bind_and_get(options);
                if (!key_bytes) {
                    throw exceptions::invalid_request_exception("null is not supported inside collections");
                }
                if (key_bytes->size() > std::numeric_limits<uint16_t>::max()) {
                    throw exceptions::invalid_request_exception(sprint("Map key is too long. Map keys are limited to %d bytes but %d bytes keys provided",
                                                           std::numeric_limits<uint16_t>::max(),
                                                           key_bytes->size()));
                }
                bytes_opt value_bytes = value->bind_and_get(options);
                if (!value_bytes) {
                    throw exceptions::invalid_request_exception("null is not supported inside collections");\
                }
                if (value_bytes->size() > std::numeric_limits<uint16_t>::max()) {
                    throw exceptions::invalid_request_exception(sprint("Map value is too long. Map values are limited to %d bytes but %d bytes value provided",
                                                            std::numeric_limits<uint16_t>::max(),
                                                            value_bytes->size()));
                }
                buffers.emplace(std::move(*key_bytes), std::move(*value_bytes));
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

    class setter : public operation {
    public:
        setter(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }

        virtual void execute(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params) override {
            tombstone ts;
            if (column.type->is_multi_cell()) {
                // delete + put
                // delete + append
                ts = params.make_tombstone_just_before();
            }
            do_put(m, row_key, params, _t, column, ts);
        }
    };

    class setter_by_key : public operation {
        const shared_ptr<term> _k;
    public:
        setter_by_key(const column_definition& column, shared_ptr<term> k, shared_ptr<term> t)
            : operation(column, std::move(t)), _k(std::move(k)) {
        }

        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override {
            operation::collect_marker_specification(bound_names);
            _k->collect_marker_specification(bound_names);
        }

        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override {
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
            map_type_impl::mutation update = { {}, { { std::move(*key), std::move(avalue) } } };
            // should have been verified as map earlier?
            auto ctype = static_pointer_cast<map_type_impl>(column.type);
            auto col_mut = ctype->serialize_mutation_form(std::move(update));
            m.set_cell(prefix, column, std::move(col_mut));
        }
    };

    class putter : public operation {
    public:
        putter(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) {
        }

        virtual void execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override {
            assert(column.type->is_multi_cell()); // "Attempted to add items to a frozen map";
            do_put(m, prefix, params, _t, column);
        }
    };

    static void do_put(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params,
            shared_ptr<term> t, const column_definition& column, tombstone ts = {}) {
        auto value = t->bind(params._options);
        auto map_value = dynamic_pointer_cast<maps::value>(value);
        if (column.type->is_multi_cell()) {
            collection_type_impl::mutation mut;
            mut.tomb = ts;

            if (!value) {
                return;
            }

            for (auto&& e : map_value->map) {
                mut.cells.emplace_back(e.first, params.make_cell(e.second));
            }
            auto ctype = static_pointer_cast<map_type_impl>(column.type);
            auto col_mut = ctype->serialize_mutation_form(std::move(mut));
            m.set_cell(prefix, column, std::move(col_mut));
        } else {
            // for frozen maps, we're overwriting the whole cell
            if (!value) {
                m.set_cell(prefix, column, params.make_dead_cell());
            } else {
                auto v = map_type_impl::serialize_partially_deserialized_form({map_value->map.begin(), map_value->map.end()},
                        serialization_format::internal());
                m.set_cell(prefix, column, params.make_cell(std::move(v)));
            }
        }
    }

#if 0
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
