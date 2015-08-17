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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "term.hh"
#include "abstract_marker.hh"

namespace cql3 {

/**
 * Static helper methods and classes for tuples.
 */
class tuples {
public:
    static shared_ptr<column_specification> component_spec_of(shared_ptr<column_specification> column, size_t component) {
        return ::make_shared<column_specification>(
                column->ks_name,
                column->cf_name,
                ::make_shared<column_identifier>(sprint("%s[%d]", column->name, component), true),
                static_pointer_cast<const tuple_type_impl>(column->type)->type(component));
    }

    /**
     * A raw, literal tuple.  When prepared, this will become a Tuples.Value or Tuples.DelayedValue, depending
     * on whether the tuple holds NonTerminals.
     */
    class literal : public term::multi_column_raw {
        std::vector<shared_ptr<term::raw>> _elements;
    public:
        literal(std::vector<shared_ptr<raw>> elements)
                : _elements(std::move(elements)) {
        }
        virtual shared_ptr<term> prepare(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) override {
            validate_assignable_to(db, keyspace, receiver);
            std::vector<shared_ptr<term>> values;
            bool all_terminal = true;
            for (size_t i = 0; i < _elements.size(); ++i) {
                auto&& value = _elements[i]->prepare(db, keyspace, component_spec_of(receiver, i));
                if (dynamic_pointer_cast<non_terminal>(value)) {
                    all_terminal = false;
                }
                values.push_back(std::move(value));
            }
            delayed_value value(static_pointer_cast<const tuple_type_impl>(receiver->type), values);
            if (all_terminal) {
                return value.bind(query_options::DEFAULT);
            } else {
                return make_shared(std::move(value));
            }
        }

        virtual shared_ptr<term> prepare(database& db, const sstring& keyspace, const std::vector<shared_ptr<column_specification>>& receivers) override {
            if (_elements.size() != receivers.size()) {
                throw exceptions::invalid_request_exception(sprint("Expected %d elements in value tuple, but got %d: %s", receivers.size(), _elements.size(), *this));
            }

            std::vector<shared_ptr<term>> values;
            std::vector<data_type> types;
            bool all_terminal = true;
            for (size_t i = 0; i < _elements.size(); ++i) {
                auto&& t = _elements[i]->prepare(db, keyspace, receivers[i]);
                if (dynamic_pointer_cast<non_terminal>(t)) {
                    all_terminal = false;
                }
                values.push_back(t);
                types.push_back(receivers[i]->type);
            }
            delayed_value value(tuple_type_impl::get_instance(std::move(types)), std::move(values));
            if (all_terminal) {
                return value.bind(query_options::DEFAULT);
            } else {
                return make_shared(std::move(value));
            }
        }

    private:
        void validate_assignable_to(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
            auto tt = dynamic_pointer_cast<const tuple_type_impl>(receiver->type);
            if (!tt) {
                throw exceptions::invalid_request_exception(sprint("Invalid tuple type literal for %s of type %s", receiver->name, receiver->type->as_cql3_type()));
            }
            for (size_t i = 0; i < _elements.size(); ++i) {
                if (i >= tt->size()) {
                    throw exceptions::invalid_request_exception(sprint("Invalid tuple literal for %s: too many elements. Type %s expects %d but got %d",
                                                                    receiver->name, tt->as_cql3_type(), tt->size(), _elements.size()));
                }

                auto&& value = _elements[i];
                auto&& spec = component_spec_of(receiver, i);
                if (!assignment_testable::is_assignable(value->test_assignment(db, keyspace, spec))) {
                    throw exceptions::invalid_request_exception(sprint("Invalid tuple literal for %s: component %d is not of type %s", receiver->name, i, spec->type->as_cql3_type()));
                }
            }
        }
    public:
        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) override {
            try {
                validate_assignable_to(db, keyspace, receiver);
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            } catch (exceptions::invalid_request_exception e) {
                return assignment_testable::test_result::NOT_ASSIGNABLE;
            }
        }

        virtual sstring to_string() const override {
            return tuple_to_string(_elements);
        }
    };

    /**
     * A tuple of terminal values (e.g (123, 'abc')).
     */
    class value : public multi_item_terminal {
    public:
        std::vector<bytes_opt> _elements;
    public:
        value(std::vector<bytes_opt> elements)
                : _elements(std::move(elements)) {
        }
        value(std::vector<bytes_view_opt> elements) {
            for (auto&& e : elements) {
                _elements.push_back(e ? bytes_opt(bytes(e->begin(), e->size())) : bytes_opt());
            }
        }
        static value from_serialized(bytes_view buffer, tuple_type type) {
            return value(type->split(buffer));
        }
        virtual bytes_opt get(const query_options& options) override {
            return tuple_type_impl::build_value(_elements);
        }

        virtual std::vector<bytes_opt> get_elements() override {
            return _elements;
        }
        virtual sstring to_string() const override {
            return sprint("(%s)", join(", ", _elements));
        }
    };

    /**
     * Similar to Value, but contains at least one NonTerminal, such as a non-pure functions or bind marker.
     */
    class delayed_value : public non_terminal {
        tuple_type _type;
        std::vector<shared_ptr<term>> _elements;
    public:
        delayed_value(tuple_type type, std::vector<shared_ptr<term>> elements)
                : _type(std::move(type)), _elements(std::move(elements)) {
        }

        virtual bool contains_bind_marker() const override {
            return std::all_of(_elements.begin(), _elements.end(), std::mem_fn(&term::contains_bind_marker));
        }

        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override {
            for (auto&& term : _elements) {
                term->collect_marker_specification(bound_names);
            }
        }
    private:
        std::vector<bytes_opt> bind_internal(const query_options& options) {
            std::vector<bytes_opt> buffers;
            buffers.resize(_elements.size());
            for (size_t i = 0; i < _elements.size(); ++i) {
                buffers[i] = _elements[i]->bind_and_get(options);
                // Inside tuples, we must force the serialization of collections to v3 whatever protocol
                // version is in use since we're going to store directly that serialized value.
                if (options.get_serialization_format() != serialization_format::internal()
                        && _type->type(i)->is_collection()) {
                    if (buffers[i]) {
                        buffers[i] = static_pointer_cast<const collection_type_impl>(_type->type(i))->reserialize(
                                options.get_serialization_format(),
                                serialization_format::internal(),
                                bytes_view(*buffers[i]));
                    }
                }
            }
            return buffers;
        }

    public:
        virtual shared_ptr<terminal> bind(const query_options& options) override {
            return ::make_shared<value>(bind_internal(options));
        }

        virtual bytes_opt bind_and_get(const query_options& options) override {
            // We don't "need" that override but it saves us the allocation of a Value object if used
            return _type->build_value(bind_internal(options));
        }

#if 0
        @Override
        public String toString()
        {
            return tupleToString(elements);
        }
#endif
    };

    /**
     * A terminal value for a list of IN values that are tuples. For example: "SELECT ... WHERE (a, b, c) IN ?"
     * This is similar to Lists.Value, but allows us to keep components of the tuples in the list separate.
     */
    class in_value : public terminal {
    private:
        std::vector<std::vector<bytes_opt>> _elements;
    public:
        in_value(std::vector<std::vector<bytes_opt>> items) : _elements(std::move(items)) { }
        in_value(std::vector<std::vector<bytes_view_opt>> items) {
            _elements.reserve(items.size());
            for (auto&& tuple : items) {
                std::vector<bytes_opt> elems;
                elems.reserve(tuple.size());
                for (auto&& e : tuple) {
                    elems.emplace_back(e ? bytes_opt(bytes(e->begin(), e->end())) : bytes_opt());
                }
                _elements.emplace_back(std::move(elems));
            }
        }

        static in_value from_serialized(bytes_view value, list_type type, const query_options& options) {
            try {
                // Collections have this small hack that validate cannot be called on a serialized object,
                // but the deserialization does the validation (so we're fine).
                auto l = boost::any_cast<list_type_impl::native_type>(type->deserialize(value, options.get_serialization_format()));
                auto ttype = dynamic_pointer_cast<const tuple_type_impl>(type->get_elements_type());
                assert(ttype);

                std::vector<std::vector<bytes_view_opt>> elements;
                elements.reserve(l.size());
                for (auto&& element : l) {
                    elements.emplace_back(ttype->split(ttype->decompose(element)));
                }
                return in_value(elements);
            } catch (marshal_exception& e) {
                throw exceptions::invalid_request_exception(e.what());
            }
        }

        virtual bytes_opt get(const query_options& options) override {
            throw exceptions::unsupported_operation_exception();
        }

        std::vector<std::vector<bytes_opt>> get_split_values() const {
            return _elements;
        }

        virtual sstring to_string() const override {
            std::vector<sstring> tuples(_elements.size());
            std::transform(_elements.begin(), _elements.end(), tuples.begin(), [] (auto&& e) {
                return tuple_to_string(e);
            });
            return tuple_to_string(tuples);
        }
    };

    /**
     * A raw placeholder for a tuple of values for different multiple columns, each of which may have a different type.
     * For example, "SELECT ... WHERE (col1, col2) > ?".
     */
    class raw : public abstract_marker::raw, public term::multi_column_raw {
    public:
        using abstract_marker::raw::raw;

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, const std::vector<shared_ptr<column_specification>>& receivers) override {
            return make_shared<tuples::marker>(_bind_index, make_receiver(receivers));
        }

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override {
            throw std::runtime_error("Tuples.Raw.prepare() requires a list of receivers");
        }

        virtual sstring assignment_testable_source_context() const override {
            return abstract_marker::raw::to_string();
        }

        virtual sstring to_string() const override {
            return abstract_marker::raw::to_string();
        }
    private:
        static ::shared_ptr<column_specification> make_receiver(const std::vector<shared_ptr<column_specification>>& receivers) {
            std::vector<data_type> types;
            types.reserve(receivers.size());
            sstring in_name = "(";
            for (auto&& receiver : receivers) {
                in_name += receiver->name->text();
                if (receiver != receivers.back()) {
                    in_name += ",";
                }
                types.push_back(receiver->type);
            }
            in_name += ")";

            auto identifier = make_shared<column_identifier>(in_name, true);
            auto type = tuple_type_impl::get_instance(types);
            return make_shared<column_specification>(receivers.front()->ks_name, receivers.front()->cf_name, identifier, type);
        }
    };

    /**
     * A raw marker for an IN list of tuples, like "SELECT ... WHERE (a, b, c) IN ?"
     */
    class in_raw : public abstract_marker::raw, public term::multi_column_raw {
    public:
        using abstract_marker::raw::raw;

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, const std::vector<shared_ptr<column_specification>>& receivers) override {
            return make_shared<tuples::in_marker>(_bind_index, make_in_receiver(receivers));
        }

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override {
            throw std::runtime_error("Tuples.INRaw.prepare() requires a list of receivers");
        }

        virtual sstring assignment_testable_source_context() const override {
            return to_string();
        }

        virtual sstring to_string() const override {
            return abstract_marker::raw::to_string();
        }
    private:
        static ::shared_ptr<column_specification> make_in_receiver(const std::vector<shared_ptr<column_specification>>& receivers) {
            std::vector<data_type> types;
            types.reserve(receivers.size());
            sstring in_name = "in(";
            for (auto&& receiver : receivers) {
                in_name += receiver->name->text();
                if (receiver != receivers.back()) {
                    in_name += ",";
                }

                if (receiver->type->is_collection() && receiver->type->is_multi_cell()) {
                    throw exceptions::invalid_request_exception("Non-frozen collection columns do not support IN relations");
                }

                types.emplace_back(receiver->type);
            }
            in_name += ")";

            auto identifier = make_shared<column_identifier>(in_name, true);
            auto type = tuple_type_impl::get_instance(types);
            return make_shared<column_specification>(receivers.front()->ks_name, receivers.front()->cf_name, identifier, list_type_impl::get_instance(type, false));
        }
    };

    /**
     * Represents a marker for a single tuple, like "SELECT ... WHERE (a, b, c) > ?"
     */
    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker(bind_index, std::move(receiver))
        { }

        virtual shared_ptr<terminal> bind(const query_options& options) override {
            auto value = options.get_values().at(_bind_index);
            if (!value) {
                return nullptr;
            } else {
                auto as_tuple_type = static_pointer_cast<const tuple_type_impl>(_receiver->type);
                return make_shared(value::from_serialized(*value, as_tuple_type));
            }
        }
    };

    /**
     * Represents a marker for a set of IN values that are tuples, like "SELECT ... WHERE (a, b, c) IN ?"
     */
    class in_marker : public abstract_marker {
    public:
        in_marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker(bind_index, std::move(receiver))
        {
            assert(dynamic_pointer_cast<const list_type_impl>(receiver->type));
        }

        virtual shared_ptr<terminal> bind(const query_options& options) override {
            auto value = options.get_values().at(_bind_index);
            if (!value) {
                return nullptr;
            } else {
                auto as_list_type = static_pointer_cast<const list_type_impl>(_receiver->type);
                return make_shared(in_value::from_serialized(*value, as_list_type, options));
            }
        }
    };

    template <typename T>
    static sstring tuple_to_string(const std::vector<T>& items) {
        return sprint("(%s)", join(", ", items));
    }
};

}
