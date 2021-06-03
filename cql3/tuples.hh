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
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "term.hh"
#include "abstract_marker.hh"
#include "types/tuple.hh"
#include "types/collection.hh"
#include "utils/chunked_vector.hh"

class list_type_impl;

namespace cql3 {

/**
 * Static helper methods and classes for tuples.
 */
class tuples {
public:
    static lw_shared_ptr<column_specification> component_spec_of(const column_specification& column, size_t component);

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
        virtual shared_ptr<term> prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const override;

        virtual shared_ptr<term> prepare(database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) const override;

    private:
        void validate_assignable_to(database& db, const sstring& keyspace, const column_specification& receiver) const {
            auto tt = dynamic_pointer_cast<const tuple_type_impl>(receiver.type->underlying_type());
            if (!tt) {
                throw exceptions::invalid_request_exception(format("Invalid tuple type literal for {} of type {}", receiver.name, receiver.type->as_cql3_type()));
            }
            for (size_t i = 0; i < _elements.size(); ++i) {
                if (i >= tt->size()) {
                    throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: too many elements. Type {} expects {:d} but got {:d}",
                                                                    receiver.name, tt->as_cql3_type(), tt->size(), _elements.size()));
                }

                auto&& value = _elements[i];
                auto&& spec = component_spec_of(receiver, i);
                if (!assignment_testable::is_assignable(value->test_assignment(db, keyspace, *spec))) {
                    throw exceptions::invalid_request_exception(format("Invalid tuple literal for {}: component {:d} is not of type {}", receiver.name, i, spec->type->as_cql3_type()));
                }
            }
        }
    public:
        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const override {
            try {
                validate_assignable_to(db, keyspace, receiver);
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            } catch (exceptions::invalid_request_exception& e) {
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
        std::vector<managed_bytes_opt> _elements;
    public:
        value(std::vector<managed_bytes_opt> elements)
                : _elements(std::move(elements)) {
        }
        static value from_serialized(const raw_value_view& buffer, const tuple_type_impl& type) {
          return buffer.with_value([&] (const FragmentedView auto& view) {
              return value(type.split_fragmented(view));
          });
        }
        virtual cql3::raw_value get(const query_options& options) override {
            return cql3::raw_value::make_value(tuple_type_impl::build_value_fragmented(_elements));
        }

        const std::vector<managed_bytes_opt>& get_elements() const {
            return _elements;
        }
        virtual std::vector<managed_bytes_opt> copy_elements() const override {
            return _elements;
        }
        size_t size() const {
            return _elements.size();
        }
        virtual sstring to_string() const override {
            return format("({})", join(", ", _elements));
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
            return std::any_of(_elements.begin(), _elements.end(), std::mem_fn(&term::contains_bind_marker));
        }

        virtual void collect_marker_specification(variable_specifications& bound_names) const override {
            for (auto&& term : _elements) {
                term->collect_marker_specification(bound_names);
            }
        }
    private:
        std::vector<managed_bytes_opt> bind_internal(const query_options& options) {
            std::vector<managed_bytes_opt> buffers;
            buffers.resize(_elements.size());
            for (size_t i = 0; i < _elements.size(); ++i) {
                const auto& value = _elements[i]->bind_and_get(options);
                if (value.is_unset_value()) {
                    throw exceptions::invalid_request_exception(format("Invalid unset value for tuple field number {:d}", i));
                }
                buffers[i] = to_managed_bytes_opt(value);
                // Inside tuples, we must force the serialization of collections to v3 whatever protocol
                // version is in use since we're going to store directly that serialized value.
                if (options.get_cql_serialization_format() != cql_serialization_format::internal()
                        && _type->type(i)->is_collection()) {
                    if (buffers[i]) {
                        buffers[i] = static_pointer_cast<const collection_type_impl>(_type->type(i))->reserialize(
                                options.get_cql_serialization_format(),
                                cql_serialization_format::internal(),
                                managed_bytes_view(*buffers[i]));
                    }
                }
            }
            return buffers;
        }

    public:
        virtual shared_ptr<terminal> bind(const query_options& options) override {
            return ::make_shared<value>(bind_internal(options));
        }

        virtual cql3::raw_value_view bind_and_get(const query_options& options) override {
            // We don't "need" that override but it saves us the allocation of a Value object if used
            return cql3::raw_value_view::make_temporary(cql3::raw_value::make_value(_type->build_value_fragmented(bind_internal(options))));
        }
    };

    /**
     * A terminal value for a list of IN values that are tuples. For example: "SELECT ... WHERE (a, b, c) IN ?"
     * This is similar to Lists.Value, but allows us to keep components of the tuples in the list separate.
     */
    class in_value : public terminal {
    private:
        utils::chunked_vector<std::vector<managed_bytes_opt>> _elements;
    public:
        in_value(utils::chunked_vector<std::vector<managed_bytes_opt>> items) : _elements(std::move(items)) { }

        static in_value from_serialized(const raw_value_view& value_view, const list_type_impl& type, const query_options& options);

        virtual cql3::raw_value get(const query_options& options) override {
            throw exceptions::unsupported_operation_exception();
        }

        utils::chunked_vector<std::vector<managed_bytes_opt>> get_split_values() const {
            return _elements;
        }

        virtual sstring to_string() const override {
            std::vector<sstring> tuples(_elements.size());
            std::transform(_elements.begin(), _elements.end(), tuples.begin(), &tuples::tuple_to_string<managed_bytes_opt>);
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

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) const override {
            return make_shared<tuples::marker>(_bind_index, make_receiver(receivers));
        }

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const override {
            throw std::runtime_error("Tuples.Raw.prepare() requires a list of receivers");
        }

        virtual sstring assignment_testable_source_context() const override {
            return abstract_marker::raw::to_string();
        }

        virtual sstring to_string() const override {
            return abstract_marker::raw::to_string();
        }
    private:
        static lw_shared_ptr<column_specification> make_receiver(const std::vector<lw_shared_ptr<column_specification>>& receivers) {
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

            auto identifier = ::make_shared<column_identifier>(in_name, true);
            auto type = tuple_type_impl::get_instance(types);
            return make_lw_shared<column_specification>(receivers.front()->ks_name, receivers.front()->cf_name, identifier, type);
        }
    };

    /**
     * A raw marker for an IN list of tuples, like "SELECT ... WHERE (a, b, c) IN ?"
     */
    class in_raw : public abstract_marker::raw, public term::multi_column_raw {
    public:
        using abstract_marker::raw::raw;

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, const std::vector<lw_shared_ptr<column_specification>>& receivers) const override {
            return make_shared<tuples::in_marker>(_bind_index, make_in_receiver(receivers));
        }

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const override {
            throw std::runtime_error("Tuples.INRaw.prepare() requires a list of receivers");
        }

        virtual sstring assignment_testable_source_context() const override {
            return to_string();
        }

        virtual sstring to_string() const override {
            return abstract_marker::raw::to_string();
        }
    private:
        static lw_shared_ptr<column_specification> make_in_receiver(const std::vector<lw_shared_ptr<column_specification>>& receivers);
    };

    /**
     * Represents a marker for a single tuple, like "SELECT ... WHERE (a, b, c) > ?"
     */
    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
            : abstract_marker(bind_index, std::move(receiver))
        { }

        virtual shared_ptr<terminal> bind(const query_options& options) override {
            const auto& value = options.get_value_at(_bind_index);
            if (value.is_null()) {
                return nullptr;
            } else if (value.is_unset_value()) {
                throw exceptions::invalid_request_exception(format("Invalid unset value for tuple {}", _receiver->name->text()));
            } else {
                auto& type = static_cast<const tuple_type_impl&>(*_receiver->type);
                try {
                    value.validate(type, options.get_cql_serialization_format());
                } catch (marshal_exception& e) {
                    throw exceptions::invalid_request_exception(
                            format("Exception while binding column {:s}: {:s}", _receiver->name->to_cql_string(), e.what()));
                }
                return make_shared<tuples::value>(value::from_serialized(value, type));
            }
        }
    };

    /**
     * Represents a marker for a set of IN values that are tuples, like "SELECT ... WHERE (a, b, c) IN ?"
     */
    class in_marker : public abstract_marker {
    public:
        in_marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver);

        virtual shared_ptr<terminal> bind(const query_options& options) override;
    };

    template <typename T>
    static sstring tuple_to_string(const std::vector<T>& items) {
        return format("({})", join(", ", items));
    }
};

}
