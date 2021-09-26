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
#include "cql3/column_identifier.hh"
#include "cql3/expr/expression.hh"

class list_type_impl;

namespace cql3 {

/**
 * Static helper methods and classes for tuples.
 */
class tuples {
public:
    /**
     * A tuple of terminal values (e.g (123, 'abc')).
     */
    class value : public terminal {
    public:
        std::vector<managed_bytes_opt> _elements;
    public:
        value(std::vector<managed_bytes_opt> elements, data_type my_type)
                : terminal(std::move(my_type)), _elements(std::move(elements)) {
        }
        static value from_serialized(const raw_value_view& buffer, const tuple_type_impl& type) {
          return buffer.with_value([&] (const FragmentedView auto& view) {
              return value(type.split_fragmented(view), type.shared_from_this());
          });
        }
        virtual cql3::raw_value get(const query_options& options) override {
            return cql3::raw_value::make_value(tuple_type_impl::build_value_fragmented(_elements));
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

        virtual void fill_prepare_context(prepare_context& ctx) const override {
            for (auto&& term : _elements) {
                term->fill_prepare_context(ctx);
            }
        }
    private:
        std::vector<managed_bytes_opt> bind_internal(const query_options& options) {
            std::vector<managed_bytes_opt> buffers;
            buffers.resize(_elements.size());
            for (size_t i = 0; i < _elements.size(); ++i) {
                const auto& value = expr::evaluate_to_raw_view(_elements[i], options);
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
            return ::make_shared<value>(bind_internal(options), _type);
        }

        virtual expr::expression to_expression() override;
    };

    /**
     * A terminal value for a list of IN values that are tuples. For example: "SELECT ... WHERE (a, b, c) IN ?"
     * This is similar to Lists.Value, but allows us to keep components of the tuples in the list separate.
     */
    class in_value : public terminal {
    private:
        utils::chunked_vector<std::vector<managed_bytes_opt>> _elements;
    public:
        in_value(utils::chunked_vector<std::vector<managed_bytes_opt>> items, data_type my_type)
            : terminal(std::move(my_type)), _elements(std::move(items)) { }

        static in_value from_serialized(const raw_value_view& value_view, const list_type_impl& type, const query_options& options);

        virtual cql3::raw_value get(const query_options& options) override;

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

        virtual expr::expression to_expression() override;
    };

    /**
     * Represents a marker for a set of IN values that are tuples, like "SELECT ... WHERE (a, b, c) IN ?"
     */
    class in_marker : public abstract_marker {
    public:
        in_marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver);

        virtual shared_ptr<terminal> bind(const query_options& options) override;
        virtual expr::expression to_expression() override;
    };

    template <typename T>
    static sstring tuple_to_string(const std::vector<T>& items) {
        return format("({})", join(", ", items));
    }
};

}
