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
 *
 * Copyright (C) 2015 ScyllaDB
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

#include "cql3/user_types.hh"

#include "cql3/cql3_type.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm/count.hpp>

namespace cql3 {

shared_ptr<column_specification> user_types::field_spec_of(shared_ptr<column_specification> column, size_t field) {
    auto&& ut = static_pointer_cast<const user_type_impl>(column->type);
    auto&& name = ut->field_name(field);
    auto&& sname = sstring(reinterpret_cast<const char*>(name.data()), name.size());
    return make_shared<column_specification>(
                                   column->ks_name,
                                   column->cf_name,
                                   make_shared<column_identifier>(column->name->to_string() + "." + sname, true),
                                   ut->field_type(field));
}

user_types::literal::literal(elements_map_type entries)
        : _entries(std::move(entries)) {
}

shared_ptr<term> user_types::literal::prepare(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    validate_assignable_to(db, keyspace, receiver);
    auto&& ut = static_pointer_cast<const user_type_impl>(receiver->type);
    bool all_terminal = true;
    std::vector<shared_ptr<term>> values;
    values.reserve(_entries.size());
    size_t found_values = 0;
    for (size_t i = 0; i < ut->size(); ++i) {
        auto&& field = column_identifier(to_bytes(ut->field_name(i)), utf8_type);
        auto iraw = _entries.find(field);
        shared_ptr<term::raw> raw;
        if (iraw == _entries.end()) {
            raw = cql3::constants::NULL_LITERAL;
        } else {
            raw = iraw->second;
            ++found_values;
        }
        auto&& value = raw->prepare(db, keyspace, field_spec_of(receiver, i));

        if (dynamic_cast<non_terminal*>(value.get())) {
            all_terminal = false;
        }

        values.push_back(std::move(value));
    }
    if (found_values != _entries.size()) {
        // We had some field that are not part of the type
        for (auto&& id_val : _entries) {
            auto&& id = id_val.first;
            if (!boost::range::count(ut->field_names(), id.bytes_)) {
                throw exceptions::invalid_request_exception(sprint("Unknown field '%s' in value of user defined type %s", id, ut->get_name_as_string()));
            }
        }
    }

    delayed_value value(ut, values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared(std::move(value));
    }
}

void user_types::literal::validate_assignable_to(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    auto&& ut = dynamic_pointer_cast<const user_type_impl>(receiver->type);
    if (!ut) {
        throw exceptions::invalid_request_exception(sprint("Invalid user type literal for %s of type %s", receiver->name, receiver->type->as_cql3_type()));
    }

    for (size_t i = 0; i < ut->size(); i++) {
        column_identifier field(to_bytes(ut->field_name(i)), utf8_type);
        if (_entries.count(field) == 0) {
            continue;
        }
        shared_ptr<term::raw> value = _entries[field];
        auto&& field_spec = field_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(value->test_assignment(db, keyspace, field_spec))) {
            throw exceptions::invalid_request_exception(sprint("Invalid user type literal for %s: field %s is not of type %s", receiver->name, field, field_spec->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result user_types::literal::test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    try {
        validate_assignable_to(db, keyspace, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

sstring user_types::literal::assignment_testable_source_context() const {
    return to_string();
}

sstring user_types::literal::to_string() const {
    auto kv_to_str = [] (auto&& kv) { return sprint("%s:%s", kv.first, kv.second); };
    return sprint("{%s}", ::join(", ", _entries | boost::adaptors::transformed(kv_to_str)));
}

user_types::delayed_value::delayed_value(user_type type, std::vector<shared_ptr<term>> values)
        : _type(std::move(type)), _values(std::move(values)) {
}
bool user_types::delayed_value::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return boost::algorithm::any_of(_values,
                std::bind(&term::uses_function, std::placeholders::_1, std::cref(ks_name), std::cref(function_name)));
}
bool user_types::delayed_value::contains_bind_marker() const {
    return boost::algorithm::any_of(_values, std::mem_fn(&term::contains_bind_marker));
}

void user_types::delayed_value::collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
    for (auto&& v : _values) {
        v->collect_marker_specification(bound_names);
    }
}

std::vector<cql3::raw_value> user_types::delayed_value::bind_internal(const query_options& options) {
    auto sf = options.get_cql_serialization_format();
    std::vector<cql3::raw_value> buffers;
    for (size_t i = 0; i < _type->size(); ++i) {
        const auto& value = _values[i]->bind_and_get(options);
        if (!_type->is_multi_cell() && value.is_unset_value()) {
            throw exceptions::invalid_request_exception(sprint("Invalid unset value for field '%s' of user defined type %s", _type->field_name_as_string(i), _type->get_name_as_string()));
        }
        buffers.push_back(cql3::raw_value::make_value(value));
        // Inside UDT values, we must force the serialization of collections to v3 whatever protocol
        // version is in use since we're going to store directly that serialized value.
        if (!sf.collection_format_unchanged() && _type->field_type(i)->is_collection() && buffers.back()) {
            auto&& ctype = static_pointer_cast<const collection_type_impl>(_type->field_type(i));
            buffers.back() = cql3::raw_value::make_value(
                    ctype->reserialize(sf, cql_serialization_format::latest(), bytes_view(*buffers.back())));
        }
    }
    return buffers;
}

shared_ptr<terminal> user_types::delayed_value::bind(const query_options& options) {
    return ::make_shared<constants::value>(cql3::raw_value::make_value((bind_and_get(options))));
}

cql3::raw_value_view user_types::delayed_value::bind_and_get(const query_options& options) {
    return options.make_temporary(cql3::raw_value::make_value(user_type_impl::build_value(bind_internal(options))));
}

}
