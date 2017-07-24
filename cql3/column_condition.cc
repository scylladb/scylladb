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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "cql3/column_condition.hh"
#include "statements/request_validations.hh"
#include "unimplemented.hh"
#include "lists.hh"
#include "maps.hh"
#include <boost/range/algorithm_ext/push_back.hpp>

namespace {

void validate_operation_on_durations(const abstract_type& type, const cql3::operator_type& op) {
    using cql3::statements::request_validations::check_false;

    if (op.is_slice() && type.references_duration()) {
        check_false(type.is_collection(), "Slice conditions are not supported on collections containing durations");
        check_false(type.is_tuple(), "Slice conditions are not supported on tuples containing durations");
        check_false(type.is_user_type(), "Slice conditions are not supported on UDTs containing durations");

        // We're a duration.
        throw exceptions::invalid_request_exception(sprint("Slice conditions are not supported on durations"));
    }
}

}

namespace cql3 {

bool
column_condition::uses_function(const sstring& ks_name, const sstring& function_name) {
    if (bool(_collection_element) && _collection_element->uses_function(ks_name, function_name)) {
        return true;
    }
    if (bool(_value) && _value->uses_function(ks_name, function_name)) {
        return true;
    }
    if (!_in_values.empty()) {
        for (auto&& value : _in_values) {
            if (bool(value) && value->uses_function(ks_name, function_name)) {
                return true;
            }
        }
    }
    return false;
}

void column_condition::collect_marker_specificaton(::shared_ptr<variable_specifications> bound_names) {
    if (_collection_element) {
        _collection_element->collect_marker_specification(bound_names);
    }
    if (!_in_values.empty()) {
        for (auto&& value : _in_values) {
            value->collect_marker_specification(bound_names);
        }
    }
    _value->collect_marker_specification(bound_names);
}

::shared_ptr<column_condition>
column_condition::raw::prepare(database& db, const sstring& keyspace, const column_definition& receiver) {
    if (receiver.type->is_counter()) {
        throw exceptions::invalid_request_exception("Conditions on counters are not supported");
    }

    if (!_collection_element) {
        if (_op == operator_type::IN) {
            if (_in_values.empty()) { // ?
                return column_condition::in_condition(receiver, _in_marker->prepare(db, keyspace, receiver.column_specification));
            }

            std::vector<::shared_ptr<term>> terms;
            for (auto&& value : _in_values) {
                terms.push_back(value->prepare(db, keyspace, receiver.column_specification));
            }
            return column_condition::in_condition(receiver, std::move(terms));
        } else {
            validate_operation_on_durations(*receiver.type, _op);
            return column_condition::condition(receiver, _value->prepare(db, keyspace, receiver.column_specification), _op);
        }
    }

    if (!receiver.type->is_collection()) {
        throw exceptions::invalid_request_exception(sprint("Invalid element access syntax for non-collection column %s", receiver.name_as_text()));
    }

    shared_ptr<column_specification> element_spec, value_spec;
    auto ctype = static_cast<const collection_type_impl*>(receiver.type.get());
    if (&ctype->_kind == &collection_type_impl::kind::list) {
        element_spec = lists::index_spec_of(receiver.column_specification);
        value_spec = lists::value_spec_of(receiver.column_specification);
    } else if (&ctype->_kind == &collection_type_impl::kind::map) {
        element_spec = maps::key_spec_of(*receiver.column_specification);
        value_spec = maps::value_spec_of(*receiver.column_specification);
    } else if (&ctype->_kind == &collection_type_impl::kind::set) {
        throw exceptions::invalid_request_exception(sprint("Invalid element access syntax for set column %s", receiver.name()));
    } else {
        abort();
    }

    if (_op == operator_type::IN) {
        if (_in_values.empty()) {
            return column_condition::in_condition(receiver,
                    _collection_element->prepare(db, keyspace, element_spec),
                    _in_marker->prepare(db, keyspace, value_spec));
        }
        std::vector<shared_ptr<term>> terms;
        terms.reserve(_in_values.size());
        boost::push_back(terms, _in_values
                                | boost::adaptors::transformed(std::bind(&term::raw::prepare, std::placeholders::_1, std::ref(db), std::ref(keyspace), value_spec)));
        return column_condition::in_condition(receiver, _collection_element->prepare(db, keyspace, element_spec), terms);
    } else {
        validate_operation_on_durations(*receiver.type, _op);

        return column_condition::condition(receiver,
                _collection_element->prepare(db, keyspace, element_spec),
                _value->prepare(db, keyspace, value_spec),
                _op);
    }
}

}
