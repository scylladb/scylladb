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

#include "cql3/column_condition.hh"
#include "unimplemented.hh"

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
column_condition::raw::prepare(const sstring& keyspace, const column_definition& receiver) {
    if (receiver.type->is_counter()) {
        throw exceptions::invalid_request_exception("Conditions on counters are not supported");
    }

    if (!_collection_element) {
        if (_op == operator_type::IN) {
            if (_in_values.empty()) { // ?
                return column_condition::in_condition(receiver, _in_marker->prepare(keyspace, receiver.column_specification));
            }

            std::vector<::shared_ptr<term>> terms;
            for (auto&& value : _in_values) {
                terms.push_back(value->prepare(keyspace, receiver.column_specification));
            }
            return column_condition::in_condition(receiver, std::move(terms));
        } else {
            return column_condition::condition(receiver, _value->prepare(keyspace, receiver.column_specification), _op);
        }
    }

    if (!receiver.type->is_collection()) {
        throw exceptions::invalid_request_exception(sprint("Invalid element access syntax for non-collection column %s", receiver.name_as_text()));
    }

    fail(unimplemented::cause::COLLECTIONS);
#if 0
            ColumnSpecification elementSpec, valueSpec;
            switch ((((CollectionType)receiver.type).kind))
            {
                case LIST:
                    elementSpec = Lists.indexSpecOf(receiver);
                    valueSpec = Lists.valueSpecOf(receiver);
                    break;
                case MAP:
                    elementSpec = Maps.keySpecOf(receiver);
                    valueSpec = Maps.valueSpecOf(receiver);
                    break;
                case SET:
                    throw new InvalidRequestException(String.format("Invalid element access syntax for set column %s", receiver.name));
                default:
                    throw new AssertionError();
            }
            if (operator == Operator.IN)
            {
                if (inValues == null)
                    return ColumnCondition.inCondition(receiver, collectionElement.prepare(keyspace, elementSpec), inMarker.prepare(keyspace, valueSpec));
                List<Term> terms = new ArrayList<>(inValues.size());
                for (Term.Raw value : inValues)
                    terms.add(value.prepare(keyspace, valueSpec));
                return ColumnCondition.inCondition(receiver, collectionElement.prepare(keyspace, elementSpec), terms);
            }
            else
            {
                return ColumnCondition.condition(receiver, collectionElement.prepare(keyspace, elementSpec), value.prepare(keyspace, valueSpec), operator);
            }
#endif
}

}
