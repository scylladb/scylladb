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
 * Copyright (C) 2015-present ScyllaDB
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
#include "types/map.hh"
#include "types/list.hh"
#include "utils/like_matcher.hh"
#include "expr/expression.hh"

namespace {

void validate_operation_on_durations(const abstract_type& type, cql3::expr::oper_t op) {
    using cql3::statements::request_validations::check_false;

    if (is_slice(op) && type.references_duration()) {
        check_false(type.is_collection(), "Slice conditions are not supported on collections containing durations");
        check_false(type.is_tuple(), "Slice conditions are not supported on tuples containing durations");
        check_false(type.is_user_type(), "Slice conditions are not supported on UDTs containing durations");

        // We're a duration.
        throw exceptions::invalid_request_exception(format("Slice conditions are not supported on durations"));
    }
}

int is_satisfied_by(cql3::expr::oper_t op, const abstract_type& cell_type,
        const abstract_type& param_type, const data_value& cell_value, const bytes& param) {

        int rc;
        // For multi-cell sets and lists, cell value is represented as a map,
        // thanks to collections_as_maps flag in partition_slice. param, however,
        // is represented as a set or list type.
        // We must implement an own compare of two different representations
        // to compare the two.
        if (cell_type.is_map() && cell_type.is_multi_cell() && param_type.is_listlike()) {
            const listlike_collection_type_impl& list_type = static_cast<const listlike_collection_type_impl&>(param_type);
            const map_type_impl& map_type = static_cast<const map_type_impl&>(cell_type);
            assert(list_type.is_multi_cell());
            // Inverse comparison result since the order of arguments is inverse.
            rc = -list_type.compare_with_map(map_type, param, map_type.decompose(cell_value));
        } else {
            rc = cell_type.compare(cell_type.decompose(cell_value), param);
        }
        switch (op) {
            using cql3::expr::oper_t;
        case oper_t::EQ:
            return rc == 0;
        case oper_t::NEQ:
            return rc != 0;
        case oper_t::GTE:
            return rc >= 0;
        case oper_t::LTE:
            return rc <= 0;
        case oper_t::GT:
            return rc > 0;
        case oper_t::LT:
            return rc < 0;
        default:
            assert(false);
            return false;
        }
}

// Read the list index from key and check that list index is not
// negative. The negative range check repeats Cassandra behaviour.
uint32_t read_and_check_list_index(const cql3::raw_value_view& key) {
    // The list element type is always int32_type, see lists::index_spec_of
    int32_t idx = read_simple_exactly<int32_t>(to_bytes(key));
    if (idx < 0) {
        throw exceptions::invalid_request_exception(format("Invalid negative list index {}", idx));
    }
    return static_cast<uint32_t>(idx);
}

} // end of anonymous namespace

namespace cql3 {

void column_condition::collect_marker_specificaton(variable_specifications& bound_names) const {
    if (_collection_element) {
        _collection_element->collect_marker_specification(bound_names);
    }
    if (!_in_values.empty()) {
        for (auto&& value : _in_values) {
            value->collect_marker_specification(bound_names);
        }
    }
    if (_value) {
        _value->collect_marker_specification(bound_names);
    }
}

bool column_condition::applies_to(const data_value* cell_value, const query_options& options) const {

    // Cassandra condition support has a few quirks:
    // - only a simple conjunct of predicates is supported "predicate AND predicate AND ..."
    // - a predicate can operate on a column or a collection element, which must always be
    // on the right side: "a = 3" or "collection['key'] IN (1,2,3)"
    // - parameter markers are allowed on the right hand side only
    // - only <, >, >=, <=, !=, LIKE, and IN predicates are supported.
    // - NULLs and missing values are treated differently from the WHERE clause:
    // a term or cell in IF clause is allowed to be NULL or compared with NULL,
    // and NULL value is treated just like any other value in the domain (there is no
    // three-value logic or UNKNOWN like in SQL).
    // - empty sets/lists/maps are treated differently when comparing with NULLs depending on
    // whether the object is frozen or not. An empty *frozen* set/map/list is not equal to NULL.
    //  An empty *multi-cell* set/map/list is identical to NULL.
    // The code below implements these rules in a way compatible with Cassandra.

    // Use a map/list value instead of entire collection if a key is present in the predicate.
    if (_collection_element != nullptr && cell_value != nullptr) {
        // Checked in column_condition::raw::prepare()
        assert(cell_value->type()->is_collection());
        const collection_type_impl& cell_type = static_cast<const collection_type_impl&>(*cell_value->type());

        cql3::raw_value_view key = _collection_element->bind_and_get(options);
        if (key.is_unset_value()) {
            throw exceptions::invalid_request_exception(
                    format("Invalid 'unset' value in {} element access", cell_type.cql3_type_name()));
        }
        if (key.is_null()) {
            throw exceptions::invalid_request_exception(
                    format("Invalid null value for {} element access", cell_type.cql3_type_name()));
        }
        if (cell_type.is_map()) {
            // If a collection is multi-cell and not frozen, it is returned as a map even if the
            // underlying data type is "set" or "list". This is controlled by
            // partition_slice::collections_as_maps enum, which is set when preparing a read command
            // object. Representing a list as a map<timeuuid, listval> is necessary to identify the list field
            // being updated, e.g. in case of UPDATE t SET list[3] = null WHERE a = 1 IF list[3]
            // = 'key'
            const map_type_impl& map_type = static_cast<const map_type_impl&>(cell_type);
            // A map is serialized as a vector of data value pairs.
            const std::vector<std::pair<data_value, data_value>>& map = map_type.from_value(*cell_value);
            if (column.type->is_map()) {
                // We're working with a map *type*, not only map *representation*.
                key.with_linearized([&map, &map_type, &cell_value] (bytes_view key) {
                    auto end = map.end();
                    const auto& map_key_type = *map_type.get_keys_type();
                    auto less = [&map_key_type](const std::pair<data_value, data_value>& value, bytes_view key) {
                        return map_key_type.less(map_key_type.decompose(value.first), key);
                    };
                    // Map elements are sorted by key.
                    auto it = std::lower_bound(map.begin(), end, key, less);
                    if (it != end && map_key_type.equal(map_key_type.decompose(it->first), key)) {
                        cell_value = &it->second;
                    } else {
                        cell_value = nullptr;
                    }
                });
            } else if (column.type->is_list()) {
                // We're working with a list type, represented as map.
                uint32_t idx = read_and_check_list_index(key);
                cell_value = idx >= map.size() ? nullptr : &map[idx].second;
            } else {
                // Syntax like "set_column['key'] = constant" is invalid.
                assert(false);
            }
        } else if (cell_type.is_list()) {
            // This is a *frozen* list.
            const list_type_impl& list_type = static_cast<const list_type_impl&>(cell_type);
            const std::vector<data_value>& list = list_type.from_value(*cell_value);
            uint32_t idx = read_and_check_list_index(key);
            cell_value = idx >= list.size() ? nullptr : &list[idx];
        } else {
            assert(false);
        }
    }

    if (is_compare(_op)) {
        // <, >, >=, <=, !=
        cql3::raw_value_view param = _value->bind_and_get(options);

        if (param.is_unset_value()) {
            throw exceptions::invalid_request_exception("Invalid 'unset' value in condition");
        }
        if (param.is_null()) {
            if (_op == expr::oper_t::EQ) {
                return cell_value == nullptr;
            } else if (_op == expr::oper_t::NEQ) {
                return cell_value != nullptr;
            } else {
                throw exceptions::invalid_request_exception(format("Invalid comparison with null for operator \"{}\"", _op));
            }
        } else if (cell_value == nullptr) {
            // The condition parameter is not null, so only NEQ can return true
            return _op == expr::oper_t::NEQ;
        }
        // type::validate() is called by bind_and_get(), so it's safe to pass to_bytes() result
        // directly to compare.
        return is_satisfied_by(_op, *cell_value->type(), *column.type, *cell_value, to_bytes(param));
    }

    if (_op == expr::oper_t::LIKE) {
        if (cell_value == nullptr) {
            return false;
        }
        if (_matcher) {
            return (*_matcher)(bytes_view(cell_value->serialize_nonnull()));
        } else {
            auto param = _value->bind_and_get(options);  // LIKE pattern
            if (param.is_unset_value()) {
                throw exceptions::invalid_request_exception("Invalid 'unset' value in LIKE pattern");
            }
            if (param.is_null()) {
                throw exceptions::invalid_request_exception("Invalid NULL value in LIKE pattern");
            }
            like_matcher matcher(to_bytes(param));
            return matcher(bytes_view(cell_value->serialize_nonnull()));
        }
    }

    assert(_op == expr::oper_t::IN);

    // FIXME Use managed_bytes_opt
    std::vector<bytes_opt> in_values;

    if (_value) {
        auto&& lval = dynamic_pointer_cast<multi_item_terminal>(_value->bind(options));
        if (!lval) {
            throw exceptions::invalid_request_exception("Invalid null value for IN condition");
        }
        for (const managed_bytes_opt& v : lval->copy_elements()) {
            if (v) {
                in_values.push_back(to_bytes(*v));
            } else {
                in_values.push_back(std::nullopt);
            }
        }
    } else {
        for (auto&& v : _in_values) {
            in_values.emplace_back(to_bytes_opt(v->bind_and_get(options)));
        }
    }
    // If cell value is NULL, IN list must contain NULL or an empty set/list. Otherwise it must contain cell value.
    if (cell_value) {
        return std::any_of(in_values.begin(), in_values.end(), [this, cell_value] (const bytes_opt& value) {
            return value.has_value() && is_satisfied_by(expr::oper_t::EQ, *cell_value->type(), *column.type, *cell_value, *value);
        });
    } else {
        return std::any_of(in_values.begin(), in_values.end(), [] (const bytes_opt& value) { return !value.has_value() || value->empty(); });
    }
}

lw_shared_ptr<column_condition>
column_condition::raw::prepare(database& db, const sstring& keyspace, const column_definition& receiver) const {
    if (receiver.type->is_counter()) {
        throw exceptions::invalid_request_exception("Conditions on counters are not supported");
    }
    shared_ptr<term> collection_element_term;
    lw_shared_ptr<column_specification> value_spec = receiver.column_specification;

    if (_collection_element) {
        if (!receiver.type->is_collection()) {
            throw exceptions::invalid_request_exception(format("Invalid element access syntax for non-collection column {}",
                        receiver.name_as_text()));
        }
        // Pass  a correct type specification to the collection_element->prepare(), so that it can
        // later be used to validate the parameter type is compatible with receiver type.
        lw_shared_ptr<column_specification> element_spec;
        auto ctype = static_cast<const collection_type_impl*>(receiver.type.get());
        const column_specification& recv_column_spec = *receiver.column_specification;
        if (ctype->get_kind() == abstract_type::kind::list) {
            element_spec = lists::index_spec_of(recv_column_spec);
            value_spec = lists::value_spec_of(recv_column_spec);
        } else if (ctype->get_kind() == abstract_type::kind::map) {
            element_spec = maps::key_spec_of(recv_column_spec);
            value_spec = maps::value_spec_of(recv_column_spec);
        } else if (ctype->get_kind() == abstract_type::kind::set) {
            throw exceptions::invalid_request_exception(format("Invalid element access syntax for set column {}",
                        receiver.name_as_text()));
        } else {
            throw exceptions::invalid_request_exception(
                    format("Unsupported collection type {} in a condition with element access", ctype->cql3_type_name()));
        }
        collection_element_term = _collection_element->prepare(db, keyspace, element_spec);
    }

    if (is_compare(_op)) {
        validate_operation_on_durations(*receiver.type, _op);
        return column_condition::condition(receiver, collection_element_term,
                _value->prepare(db, keyspace, value_spec), nullptr, _op);
    }

    if (_op == expr::oper_t::LIKE) {
        auto literal_term = dynamic_pointer_cast<constants::literal>(_value);
        if (literal_term) {
            // Pass matcher object
            const sstring& pattern = literal_term->get_raw_text();
            return column_condition::condition(receiver, collection_element_term,
                    _value->prepare(db, keyspace, value_spec),
                    std::make_unique<like_matcher>(bytes_view(reinterpret_cast<const int8_t*>(pattern.data()), pattern.size())),
                    _op);
        } else {
            // Pass through rhs value, matcher object built on execution
            // TODO: caller should validate parametrized LIKE pattern
            return column_condition::condition(receiver, collection_element_term,
                    _value->prepare(db, keyspace, value_spec), nullptr, _op);
        }
    }

    if (_op != expr::oper_t::IN) {
        throw exceptions::invalid_request_exception(format("Unsupported operator type {} in a condition ", _op));
    }

    if (_in_marker) {
        assert(_in_values.empty());
        shared_ptr<term> multi_item_term = _in_marker->prepare(db, keyspace, value_spec);
        return column_condition::in_condition(receiver, collection_element_term, multi_item_term, {});
    }
    // Both _in_values and in _in_marker can be missing in case of empty IN list: "a IN ()"
    std::vector<::shared_ptr<term>> terms;
    terms.reserve(_in_values.size());
    for (auto&& value : _in_values) {
        terms.push_back(value->prepare(db, keyspace, value_spec));
    }
    return column_condition::in_condition(receiver, collection_element_term, {}, std::move(terms));
}

} // end of namespace cql3
