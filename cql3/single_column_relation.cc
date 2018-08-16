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

#include "cql3/single_column_relation.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "cql3/statements/request_validations.hh"
#include "cql3/cql3_type.hh"
#include "cql3/lists.hh"
#include "unimplemented.hh"

using namespace cql3::restrictions;

namespace cql3 {

::shared_ptr<term>
single_column_relation::to_term(const std::vector<::shared_ptr<column_specification>>& receivers,
                                ::shared_ptr<term::raw> raw,
                                database& db,
                                const sstring& keyspace,
                                ::shared_ptr<variable_specifications> bound_names) {
    // TODO: optimize vector away, accept single column_specification
    assert(receivers.size() == 1);
    auto term = raw->prepare(db, keyspace, receivers[0]);
    term->collect_marker_specification(bound_names);
    return term;
}

::shared_ptr<restrictions::restriction>
single_column_relation::new_EQ_restriction(database& db, schema_ptr schema, ::shared_ptr<variable_specifications> bound_names) {
    const column_definition& column_def = to_column_definition(schema, _entity);
    if (!_map_key) {
        auto term = to_term(to_receivers(schema, column_def), _value, db, schema->ks_name(), bound_names);
        return ::make_shared<single_column_restriction::EQ>(column_def, std::move(term));
    }
    auto&& receivers = to_receivers(schema, column_def);
    auto&& entry_key = to_term({receivers[0]}, _map_key, db, schema->ks_name(), bound_names);
    auto&& entry_value = to_term({receivers[1]}, _value, db, schema->ks_name(), bound_names);
    return make_shared<single_column_restriction::contains>(column_def, std::move(entry_key), std::move(entry_value));
}

::shared_ptr<restrictions::restriction>
single_column_relation::new_IN_restriction(database& db, schema_ptr schema, ::shared_ptr<variable_specifications> bound_names) {
    const column_definition& column_def = to_column_definition(schema, _entity);
    auto receivers = to_receivers(schema, column_def);
    assert(_in_values.empty() || !_value);
    if (_value) {
        auto term = to_term(receivers, _value, db, schema->ks_name(), bound_names);
        return make_shared<single_column_restriction::IN_with_marker>(column_def, dynamic_pointer_cast<lists::marker>(term));
    }
    auto terms = to_terms(receivers, _in_values, db, schema->ks_name(), bound_names);
    return ::make_shared<single_column_restriction::IN_with_values>(column_def, std::move(terms));
}

std::vector<::shared_ptr<column_specification>>
single_column_relation::to_receivers(schema_ptr schema, const column_definition& column_def)
{
    using namespace statements::request_validations;
    auto receiver = column_def.column_specification;

    if (schema->is_dense() && column_def.is_regular()) {
        throw exceptions::invalid_request_exception(sprint(
            "Predicates on the non-primary-key column (%s) of a COMPACT table are not yet supported", column_def.name_as_text()));
    }

    if (is_IN()) {
        // We only allow IN on the row key and the clustering key so far, never on non-PK columns, and this even if
        // there's an index
        // Note: for backward compatibility reason, we conside a IN of 1 value the same as a EQ, so we let that
        // slide.
        if (!column_def.is_primary_key() && !can_have_only_one_value()) {
            throw exceptions::invalid_request_exception(sprint(
                   "IN predicates on non-primary-key columns (%s) is not yet supported", column_def.name_as_text()));
        }
    }

    if (is_contains() && !receiver->type->is_collection()) {
        throw exceptions::invalid_request_exception(sprint("Cannot use CONTAINS on non-collection column \"%s\"", receiver->name));
    }

    if (is_contains_key()) {
        if (!dynamic_cast<const map_type_impl*>(receiver->type.get())) {
            throw exceptions::invalid_request_exception(sprint("Cannot use CONTAINS KEY on non-map column %s", receiver->name));
        }
    }

    if (_map_key) {
        check_false(dynamic_cast<const list_type_impl*>(receiver->type.get()), "Indexes on list entries (%s[index] = value) are not currently supported.", receiver->name);
        check_true(dynamic_cast<const map_type_impl*>(receiver->type.get()), "Column %s cannot be used as a map", receiver->name);
        check_true(receiver->type->is_multi_cell(), "Map-entry equality predicates on frozen map column %s are not supported", receiver->name);
        check_true(is_EQ(), "Only EQ relations are supported on map entries");
    }

    if (receiver->type->is_collection()) {
        // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
        check_false(receiver->type->is_multi_cell() && !is_legal_relation_for_non_frozen_collection(),
                   "Collection column '%s' (%s) cannot be restricted by a '%s' relation",
                   receiver->name,
                   receiver->type->as_cql3_type(),
                   get_operator());

        if (is_contains_key() || is_contains()) {
            receiver = make_collection_receiver(receiver, is_contains_key());
        } else if (receiver->type->is_multi_cell() && _map_key && is_EQ()) {
            return {
                make_collection_receiver(receiver, true),
                make_collection_receiver(receiver, false),
            };
        }
    }

    return {std::move(receiver)};
}

}
