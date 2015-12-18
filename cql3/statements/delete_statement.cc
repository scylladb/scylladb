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

#include "delete_statement.hh"

namespace cql3 {

namespace statements {

delete_statement::delete_statement(statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs)
        : modification_statement{type, bound_terms, std::move(s), std::move(attrs)}
{ }

bool delete_statement::require_full_clustering_key() const {
    return false;
}

void delete_statement::add_update_for_key(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    if (_column_operations.empty()) {
        m.partition().apply_delete(*s, prefix, params.make_tombstone());
        return;
    }

    if (prefix.size() < s->clustering_key_size()) {
        // In general, we can't delete specific columns if not all clustering columns have been specified.
        // However, if we delete only static columns, it's fine since we won't really use the prefix anyway.
        for (auto&& deletion : _column_operations) {
            if (!deletion->column.is_static()) {
                throw exceptions::invalid_request_exception(sprint(
                    "Primary key column '%s' must be specified in order to delete column '%s'",
                    get_first_empty_key()->name_as_text(), deletion->column.name_as_text()));
            }
        }
    }

    for (auto&& op : _column_operations) {
        op->execute(m, prefix, params);
    }
}

::shared_ptr<modification_statement>
delete_statement::parsed::prepare_internal(database& db, schema_ptr schema, ::shared_ptr<variable_specifications> bound_names,
        std::unique_ptr<attributes> attrs) {

    auto stmt = ::make_shared<delete_statement>(statement_type::DELETE, bound_names->size(), schema, std::move(attrs));

    for (auto&& deletion : _deletions) {
        auto&& id = deletion->affected_column()->prepare_column_identifier(schema);
        auto def = get_column_definition(schema, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(sprint("Unknown identifier %s", *id));
        }

        // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
        // list. However, we support having the value name for coherence with the static/sparse case
        if (def->is_primary_key()) {
            throw exceptions::invalid_request_exception(sprint("Invalid identifier %s for deletion (should not be a PRIMARY KEY part)", def->name_as_text()));
        }

        auto&& op = deletion->prepare(db, schema->ks_name(), *def);
        op->collect_marker_specification(bound_names);
        stmt->add_operation(op);
    }

    stmt->process_where_clause(db, _where_clause, std::move(bound_names));
    return stmt;
}

delete_statement::parsed::parsed(::shared_ptr<cf_name> name,
                                 ::shared_ptr<attributes::raw> attrs,
                                 std::vector<::shared_ptr<operation::raw_deletion>> deletions,
                                 std::vector<::shared_ptr<relation>> where_clause,
                                 conditions_vector conditions,
                                 bool if_exists)
    : modification_statement::parsed(std::move(name), std::move(attrs), std::move(conditions), false, if_exists)
    , _deletions(std::move(deletions))
    , _where_clause(std::move(where_clause))
{ }

}

}
