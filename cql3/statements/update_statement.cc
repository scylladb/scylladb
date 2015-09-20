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

#include "update_statement.hh"
#include "unimplemented.hh"

#include "cql3/operation_impl.hh"

namespace cql3 {

namespace statements {

void update_statement::add_update_for_key(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    if (s->is_dense()) {
        if (!prefix || (prefix.size() == 1 && prefix.components().front().empty())) {
            throw exceptions::invalid_request_exception(sprint("Missing PRIMARY KEY part %s", *s->clustering_key_columns().begin()));
        }

        // An empty name for the compact value is what we use to recognize the case where there is not column
        // outside the PK, see CreateStatement.
        if (s->compact_column().name().empty()) {
            // There is no column outside the PK. So no operation could have passed through validation
            assert(_column_operations.empty());
            constants::setter(s->compact_column(), make_shared(constants::value(bytes()))).execute(m, prefix, params);
        } else {
            // dense means we don't have a row marker, so don't accept to set only the PK. See CASSANDRA-5648.
            if (_column_operations.empty()) {
                throw exceptions::invalid_request_exception(sprint("Column %s is mandatory for this COMPACT STORAGE table", s->compact_column().name_as_text()));
            }
        }
    } else {
        // If there are static columns, there also must be clustering columns, in which
        // case empty prefix can only refer to the static row.
        bool is_static_prefix = s->has_static_columns() && !prefix;
        if (type == statement_type::INSERT && !is_static_prefix) {
            auto& row = m.partition().clustered_row(clustering_key::from_clustering_prefix(*s, prefix));
            row.apply(row_marker(params.timestamp(), params.ttl(), params.expiry()));
        }
    }

    for (auto&& update : _column_operations) {
        update->execute(m, prefix, params);
    }

    warn(unimplemented::cause::INDEXES);
#if 0
        SecondaryIndexManager indexManager = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfId).indexManager;
        if (indexManager.hasIndexes())
        {
            for (Cell cell : cf)
            {
                // Indexed values must be validated by any applicable index. See CASSANDRA-3057/4240/8081 for more details
                if (!indexManager.validate(cell))
                    throw new InvalidRequestException(String.format("Can't index column value of size %d for index %s on %s.%s",
                                                                    cell.value().remaining(),
                                                                    cfm.getColumnDefinition(cell.name()).getIndexName(),
                                                                    cfm.ksName,
                                                                    cfm.cfName));
            }
        }
    }
#endif
}

::shared_ptr<modification_statement>
update_statement::parsed_insert::prepare_internal(database& db, schema_ptr schema,
    ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs)
{
    auto stmt = ::make_shared<update_statement>(statement_type::INSERT, bound_names->size(), schema, std::move(attrs));

    // Created from an INSERT
    if (stmt->is_counter()) {
        throw exceptions::invalid_request_exception("INSERT statement are not allowed on counter tables, use UPDATE instead");
    }

    if (_column_names.size() != _column_values.size()) {
        throw exceptions::invalid_request_exception("Unmatched column names/values");
    }

    if (_column_names.empty()) {
        throw exceptions::invalid_request_exception("No columns provided to INSERT");
    }

    for (size_t i = 0; i < _column_names.size(); i++) {
        auto id = _column_names[i]->prepare_column_identifier(schema);
        auto def = get_column_definition(schema, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(sprint("Unknown identifier %s", *id));
        }

        for (size_t j = 0; j < i; j++) {
            auto other_id = _column_names[j]->prepare_column_identifier(schema);
            if (*id == *other_id) {
                throw exceptions::invalid_request_exception(sprint("Multiple definitions found for column %s", *id));
            }
        }

        auto&& value = _column_values[i];

        if (def->is_primary_key()) {
            auto t = value->prepare(db, keyspace(), def->column_specification);
            t->collect_marker_specification(bound_names);
            stmt->add_key_value(*def, std::move(t));
        } else {
            auto operation = operation::set_value(value).prepare(db, keyspace(), *def);
            operation->collect_marker_specification(bound_names);
            stmt->add_operation(std::move(operation));
        };
    }
    return stmt;
}

::shared_ptr<modification_statement>
update_statement::parsed_update::prepare_internal(database& db, schema_ptr schema,
    ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs)
{
    auto stmt = ::make_shared<update_statement>(statement_type::UPDATE, bound_names->size(), schema, std::move(attrs));

    for (auto&& entry : _updates) {
        auto id = entry.first->prepare_column_identifier(schema);
        auto def = get_column_definition(schema, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(sprint("Unknown identifier %s", *entry.first));
        }

        auto operation = entry.second->prepare(db, keyspace(), *def);
        operation->collect_marker_specification(bound_names);

        if (def->is_primary_key()) {
            throw exceptions::invalid_request_exception(sprint("PRIMARY KEY part %s found in SET part", *entry.first));
        }
        stmt->add_operation(std::move(operation));
    }

    stmt->process_where_clause(db, _where_clause, bound_names);
    return stmt;
}

}

}
