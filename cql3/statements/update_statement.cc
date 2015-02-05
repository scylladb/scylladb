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

#include "update_statement.hh"
#include "unimplemented.hh"

#include "cql3/operation_impl.hh"

namespace cql3 {

namespace statements {

void update_statement::add_update_for_key(api::mutation& m, const api::clustering_prefix& prefix, const update_parameters& params) {
    if (s->is_dense()) {
        throw std::runtime_error("Dense tables not supported yet");
#if 0
            if (prefix.isEmpty())
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s", cfm.clusteringColumns().get(0)));

            // An empty name for the compact value is what we use to recognize the case where there is not column
            // outside the PK, see CreateStatement.
            if (!cfm.compactValueColumn().name.bytes.hasRemaining())
            {
                // There is no column outside the PK. So no operation could have passed through validation
                assert updates.isEmpty();
                new Constants.Setter(cfm.compactValueColumn(), EMPTY).execute(key, cf, prefix, params);
            }
            else
            {
                // dense means we don't have a row marker, so don't accept to set only the PK. See CASSANDRA-5648.
                if (updates.isEmpty())
                    throw new InvalidRequestException(String.format("Column %s is mandatory for this COMPACT STORAGE table", cfm.compactValueColumn().name));

                for (Operation update : updates)
                    update.execute(key, cf, prefix, params);
            }
#endif
    }

    for (auto&& update : _column_operations) {
        update->execute(m, prefix, params);
    }

    unimplemented::indexes();
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
update_statement::parsed_insert::prepare_internal(schema_ptr schema,
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

        switch(def->kind) {
            case column_definition::PARTITION:
            case column_definition::CLUSTERING: {
                auto t = value->prepare(keyspace(), def->column_specification);
                t->collect_marker_specification(bound_names);
                stmt->add_key_value(*def, std::move(t));
                break;
            }
            default: {
                auto operation = operation::set_value(value).prepare(keyspace(), *def);
                operation->collect_marker_specification(bound_names);
                stmt->add_operation(std::move(operation));
                break;
            }
        };
    }
    return stmt;
}

}

}
