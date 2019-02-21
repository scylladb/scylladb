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

#include "update_statement.hh"
#include "raw/update_statement.hh"

#include "raw/insert_statement.hh"
#include "unimplemented.hh"

#include "cql3/operation_impl.hh"
#include "json.hh"

namespace cql3 {

namespace json_helpers {

/*
 * According to CQL3+JSON documentation names wrapped with double quotes
 * should be treated as case-sensitive, while regular strings should be
 * case-insensitive.
 */
static std::unordered_map<sstring, Json::Value> handle_case_sensitivity(Json::Value&& value_map) {
    std::unordered_map<sstring, Json::Value> case_sensitive_map;
    for (auto it = value_map.begin(); it != value_map.end(); ++it) {
#if defined(JSONCPP_VERSION_HEXA) && (JSONCPP_VERSION_HEXA >= 0x010600) // >= 1.6.0
        sstring name = it.name();
#else
        // NOTICE(sarna): Unsafe for strings with embedded null character
        sstring name = it.memberName();
#endif
        if (name.size() > 1 && *name.begin() == '"' && name.back() == '"') {
            case_sensitive_map.emplace(name.substr(1, name.size() - 2), std::move(*it));
        } else {
            std::transform(name.begin(), name.end(), name.begin(), ::tolower);
            case_sensitive_map.emplace(std::move(name), std::move(*it));
        }
    }
    return case_sensitive_map;
}

std::unordered_map<sstring, bytes_opt>
parse(const sstring& json_string, const std::vector<column_definition>& expected_receivers, cql_serialization_format sf) {
    std::unordered_map<sstring, bytes_opt> json_map;
    Json::Value raw_value_map = json::to_json_value(json_string);
    std::unordered_map<sstring, Json::Value> prepared_map = handle_case_sensitivity(std::move(raw_value_map));
    for (const auto& def : expected_receivers) {
        sstring cql_name = def.name_as_text();
        auto value_it = prepared_map.find(cql_name);
        if (value_it == prepared_map.end()) {
            continue;
        } else if (value_it->second.isNull()) {
            json_map.emplace(std::move(cql_name), bytes_opt{});
            prepared_map.erase(value_it);
        } else {
            json_map.emplace(std::move(cql_name), def.type->from_json_object(value_it->second, sf));
            prepared_map.erase(value_it);
        }
    }
    if (!prepared_map.empty()) {
        throw exceptions::invalid_request_exception(sprint("JSON values map contains unrecognized column: %s", prepared_map.begin()->first));
    }
    return json_map;
}

}

namespace statements {

update_statement::update_statement(statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, uint64_t* cql_stats_counter_ptr)
    : modification_statement{type, bound_terms, std::move(s), std::move(attrs), cql_stats_counter_ptr}
{ }

bool update_statement::require_full_clustering_key() const {
    return true;
}

bool update_statement::allow_clustering_key_slices() const {
    return false;
}

void update_statement::execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) {
    for (auto&& update : _column_operations) {
        update->execute(m, prefix, params);
    }
}

void update_statement::add_update_for_key(mutation& m, const query::clustering_range& range, const update_parameters& params, const json_cache_opt& json_cache) {
    auto prefix = range.start() ? std::move(range.start()->value()) : clustering_key_prefix::make_empty();
    if (s->is_dense()) {
        if (prefix.is_empty(*s) || prefix.components().front().empty()) {
            throw exceptions::invalid_request_exception(sprint("Missing PRIMARY KEY part %s", s->clustering_key_columns().begin()->name_as_text()));
        }
        // An empty name for the value is what we use to recognize the case where there is not column
        // outside the PK, see CreateStatement.
        // Since v3 schema we use empty_type instead, see schema.cc.
        auto rb = s->regular_begin();
        if (rb->name().empty() || rb->type == empty_type) {
            // There is no column outside the PK. So no operation could have passed through validation
            assert(_column_operations.empty());
            constants::setter(*s->regular_begin(), make_shared(constants::value(cql3::raw_value::make_value(bytes())))).execute(m, prefix, params);
        } else {
            // dense means we don't have a row marker, so don't accept to set only the PK. See CASSANDRA-5648.
            if (_column_operations.empty()) {
                throw exceptions::invalid_request_exception(sprint("Column %s is mandatory for this COMPACT STORAGE table", s->regular_begin()->name_as_text()));
            }
        }
    } else {
        // If there are static columns, there also must be clustering columns, in which
        // case empty prefix can only refer to the static row.
        bool is_static_prefix = s->has_static_columns() && prefix.is_empty(*s);
        if (type.is_insert() && !is_static_prefix && s->is_cql3_table()) {
            auto& row = m.partition().clustered_row(*s, prefix);
            row.apply(row_marker(params.timestamp(), params.ttl(), params.expiry()));
        }
    }

    execute_operations_for_key(m, prefix, params, json_cache);

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

modification_statement::json_cache_opt insert_prepared_json_statement::maybe_prepare_json_cache(const query_options& options) {
    sstring json_string = with_linearized(_term->bind_and_get(options).data().value(), [&] (bytes_view value) {
        return utf8_type->to_string(value.to_string());
    });
    return json_helpers::parse(std::move(json_string), s->all_columns(), options.get_cql_serialization_format());
}

void
insert_prepared_json_statement::execute_set_value(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, const bytes_opt& value) {
    if (!value) {
        if (column.type->is_collection()) {
            auto& k = static_pointer_cast<const collection_type_impl>(column.type)->_kind;
            if (&k == &collection_type_impl::kind::list) {
                lists::setter::execute(m, prefix, params, column, make_shared<lists::value>(lists::value(std::vector<bytes_opt>())));
            } else if (&k == &collection_type_impl::kind::set) {
                sets::setter::execute(m, prefix, params, column, make_shared<sets::value>(sets::value(std::set<bytes, serialized_compare>(serialized_compare(empty_type)))));
            } else if (&k == &collection_type_impl::kind::map) {
                maps::setter::execute(m, prefix, params, column, make_shared<maps::value>(maps::value(std::map<bytes, bytes, serialized_compare>(serialized_compare(empty_type)))));
            } else {
                throw exceptions::invalid_request_exception("Incorrect value kind in JSON INSERT statement");
            }
            return;
        }
        m.set_cell(prefix, column, std::move(operation::make_dead_cell(params)));
        return;
    } else if (!column.type->is_collection()) {
        constants::setter::execute(m, prefix, params, column, raw_value_view::make_value(fragmented_temporary_buffer::view(*value)));
        return;
    }

    auto& k = static_pointer_cast<const collection_type_impl>(column.type)->_kind;
    cql_serialization_format sf = params._options.get_cql_serialization_format();
    if (&k == &collection_type_impl::kind::list) {
        auto list_terminal = make_shared<lists::value>(lists::value::from_serialized(fragmented_temporary_buffer::view(*value), dynamic_pointer_cast<const list_type_impl>(column.type), sf));
        lists::setter::execute(m, prefix, params, column, std::move(list_terminal));
    } else if (&k == &collection_type_impl::kind::set) {
        auto set_terminal = make_shared<sets::value>(sets::value::from_serialized(fragmented_temporary_buffer::view(*value), dynamic_pointer_cast<const set_type_impl>(column.type), sf));
        sets::setter::execute(m, prefix, params, column, std::move(set_terminal));
    } else if (&k == &collection_type_impl::kind::map) {
        auto map_terminal = make_shared<maps::value>(maps::value::from_serialized(fragmented_temporary_buffer::view(*value), dynamic_pointer_cast<const map_type_impl>(column.type), sf));
        maps::setter::execute(m, prefix, params, column, std::move(map_terminal));
    } else {
        throw exceptions::invalid_request_exception("Incorrect value kind in JSON INSERT statement");
    }
}

dht::partition_range_vector
insert_prepared_json_statement::build_partition_keys(const query_options& options, const json_cache_opt& json_cache) {
    dht::partition_range_vector ranges;
    std::vector<bytes_opt> exploded;
    for (const auto& def : s->partition_key_columns()) {
        auto json_value = json_cache->at(def.name_as_text());
        if (!json_value) {
            throw exceptions::invalid_request_exception(sprint("Missing mandatory PRIMARY KEY part %s", def.name_as_text()));
        }
        exploded.emplace_back(*json_value);
    }
    auto pkey = partition_key::from_optional_exploded(*s, std::move(exploded));
    auto k = query::range<query::ring_position>::make_singular(dht::global_partitioner().decorate_key(*s, std::move(pkey)));
    ranges.emplace_back(std::move(k));
    return ranges;
}

query::clustering_row_ranges insert_prepared_json_statement::create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) {
    query::clustering_row_ranges ranges;
    std::vector<bytes_opt> exploded;
    for (const auto& def : s->clustering_key_columns()) {
        auto json_value = json_cache->at(def.name_as_text());
        if (!json_value) {
            throw exceptions::invalid_request_exception(sprint("Missing mandatory PRIMARY KEY part %s", def.name_as_text()));
        }
        exploded.emplace_back(*json_value);
    }
    auto k = query::range<clustering_key_prefix>::make_singular(clustering_key_prefix::from_optional_exploded(*s, std::move(exploded)));
    ranges.emplace_back(query::clustering_range(std::move(k)));
    return ranges;
}

void insert_prepared_json_statement::execute_operations_for_key(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const json_cache_opt& json_cache) {
    for (const auto& def : s->regular_columns()) {
        if (def.type->is_counter()) {
            throw exceptions::invalid_request_exception(sprint("Cannot set the value of counter column %s in JSON", def.name_as_text()));
        }

        auto it = json_cache->find(def.name_as_text());
        if (it != json_cache->end()) {
            execute_set_value(m, prefix, params, def, it->second);
        } else if (!_default_unset) {
            execute_set_value(m, prefix, params, def, bytes_opt{});
        }
    }
}

namespace raw {

insert_statement::insert_statement(            ::shared_ptr<cf_name> name,
                                               ::shared_ptr<attributes::raw> attrs,
                                               std::vector<::shared_ptr<column_identifier::raw>> column_names,
                                               std::vector<::shared_ptr<term::raw>> column_values,
                                               bool if_not_exists)
    : raw::modification_statement{std::move(name), std::move(attrs), conditions_vector{}, if_not_exists, false}
    , _column_names{std::move(column_names)}
    , _column_values{std::move(column_values)}
{ }

::shared_ptr<cql3::statements::modification_statement>
insert_statement::prepare_internal(database& db, schema_ptr schema,
    ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs, cql_stats& stats)
{
    auto stmt = ::make_shared<cql3::statements::update_statement>(statement_type::INSERT, bound_names->size(), schema, std::move(attrs), &stats.inserts);

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

    std::vector<::shared_ptr<relation>> relations;
    std::unordered_set<bytes> column_ids;
    for (size_t i = 0; i < _column_names.size(); i++) {
        auto&& col = _column_names[i];
        auto id = col->prepare_column_identifier(schema);
        auto def = get_column_definition(schema, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(sprint("Unknown identifier %s", *id));
        }
        if (column_ids.count(id->name())) {
            throw exceptions::invalid_request_exception(sprint("Multiple definitions found for column %s", *id));
        }
        column_ids.emplace(id->name());

        auto&& value = _column_values[i];

        if (def->is_primary_key()) {
            relations.push_back(::make_shared<single_column_relation>(col, operator_type::EQ, value));
        } else {
            auto operation = operation::set_value(value).prepare(db, keyspace(), *def);
            operation->collect_marker_specification(bound_names);
            stmt->add_operation(std::move(operation));
        };
    }
    stmt->process_where_clause(db, relations, std::move(bound_names));
    return stmt;
}

insert_json_statement::insert_json_statement(  ::shared_ptr<cf_name> name,
                                               ::shared_ptr<attributes::raw> attrs,
                                               ::shared_ptr<term::raw> json_value,
                                               bool if_not_exists,
                                               bool default_unset)
    : raw::modification_statement{name, attrs, conditions_vector{}, if_not_exists, false}
    , _name(name)
    , _attrs(attrs)
    , _json_value(json_value)
    , _if_not_exists(if_not_exists)
    , _default_unset(default_unset) { }

::shared_ptr<cql3::statements::modification_statement>
insert_json_statement::prepare_internal(database& db, schema_ptr schema,
    ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs, cql_stats& stats)
{
    assert(dynamic_pointer_cast<constants::literal>(_json_value) || dynamic_pointer_cast<abstract_marker::raw>(_json_value));
    auto json_column_placeholder = ::make_shared<column_identifier>("", true);
    auto prepared_json_value = _json_value->prepare(db, "", ::make_shared<column_specification>("", "", json_column_placeholder, utf8_type));
    prepared_json_value->collect_marker_specification(bound_names);
    return ::make_shared<cql3::statements::insert_prepared_json_statement>(bound_names->size(), schema, std::move(attrs), &stats.inserts, std::move(prepared_json_value), _default_unset);
}

update_statement::update_statement(            ::shared_ptr<cf_name> name,
                                               ::shared_ptr<attributes::raw> attrs,
                                               std::vector<std::pair<::shared_ptr<column_identifier::raw>, ::shared_ptr<operation::raw_update>>> updates,
                                               std::vector<relation_ptr> where_clause,
                                               conditions_vector conditions)
    : raw::modification_statement(std::move(name), std::move(attrs), std::move(conditions), false, false)
    , _updates(std::move(updates))
    , _where_clause(std::move(where_clause))
{ }

::shared_ptr<cql3::statements::modification_statement>
update_statement::prepare_internal(database& db, schema_ptr schema,
    ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs, cql_stats& stats)
{
    auto stmt = ::make_shared<cql3::statements::update_statement>(statement_type::UPDATE, bound_names->size(), schema, std::move(attrs), &stats.updates);

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

    stmt->process_where_clause(db, _where_clause, std::move(bound_names));
    return stmt;
}

}

}

}
