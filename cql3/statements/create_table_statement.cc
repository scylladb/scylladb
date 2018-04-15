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


#include <inttypes.h>
#include <regex>

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/adjacent_find.hpp>

#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/prepared_statement.hh"

#include "auth/resource.hh"
#include "auth/service.hh"
#include "schema_builder.hh"
#include "service/storage_service.hh"

namespace cql3 {

namespace statements {

create_table_statement::create_table_statement(::shared_ptr<cf_name> name,
                                               ::shared_ptr<cf_prop_defs> properties,
                                               bool if_not_exists,
                                               column_set_type static_columns,
                                               const stdx::optional<utils::UUID>& id)
    : schema_altering_statement{name}
    , _use_compact_storage(false)
    , _static_columns{static_columns}
    , _properties{properties}
    , _if_not_exists{if_not_exists}
    , _id(id)
{
}

future<> create_table_statement::check_access(const service::client_state& state) {
    return state.has_keyspace_access(keyspace(), auth::permission::CREATE);
}

void create_table_statement::validate(service::storage_proxy&, const service::client_state& state) {
    // validated in announceMigration()
}

// Column definitions
std::vector<column_definition> create_table_statement::get_columns()
{
    std::vector<column_definition> column_defs;
    for (auto&& col : _columns) {
        column_kind kind = column_kind::regular_column;
        if (_static_columns.count(col.first)) {
            kind = column_kind::static_column;
        }
        column_defs.emplace_back(col.first->name(), col.second, kind);
    }
    return column_defs;
}

future<shared_ptr<cql_transport::event::schema_change>> create_table_statement::announce_migration(service::storage_proxy& proxy, bool is_local_only) {
    return make_ready_future<>().then([this, is_local_only, &proxy] {
        return service::get_local_migration_manager().announce_new_column_family(get_cf_meta_data(proxy.get_db().local()), is_local_only);
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            using namespace cql_transport;
            return make_shared<event::schema_change>(
                    event::schema_change::change_type::CREATED,
                    event::schema_change::target_type::TABLE,
                    this->keyspace(),
                    this->column_family());
        } catch (const exceptions::already_exists_exception& e) {
            if (_if_not_exists) {
                return ::shared_ptr<cql_transport::event::schema_change>();
            }
            throw e;
        }
    });
}

/**
 * Returns a CFMetaData instance based on the parameters parsed from this
 * <code>CREATE</code> statement, or defaults where applicable.
 *
 * @return a CFMetaData instance corresponding to the values parsed from this statement
 * @throws InvalidRequestException on failure to validate parsed parameters
 */
schema_ptr create_table_statement::get_cf_meta_data(const database& db) {
    schema_builder builder{keyspace(), column_family(), _id};
    apply_properties_to(builder, db);
    return builder.build(_use_compact_storage ? schema_builder::compact_storage::yes : schema_builder::compact_storage::no);
}

void create_table_statement::apply_properties_to(schema_builder& builder, const database& db) {
    auto&& columns = get_columns();
    for (auto&& column : columns) {
        builder.with_column(column);
    }
#if 0
    cfmd.defaultValidator(defaultValidator)
        .addAllColumnDefinitions(getColumns(cfmd))
#endif
    add_column_metadata_from_aliases(builder, _key_aliases, _partition_key_types, column_kind::partition_key);
    add_column_metadata_from_aliases(builder, _column_aliases, _clustering_key_types, column_kind::clustering_key);
#if 0
    if (valueAlias != null)
        addColumnMetadataFromAliases(cfmd, Collections.singletonList(valueAlias), defaultValidator, ColumnDefinition.Kind.COMPACT_VALUE);
#endif

    _properties->apply_to_builder(builder, db.get_config().extensions());
}

void create_table_statement::add_column_metadata_from_aliases(schema_builder& builder, std::vector<bytes> aliases, const std::vector<data_type>& types, column_kind kind)
{
    assert(aliases.size() == types.size());
    for (size_t i = 0; i < aliases.size(); i++) {
        if (!aliases[i].empty()) {
            builder.with_column(aliases[i], types[i], kind);
        }
    }
}

std::unique_ptr<prepared_statement>
create_table_statement::prepare(database& db, cql_stats& stats) {
    // Cannot happen; create_table_statement is never instantiated as a raw statement
    // (instead we instantiate create_table_statement::raw_statement)
    abort();
}

future<> create_table_statement::grant_permissions_to_creator(const service::client_state& cs) {
    return do_with(auth::make_data_resource(keyspace(), column_family()), [&cs](const auth::resource& r) {
        return auth::grant_applicable_permissions(
                *cs.get_auth_service(),
                *cs.user(),
                r).handle_exception_type([](const auth::unsupported_authorization_operation&) {
            // Nothing.
        });
    });
}

create_table_statement::raw_statement::raw_statement(::shared_ptr<cf_name> name, bool if_not_exists)
    : cf_statement{std::move(name)}
    , _if_not_exists{if_not_exists}
{ }

std::unique_ptr<prepared_statement> create_table_statement::raw_statement::prepare(database& db, cql_stats& stats) {
    // Column family name
    const sstring& cf_name = _cf_name->get_column_family();
    std::regex name_regex("\\w+");
    if (!std::regex_match(std::string(cf_name), name_regex)) {
        throw exceptions::invalid_request_exception(sprint("\"%s\" is not a valid table name (must be alphanumeric character only: [0-9A-Za-z]+)", cf_name.c_str()));
    }
    if (cf_name.size() > size_t(schema::NAME_LENGTH)) {
        throw exceptions::invalid_request_exception(sprint("Table names shouldn't be more than %d characters long (got \"%s\")", schema::NAME_LENGTH, cf_name.c_str()));
    }

    // Check for duplicate column names
    auto i = boost::range::adjacent_find(_defined_names, [] (auto&& e1, auto&& e2) {
        return e1->text() == e2->text();
    });
    if (i != _defined_names.end()) {
        throw exceptions::invalid_request_exception(sprint("Multiple definition of identifier %s", (*i)->text()));
    }

    _properties.validate(db.get_config().extensions());

    auto stmt = ::make_shared<create_table_statement>(_cf_name, _properties.properties(), _if_not_exists, _static_columns, _properties.properties()->get_id());

    std::experimental::optional<std::map<bytes, data_type>> defined_multi_cell_collections;
    for (auto&& entry : _definitions) {
        ::shared_ptr<column_identifier> id = entry.first;
        ::shared_ptr<cql3_type> pt = entry.second->prepare(db, keyspace());
        if (pt->is_counter() && !service::get_local_storage_service().cluster_supports_counters()) {
            throw exceptions::invalid_request_exception("Counter support is not enabled");
        }
        if (pt->is_collection() && pt->get_type()->is_multi_cell()) {
            if (!defined_multi_cell_collections) {
                defined_multi_cell_collections = std::map<bytes, data_type>{};
            }
            defined_multi_cell_collections->emplace(id->name(), pt->get_type());
        }
        stmt->_columns.emplace(id, pt->get_type()); // we'll remove what is not a column below
    }
    if (_key_aliases.empty()) {
        throw exceptions::invalid_request_exception("No PRIMARY KEY specifed (exactly one required)");
    } else if (_key_aliases.size() > 1) {
        throw exceptions::invalid_request_exception("Multiple PRIMARY KEYs specifed (exactly one required)");
    }

    stmt->_use_compact_storage = _properties.use_compact_storage();

    auto& key_aliases = _key_aliases[0];
    std::vector<data_type> key_types;
    for (auto&& alias : key_aliases) {
        stmt->_key_aliases.emplace_back(alias->name());
        auto t = get_type_and_remove(stmt->_columns, alias);
        if (t->is_counter()) {
            throw exceptions::invalid_request_exception(sprint("counter type is not supported for PRIMARY KEY part %s", alias->text()));
        }
        if (t->references_duration()) {
            throw exceptions::invalid_request_exception(sprint("duration type is not supported for PRIMARY KEY part %s", alias->text()));
        }
        if (_static_columns.count(alias) > 0) {
            throw exceptions::invalid_request_exception(sprint("Static column %s cannot be part of the PRIMARY KEY", alias->text()));
        }
        key_types.emplace_back(t);
    }
    stmt->_partition_key_types = key_types;

    // Handle column aliases
    if (_column_aliases.empty()) {
        if (_properties.use_compact_storage()) {
            // There should remain some column definition since it is a non-composite "static" CF
            if (stmt->_columns.empty()) {
                throw exceptions::invalid_request_exception("No definition found that is not part of the PRIMARY KEY");
            }
            if (defined_multi_cell_collections) {
                throw exceptions::invalid_request_exception("Non-frozen collection types are not supported with COMPACT STORAGE");
            }
        }
        stmt->_clustering_key_types = std::vector<data_type>{};
    } else {
        // If we use compact storage and have only one alias, it is a
        // standard "dynamic" CF, otherwise it's a composite
        if (_properties.use_compact_storage() && _column_aliases.size() == 1) {
            if (defined_multi_cell_collections) {
                throw exceptions::invalid_request_exception("Collection types are not supported with COMPACT STORAGE");
            }
            auto alias = _column_aliases[0];
            if (_static_columns.count(alias) > 0) {
                throw exceptions::invalid_request_exception(sprint("Static column %s cannot be part of the PRIMARY KEY", alias->text()));
            }
            stmt->_column_aliases.emplace_back(alias->name());
            auto at = get_type_and_remove(stmt->_columns, alias);
            if (at->is_counter()) {
                throw exceptions::invalid_request_exception(sprint("counter type is not supported for PRIMARY KEY part %s", stmt->_column_aliases[0]));
            }
            if (at->references_duration()) {
                throw exceptions::invalid_request_exception(sprint("duration type is not supported for PRIMARY KEY part %s", stmt->_column_aliases[0]));
            }
            stmt->_clustering_key_types.emplace_back(at);
        } else {
            std::vector<data_type> types;
            for (auto&& t : _column_aliases) {
                stmt->_column_aliases.emplace_back(t->name());
                auto type = get_type_and_remove(stmt->_columns, t);
                if (type->is_counter()) {
                    throw exceptions::invalid_request_exception(sprint("counter type is not supported for PRIMARY KEY part %s", t->text()));
                }
                if (type->references_duration()) {
                    throw exceptions::invalid_request_exception(sprint("duration type is not supported for PRIMARY KEY part %s", t->text()));
                }
                if (_static_columns.count(t) > 0) {
                    throw exceptions::invalid_request_exception(sprint("Static column %s cannot be part of the PRIMARY KEY", t->text()));
                }
                types.emplace_back(type);
            }

            if (_properties.use_compact_storage()) {
                if (defined_multi_cell_collections) {
                    throw exceptions::invalid_request_exception("Collection types are not supported with COMPACT STORAGE");
                }
                stmt->_clustering_key_types = types;
            } else {
                stmt->_clustering_key_types = types;
            }
        }
    }

    if (!_static_columns.empty()) {
        // Only CQL3 tables can have static columns
        if (_properties.use_compact_storage()) {
            throw exceptions::invalid_request_exception("Static columns are not supported in COMPACT STORAGE tables");
        }
        // Static columns only make sense if we have at least one clustering column. Otherwise everything is static anyway
        if (_column_aliases.empty()) {
            throw exceptions::invalid_request_exception("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
        }
    }

    if (_properties.use_compact_storage() && !stmt->_column_aliases.empty()) {
        if (stmt->_columns.empty()) {
#if 0
            // The only value we'll insert will be the empty one, so the default validator don't matter
            stmt.defaultValidator = BytesType.instance;
            // We need to distinguish between
            //   * I'm upgrading from thrift so the valueAlias is null
            //   * I've defined my table with only a PK (and the column value will be empty)
            // So, we use an empty valueAlias (rather than null) for the second case
            stmt.valueAlias = ByteBufferUtil.EMPTY_BYTE_BUFFER;
#endif
        } else {
            if (stmt->_columns.size() > 1) {
                throw exceptions::invalid_request_exception(sprint("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: %s)",
                    ::join( ", ", stmt->_columns | boost::adaptors::map_keys)));
            }
#if 0
            Map.Entry<ColumnIdentifier, AbstractType> lastEntry = stmt.columns.entrySet().iterator().next();
            stmt.defaultValidator = lastEntry.getValue();
            stmt.valueAlias = lastEntry.getKey().bytes;
            stmt.columns.remove(lastEntry.getKey());
#endif
        }
    } else {
        // For compact, we are in the "static" case, so we need at least one column defined. For non-compact however, having
        // just the PK is fine since we have CQL3 row marker.
        if (_properties.use_compact_storage() && stmt->_columns.empty()) {
            throw exceptions::invalid_request_exception("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");
        }
#if 0
        // There is no way to insert/access a column that is not defined for non-compact storage, so
        // the actual validator don't matter much (except that we want to recognize counter CF as limitation apply to them).
        stmt.defaultValidator = !stmt.columns.isEmpty() && (stmt.columns.values().iterator().next() instanceof CounterColumnType)
            ? CounterColumnType.instance
            : BytesType.instance;
#endif
    }

    // If we give a clustering order, we must explicitly do so for all aliases and in the order of the PK
    if (!_properties.defined_ordering().empty()) {
        if (_properties.defined_ordering().size() > _column_aliases.size()) {
            throw exceptions::invalid_request_exception("Only clustering key columns can be defined in CLUSTERING ORDER directive");
        }

        int i = 0;
        for (auto& pair: _properties.defined_ordering()){
            auto& id = pair.first;
            auto& c = _column_aliases.at(i);

            if (!(*id == *c)) {
                if (_properties.find_ordering_info(c)) {
                    throw exceptions::invalid_request_exception(sprint("The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key (%s must appear before %s)", c, id));
                } else {
                    throw exceptions::invalid_request_exception(sprint("Missing CLUSTERING ORDER for column %s", c));
                }
            }
            ++i;
        }
    }

    return std::make_unique<prepared>(stmt);
}

data_type create_table_statement::raw_statement::get_type_and_remove(column_map_type& columns, ::shared_ptr<column_identifier> t)
{
    auto it = columns.find(t);
    if (it == columns.end()) {
        throw exceptions::invalid_request_exception(sprint("Unknown definition %s referenced in PRIMARY KEY", t->text()));
    }
    auto type = it->second;
    if (type->is_collection() && type->is_multi_cell()) {
        throw exceptions::invalid_request_exception(sprint("Invalid collection type for PRIMARY KEY component %s", t->text()));
    }
    columns.erase(t);

    return _properties.get_reversable_type(t, type);
}

void create_table_statement::raw_statement::add_definition(::shared_ptr<column_identifier> def, ::shared_ptr<cql3_type::raw> type, bool is_static) {
    _defined_names.emplace(def);
    _definitions.emplace(def, type);
    if (is_static) {
        _static_columns.emplace(def);
    }
}

void create_table_statement::raw_statement::add_key_aliases(const std::vector<::shared_ptr<column_identifier>> aliases) {
    _key_aliases.emplace_back(aliases);
}

void create_table_statement::raw_statement::add_column_alias(::shared_ptr<column_identifier> alias) {
    _column_aliases.emplace_back(alias);
}

}

}
