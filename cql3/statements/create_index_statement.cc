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

#include "create_index_statement.hh"
#include "validation.hh"
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "schema.hh"
#include "schema_builder.hh"

cql3::statements::create_index_statement::create_index_statement(
        ::shared_ptr<cf_name> name, ::shared_ptr<index_name> index_name,
        ::shared_ptr<index_target::raw> raw_target,
        ::shared_ptr<index_prop_defs> properties, bool if_not_exists)
        : schema_altering_statement(name), _index_name(index_name->get_idx()), _raw_target(
                raw_target), _properties(properties), _if_not_exists(
                if_not_exists) {
}

future<>
cql3::statements::create_index_statement::check_access(const service::client_state& state) {
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::ALTER);
}

void
cql3::statements::create_index_statement::validate(distributed<service::storage_proxy>& proxy
        , const service::client_state& state)
{
    auto schema = validation::validate_column_family(proxy.local().get_db().local(), keyspace(), column_family());

    if (schema->is_counter()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on counter tables");
    }

    // Added since we might as well fail fast if anyone is trying to insert java classes here...
    if (_properties->is_custom) {
        throw exceptions::invalid_request_exception("CUSTOM index not supported");
    }

    auto target = _raw_target->prepare(schema);
    auto cd = schema->get_column_definition(target->column->name());

    if (cd == nullptr) {
        throw exceptions::invalid_request_exception(sprint("No column definition found for column %s", *target->column));
    }

    bool is_map = dynamic_cast<const collection_type_impl *>(cd->type.get()) != nullptr
            && dynamic_cast<const collection_type_impl *>(cd->type.get())->is_map();
    bool is_frozen_collection = cd->type->is_collection() && !cd->type->is_multi_cell();

    if (is_frozen_collection) {
        if (target->type != index_target::target_type::full) {
            throw exceptions::invalid_request_exception(
                    sprint("Cannot create index on %s of frozen<map> column %s",
                            index_target::index_option(target->type),
                            *target->column));
        }
    } else {
        // validateNotFullIndex
        if (target->type == index_target::target_type::full) {
            throw exceptions::invalid_request_exception("full() indexes can only be created on frozen collections");
        }
        // validateIsValuesIndexIfTargetColumnNotCollection
        if (!cd->type->is_collection()
                && target->type != index_target::target_type::values) {
            throw exceptions::invalid_request_exception(
                    sprint(
                            "Cannot create index on %s of column %s; only non-frozen collections support %s indexes",
                            index_target::index_option(target->type),
                            *target->column,
                            index_target::index_option(target->type)));
        }
        // validateTargetColumnIsMapIfIndexInvolvesKeys
        if (target->type == index_target::target_type::keys
                || target->type == index_target::target_type::keys_and_values) {
            if (!is_map) {
                throw exceptions::invalid_request_exception(
                        sprint(
                                "Cannot create index on %s of column %s with non-map type",
                                index_target::index_option(target->type),
                                *target->column));

            }
        }
    }

    if (cd->idx_info.index_type != ::index_type::none) {
        auto prev_type = index_target::from_column_definition(*cd);
        if (is_map && target->type != prev_type) {
            throw exceptions::invalid_request_exception(
                    sprint(
                            "Cannot create index on %s(%s): an index on %s(%s) already exists and indexing "
                                    "a map on more than one dimension at the same time is not currently supported",
                            index_target::index_option(target->type),
                            *target->column,
                            index_target::index_option(prev_type),
                            *target->column));
        }
        if (_if_not_exists) {
            return;
        } else {
            throw exceptions::invalid_request_exception("Index already exists");
        }
    }

    _properties->validate();


    // Origin TODO: we could lift that limitation
    if ((schema->is_dense() || !schema->thrift().has_compound_comparator()) && cd->kind != column_kind::regular_column) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");
    }

    // It would be possible to support 2ndary index on static columns (but not without modifications of at least ExtendedFilter and
    // CompositesIndex) and maybe we should, but that means a query like:
    //     SELECT * FROM foo WHERE static_column = 'bar'
    // would pull the full partition every time the static column of partition is 'bar', which sounds like offering a
    // fair potential for foot-shooting, so I prefer leaving that to a follow up ticket once we have identified cases where
    // such indexing is actually useful.
    if (cd->is_static()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not allowed on static columns");
    }
    if (cd->kind == column_kind::partition_key && cd->is_on_all_components()) {
        throw exceptions::invalid_request_exception(
                sprint(
                        "Cannot create secondary index on partition key column %s",
                        *target->column));
    }
}

future<bool>
cql3::statements::create_index_statement::announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only) {
    throw std::runtime_error("Indexes are not supported yet");
    auto schema = proxy.local().get_db().local().find_schema(keyspace(), column_family());
    auto target = _raw_target->prepare(schema);

    schema_builder cfm(schema);

    auto* cd = schema->get_column_definition(target->column->name());
    index_info idx = cd->idx_info;

    if (idx.index_type != ::index_type::none && _if_not_exists) {
        return make_ready_future<bool>(false);
    }
    if (_properties->is_custom) {
        idx.index_type = index_type::custom;
        idx.index_options = _properties->get_options();
    } else if (schema->thrift().has_compound_comparator()) {
        index_options_map options;

        if (cd->type->is_collection() && cd->type->is_multi_cell()) {
            options[index_target::index_option(target->type)] = "";
        }
        idx.index_type = index_type::composites;
        idx.index_options = options;
    } else {
        idx.index_type = index_type::keys;
        idx.index_options = index_options_map();
    }

    idx.index_name = _index_name;
    cfm.add_default_index_names(proxy.local().get_db().local());

    return service::get_local_migration_manager().announce_column_family_update(
            cfm.build(), false, is_local_only).then([]() {
        return make_ready_future<bool>(true);
    });
}


