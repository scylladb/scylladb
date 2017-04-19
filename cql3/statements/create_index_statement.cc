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
#include "prepared_statement.hh"
#include "validation.hh"
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "schema.hh"
#include "schema_builder.hh"

namespace cql3 {

namespace statements {

create_index_statement::create_index_statement(::shared_ptr<cf_name> name,
                                               ::shared_ptr<index_name> index_name,
                                               ::shared_ptr<index_target::raw> raw_target,
                                               ::shared_ptr<index_prop_defs> properties,
                                               bool if_not_exists)
    : schema_altering_statement(name)
    , _index_name(index_name->get_idx())
    , _raw_target(raw_target)
    , _properties(properties)
    , _if_not_exists(if_not_exists)
{
}

future<>
create_index_statement::check_access(const service::client_state& state) {
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::ALTER);
}

void
create_index_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state)
{
    auto schema = validation::validate_column_family(proxy.local().get_db().local(), keyspace(), column_family());

    if (schema->is_counter()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on counter tables");
    }

    if (schema->is_view()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on materialized views");
    }

    auto target = _raw_target->prepare(schema);
    auto cd = schema->get_column_definition(target->column->name());

    if (cd == nullptr) {
        throw exceptions::invalid_request_exception(sprint("No column definition found for column %s", *target->column));
    }

    // Origin TODO: we could lift that limitation
    if ((schema->is_dense() || !schema->thrift().has_compound_comparator()) && cd->kind != column_kind::regular_column) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");
    }

    if (cd->kind == column_kind::partition_key && cd->is_on_all_components()) {
        throw exceptions::invalid_request_exception(
                sprint(
                        "Cannot create secondary index on partition key column %s",
                        *target->column));
    }

    bool is_map = dynamic_cast<const collection_type_impl *>(cd->type.get()) != nullptr
            && dynamic_cast<const collection_type_impl *>(cd->type.get())->is_map();
    bool is_frozen_collection = cd->type->is_collection() && !cd->type->is_multi_cell();

    if (is_frozen_collection) {
        validate_for_frozen_collection(target);
    } else {
        validate_not_full_index(target);
        validate_is_values_index_if_target_column_not_collection(cd, target);
        validate_target_column_is_map_if_index_involves_keys(is_map, target);
    }

    if (cd->idx_info.index_type != ::index_type::none) {
        if (_if_not_exists) {
            return;
        } else {
            throw exceptions::invalid_request_exception("Index already exists");
        }
    }

    _properties->validate();
}

void create_index_statement::validate_for_frozen_collection(::shared_ptr<index_target> target) const
{
    if (target->type != index_target::target_type::full) {
        throw exceptions::invalid_request_exception(
                sprint("Cannot create index on %s of frozen<map> column %s",
                        index_target::index_option(target->type),
                        *target->column));
    }
}

void create_index_statement::validate_not_full_index(::shared_ptr<index_target> target) const
{
    if (target->type == index_target::target_type::full) {
        throw exceptions::invalid_request_exception("full() indexes can only be created on frozen collections");
    }
}

void create_index_statement::validate_is_values_index_if_target_column_not_collection(
        const column_definition* cd, ::shared_ptr<index_target> target) const
{
    if (!cd->type->is_collection()
            && target->type != index_target::target_type::values) {
        throw exceptions::invalid_request_exception(
                sprint("Cannot create index on %s of column %s; only non-frozen collections support %s indexes",
                       index_target::index_option(target->type),
                       *target->column,
                       index_target::index_option(target->type)));
    }
}

void create_index_statement::validate_target_column_is_map_if_index_involves_keys(bool is_map, ::shared_ptr<index_target> target) const
{
    if (target->type == index_target::target_type::keys
            || target->type == index_target::target_type::keys_and_values) {
        if (!is_map) {
            throw exceptions::invalid_request_exception(
                    sprint("Cannot create index on %s of column %s with non-map type",
                           index_target::index_option(target->type), *target->column));
        }
    }
}

future<bool>
create_index_statement::announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only) {
    if (!service::get_local_storage_service().cluster_supports_indexes()) {
        throw exceptions::invalid_request_exception("Index support is not enabled");
    }
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
            cfm.build(), false, {}, is_local_only).then([]() {
        return make_ready_future<bool>(true);
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
create_index_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_index_statement>(*this));
}

}

}
