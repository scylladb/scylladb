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
#include "request_validations.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/string/join.hpp>

namespace cql3 {

namespace statements {

create_index_statement::create_index_statement(::shared_ptr<cf_name> name,
                                               ::shared_ptr<index_name> index_name,
                                               std::vector<::shared_ptr<index_target::raw>> raw_targets,
                                               ::shared_ptr<index_prop_defs> properties,
                                               bool if_not_exists)
    : schema_altering_statement(name)
    , _index_name(index_name->get_idx())
    , _raw_targets(raw_targets)
    , _properties(properties)
    , _if_not_exists(if_not_exists)
{
}

future<>
create_index_statement::check_access(const service::client_state& state) {
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::ALTER);
}

void
create_index_statement::validate(service::storage_proxy& proxy, const service::client_state& state)
{
    auto& db = proxy.get_db().local();
    auto schema = validation::validate_column_family(db, keyspace(), column_family());

    if (schema->is_counter()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on counter tables");
    }

    if (schema->is_view()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on materialized views");
    }

    if (schema->is_dense()) {
        throw exceptions::invalid_request_exception(
                "Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns");
    }

    std::vector<::shared_ptr<index_target>> targets;
    for (auto& raw_target : _raw_targets) {
        targets.emplace_back(raw_target->prepare(schema));
    }

    if (targets.empty() && !_properties->is_custom) {
        throw exceptions::invalid_request_exception("Only CUSTOM indexes can be created without specifying a target column");
    }

    if (targets.size() > 1) {
        validate_targets_for_multi_column_index(targets);
    }

    for (auto& target : targets) {
        auto cd = schema->get_column_definition(target->column->name());

        if (cd == nullptr) {
            throw exceptions::invalid_request_exception(
                    sprint("No column definition found for column %s", *target->column));
        }

        //NOTICE(sarna): Should be lifted after resolving issue #2963
        if (cd->is_static()) {
            throw exceptions::invalid_request_exception("Indexing static columns is not implemented yet.");
        }

        if (cd->type->references_duration()) {
            using request_validations::check_false;
            const auto& ty = *cd->type;

            check_false(ty.is_collection(), "Secondary indexes are not supported on collections containing durations");
            check_false(ty.is_tuple(), "Secondary indexes are not supported on tuples containing durations");
            check_false(ty.is_user_type(), "Secondary indexes are not supported on UDTs containing durations");

            // We're a duration.
            throw exceptions::invalid_request_exception("Secondary indexes are not supported on duration columns");
        }

        // Origin TODO: we could lift that limitation
        if ((schema->is_dense() || !schema->thrift().has_compound_comparator()) && cd->is_primary_key()) {
            throw exceptions::invalid_request_exception(
                    "Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");
        }

        if (cd->kind == column_kind::partition_key && cd->is_on_all_components()) {
            throw exceptions::invalid_request_exception(
                    sprint(
                            "Cannot create secondary index on partition key column %s",
                            *target->column));
        }

        bool is_map = dynamic_cast<const collection_type_impl *>(cd->type.get()) != nullptr
                      && dynamic_cast<const collection_type_impl *>(cd->type.get())->is_map();
        bool is_collection = cd->type->is_collection();
        bool is_frozen_collection = is_collection && !cd->type->is_multi_cell();

        if (is_frozen_collection) {
            validate_for_frozen_collection(target);
        } else if (is_collection) {
            // NOTICE(sarna): should be lifted after #2962 (indexes on non-frozen collections) is implemented
            throw exceptions::invalid_request_exception(
                    sprint("Cannot create secondary index on non-frozen collection column %s", cd->name_as_text()));
        } else {
            validate_not_full_index(target);
            validate_is_values_index_if_target_column_not_collection(cd, target);
            validate_target_column_is_map_if_index_involves_keys(is_map, target);
        }
    }

    if (db.existing_index_names(keyspace()).count(_index_name) > 0) {
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

void create_index_statement::validate_targets_for_multi_column_index(std::vector<::shared_ptr<index_target>> targets) const
{
    if (!_properties->is_custom) {
        throw exceptions::invalid_request_exception("Only CUSTOM indexes support multiple columns");
    }
    std::unordered_set<::shared_ptr<column_identifier>> columns;
    for (auto& target : targets) {
        if (columns.count(target->column) > 0) {
            throw exceptions::invalid_request_exception(sprint("Duplicate column %s in index target list", target->column->name()));
        }
        columns.emplace(target->column);
    }
}

future<::shared_ptr<cql_transport::event::schema_change>>
create_index_statement::announce_migration(service::storage_proxy& proxy, bool is_local_only) {
    if (!service::get_local_storage_service().cluster_supports_indexes()) {
        throw exceptions::invalid_request_exception("Index support is not enabled");
    }
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(keyspace(), column_family());
    std::vector<::shared_ptr<index_target>> targets;
    for (auto& raw_target : _raw_targets) {
        targets.emplace_back(raw_target->prepare(schema));
    }
    sstring accepted_name = _index_name;
    if (accepted_name.empty()) {
        std::experimental::optional<sstring> index_name_root;
        if (targets.size() == 1) {
           index_name_root = targets[0]->column->to_string();
        }
        accepted_name = db.get_available_index_name(keyspace(), column_family(), index_name_root);
    }
    index_metadata_kind kind;
    index_options_map index_options;
    if (_properties->is_custom) {
        kind = index_metadata_kind::custom;
        index_options = _properties->get_options();
    } else {
        kind = schema->is_compound() ? index_metadata_kind::composites : index_metadata_kind::keys;
    }
    auto index = make_index_metadata(schema, targets, accepted_name, kind, index_options);
    auto existing_index = schema->find_index_noname(index);
    if (existing_index) {
        if (_if_not_exists) {
            return make_ready_future<::shared_ptr<cql_transport::event::schema_change>>(nullptr);
        } else {
            throw exceptions::invalid_request_exception(
                    sprint("Index %s is a duplicate of existing index %s", index.name(), existing_index.value().name()));
        }
    }
    ++_cql_stats->secondary_index_creates;
    schema_builder builder{schema};
    builder.with_index(index);
    return service::get_local_migration_manager().announce_column_family_update(
            builder.build(), false, {}, is_local_only).then([this]() {
        using namespace cql_transport;
        return make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::TABLE,
                keyspace(),
                column_family());
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
create_index_statement::prepare(database& db, cql_stats& stats) {
    _cql_stats = &stats;
    return std::make_unique<prepared_statement>(make_shared<create_index_statement>(*this));
}

index_metadata create_index_statement::make_index_metadata(schema_ptr schema,
                                                           const std::vector<::shared_ptr<index_target>>& targets,
                                                           const sstring& name,
                                                           index_metadata_kind kind,
                                                           const index_options_map& options)
{
    index_options_map new_options = options;
    auto target_option = boost::algorithm::join(targets | boost::adaptors::transformed(
            [schema](const auto &target) -> sstring {
                return target->as_string();
            }), ",");
    new_options.emplace(index_target::target_option_name, target_option);
    return index_metadata{name, new_options, kind};
}

}

}
