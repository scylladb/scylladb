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
 * Copyright 2015 ScyllaDB
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

#include "cql3/statements/alter_table_statement.hh"
#include "index/secondary_index_manager.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "validation.hh"
#include "db/config.hh"
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include "cql3/util.hh"
#include "view_info.hh"

namespace cql3 {

namespace statements {

alter_table_statement::alter_table_statement(shared_ptr<cf_name> name,
                                             type t,
                                             shared_ptr<column_identifier::raw> column_name,
                                             shared_ptr<cql3_type::raw> validator,
                                             shared_ptr<cf_prop_defs> properties,
                                             renames_type renames,
                                             bool is_static)
    : schema_altering_statement(std::move(name))
    , _type(t)
    , _raw_column_name(std::move(column_name))
    , _validator(std::move(validator))
    , _properties(std::move(properties))
    , _renames(std::move(renames))
    , _is_static(is_static)
{
}

future<> alter_table_statement::check_access(const service::client_state& state) {
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::ALTER);
}

void alter_table_statement::validate(service::storage_proxy& proxy, const service::client_state& state)
{
    // validated in announce_migration()
}

static data_type validate_alter(schema_ptr schema, const column_definition& def, const cql3_type& validator)
{
    auto type = def.type->is_reversed() && !validator.get_type()->is_reversed()
              ? reversed_type_impl::get_instance(validator.get_type())
              : validator.get_type();
    switch (def.kind) {
    case column_kind::partition_key:
        if (type->is_counter()) {
            throw exceptions::invalid_request_exception(
                    sprint("counter type is not supported for PRIMARY KEY part %s", def.name_as_text()));
        }

        if (!type->is_value_compatible_with(*def.type)) {
            throw exceptions::configuration_exception(
                    sprint("Cannot change %s from type %s to type %s: types are incompatible.",
                           def.name_as_text(),
                           def.type->as_cql3_type(),
                           validator));
        }
        break;

    case column_kind::clustering_key:
        if (!schema->is_cql3_table()) {
            throw exceptions::invalid_request_exception(
                    sprint("Cannot alter clustering column %s in a non-CQL3 table", def.name_as_text()));
        }

        // Note that CFMetaData.validateCompatibility already validate the change we're about to do. However, the error message it
        // sends is a bit cryptic for a CQL3 user, so validating here for a sake of returning a better error message
        // Do note that we need isCompatibleWith here, not just isValueCompatibleWith.
        if (!type->is_compatible_with(*def.type)) {
            throw exceptions::configuration_exception(
                    sprint("Cannot change %s from type %s to type %s: types are not order-compatible.",
                           def.name_as_text(),
                           def.type->as_cql3_type(),
                           validator));
        }
        break;

    case column_kind::regular_column:
    case column_kind::static_column:
        // Thrift allows to change a column validator so CFMetaData.validateCompatibility will let it slide
        // if we change to an incompatible type (contrarily to the comparator case). But we don't want to
        // allow it for CQL3 (see #5882) so validating it explicitly here. We only care about value compatibility
        // though since we won't compare values (except when there is an index, but that is validated by
        // ColumnDefinition already).
        if (!type->is_value_compatible_with(*def.type)) {
            throw exceptions::configuration_exception(
                    sprint("Cannot change %s from type %s to type %s: types are incompatible.",
                           def.name_as_text(),
                           def.type->as_cql3_type(),
                           validator));
        }
        break;
    }
    return type;
}

static void validate_column_rename(database& db, const schema& schema, const column_identifier& from, const column_identifier& to)
{
    auto def = schema.get_column_definition(from.name());
    if (!def) {
        throw exceptions::invalid_request_exception(sprint("Cannot rename unknown column %s in table %s", from, schema.cf_name()));
    }

    if (schema.get_column_definition(to.name())) {
        throw exceptions::invalid_request_exception(sprint("Cannot rename column %s to %s in table %s; another column of that name already exist", from, to, schema.cf_name()));
    }

    if (def->is_part_of_cell_name()) {
        throw exceptions::invalid_request_exception(sprint("Cannot rename non PRIMARY KEY part %s", from));
    }

    if (!schema.indices().empty()) {
        auto dependent_indices = db.find_column_family(schema.id()).get_index_manager().get_dependent_indices(*def);
        if (!dependent_indices.empty()) {
            auto index_names = ::join(", ", dependent_indices | boost::adaptors::transformed([](const index_metadata& im) {
                return im.name();
            }));
            throw exceptions::invalid_request_exception(
                    sprint("Cannot rename column %s because it has dependent secondary indexes (%s)", from, index_names));
        }
    }
}

future<shared_ptr<cql_transport::event::schema_change>> alter_table_statement::announce_migration(service::storage_proxy& proxy, bool is_local_only)
{
    auto& db = proxy.get_db().local();
    auto schema = validation::validate_column_family(db, keyspace(), column_family());
    if (schema->is_view()) {
        throw exceptions::invalid_request_exception("Cannot use ALTER TABLE on Materialized View");
    }

    auto cfm = schema_builder(schema);

    shared_ptr<cql3_type> validator;
    if (_validator) {
        validator = _validator->prepare(db, keyspace());
    }
    shared_ptr<column_identifier> column_name;
    const column_definition* def = nullptr;
    if (_raw_column_name) {
        column_name = _raw_column_name->prepare_column_identifier(schema);
        def = get_column_definition(schema, *column_name);
    }
    if (_properties->get_id()) {
        throw exceptions::configuration_exception("Cannot alter table id.");
    }

    auto& cf = db.find_column_family(schema);
    std::vector<view_ptr> view_updates;

    switch (_type) {
    case alter_table_statement::type::add:
    {
        assert(column_name);
        if (schema->is_dense()) {
            throw exceptions::invalid_request_exception("Cannot add new column to a COMPACT STORAGE table");
        }

        if (_is_static) {
            if (!schema->is_compound()) {
                throw exceptions::invalid_request_exception("Static columns are not allowed in COMPACT STORAGE tables");
            }
            if (!schema->clustering_key_size()) {
                throw exceptions::invalid_request_exception("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
            }
        }

        if (def) {
            if (def->is_partition_key()) {
                throw exceptions::invalid_request_exception(sprint("Invalid column name %s because it conflicts with a PRIMARY KEY part", column_name));
            } else {
                throw exceptions::invalid_request_exception(sprint("Invalid column name %s because it conflicts with an existing column", column_name));
            }
        }

        // Cannot re-add a dropped counter column. See #7831.
        if (schema->is_counter() && schema->dropped_columns().count(column_name->text())) {
            throw exceptions::invalid_request_exception(sprint("Cannot re-add previously dropped counter column %s", column_name));
        }

        auto type = validator->get_type();
        if (type->is_collection() && type->is_multi_cell()) {
            if (!schema->is_compound()) {
                throw exceptions::invalid_request_exception("Cannot use non-frozen collections with a non-composite PRIMARY KEY");
            }
            if (schema->is_super()) {
                throw exceptions::invalid_request_exception("Cannot use non-frozen collections with super column families");
            }


            // If there used to be a non-frozen collection column with the same name (that has been dropped),
            // we could still have some data using the old type, and so we can't allow adding a collection
            // with the same name unless the types are compatible (see #6276).
            auto& dropped = schema->dropped_columns();
            auto i = dropped.find(column_name->text());
            if (i != dropped.end() && i->second.type->is_collection() && i->second.type->is_multi_cell()
                    && !type->is_compatible_with(*i->second.type)) {
                throw exceptions::invalid_request_exception(sprint("Cannot add a collection with the name %s "
                    "because a collection with the same name and a different type has already been used in the past", column_name));
            }
        }

        cfm.with_column(column_name->name(), type, _is_static ? column_kind::static_column : column_kind::regular_column);

        // Adding a column to a base table always requires updating the view
        // schemas: If the view includes all columns it should include the new
        // column, but if it doesn't, it may need to include the new
        // unselected column as a virtual column. The case when it we
        // shouldn't add a virtual column is when the view has in its PK one
        // of the base's regular columns - but even in this case we need to
        // rebuild the view schema, to update the column ID.
        if (!_is_static) {
            for (auto&& view : cf.views()) {
                schema_builder builder(view);
                if (view->view_info()->include_all_columns()) {
                    builder.with_column(column_name->name(), type);
                } else if (!view->view_info()->base_non_pk_column_in_view_pk()) {
                    db::view::create_virtual_column(builder, column_name->name(), type);
                }
                view_updates.push_back(view_ptr(builder.build()));
            }
        }

        break;
    }
    case alter_table_statement::type::alter:
    {
        assert(column_name);
        if (!def) {
            throw exceptions::invalid_request_exception(sprint("Column %s was not found in table %s", column_name, column_family()));
        }

        auto type = validate_alter(schema, *def, *validator);
        // In any case, we update the column definition
        cfm.alter_column_type(column_name->name(), type);

        // We also have to validate the view types here. If we have a view which includes a column as part of
        // the clustering key, we need to make sure that it is indeed compatible.
        for (auto&& view : cf.views()) {
            auto* view_def = view->get_column_definition(column_name->name());
            if (view_def) {
                schema_builder builder(view);
                auto view_type = validate_alter(view, *view_def, *validator);
                builder.alter_column_type(column_name->name(), std::move(view_type));
                view_updates.push_back(view_ptr(builder.build()));
            }
        }
        break;
    }
    case alter_table_statement::type::drop:
    {
        assert(column_name);
        if (!schema->is_cql3_table()) {
            throw exceptions::invalid_request_exception("Cannot drop columns from a non-CQL3 table");
        }
        if (!def) {
            throw exceptions::invalid_request_exception(sprint("Column %s was not found in table %s", column_name, column_family()));
        }

        if (def->is_primary_key()) {
            throw exceptions::invalid_request_exception(sprint("Cannot drop PRIMARY KEY part %s", column_name));
        } else {
            for (auto&& column_def : boost::range::join(schema->static_columns(), schema->regular_columns())) { // find
                if (column_def.name() == column_name->name()) {
                    cfm.remove_column(column_name->name());
                    break;
                }
            }
        }

        if (!cf.views().empty()) {
            throw exceptions::invalid_request_exception(sprint(
                    "Cannot drop column %s on base table %s.%s with materialized views",
                    column_name, keyspace(), column_family()));
        }
        break;
    }

    case alter_table_statement::type::opts:
        if (!_properties) {
            throw exceptions::invalid_request_exception("ALTER COLUMNFAMILY WITH invoked, but no parameters found");
        }

        _properties->validate(db.get_config().extensions());

        if (!cf.views().empty() && _properties->get_gc_grace_seconds() == 0) {
            throw exceptions::invalid_request_exception(
                    "Cannot alter gc_grace_seconds of the base table of a "
                    "materialized view to 0, since this value is used to TTL "
                    "undelivered updates. Setting gc_grace_seconds too low might "
                    "cause undelivered updates to expire "
                    "before being replayed.");
        }

        if (schema->is_counter() && _properties->get_default_time_to_live() > 0) {
            throw exceptions::invalid_request_exception("Cannot set default_time_to_live on a table with counters");
        }

        _properties->apply_to_builder(cfm, db.get_config().extensions());
        break;

    case alter_table_statement::type::rename:
        for (auto&& entry : _renames) {
            auto from = entry.first->prepare_column_identifier(schema);
            auto to = entry.second->prepare_column_identifier(schema);

            validate_column_rename(db, *schema, *from, *to);
            cfm.rename_column(from->name(), to->name());

            // If the view includes a renamed column, it must be renamed in
            // the view table and the definition.
            for (auto&& view : cf.views()) {
                if (view->get_column_definition(from->name())) {
                    schema_builder builder(view);

                    auto view_from = entry.first->prepare_column_identifier(view);
                    auto view_to = entry.second->prepare_column_identifier(view);
                    validate_column_rename(db, *view, *view_from, *view_to);
                    builder.rename_column(view_from->name(), view_to->name());

                    auto new_where = util::rename_column_in_where_clause(
                            view->view_info()->where_clause(),
                            column_identifier::raw(view_from->text(), true),
                            column_identifier::raw(view_to->text(), true));
                    builder.with_view_info(view->view_info()->base_id(), view->view_info()->base_name(),
                            view->view_info()->include_all_columns(), std::move(new_where));

                    view_updates.push_back(view_ptr(builder.build()));
                }
            }
        }
        break;
    }

    return service::get_local_migration_manager().announce_column_family_update(cfm.build(), false, std::move(view_updates), is_local_only).then([this] {
        using namespace cql_transport;
        return make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::TABLE,
                keyspace(),
                column_family());
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::alter_table_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<alter_table_statement>(*this));
}

}

}
