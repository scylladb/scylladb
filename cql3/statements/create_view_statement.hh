/*
 * This file is part of Scylla.
 * Copyright (C) 2016 ScyllaDB
 *
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

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/cql3_type.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/relation.hh"
#include "cql3/cf_name.hh"

#include "service/migration_manager.hh"
#include "schema.hh"

#include "core/shared_ptr.hh"

#include <utility>
#include <vector>
#include <experimental/optional>

namespace cql3 {

namespace statements {

/** A <code>CREATE MATERIALIZED VIEW</code> parsed from a CQL query statement. */
class create_view_statement : public schema_altering_statement {
private:
    ::shared_ptr<cf_name> _base_name;
    std::vector<::shared_ptr<selection::raw_selector>> _select_clause;
    std::vector<::shared_ptr<relation>> _where_clause;
    std::vector<::shared_ptr<cql3::column_identifier::raw>> _partition_keys;
    std::vector<::shared_ptr<cql3::column_identifier::raw>> _clustering_keys;
    const ::shared_ptr<cf_prop_defs> _properties;
    bool _if_not_exists;

public:
    create_view_statement(
            ::shared_ptr<cf_name> view_name,
            ::shared_ptr<cf_name> base_name,
            std::vector<::shared_ptr<selection::raw_selector>> select_clause,
            std::vector<::shared_ptr<relation>> where_clause,
            std::vector<::shared_ptr<cql3::column_identifier::raw>> partition_keys,
            std::vector<::shared_ptr<cql3::column_identifier::raw>> clustering_keys,
            ::shared_ptr<cf_prop_defs> properties,
            bool if_not_exists);

    // Functions we need to override to subclass schema_altering_statement
    virtual future<> check_access(const service::client_state& state) override;
    virtual void validate(distributed<service::storage_proxy>&, const service::client_state& state) override;
    virtual future<bool> announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only) override;
    virtual shared_ptr<transport::event::schema_change> change_event() override;
    virtual shared_ptr<prepared> prepare(database& db) override;

    // FIXME: continue here. See create_table_statement.hh and CreateViewStatement.java
};

}
}
