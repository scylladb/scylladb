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
 * Copyright (C) 2015-present ScyllaDB
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

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/statements/cf_properties.hh"
#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/cql3_type.hh"

#include "service/migration_manager.hh"
#include "schema_fwd.hh"

#include <seastar/core/shared_ptr.hh>

#include <seastar/util/indirect.hh>
#include <unordered_map>
#include <utility>
#include <vector>
#include <set>
#include <optional>

namespace cql3 {

class query_processor;

namespace statements {

/** A <code>CREATE TABLE</code> parsed from a CQL query statement. */
class create_table_statement : public schema_altering_statement {
#if 0
    private AbstractType<?> defaultValidator;
#endif
    std::vector<data_type> _partition_key_types;
    std::vector<data_type> _clustering_key_types;
    std::vector<bytes> _key_aliases;
    std::vector<bytes> _column_aliases;
#if 0
    private ByteBuffer valueAlias;
#endif
    bool _use_compact_storage;

    using column_map_type =
        std::unordered_map<::shared_ptr<column_identifier>,
                           data_type,
                           shared_ptr_value_hash<column_identifier>,
                           shared_ptr_equal_by_value<column_identifier>>;
    using column_set_type =
        std::unordered_set<::shared_ptr<column_identifier>,
                           shared_ptr_value_hash<column_identifier>,
                           shared_ptr_equal_by_value<column_identifier>>;
    column_map_type _columns;
    column_set_type _static_columns;
    const ::shared_ptr<cf_prop_defs> _properties;
    const bool _if_not_exists;
    std::optional<utils::UUID> _id;
public:
    create_table_statement(cf_name name,
                           ::shared_ptr<cf_prop_defs> properties,
                           bool if_not_exists,
                           column_set_type static_columns,
                           const std::optional<utils::UUID>& id);

    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;

    virtual void validate(service::storage_proxy&, const service::client_state& state) const override;

    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(query_processor& qp) const override;

    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;

    virtual future<> grant_permissions_to_creator(const service::client_state&) const override;

    schema_ptr get_cf_meta_data(const database&) const;

    class raw_statement;

    friend raw_statement;
private:
    std::vector<column_definition> get_columns() const;

    void apply_properties_to(schema_builder& builder, const database&) const;

    void add_column_metadata_from_aliases(schema_builder& builder, std::vector<bytes> aliases, const std::vector<data_type>& types, column_kind kind) const;
};

class create_table_statement::raw_statement : public raw::cf_statement {
private:
    using defs_type = std::unordered_map<::shared_ptr<column_identifier>,
                                         ::shared_ptr<cql3_type::raw>,
                                         shared_ptr_value_hash<column_identifier>,
                                         shared_ptr_equal_by_value<column_identifier>>;
    defs_type _definitions;
    std::vector<std::vector<::shared_ptr<column_identifier>>> _key_aliases;
    std::vector<::shared_ptr<column_identifier>> _column_aliases;
    create_table_statement::column_set_type _static_columns;

    std::multiset<::shared_ptr<column_identifier>,
            indirect_less<::shared_ptr<column_identifier>, column_identifier::text_comparator>> _defined_names;
    bool _if_not_exists;
    cf_properties _properties;
public:
    raw_statement(cf_name name, bool if_not_exists);

    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;

    cf_properties& properties() {
        return _properties;
    }

    data_type get_type_and_remove(column_map_type& columns, ::shared_ptr<column_identifier> t);

    void add_definition(::shared_ptr<column_identifier> def, ::shared_ptr<cql3_type::raw> type, bool is_static);

    void add_key_aliases(const std::vector<::shared_ptr<column_identifier>> aliases);

    void add_column_alias(::shared_ptr<column_identifier> alias);
};

}

}
