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

#include "schema_altering_statement.hh"
#include "index_prop_defs.hh"
#include "index_target.hh"
#include "raw/cf_statement.hh"

#include "cql3/index_name.hh"
#include "cql3/cql3_type.hh"

#include "service/migration_manager.hh"
#include "schema.hh"

#include <seastar/core/shared_ptr.hh>

#include <unordered_map>
#include <utility>
#include <vector>
#include <set>


namespace cql3 {

class query_processor;

namespace statements {

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
class create_index_statement : public schema_altering_statement {
    const sstring _index_name;
    const std::vector<::shared_ptr<index_target::raw>> _raw_targets;
    const ::shared_ptr<index_prop_defs> _properties;
    const bool _if_not_exists;
    cql_stats* _cql_stats = nullptr;

public:
    create_index_statement(cf_name name, ::shared_ptr<index_name> index_name,
            std::vector<::shared_ptr<index_target::raw>> raw_targets,
            ::shared_ptr<index_prop_defs> properties, bool if_not_exists);

    future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;
    void validate(service::storage_proxy&, const service::client_state& state) const override;
    future<::shared_ptr<cql_transport::event::schema_change>> announce_migration(query_processor&) const override;

    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
private:
    void validate_for_local_index(const schema& schema) const;
    void validate_for_frozen_collection(const index_target& target) const;
    void validate_not_full_index(const index_target& target) const;
    void validate_is_values_index_if_target_column_not_collection(const column_definition* cd,
                                                                  const index_target& target) const;
    void validate_target_column_is_map_if_index_involves_keys(bool is_map, const index_target& target) const;
    void validate_targets_for_multi_column_index(std::vector<::shared_ptr<index_target>> targets) const;
    static index_metadata make_index_metadata(const std::vector<::shared_ptr<index_target>>& targets,
                                              const sstring& name,
                                              index_metadata_kind kind,
                                              const index_options_map& options);
};

}
}
