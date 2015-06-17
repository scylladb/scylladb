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

#pragma once

#include "schema_altering_statement.hh"
#include "index_prop_defs.hh"
#include "index_target.hh"
#include "cf_statement.hh"

#include "cql3/cql3_type.hh"

#include "service/migration_manager.hh"
#include "schema.hh"

#include "core/shared_ptr.hh"

#include <unordered_map>
#include <utility>
#include <vector>
#include <set>


namespace cql3 {

namespace statements {

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
class create_index_statement : public schema_altering_statement {
    const sstring _index_name;
    const ::shared_ptr<index_target::raw> _raw_target;
    const ::shared_ptr<index_prop_defs> _properties;
    const bool _if_not_exists;


public:
    create_index_statement(::shared_ptr<cf_name> name, sstring index_name,
            ::shared_ptr<index_target::raw> raw_target,
            ::shared_ptr<index_prop_defs> properties, bool if_not_exists);

    void check_access(const service::client_state& state) override;
    void validate(distributed<service::storage_proxy>&, const service::client_state& state) override;
    future<bool> announce_migration(distributed<service::storage_proxy>&, bool is_local_only) override;

    virtual shared_ptr<transport::event::schema_change> change_event() override {
        return make_shared<transport::event::schema_change>(
                transport::event::schema_change::change_type::UPDATED,
                transport::event::schema_change::target_type::TABLE, keyspace(),
                column_family());
    }
};

}
}
