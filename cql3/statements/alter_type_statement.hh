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

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/cql3_type.hh"
#include "cql3/ut_name.hh"

namespace cql3 {

namespace statements {

class alter_type_statement : public schema_altering_statement {
protected:
    ut_name _name;
public:
    alter_type_statement(const ut_name& name);

    virtual void prepare_keyspace(const service::client_state& state) override;

    virtual future<> check_access(const service::client_state& state) override;

    virtual void validate(service::storage_proxy& proxy, const service::client_state& state) override;

    virtual const sstring& keyspace() const override;

    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(service::storage_proxy& proxy, bool is_local_only) override;

    class add_or_alter;
    class renames;
protected:
    virtual user_type make_updated_type(database& db, user_type to_update) const = 0;
private:
    void do_announce_migration(database& db, ::keyspace& ks, bool is_local_only);
};

class alter_type_statement::add_or_alter : public alter_type_statement {
    bool _is_add;
    shared_ptr<column_identifier> _field_name;
    shared_ptr<cql3_type::raw> _field_type;
public:
    add_or_alter(const ut_name& name, bool is_add,
                 const shared_ptr<column_identifier> field_name,
                 const shared_ptr<cql3_type::raw> field_type);
    virtual user_type make_updated_type(database& db, user_type to_update) const override;
    virtual std::unique_ptr<prepared> prepare(database& db, cql_stats& stats) override;
private:
    user_type do_add(database& db, user_type to_update) const;
    user_type do_alter(database& db, user_type to_update) const;
};


class alter_type_statement::renames : public alter_type_statement {
    using renames_type = std::vector<std::pair<shared_ptr<column_identifier>,
                                               shared_ptr<column_identifier>>>;
    renames_type _renames;
public:
    renames(const ut_name& name);

    void add_rename(shared_ptr<column_identifier> previous_name, shared_ptr<column_identifier> new_name);

    virtual user_type make_updated_type(database& db, user_type to_update) const override;
    virtual std::unique_ptr<prepared> prepare(database& db, cql_stats& stats) override;
};

}

}
