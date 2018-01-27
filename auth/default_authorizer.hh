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
 * Copyright (C) 2016 ScyllaDB
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

#include <functional>

#include "auth/authorizer.hh"
#include "cql3/query_processor.hh"
#include "service/migration_manager.hh"

namespace auth {

const sstring& default_authorizer_name();

class default_authorizer : public authorizer {
    cql3::query_processor& _qp;

    ::service::migration_manager& _migration_manager;

public:
    default_authorizer(cql3::query_processor&, ::service::migration_manager&);

    ~default_authorizer();

    virtual future<> start() override;

    virtual future<> stop() override;

    virtual const sstring& qualified_java_name() const override {
        return default_authorizer_name();
    }

    virtual future<permission_set> authorize(stdx::string_view, const resource&, service&) const override;

    virtual future<> grant(stdx::string_view, permission_set, const resource&) override;

    virtual future<> revoke( stdx::string_view, permission_set, const resource&) override;

    virtual future<std::vector<permission_details>>
    list(
            permission_set,
            const std::optional<resource>&,
            const std::optional<stdx::string_view>&,
            service&) const override;

    virtual future<> revoke_all(stdx::string_view) override;

    virtual future<> revoke_all(const resource&) override;

    virtual const resource_set& protected_resources() const override;

private:
    future<> modify(stdx::string_view, permission_set, const resource&, stdx::string_view);

    ///
    /// Permissions granted directly to a role, rather than those inherited.
    ///
    future<permission_set> authorize_role_directly(stdx::string_view role_name, const resource&, const service&) const;
};

} /* namespace auth */

