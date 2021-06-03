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
 * Copyright 2016-present ScyllaDB
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

#include <seastar/core/thread.hh>

#include "auth/service.hh"
#include "permission_altering_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/role_name.hh"
#include "cql3/selection/selection.hh"
#include "gms/feature_service.hh"

static auth::permission_set filter_applicable_permissions(const auth::permission_set& ps, const auth::resource& r) {
    auto const filtered_permissions = auth::permission_set::from_mask(ps.mask() & r.applicable_permissions().mask());

    if (!filtered_permissions) {
        throw exceptions::invalid_request_exception(
                format("Resource {} does not support any of the requested permissions.", r));
    }

    return filtered_permissions;
}

cql3::statements::permission_altering_statement::permission_altering_statement(
                auth::permission_set permissions, auth::resource resource,
                const role_name& rn)
                : _permissions(filter_applicable_permissions(permissions, resource))
                , _resource(std::move(resource))
                , _role_name(rn.to_string()) {
}

void cql3::statements::permission_altering_statement::validate(service::storage_proxy&, const service::client_state&) const {
}

future<> cql3::statements::permission_altering_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const {
    state.ensure_not_anonymous();
    maybe_correct_resource(_resource, state);

    return state.ensure_exists(_resource).then([this, &state] {
        // check that the user has AUTHORIZE permission on the resource or its parents, otherwise reject
        // GRANT/REVOKE.
        return state.ensure_has_permission({auth::permission::AUTHORIZE, _resource}).then([this, &state] {
            return do_for_each(_permissions, [this, &state](auth::permission p) {
                // TODO: how about we re-write the access check to check a set
                // right away.
                return state.ensure_has_permission({p, _resource});
            });
        });
    });
}
