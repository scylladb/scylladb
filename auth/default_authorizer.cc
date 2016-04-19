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

#include <unistd.h>
#include <crypt.h>
#include <random>
#include <chrono>

#include <seastar/core/reactor.hh>

#include "auth.hh"
#include "default_authorizer.hh"
#include "authenticated_user.hh"
#include "permission.hh"
#include "cql3/query_processor.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"

const sstring auth::default_authorizer::DEFAULT_AUTHORIZER_NAME(
                "org.apache.cassandra.auth.CassandraAuthorizer");

static const sstring USER_NAME = "username";
static const sstring RESOURCE_NAME = "resource";
static const sstring PERMISSIONS_NAME = "permissions";
static const sstring PERMISSIONS_CF = "permissions";

static logging::logger logger("default_authorizer");

auth::default_authorizer::default_authorizer() {
}
auth::default_authorizer::~default_authorizer() {
}

future<> auth::default_authorizer::init() {
    sstring create_table = sprint("CREATE TABLE %s.%s ("
                    "%s text,"
                    "%s text,"
                    "%s set<text>,"
                    "PRIMARY KEY(%s, %s)"
                    ") WITH gc_grace_seconds=%d", auth::auth::AUTH_KS,
                    PERMISSIONS_CF, USER_NAME, RESOURCE_NAME, PERMISSIONS_NAME,
                    USER_NAME, RESOURCE_NAME, 90 * 24 * 60 * 60); // 3 months.

    return auth::setup_table(PERMISSIONS_CF, create_table);
}


future<auth::permission_set> auth::default_authorizer::authorize(
                ::shared_ptr<authenticated_user> user, data_resource resource) const {
    return user->is_super().then([this, user, resource = std::move(resource)](bool is_super) {
        if (is_super) {
            return make_ready_future<permission_set>(permissions::ALL);
        }

        /**
         * TOOD: could create actual data type for permission (translating string<->perm),
         * but this seems overkill right now. We still must store strings so...
         */
        auto& qp = cql3::get_local_query_processor();
        auto query = sprint("SELECT %s FROM %s.%s WHERE %s = ? AND %s = ?"
                        , PERMISSIONS_NAME, auth::AUTH_KS, PERMISSIONS_CF, USER_NAME, RESOURCE_NAME);
        return qp.process(query, db::consistency_level::LOCAL_ONE, {user->name(), resource.name() })
                        .then_wrapped([=](future<::shared_ptr<cql3::untyped_result_set>> f) {
            try {
                auto res = f.get0();

                if (res->empty() || !res->one().has(PERMISSIONS_NAME)) {
                    return make_ready_future<permission_set>(permissions::NONE);
                }
                return make_ready_future<permission_set>(permissions::from_strings(res->one().get_set<sstring>(PERMISSIONS_NAME)));
            } catch (exceptions::request_execution_exception& e) {
                logger.warn("CassandraAuthorizer failed to authorize {} for {}", user->name(), resource);
                return make_ready_future<permission_set>(permissions::NONE);
            }
        });
    });
}

#include <boost/range.hpp>

future<> auth::default_authorizer::modify(
                ::shared_ptr<authenticated_user> performer, permission_set set,
                data_resource resource, sstring user, sstring op) {
    // TODO: why does this not check super user?
    auto& qp = cql3::get_local_query_processor();
    auto query = sprint("UPDATE %s.%s SET %s = %s %s ? WHERE %s = ? AND %s = ?",
                    auth::AUTH_KS, PERMISSIONS_CF, PERMISSIONS_NAME,
                    PERMISSIONS_NAME, op, USER_NAME, RESOURCE_NAME);
    return qp.process(query, db::consistency_level::ONE, {
                    permissions::to_strings(set), user, resource.name() }).discard_result();
}


future<> auth::default_authorizer::grant(
                ::shared_ptr<authenticated_user> performer, permission_set set,
                data_resource resource, sstring to) {
    return modify(std::move(performer), std::move(set), std::move(resource), std::move(to), "+");
}

future<> auth::default_authorizer::revoke(
                ::shared_ptr<authenticated_user> performer, permission_set set,
                data_resource resource, sstring from) {
    return modify(std::move(performer), std::move(set), std::move(resource), std::move(from), "-");
}

future<std::vector<auth::permission_details>> auth::default_authorizer::list(
                ::shared_ptr<authenticated_user> performer, permission_set set,
                optional<data_resource> resource, optional<sstring> user) const {
    return performer->is_super().then([this, performer, set = std::move(set), resource = std::move(resource), user = std::move(user)](bool is_super) {
        if (!is_super && (!user || performer->name() != *user)) {
            throw exceptions::unauthorized_exception(sprint("You are not authorized to view %s's permissions", user ? *user : "everyone"));
        }

        auto query = sprint("SELECT %s, %s, %s FROM %s.%s", USER_NAME, RESOURCE_NAME, PERMISSIONS_NAME, auth::AUTH_KS, PERMISSIONS_CF);
        auto& qp = cql3::get_local_query_processor();

        // Oh, look, it is a case where it does not pay off to have
        // parameters to process in an initializer list.
        future<::shared_ptr<cql3::untyped_result_set>> f = make_ready_future<::shared_ptr<cql3::untyped_result_set>>();

        if (resource && user) {
            query += sprint(" WHERE %s = ? AND %s = ?", USER_NAME, RESOURCE_NAME);
            f = qp.process(query, db::consistency_level::ONE, {*user, resource->name()});
        } else if (resource) {
            query += sprint(" WHERE %s = ? ALLOW FILTERING", RESOURCE_NAME);
            f = qp.process(query, db::consistency_level::ONE, {resource->name()});
        } else if (user) {
            query += sprint(" WHERE %s = ?", USER_NAME);
            f = qp.process(query, db::consistency_level::ONE, {*user});
        } else {
            f = qp.process(query, db::consistency_level::ONE, {});
        }

        return f.then([set](::shared_ptr<cql3::untyped_result_set> res) {
            std::vector<permission_details> result;

            for (auto& row : *res) {
                if (row.has(PERMISSIONS_NAME)) {
                    auto username = row.get_as<sstring>(USER_NAME);
                    auto resource = data_resource::from_name(row.get_as<sstring>(RESOURCE_NAME));
                    auto ps = permissions::from_strings(row.get_set<sstring>(PERMISSIONS_NAME));
                    ps = permission_set::from_mask(ps.mask() & set.mask());

                    result.emplace_back(permission_details {username, resource, ps});
                }
            }
            return make_ready_future<std::vector<permission_details>>(std::move(result));
        });
    });
}

future<> auth::default_authorizer::revoke_all(sstring dropped_user) {
    auto& qp = cql3::get_local_query_processor();
    auto query = sprint("DELETE FROM %s.%s WHERE %s = ?", auth::AUTH_KS,
                    PERMISSIONS_CF, USER_NAME);
    return qp.process(query, db::consistency_level::ONE, { dropped_user }).discard_result().handle_exception(
                    [dropped_user](auto ep) {
                        try {
                            std::rethrow_exception(ep);
                        } catch (exceptions::request_execution_exception& e) {
                            logger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}", dropped_user, e);
                        }
                    });
}

future<> auth::default_authorizer::revoke_all(data_resource resource) {
    auto& qp = cql3::get_local_query_processor();
    auto query = sprint("SELECT %s FROM %s.%s WHERE %s = ? ALLOW FILTERING",
                    USER_NAME, auth::AUTH_KS, PERMISSIONS_CF, RESOURCE_NAME);
    return qp.process(query, db::consistency_level::LOCAL_ONE, { resource.name() })
                    .then_wrapped([resource, &qp](future<::shared_ptr<cql3::untyped_result_set>> f) {
        try {
            auto res = f.get0();
            return parallel_for_each(res->begin(), res->end(), [&qp, res, resource](const cql3::untyped_result_set::row& r) {
                auto query = sprint("DELETE FROM %s.%s WHERE %s = ? AND %s = ?"
                                , auth::AUTH_KS, PERMISSIONS_CF, USER_NAME, RESOURCE_NAME);
                return qp.process(query, db::consistency_level::LOCAL_ONE, { r.get_as<sstring>(USER_NAME), resource.name() })
                                .discard_result().handle_exception([resource](auto ep) {
                    try {
                        std::rethrow_exception(ep);
                    } catch (exceptions::request_execution_exception& e) {
                        logger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", resource, e);
                    }

                });
            });
        } catch (exceptions::request_execution_exception& e) {
            logger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", resource, e);
            return make_ready_future();
        }
    });
}


const auth::resource_ids& auth::default_authorizer::protected_resources() {
    static const resource_ids ids({ data_resource(auth::AUTH_KS, PERMISSIONS_CF) });
    return ids;
}

future<> auth::default_authorizer::validate_configuration() const {
    return make_ready_future();
}
