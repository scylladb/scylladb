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
 * Copyright (C) 2016-present ScyllaDB
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

#include "auth/default_authorizer.hh"

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

#include <chrono>
#include <random>

#include <boost/algorithm/string/join.hpp>
#include <boost/range.hpp>
#include <seastar/core/seastar.hh>

#include "auth/authenticated_user.hh"
#include "auth/common.hh"
#include "auth/permission.hh"
#include "auth/role_or_anonymous.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"
#include "database.hh"
#include "utils/class_registrator.hh"

namespace auth {

std::string_view default_authorizer::qualified_java_name() const {
    return "org.apache.cassandra.auth.CassandraAuthorizer";
}

static constexpr std::string_view ROLE_NAME = "role";
static constexpr std::string_view RESOURCE_NAME = "resource";
static constexpr std::string_view PERMISSIONS_NAME = "permissions";
static constexpr std::string_view PERMISSIONS_CF = "role_permissions";

static logging::logger alogger("default_authorizer");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
        authorizer,
        default_authorizer,
        cql3::query_processor&,
        ::service::migration_manager&> password_auth_reg("org.apache.cassandra.auth.CassandraAuthorizer");

default_authorizer::default_authorizer(cql3::query_processor& qp, ::service::migration_manager& mm)
        : _qp(qp)
        , _migration_manager(mm) {
}

default_authorizer::~default_authorizer() {
}

static const sstring legacy_table_name{"permissions"};

bool default_authorizer::legacy_metadata_exists() const {
    return _qp.db().has_schema(meta::AUTH_KS, legacy_table_name);
}

future<bool> default_authorizer::any_granted() const {
    static const sstring query = format("SELECT * FROM {}.{} LIMIT 1", meta::AUTH_KS, PERMISSIONS_CF);

    return _qp.execute_internal(
            query,
            db::consistency_level::LOCAL_ONE,
            {},
            true).then([this](::shared_ptr<cql3::untyped_result_set> results) {
        return !results->empty();
    });
}

future<> default_authorizer::migrate_legacy_metadata() const {
    alogger.info("Starting migration of legacy permissions metadata.");
    static const sstring query = format("SELECT * FROM {}.{}", meta::AUTH_KS, legacy_table_name);

    return _qp.execute_internal(
            query,
            db::consistency_level::LOCAL_ONE).then([this](::shared_ptr<cql3::untyped_result_set> results) {
        return do_for_each(*results, [this](const cql3::untyped_result_set_row& row) {
            return do_with(
                    row.get_as<sstring>("username"),
                    parse_resource(row.get_as<sstring>(RESOURCE_NAME)),
                    [this, &row](const auto& username, const auto& r) {
                const permission_set perms = permissions::from_strings(row.get_set<sstring>(PERMISSIONS_NAME));
                return grant(username, perms, r);
            });
        }).finally([results] {});
    }).then([] {
        alogger.info("Finished migrating legacy permissions metadata.");
    }).handle_exception([](std::exception_ptr ep) {
        alogger.error("Encountered an error during migration!");
        std::rethrow_exception(ep);
    });
}

future<> default_authorizer::start() {
    static const sstring create_table = sprint(
            "CREATE TABLE %s.%s ("
            "%s text,"
            "%s text,"
            "%s set<text>,"
            "PRIMARY KEY(%s, %s)"
            ") WITH gc_grace_seconds=%d",
            meta::AUTH_KS,
            PERMISSIONS_CF,
            ROLE_NAME,
            RESOURCE_NAME,
            PERMISSIONS_NAME,
            ROLE_NAME,
            RESOURCE_NAME,
            90 * 24 * 60 * 60); // 3 months.

    return once_among_shards([this] {
        return create_metadata_table_if_missing(
                PERMISSIONS_CF,
                _qp,
                create_table,
                _migration_manager).then([this] {
            _finished = do_after_system_ready(_as, [this] {
                return async([this] {
                    wait_for_schema_agreement(_migration_manager, _qp.db(), _as).get0();

                    if (legacy_metadata_exists()) {
                        if (!any_granted().get0()) {
                            migrate_legacy_metadata().get0();
                            return;
                        }

                        alogger.warn("Ignoring legacy permissions metadata since role permissions exist.");
                    }
                });
            });
        });
    });
}

future<> default_authorizer::stop() {
    _as.request_abort();
    return _finished.handle_exception_type([](const sleep_aborted&) {}).handle_exception_type([](const abort_requested_exception&) {});
}

future<permission_set>
default_authorizer::authorize(const role_or_anonymous& maybe_role, const resource& r) const {
    if (is_anonymous(maybe_role)) {
        return make_ready_future<permission_set>(permissions::NONE);
    }

    static const sstring query = format("SELECT {} FROM {}.{} WHERE {} = ? AND {} = ?",
            PERMISSIONS_NAME,
            meta::AUTH_KS,
            PERMISSIONS_CF,
            ROLE_NAME,
            RESOURCE_NAME);

    return _qp.execute_internal(
            query,
            db::consistency_level::LOCAL_ONE,
            {*maybe_role.name, r.name()}).then([](::shared_ptr<cql3::untyped_result_set> results) {
        if (results->empty()) {
            return permissions::NONE;
        }

        return permissions::from_strings(results->one().get_set<sstring>(PERMISSIONS_NAME));
    });
}

future<>
default_authorizer::modify(
        std::string_view role_name,
        permission_set set,
        const resource& resource,
        std::string_view op) const {
    return do_with(
            format("UPDATE {}.{} SET {} = {} {} ? WHERE {} = ? AND {} = ?",
                    meta::AUTH_KS,
                    PERMISSIONS_CF,
                    PERMISSIONS_NAME,
                    PERMISSIONS_NAME,
                    op,
                    ROLE_NAME,
                    RESOURCE_NAME),
            [this, &role_name, set, &resource](const auto& query) {
        return _qp.execute_internal(
                query,
                db::consistency_level::ONE,
                internal_distributed_query_state(),
                {permissions::to_strings(set), sstring(role_name), resource.name()}).discard_result();
    });
}


future<> default_authorizer::grant(std::string_view role_name, permission_set set, const resource& resource) const {
    return modify(role_name, std::move(set), resource, "+");
}

future<> default_authorizer::revoke(std::string_view role_name, permission_set set, const resource& resource) const {
    return modify(role_name, std::move(set), resource, "-");
}

future<std::vector<permission_details>> default_authorizer::list_all() const {
    static const sstring query = format("SELECT {}, {}, {} FROM {}.{}",
            ROLE_NAME,
            RESOURCE_NAME,
            PERMISSIONS_NAME,
            meta::AUTH_KS,
            PERMISSIONS_CF);

    return _qp.execute_internal(
            query,
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            {},
            true).then([](::shared_ptr<cql3::untyped_result_set> results) {
        std::vector<permission_details> all_details;

        for (const auto& row : *results) {
            if (row.has(PERMISSIONS_NAME)) {
                auto role_name = row.get_as<sstring>(ROLE_NAME);
                auto resource = parse_resource(row.get_as<sstring>(RESOURCE_NAME));
                auto perms = permissions::from_strings(row.get_set<sstring>(PERMISSIONS_NAME));
                all_details.push_back(permission_details{std::move(role_name), std::move(resource), std::move(perms)});
            }
        }

        return all_details;
    });
}

future<> default_authorizer::revoke_all(std::string_view role_name) const {
    static const sstring query = format("DELETE FROM {}.{} WHERE {} = ?",
            meta::AUTH_KS,
            PERMISSIONS_CF,
            ROLE_NAME);

    return _qp.execute_internal(
            query,
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            {sstring(role_name)}).discard_result().handle_exception([role_name](auto ep) {
        try {
            std::rethrow_exception(ep);
        } catch (exceptions::request_execution_exception& e) {
            alogger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}", role_name, e);
        }
    });
}

future<> default_authorizer::revoke_all(const resource& resource) const {
    static const sstring query = format("SELECT {} FROM {}.{} WHERE {} = ? ALLOW FILTERING",
            ROLE_NAME,
            meta::AUTH_KS,
            PERMISSIONS_CF,
            RESOURCE_NAME);

    return _qp.execute_internal(
            query,
            db::consistency_level::LOCAL_ONE,
            {resource.name()}).then_wrapped([this, resource](future<::shared_ptr<cql3::untyped_result_set>> f) {
        try {
            auto res = f.get0();
            return parallel_for_each(
                    res->begin(),
                    res->end(),
                    [this, res, resource](const cql3::untyped_result_set::row& r) {
                static const sstring query = format("DELETE FROM {}.{} WHERE {} = ? AND {} = ?",
                        meta::AUTH_KS,
                        PERMISSIONS_CF,
                        ROLE_NAME,
                        RESOURCE_NAME);

                return _qp.execute_internal(
                        query,
                        db::consistency_level::LOCAL_ONE,
                        {r.get_as<sstring>(ROLE_NAME), resource.name()}).discard_result().handle_exception(
                                [resource](auto ep) {
                    try {
                        std::rethrow_exception(ep);
                    } catch (exceptions::request_execution_exception& e) {
                        alogger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", resource, e);
                    }

                });
            });
        } catch (exceptions::request_execution_exception& e) {
            alogger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", resource, e);
            return make_ready_future();
        }
    });
}

const resource_set& default_authorizer::protected_resources() const {
    static const resource_set resources({ make_data_resource(meta::AUTH_KS, PERMISSIONS_CF) });
    return resources;
}

}
