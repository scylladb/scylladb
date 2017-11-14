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

#include "authorizer.hh"
#include "authenticated_user.hh"
#include "common.hh"
#include "default_authorizer.hh"
#include "auth.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "utils/class_registrator.hh"

const sstring& auth::allow_all_authorizer_name() {
    static const sstring name = meta::AUTH_PACKAGE_NAME + "AllowAllAuthorizer";
    return name;
}

/**
 * Authenticator is assumed to be a fully state-less immutable object (note all the const).
 * We thus store a single instance globally, since it should be safe/ok.
 */
static std::unique_ptr<auth::authorizer> global_authorizer;
using authorizer_registry = class_registry<auth::authorizer, cql3::query_processor&>;

future<>
auth::authorizer::setup(const sstring& type) {
    if (type == allow_all_authorizer_name()) {
        class allow_all_authorizer : public authorizer {
        public:
            future<> start() override {
                return make_ready_future<>();
            }
            future<> stop() override {
                return make_ready_future<>();
            }
            const sstring& qualified_java_name() const override {
                return allow_all_authorizer_name();
            }
            future<permission_set> authorize(::shared_ptr<authenticated_user>, data_resource) const override {
                return make_ready_future<permission_set>(permissions::ALL);
            }
            future<> grant(::shared_ptr<authenticated_user>, permission_set, data_resource, sstring) override {
                throw exceptions::invalid_request_exception("GRANT operation is not supported by AllowAllAuthorizer");
            }
            future<> revoke(::shared_ptr<authenticated_user>, permission_set, data_resource, sstring) override {
                throw exceptions::invalid_request_exception("REVOKE operation is not supported by AllowAllAuthorizer");
            }
            future<std::vector<permission_details>> list(::shared_ptr<authenticated_user> performer, permission_set, optional<data_resource>, optional<sstring>) const override {
                throw exceptions::invalid_request_exception("LIST PERMISSIONS operation is not supported by AllowAllAuthorizer");
            }
            future<> revoke_all(sstring dropped_user) override {
                return make_ready_future();
            }
            future<> revoke_all(data_resource) override {
                return make_ready_future();
            }
            const resource_ids& protected_resources() override {
                static const resource_ids ids;
                return ids;
            }
            future<> validate_configuration() const override {
                return make_ready_future();
            }
        };

        global_authorizer = std::make_unique<allow_all_authorizer>();
        return make_ready_future();
    } else {
        auto a = authorizer_registry::create(type, cql3::get_local_query_processor());
        auto f = a->start();
        return f.then([a = std::move(a)]() mutable {
            global_authorizer = std::move(a);
        });
    }
}

auth::authorizer& auth::authorizer::get() {
    assert(global_authorizer);
    return *global_authorizer;
}
