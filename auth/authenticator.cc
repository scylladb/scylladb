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

#include "authenticator.hh"
#include "authenticated_user.hh"
#include "password_authenticator.hh"
#include "auth.hh"
#include "db/config.hh"

const sstring auth::authenticator::USERNAME_KEY("username");
const sstring auth::authenticator::PASSWORD_KEY("password");
const sstring auth::authenticator::ALLOW_ALL_AUTHENTICATOR_NAME("org.apache.cassandra.auth.AllowAllAuthenticator");

/**
 * Authenticator is assumed to be a fully state-less immutable object (note all the const).
 * We thus store a single instance globally, since it should be safe/ok.
 */
static std::unique_ptr<auth::authenticator> global_authenticator;

future<>
auth::authenticator::setup(const sstring& type) throw (exceptions::configuration_exception) {
    if (auth::auth::is_class_type(type, ALLOW_ALL_AUTHENTICATOR_NAME)) {
        class allow_all_authenticator : public authenticator {
        public:
            const sstring& class_name() const override {
                return ALLOW_ALL_AUTHENTICATOR_NAME;
            }
            bool require_authentication() const override {
                return false;
            }
            option_set supported_options() const override {
                return option_set();
            }
            option_set alterable_options() const override {
                return option_set();
            }
            future<::shared_ptr<authenticated_user>> authenticate(const credentials_map& credentials) const throw(exceptions::authentication_exception) override {
                return make_ready_future<::shared_ptr<authenticated_user>>(::make_shared<authenticated_user>());
            }
            future<> create(sstring username, const option_map& options) throw(exceptions::request_validation_exception, exceptions::request_execution_exception) override {
                return make_ready_future();
            }
            future<> alter(sstring username, const option_map& options) throw(exceptions::request_validation_exception, exceptions::request_execution_exception) override {
                return make_ready_future();
            }
            future<> drop(sstring username) throw(exceptions::request_validation_exception, exceptions::request_execution_exception) override {
                return make_ready_future();
            }
            resource_ids protected_resources() const override {
                return resource_ids();
            }
            ::shared_ptr<sasl_challenge> new_sasl_challenge() const override {
                throw std::runtime_error("Should not reach");
            }
        };
        global_authenticator = std::make_unique<allow_all_authenticator>();
    } else if (auth::auth::is_class_type(type, password_authenticator::PASSWORD_AUTHENTICATOR_NAME)) {
        auto pwa = std::make_unique<password_authenticator>();
        auto f = pwa->init();
        return f.then([pwa = std::move(pwa)]() mutable {
            global_authenticator = std::move(pwa);
        });
    } else {
        throw exceptions::configuration_exception("Invalid authenticator type: " + type);
    }
    return make_ready_future();
}

auth::authenticator& auth::authenticator::get() {
    assert(global_authenticator);
    return *global_authenticator;
}
