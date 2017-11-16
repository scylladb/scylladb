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
 * Copyright (C) 2017 ScyllaDB
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
#include "authenticator.hh"
#include "authorizer.hh"
#include "password_authenticator.hh"
#include "default_authorizer.hh"
#include "permission.hh"
#include "db/config.hh"
#include "utils/class_registrator.hh"

namespace auth {

class service;

static const sstring PACKAGE_NAME("com.scylladb.auth.");

static const sstring& transitional_authenticator_name() {
    static const sstring name = PACKAGE_NAME + "TransitionalAuthenticator";
    return name;
}

static const sstring& transitional_authorizer_name() {
    static const sstring name = PACKAGE_NAME + "TransitionalAuthorizer";
    return name;
}

class transitional_authenticator : public authenticator {
    std::unique_ptr<authenticator> _authenticator;
public:
    static const sstring PASSWORD_AUTHENTICATOR_NAME;

    transitional_authenticator(cql3::query_processor& qp, ::service::migration_manager& mm)
            : transitional_authenticator(std::make_unique<password_authenticator>(qp, mm))
    {}
    transitional_authenticator(std::unique_ptr<authenticator> a)
        : _authenticator(std::move(a))
    {}
    future<> start() override {
        return _authenticator->start();
    }
    future<> stop() override {
        return _authenticator->stop();
    }
    const sstring& qualified_java_name() const override {
        return transitional_authenticator_name();
    }
    bool require_authentication() const override {
        return true;
    }
    option_set supported_options() const override {
        return _authenticator->supported_options();
    }
    option_set alterable_options() const override {
        return _authenticator->alterable_options();
    }
    future<::shared_ptr<authenticated_user>> authenticate(const credentials_map& credentials) const override {
        auto i = credentials.find(authenticator::USERNAME_KEY);
        if ((i == credentials.end() || i->second.empty()) && (!credentials.count(PASSWORD_KEY) || credentials.at(PASSWORD_KEY).empty())) {
            // return anon user
            return make_ready_future<::shared_ptr<authenticated_user>>(::make_shared<authenticated_user>());
        }
        return make_ready_future().then([this, &credentials] {
            return _authenticator->authenticate(credentials);
        }).handle_exception([](auto ep) {
            try {
                std::rethrow_exception(ep);
            } catch (exceptions::authentication_exception&) {
                // return anon user
                return make_ready_future<::shared_ptr<authenticated_user>>(::make_shared<authenticated_user>());
            }
        });
    }
    future<> create(sstring username, const option_map& options) override {
        return _authenticator->create(username, options);
    }
    future<> alter(sstring username, const option_map& options) override {
        return _authenticator->alter(username, options);
    }
    future<> drop(sstring username) override {
        return _authenticator->drop(username);
    }
    const resource_ids& protected_resources() const override {
        return _authenticator->protected_resources();
    }
    ::shared_ptr<sasl_challenge> new_sasl_challenge() const override {
        class sasl_wrapper : public sasl_challenge {
        public:
            sasl_wrapper(::shared_ptr<sasl_challenge> sasl)
                : _sasl(std::move(sasl))
            {}
            bytes evaluate_response(bytes_view client_response) override {
                try {
                    return _sasl->evaluate_response(client_response);
                } catch (exceptions::authentication_exception&) {
                    _complete = true;
                    return {};
                }
            }
            bool is_complete() const {
                return _complete || _sasl->is_complete();
            }
            future<::shared_ptr<authenticated_user>> get_authenticated_user() const {
                return _sasl->get_authenticated_user();
            }
        private:
            ::shared_ptr<sasl_challenge> _sasl;
            bool _complete = false;
        };
        return ::make_shared<sasl_wrapper>(_authenticator->new_sasl_challenge());
    }
};

class transitional_authorizer : public authorizer {
    std::unique_ptr<authorizer> _authorizer;
public:
    transitional_authorizer(cql3::query_processor& qp, ::service::migration_manager& mm)
        : transitional_authorizer(std::make_unique<default_authorizer>(qp, mm))
    {}
    transitional_authorizer(std::unique_ptr<authorizer> a)
        : _authorizer(std::move(a))
    {}
    ~transitional_authorizer()
    {}
    future<> start() override {
        return _authorizer->start();
    }
    future<> stop() override {
        return _authorizer->stop();
    }
    const sstring& qualified_java_name() const override {
        return transitional_authorizer_name();
    }
    future<permission_set> authorize(service& ser, ::shared_ptr<authenticated_user> user, data_resource resource) const override {
        return is_super_user(ser, *user).then([](bool s) {
            static const permission_set transitional_permissions =
                            permission_set::of<permission::CREATE,
                                            permission::ALTER, permission::DROP,
                                            permission::SELECT, permission::MODIFY>();

            return make_ready_future<permission_set>(s ? permissions::ALL : transitional_permissions);
        });
    }
    future<> grant(::shared_ptr<authenticated_user> user, permission_set ps, data_resource r, sstring s) override {
        return _authorizer->grant(std::move(user), std::move(ps), std::move(r), std::move(s));
    }
    future<> revoke(::shared_ptr<authenticated_user> user, permission_set ps, data_resource r, sstring s) override {
        return _authorizer->revoke(std::move(user), std::move(ps), std::move(r), std::move(s));
    }
    future<std::vector<permission_details>> list(service& ser, ::shared_ptr<authenticated_user> user, permission_set ps, optional<data_resource> r, optional<sstring> s) const override {
        return _authorizer->list(ser, std::move(user), std::move(ps), std::move(r), std::move(s));
    }
    future<> revoke_all(sstring s) override {
        return _authorizer->revoke_all(std::move(s));
    }
    future<> revoke_all(data_resource r) override {
        return _authorizer->revoke_all(std::move(r));
    }
    const resource_ids& protected_resources() override {
        return _authorizer->protected_resources();
    }
    future<> validate_configuration() const override {
        return _authorizer->validate_configuration();
    }
};

}

//
// To ensure correct initialization order, we unfortunately need to use string literals.
//

static const class_registrator<
        auth::authenticator,
        auth::transitional_authenticator,
        cql3::query_processor&,
        ::service::migration_manager&> transitional_authenticator_reg("com.scylladb.auth.TransitionalAuthenticator");

static const class_registrator<
        auth::authorizer,
        auth::transitional_authorizer,
        cql3::query_processor&,
        ::service::migration_manager&> transitional_authorizer_reg("com.scylladb.auth.TransitionalAuthorizer");
