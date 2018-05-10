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

#include "auth/authenticated_user.hh"
#include "auth/authenticator.hh"
#include "auth/authorizer.hh"
#include "auth/default_authorizer.hh"
#include "auth/password_authenticator.hh"
#include "auth/permission.hh"
#include "db/config.hh"
#include "utils/class_registrator.hh"

namespace auth {

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
            : transitional_authenticator(std::make_unique<password_authenticator>(qp, mm)) {
    }
    transitional_authenticator(std::unique_ptr<authenticator> a)
            : _authenticator(std::move(a)) {
    }

    virtual future<> start() override {
        return _authenticator->start();
    }

    virtual future<> stop() override {
        return _authenticator->stop();
    }

    virtual const sstring& qualified_java_name() const override {
        return transitional_authenticator_name();
    }

    virtual bool require_authentication() const override {
        return true;
    }

    virtual authentication_option_set supported_options() const override {
        return _authenticator->supported_options();
    }

    virtual authentication_option_set alterable_options() const override {
        return _authenticator->alterable_options();
    }

    virtual future<authenticated_user> authenticate(const credentials_map& credentials) const override {
        auto i = credentials.find(authenticator::USERNAME_KEY);
        if ((i == credentials.end() || i->second.empty())
                && (!credentials.count(PASSWORD_KEY) || credentials.at(PASSWORD_KEY).empty())) {
            // return anon user
            return make_ready_future<authenticated_user>(anonymous_user());
        }
        return make_ready_future().then([this, &credentials] {
            return _authenticator->authenticate(credentials);
        }).handle_exception([](auto ep) {
            try {
                std::rethrow_exception(ep);
            } catch (exceptions::authentication_exception&) {
                // return anon user
                return make_ready_future<authenticated_user>(anonymous_user());
            }
        });
    }

    virtual future<> create(stdx::string_view role_name, const authentication_options& options) const override {
        return _authenticator->create(role_name, options);
    }

    virtual future<> alter(stdx::string_view role_name, const authentication_options& options) const override {
        return _authenticator->alter(role_name, options);
    }

    virtual future<> drop(stdx::string_view role_name) const override {
        return _authenticator->drop(role_name);
    }

    virtual future<custom_options> query_custom_options(stdx::string_view role_name) const override {
        return _authenticator->query_custom_options(role_name);
    }

    virtual const resource_set& protected_resources() const override {
        return _authenticator->protected_resources();
    }

    virtual ::shared_ptr<sasl_challenge> new_sasl_challenge() const override {
        class sasl_wrapper : public sasl_challenge {
        public:
            sasl_wrapper(::shared_ptr<sasl_challenge> sasl)
                    : _sasl(std::move(sasl)) {
            }

            virtual bytes evaluate_response(bytes_view client_response) override {
                try {
                    return _sasl->evaluate_response(client_response);
                } catch (exceptions::authentication_exception&) {
                    _complete = true;
                    return {};
                }
            }

            virtual bool is_complete() const override {
                return _complete || _sasl->is_complete();
            }

            virtual future<authenticated_user> get_authenticated_user() const {
                return futurize_apply([this] {
                    return _sasl->get_authenticated_user().handle_exception([](auto ep) {
                        try {
                            std::rethrow_exception(ep);
                        } catch (exceptions::authentication_exception&) {
                            // return anon user
                            return make_ready_future<authenticated_user>(anonymous_user());
                        }
                    });
                });
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
            : transitional_authorizer(std::make_unique<default_authorizer>(qp, mm)) {
    }
    transitional_authorizer(std::unique_ptr<authorizer> a)
            : _authorizer(std::move(a)) {
    }

    ~transitional_authorizer() {
    }

    virtual future<> start() override {
        return _authorizer->start();
    }

    virtual future<> stop() override {
        return _authorizer->stop();
    }

    virtual const sstring& qualified_java_name() const override {
        return transitional_authorizer_name();
    }

    virtual future<permission_set> authorize(const role_or_anonymous&, const resource&) const override {
        static const permission_set transitional_permissions =
                permission_set::of<
                        permission::CREATE,
                        permission::ALTER,
                        permission::DROP,
                        permission::SELECT,
                        permission::MODIFY>();

        return make_ready_future<permission_set>(transitional_permissions);
    }

    virtual future<> grant(stdx::string_view s, permission_set ps, const resource& r) const override {
        return _authorizer->grant(s, std::move(ps), r);
    }

    virtual future<> revoke(stdx::string_view s, permission_set ps, const resource& r) const override {
        return _authorizer->revoke(s, std::move(ps), r);
    }

    virtual future<std::vector<permission_details>> list_all() const override {
        return _authorizer->list_all();
    }

    virtual future<> revoke_all(stdx::string_view s) const override {
        return _authorizer->revoke_all(s);
    }

    virtual future<> revoke_all(const resource& r) const override {
        return _authorizer->revoke_all(r);
    }

    virtual const resource_set& protected_resources() const override {
        return _authorizer->protected_resources();
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
        ::service::migration_manager&> transitional_authenticator_reg(auth::PACKAGE_NAME + "TransitionalAuthenticator");

static const class_registrator<
        auth::authorizer,
        auth::transitional_authorizer,
        cql3::query_processor&,
        ::service::migration_manager&> transitional_authorizer_reg(auth::PACKAGE_NAME + "TransitionalAuthorizer");
