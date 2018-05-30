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
 * Copyright 2017 ScyllaDB
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

#include <algorithm>

#include "auth/authentication_options.hh"
#include "auth/common.hh"
#include "auth/role_manager.hh"
#include "cql3/column_specification.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/alter_role_statement.hh"
#include "cql3/statements/create_role_statement.hh"
#include "cql3/statements/drop_role_statement.hh"
#include "cql3/statements/grant_role_statement.hh"
#include "cql3/statements/list_roles_statement.hh"
#include "cql3/statements/revoke_role_statement.hh"
#include "cql3/statements/request_validations.hh"
#include "exceptions/exceptions.hh"
#include "service/storage_service.hh"
#include "transport/messages/result_message.hh"
#include "unimplemented.hh"

namespace cql3 {

namespace statements {

using result_message = cql_transport::messages::result_message;
using result_message_ptr = ::shared_ptr<result_message>;

static auth::authentication_options extract_authentication_options(const cql3::role_options& options) {
    auth::authentication_options authen_options;
    authen_options.password = options.password;

    if (options.options) {
        authen_options.options = std::unordered_map<sstring, sstring>(options.options->begin(), options.options->end());
    }

    return authen_options;
}

static future<result_message_ptr> void_result_message() {
    return make_ready_future<result_message_ptr>(nullptr);
}

void validate_cluster_support() {
    // TODO(jhaberku): All other feature-checking CQL statements also grab the `storage_service` globally. I'm not sure
    // if it's accessible through some other object, but for now I'm sticking with convention.
    if (!service::get_local_storage_service().cluster_supports_roles()) {
        throw exceptions::invalid_request_exception(
                "You cannot modify access-control information until the cluster has fully upgraded.");
    }
}

//
// `create_role_statement`
//

future<> create_role_statement::grant_permissions_to_creator(const service::client_state& cs) const {
    return do_with(auth::make_role_resource(_role), [&cs](const auth::resource& r) {
        return auth::grant_applicable_permissions(
                *cs.get_auth_service(),
                *cs.user(),
                r).handle_exception_type([](const auth::unsupported_authorization_operation&) {
            // Nothing.
        });
    });
}

void create_role_statement::validate(service::storage_proxy&, const service::client_state&) {
    validate_cluster_support();
}

future<> create_role_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();

    return async([this, &state] {
        state.ensure_has_permission(auth::permission::CREATE, auth::root_role_resource()).get0();

        if (*_options.is_superuser) {
            if (!auth::has_superuser(*state.get_auth_service(), *state.user()).get0()) {
                throw exceptions::unauthorized_exception("Only superusers can create a role with superuser status.");
            }
        }
    });
}

future<result_message_ptr>
create_role_statement::execute(service::storage_proxy&,
                               service::query_state& state,
                               const query_options&) {
    auth::role_config config;
    config.is_superuser = *_options.is_superuser;
    config.can_login = *_options.can_login;

    return do_with(
            std::move(config),
            extract_authentication_options(_options),
            [this, &state](const auth::role_config& config, const auth::authentication_options& authen_options) {
        const auto& cs = state.get_client_state();
        auto& as = *cs.get_auth_service();

        return auth::create_role(as, _role, config, authen_options).then([this, &cs] {
            return grant_permissions_to_creator(cs);
        }).then([] {
            return void_result_message();
        }).handle_exception_type([this](const auth::role_already_exists& e) {
            if (!_if_not_exists) {
                return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
            }

            return void_result_message();
        }).handle_exception_type([](const auth::unsupported_authentication_option& e) {
            return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
        });
    });
}

//
// `alter_role_statement`
//

void alter_role_statement::validate(service::storage_proxy&, const service::client_state&) {
    validate_cluster_support();
}

future<> alter_role_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();

    return async([this, &state] {
        auto& as = *state.get_auth_service();

        const auto& user = *state.user();
        const bool user_is_superuser = auth::has_superuser(as, user).get0();

        if (_options.is_superuser) {
            if (!user_is_superuser) {
                throw exceptions::unauthorized_exception("Only superusers are allowed to alter superuser status.");
            }

            try {
                if (auth::has_role(as, user, _role).get0()) {
                    throw exceptions::unauthorized_exception(
                        "You aren't allowed to alter your own superuser status or that of a role granted to you.");
                }
            } catch (const auth::nonexistant_role& e) {
                throw exceptions::invalid_request_exception(e.what());
            }
        }

        if (*user.name != _role) {
            state.ensure_has_permission(auth::permission::ALTER, auth::make_role_resource(_role)).get0();
        } else {
            const auto alterable_options = state.get_auth_service()->underlying_authenticator().alterable_options();

            const auto check = [&alterable_options](auth::authentication_option ao) {
                if (alterable_options.count(ao) == 0) {
                    throw exceptions::unauthorized_exception(sprint("You aren't allowed to alter the %s option.", ao));
                }
            };

            if (_options.password) {
                check(auth::authentication_option::password);
            }

            if (_options.options) {
                check(auth::authentication_option::options);
            }
        }
    });
}

future<result_message_ptr>
alter_role_statement::execute(service::storage_proxy&, service::query_state& state, const query_options&) {
    auth::role_config_update update;
    update.is_superuser = _options.is_superuser;
    update.can_login = _options.can_login;

    return do_with(
            std::move(update),
            extract_authentication_options(_options),
            [this, &state](const auth::role_config_update& update, const auth::authentication_options& authen_options) {
        auto& as = *state.get_client_state().get_auth_service();

        return auth::alter_role(as, _role, update, authen_options).then([] {
            return void_result_message();
        }).handle_exception_type([](const auth::nonexistant_role& e) {
            return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
        }).handle_exception_type([](const auth::unsupported_authentication_option& e) {
            return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
        });
    });
}

//
// `drop_role_statement`
//

void drop_role_statement::validate(service::storage_proxy&, const service::client_state& state) {
    validate_cluster_support();

    if (*state.user() == auth::authenticated_user(_role)) {
        throw request_validations::invalid_request("Cannot DROP primary role for current login.");
    }
}

future<> drop_role_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();

    return async([this, &state] {
        state.ensure_has_permission(auth::permission::DROP, auth::make_role_resource(_role)).get0();

        auto& as = *state.get_auth_service();

        const bool user_is_superuser = auth::has_superuser(as, *state.user()).get0();

        const bool role_has_superuser = [this, &as] {
            try {
                return as.has_superuser(_role).get0();
            } catch (const auth::nonexistant_role&) {
                // Handled as part of `execute`.
                return false;
            }
        }();

        if (role_has_superuser && !user_is_superuser) {
            throw exceptions::unauthorized_exception("Only superusers can drop a superuser role.");
        }
    });
}

future<result_message_ptr>
drop_role_statement::execute(service::storage_proxy&, service::query_state& state, const query_options&) {
    auto& as = *state.get_client_state().get_auth_service();

    return auth::drop_role(as, _role).then([] {
        return void_result_message();
    }).handle_exception_type([this](const auth::nonexistant_role& e) {
        if (!_if_exists) {
            return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
        }

        return void_result_message();
    });
}

//
// `list_roles_statement`
//

future<> list_roles_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();

    return async([this, &state] {
        if (state.check_has_permission(auth::permission::DESCRIBE, auth::root_role_resource()).get0()) {
            return;
        }

        //
        // A user can list all roles of themselves, and list all roles of any roles granted to them.
        //

        const auto user_has_grantee = [this, &state] {
            try {
                return auth::has_role(*state.get_auth_service(), *state.user(), *_grantee).get0();
            } catch (const auth::nonexistant_role& e) {
                throw exceptions::invalid_request_exception(e.what());
            }
        };

        if (_grantee && !user_has_grantee()) {
            throw exceptions::unauthorized_exception(
                    sprint("You are not authorized to view the roles granted to role '%s'.", *_grantee));
        }
    });
}

future<result_message_ptr>
list_roles_statement::execute(service::storage_proxy&, service::query_state& state, const query_options&) {
    static const sstring virtual_table_name("roles");

    static const auto make_column_spec = [](const sstring& name, const ::shared_ptr<const abstract_type>& ty) {
        return ::make_shared<column_specification>(
                auth::meta::AUTH_KS,
                virtual_table_name,
                ::make_shared<column_identifier>(name, true),
                ty);
    };

    static const thread_local auto custom_options_type = map_type_impl::get_instance(utf8_type, utf8_type, true);

    static const thread_local auto metadata = ::make_shared<cql3::metadata>(
            std::vector<::shared_ptr<column_specification>>{
                    make_column_spec("role", utf8_type),
                    make_column_spec("super", boolean_type),
                    make_column_spec("login", boolean_type),
                    make_column_spec("options", custom_options_type)});

    static const auto make_results = [](
            const auth::role_manager& rm,
            const auth::authenticator& a,
            auth::role_set&& roles) -> future<result_message_ptr> {
        auto results = std::make_unique<result_set>(metadata);

        if (roles.empty()) {
            return make_ready_future<result_message_ptr>(
                ::make_shared<result_message::rows>(result(std::move(results))));
        }

        std::vector<sstring> sorted_roles(roles.cbegin(), roles.cend());
        std::sort(sorted_roles.begin(), sorted_roles.end());

        return do_with(
                std::move(sorted_roles),
                std::move(results),
                [&rm, &a](const std::vector<sstring>& sorted_roles, std::unique_ptr<result_set>& results) {
            return do_for_each(sorted_roles, [&results, &rm, &a](const sstring& role) {
                return when_all_succeed(
                        rm.can_login(role),
                        rm.is_superuser(role),
                        a.query_custom_options(role)).then([&results, &role](
                               bool login,
                               bool super,
                               auth::custom_options os) {
                    results->add_column_value(utf8_type->decompose(role));
                    results->add_column_value(boolean_type->decompose(super));
                    results->add_column_value(boolean_type->decompose(login));

                    results->add_column_value(
                            custom_options_type->decompose(
                                    make_map_value(
                                            custom_options_type,
                                            map_type_impl::native_type(
                                                    std::make_move_iterator(os.begin()),
                                                    std::make_move_iterator(os.end())))));
                });
            }).then([&results] {
                return make_ready_future<result_message_ptr>(::make_shared<result_message::rows>(result(std::move(results))));
            });
        });
    };

    const auto& cs = state.get_client_state();
    const auto& as = *cs.get_auth_service();
    const auto user = cs.user();

    return auth::has_superuser(as, *user).then([this, &state, &cs, &as, user](bool super) {
        const auto& rm = as.underlying_role_manager();
        const auto& a = as.underlying_authenticator();
        const auto query_mode = _recursive ? auth::recursive_role_query::yes : auth::recursive_role_query::no;

        if (!_grantee) {
            // A user with DESCRIBE on the root role resource lists all roles in the system. A user without it lists
            // only the roles granted to them.
            return cs.check_has_permission(
                    auth::permission::DESCRIBE,
                    auth::root_role_resource()).then([&cs, &rm, &a, user, query_mode](bool has_describe) {
                if (has_describe) {
                    return rm.query_all().then([&rm, &a](auto&& roles) {
                        return make_results(rm, a, std::move(roles));
                    });
                }

                return rm.query_granted(*user->name, query_mode).then([&rm, &a](auth::role_set roles) {
                    return make_results(rm, a, std::move(roles));
                });
            });
        }

        return rm.query_granted(*_grantee, query_mode).then([&rm, &a](auth::role_set roles) {
            return make_results(rm, a, std::move(roles));
        });
    }).handle_exception_type([](const auth::nonexistant_role& e) {
        return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
    });
}

//
// `grant_role_statement`
//

future<> grant_role_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();

    return do_with(auth::make_role_resource(_role), [this, &state](const auto& r) {
        return state.ensure_has_permission(auth::permission::AUTHORIZE, r);
    });
}

future<result_message_ptr>
grant_role_statement::execute(service::storage_proxy&, service::query_state& state, const query_options&) {
    auto& as = *state.get_client_state().get_auth_service();

    return as.underlying_role_manager().grant(_grantee, _role).then([] {
        return void_result_message();
    }).handle_exception_type([](const auth::roles_argument_exception& e) {
        return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
    });
}

//
// `revoke_role_statement`
//

future<> revoke_role_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();

    return do_with(auth::make_role_resource(_role), [this, &state](const auto& r) {
        return state.ensure_has_permission(auth::permission::AUTHORIZE, r);
    });
}

future<result_message_ptr> revoke_role_statement::execute(
        service::storage_proxy&,
        service::query_state& state,
        const query_options&) {
    auto& rm = state.get_client_state().get_auth_service()->underlying_role_manager();

    return rm.revoke(_revokee, _role).then([] {
        return void_result_message();
    }).handle_exception_type([](const auth::roles_argument_exception& e) {
        return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
    });
}

}

}
