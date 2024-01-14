/*
 * Copyright 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <algorithm>

#include "types/map.hh"
#include "auth/authentication_options.hh"
#include "auth/common.hh"
#include "auth/role_manager.hh"
#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/alter_role_statement.hh"
#include "cql3/statements/create_role_statement.hh"
#include "cql3/statements/drop_role_statement.hh"
#include "cql3/statements/grant_role_statement.hh"
#include "cql3/statements/list_roles_statement.hh"
#include "cql3/statements/revoke_role_statement.hh"
#include "cql3/statements/request_validations.hh"
#include "exceptions/exceptions.hh"
#include "service/storage_proxy.hh"
#include "transport/messages/result_message.hh"

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

//
// `create_role_statement`
//

std::unique_ptr<prepared_statement> create_role_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<create_role_statement>(*this));
}

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

future<> create_role_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.ensure_not_anonymous();

    return async([this, &state] {
        state.ensure_has_permission({auth::permission::CREATE, auth::root_role_resource()}).get0();

        if (*_options.is_superuser) {
            if (!auth::has_superuser(*state.get_auth_service(), *state.user()).get0()) {
                throw exceptions::unauthorized_exception("Only superusers can create a role with superuser status.");
            }
        }
    });
}

future<result_message_ptr>
create_role_statement::execute(query_processor&,
                               service::query_state& state,
                               const query_options&,
                               std::optional<service::group0_guard> guard) const {
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

std::unique_ptr<prepared_statement> alter_role_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<alter_role_statement>(*this));
}

future<> alter_role_statement::check_access(query_processor& qp, const service::client_state& state) const {
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
            state.ensure_has_permission({auth::permission::ALTER, auth::make_role_resource(_role)}).get0();
        } else {
            const auto alterable_options = state.get_auth_service()->underlying_authenticator().alterable_options();

            const auto check = [&alterable_options](auth::authentication_option ao) {
                if (!alterable_options.contains(ao)) {
                    throw exceptions::unauthorized_exception(format("You aren't allowed to alter the {} option.", ao));
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
alter_role_statement::execute(query_processor&, service::query_state& state, const query_options&, std::optional<service::group0_guard> guard) const {
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

std::unique_ptr<prepared_statement> drop_role_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<drop_role_statement>(*this));
}

void drop_role_statement::validate(query_processor& qp, const service::client_state& state) const {
    if (*state.user() == auth::authenticated_user(_role)) {
        throw request_validations::invalid_request("Cannot DROP primary role for current login.");
    }
}

future<> drop_role_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.ensure_not_anonymous();

    return async([this, &state] {
        state.ensure_has_permission({auth::permission::DROP, auth::make_role_resource(_role)}).get0();

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
drop_role_statement::execute(query_processor&, service::query_state& state, const query_options&, std::optional<service::group0_guard> guard) const {
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

std::unique_ptr<prepared_statement> list_roles_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<list_roles_statement>(*this));
}

future<> list_roles_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.ensure_not_anonymous();

    return async([this, &state] {
        if (state.check_has_permission({auth::permission::DESCRIBE, auth::root_role_resource()}).get0()) {
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
                    format("You are not authorized to view the roles granted to role '{}'.", *_grantee));
        }
    });
}

future<result_message_ptr>
list_roles_statement::execute(query_processor&, service::query_state& state, const query_options&, std::optional<service::group0_guard> guard) const {
    static const sstring virtual_table_name("roles");

    static const auto make_column_spec = [](const sstring& name, const ::shared_ptr<const abstract_type>& ty) {
        return make_lw_shared<column_specification>(
                auth::meta::AUTH_KS,
                virtual_table_name,
                ::make_shared<column_identifier>(name, true),
                ty);
    };

    static const thread_local auto custom_options_type = map_type_impl::get_instance(utf8_type, utf8_type, true);

    static const thread_local auto metadata = ::make_shared<cql3::metadata>(
            std::vector<lw_shared_ptr<column_specification>>{
                    make_column_spec("role", utf8_type),
                    make_column_spec("super", boolean_type),
                    make_column_spec("login", boolean_type),
                    make_column_spec("options", custom_options_type)});

    static const auto make_results = [](
            auth::role_manager& rm,
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
                        a.query_custom_options(role)).then_unpack([&results, &role](
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

    return auth::has_superuser(as, *cs.user()).then([this, &cs, &as](bool super) {
        auto& rm = as.underlying_role_manager();
        const auto& a = as.underlying_authenticator();
        const auto query_mode = _recursive ? auth::recursive_role_query::yes : auth::recursive_role_query::no;

        if (!_grantee) {
            // A user with DESCRIBE on the root role resource lists all roles in the system. A user without it lists
            // only the roles granted to them.
            return cs.check_has_permission({
                    auth::permission::DESCRIBE,
                    auth::root_role_resource()}).then([&cs, &rm, &a, query_mode](bool has_describe) {
                if (has_describe) {
                    return rm.query_all().then([&rm, &a](auto&& roles) {
                        return make_results(rm, a, std::move(roles));
                    });
                }

                return rm.query_granted(*cs.user()->name, query_mode).then([&rm, &a](auth::role_set roles) {
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

std::unique_ptr<prepared_statement> grant_role_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<grant_role_statement>(*this));
}

future<> grant_role_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.ensure_not_anonymous();

    return do_with(auth::make_role_resource(_role), [&state](const auto& r) {
        return state.ensure_has_permission({auth::permission::AUTHORIZE, r});
    });
}

future<result_message_ptr>
grant_role_statement::execute(query_processor&, service::query_state& state, const query_options&, std::optional<service::group0_guard> guard) const {
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

std::unique_ptr<prepared_statement> revoke_role_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<revoke_role_statement>(*this));
}

future<> revoke_role_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.ensure_not_anonymous();

    return do_with(auth::make_role_resource(_role), [&state](const auto& r) {
        return state.ensure_has_permission({auth::permission::AUTHORIZE, r});
    });
}

future<result_message_ptr> revoke_role_statement::execute(
        query_processor&,
        service::query_state& state,
        const query_options&,
        std::optional<service::group0_guard> guard) const {
    auto& rm = state.get_client_state().get_auth_service()->underlying_role_manager();

    return rm.revoke(_revokee, _role).then([] {
        return void_result_message();
    }).handle_exception_type([](const auth::roles_argument_exception& e) {
        return make_exception_future<result_message_ptr>(exceptions::invalid_request_exception(e.what()));
    });
}

}

}
