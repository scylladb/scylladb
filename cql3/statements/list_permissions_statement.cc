/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include <vector>
#include <seastar/core/future-util.hh>

#include "list_permissions_statement.hh"
#include "auth/authorizer.hh"
#include "auth/common.hh"
#include "cql3/result_set.hh"
#include "db/system_keyspace.hh"
#include "transport/messages/result_message.hh"

shared_ptr<const cql3::metadata> cql3::statements::list_permissions_statement::get_result_metadata() const {
    return ::make_shared<cql3::metadata>(
            std::vector<lw_shared_ptr<cql3::column_specification>>{
                    make_column_spec(db::system_keyspace::NAME, "permissions", "role", utf8_type),
                    make_column_spec(db::system_keyspace::NAME, "permissions", "username", utf8_type),
                    make_column_spec(db::system_keyspace::NAME, "permissions", "resource", utf8_type),
                    make_column_spec(db::system_keyspace::NAME, "permissions", "permission", utf8_type)});
}

cql3::statements::list_permissions_statement::list_permissions_statement(
        auth::permission_set permissions,
        std::optional<auth::resource> resource,
        std::optional<sstring> role_name, bool recursive)
            : _permissions(permissions)
            , _resource(std::move(resource))
            , _role_name(std::move(role_name))
            , _recursive(recursive) {
}

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::list_permissions_statement::prepare(
                data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<list_permissions_statement>(*this));
}

void cql3::statements::list_permissions_statement::validate(
        query_processor& qp,
        const service::client_state& state) const {
    // a check to ensure the existence of the user isn't being leaked by user existence check.
    state.ensure_not_anonymous();
}

future<std::optional<std::string>> cql3::statements::list_permissions_statement::check_role_access(const service::client_state& state) const {
    if (co_await state.has_superuser()) {
        co_return std::nullopt;
    }

    // DESCRIBE permission on a role allows listing its permissions, and
    // DESCRIBE on all roles allows listing everyone's permissions. The creator
    // of a role gets DESCRIBE on it automatically.
    if (!_role_name) {
        if (co_await state.check_has_permission({auth::permission::DESCRIBE, auth::root_role_resource()})) {
            co_return std::nullopt;
        }
        co_return "You are not authorized to view everyone's permissions";
    }

    bool has_role;
    try {
        has_role = co_await auth::has_role(*state.get_auth_service(), *state.user(), *_role_name);
    } catch (const auth::nonexistant_role&) {
        // Treat a role which doesn't exist like a role which the user may not
        // view, so the error doesn't tell the user whether the role exists. A
        // user who may view it gets the "doesn't exist" error from execute().
        has_role = false;
    }
    if (has_role || co_await state.check_has_permission({auth::permission::DESCRIBE, auth::make_role_resource(*_role_name)})) {
        co_return std::nullopt;
    }
    co_return fmt::format("You are not authorized to view {}'s permissions", *_role_name);
}

future<> cql3::statements::list_permissions_statement::check_access(query_processor& qp, const service::client_state& state) const {
    if (_resource) {
        maybe_correct_resource(*_resource, state, qp);
    }

    if (auto denied = co_await check_role_access(state)) {
        // A user with AUTHORIZE permission on the resource - who may grant and
        // revoke any role's permissions on it - may list the permissions on it
        // anyway, but sees only the permissions it could revoke (see execute()).
        if (!_resource || !co_await state.check_has_permission({auth::permission::AUTHORIZE, *_resource})) {
            throw exceptions::unauthorized_exception(*denied);
        }
    }

    // Check that the resource exists only after checking access, so the error
    // doesn't tell an unauthorized user whether it exists (e.g., ON ROLE r).
    if (_resource) {
        co_await state.ensure_exists(*_resource);
    }
}


future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::list_permissions_statement::execute(
        query_processor& qp,
        service::query_state& state,
        const query_options& options,
        std::optional<service::group0_guard> guard) const {
    std::optional<std::pair<auth::resource, auth::recursive_permissions>> resource_filter;
    if (_resource) {
        resource_filter.emplace(*_resource, _recursive ? auth::recursive_permissions::yes : auth::recursive_permissions::no);
    }

    const auto& client_state = state.get_client_state();
    std::vector<auth::permission_details> all_details;
    try {
        all_details = co_await auth::list_filtered_permissions(*client_state.get_auth_service(), _permissions, _role_name, resource_filter);
    } catch (const auth::nonexistant_role& e) {
        throw exceptions::invalid_request_exception(e.what());
    } catch (const auth::unsupported_authorization_operation& e) {
        throw exceptions::invalid_request_exception(e.what());
    }

    // A user allowed to list these permissions only because it has AUTHORIZE
    // permission on the resource (see check_access()) sees only the
    // permissions on resources it has AUTHORIZE on - the ones it could revoke.
    // A recursive listing also includes permissions on the resources above
    // the requested one, on which the user may not have AUTHORIZE.
    if (_resource && co_await check_role_access(client_state)) {
        std::vector<auth::permission_details> authorized_details;
        for (auto& pd : all_details) {
            if (co_await client_state.check_has_permission({auth::permission::AUTHORIZE, pd.resource})) {
                authorized_details.push_back(std::move(pd));
            }
        }
        all_details = std::move(authorized_details);
    }

    std::sort(all_details.begin(), all_details.end());

    auto rs = std::make_unique<result_set>(::make_shared<cql3::metadata>(*get_result_metadata()));

    for (const auto& pd : all_details) {
        const std::vector<sstring> sorted_permission_names = [&pd] {
            std::vector<sstring> names;

            std::transform(
                    pd.permissions.begin(),
                    pd.permissions.end(),
                    std::back_inserter(names),
                    &auth::permissions::to_string);

            std::sort(names.begin(), names.end());
            return names;
        }();

        const auto decomposed_role_name = utf8_type->decompose(pd.role_name);
        const auto decomposed_resource = utf8_type->decompose(sstring(format("{}", pd.resource)));

        for (const auto& ps : sorted_permission_names) {
            rs->add_row(
                    std::vector<bytes_opt>{
                            decomposed_role_name,
                            decomposed_role_name,
                            decomposed_resource,
                            utf8_type->decompose(ps)});
        }
    }

    co_return ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(rs)));
}
