/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <vector>
#include <seastar/core/future-util.hh>

#include "list_permissions_statement.hh"
#include "auth/authorizer.hh"
#include "auth/common.hh"
#include "cql3/result_set.hh"
#include "cql3/column_identifier.hh"
#include "transport/messages/result_message.hh"

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
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<list_permissions_statement>(*this));
}

void cql3::statements::list_permissions_statement::validate(
        query_processor& qp,
        const service::client_state& state) const {
    // a check to ensure the existence of the user isn't being leaked by user existence check.
    state.ensure_not_anonymous();
}

future<> cql3::statements::list_permissions_statement::check_access(query_processor& qp, const service::client_state& state) const {
    if (_resource) {
        maybe_correct_resource(*_resource, state, qp);
        return state.ensure_exists(*_resource);
    }

    const auto& as = *state.get_auth_service();
    const auto user = state.user();

    return auth::has_superuser(as, *user).then([this, &as, user](bool has_super) {
        if (has_super) {
            return make_ready_future<>();
        }

        if (!_role_name) {
            return make_exception_future<>(
                    exceptions::unauthorized_exception("You are not authorized to view everyone's permissions"));
        }

        return auth::has_role(as, *user, *_role_name).then([this](bool has_role) {
            if (!has_role) {
                return make_exception_future<>(
                        exceptions::unauthorized_exception(
                                format("You are not authorized to view {}'s permissions", *_role_name)));
            }

            return make_ready_future<>();
        }).handle_exception_type([](const auth::nonexistant_role& e) {
            return make_exception_future<>(exceptions::invalid_request_exception(e.what()));
        });
    });
}


future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::list_permissions_statement::execute(
        query_processor& qp,
        service::query_state& state,
        const query_options& options,
        std::optional<service::group0_guard> guard) const {
    auto make_column = [auth_ks = auth::get_auth_ks_name(qp)](sstring name) {
        return make_lw_shared<column_specification>(
                auth_ks,
                "permissions",
                ::make_shared<column_identifier>(std::move(name), true),
                utf8_type);
    };

    std::vector<lw_shared_ptr<column_specification>> metadata({
        make_column("role"), make_column("username"), make_column("resource"), make_column("permission")
    });

    const auto make_resource_filter = [this]()
            -> std::optional<std::pair<auth::resource, auth::recursive_permissions>> {
        if (!_resource) {
            return {};
        }

        return std::make_pair(
                *_resource,
                _recursive ? auth::recursive_permissions::yes : auth::recursive_permissions::no);
    };

    const auto& as = *state.get_client_state().get_auth_service();

    return do_with(make_resource_filter(), [this, &as, metadata = std::move(metadata)](const auto& resource_filter) mutable {
        return auth::list_filtered_permissions(
                as,
                _permissions,
                _role_name,
                resource_filter).then([metadata = std::move(metadata)](std::vector<auth::permission_details> all_details) mutable {
            std::sort(all_details.begin(), all_details.end());

            auto rs = std::make_unique<result_set>(std::move(metadata));

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

            auto rows = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(std::move(rs))));
            return ::shared_ptr<cql_transport::messages::result_message>(rows);
        }).handle_exception_type([](const auth::nonexistant_role& e) {
            return make_exception_future<::shared_ptr<cql_transport::messages::result_message>>(
                    exceptions::invalid_request_exception(e.what()));
        }).handle_exception_type([](const auth::unsupported_authorization_operation& e) {
            return make_exception_future<::shared_ptr<cql_transport::messages::result_message>>(
                    exceptions::invalid_request_exception(e.what()));
        });
    });
}
