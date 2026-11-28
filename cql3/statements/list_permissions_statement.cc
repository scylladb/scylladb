/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include <algorithm>
#include <vector>
#include <seastar/core/future-util.hh>

#include "list_permissions_statement.hh"
#include "auth/authorizer.hh"
#include "auth/common.hh"
#include "cql3/result_set.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include "transport/messages/result_message.hh"
#include "service/raft/raft_group0_client.hh"

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
        std::optional<sstring> role_name, bool recursive, std::vector<sstring> config_options)
            : _permissions(permissions)
            , _resource(std::move(resource))
            , _role_name(std::move(role_name))
            , _recursive(recursive)
            , _config_options(std::move(config_options)) {
    if (!_config_options.empty() && _permissions.mask() != auth::permission_set::of<auth::permission::CONFIGURE>().mask()) {
        throw exceptions::syntax_exception("Only the CONFIGURE permission takes a list of cluster config options.");
    }
    // CONFIGURE is not part of ALL when granting, but a listing of all permissions shows it.
    if (_permissions.mask() == auth::permissions::ALL.mask()) {
        _permissions = auth::permissions::ALL_AND_CONFIGURE;
    }
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

future<> cql3::statements::list_permissions_statement::check_access(query_processor& qp, const service::client_state& state) const {
    if (_resource) {
        maybe_correct_resource(*_resource, state, qp);
        return state.ensure_exists(*_resource);
    }

    const auto& as = *state.get_auth_service();
    const auto user = state.user();

    return state.has_superuser().then([this, &as, user](bool has_super) {
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
    auto metadata = ::make_shared<cql3::metadata>(*get_result_metadata());

    return do_with(make_resource_filter(), [this, &as, metadata = std::move(metadata)](const auto& resource_filter) mutable {
        return auth::list_filtered_permissions(
                as,
                _permissions,
                _role_name,
                resource_filter).then([this, metadata = std::move(metadata)](std::vector<auth::permission_details> all_details) mutable {
            if (!_config_options.empty()) {
                // A grant for all options covers the listed ones too.
                std::erase_if(all_details, [this] (const auth::permission_details& pd) {
                    const auto& option = pd.resource.config_option();
                    return option && !std::ranges::contains(_config_options, *option);
                });
            }
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
                // A permission narrowed to a cluster config option is shown as "CONFIGURE (option)".
                const auto& option = pd.resource.config_option();

                for (const auto& ps : sorted_permission_names) {
                    rs->add_row(
                            std::vector<bytes_opt>{
                                    decomposed_role_name,
                                    decomposed_role_name,
                                    decomposed_resource,
                                    utf8_type->decompose(option ? sstring(format("{} ({})", ps, *option)) : ps)});
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
