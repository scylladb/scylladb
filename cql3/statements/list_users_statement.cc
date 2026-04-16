/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include <seastar/core/when_all.hh>
#include "list_users_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "auth/common.hh"
#include "db/system_keyspace.hh"
#include "transport/messages/result_message.hh"

shared_ptr<const cql3::metadata> cql3::statements::list_users_statement::get_result_metadata() const {
    return ::make_shared<cql3::metadata>(
        std::vector<lw_shared_ptr<cql3::column_specification>>{
                cql3::make_column_spec(db::system_keyspace::NAME, "users", "name", utf8_type),
                cql3::make_column_spec(db::system_keyspace::NAME, "users", "super", boolean_type)});
}

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::list_users_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<list_users_statement>(*this));
}

future<> cql3::statements::list_users_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.ensure_not_anonymous();
    return make_ready_future();
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::list_users_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    auto metadata = ::make_shared<cql3::metadata>(*get_result_metadata());

    auto make_results = [metadata = std::move(metadata)](const auth::service& as, std::unordered_set<sstring>&& roles) mutable {
        using cql_transport::messages::result_message;

        auto results = std::make_unique<result_set>(std::move(metadata));

        std::vector<sstring> sorted_roles(roles.cbegin(), roles.cend());
        std::sort(sorted_roles.begin(), sorted_roles.end());

        return do_with(
                std::move(sorted_roles),
                std::move(results),
                [&as](const std::vector<sstring>& sorted_roles, std::unique_ptr<result_set>& results) {
            return do_for_each(sorted_roles, [&as, &results](const sstring& role) {
                return when_all_succeed(
                        as.has_superuser(role),
                        as.underlying_role_manager().can_login(role)).then_unpack([&results, &role](bool super, bool login) {
                    if (login) {
                        results->add_column_value(utf8_type->decompose(role));
                        results->add_column_value(boolean_type->decompose(super));
                    }
                });
            }).then([&results] {
                return make_ready_future<::shared_ptr<result_message>>(::make_shared<result_message::rows>(
                        result(std::move(results))));
            });
        });
    };

    const auto& cs = state.get_client_state();
    const auto& as = *cs.get_auth_service();

    return cs.has_superuser().then([&cs, &as, make_results = std::move(make_results)](bool has_superuser) mutable {
        if (has_superuser) {
            return as.underlying_role_manager().query_all().then([&as, make_results = std::move(make_results)](std::unordered_set<sstring> roles) mutable {
                return make_results(as, std::move(roles));
            });
        }

        return auth::get_roles(as, *cs.user()).then([&as, make_results = std::move(make_results)](std::unordered_set<sstring> granted_roles) mutable {
            return make_results(as, std::move(granted_roles));
        });
    });
}
