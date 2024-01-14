/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "seastarx.hh"
#include "cql3/statements/list_service_level_attachments_statement.hh"
#include "cql3/column_identifier.hh"
#include "transport/messages/result_message.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"

namespace cql3 {

namespace statements {

list_service_level_attachments_statement::list_service_level_attachments_statement(sstring role_name) :
    _role_name(role_name), _describe_all(false) {
}

list_service_level_attachments_statement::list_service_level_attachments_statement() :
    _role_name(), _describe_all(true) {
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::list_service_level_attachments_statement::prepare(
        data_dictionary::database db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<list_service_level_attachments_statement>(*this));
}

future<> list_service_level_attachments_statement::check_access(query_processor& qp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::DESCRIBE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
list_service_level_attachments_statement::execute(query_processor& qp,
        service::query_state &state,
        const query_options &,
        std::optional<service::group0_guard> guard) const {

    static auto make_column = [] (sstring name, const shared_ptr<const abstract_type> type) {
        return make_lw_shared<column_specification>(
                "QOS",
                "service_levels_attachments",
                ::make_shared<column_identifier>(std::move(name), true),
                type);
    };

    static thread_local const std::vector<lw_shared_ptr<column_specification>> metadata({
        make_column("role", utf8_type), make_column("service_level", utf8_type)
    });


    return make_ready_future().then([this, &state] () {
        if (_describe_all) {
            return state.get_client_state().get_auth_service()->underlying_role_manager().query_attribute_for_all("service_level");
        } else {
            return state.get_client_state().get_auth_service()->underlying_role_manager().get_attribute(_role_name, "service_level").then([this] (std::optional<sstring> att_val) {
                std::unordered_map<sstring, sstring> ret;
                if (att_val) {
                    ret.emplace(_role_name, *att_val);
                }
                return make_ready_future<std::unordered_map<sstring, sstring>>(ret);
            });

        }
    }).then([] (std::unordered_map<sstring, sstring> roles_to_att_val) {

        auto rs = std::make_unique<result_set>(metadata);
        for (auto&& role_to_sl : roles_to_att_val) {
            rs->add_row(std::vector<bytes_opt>{
                utf8_type->decompose(role_to_sl.first),
                utf8_type->decompose(role_to_sl.second),
            });
        }
        auto rows = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(std::move(rs))));
        return ::static_pointer_cast<cql_transport::messages::result_message>(rows);

    });
}


}
}
