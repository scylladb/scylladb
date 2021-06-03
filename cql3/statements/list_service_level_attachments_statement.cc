/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "seastarx.hh"
#include "cql3/statements/list_service_level_attachments_statement.hh"
#include "service/qos/service_level_controller.hh"
#include "transport/messages/result_message.hh"

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
        database &db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<list_service_level_attachments_statement>(*this));
}

void list_service_level_attachments_statement::validate(service::storage_proxy &, const service::client_state &) const {
}

future<> list_service_level_attachments_statement::check_access(service::storage_proxy& sp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::DESCRIBE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
list_service_level_attachments_statement::execute(query_processor& qp,
        service::query_state &state,
        const query_options &) const {

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
    }).then([this, &state] (std::unordered_map<sstring, sstring> roles_to_att_val) {

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
