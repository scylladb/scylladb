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
#include "cql3/statements/drop_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

drop_service_level_statement::drop_service_level_statement(sstring service_level, bool if_exists) :
    _service_level(service_level), _if_exists(if_exists) {
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::drop_service_level_statement::prepare(
        database &db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<drop_service_level_statement>(*this));
}

void drop_service_level_statement::validate(service::storage_proxy &, const service::client_state &) const {
}

future<> drop_service_level_statement::check_access(service::storage_proxy& sp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::DROP, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
drop_service_level_statement::execute(query_processor& qp,
        service::query_state &state,
        const query_options &) const {
    return state.get_service_level_controller().drop_distributed_service_level(_service_level, _if_exists).then([] {
        using void_result_msg = cql_transport::messages::result_message::void_message;
        using result_msg = cql_transport::messages::result_message;
        return ::static_pointer_cast<result_msg>(make_shared<void_result_msg>());
    });
}
}
}
