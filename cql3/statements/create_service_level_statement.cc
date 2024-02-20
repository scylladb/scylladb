/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "seastarx.hh"
#include "cql3/statements/create_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"
#include "transport/messages/result_message.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"

namespace cql3 {

namespace statements {

create_service_level_statement::create_service_level_statement(sstring service_level, shared_ptr<sl_prop_defs> attrs, bool if_not_exists)
        : _service_level(service_level), _if_not_exists(if_not_exists) {
    attrs->validate();
    _slo = attrs->get_service_level_options();
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::create_service_level_statement::prepare(
        data_dictionary::database db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<create_service_level_statement>(*this));
}

future<> create_service_level_statement::check_access(query_processor& qp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::CREATE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
create_service_level_statement::execute(query_processor& qp,
        service::query_state &state,
        const query_options &,
        std::optional<service::group0_guard> guard) const {
    qos::service_level_options slo = _slo.replace_defaults(qos::service_level_options{});
    return state.get_service_level_controller().add_distributed_service_level(_service_level, slo, _if_not_exists, std::move(guard)).then([] {
        using void_result_msg = cql_transport::messages::result_message::void_message;
        using result_msg = cql_transport::messages::result_message;
        return ::static_pointer_cast<result_msg>(make_shared<void_result_msg>());
    });
}
}
}
