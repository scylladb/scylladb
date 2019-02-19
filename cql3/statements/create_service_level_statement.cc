/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "seastarx.hh"
#include "cql3/statements/create_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

create_service_level_statement::create_service_level_statement(sstring service_level, shared_ptr<sl_prop_defs> attrs, bool if_not_exists)
        : _service_level(service_level), _if_not_exists(if_not_exists) {
    attrs->validate();
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::create_service_level_statement::prepare(
        database &db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<create_service_level_statement>(*this));
}

void create_service_level_statement::validate(service::storage_proxy &, const service::client_state &) const {
}

future<> create_service_level_statement::check_access(service::storage_proxy& sp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::CREATE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
create_service_level_statement::execute(service::storage_proxy &sp,
        service::query_state &state,
        const query_options &) const {
    return state.get_service_level_controller().add_distributed_service_level(_service_level, _slo, _if_not_exists).then([] {
        using void_result_msg = cql_transport::messages::result_message::void_message;
        using result_msg = cql_transport::messages::result_message;
        return ::static_pointer_cast<result_msg>(make_shared<void_result_msg>());
    });
}
}
}
