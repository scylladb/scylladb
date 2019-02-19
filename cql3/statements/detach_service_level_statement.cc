/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "seastarx.hh"
#include "cql3/statements/detach_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

detach_service_level_statement::detach_service_level_statement(sstring role_name) :
    _role_name(role_name) {
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::detach_service_level_statement::prepare(
        database &db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<detach_service_level_statement>(*this));
}

void detach_service_level_statement::validate(service::storage_proxy &, const service::client_state &) const {
}

future<> detach_service_level_statement::check_access(service::storage_proxy& sp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::AUTHORIZE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
detach_service_level_statement::execute(service::storage_proxy &sp,
        service::query_state &state,
        const query_options &) const {
    return state.get_client_state().get_auth_service()->underlying_role_manager().remove_attribute(_role_name, "service_level").then([] {
        using void_result_msg = cql_transport::messages::result_message::void_message;
        using result_msg = cql_transport::messages::result_message;
        return ::static_pointer_cast<result_msg>(make_shared<void_result_msg>());
    });
}
}
}
