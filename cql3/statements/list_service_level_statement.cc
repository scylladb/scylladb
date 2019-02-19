/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "seastarx.hh"
#include "cql3/statements/list_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

list_service_level_statement::list_service_level_statement(sstring service_level, bool describe_all) :
    _service_level(service_level), _describe_all(describe_all) {
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::list_service_level_statement::prepare(
        database &db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<list_service_level_statement>(*this));
}

void list_service_level_statement::validate(service::storage_proxy &, const service::client_state &) const {
}

future<> list_service_level_statement::check_access(service::storage_proxy& sp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::DESCRIBE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
list_service_level_statement::execute(service::storage_proxy &sp,
        service::query_state &state,
        const query_options &) const {

    static auto make_column = [] (sstring name, const shared_ptr<const abstract_type> type) {
        return make_lw_shared<column_specification>(
                "QOS",
                "service_levels",
                ::make_shared<column_identifier>(std::move(name), true),
                type);
    };

    static thread_local const std::vector<lw_shared_ptr<column_specification>> metadata({make_column("service_level", utf8_type)});

    return make_ready_future().then([this, &state] () {
                                  if (_describe_all) {
                                      return state.get_service_level_controller().get_distributed_service_levels();
                                  } else {
                                      return state.get_service_level_controller().get_distributed_service_level(_service_level);
                                  }
                              })
            .then([this] (qos::service_levels_info sl_info) {
                auto rs = std::make_unique<result_set>(metadata);
                for (auto &&sl : sl_info) {
                    rs->add_row(std::vector<bytes_opt>{
                            utf8_type->decompose(sl.first)});
                }

                auto rows = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(std::move(rs))));
                return ::static_pointer_cast<cql_transport::messages::result_message>(rows);
            });
}
}
}
