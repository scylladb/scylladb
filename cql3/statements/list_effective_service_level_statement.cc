/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "list_effective_service_level_statement.hh"
#include "auth/role_manager.hh"
#include "cql3/statements/prepared_statement.hh"
#include <memory>
#include "exceptions/exceptions.hh"
#include "service/qos/qos_common.hh"
#include "service/query_state.hh"
#include "cql3/result_set.hh"
#include "types/types.hh"
#include "duration.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

list_effective_service_level_statement::list_effective_service_level_statement(sstring role_name)
: _role_name(std::move(role_name)) {}

std::unique_ptr<prepared_statement> 
list_effective_service_level_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<list_effective_service_level_statement>(*this));
}

static auto make_column(sstring name, const shared_ptr<const abstract_type> type) {
    return make_lw_shared<column_specification>(
        "QOS",
        "effective_service_level",
        ::make_shared<column_identifier>(std::move(name), true),
        type);
};

static bytes_opt decompose_timeout (const qos::service_level_options::timeout_type& duration) {
    return std::visit(overloaded_functor{
        [&] (const qos::service_level_options::unset_marker&) {
            return bytes_opt();
        },
        [&] (const qos::service_level_options::delete_marker&) {
            return bytes_opt();
        },
        [&] (const lowres_clock::duration& d) -> bytes_opt {
            auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
            return utf8_type->decompose(to_string(cql_duration(months_counter{0}, days_counter{0}, nanoseconds_counter{nanos})));
        },
    }, duration);
};

future<::shared_ptr<cql_transport::messages::result_message>>
list_effective_service_level_statement::execute(query_processor& qp, service::query_state& state, const query_options&, std::optional<service::group0_guard>) const {
    static thread_local const std::vector<lw_shared_ptr<column_specification>> metadata({
        make_column("service_level_option", utf8_type),
        make_column("effective_service_level", utf8_type),
        make_column("value", utf8_type)
    });
    auto& role_manager = state.get_client_state().get_auth_service()->underlying_role_manager();

    if (!co_await role_manager.exists(_role_name)) {
        throw auth::nonexistant_role(_role_name);
    }

    auto& sl_controller = state.get_service_level_controller();
    auto slo = co_await sl_controller.find_effective_service_level(_role_name);

    if (!slo) {
        throw exceptions::invalid_request_exception(format("Role {} doesn't have assigned any service level", _role_name));
    }

    auto rs = std::make_unique<result_set>(metadata);
    rs->add_row({
        utf8_type->decompose("workload_type"),
        utf8_type->decompose(slo->effective_names->workload),
        utf8_type->decompose(qos::service_level_options::to_string(slo->workload))
    });
    rs->add_row({
        utf8_type->decompose("timeout"),
        utf8_type->decompose(slo->effective_names->timeout),
        decompose_timeout(slo->timeout)
    });

    auto rows = ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(std::move(rs))));
    co_return ::static_pointer_cast<cql_transport::messages::result_message>(rows);    
}

}

}