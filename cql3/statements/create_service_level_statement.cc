/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/service.hh"
#include "exceptions/exceptions.hh"
#include "seastarx.hh"
#include "cql3/statements/create_service_level_statement.hh"
#include "service/qos/qos_common.hh"
#include "service/qos/service_level_controller.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "utils/error_injection.hh"

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
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<create_service_level_statement>(*this));
}

future<> create_service_level_statement::check_access(query_processor& qp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::CREATE, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
create_service_level_statement::execute(query_processor& qp,
        service::query_state &state,
        const query_options &,
        std::optional<service::group0_guard> guard) const {
    if (_service_level.starts_with('$')) {
        throw exceptions::invalid_request_exception("Names starting with '$' are reserved for internal tenants. Use a different name.");
    }

    service::group0_batch mc{std::move(guard)};
    validate_shares_option(qp, _slo);
    
    auto default_slo = qos::service_level_options{.shares = qos::service_level_controller::default_shares};
    if (utils::get_local_injector().is_enabled("create_service_levels_without_default_shares")) {
        default_slo.shares = qos::service_level_options::unset_marker{};
    }
    qos::service_level_options slo = _slo.replace_defaults(default_slo);

    auto& sl = state.get_service_level_controller();
    co_await sl.add_distributed_service_level(_service_level, slo, _if_not_exists, mc);
    co_await sl.commit_mutations(std::move(mc));
    co_return nullptr;
}
}
}
