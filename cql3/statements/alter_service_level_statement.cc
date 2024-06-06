/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "seastarx.hh"
#include "cql3/statements/alter_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"

namespace cql3 {

namespace statements {

alter_service_level_statement::alter_service_level_statement(sstring service_level, shared_ptr<sl_prop_defs> attrs)
        : _service_level(service_level) {
    attrs->validate();
    _slo = attrs->get_service_level_options();
}

std::unique_ptr<cql3::statements::prepared_statement>
cql3::statements::alter_service_level_statement::prepare(
        data_dictionary::database db, cql_stats &stats) {
    return std::make_unique<prepared_statement>(::make_shared<alter_service_level_statement>(*this));
}

future<> alter_service_level_statement::check_access(query_processor& qp, const service::client_state &state) const {
    return state.ensure_has_permission(auth::command_desc{.permission = auth::permission::ALTER, .resource = auth::root_service_level_resource()});
}

future<::shared_ptr<cql_transport::messages::result_message>>
alter_service_level_statement::execute(query_processor& qp,
        service::query_state &state,
        const query_options &, std::optional<service::group0_guard> guard) const {
    service::group0_batch mc{std::move(guard)};
    qos::service_level& sl = state.get_service_level_controller().get_service_level(_service_level);
    qos::service_level_options slo = _slo.replace_defaults(sl.slo);
    auto& slc = state.get_service_level_controller();
    co_await slc.alter_distributed_service_level(_service_level, slo, mc);
    co_await slc.commit_mutations(std::move(mc));
    co_return nullptr;
}
}
}
