/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service_level_statement.hh"
#include "service/storage_proxy.hh"
#include "gms/feature_service.hh"

namespace cql3 {

namespace statements {

uint32_t service_level_statement::get_bound_terms() const {
    return 0;
}

bool service_level_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return false;
}

future<> service_level_statement::check_access(query_processor& qp, const service::client_state &state) const {
    return make_ready_future<>();
}

bool service_level_statement::needs_guard(query_processor&, service::query_state& state) const {
    return state.get_service_level_controller().is_v2();
}

audit::statement_category service_level_statement::category() const {
    return audit::statement_category::ADMIN;
}

audit::audit_info_ptr service_level_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), sstring(), sstring());
}

void service_level_statement::validate_shares_option(const query_processor& qp, const qos::service_level_options& slo) const {
    if (!std::holds_alternative<qos::service_level_options::unset_marker>(slo.shares) && !qp.proxy().features().workload_prioritization) {
        throw exceptions::invalid_request_exception("`shares` option can only be used when the cluster is fully upgraded to enterprise");
    }
}

}
}
