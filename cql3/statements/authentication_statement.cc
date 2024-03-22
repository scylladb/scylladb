/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "authentication_statement.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "auth/common.hh"

uint32_t cql3::statements::authentication_statement::get_bound_terms() const {
    return 0;
}

bool cql3::statements::authentication_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return false;
}

future<> cql3::statements::authentication_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return make_ready_future<>();
}

bool cql3::statements::authentication_altering_statement::needs_guard(query_processor& qp, service::query_state&) const {
    return !auth::legacy_mode(qp);
}
