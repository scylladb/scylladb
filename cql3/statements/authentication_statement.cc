/*
 */

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

uint32_t cql3::statements::authentication_statement::get_bound_terms() const {
    return 0;
}

bool cql3::statements::authentication_statement::depends_on_keyspace(
                const sstring& ks_name) const {
    return false;
}

bool cql3::statements::authentication_statement::depends_on_column_family(
                const sstring& cf_name) const {
    return false;
}

void cql3::statements::authentication_statement::validate(
                query_processor&,
                const service::client_state& state) const {
}

future<> cql3::statements::authentication_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return make_ready_future<>();
}
