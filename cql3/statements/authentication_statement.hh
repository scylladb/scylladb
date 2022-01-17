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

#pragma once

#include "cql3/cql_statement.hh"
#include "raw/parsed_statement.hh"
#include "transport/messages_fwd.hh"

namespace cql3 {

namespace statements {

class authentication_statement : public raw::parsed_statement, public cql_statement_no_metadata {
public:
    authentication_statement() : cql_statement_no_metadata(&timeout_config::other_timeout) {}

    uint32_t get_bound_terms() const override;

    bool depends_on_keyspace(const sstring& ks_name) const override;

    bool depends_on_column_family(const sstring& cf_name) const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;

    void validate(query_processor&, const service::client_state& state) const override;
};

}

}
