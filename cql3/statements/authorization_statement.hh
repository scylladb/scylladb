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

namespace auth {
class resource;
}

namespace cql3 {

namespace statements {

class authorization_statement : public raw::parsed_statement, public cql_statement_no_metadata {
public:
    authorization_statement() : cql_statement_no_metadata(&timeout_config::other_timeout) {}

    uint32_t get_bound_terms() const override;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;

protected:
    static void maybe_correct_resource(auth::resource&, const service::client_state&, query_processor&);
};

class authorization_altering_statement : public authorization_statement {
public:
    virtual bool needs_guard(query_processor& qp, service::query_state& state) const override;
};

}

}
