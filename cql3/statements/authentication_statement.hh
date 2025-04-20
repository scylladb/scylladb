/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "raw/parsed_statement.hh"

namespace cql3 {

namespace statements {

class authentication_statement : public raw::parsed_statement, public cql_statement_no_metadata {
public:
    authentication_statement() : cql_statement_no_metadata(&timeout_config::other_timeout) {}

    uint32_t get_bound_terms() const override;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;
protected:
    virtual audit::statement_category category() const override;
    virtual audit::audit_info_ptr audit_info() const override {
        return audit::audit::create_audit_info(category(), sstring(), sstring());
    }
};

class authentication_altering_statement : public authentication_statement {
public:
     virtual bool needs_guard(query_processor& qp, service::query_state& state) const override;
};

}

}
