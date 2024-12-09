/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/query_processor.hh"
#include "raw/parsed_statement.hh"
#include "service/query_state.hh"

namespace cql3 {

namespace statements {

class service_level_statement : public raw::parsed_statement, public cql_statement_no_metadata {
public:
    service_level_statement() : cql_statement_no_metadata(&timeout_config::other_timeout) {}

    virtual bool needs_guard(query_processor& qp, service::query_state& state) const override;

    uint32_t get_bound_terms() const override;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;
};

}
}
