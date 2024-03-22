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

///
/// A logical argument error for a service_level statement operation.
///
class service_level_argument_exception : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

///
/// An exception to indicate that the service level given as parameter doesn't exist.
///
class nonexitent_service_level_exception : public service_level_argument_exception {
public:
    nonexitent_service_level_exception(sstring service_level_name)
            : service_level_argument_exception(format("Service Level {} doesn't exists.", service_level_name)) {
    }
};




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
