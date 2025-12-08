/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/cql_statement.hh"

namespace cql3 {

namespace statements {

class strongly_consistent_statement : public cql_statement_opt_metadata {
    public:
    seastar::shared_ptr<cql_statement> statement;

    strongly_consistent_statement(seastar::shared_ptr<cql_statement> statement);

    virtual uint32_t get_bound_terms() const override;

    virtual seastar::future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_direct(query_processor& qp, service::query_state& qs, const query_options& options) const;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    virtual seastar::shared_ptr<const metadata> get_result_metadata() const override;

};

}

}
