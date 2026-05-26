/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/statements/batch_statement.hh"

namespace cql3::statements::strong_consistency {

class batch_statement : public cql_statement_opt_metadata {
    using result_message = cql_transport::messages::result_message;
    using inner_statement = cql3::statements::batch_statement;

    shared_ptr<inner_statement> _batch;
public:
    batch_statement(shared_ptr<inner_statement> batch);

    virtual future<shared_ptr<result_message>> execute(query_processor& qp, service::query_state& state,
        const query_options& options, std::optional<service::group0_guard> guard) const override;

    virtual future<shared_ptr<result_message>> execute_without_checking_exception_message(query_processor& qp,
        service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual uint32_t get_bound_terms() const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    virtual void validate(query_processor& qp, const service::client_state& state) const override;

    virtual std::optional<std::vector<shared_ptr<cql3::statements::modification_statement>>> get_batch_statements() const override;
};

}
