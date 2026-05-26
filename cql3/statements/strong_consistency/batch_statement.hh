/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/strong_consistency/modification_statement.hh"

namespace cql3::statements::strong_consistency {

class batch_statement : public cql_statement_opt_metadata {
    using result_message = cql_transport::messages::result_message;
public:
    using type = cql3::statements::batch_statement::type;

    struct single_statement {
        shared_ptr<modification_statement> statement;
        bool needs_authorization = true;

        single_statement(shared_ptr<modification_statement> s)
            : statement(std::move(s))
        {}
        single_statement(shared_ptr<modification_statement> s, bool na)
            : statement(std::move(s))
            , needs_authorization(na)
        {}
    };
private:
    int _bound_terms;
    type _type;
    std::vector<single_statement> _statements;
    std::unique_ptr<attributes> _attrs;

public:
    batch_statement(int bound_terms, type type_, std::vector<single_statement> statements, std::unique_ptr<attributes> attrs);

    batch_statement(type type_, std::vector<single_statement> statements, std::unique_ptr<attributes> attrs);

    virtual future<shared_ptr<result_message>> execute(query_processor& qp, service::query_state& state,
        const query_options& options, std::optional<service::group0_guard> guard) const override;

    virtual future<shared_ptr<result_message>> execute_without_checking_exception_message(query_processor& qp,
        service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual uint32_t get_bound_terms() const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    void validate() const;

    virtual void validate(query_processor& qp, const service::client_state& state) const override;

    const std::vector<single_statement>& get_statements() const {
        return _statements;
    }

    bool should_reclassify_control_connection() const override {
        return true;
    }
};

}
