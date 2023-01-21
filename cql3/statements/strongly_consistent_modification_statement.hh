/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "service/broadcast_tables/experimental/lang.hh"

namespace cql3 {

namespace statements {

class strongly_consistent_modification_statement : public cql_statement_opt_metadata {
    const uint32_t _bound_terms;
    const schema_ptr _schema;
    const service::broadcast_tables::update_query _query;

public:
    strongly_consistent_modification_statement(uint32_t bound_terms, schema_ptr schema, service::broadcast_tables::update_query query);

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options) const override;

    virtual uint32_t get_bound_terms() const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    // Validate before execute, using client state and current schema
    void validate(query_processor&, const service::client_state& state) const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;
};


}

}
