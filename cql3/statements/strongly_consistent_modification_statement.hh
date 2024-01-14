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

namespace cql3 {

namespace statements {

namespace broadcast_tables {

struct prepared_update {
    expr::expression key;
    expr::expression new_value;
    std::optional<expr::expression> value_condition;
};

}

class strongly_consistent_modification_statement : public cql_statement_opt_metadata {
    const uint32_t _bound_terms;
    const schema_ptr _schema;
    const broadcast_tables::prepared_update _query;

public:
    strongly_consistent_modification_statement(uint32_t bound_terms, schema_ptr schema, broadcast_tables::prepared_update query);

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    virtual uint32_t get_bound_terms() const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;
};


}

}
