/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"

namespace cql3 {

namespace statements {

class select_statement;

class rebuild_materialized_view_statement : public cql_statement_no_metadata {
    schema_ptr _view_schema;
    expr::expression _where_clause;
    std::unique_ptr<attributes> _attrs;

    
public:
    rebuild_materialized_view_statement(schema_ptr view_schema, expr::expression where_clause, std::unique_ptr<attributes> prepared_attrs);

    virtual uint32_t get_bound_terms() const override;
    
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;
    
    virtual void validate(query_processor&, const service::client_state& state) const override;
    
    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;
    
    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;
private:
    shared_ptr<cql3::statements::select_statement> build_base_select_statement(data_dictionary::database db) const;

    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;
};

}

}

