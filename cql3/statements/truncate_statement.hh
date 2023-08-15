/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"

namespace cql3 {

class query_processor;

namespace statements {

class truncate_statement : public cql_statement_no_metadata {
    schema_ptr _schema;
    const std::unique_ptr<attributes> _attrs;
public:
    truncate_statement(schema_ptr schema, std::unique_ptr<attributes> prepared_attrs);
    truncate_statement(const truncate_statement&);

    const sstring& keyspace() const;

    const sstring& column_family() const;

    virtual uint32_t get_bound_terms() const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual void validate(query_processor&, const service::client_state& state) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;
private:
    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;
};

}

}
