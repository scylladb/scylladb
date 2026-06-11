/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "transport/messages_fwd.hh"
#include "cql3/cql_statement.hh"

namespace cql3 {

class query_processor;

namespace statements {

class use_statement : public cql_statement_no_metadata {
private:
    const seastar::sstring _keyspace;

public:
    use_statement(seastar::sstring keyspace);

    virtual uint32_t get_bound_terms() const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    // Only selects the active keyspace, it does not touch user data.
    bool should_reclassify_control_connection() const override {
        return false;
    }

    virtual seastar::future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual seastar::future<seastar::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;
};

}

}
