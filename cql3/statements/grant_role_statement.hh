/*
 * Copyright 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "cql3/role_name.hh"
#include "cql3/statements/authorization_statement.hh"
#include "seastarx.hh"

namespace cql3 {

class query_processor;

namespace statements {

class grant_role_statement final : public authorization_altering_statement {
    sstring _role;

    sstring _grantee;

public:
    grant_role_statement(const cql3::role_name& name, const cql3::role_name& grantee)
        : _role(name.to_string()), _grantee(grantee.to_string()) {
    }

    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    virtual future<> check_access(query_processor& qp, const service::client_state&) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state&, const query_options&, std::optional<service::group0_guard> guard) const override;
};

}

}
