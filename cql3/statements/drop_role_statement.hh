/*
 * Copyright 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "cql3/role_name.hh"
#include "cql3/statements/authentication_statement.hh"
#include "seastarx.hh"

namespace cql3 {

class query_processor;

namespace statements {

class drop_role_statement final : public authentication_altering_statement {
    sstring _role;

    bool _if_exists;

public:
    drop_role_statement(const cql3::role_name& name, bool if_exists) : _role(name.to_string()), _if_exists(if_exists) {
    }

    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    virtual void validate(query_processor&, const service::client_state&) const override;

    virtual future<> check_access(query_processor& qp, const service::client_state&) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state&, const query_options&, std::optional<service::group0_guard> guard) const override;
};

}

}
