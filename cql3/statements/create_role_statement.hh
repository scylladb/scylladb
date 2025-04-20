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

#include "cql3/statements/authentication_statement.hh"
#include "cql3/role_name.hh"
#include "cql3/role_options.hh"

namespace cql3 {

class query_processor;

namespace statements {

class create_role_statement final : public authentication_altering_statement {
    sstring _role;

    role_options _options;

    bool _if_not_exists;

public:
    create_role_statement(
            const cql3::role_name& name, const role_options& options, bool if_not_exists)
                : _role(name.to_string())
                , _options(std::move(options))
                , _if_not_exists(if_not_exists) {
    }

    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    virtual future<> check_access(query_processor& qp, const service::client_state&) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state&, const query_options&, std::optional<service::group0_guard> guard) const override;

    virtual void sanitize_audit_info() override;

private:
    future<> grant_permissions_to_creator(const service::client_state&, ::service::group0_batch&) const;
};

}

}
