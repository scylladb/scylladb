/*
 * Copyright 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <optional>

#include <seastar/core/sstring.hh>

#include "cql3/statements/authorization_statement.hh"
#include "cql3/role_name.hh"
#include "seastarx.hh"

namespace cql3 {

class query_processor;

namespace statements {

class list_roles_statement final : public authorization_statement {
    std::optional<sstring> _grantee;

    bool _recursive;

public:
    list_roles_statement(const std::optional<role_name>& grantee, bool recursive)
        : _grantee(grantee ? sstring(grantee->to_string()) : std::optional<sstring>()), _recursive(recursive) {}

    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    virtual future<> check_access(query_processor& qp, const service::client_state&) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state&, const query_options&, std::optional<service::group0_guard> guard) const override;
};

}

}
