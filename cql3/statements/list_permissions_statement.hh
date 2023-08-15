/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <optional>

#include "authorization_statement.hh"
#include "auth/permission.hh"
#include "auth/resource.hh"

namespace cql3 {

class query_processor;

namespace statements {

class list_permissions_statement : public authorization_statement {
private:
    auth::permission_set _permissions;
    mutable std::optional<auth::resource> _resource;
    std::optional<sstring> _role_name;
    bool _recursive;

public:
    list_permissions_statement(auth::permission_set, std::optional<auth::resource>, std::optional<sstring>, bool);

    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    void validate(query_processor&, const service::client_state&) const override;

    future<> check_access(query_processor& qp, const service::client_state&) const override;

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state& , const query_options&, std::optional<service::group0_guard> guard) const override;
};

}

}
