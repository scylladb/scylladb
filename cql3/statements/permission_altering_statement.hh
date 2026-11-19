/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "authorization_statement.hh"
#include "auth/permission.hh"
#include "auth/resource.hh"

#include <functional>
#include <vector>

namespace cql3 {

class role_name;

namespace statements {

class permission_altering_statement : public authorization_altering_statement {
protected:
    auth::permission_set _permissions;
    mutable auth::resource _resource;
    sstring _role_name;
    // The cluster config options a CONFIGURE permission is narrowed to. Empty for all options.
    std::vector<sstring> _config_options;

    // Set for the ALL form. ALL leaves CONFIGURE out of _permissions.
    bool _all_permissions;

    // True for GRANT. REVOKE must also work on what can no longer be granted: a datacenter,
    // rack or node that left the topology, and an option that left the registry.
    virtual bool is_grant() const = 0;

    // Calls `f` with each resource the statement grants or revokes permissions on.
    future<> for_each_resource(std::function<future<>(const auth::resource&)> f) const;

public:
    permission_altering_statement(auth::permission_set, auth::resource, const cql3::role_name&, std::vector<sstring> config_options = {});

    future<> check_access(query_processor& qp, const service::client_state&) const override;
};

}

}
