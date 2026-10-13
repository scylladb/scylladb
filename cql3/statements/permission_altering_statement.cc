/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include <seastar/core/thread.hh>

#include "auth/service.hh"
#include "db/system_keyspace.hh"
#include "permission_altering_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/role_name.hh"
#include "gms/feature_service.hh"

static auth::permission_set filter_applicable_permissions(const auth::permission_set& ps, const auth::resource& r) {
    auto const filtered_permissions = auth::permission_set::from_mask(ps.mask() & r.applicable_permissions().mask());

    if (!filtered_permissions) {
        throw exceptions::syntax_exception(
                format("Resource {} does not support any of the requested permissions.", r));
    }

    return filtered_permissions;
}

cql3::statements::permission_altering_statement::permission_altering_statement(
                auth::permission_set permissions, auth::resource resource,
                const role_name& rn)
                : _permissions(filter_applicable_permissions(permissions, resource))
                , _resource(std::move(resource))
                , _role_name(rn.to_string()) {
}

future<> cql3::statements::permission_altering_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.ensure_not_anonymous();
    maybe_correct_resource(_resource, state, qp);

    if (_resource.kind() == auth::resource_kind::functions) {
        // The resource may be a builtin function or all builtin functions, in which case we disallow altering permissions on it.
        auth::functions_resource_view v(_resource);
        if (v.keyspace() && *v.keyspace() == db::system_keyspace::NAME) {
            throw exceptions::invalid_request_exception("Altering permissions on builtin functions is not supported");
        }
    }
    // check that the user has AUTHORIZE permission on the resource or its parents, otherwise reject
    // GRANT/REVOKE.
    co_await state.ensure_has_permission({auth::permission::AUTHORIZE, _resource});
    for (auth::permission p : _permissions) {
        // TODO: how about we re-write the access check to check a set
        // right away.
        co_await state.ensure_has_permission({p, _resource});
    }
    // Check that the resource exists only after checking access, so the error
    // doesn't tell an unauthorized user whether it exists (e.g., ON ROLE r).
    co_await state.ensure_exists(_resource);
}
