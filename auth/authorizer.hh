/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <string_view>
#include <stdexcept>
#include <tuple>
#include <vector>

#include <seastar/core/future.hh>

#include "auth/permission.hh"
#include "auth/resource.hh"
#include "service/raft/raft_group0_client.hh"
#include "seastarx.hh"

namespace auth {

class role_or_anonymous;

struct permission_details {
    sstring role_name;
    ::auth::resource resource;
    permission_set permissions;
};

inline bool operator==(const permission_details& pd1, const permission_details& pd2) {
    return std::forward_as_tuple(pd1.role_name, pd1.resource, pd1.permissions.mask())
            == std::forward_as_tuple(pd2.role_name, pd2.resource, pd2.permissions.mask());
}

inline bool operator<(const permission_details& pd1, const permission_details& pd2) {
    return std::forward_as_tuple(pd1.role_name, pd1.resource, pd1.permissions)
            < std::forward_as_tuple(pd2.role_name, pd2.resource, pd2.permissions);
}

class unsupported_authorization_operation : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

///
/// Abstract client for authorizing roles to access resources.
///
/// All state necessary to authorize a role is stored externally to the client instance.
///
class authorizer {
public:
    using ptr_type = std::unique_ptr<authorizer>;

    virtual ~authorizer() = default;

    virtual future<> start() = 0;

    virtual future<> stop() = 0;

    ///
    /// A fully-qualified (class with package) Java-like name for this implementation.
    ///
    virtual std::string_view qualified_java_name() const = 0;

    ///
    /// Query for the permissions granted directly to a role for a particular \ref resource (and not any of its
    /// parents).
    ///
    /// The optional role name is empty when an anonymous user is authorized. Some implementations may still wish to
    /// grant default permissions in this case.
    ///
    virtual future<permission_set> authorize(const role_or_anonymous&, const resource&) const = 0;

    ///
    /// Grant a set of permissions to a role for a particular \ref resource.
    ///
    /// \throws \ref unsupported_authorization_operation if granting permissions is not supported.
    ///
    virtual future<> grant(std::string_view role_name, permission_set, const resource&, ::service::group0_batch&) = 0;

    ///
    /// Revoke a set of permissions from a role for a particular \ref resource.
    ///
    /// \throws \ref unsupported_authorization_operation if revoking permissions is not supported.
    ///
    virtual future<> revoke(std::string_view role_name, permission_set, const resource&, ::service::group0_batch&) = 0;

    ///
    /// Query for all directly granted permissions.
    ///
    /// \throws \ref unsupported_authorization_operation if listing permissions is not supported.
    ///
    virtual future<std::vector<permission_details>> list_all() const = 0;

    ///
    /// Revoke all permissions granted directly to a particular role.
    ///
    /// \throws \ref unsupported_authorization_operation if revoking permissions is not supported.
    ///
    virtual future<> revoke_all(std::string_view role_name, ::service::group0_batch&) = 0;

    ///
    /// Revoke all permissions granted to any role for a particular resource.
    ///
    /// \throws \ref unsupported_authorization_operation if revoking permissions is not supported.
    ///
    virtual future<> revoke_all(const resource&, ::service::group0_batch&) = 0;

    ///
    /// System resources used internally as part of the implementation. These are made inaccessible to users.
    ///
    virtual const resource_set& protected_resources() const = 0;
};

}
