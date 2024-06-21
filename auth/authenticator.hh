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
#include <memory>
#include <unordered_map>
#include <optional>
#include <functional>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "auth/authentication_options.hh"
#include "auth/resource.hh"
#include "auth/sasl_challenge.hh"

#include "service/raft/raft_group0_client.hh"

namespace db {
    class config;
}

namespace auth {

class authenticated_user;

// Query alt name info as a single (subject style) string
using alt_name_func = std::function<future<std::string>()>;

struct certificate_info {
    std::string subject;
    alt_name_func get_alt_names;
};

using session_dn_func = std::function<future<std::optional<certificate_info>>()>;

///
/// Abstract client for authenticating role identity.
///
/// All state necessary to authorize a role is stored externally to the client instance.
///
class authenticator {
public:
    using ptr_type = std::unique_ptr<authenticator>;

    ///
    /// The name of the key to be used for the user-name part of password authentication with \ref authenticate.
    ///
    static const sstring USERNAME_KEY;

    ///
    /// The name of the key to be used for the password part of password authentication with \ref authenticate.
    ///
    static const sstring PASSWORD_KEY;

    using credentials_map = std::unordered_map<sstring, sstring>;

    virtual ~authenticator() = default;

    virtual future<> start() = 0;

    virtual future<> stop() = 0;

    ///
    /// A fully-qualified (class with package) Java-like name for this implementation.
    ///
    virtual std::string_view qualified_java_name() const = 0;

    virtual bool require_authentication() const = 0;

    virtual authentication_option_set supported_options() const = 0;

    ///
    /// A subset of `supported_options()` that users are permitted to alter for themselves.
    ///
    virtual authentication_option_set alterable_options() const = 0;

    ///
    /// Authenticate a user given implementation-specific credentials.
    ///
    /// If this implementation does not require authentication (\ref require_authentication), an anonymous user may
    /// result.
    ///
    /// \returns an exceptional future with \ref exceptions::authentication_exception if given invalid credentials.
    ///
    virtual future<authenticated_user> authenticate(const credentials_map& credentials) const = 0;

    ///
    /// Authenticate (early) using transport info
    ///
    /// \returns nullopt if not supported/required. exceptional future if failed
    ///
    virtual future<std::optional<authenticated_user>> authenticate(session_dn_func) const;

    ///
    /// Create an authentication record for a new user. This is required before the user can log-in.
    ///
    /// The options provided must be a subset of `supported_options()`.
    ///
    virtual future<> create(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) = 0;

    ///
    /// Alter the authentication record of an existing user.
    ///
    /// The options provided must be a subset of `supported_options()`.
    ///
    /// Callers must ensure that the specification of `alterable_options()` is adhered to.
    ///
    virtual future<> alter(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) = 0;

    ///
    /// Delete the authentication record for a user. This will disallow the user from logging in.
    ///
    virtual future<> drop(std::string_view role_name, ::service::group0_batch&) = 0;

    ///
    /// Query for custom options (those corresponding to \ref authentication_options::options).
    ///
    /// If no options are set the result is an empty container.
    ///
    virtual future<custom_options> query_custom_options(std::string_view role_name) const = 0;

    ///
    /// System resources used internally as part of the implementation. These are made inaccessible to users.
    ///
    virtual const resource_set& protected_resources() const = 0;

    virtual ::shared_ptr<sasl_challenge> new_sasl_challenge() const = 0;
};

}

