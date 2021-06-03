/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string_view>
#include <memory>
#include <set>
#include <stdexcept>
#include <unordered_map>

#include <seastar/core/enum.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>

#include "auth/authentication_options.hh"
#include "auth/resource.hh"
#include "auth/sasl_challenge.hh"
#include "bytes.hh"
#include "enum_set.hh"
#include "exceptions/exceptions.hh"

namespace db {
    class config;
}

namespace auth {

class authenticated_user;

///
/// Abstract client for authenticating role identity.
///
/// All state necessary to authorize a role is stored externally to the client instance.
///
class authenticator {
public:
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
    /// Create an authentication record for a new user. This is required before the user can log-in.
    ///
    /// The options provided must be a subset of `supported_options()`.
    ///
    virtual future<> create(std::string_view role_name, const authentication_options& options) const = 0;

    ///
    /// Alter the authentication record of an existing user.
    ///
    /// The options provided must be a subset of `supported_options()`.
    ///
    /// Callers must ensure that the specification of `alterable_options()` is adhered to.
    ///
    virtual future<> alter(std::string_view role_name, const authentication_options& options) const = 0;

    ///
    /// Delete the authentication record for a user. This will disallow the user from logging in.
    ///
    virtual future<> drop(std::string_view role_name) const = 0;

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

