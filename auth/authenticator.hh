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
 * Copyright (C) 2016 ScyllaDB
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

#include <memory>
#include <unordered_map>
#include <set>
#include <stdexcept>
#include <boost/any.hpp>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/enum.hh>

#include "bytes.hh"
#include "data_resource.hh"
#include "enum_set.hh"
#include "exceptions/exceptions.hh"

namespace db {
    class config;
}

namespace auth {

class authenticated_user;

class authenticator {
public:
    static const sstring USERNAME_KEY;
    static const sstring PASSWORD_KEY;

    /**
     * Supported CREATE USER/ALTER USER options.
     * Currently only PASSWORD is available.
     */
    enum class option {
        PASSWORD
    };

    static option string_to_option(const sstring&);
    static sstring option_to_string(option);

    using option_set = enum_set<super_enum<option, option::PASSWORD>>;
    using option_map = std::unordered_map<option, boost::any, enum_hash<option>>;
    using credentials_map = std::unordered_map<sstring, sstring>;

    virtual ~authenticator()
    {}

    virtual future<> start() = 0;

    virtual future<> stop() = 0;

    virtual const sstring& qualified_java_name() const = 0;

    /**
     * Whether or not the authenticator requires explicit login.
     * If false will instantiate user with AuthenticatedUser.ANONYMOUS_USER.
     */
    virtual bool require_authentication() const = 0;

    /**
     * Set of options supported by CREATE USER and ALTER USER queries.
     * Should never return null - always return an empty set instead.
     */
    virtual option_set supported_options() const = 0;

    /**
     * Subset of supportedOptions that users are allowed to alter when performing ALTER USER [themselves].
     * Should never return null - always return an empty set instead.
     */
    virtual option_set alterable_options() const = 0;

    /**
     * Authenticates a user given a Map<String, String> of credentials.
     * Should never return null - always throw AuthenticationException instead.
     * Returning AuthenticatedUser.ANONYMOUS_USER is an option as well if authentication is not required.
     *
     * @throws authentication_exception if credentials don't match any known user.
     */
    virtual future<::shared_ptr<authenticated_user>> authenticate(const credentials_map& credentials) const = 0;

    /**
     * Called during execution of CREATE USER query (also may be called on startup, see seedSuperuserOptions method).
     * If authenticator is static then the body of the method should be left blank, but don't throw an exception.
     * options are guaranteed to be a subset of supportedOptions().
     *
     * @param username Username of the user to create.
     * @param options Options the user will be created with.
     * @throws exceptions::request_validation_exception
     * @throws exceptions::request_execution_exception
     */
    virtual future<> create(sstring username, const option_map& options) = 0;

    /**
     * Called during execution of ALTER USER query.
     * options are always guaranteed to be a subset of supportedOptions(). Furthermore, if the user performing the query
     * is not a superuser and is altering himself, then options are guaranteed to be a subset of alterableOptions().
     * Keep the body of the method blank if your implementation doesn't support any options.
     *
     * @param username Username of the user that will be altered.
     * @param options Options to alter.
     * @throws exceptions::request_validation_exception
     * @throws exceptions::request_execution_exception
     */
    virtual future<> alter(sstring username, const option_map& options) = 0;


    /**
     * Called during execution of DROP USER query.
     *
     * @param username Username of the user that will be dropped.
     * @throws exceptions::request_validation_exception
     * @throws exceptions::request_execution_exception
     */
    virtual future<> drop(sstring username) = 0;

     /**
     * Set of resources that should be made inaccessible to users and only accessible internally.
     *
     * @return Keyspaces, column families that will be unmodifiable by users; other resources.
     * @see resource_ids
     */
    virtual const resource_ids& protected_resources() const = 0;

    class sasl_challenge {
    public:
        virtual ~sasl_challenge() {}
        virtual bytes evaluate_response(bytes_view client_response) = 0;
        virtual bool is_complete() const = 0;
        virtual future<::shared_ptr<authenticated_user>> get_authenticated_user() const = 0;
    };

    /**
     * Provide a sasl_challenge to be used by the CQL binary protocol server. If
     * the configured authenticator requires authentication but does not implement this
     * interface we refuse to start the binary protocol server as it will have no way
     * of authenticating clients.
     * @return sasl_challenge implementation
     */
    virtual ::shared_ptr<sasl_challenge> new_sasl_challenge() const = 0;
};

inline std::ostream& operator<<(std::ostream& os, authenticator::option opt) {
    return os << authenticator::option_to_string(opt);
}

}

