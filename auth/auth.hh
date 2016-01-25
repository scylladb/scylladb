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
 * Copyright 2016 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include <chrono>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

#include "exceptions/exceptions.hh"

namespace auth {

class auth {
public:
    static const sstring DEFAULT_SUPERUSER_NAME;
    static const sstring AUTH_KS;
    static const sstring USERS_CF;
    static const std::chrono::milliseconds SUPERUSER_SETUP_DELAY;

    static bool is_class_type(const sstring& type, const sstring& classname);

#if 0
    public static Set<Permission> getPermissions(AuthenticatedUser user, IResource resource)
    {
        return permissionsCache.getPermissions(user, resource);
    }
#endif

    /**
     * Checks if the username is stored in AUTH_KS.USERS_CF.
     *
     * @param username Username to query.
     * @return whether or not Cassandra knows about the user.
     */
    static future<bool> is_existing_user(const sstring& username);

    /**
     * Checks if the user is a known superuser.
     *
     * @param username Username to query.
     * @return true is the user is a superuser, false if they aren't or don't exist at all.
     */
    static future<bool> is_super_user(const sstring& username);

    /**
     * Inserts the user into AUTH_KS.USERS_CF (or overwrites their superuser status as a result of an ALTER USER query).
     *
     * @param username Username to insert.
     * @param isSuper User's new status.
     * @throws RequestExecutionException
     */
    static future<> insert_user(const sstring& username, bool is_super) throw(exceptions::request_execution_exception);

    /**
     * Deletes the user from AUTH_KS.USERS_CF.
     *
     * @param username Username to delete.
     * @throws RequestExecutionException
     */
    static future<> delete_user(const sstring& username) throw(exceptions::request_execution_exception);

    /**
     * Sets up Authenticator and Authorizer.
     */
    static future<> setup();

    /**
     * Set up table from given CREATE TABLE statement under system_auth keyspace, if not already done so.
     *
     * @param name name of the table
     * @param cql CREATE TABLE statement
     */
    static future<> setup_table(const sstring& name, const sstring& cql);

    static future<bool> has_existing_users(const sstring& cfname, const sstring& def_user_name, const sstring& name_column_name);

    // For internal use. Run function "when system is up".
    typedef std::function<future<>()> scheduled_func;
    static void schedule_when_up(scheduled_func);
};
}
