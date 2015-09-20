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
 * Copyright 2015 Cloudius Systems
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

#include "exceptions/exceptions.hh"
#include "unimplemented.hh"
#include "timestamp.hh"
#include "db_clock.hh"
#include "database.hh"

namespace service {

/**
 * State related to a client connection.
 */
class client_state {
private:
    sstring _keyspace;
#if 0
    private static final Logger logger = LoggerFactory.getLogger(ClientState.class);
    public static final SemanticVersion DEFAULT_CQL_VERSION = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

    private static final Set<IResource> READABLE_SYSTEM_RESOURCES = new HashSet<>();
    private static final Set<IResource> PROTECTED_AUTH_RESOURCES = new HashSet<>();

    static
    {
        // We want these system cfs to be always readable to authenticated users since many tools rely on them
        // (nodetool, cqlsh, bulkloader, etc.)
        for (String cf : Iterables.concat(Arrays.asList(SystemKeyspace.LOCAL, SystemKeyspace.PEERS), LegacySchemaTables.ALL))
            READABLE_SYSTEM_RESOURCES.add(DataResource.columnFamily(SystemKeyspace.NAME, cf));

        PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthenticator().protectedResources());
        PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthorizer().protectedResources());
    }

    // Current user for the session
    private volatile AuthenticatedUser user;
    private volatile String keyspace;
#endif

public:
    struct internal_tag {};
    struct external_tag {};

    // isInternal is used to mark ClientState as used by some internal component
    // that should have an ability to modify system keyspace.
    const bool _is_internal;

    // The biggest timestamp that was returned by getTimestamp/assigned to a query
    api::timestamp_type _last_timestamp_micros = 0;

    // Note: Origin passes here a RemoteAddress parameter, but it doesn't seem to be used
    // anywhere so I didn't bother converting it.
    client_state(external_tag) : _is_internal(false) {
        warn(unimplemented::cause::AUTH);
#if 0
            if (!DatabaseDescriptor.getAuthenticator().requireAuthentication())
                this.user = AuthenticatedUser.ANONYMOUS_USER;
#endif
    }

    client_state(internal_tag) : _keyspace("system"), _is_internal(true) {}

    virtual bool is_thrift() const {
        return false;
    }

    /**
     * @return a ClientState object for internal C* calls (not limited by any kind of auth).
     */
    static client_state for_internal_calls() {
        return client_state(internal_tag());
    }

    /**
     * @return a ClientState object for external clients (thrift/native protocol users).
     */
    static client_state for_external_calls() {
        return client_state(external_tag());
    }

    /**
     * This clock guarantees that updates for the same ClientState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    api::timestamp_type get_timestamp() {
        auto current = db_clock::now().time_since_epoch().count() * 1000;
        auto last = _last_timestamp_micros;
        auto result = last >= current ? last + 1 : current;
        _last_timestamp_micros = result;
        return result;
    }

#if 0
    /**
     * Can be use when a timestamp has been assigned by a query, but that timestamp is
     * not directly one returned by getTimestamp() (see SP.beginAndRepairPaxos()).
     * This ensure following calls to getTimestamp() will return a timestamp strictly
     * greated than the one provided to this method.
     */
    public void updateLastTimestamp(long tstampMicros)
    {
        while (true)
        {
            long last = lastTimestampMicros.get();
            if (tstampMicros <= last || lastTimestampMicros.compareAndSet(last, tstampMicros))
                return;
        }
    }

    public SocketAddress getRemoteAddress()
    {
        return remoteAddress;
    }
#endif

    const sstring& get_raw_keyspace() const {
        return _keyspace;
    }

public:
    void set_keyspace(seastar::sharded<database>& db, sstring keyspace) {
        // Skip keyspace validation for non-authenticated users. Apparently, some client libraries
        // call set_keyspace() before calling login(), and we have to handle that.
#if 0
        if (user && Schema.instance.getKSMetaData(ks) == null) {
#endif
        if (!db.local().has_keyspace(keyspace)) {
            throw exceptions::invalid_request_exception(sprint("Keyspace '%s' does not exist", keyspace));
        }
        _keyspace = keyspace;
    }

    const sstring& get_keyspace() const {
        if (_keyspace.empty()) {
            throw exceptions::invalid_request_exception("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
        }
        return _keyspace;
    }

#if 0
    /**
     * Attempts to login the given user.
     */
    public void login(AuthenticatedUser user) throws AuthenticationException
    {
        if (!user.isAnonymous() && !Auth.isExistingUser(user.getName()))
           throw new AuthenticationException(String.format("User %s doesn't exist - create it with CREATE USER query first",
                                                           user.getName()));
        this.user = user;
    }

    public void hasAllKeyspacesAccess(Permission perm) throws UnauthorizedException
    {
        if (isInternal)
            return;
        validateLogin();
        ensureHasPermission(perm, DataResource.root());
    }

    public void hasKeyspaceAccess(String keyspace, Permission perm) throws UnauthorizedException, InvalidRequestException
    {
        hasAccess(keyspace, perm, DataResource.keyspace(keyspace));
    }

    public void hasColumnFamilyAccess(String keyspace, String columnFamily, Permission perm)
    throws UnauthorizedException, InvalidRequestException
    {
        ThriftValidation.validateColumnFamily(keyspace, columnFamily);
        hasAccess(keyspace, perm, DataResource.columnFamily(keyspace, columnFamily));
    }

    private void hasAccess(String keyspace, Permission perm, DataResource resource)
    throws UnauthorizedException, InvalidRequestException
    {
        validateKeyspace(keyspace);
        if (isInternal)
            return;
        validateLogin();
        preventSystemKSSchemaModification(keyspace, resource, perm);
        if ((perm == Permission.SELECT) && READABLE_SYSTEM_RESOURCES.contains(resource))
            return;
        if (PROTECTED_AUTH_RESOURCES.contains(resource))
            if ((perm == Permission.CREATE) || (perm == Permission.ALTER) || (perm == Permission.DROP))
                throw new UnauthorizedException(String.format("%s schema is protected", resource));
        ensureHasPermission(perm, resource);
    }

    public void ensureHasPermission(Permission perm, IResource resource) throws UnauthorizedException
    {
        for (IResource r : Resources.chain(resource))
            if (authorize(r).contains(perm))
                return;

        throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents",
                                                      user.getName(),
                                                      perm,
                                                      resource));
    }

    private void preventSystemKSSchemaModification(String keyspace, DataResource resource, Permission perm) throws UnauthorizedException
    {
        // we only care about schema modification.
        if (!((perm == Permission.ALTER) || (perm == Permission.DROP) || (perm == Permission.CREATE)))
            return;

        // prevent system keyspace modification
        if (SystemKeyspace.NAME.equalsIgnoreCase(keyspace))
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");

        // we want to allow altering AUTH_KS and TRACING_KS.
        Set<String> allowAlter = Sets.newHashSet(Auth.AUTH_KS, TraceKeyspace.NAME);
        if (allowAlter.contains(keyspace.toLowerCase()) && !(resource.isKeyspaceLevel() && (perm == Permission.ALTER)))
            throw new UnauthorizedException(String.format("Cannot %s %s", perm, resource));
    }
#endif

public:
    void validate_login() const {
        warn(unimplemented::cause::AUTH);
#if 0
        if (user == null)
            throw new UnauthorizedException("You have not logged in");
#endif
    }

#if 0
    public void ensureNotAnonymous() throws UnauthorizedException
    {
        validateLogin();
        if (user.isAnonymous())
            throw new UnauthorizedException("You have to be logged in and not anonymous to perform this request");
    }

    public void ensureIsSuper(String message) throws UnauthorizedException
    {
        if (DatabaseDescriptor.getAuthenticator().requireAuthentication() && (user == null || !user.isSuper()))
            throw new UnauthorizedException(message);
    }

    private static void validateKeyspace(String keyspace) throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
    }

    public AuthenticatedUser getUser()
    {
        return user;
    }

    public static SemanticVersion[] getCQLSupportedVersion()
    {
        return new SemanticVersion[]{ QueryProcessor.CQL_VERSION };
    }

    private Set<Permission> authorize(IResource resource)
    {
        // AllowAllAuthorizer or manually disabled caching.
        if (Auth.permissionsCache == null)
            return DatabaseDescriptor.getAuthorizer().authorize(user, resource);

        try
        {
            return Auth.permissionsCache.get(Pair.create(user, resource));
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
#endif
};

}
