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
 * Copyright (C) 2015 ScyllaDB
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

#include "auth/service.hh"
#include "exceptions/exceptions.hh"
#include "unimplemented.hh"
#include "timestamp.hh"
#include "db_clock.hh"
#include "database.hh"
#include "auth/authenticated_user.hh"
#include "auth/authenticator.hh"
#include "auth/permission.hh"
#include "tracing/tracing.hh"
#include "tracing/trace_state.hh"

namespace service {

/**
 * State related to a client connection.
 */
class client_state {
private:
    sstring _keyspace;
    tracing::trace_state_ptr _trace_state_ptr;
    lw_shared_ptr<utils::UUID> _tracing_session_id;
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
    ::shared_ptr<auth::authenticated_user> _user;

    // isInternal is used to mark ClientState as used by some internal component
    // that should have an ability to modify system keyspace.
    bool _is_internal;
    bool _is_thrift;

    // The biggest timestamp that was returned by getTimestamp/assigned to a query
    api::timestamp_type _last_timestamp_micros = 0;

    bool _dirty = false;

    // Address of a client
    socket_address _remote_address;

    // Only populated for external client state.
    auth::service* _auth_service{nullptr};
public:
    struct internal_tag {};
    struct external_tag {};

    void create_tracing_session(tracing::trace_type type, tracing::trace_state_props_set props) {
        _trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(type, props);
        // store a session ID separately because its lifetime is not always
        // coupled with the trace_state because the trace_state may already be
        // destroyed when we need a session ID for a response to a client (e.g.
        // in case of errors).
        if (_trace_state_ptr) {
            _tracing_session_id = make_lw_shared<utils::UUID>(_trace_state_ptr->session_id());
        }
    }

    tracing::trace_state_ptr& get_trace_state() {
        return _trace_state_ptr;
    }

    const tracing::trace_state_ptr& get_trace_state() const {
        return _trace_state_ptr;
    }

    client_state(external_tag, auth::service& auth_service, const socket_address& remote_address = socket_address(), bool thrift = false)
            : _is_internal(false)
            , _is_thrift(thrift)
            , _remote_address(remote_address)
            , _auth_service(&auth_service) {
        if (!auth_service.underlying_authenticator().require_authentication()) {
            _user = ::make_shared<auth::authenticated_user>();
        }
    }

    gms::inet_address get_client_address() const {
        return gms::inet_address(_remote_address);
    }

    client_state(internal_tag) : _keyspace("system"), _is_internal(true), _is_thrift(false) {}

    // `nullptr` for internal instances.
    auth::service* get_auth_service() {
        return _auth_service;
    }

    // See above.
    const auth::service* get_auth_service() const {
        return _auth_service;
    }

    void merge(const client_state& other);

    bool is_thrift() const {
        return _is_thrift;
    }

    bool is_internal() const {
        return _is_internal;
    }

    /**
     * @return a ClientState object for internal C* calls (not limited by any kind of auth).
     */
    static client_state for_internal_calls() {
        return client_state(internal_tag());
    }

    /**
     * The `auth::service` should be non-`nullptr` for native protocol users.
     *
     * @return a ClientState object for external clients (thrift/native protocol users).
     */
    static client_state for_external_calls(auth::service& ser) {
        return client_state(external_tag(), ser);
    }
    static client_state for_external_thrift_calls(auth::service& ser) {
        return client_state(external_tag(), ser, socket_address(), true);
    }

    /**
     * This clock guarantees that updates for the same ClientState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    api::timestamp_type get_timestamp() {
        auto current = api::new_timestamp();
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
        if (_user && !db.local().has_keyspace(keyspace)) {
            throw exceptions::invalid_request_exception(sprint("Keyspace '%s' does not exist", keyspace));
        }
        _keyspace = keyspace;
        _dirty = true;
    }

    const sstring& get_keyspace() const {
        if (_keyspace.empty()) {
            throw exceptions::invalid_request_exception("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
        }
        return _keyspace;
    }

    /**
     * Sets active user. Does _not_ validate anything
     */
    void set_login(::shared_ptr<auth::authenticated_user>);

    /**
     * Attempts to validate login for the set user.
     */
    future<> check_user_exists();

    future<> has_all_keyspaces_access(auth::permission) const;
    future<> has_keyspace_access(const sstring&, auth::permission) const;
    future<> has_column_family_access(const sstring&, const sstring&, auth::permission) const;
    future<> has_schema_access(const schema& s, auth::permission p) const;

private:
    future<> has_access(const sstring&, auth::permission, auth::data_resource) const;
    future<bool> check_has_permission(auth::permission, auth::data_resource) const;
public:
    future<> ensure_has_permission(auth::permission, auth::data_resource) const;

    void validate_login() const;
    void ensure_not_anonymous() const; // unauthorized_exception on error

#if 0
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
#endif

    ::shared_ptr<auth::authenticated_user> user() const {
        return _user;
    }

#if 0
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

