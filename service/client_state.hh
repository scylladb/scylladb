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
 * Copyright (C) 2015-present ScyllaDB
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
#include "timeout_config.hh"
#include "timestamp.hh"
#include "db_clock.hh"
#include "database_fwd.hh"
#include "auth/authenticated_user.hh"
#include "auth/authenticator.hh"
#include "auth/permission.hh"
#include "tracing/tracing.hh"
#include "tracing/trace_state.hh"

#include "enum_set.hh"
#include "transport/cql_protocol_extension.hh"
#include "service/qos/service_level_controller.hh"

namespace auth {
class resource;
}

namespace service {

/**
 * State related to a client connection.
 */
class client_state {
public:
    enum class auth_state : uint8_t {
        UNINITIALIZED, AUTHENTICATION, READY
    };
    using workload_type = qos::service_level_options::workload_type;

    // This class is used to move client_state between shards
    // It is created on a shard that owns client_state than passed
    // to a target shard where client_state_for_another_shard::get()
    // can be called to obtain a shard local copy.
    class client_state_for_another_shard {
    private:
        const client_state* _cs;
        seastar::sharded<auth::service>* _auth_service;
        client_state_for_another_shard(const client_state* cs, seastar::sharded<auth::service>* auth_service) : _cs(cs), _auth_service(auth_service) {}
        friend client_state;
    public:
        client_state get() const {
            return client_state(_cs, _auth_service);
        }
    };
private:
    client_state(const client_state* cs, seastar::sharded<auth::service>* auth_service)
            : _keyspace(cs->_keyspace)
            , _user(cs->_user)
            , _auth_state(cs->_auth_state)
            , _is_internal(cs->_is_internal)
            , _is_thrift(cs->_is_thrift)
            , _remote_address(cs->_remote_address)
            , _auth_service(auth_service ? &auth_service->local() : nullptr)
            , _default_timeout_config(cs->_default_timeout_config)
            , _timeout_config(cs->_timeout_config)
            , _enabled_protocol_extensions(cs->_enabled_protocol_extensions)
    {}
    friend client_state_for_another_shard;
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
    std::optional<auth::authenticated_user> _user;
    std::optional<sstring> _driver_name, _driver_version;

    auth_state _auth_state = auth_state::UNINITIALIZED;

    // isInternal is used to mark ClientState as used by some internal component
    // that should have an ability to modify system keyspace.
    bool _is_internal;
    bool _is_thrift;

    // The biggest timestamp that was returned by getTimestamp/assigned to a query
    static thread_local api::timestamp_type _last_timestamp_micros;

    // Address of a client
    socket_address _remote_address;

    // Only populated for external client state.
    auth::service* _auth_service{nullptr};
    qos::service_level_controller* _sl_controller{nullptr};

    // For restoring default values in the timeout config
    timeout_config _default_timeout_config;
    timeout_config _timeout_config;

    workload_type _workload_type = workload_type::unspecified;

public:
    struct internal_tag {};
    struct external_tag {};

    workload_type get_workload_type() const noexcept {
        return _workload_type;
    }

    auth_state get_auth_state() const noexcept {
        return _auth_state;
    }

    void set_auth_state(auth_state new_state) noexcept {
        _auth_state = new_state;
    }

    std::optional<sstring> get_driver_name() const {
        return _driver_name;
    }
    void set_driver_name(sstring driver_name) {
        _driver_name = std::move(driver_name);
    }

    std::optional<sstring> get_driver_version() const {
        return _driver_version;
    }
    void set_driver_version(sstring driver_version) {
        _driver_version = std::move(driver_version);
    }

    client_state(external_tag, auth::service& auth_service, qos::service_level_controller* sl_controller, timeout_config timeout_config, const socket_address& remote_address = socket_address(), bool thrift = false)
            : _is_internal(false)
            , _is_thrift(thrift)
            , _remote_address(remote_address)
            , _auth_service(&auth_service)
            , _sl_controller(sl_controller)
            , _default_timeout_config(timeout_config)
            , _timeout_config(timeout_config) {
        if (!auth_service.underlying_authenticator().require_authentication()) {
            _user = auth::authenticated_user();
        }
    }

    gms::inet_address get_client_address() const {
        return gms::inet_address(_remote_address);
    }
    
    ::in_port_t get_client_port() const {
        return _remote_address.port();
    }

    const timeout_config& get_timeout_config() const {
        return _timeout_config;
    }

    qos::service_level_controller& get_service_level_controller() const {
        return *_sl_controller;
    }

    client_state(internal_tag) : client_state(internal_tag{}, infinite_timeout_config)
    {}

    client_state(internal_tag, const timeout_config& config)
            : _keyspace("system")
            , _is_internal(true)
            , _is_thrift(false)
            , _default_timeout_config(config)
            , _timeout_config(config)
    {}

    client_state(const client_state&) = delete;
    client_state(client_state&&) = default;

    ///
    /// `nullptr` for internal instances.
    ///
    const auth::service* get_auth_service() const {
        return _auth_service;
    }

    bool is_thrift() const {
        return _is_thrift;
    }

    bool is_internal() const {
        return _is_internal;
    }

    /**
     * @return a ClientState object for internal C* calls (not limited by any kind of auth).
     */
    static client_state& for_internal_calls() {
        static thread_local client_state s(internal_tag{});
        return s;
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

    /**
     * Returns a timestamp suitable for paxos given the timestamp of the last known commit (or in progress update).
     *
     * Paxos ensures that the timestamp it uses for commits respects the serial order of those commits. It does so
     * by having each replica reject any proposal whose timestamp is not strictly greater than the last proposal it
     * accepted. So in practice, which timestamp we use for a given proposal doesn't affect correctness but it does
     * affect the chance of making progress (if we pick a timestamp lower than what has been proposed before, our
     * new proposal will just get rejected).
     *
     * As during the prepared phase replica send us the last propose they accepted, a first option would be to take
     * the maximum of those last accepted proposal timestamp plus 1 (and use a default value, say 0, if it's the
     * first known proposal for the partition). This would mostly work (giving commits the timestamp 0, 1, 2, ...
     * in the order they are commited) but with 2 important caveats:
     *   1) it would give a very poor experience when Paxos and non-Paxos updates are mixed in the same partition,
     *      since paxos operations wouldn't be using microseconds timestamps. And while you shouldn't theoretically
     *      mix the 2 kind of operations, this would still be pretty nonintuitive. And what if you started writing
     *      normal updates and realize later you should switch to Paxos to enforce a property you want?
     *   2) this wouldn't actually be safe due to the expiration set on the Paxos state table.
     *
     * So instead, we initially chose to use the current time in microseconds as for normal update. Which works in
     * general but mean that clock skew creates unavailability periods for Paxos updates (either a node has his clock
     * in the past and he may no be able to get commit accepted until its clock catch up, or a node has his clock in
     * the future and then once one of its commit his accepted, other nodes ones won't be until they catch up). This
     * is ok for small clock skew (few ms) but can be pretty bad for large one.
     *
     * Hence our current solution: we mix both approaches. That is, we compare the timestamp of the last known
     * accepted proposal and the local time. If the local time is greater, we use it, thus keeping paxos timestamps
     * locked to the current time in general (making mixing Paxos and non-Paxos more friendly, and behaving correctly
     * when the paxos state expire (as long as your maximum clock skew is lower than the Paxos state expiration
     * time)). Otherwise (the local time is lower than the last proposal, meaning that this last proposal was done
     * with a clock in the future compared to the local one), we use the last proposal timestamp plus 1, ensuring
     * progress.
     *
     * @param min_timestamp_to_use the max timestamp of the last proposal accepted by replica having responded
     * to the prepare phase of the paxos round this is for. In practice, that's the minimum timestamp this method
     * may return.
     * @return a timestamp suitable for a Paxos proposal (using the reasoning described above). Note that
     * contrary to the get_timestamp() method, the return value is not guaranteed to be unique (nor
     * monotonic) across calls since it can return it's argument (so if the same argument is passed multiple times,
     * it may be returned multiple times). Note that we still ensure Paxos "ballot" are unique (for different
     * proposal) by (securely) randomizing the non-timestamp part of the UUID.
     */
    api::timestamp_type get_timestamp_for_paxos(api::timestamp_type min_timestamp_to_use) {
        api::timestamp_type current = std::max(api::new_timestamp(), min_timestamp_to_use);
        _last_timestamp_micros = _last_timestamp_micros >= current ? _last_timestamp_micros + 1 : current;
        return _last_timestamp_micros;
    }

#if 0
    public SocketAddress getRemoteAddress()
    {
        return remoteAddress;
    }
#endif

    const sstring& get_raw_keyspace() const noexcept {
        return _keyspace;
    }

    sstring& get_raw_keyspace() noexcept {
        return _keyspace;
    }

public:
    void set_keyspace(database& db, std::string_view keyspace);

    void set_raw_keyspace(sstring new_keyspace) noexcept {
        _keyspace = std::move(new_keyspace);
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
    void set_login(auth::authenticated_user);

    /// \brief A user can login if it's anonymous, or if it exists and the `LOGIN` option for the user is `true`.
    future<> check_user_can_login();

    future<> has_all_keyspaces_access(auth::permission) const;
    future<> has_keyspace_access(const sstring&, auth::permission) const;
    future<> has_column_family_access(const database& db, const sstring&, const sstring&, auth::permission,
                                      auth::command_desc::type = auth::command_desc::type::OTHER) const;
    future<> has_schema_access(const schema& s, auth::permission p) const;

private:
    future<> has_access(const sstring& keyspace, auth::command_desc) const;

public:
    future<bool> check_has_permission(auth::command_desc) const;
    future<> ensure_has_permission(auth::command_desc) const;
    future<> maybe_update_per_service_level_params();

    /**
     * Returns an exceptional future with \ref exceptions::invalid_request_exception if the resource does not exist.
     */
    future<> ensure_exists(const auth::resource&) const;

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

    const std::optional<auth::authenticated_user>& user() const {
        return _user;
    }

    client_state_for_another_shard move_to_other_shard() {
        return client_state_for_another_shard(this, _auth_service ? &_auth_service->container() : nullptr);
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

private:

    cql_transport::cql_protocol_extension_enum_set _enabled_protocol_extensions;

public:

    bool is_protocol_extension_set(cql_transport::cql_protocol_extension ext) const {
        return _enabled_protocol_extensions.contains(ext);
    }

    void set_protocol_extensions(cql_transport::cql_protocol_extension_enum_set exts) {
        _enabled_protocol_extensions = std::move(exts);
    }
};

}

