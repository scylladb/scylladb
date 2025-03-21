/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "auth/service.hh"
#include "exceptions/exceptions.hh"
#include "timeout_config.hh"
#include "timestamp.hh"
#include "replica/database_fwd.hh"
#include "auth/authenticated_user.hh"
#include "auth/authenticator.hh"
#include "auth/permission.hh"

#include "transport/cql_protocol_extension.hh"
#include "service/qos/service_level_controller.hh"

namespace auth {
class resource;
}

namespace data_dictionary {
class database;
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
        seastar::sharded<qos::service_level_controller>* _sl_controller;
        client_state_for_another_shard(const client_state* cs,
            seastar::sharded<auth::service>* auth_service,
            seastar::sharded<qos::service_level_controller>* sl_controller)
            : _cs(cs), _auth_service(auth_service), _sl_controller(sl_controller) {}
        friend client_state;
    public:
        client_state get() const {
            return client_state(_cs, _auth_service, _sl_controller);
        }
    };
private:
    client_state(const client_state* cs,
        seastar::sharded<auth::service>* auth_service,
        seastar::sharded<qos::service_level_controller>* sl_controller)
            : _keyspace(cs->_keyspace)
            , _user(cs->_user)
            , _auth_state(cs->_auth_state)
            , _is_internal(cs->_is_internal)
            , _remote_address(cs->_remote_address)
            , _auth_service(auth_service ? &auth_service->local() : nullptr)
            , _sl_controller(sl_controller ? &sl_controller->local() : nullptr)
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

    client_state(external_tag,
                 auth::service& auth_service,
                 qos::service_level_controller* sl_controller,
                 timeout_config timeout_config,
                 const socket_address& remote_address = socket_address())
            : _is_internal(false)
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

    const socket_address& get_remote_address() const {
        return _remote_address;
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
            , _default_timeout_config(config)
            , _timeout_config(config)
    {}

    client_state(internal_tag, auth::service& auth_service, qos::service_level_controller& sl_controller, sstring username)
        : _user(auth::authenticated_user(username))
        , _auth_state(auth_state::READY)
        , _is_internal(true)
        , _auth_service(&auth_service)
        , _sl_controller(&sl_controller)
    {}

    client_state(const client_state&) = delete;
    client_state(client_state&&) = default;

    ///
    /// `nullptr` for internal instances.
    ///
    auth::service* get_auth_service() const {
        return _auth_service;
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
     * in the order they are committed) but with 2 important caveats:
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
    void set_keyspace(replica::database& db, std::string_view keyspace);

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
    future<> has_column_family_access(const sstring&, const sstring&, auth::permission,
                                      auth::command_desc::type = auth::command_desc::type::OTHER) const;
    future<> has_schema_access(const schema& s, auth::permission p) const;
    future<> has_schema_access(const sstring&, const sstring&, auth::permission p) const;

    future<> has_functions_access(auth::permission p) const;
    future<> has_functions_access(const sstring& ks, auth::permission p) const;
    future<> has_function_access(const sstring& ks, const sstring& function_signature, auth::permission p) const;
private:
    future<> has_access(const sstring& keyspace, auth::command_desc) const;

public:
    future<bool> check_has_permission(auth::command_desc) const;
    future<> ensure_has_permission(auth::command_desc) const;
    future<> maybe_update_per_service_level_params();
    void update_per_service_level_params(qos::service_level_options& slo);

    /**
     * Returns an exceptional future with \ref exceptions::invalid_request_exception if the resource does not exist.
     */
    future<> ensure_exists(const auth::resource&) const;

    void validate_login() const;
    void ensure_not_anonymous() const; // unauthorized_exception on error
    bool is_anonymous() const; // non-throwing version of ensure_not_anonymous()

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
        return client_state_for_another_shard(this,
            _auth_service ? &_auth_service->container() : nullptr,
            _sl_controller ? &_sl_controller->container() : nullptr);
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

