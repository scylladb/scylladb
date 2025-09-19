/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "auth/service.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/scheduling.hh>
#include "cql3/prepared_statements_cache.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/migration_listener.hh"
#include "auth/authenticator.hh"
#include <seastar/core/sharded.hh>
#include "service/qos/qos_configuration_change_subscriber.hh"
#include "timeout_config.hh"
#include <seastar/core/semaphore.hh>
#include <memory>
#include <type_traits>
#include <boost/intrusive/list.hpp>
#include <seastar/net/tls.hh>
#include <seastar/core/metrics_registration.hh>
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/result.hh"
#include "service_permit.hh"
#include <seastar/core/sharded.hh>
#include <seastar/core/execution_stage.hh>
#include "utils/updateable_value.hh"
#include "generic_server.hh"
#include "service/query_state.hh"
#include "cql3/query_options.hh"
#include "cql3/dialect.hh"
#include "transport/messages/result_message.hh"
#include "utils/chunked_vector.hh"
#include "exceptions/coordinator_result.hh"
#include "exceptions/exceptions.hh"
#include "db/operation_type.hh"
#include "service/maintenance_mode.hh"

namespace cql3 {

class query_processor;

}

namespace scollectd {

class registrations;

}

namespace service {
class memory_limiter;
}

enum class client_type;
struct client_data;

namespace qos {
    class service_level_controller;
} // namespace qos

namespace gms {
    class gossiper;
}

namespace cql_transport {

class request_reader;
class response;
enum class cql_binary_opcode : uint8_t;

enum class cql_compression {
    none,
    lz4,
    snappy,
};

enum cql_frame_flags {
    compression = 0x01,
    tracing     = 0x02,
    custom_payload = 0x04,
    warning     = 0x08,
};

struct [[gnu::packed]] cql_binary_frame_v3 {
    uint8_t  version;
    uint8_t  flags;
    net::packed<uint16_t> stream;
    uint8_t  opcode;
    net::packed<uint32_t> length;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) {
        return a(stream, length);
    }
};

struct cql_query_state {
    service::query_state query_state;
    std::unique_ptr<cql3::query_options> options;

    cql_query_state(service::client_state& client_state, tracing::trace_state_ptr trace_state_ptr, service_permit permit)
        : query_state(client_state, std::move(trace_state_ptr), std::move(permit))
    { }
};

struct cql_server_config {
    updateable_timeout_config timeout_config;
    size_t max_request_size;
    sstring partitioner_name;
    unsigned sharding_ignore_msb;
    std::optional<uint16_t> shard_aware_transport_port;
    std::optional<uint16_t> shard_aware_transport_port_ssl;
    bool allow_shard_aware_drivers = true;
    smp_service_group bounce_request_smp_service_group = default_smp_service_group();
    utils::updateable_value<uint32_t> max_concurrent_requests;
    utils::updateable_value<bool> cql_duplicate_bind_variable_names_refer_to_same_variable;
    utils::updateable_value<uint32_t> uninitialized_connections_semaphore_cpu_concurrency;
    utils::updateable_value<uint32_t> request_timeout_on_shutdown_in_seconds;
};

/**
 * CQL op-code stats collected for each scheduling group
 */
struct cql_sg_stats {
    struct request_kind_stats {
        uint64_t count = 0;
        uint64_t request_size = 0;
        uint64_t response_size = 0;
    };

    cql_sg_stats(maintenance_socket_enabled);
    request_kind_stats& get_cql_opcode_stats(cql_binary_opcode op) { return _cql_requests_stats[static_cast<uint8_t>(op)]; }
    void register_metrics();
    void rename_metrics();
private:
    bool _use_metrics = false;
    seastar::metrics::metric_groups _metrics;
    std::vector<request_kind_stats> _cql_requests_stats;
};

struct connection_service_level_params {
    sstring role_name;
    timeout_config timeout_config;
    qos::service_level_options::workload_type workload_type;
    sstring scheduling_group_name;
};

class cql_metadata_id_wrapper {
private:
    std::optional<cql3::cql_metadata_id_type> _request_metadata_id;
    std::optional<cql3::cql_metadata_id_type> _response_metadata_id;

public:
    cql_metadata_id_wrapper()
        : _request_metadata_id(std::nullopt)
        , _response_metadata_id(std::nullopt)
    { }

    explicit cql_metadata_id_wrapper(cql3::cql_metadata_id_type&& response_metadata_id)
        : _request_metadata_id(std::nullopt)
        , _response_metadata_id(std::move(response_metadata_id))
    { }

    cql_metadata_id_wrapper(cql3::cql_metadata_id_type&& request_metadata_id, cql3::cql_metadata_id_type&& response_metadata_id)
        : _request_metadata_id(std::move(request_metadata_id))
        , _response_metadata_id(std::move(response_metadata_id))
    { }

    bool has_request_metadata_id() const;

    bool has_response_metadata_id() const;

    const cql3::cql_metadata_id_type& get_request_metadata_id() const;

    const cql3::cql_metadata_id_type& get_response_metadata_id() const;
};

class cql_server : public seastar::peering_sharded_service<cql_server>, public generic_server::server {
private:
    struct transport_stats {
        // server stats
        uint64_t connects = 0;
        uint64_t connections = 0;
        uint64_t requests_served = 0;
        uint32_t requests_serving = 0;
        uint64_t requests_blocked_memory = 0;
        uint64_t requests_shed = 0;

        std::unordered_map<exceptions::exception_code, uint64_t> errors;
    };
private:
    class event_notifier;

    static constexpr cql_protocol_version_type current_version = cql_serialization_format::latest_version;

    sharded<cql3::query_processor>& _query_processor;
    cql_server_config _config;
    semaphore& _memory_available;
    seastar::metrics::metric_groups _metrics;
    std::unique_ptr<event_notifier> _notifier;
private:
    transport_stats _stats;
    auth::service& _auth_service;
    qos::service_level_controller& _sl_controller;
    gms::gossiper& _gossiper;
    scheduling_group_key _stats_key;
public:
    cql_server(sharded<cql3::query_processor>& qp, auth::service&,
            service::memory_limiter& ml,
            cql_server_config config,
            qos::service_level_controller& sl_controller,
            gms::gossiper& g,
            scheduling_group_key stats_key,
            maintenance_socket_enabled used_by_maintenance_socket);
    ~cql_server();

public:
    using response = cql_transport::response;
    using result_with_foreign_response_ptr = exceptions::coordinator_result<foreign_ptr<std::unique_ptr<cql_server::response>>>;
    using result_with_bounce_to_shard = foreign_ptr<seastar::shared_ptr<messages::result_message::bounce_to_shard>>;
    using process_fn_return_type = std::variant<result_with_foreign_response_ptr, result_with_bounce_to_shard>;

    service::endpoint_lifecycle_subscriber* get_lifecycle_listener() const noexcept;
    service::migration_listener* get_migration_listener() const noexcept;
    qos::qos_configuration_change_subscriber* get_qos_configuration_listener() const noexcept;
    cql_sg_stats::request_kind_stats& get_cql_opcode_stats(cql_binary_opcode op) {
        return scheduling_group_get_specific<cql_sg_stats>(_stats_key).get_cql_opcode_stats(op);
    }

    future<utils::chunked_vector<client_data>> get_client_data();
    future<> update_connections_scheduling_group();
    future<> update_connections_service_level_params();
    future<std::vector<connection_service_level_params>> get_connections_service_level_params();
private:
    class fmt_visitor;
    friend class connection;
    friend std::unique_ptr<cql_server::response> make_result(int16_t stream, messages::result_message& msg,
            const tracing::trace_state_ptr& tr_state, cql_protocol_version_type version, cql_metadata_id_wrapper&& metadata_id, bool skip_metadata);

    class connection : public generic_server::connection {
        cql_server& _server;
        socket_address _server_addr;
        fragmented_temporary_buffer::reader _buffer_reader;
        cql_protocol_version_type _version = 0;
        cql_compression _compression = cql_compression::none;
        service::client_state _client_state;
        timer<lowres_clock> _shedding_timer;
        scheduling_group _current_scheduling_group;
        bool _shed_incoming_requests = false;
        bool _ready = false;
        bool _authenticating = false;
        bool _tenant_switch = false;

        enum class tracing_request_type : uint8_t {
            not_requested,
            no_write_on_close,
            write_on_close
        };
    private:
        using execution_stage_type = inheriting_concrete_execution_stage<
                future<foreign_ptr<std::unique_ptr<cql_server::response>>>,
                cql_server::connection*,
                fragmented_temporary_buffer::istream,
                uint8_t,
                uint16_t,
                service::client_state&,
                tracing_request_type,
                service_permit>;
        static thread_local execution_stage_type _process_request_stage;
    public:
        connection(cql_server& server, socket_address server_addr, connected_socket&& fd, socket_address addr, named_semaphore& sem, semaphore_units<named_semaphore_exception_factory> initial_sem_units);
        virtual ~connection();
        future<> process_request() override;
        void handle_error(future<>&& f) override;
        client_data make_client_data() const;
        const service::client_state& get_client_state() const { return _client_state; }
        void update_scheduling_group();
        service::client_state& get_client_state() { return _client_state; }
        scheduling_group get_scheduling_group() const { return _current_scheduling_group; }
    private:
        friend class process_request_executor;

        future<foreign_ptr<std::unique_ptr<cql_server::response>>> sleep_until_timeout_passes(const seastar::lowres_clock::time_point& timeout, std::unique_ptr<cql_server::response>&& resp) const;
        future<foreign_ptr<std::unique_ptr<cql_server::response>>> process_request_one(fragmented_temporary_buffer::istream buf, uint8_t op, uint16_t stream, service::client_state& client_state, tracing_request_type tracing_request, service_permit permit);
        unsigned frame_size() const;
        unsigned pick_request_cpu();
        utils::result_with_exception<cql_binary_frame_v3, exceptions::protocol_exception, class cql_frame_error> parse_frame(temporary_buffer<char> buf) const;
        future<fragmented_temporary_buffer> read_and_decompress_frame(size_t length, uint8_t flags);
        future<std::optional<cql_binary_frame_v3>> read_frame();
        future<std::unique_ptr<cql_server::response>> process_startup(uint16_t stream, request_reader in, service::client_state& client_state, tracing::trace_state_ptr trace_state);
        future<std::unique_ptr<cql_server::response>> process_auth_response(uint16_t stream, request_reader in, service::client_state& client_state, tracing::trace_state_ptr trace_state);
        future<std::unique_ptr<cql_server::response>> process_options(uint16_t stream, request_reader in, service::client_state& client_state, tracing::trace_state_ptr trace_state);
        future<result_with_foreign_response_ptr> process_query(uint16_t stream, request_reader in, service::client_state& client_state, service_permit permit, tracing::trace_state_ptr trace_state);
        future<std::unique_ptr<cql_server::response>> process_prepare(uint16_t stream, request_reader in, service::client_state& client_state, tracing::trace_state_ptr trace_state);
        future<result_with_foreign_response_ptr> process_execute(uint16_t stream, request_reader in, service::client_state& client_state, service_permit permit, tracing::trace_state_ptr trace_state);
        future<result_with_foreign_response_ptr> process_batch(uint16_t stream, request_reader in, service::client_state& client_state, service_permit permit, tracing::trace_state_ptr trace_state);
        future<std::unique_ptr<cql_server::response>> process_register(uint16_t stream, request_reader in, service::client_state& client_state, tracing::trace_state_ptr trace_state);

        std::unique_ptr<cql_server::response> make_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_read_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_mutation_write_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_function_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring func_name, std::vector<sstring> args, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_rate_limit_error(int16_t stream, exceptions::exception_code err, sstring msg, db::operation_type op_type, bool rejected_by_coordinator, const tracing::trace_state_ptr& tr_state, const service::client_state& client_state) const;
        std::unique_ptr<cql_server::response> make_error(int16_t stream, exceptions::exception_code err, sstring msg, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_ready(int16_t stream, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_supported(int16_t stream, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_topology_change_event(const cql_transport::event::topology_change& event) const;
        std::unique_ptr<cql_server::response> make_status_change_event(const cql_transport::event::status_change& event) const;
        std::unique_ptr<cql_server::response> make_schema_change_event(const cql_transport::event::schema_change& event) const;
        std::unique_ptr<cql_server::response> make_autheticate(int16_t, std::string_view, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_auth_success(int16_t, bytes, const tracing::trace_state_ptr& tr_state) const;
        std::unique_ptr<cql_server::response> make_auth_challenge(int16_t, bytes, const tracing::trace_state_ptr& tr_state) const;

        cql3::dialect get_dialect() const;

        // Helper functions to encapsulate bounce_to_shard processing for query, execute and batch verbs
        template <typename Process>
            requires std::is_invocable_r_v<future<cql_server::process_fn_return_type>,
                                           Process,
                                           service::client_state&,
                                           sharded<cql3::query_processor>&,
                                           request_reader,
                                           uint16_t,
                                           cql_protocol_version_type,
                                           service_permit,
                                           tracing::trace_state_ptr,
                                           bool,
                                           cql3::computed_function_values,
                                           cql3::dialect>
        future<result_with_foreign_response_ptr>
        process(uint16_t stream, request_reader in, service::client_state& client_state, service_permit permit, tracing::trace_state_ptr trace_state,
                Process process_fn);

        template <typename Process>
            requires std::is_invocable_r_v<future<cql_server::process_fn_return_type>,
                                           Process,
                                           service::client_state&,
                                           sharded<cql3::query_processor>&,
                                           request_reader,
                                           uint16_t,
                                           cql_protocol_version_type,
                                           service_permit,
                                           tracing::trace_state_ptr,
                                           bool,
                                           cql3::computed_function_values,
                                           cql3::dialect>
        future<process_fn_return_type>
        process_on_shard(shard_id shard, uint16_t stream, fragmented_temporary_buffer::istream is, service::client_state& cs,
                tracing::trace_state_ptr trace_state, cql3::dialect dialect, cql3::computed_function_values&& cached_vals, Process process_fn);

        void write_response(foreign_ptr<std::unique_ptr<cql_server::response>>&& response, service_permit permit = empty_service_permit(), cql_compression compression = cql_compression::none);

        friend event_notifier;
    };

    friend class type_codec;

private:
    virtual shared_ptr<generic_server::connection> make_connection(socket_address server_addr, connected_socket&& fd, socket_address addr, named_semaphore& sem, semaphore_units<named_semaphore_exception_factory> initial_sem_units) override;
    scheduling_group get_scheduling_group_for_new_connection() const override {
        if (_sl_controller.has_service_level(qos::service_level_controller::driver_service_level_name)) {
            return _sl_controller.get_scheduling_group(qos::service_level_controller::driver_service_level_name);
        }
        return default_scheduling_group();
    }

    ::timeout_config timeout_config() const { return _config.timeout_config.current_values(); }
};

class cql_server::event_notifier : public service::migration_listener,
                                   public service::endpoint_lifecycle_subscriber,
                                   public qos::qos_configuration_change_subscriber
{
    cql_server& _server;
    std::set<cql_server::connection*> _topology_change_listeners;
    std::set<cql_server::connection*> _status_change_listeners;
    std::set<cql_server::connection*> _schema_change_listeners;
    std::unordered_map<gms::inet_address, event::status_change::status_type> _last_status_change;

    // We want to delay sending NEW_NODE CQL event to clients until the new node
    // has started listening for CQL requests.
    std::unordered_set<gms::inet_address> _endpoints_pending_joined_notification;

    void send_join_cluster(const gms::inet_address& endpoint);
public:
    explicit event_notifier(cql_server& s) noexcept : _server(s) {}
    void register_event(cql_transport::event::event_type et, cql_server::connection* conn);
    void unregister_connection(cql_server::connection* conn);

    virtual void on_create_keyspace(const sstring& ks_name) override;
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override;
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_update_keyspace(const sstring& ks_name) override;
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) override;
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override;
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_drop_keyspace(const sstring& ks_name) override;
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override;
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual future<> on_before_service_level_add(qos::service_level_options, qos::service_level_info sl_info) override;
    virtual future<> on_after_service_level_remove(qos::service_level_info sl_info) override;
    virtual future<> on_before_service_level_change(qos::service_level_options slo_before, qos::service_level_options slo_after, qos::service_level_info sl_info) override;
    virtual future<> on_effective_service_levels_cache_reloaded() override;

    virtual void on_join_cluster(const gms::inet_address& endpoint, locator::host_id hid) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint, const locator::host_id& hid) override;
    virtual void on_up(const gms::inet_address& endpoint, locator::host_id hid) override;
    virtual void on_down(const gms::inet_address& endpoint, locator::host_id hid) override;
};

inline service::endpoint_lifecycle_subscriber* cql_server::get_lifecycle_listener() const noexcept { return _notifier.get(); }
inline service::migration_listener* cql_server::get_migration_listener() const noexcept { return _notifier.get(); }
inline qos::qos_configuration_change_subscriber* cql_server::get_qos_configuration_listener() const noexcept { return _notifier.get(); }
}
