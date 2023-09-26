/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "auth/service.hh"
#include <seastar/core/seastar.hh>
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/migration_listener.hh"
#include "auth/authenticator.hh"
#include <seastar/core/distributed.hh>
#include "service/qos/qos_configuration_change_subscriber.hh"
#include "timeout_config.hh"
#include <seastar/core/semaphore.hh>
#include <memory>
#include <boost/intrusive/list.hpp>
#include <seastar/net/tls.hh>
#include <seastar/core/metrics_registration.hh>
#include "utils/fragmented_temporary_buffer.hh"
#include "service_permit.hh"
#include <seastar/core/sharded.hh>
#include <seastar/core/execution_stage.hh>
#include "utils/updateable_value.hh"
#include "generic_server.hh"
#include "service/query_state.hh"
#include "cql3/query_options.hh"
#include "transport/messages/result_message.hh"
#include "utils/chunked_vector.hh"
#include "exceptions/coordinator_result.hh"
#include "db/operation_type.hh"
#include "db/config.hh"
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
private:
    seastar::metrics::metric_groups _metrics;
    std::vector<request_kind_stats> _cql_requests_stats;
};

struct connection_service_level_params {
    sstring role_name;
    timeout_config timeout_config;
    qos::service_level_options::workload_type workload_type;
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

    distributed<cql3::query_processor>& _query_processor;
    cql_server_config _config;
    size_t _max_request_size;
    utils::updateable_value<uint32_t> _max_concurrent_requests;
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
    cql_server(distributed<cql3::query_processor>& qp, auth::service&,
            service::memory_limiter& ml,
            cql_server_config config,
            const db::config& db_cfg,
            qos::service_level_controller& sl_controller,
            gms::gossiper& g,
            scheduling_group_key stats_key,
            maintenance_socket_enabled used_by_maintenance_socket);
    ~cql_server();

public:
    using response = cql_transport::response;
    using result_with_foreign_response_ptr = exceptions::coordinator_result<foreign_ptr<std::unique_ptr<cql_server::response>>>;
    service::endpoint_lifecycle_subscriber* get_lifecycle_listener() const noexcept;
    service::migration_listener* get_migration_listener() const noexcept;
    qos::qos_configuration_change_subscriber* get_qos_configuration_listener() const noexcept;
    cql_sg_stats::request_kind_stats& get_cql_opcode_stats(cql_binary_opcode op) {
        return scheduling_group_get_specific<cql_sg_stats>(_stats_key).get_cql_opcode_stats(op);
    }

    future<utils::chunked_vector<client_data>> get_client_data();
    future<> update_connections_service_level_params();
    future<std::vector<connection_service_level_params>> get_connections_service_level_params();
private:
    class fmt_visitor;
    friend class connection;
    friend std::unique_ptr<cql_server::response> make_result(int16_t stream, messages::result_message& msg,
            const tracing::trace_state_ptr& tr_state, cql_protocol_version_type version, bool skip_metadata);

    class connection : public generic_server::connection {
        cql_server& _server;
        socket_address _server_addr;
        fragmented_temporary_buffer::reader _buffer_reader;
        cql_protocol_version_type _version = 0;
        cql_compression _compression = cql_compression::none;
        service::client_state _client_state;
        timer<lowres_clock> _shedding_timer;
        bool _shed_incoming_requests = false;
        unsigned _request_cpu = 0;
        bool _ready = false;
        bool _authenticating = false;

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
        connection(cql_server& server, socket_address server_addr, connected_socket&& fd, socket_address addr);
        virtual ~connection();
        future<> process_request() override;
        void handle_error(future<>&& f) override;
        void on_connection_close() override;
        static std::pair<net::inet_address, int> make_client_key(const service::client_state& cli_state);
        client_data make_client_data() const;
        const service::client_state& get_client_state() const { return _client_state; }
        service::client_state& get_client_state() { return _client_state; }
    private:
        friend class process_request_executor;
        future<foreign_ptr<std::unique_ptr<cql_server::response>>> process_request_one(fragmented_temporary_buffer::istream buf, uint8_t op, uint16_t stream, service::client_state& client_state, tracing_request_type tracing_request, service_permit permit);
        unsigned frame_size() const;
        unsigned pick_request_cpu();
        cql_binary_frame_v3 parse_frame(temporary_buffer<char> buf) const;
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

        // Helper functions to encapsulate bounce_to_shard processing for query, execute and batch verbs
        template<typename Process>
        future<result_with_foreign_response_ptr>
        process(uint16_t stream, request_reader in, service::client_state& client_state, service_permit permit, tracing::trace_state_ptr trace_state,
                Process process_fn);
        template<typename Process>
        future<result_with_foreign_response_ptr>
        process_on_shard(::shared_ptr<messages::result_message::bounce_to_shard> bounce_msg, uint16_t stream, fragmented_temporary_buffer::istream is, service::client_state& cs,
                service_permit permit, tracing::trace_state_ptr trace_state, Process process_fn);

        void write_response(foreign_ptr<std::unique_ptr<cql_server::response>>&& response, service_permit permit = empty_service_permit(), cql_compression compression = cql_compression::none);

        friend event_notifier;
    };

    friend class type_codec;

private:
    virtual shared_ptr<generic_server::connection> make_connection(socket_address server_addr, connected_socket&& fd, socket_address addr) override;
    future<> advertise_new_connection(shared_ptr<generic_server::connection> conn) override;
    future<> unadvertise_connection(shared_ptr<generic_server::connection> conn) override;

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
    virtual void on_update_tablet_metadata(const locator::tablet_metadata_change_hint&) override;

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

    virtual void on_join_cluster(const gms::inet_address& endpoint) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint, const locator::host_id& hid) override;
    virtual void on_up(const gms::inet_address& endpoint) override;
    virtual void on_down(const gms::inet_address& endpoint) override;
};

inline service::endpoint_lifecycle_subscriber* cql_server::get_lifecycle_listener() const noexcept { return _notifier.get(); }
inline service::migration_listener* cql_server::get_migration_listener() const noexcept { return _notifier.get(); }
inline qos::qos_configuration_change_subscriber* cql_server::get_qos_configuration_listener() const noexcept { return _notifier.get(); }
}
