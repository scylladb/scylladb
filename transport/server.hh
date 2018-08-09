/*
 * Copyright (C) 2015 ScyllaDB
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
#include "core/reactor.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/migration_listener.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "cql3/values.hh"
#include "auth/authenticator.hh"
#include "core/distributed.hh"
#include "timeout_config.hh"
#include <seastar/core/semaphore.hh>
#include <memory>
#include <boost/intrusive/list.hpp>
#include <seastar/net/tls.hh>
#include <seastar/core/metrics_registration.hh>
#include "utils/fragmented_temporary_buffer.hh"

namespace scollectd {

class registrations;

}

class database;

namespace cql_transport {

class request_reader;
class response;

enum class cql_compression {
    none,
    lz4,
    snappy,
};

enum cql_frame_flags {
    compression = 0x01,
    tracing     = 0x02,
};

struct [[gnu::packed]] cql_binary_frame_v1 {
    uint8_t  version;
    uint8_t  flags;
    uint8_t  stream;
    uint8_t  opcode;
    net::packed<uint32_t> length;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) {
        return a(length);
    }
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

enum class cql_load_balance {
    none,
    round_robin,
};

cql_load_balance parse_load_balance(sstring value);

struct cql_query_state {
    service::query_state query_state;
    std::unique_ptr<cql3::query_options> options;

    cql_query_state(service::client_state& client_state)
        : query_state(client_state)
    { }
};

struct cql_server_config {
    ::timeout_config timeout_config;
    size_t max_request_size;
};

class cql_server {
private:
    class event_notifier;

    static constexpr cql_protocol_version_type current_version = cql_serialization_format::latest_version;

    std::vector<server_socket> _listeners;
    distributed<service::storage_proxy>& _proxy;
    distributed<cql3::query_processor>& _query_processor;
    cql_server_config _config;
    size_t _max_request_size;
    semaphore _memory_available;
    seastar::metrics::metric_groups _metrics;
    std::unique_ptr<event_notifier> _notifier;
private:
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
    uint64_t _requests_blocked_memory = 0;
    cql_load_balance _lb;
    auth::service& _auth_service;
public:
    cql_server(distributed<service::storage_proxy>& proxy, distributed<cql3::query_processor>& qp, cql_load_balance lb, auth::service&,
            cql_server_config config);
    future<> listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> = {}, bool keepalive = false);
    future<> do_accepts(int which, bool keepalive, ipv4_addr server_addr);
    future<> stop();
public:
    using response = cql_transport::response;
    using response_type = std::pair<std::unique_ptr<cql_server::response>, service::client_state>;
private:
    class fmt_visitor;
    friend class connection;
    class connection : public boost::intrusive::list_base_hook<> {
        struct processing_result {
            foreign_ptr<std::unique_ptr<cql_server::response>> cql_response;
            foreign_ptr<std::unique_ptr<sstring>> keyspace;
            foreign_ptr<shared_ptr<auth::authenticated_user>> user;
            service::client_state::auth_state auth_state;

            processing_result(response_type r);
        };

        cql_server& _server;
        ipv4_addr _server_addr;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        fragmented_temporary_buffer::reader _buffer_reader;
        seastar::gate _pending_requests_gate;
        future<> _ready_to_respond = make_ready_future<>();
        cql_protocol_version_type _version = 0;
        cql_compression _compression = cql_compression::none;
        cql_serialization_format _cql_serialization_format = cql_serialization_format::latest();
        service::client_state _client_state;
        std::unordered_map<uint16_t, cql_query_state> _query_states;
        unsigned _request_cpu = 0;

        enum class tracing_request_type : uint8_t {
            not_requested,
            no_write_on_close,
            write_on_close
        };
    private:
        using execution_stage_type = inheriting_concrete_execution_stage<
                future<cql_server::connection::processing_result>,
                cql_server::connection*,
                fragmented_temporary_buffer::istream,
                uint8_t,
                uint16_t,
                service::client_state,
                tracing_request_type>;
        static thread_local execution_stage_type _process_request_stage;
    public:
        connection(cql_server& server, ipv4_addr server_addr, connected_socket&& fd, socket_address addr);
        ~connection();
        future<> process();
        future<> process_request();
        future<> shutdown();
    private:
        const ::timeout_config& timeout_config() { return _server.timeout_config(); }
        friend class process_request_executor;
        future<processing_result> process_request_one(fragmented_temporary_buffer::istream buf, uint8_t op, uint16_t stream, service::client_state client_state, tracing_request_type tracing_request);
        unsigned frame_size() const;
        unsigned pick_request_cpu();
        void update_client_state(processing_result& r);
        cql_binary_frame_v3 parse_frame(temporary_buffer<char> buf);
        future<fragmented_temporary_buffer> read_and_decompress_frame(size_t length, uint8_t flags);
        future<std::experimental::optional<cql_binary_frame_v3>> read_frame();
        future<response_type> process_startup(uint16_t stream, request_reader in, service::client_state client_state);
        future<response_type> process_auth_response(uint16_t stream, request_reader in, service::client_state client_state);
        future<response_type> process_options(uint16_t stream, request_reader in, service::client_state client_state);
        future<response_type> process_query(uint16_t stream, request_reader in, service::client_state client_state);
        future<response_type> process_prepare(uint16_t stream, request_reader in, service::client_state client_state);
        future<response_type> process_execute(uint16_t stream, request_reader in, service::client_state client_state);
        future<response_type> process_batch(uint16_t stream, request_reader in, service::client_state client_state);
        future<response_type> process_register(uint16_t stream, request_reader in, service::client_state client_state);

        std::unique_ptr<cql_server::response> make_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_read_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_mutation_write_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_error(int16_t stream, exceptions::exception_code err, sstring msg, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_ready(int16_t stream, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_supported(int16_t stream, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_result(int16_t stream, shared_ptr<cql_transport::messages::result_message> msg, const tracing::trace_state_ptr& tr_state, bool skip_metadata = false);
        std::unique_ptr<cql_server::response> make_topology_change_event(const cql_transport::event::topology_change& event);
        std::unique_ptr<cql_server::response> make_status_change_event(const cql_transport::event::status_change& event);
        std::unique_ptr<cql_server::response> make_schema_change_event(const cql_transport::event::schema_change& event);
        std::unique_ptr<cql_server::response> make_autheticate(int16_t, const sstring&, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_auth_success(int16_t, bytes, const tracing::trace_state_ptr& tr_state);
        std::unique_ptr<cql_server::response> make_auth_challenge(int16_t, bytes, const tracing::trace_state_ptr& tr_state);

        void write_response(foreign_ptr<std::unique_ptr<cql_server::response>>&& response, cql_compression compression = cql_compression::none);

        void init_cql_serialization_format();

        friend event_notifier;
    };

    friend class type_codec;
private:
    bool _stopping = false;
    promise<> _all_connections_stopped;
    future<> _stopped = _all_connections_stopped.get_future();
    boost::intrusive::list<connection> _connections_list;
    uint64_t _total_connections = 0;
    uint64_t _current_connections = 0;
    uint64_t _connections_being_accepted = 0;
private:
    void maybe_idle() {
        if (_stopping && !_connections_being_accepted && !_current_connections) {
            _all_connections_stopped.set_value();
        }
    }
    const ::timeout_config& timeout_config() { return _config.timeout_config; }
};

class cql_server::event_notifier : public service::migration_listener,
                                   public service::endpoint_lifecycle_subscriber
{
    std::set<cql_server::connection*> _topology_change_listeners;
    std::set<cql_server::connection*> _status_change_listeners;
    std::set<cql_server::connection*> _schema_change_listeners;
    std::unordered_map<gms::inet_address, event::status_change::status_type> _last_status_change;
public:
    event_notifier();
    ~event_notifier();
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

    virtual void on_join_cluster(const gms::inet_address& endpoint) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint) override;
    virtual void on_up(const gms::inet_address& endpoint) override;
    virtual void on_down(const gms::inet_address& endpoint) override;
};

using response_type = cql_server::response_type;

}
