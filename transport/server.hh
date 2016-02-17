/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "core/reactor.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/migration_listener.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "auth/authenticator.hh"
#include "core/distributed.hh"
#include <seastar/core/semaphore.hh>
#include <memory>
#include <boost/intrusive/list.hpp>
#include <seastar/net/tls.hh>

namespace scollectd {

class registrations;

}

class database;

namespace transport {

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

class cql_server {
private:
    class event_notifier;

    static constexpr cql_protocol_version_type current_version = cql_serialization_format::latest_version;

    std::vector<server_socket> _listeners;
    distributed<service::storage_proxy>& _proxy;
    distributed<cql3::query_processor>& _query_processor;
    size_t _max_request_size;
    semaphore _memory_available;
    std::unique_ptr<scollectd::registrations> _collectd_registrations;
    std::unique_ptr<event_notifier> _notifier;
private:
    scollectd::registrations setup_collectd();
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
    cql_load_balance _lb;
public:
    cql_server(distributed<service::storage_proxy>& proxy, distributed<cql3::query_processor>& qp, cql_load_balance lb);
    future<> listen(ipv4_addr addr, ::shared_ptr<seastar::tls::server_credentials> = {});
    future<> do_accepts(int which);
    future<> stop();
public:
    class response;
    using response_type = std::pair<shared_ptr<cql_server::response>, service::client_state>;
private:
    class fmt_visitor;
    class connection : public boost::intrusive::list_base_hook<> {
        cql_server& _server;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        seastar::gate _pending_requests_gate;
        future<> _ready_to_respond = make_ready_future<>();
        cql_protocol_version_type _version = 0;
        cql_serialization_format _cql_serialization_format = cql_serialization_format::latest();
        service::client_state _client_state;
        std::unordered_map<uint16_t, cql_query_state> _query_states;
        unsigned _request_cpu = 0;

        enum class state : uint8_t {
            UNINITIALIZED, AUTHENTICATION, READY
        };

        state _state = state::UNINITIALIZED;
        ::shared_ptr<auth::authenticator::sasl_challenge> _sasl_challenge;
    public:
        connection(cql_server& server, connected_socket&& fd, socket_address addr);
        ~connection();
        future<> process();
        future<> process_request();
        future<> shutdown();
    private:
        future<response_type> process_request_one(bytes_view buf, uint8_t op, uint16_t stream, service::client_state client_state);
        unsigned frame_size() const;
        unsigned pick_request_cpu();
        cql_binary_frame_v3 parse_frame(temporary_buffer<char> buf);
        future<std::experimental::optional<cql_binary_frame_v3>> read_frame();
        future<response_type> process_startup(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_auth_response(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_options(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_query(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_prepare(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_execute(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_batch(uint16_t stream, bytes_view buf, service::client_state client_state);
        future<response_type> process_register(uint16_t stream, bytes_view buf, service::client_state client_state);

        shared_ptr<cql_server::response> make_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive);
        shared_ptr<cql_server::response> make_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present);
        shared_ptr<cql_server::response> make_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type);
        shared_ptr<cql_server::response> make_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name);
        shared_ptr<cql_server::response> make_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id);
        shared_ptr<cql_server::response> make_error(int16_t stream, exceptions::exception_code err, sstring msg);
        shared_ptr<cql_server::response> make_ready(int16_t stream);
        shared_ptr<cql_server::response> make_supported(int16_t stream);
        shared_ptr<cql_server::response> make_result(int16_t stream, shared_ptr<transport::messages::result_message> msg);
        shared_ptr<cql_server::response> make_topology_change_event(const transport::event::topology_change& event);
        shared_ptr<cql_server::response> make_status_change_event(const transport::event::status_change& event);
        shared_ptr<cql_server::response> make_schema_change_event(const transport::event::schema_change& event);
        shared_ptr<cql_server::response> make_autheticate(int16_t, const sstring&);
        shared_ptr<cql_server::response> make_auth_success(int16_t, bytes);
        shared_ptr<cql_server::response> make_auth_challenge(int16_t, bytes);

        future<> write_response(foreign_ptr<shared_ptr<cql_server::response>>&& response);

        void check_room(bytes_view& buf, size_t n);
        void validate_utf8(sstring_view s);
        int8_t read_byte(bytes_view& buf);
        int32_t read_int(bytes_view& buf);
        int64_t read_long(bytes_view& buf);
        uint16_t read_short(bytes_view& buf);
        uint16_t read_unsigned_short(bytes_view& buf);
        sstring read_string(bytes_view& buf);
        sstring_view read_string_view(bytes_view& buf);
        sstring_view read_long_string_view(bytes_view& buf);
        bytes read_short_bytes(bytes_view& buf);
        bytes_opt read_value(bytes_view& buf);
        bytes_view_opt read_value_view(bytes_view& buf);
        void read_name_and_value_list(bytes_view& buf, std::vector<sstring_view>& names, std::vector<bytes_view_opt>& values);
        void read_string_list(bytes_view& buf, std::vector<sstring>& strings);
        void read_value_view_list(bytes_view& buf, std::vector<bytes_view_opt>& values);
        db::consistency_level read_consistency(bytes_view& buf);
        std::unordered_map<sstring, sstring> read_string_map(bytes_view& buf);
        std::unique_ptr<cql3::query_options> read_options(bytes_view& buf);
        std::unique_ptr<cql3::query_options> read_options(bytes_view& buf, uint8_t);

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
};

class cql_server::event_notifier : public service::migration_listener,
                                   public service::endpoint_lifecycle_subscriber
{
    uint16_t _port;
    std::set<cql_server::connection*> _topology_change_listeners;
    std::set<cql_server::connection*> _status_change_listeners;
    std::set<cql_server::connection*> _schema_change_listeners;
    std::unordered_map<gms::inet_address, event::status_change::status_type> _last_status_change;
public:
    event_notifier(uint16_t port);
    void register_event(transport::event::event_type et, cql_server::connection* conn);
    void unregister_connection(cql_server::connection* conn);

    virtual void on_create_keyspace(const sstring& ks_name) override;
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_update_keyspace(const sstring& ks_name) override;
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) override;
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_drop_keyspace(const sstring& ks_name) override;
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_join_cluster(const gms::inet_address& endpoint) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint) override;
    virtual void on_up(const gms::inet_address& endpoint) override;
    virtual void on_down(const gms::inet_address& endpoint) override;
    virtual void on_move(const gms::inet_address& endpoint) override;
};

using response_type = cql_server::response_type;

}
