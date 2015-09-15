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
#include "core/distributed.hh"
#include <memory>

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

class cql_server {
    class event_notifier;

    static constexpr int current_version = 3;

    std::vector<server_socket> _listeners;
    distributed<service::storage_proxy>& _proxy;
    distributed<cql3::query_processor>& _query_processor;
    std::unique_ptr<scollectd::registrations> _collectd_registrations;
    std::unique_ptr<event_notifier> _notifier;
private:
    scollectd::registrations setup_collectd();
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
public:
    cql_server(distributed<service::storage_proxy>& proxy, distributed<cql3::query_processor>& qp);
    future<> listen(ipv4_addr addr);
    void do_accepts(int which);
    future<> stop();
private:
    class fmt_visitor;
    class connection;
    class response;
    friend class type_codec;
};

class cql_server::event_notifier : public service::migration_listener,
                                   public service::endpoint_lifecycle_subscriber
{
    uint16_t _port;
    std::set<cql_server::connection*> _topology_change_listeners;
    std::set<cql_server::connection*> _status_change_listeners;
    std::set<cql_server::connection*> _schema_change_listeners;
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
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name) override;
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

struct cql_query_state {
    service::query_state query_state;
    std::unique_ptr<cql3::query_options> options;

    cql_query_state(service::client_state& client_state)
        : query_state(client_state)
    { }
};

class cql_server::connection {
    cql_server& _server;
    connected_socket _fd;
    input_stream<char> _read_buf;
    output_stream<char> _write_buf;
    seastar::gate _pending_requests_gate;
    future<> _ready_to_respond = make_ready_future<>();
    uint8_t _version = 0;
    serialization_format _serialization_format = serialization_format::use_16_bit();
    service::client_state _client_state;
    std::unordered_map<uint16_t, cql_query_state> _query_states;
public:
    connection(cql_server& server, connected_socket&& fd, socket_address addr);
    ~connection();
    future<> process();
    future<> process_request();
private:
    future<shared_ptr<cql_server::response>> process_request_one(temporary_buffer<char> buf, uint8_t op, uint16_t stream);
    unsigned frame_size() const;
    cql_binary_frame_v3 parse_frame(temporary_buffer<char> buf);
    future<std::experimental::optional<cql_binary_frame_v3>> read_frame();
    future<shared_ptr<cql_server::response>> process_startup(uint16_t stream, temporary_buffer<char> buf);
    future<shared_ptr<cql_server::response>> process_auth_response(uint16_t stream, temporary_buffer<char> buf);
    future<shared_ptr<cql_server::response>> process_options(uint16_t stream, temporary_buffer<char> buf);
    future<shared_ptr<cql_server::response>> process_query(uint16_t stream, temporary_buffer<char> buf);
    future<shared_ptr<cql_server::response>> process_prepare(uint16_t stream, temporary_buffer<char> buf);
    future<shared_ptr<cql_server::response>> process_execute(uint16_t stream, temporary_buffer<char> buf);
    future<shared_ptr<cql_server::response>> process_batch(uint16_t stream, temporary_buffer<char> buf);
    future<shared_ptr<cql_server::response>> process_register(uint16_t stream, temporary_buffer<char> buf);

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
    future<> write_response(shared_ptr<cql_server::response> response);

    void check_room(temporary_buffer<char>& buf, size_t n);
    void validate_utf8(sstring_view s);
    int8_t read_byte(temporary_buffer<char>& buf);
    int32_t read_int(temporary_buffer<char>& buf);
    int64_t read_long(temporary_buffer<char>& buf);
    int16_t read_short(temporary_buffer<char>& buf);
    uint16_t read_unsigned_short(temporary_buffer<char>& buf);
    sstring read_string(temporary_buffer<char>& buf);
    sstring_view read_string_view(temporary_buffer<char>& buf);
    sstring_view read_long_string_view(temporary_buffer<char>& buf);
    bytes read_short_bytes(temporary_buffer<char>& buf);
    bytes_opt read_value(temporary_buffer<char>& buf);
    bytes_view_opt read_value_view(temporary_buffer<char>& buf);
    void read_name_and_value_list(temporary_buffer<char>& buf, std::vector<sstring_view>& names, std::vector<bytes_view_opt>& values);
    void read_string_list(temporary_buffer<char>& buf, std::vector<sstring>& strings);
    void read_value_view_list(temporary_buffer<char>& buf, std::vector<bytes_view_opt>& values);
    db::consistency_level read_consistency(temporary_buffer<char>& buf);
    std::unordered_map<sstring, sstring> read_string_map(temporary_buffer<char>& buf);
    std::unique_ptr<cql3::query_options> read_options(temporary_buffer<char>& buf);

    void init_serialization_format();

    friend event_notifier;
};

}
