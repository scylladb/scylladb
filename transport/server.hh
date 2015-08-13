/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

    future<> process_request_one(temporary_buffer<char> buf,
                                 uint8_t op,
                                 uint16_t stream);
    unsigned frame_size() const;
    cql_binary_frame_v3 parse_frame(temporary_buffer<char> buf);
    future<std::experimental::optional<cql_binary_frame_v3>> read_frame();
    future<> process_startup(uint16_t stream, temporary_buffer<char> buf);
    future<> process_auth_response(uint16_t stream, temporary_buffer<char> buf);
    future<> process_options(uint16_t stream, temporary_buffer<char> buf);
    future<> process_query(uint16_t stream, temporary_buffer<char> buf);
    future<> process_prepare(uint16_t stream, temporary_buffer<char> buf);
    future<> process_execute(uint16_t stream, temporary_buffer<char> buf);
    future<> process_batch(uint16_t stream, temporary_buffer<char> buf);
    future<> process_register(uint16_t stream, temporary_buffer<char> buf);

    future<> write_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive);
    future<> write_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present);
    future<> write_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type);
    future<> write_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name);
    future<> write_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id);
    future<> write_error(int16_t stream, exceptions::exception_code err, sstring msg);
    future<> write_ready(int16_t stream);
    future<> write_supported(int16_t stream);
    future<> write_result(int16_t stream, shared_ptr<transport::messages::result_message> msg);
    future<> write_topology_change_event(const transport::event::topology_change& event);
    future<> write_status_change_event(const transport::event::status_change& event);
    future<> write_schema_change_event(const transport::event::schema_change& event);
    future<> write_response(shared_ptr<cql_server::response> response);

    void check_room(temporary_buffer<char>& buf, size_t n);
    void validate_utf8(sstring_view s);
    int8_t read_byte(temporary_buffer<char>& buf);
    int32_t read_int(temporary_buffer<char>& buf);
    int64_t read_long(temporary_buffer<char>& buf);
    int16_t read_short(temporary_buffer<char>& buf);
    uint16_t read_unsigned_short(temporary_buffer<char>& buf);
    sstring read_string(temporary_buffer<char>& buf);
    bytes read_short_bytes(temporary_buffer<char>& buf);
    bytes_opt read_value(temporary_buffer<char>& buf);
    bytes_view_opt read_value_view(temporary_buffer<char>& buf);
    sstring_view read_long_string_view(temporary_buffer<char>& buf);
    void read_name_and_value_list(temporary_buffer<char>& buf, std::vector<sstring>& names, std::vector<bytes_view_opt>& values);
    void read_string_list(temporary_buffer<char>& buf, std::vector<sstring>& strings);
    void read_value_view_list(temporary_buffer<char>& buf, std::vector<bytes_view_opt>& values);
    db::consistency_level read_consistency(temporary_buffer<char>& buf);
    std::unordered_map<sstring, sstring> read_string_map(temporary_buffer<char>& buf);
    std::unique_ptr<cql3::query_options> read_options(temporary_buffer<char>& buf);

    cql_query_state& get_query_state(uint16_t stream);
    void init_serialization_format();

    friend event_notifier;
};

}
