/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "log.hh"

#include "seastarx.hh"

#include <list>

#include <seastar/core/file-types.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>

#include <boost/intrusive/list.hpp>

namespace generic_server {

class server;

// A generic TCP connection.
//
// This class is used in tandem with the `server`class to implement a protocol
// specific TCP connection.
//
// Protocol specific classes are expected to override the `process_request`
// member function to perform request processing. This base class provides a
// `_read_buf` and a `_write_buf` for reading requests and writing responses.
class connection : public boost::intrusive::list_base_hook<> {
protected:
    server& _server;
    connected_socket _fd;
    input_stream<char> _read_buf;
    output_stream<char> _write_buf;
    future<> _ready_to_respond = make_ready_future<>();
    seastar::gate _pending_requests_gate;
    seastar::gate::holder _hold_server;

public:
    connection(server& server, connected_socket&& fd);
    virtual ~connection();

    virtual future<> process();

    virtual void handle_error(future<>&& f) = 0;

    virtual future<> process_request() = 0;

    virtual void on_connection_close();

    virtual future<> shutdown();
};

// A generic TCP socket server.
//
// This class can be used as a base for a protocol specific TCP socket server
// that listens to incoming connections and processes requests coming over the
// connection.
//
// The provides a `listen` member function that creates a TCP server socket and
// registers it to the Seastar reactor. The class also provides a `stop` member
// function that can be used to safely stop the server.
//
// Protocol specific classes that inherit `server` are expected to also inherit
// a connection class from `connection` and override the `make_connection` member
// function to create a protocol specific connection upon `accept`.
class server {
    friend class connection;

protected:
    sstring _server_name;
    logging::logger& _logger;
    seastar::gate _gate;
    future<> _all_connections_stopped = make_ready_future<>();
    uint64_t _total_connections = 0;
    future<> _listeners_stopped = make_ready_future<>();
    using connections_list_t = boost::intrusive::list<connection>;
    connections_list_t _connections_list;
    struct gentle_iterator {
        connections_list_t::iterator iter, end;
        gentle_iterator(server& s) : iter(s._connections_list.begin()), end(s._connections_list.end()) {}
        gentle_iterator(const gentle_iterator&) = delete;
        gentle_iterator(gentle_iterator&&) = delete;
    };
    std::list<gentle_iterator> _gentle_iterators;
    std::vector<server_socket> _listeners;

public:
    server(const sstring& server_name, logging::logger& logger);

    virtual ~server();

    // Makes sure listening sockets no longer generate new connections and aborts the
    // connected sockets, so that new requests are not served and existing requests don't
    // send responses back.
    //
    // It does _not_ wait for any internal activity started by the established connections
    // to finish. It's the .stop() method that does it
    future<> shutdown();
    future<> stop();

    future<> listen(socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool is_shard_aware, bool keepalive, std::optional<file_permissions> unix_domain_socket_permissions);

    future<> do_accepts(int which, bool keepalive, socket_address server_addr);

protected:
    virtual seastar::shared_ptr<connection> make_connection(socket_address server_addr, connected_socket&& fd, socket_address addr) = 0;

    virtual future<> advertise_new_connection(shared_ptr<connection> conn);

    virtual future<> unadvertise_connection(shared_ptr<connection> conn);

    future<> for_each_gently(noncopyable_function<future<>(connection&)>);
};

}
