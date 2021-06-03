/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "log.hh"

#include "seastarx.hh"

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
    bool _stopping = false;
    promise<> _all_connections_stopped;
    uint64_t _current_connections = 0;
    uint64_t _connections_being_accepted = 0;
    uint64_t _total_connections = 0;
    future<> _stopped = _all_connections_stopped.get_future();
    boost::intrusive::list<connection> _connections_list;
    std::vector<server_socket> _listeners;

public:
    server(const sstring& server_name, logging::logger& logger);

    virtual ~server();

    future<> stop();

    future<> listen(socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool is_shard_aware, bool keepalive);

    future<> do_accepts(int which, bool keepalive, socket_address server_addr);

protected:
    virtual seastar::shared_ptr<connection> make_connection(socket_address server_addr, connected_socket&& fd, socket_address addr) = 0;

    virtual future<> on_stop();

    virtual future<> advertise_new_connection(shared_ptr<connection> conn);

    virtual future<> unadvertise_connection(shared_ptr<connection> conn);

    void maybe_stop();
};

}
