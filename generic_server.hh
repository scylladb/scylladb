/*
 * Copyright (C) 2021 ScyllaDB
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

#include "seastarx.hh"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/net/api.hh>

#include <boost/intrusive/list.hpp>

namespace generic_server {

class server;

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

    virtual future<> process_request() = 0;

    virtual void on_connection_close();

    virtual future<> shutdown();
};

class server {
    friend class connection;

protected:
    bool _stopping = false;
    promise<> _all_connections_stopped;
    uint64_t _current_connections = 0;
    uint64_t _connections_being_accepted = 0;
    uint64_t _total_connections = 0;
    future<> _stopped = _all_connections_stopped.get_future();
    boost::intrusive::list<connection> _connections_list;

    void maybe_idle();
};

}
