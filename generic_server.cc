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

#include "generic_server.hh"

namespace generic_server {

connection::connection(server& server, connected_socket&& fd)
    : _server{server}
    , _fd{std::move(fd)}
{
    ++_server._total_connections;
    ++_server._current_connections;
    _server._connections_list.push_back(*this);
}

connection::~connection()
{
    --_server._current_connections;
    _server._connections_list.erase(_server._connections_list.iterator_to(*this));
    _server.maybe_idle();
}

future<> connection::shutdown()
{
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
    }
    return make_ready_future<>();
}

server::~server()
{
}

void server::maybe_idle() {
    if (_stopping && !_connections_being_accepted && !_current_connections) {
        _all_connections_stopped.set_value();
    }
}

}
