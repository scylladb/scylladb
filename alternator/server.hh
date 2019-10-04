/*
 * Copyright 2019 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "alternator/executor.hh"
#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/tls.hh>
#include <optional>

namespace alternator {

class server {
    seastar::httpd::http_server_control _control;
    seastar::httpd::http_server_control _https_control;
    seastar::sharded<executor>& _executor;
    bool _enforce_authorization;
public:
    server(seastar::sharded<executor>& executor) : _executor(executor) {}

    seastar::future<> init(net::inet_address addr, std::optional<uint16_t> port, std::optional<uint16_t> https_port, std::optional<tls::credentials_builder> creds, bool enforce_authorization);
private:
    void set_routes(seastar::httpd::routes& r);
};

}

