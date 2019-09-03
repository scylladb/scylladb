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

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include "seastarx.hh"
#include <seastar/json/json_elements.hh>

#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "service/client_state.hh"

#include "stats.hh"

namespace alternator {

class executor {
    service::storage_proxy& _proxy;
    service::migration_manager& _mm;

public:
    using client_state = service::client_state;
    stats _stats;
    static constexpr auto ATTRS_COLUMN_NAME = ":attrs";
    static constexpr auto KEYSPACE_NAME = "alternator";

    executor(service::storage_proxy& proxy, service::migration_manager& mm) : _proxy(proxy), _mm(mm) {}

    future<json::json_return_type> create_table(client_state& client_state, std::string content);
    future<json::json_return_type> describe_table(client_state& client_state, std::string content);
    future<json::json_return_type> delete_table(client_state& client_state, std::string content);
    future<json::json_return_type> put_item(client_state& client_state, std::string content);
    future<json::json_return_type> get_item(client_state& client_state, std::string content);
    future<json::json_return_type> delete_item(client_state& client_state, std::string content);
    future<json::json_return_type> update_item(client_state& client_state, std::string content);
    future<json::json_return_type> list_tables(client_state& client_state, std::string content);
    future<json::json_return_type> scan(client_state& client_state, std::string content);
    future<json::json_return_type> describe_endpoints(client_state& client_state, std::string content, std::string host_header);
    future<json::json_return_type> batch_write_item(client_state& client_state, std::string content);
    future<json::json_return_type> batch_get_item(client_state& client_state, std::string content);
    future<json::json_return_type> query(client_state& client_state, std::string content);

    future<> start();
    future<> stop() { return make_ready_future<>(); }

    future<> maybe_create_keyspace();

    static void maybe_trace_query(client_state& client_state, sstring_view op, sstring_view query);
};

}
