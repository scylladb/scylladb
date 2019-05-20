/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include "seastarx.hh"
#include <seastar/json/json_elements.hh>

#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"

namespace alternator {

class executor {
    service::storage_proxy& _proxy;
    service::migration_manager& _mm;

public:
    static constexpr auto ATTRS_COLUMN_NAME = "attrs";
    static constexpr auto KEYSPACE_NAME = "alternator";

    executor(service::storage_proxy& proxy, service::migration_manager& mm) : _proxy(proxy), _mm(mm) {}

    future<json::json_return_type> create_table(std::string content);
    future<json::json_return_type> describe_table(std::string content);
    future<json::json_return_type> delete_table(std::string content);
    future<json::json_return_type> put_item(std::string content);
    future<json::json_return_type> get_item(std::string content);
    future<json::json_return_type> update_item(std::string content);
    future<json::json_return_type> list_tables(std::string content);
    future<json::json_return_type> scan(std::string content);
    future<json::json_return_type> describe_endpoints(std::string content, std::string host_header);
    future<json::json_return_type> batch_write_item(std::string content);
    future<json::json_return_type> query(std::string content);

    future<> start();
    future<> stop() { return make_ready_future<>(); }
};

}
