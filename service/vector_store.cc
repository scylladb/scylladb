/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store.hh"
#include "dht/i_partitioner.hh"
#include "keys.hh"
#include "utils/rjson.hh"
#include "schema/schema.hh"
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/short_streams.hh>
#include <stdexcept>

using namespace seastar;

logging::logger vslogger("vector_store");

namespace {

bytes get_key_column_value(const rjson::value& item, std::size_t idx, const column_definition& column) {
    auto const& column_name = column.name_as_text();
    auto const* keys_obj = rjson::find(item, column_name);
    auto const& keys_arr = keys_obj->GetArray();
    auto const& key = keys_arr[idx];
    return column.type->from_string(rjson::print(key));
}

partition_key pk_from_json(rjson::value const& item, std::size_t idx, schema_ptr const& schema) {
    std::vector<bytes> raw_pk;
    for (const column_definition& cdef : schema->partition_key_columns()) {
        bytes raw_value = get_key_column_value(item, idx, cdef);
        raw_pk.push_back(std::move(raw_value));
    }
    return partition_key::from_exploded(raw_pk);
}

clustering_key_prefix ck_from_json(rjson::value const& item, std::size_t idx, schema_ptr const& schema) {
    if (schema->clustering_key_size() == 0) {
        return clustering_key_prefix::make_empty();
    }
    std::vector<bytes> raw_ck;
    for (const column_definition& cdef : schema->clustering_key_columns()) {
        bytes raw_value = get_key_column_value(item, idx, cdef);
        raw_ck.push_back(std::move(raw_value));
    }

    return clustering_key_prefix::from_exploded(raw_ck);
}

} // namespace

namespace service {

vector_store::vector_store() {
    auto const* ip_env = std::getenv("VECTOR_STORE_IP");
    auto const* port_env = std::getenv("VECTOR_STORE_PORT");

    if (ip_env == nullptr || port_env == nullptr) {
        vslogger.info("Disable Vector Store: Missing environment values");
        return;
    }

    auto port = std::uint16_t{};
    auto [ptr, ec] = std::from_chars(port_env, port_env + std::strlen(port_env), port);
    if (*ptr != '\0' || ec != std::errc{}) {
        vslogger.error("Disable Vector Store: Invalid port number: {}", port_env);
        return;
    }

    _host = ip_env;
    auto addr = net::inet_address{};
    try {
        addr = net::inet_address(_host);
    } catch (const std::invalid_argument& e) {
        vslogger.error("Disable Vector Store: Invalid ip address: {}", _host);
        return;
    }

    _client = std::make_unique<client_t>(socket_address(addr, port));
}

vector_store::~vector_store() = default;

void vector_store::set_service(socket_address const& addr) {
    _client = std::make_unique<client_t>(addr);
    _host = format("{}", addr);
}

[[nodiscard]] auto vector_store::ann(keyspace_name_t keyspace, index_name_t name, schema_ptr schema, embedding_t embedding, limit_t limit) const
        -> future<std::optional<std::vector<primary_key_t>>> {
    // TODO: fix error checking in the method

    if (!_client) {
        vslogger.error("Disabled Vector Store while calling ann");
        co_return std::nullopt;
    }

    auto req = http::request::make("POST", _host, format("/api/v1/indexes/{}/{}/ann", keyspace, name));
    req.write_body("json", [limit, embedding = std::move(embedding)](output_stream<char> out) -> future<> {
        std::exception_ptr ex;
        try {
            co_await out.write(R"({"embedding":[)");
            auto first = true;
            for (auto value : embedding) {
                if (!first) {
                    co_await out.write(",");
                } else {
                    first = false;
                }
                co_await out.write(format("{}", value));
            }
            co_await out.write(format(R"(],"limit":{}}})", limit));
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    });

    auto resp_status = make_lw_shared(http::reply::status_type::ok);
    auto resp_content = make_lw_shared<std::vector<temporary_buffer<char>>>();
    co_await _client->make_request(std::move(req), [resp_status, resp_content](http::reply const& reply, input_stream<char> body) -> future<> {
        *resp_status = reply._status;
        *resp_content = co_await util::read_entire_stream(body);
    });

    auto const resp = rjson::parse(std::move(*resp_content));
    auto const& primary_keys = resp["primary_keys"];
    auto const distances = resp["distances"].GetArray();
    auto size = distances.Size();
    auto keys = std::vector<primary_key_t>{};
    for (auto idx = 0U; idx < size; ++idx) {
        auto pk = pk_from_json(primary_keys, idx, schema);
        auto ck = ck_from_json(primary_keys, idx, schema);
        keys.push_back(primary_key_t{dht::decorate_key(*schema, std::move(pk)), ck});
    }
    co_return std::optional{keys};
}

} // namespace service

