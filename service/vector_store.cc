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
#include <system_error>

using namespace seastar;

// NOLINTNEXTLINE(misc-use-internal-linkage,cppcoreguidelines-avoid-non-const-global-variables)
logging::logger vslogger("vector_store");

namespace {

using embedding = service::vector_store::embedding;
using limit = service::vector_store::limit;
using primary_key = service::vector_store::primary_key;
using primary_keys = service::vector_store::primary_keys;
using service_reply_format_error = service::vector_store::service_reply_format_error;
using ann_error = service::vector_store::ann_error;

auto parse_port(char const* port_env) -> std::optional<std::uint16_t> {
    auto port = std::uint16_t{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    auto [ptr, ec] = std::from_chars(port_env, port_env + std::strlen(port_env), port);
    if (*ptr != '\0' || ec != std::errc{}) {
        return std::nullopt;
    }
    return port;
}

auto parse_addr(sstring const& host) -> std::optional<net::inet_address> {
    try {
        return net::inet_address(host);
    } catch (const std::invalid_argument& e) {
        return std::nullopt;
    }
}

auto get_key_column_value(const rjson::value& item, std::size_t idx, const column_definition& column) -> std::expected<bytes, ann_error> {
    auto const& column_name = column.name_as_text();
    auto const* keys_obj = rjson::find(item, column_name);
    if (keys_obj == nullptr) {
        vslogger.error("Vector Store returned invalid JSON: missing key column '{}'", column_name);
        return std::unexpected{service_reply_format_error{}};
    }
    if (!keys_obj->IsArray()) {
        vslogger.error("Vector Store returned invalid JSON: key column '{}' is not an array", column_name);
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& keys_arr = keys_obj->GetArray();
    if (keys_arr.Size() <= idx) {
        vslogger.error("Vector Store returned invalid JSON: key column '{}' array too small", column_name);
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& key = keys_arr[idx];
    return column.type->from_string(rjson::print(key));
}

auto pk_from_json(rjson::value const& item, std::size_t idx, schema_ptr const& schema) -> std::expected<partition_key, ann_error> {
    std::vector<bytes> raw_pk;
    for (const column_definition& cdef : schema->partition_key_columns()) {
        auto raw_value = get_key_column_value(item, idx, cdef);
        if (!raw_value) {
            return std::unexpected{raw_value.error()};
        }
        raw_pk.emplace_back(*raw_value);
    }
    return partition_key::from_exploded(raw_pk);
}

auto ck_from_json(rjson::value const& item, std::size_t idx, schema_ptr const& schema) -> std::expected<clustering_key_prefix, ann_error> {
    if (schema->clustering_key_size() == 0) {
        return clustering_key_prefix::make_empty();
    }

    std::vector<bytes> raw_ck;
    for (const column_definition& cdef : schema->clustering_key_columns()) {
        auto raw_value = get_key_column_value(item, idx, cdef);
        if (!raw_value) {
            return std::unexpected{raw_value.error()};
        }
        raw_ck.emplace_back(*raw_value);
    }

    return clustering_key_prefix::from_exploded(raw_ck);
}

auto write_ann_json(output_stream<char> out, embedding embedding, limit limit) -> future<> {
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
}

auto read_ann_json(rjson::value const& json, schema_ptr const& schema) -> std::expected<primary_keys, ann_error> {
    if (!json.HasMember("primary_keys")) {
        vslogger.error("Vector Store returned invalid JSON: missing 'primary_keys'");
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& keys_json = json["primary_keys"];
    if (!keys_json.IsObject()) {
        vslogger.error("Vector Store returned invalid JSON: 'primary_keys' is not an object");
        return std::unexpected{service_reply_format_error{}};
    }

    if (!json.HasMember("distances")) {
        vslogger.error("Vector Store returned invalid JSON: missing 'distances'");
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& distances_json = json["distances"];
    if (!distances_json.IsArray()) {
        vslogger.error("Vector Store returned invalid JSON: 'distances' is not an array");
        return std::unexpected{service_reply_format_error{}};
    }
    auto const& distances_arr = json["distances"].GetArray();

    auto size = distances_arr.Size();
    auto keys = primary_keys{};
    for (auto idx = 0U; idx < size; ++idx) {
        auto pk = pk_from_json(keys_json, idx, schema);
        if (!pk) {
            return std::unexpected{pk.error()};
        }
        auto ck = ck_from_json(keys_json, idx, schema);
        if (!ck) {
            return std::unexpected{ck.error()};
        }
        keys.push_back(primary_key{dht::decorate_key(*schema, *pk), *ck});
    }
    return std::move(keys);
}

} // namespace

namespace service {

vector_store::vector_store(char const* ip_env, char const* port_env) {
    if (ip_env == nullptr || port_env == nullptr) {
        vslogger.info("Disable Vector Store: Missing environment values");
        return;
    }

    auto port = parse_port(port_env);
    if (!port) {
        vslogger.error("Disable Vector Store: Invalid port number: {}", port_env);
        return;
    }

    auto addr = parse_addr(ip_env);
    if (!addr) {
        vslogger.error("Disable Vector Store: Invalid ip address: {}", ip_env);
        return;
    }

    _host = seastar::format("{}:{}", *addr, *port);
    _client = std::make_unique<client>(socket_address(*addr, *port));
}

vector_store::~vector_store() = default;

void vector_store::set_service(socket_address const& addr) {
    _client = std::make_unique<client>(addr);
    _host = format("{}", addr);
}

auto vector_store::ann(keyspace_name keyspace, index_name name, schema_ptr schema, embedding embedding, limit limit) const
        -> future<std::expected<primary_keys, ann_error>> {
    if (!_client) {
        vslogger.error("Disabled Vector Store while calling ann");
        co_return std::unexpected{disabled{}};
    }

    auto req = http::request::make("POST", _host, format("/api/v1/indexes/{}/{}/ann", keyspace, name));
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines)
    req.write_body("json", [limit, embedding = std::move(embedding)](output_stream<char> out) mutable -> future<> {
        co_await write_ann_json(std::move(out), std::move(embedding), limit);
    });

    auto resp_status = make_lw_shared(http::reply::status_type::ok);
    auto resp_content = make_lw_shared<std::vector<temporary_buffer<char>>>();
    try {
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines, cppcoreguidelines-avoid-reference-coroutine-parameters)
        co_await _client->make_request(std::move(req), [resp_status, resp_content](http::reply const& reply, input_stream<char> body) -> future<> {
            *resp_status = reply._status;
            *resp_content = co_await util::read_entire_stream(body);
        });
    } catch (const std::system_error& e) {
        vslogger.error("Vector Store service unavailable: {}", e.what());
        co_return std::unexpected{service_unavailable{}};
    }

    if (*resp_status != status_type::ok) {
        vslogger.error("Vector Store returned error: HTTP status {}: {}", *resp_status, *resp_content);
        co_return std::unexpected{service_error{*resp_status}};
    }

    try {
        co_return read_ann_json(rjson::parse(std::move(*resp_content)), schema);
    } catch (const rjson::error& e) {
        vslogger.error("Vector Store returned invalid JSON: {}", e.what());
        co_return std::unexpected{service_reply_format_error{}};
    }
}

} // namespace service

