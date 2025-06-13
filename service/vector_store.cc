/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/type_json.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "dht/i_partitioner.hh"
#include "keys.hh"
#include "utils/rjson.hh"
#include "schema/schema.hh"
#include <charconv>
#include <regex>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/net/dns.hh>
#include <seastar/util/short_streams.hh>

// NOLINTNEXTLINE(misc-use-internal-linkage,cppcoreguidelines-avoid-non-const-global-variables)
logging::logger vslogger("vector_store");

namespace {

using ann_error = service::vector_store::ann_error;
using client = http::experimental::client;
using configuration_exception = exceptions::configuration_exception;
using embedding = service::vector_store::embedding;
using limit = service::vector_store::limit;
using host_name = service::vector_store::host_name;
using port_number = service::vector_store::port_number;
using primary_key = service::vector_store::primary_key;
using primary_keys = service::vector_store::primary_keys;
using service_reply_format_error = service::vector_store::service_reply_format_error;

auto parse_port(std::string const& port_txt) -> std::optional<port_number> {
    auto port = port_number{};
    auto [ptr, ec] = std::from_chars(&*port_txt.begin(), &*port_txt.end(), port);
    if (*ptr != '\0' || ec != std::errc{}) {
        return std::nullopt;
    }
    return port;
}

auto parse_service_uri(std::string_view uri) -> std::optional<std::tuple<host_name, port_number>> {
    constexpr auto URI_REGEX = R"(^http:\/\/([a-z0-9._-]+):([0-9]+)$)";
    auto const uri_regex = std::regex(URI_REGEX);
    auto uri_match = std::smatch{};
    auto uri_txt = std::string(uri);
    if (!std::regex_match(uri_txt, uri_match, uri_regex) || uri_match.size() != 3) {
        return {};
    }
    auto host = uri_match[1].str();
    auto port = parse_port(uri_match[2].str());
    if (!port) {
        return {};
    }
    return {{host, *port}};
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
    return from_json_object(*column.type, key);
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

/// Stop and remove old clients if they are not used anymore.
auto cleanup_old_clients(std::vector<lw_shared_ptr<client>>& clients) -> future<> {
    for (auto& client : clients) {
        if (client && client.owned()) {
            co_await client->close();
            client = nullptr;
        }
    }
    std::erase_if(clients, [](auto const& client) {
        return !client;
    });
}

} // namespace

namespace service {

vector_store::vector_store(config const& cfg) {
    auto config_uri = cfg.vector_store_uri();
    if (config_uri.empty()) {
        return;
    }

    auto parsed_uri = parse_service_uri(config_uri);
    if (!parsed_uri) {
        throw configuration_exception(format("Invalid Vector Store service URI: {}", config_uri));
    }

    std::tie(_host, _port) = *parsed_uri;
}

vector_store::~vector_store() = default;

auto vector_store::stop() -> future<> {
    if (_client) {
        co_await _client->close();
    }
    _client = nullptr;
    for (auto& client : _old_clients) {
        if (!client) {
            continue;
        }
        co_await client->close();
    }
    _old_clients.clear();
}

auto vector_store::ann(keyspace_name keyspace, index_name name, schema_ptr schema, embedding embedding, limit limit)
        -> future<std::expected<primary_keys, ann_error>> {
    if (is_disabled()) {
        vslogger.error("Disabled Vector Store while calling ann");
        co_return std::unexpected{disabled{}};
    }

    if (!_client) {
        if (!co_await refresh_service_addr()) {
            co_return std::unexpected{addr_unavailable{}};
        }
    }

    auto req = http::request::make("POST", _host, format("/api/v1/indexes/{}/{}/ann", keyspace, name));
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines)
    req.write_body("json", [limit, embedding = std::move(embedding)](output_stream<char> out) mutable -> future<> {
        co_await write_ann_json(std::move(out), std::move(embedding), limit);
    });

    auto resp_status = http::reply::status_type::ok;
    auto resp_content = std::vector<temporary_buffer<char>>();
    auto retry = true;
    for (;;) {
        auto exception_txt = sstring{};
        try {
            // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines, cppcoreguidelines-avoid-reference-coroutine-parameters)
            co_await _client->make_request(std::move(req), [&resp_status, &resp_content](http::reply const& reply, input_stream<char> body) -> future<> {
                resp_status = reply._status;
                resp_content = co_await util::read_entire_stream(body);
            });
            break;
        } catch (const std::system_error& e) {
            exception_txt = e.what();
        }
        if (!retry) {
            vslogger.error("Vector Store service unavailable: {}", exception_txt);
            co_return std::unexpected{service_unavailable{}};
        }
        if (!co_await refresh_service_addr()) {
            co_return std::unexpected{addr_unavailable{}};
        }
        retry = false; // Only retry once
    }

    if (resp_status != status_type::ok) {
        vslogger.error("Vector Store returned error: HTTP status {}: {}", resp_status, resp_content);
        co_return std::unexpected{service_error{resp_status}};
    }

    try {
        co_return read_ann_json(rjson::parse(std::move(resp_content)), schema);
    } catch (const rjson::error& e) {
        vslogger.error("Vector Store returned invalid JSON: {}", e.what());
        co_return std::unexpected{service_reply_format_error{}};
    }
}

auto vector_store::refresh_service_addr() -> future<bool> {
    if (is_disabled()) {
        co_return false;
    }

    // Do not refresh the service address too often
    if (_client) {
        // Minimum period between dns name refreshes
        constexpr auto REFRESH_PERIOD = std::chrono::seconds(5);
        if (lowres_system_clock::now() - _last_dns_refresh < REFRESH_PERIOD) {
            co_return true;
        }
    }

    _last_dns_refresh = lowres_system_clock::now();
    auto addr = inet_address{};
    try {
        addr = co_await net::dns::resolve_name(_host);
    } catch (std::system_error const&) {
        // addr should be empty if the resolution failed
    }
    if (addr.is_addr_any()) {
        if (_client) {
            // If we have a client, save it for later cleanup
            _old_clients.emplace_back(std::move(_client));
        }

        co_await cleanup_old_clients(_old_clients);
        co_return false;
    }

    // Check if the new address is the same as the current one
    if (_client) {
        if (addr == _addr) {
            co_return true; // Address was refreshed but remains the same
        }

        auto old_client = std::move(_client);
        if (old_client.owned()) {
            // Close the old client if it is not in use
            co_await old_client->close();
        } else {
            // Save the old client for later cleanup
            _old_clients.emplace_back(std::move(old_client));
        }
    }

    _addr = addr;
    _client = make_lw_shared<client>(socket_address(_addr, _port));

    co_await cleanup_old_clients(_old_clients);
    co_return true;
}

} // namespace service

