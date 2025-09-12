/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_store_client.hh"
#include "vector_search/high_availability.hh"
#include "vector_search/exception.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/type_json.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "utils/sequential_producer.hh"
#include "dht/i_partitioner.hh"
#include "keys/keys.hh"
#include "utils/rjson.hh"
#include "schema/schema.hh"
#include <charconv>
#include <exception>
#include <fmt/ranges.h>
#include <regex>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/short_streams.hh>

namespace {

using namespace std::chrono_literals;

using ann_error = service::vector_store_client::ann_error;
using configuration_exception = exceptions::configuration_exception;
using duration = lowres_clock::duration;
using embedding = service::vector_store_client::embedding;
using limit = service::vector_store_client::limit;
using host_name = service::vector_store_client::host_name;
using http_path = sstring;
using inet_address = seastar::net::inet_address;
using json_content = sstring;
using milliseconds = std::chrono::milliseconds;
using operation_type = httpd::operation_type;
using port_number = service::vector_store_client::port_number;
using primary_key = service::vector_store_client::primary_key;
using primary_keys = service::vector_store_client::primary_keys;
using service_reply_format_error = service::vector_store_client::service_reply_format_error;
using tcp_keepalive_params = net::tcp_keepalive_params;
using time_point = lowres_clock::time_point;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
logging::logger vslogger("vector_store_client");

auto parse_port(std::string const& port_txt) -> std::optional<port_number> {
    auto port = port_number{};
    auto [ptr, ec] = std::from_chars(&*port_txt.begin(), &*port_txt.end(), port);
    if (*ptr != '\0' || ec != std::errc{}) {
        return std::nullopt;
    }
    return port;
}

using host_port = service::vector_search::high_availability::uri;

auto parse_service_uri(std::string_view uri) -> std::optional<host_port> {
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

bool should_vector_store_service_be_disabled(std::string_view const& uri) {
    return uri.empty();
}

auto get_host_port(std::string_view uri) -> std::optional<host_port> {
    if (should_vector_store_service_be_disabled(uri)) {
        vslogger.info("Vector Store service URI is empty, disabling Vector Store service");
        return std::nullopt;
    }
    auto parsed = parse_service_uri(uri);
    if (!parsed) {
        throw configuration_exception(fmt::format("Invalid Vector Store service URI: {}", uri));
    }
    vslogger.info("Vector Store service URI is set to '{}'", uri);
    return *parsed;
}

} // namespace

namespace service {

struct vector_store_client::impl {

    utils::observer<sstring> uri_observer;
    vector_search::high_availability ha;
    std::optional<host_port> _uri;

    impl(utils::config_file::named_value<sstring> cfg)
        : uri_observer(cfg.observe([this](std::string_view uri) -> future<> {
            try {
                _uri = get_host_port(uri);
            } catch (const configuration_exception& e) {
                vslogger.error("Failed to parse Vector Store service URI: {}", e.what());
                _uri = std::nullopt;
            }
            co_await ha.set_uri(_uri);
        }))
        , ha([](auto const& host) -> future<std::optional<inet_address>> {
            auto addr = co_await coroutine::as_future(net::dns::resolve_name(host));
            if (addr.failed()) {
                auto err = addr.get_exception();
                if (try_catch<std::system_error>(err) != nullptr) {
                    co_return std::nullopt;
                }
                co_await coroutine::return_exception_ptr(std::move(err));
            }
            co_return co_await std::move(addr);
        })
        , _uri(get_host_port(cfg())) {
    }
};

vector_store_client::vector_store_client(config const& cfg)
    : _impl(std::make_unique<impl>(cfg.vector_store_uri)) {
}

vector_store_client::~vector_store_client() = default;

void vector_store_client::start_background_tasks() {
    auto f = start();
}

auto vector_store_client::start() -> future<> {
    return _impl->ha.set_uri(_impl->_uri);
}

auto vector_store_client::stop() -> future<> {
    return _impl->ha.stop();
}

auto vector_store_client::ann(keyspace_name keyspace, index_name name, schema_ptr schema, embedding embedding, limit limit, abort_source& as)
        -> future<std::expected<primary_keys, ann_error>> {
    try {
        auto content = co_await _impl->ha.ann(std::move(keyspace), std::move(name), std::move(embedding), limit, &as);
        co_return read_ann_json(rjson::parse(std::move(content)), schema);
    } catch (const rjson::error& e) {
        vslogger.error("{}", e.what());
        co_return std::unexpected{service_reply_format_error{}};
    } catch (const vector_search::service_address_unavailable_exception& e) {
        vslogger.error("{}", e.what());
        co_return std::unexpected{addr_unavailable{}};
    } catch (const vector_search::service_disabled_exception& e) {
        vslogger.error("{}", e.what());
        co_return std::unexpected{disabled{}};
    } catch (const vector_search::service_status_exception& e) {
        vslogger.error("{}: {}", e.what(), e.content());
        co_return std::unexpected{service_error{e.status()}};
    } catch (const seastar::abort_requested_exception& e) {
        vslogger.error("{}", e.what());
        co_return std::unexpected{aborted{}};
    } catch (const std::exception& e) {
        vslogger.error("{}", e.what());
        co_return std::unexpected{service_unavailable{}};
    } catch (...) {
        vslogger.error("Vector Store ann request failed with unknown exception");
        co_return std::unexpected{service_unavailable{}};
    }
}

void vector_store_client_tester::set_dns_refresh_interval(vector_store_client& vsc, std::chrono::milliseconds interval) {
    vsc._impl->ha.set_dns_refresh_interval(interval);
}

void vector_store_client_tester::set_dns_resolver(vector_store_client& vsc, std::function<future<std::optional<inet_address>>(sstring const&)> resolver) {
    vsc._impl->ha.set_dns_resolver(std::move(resolver));
}

} // namespace service
