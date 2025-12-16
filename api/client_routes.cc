/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

 #include <seastar/http/short_streams.hh>

#include "client_routes.hh"
#include "api/api.hh"
#include "service/storage_service.hh"
#include "service/client_routes.hh"
#include "utils/rjson.hh"


#include "api/api-doc/client_routes.json.hh"

using namespace seastar::httpd;
using namespace std::chrono_literals;
using namespace json;

extern logging::logger apilog;

namespace api {

static void validate_client_routes_endpoint(sharded<service::client_routes_service>& cr, sstring endpoint_name) {
    if (!cr.local().get_feature_service().client_routes) {
        apilog.warn("{}: called before the cluster feature was enabled", endpoint_name);
        throw std::runtime_error(fmt::format("{} requires all nodes to support the CLIENT_ROUTES cluster feature", endpoint_name));
    }
}

static sstring parse_string(const char* name, rapidjson::Value const& v) {
    const auto it = v.FindMember(name);
    if (it == v.MemberEnd()) {
        throw bad_param_exception(fmt::format("Missing '{}'", name));
    }
    if (!it->value.IsString()) {
        throw bad_param_exception(fmt::format("'{}' must be a string", name));
    }
    return {it->value.GetString(), it->value.GetStringLength()};
}

static std::optional<uint32_t> parse_port(const char* name, rapidjson::Value const& v) {
    const auto it = v.FindMember(name);
    if (it == v.MemberEnd()) {
        return std::nullopt;
    }
    if (!it->value.IsInt()) {
        throw bad_param_exception(fmt::format("'{}' must be an integer", name));
    }
    auto port = it->value.GetInt();
    if (port < 1 || port > 65535) {
        throw bad_param_exception(fmt::format("'{}' value={} is outside the allowed port range", name, port));
    }
    return port;
}

static std::vector<service::client_routes_service::client_route_entry> parse_set_client_array(const rapidjson::Document& root) {
    if (!root.IsArray()) {
        throw bad_param_exception("Body must be a JSON array");
    }

    std::vector<service::client_routes_service::client_route_entry> v;
    v.reserve(root.GetArray().Size());
    for (const auto& element : root.GetArray()) {
        if (!element.IsObject()) { throw bad_param_exception("Each element must be object"); }

        const auto port = parse_port("port", element);
        const auto tls_port = parse_port("tls_port", element);
        const auto alternator_port = parse_port("alternator_port", element);
        const auto alternator_https_port = parse_port("alternator_https_port", element);

        if (!port.has_value() && !tls_port.has_value() && !alternator_port.has_value() && !alternator_https_port.has_value()) {
            throw bad_param_exception("At least one port field ('port', 'tls_port', 'alternator_port', 'alternator_https_port') must be specified");
        }

        v.emplace_back(
            parse_string("connection_id", element),
            utils::UUID{parse_string("host_id", element)},
            parse_string("address", element),
            port,
            tls_port,
            alternator_port,
            alternator_https_port
        );
    }

    return v;
}

static
future<json::json_return_type>
rest_set_client_routes(http_context& ctx, sharded<service::client_routes_service>& cr, std::unique_ptr<http::request> req) {
    validate_client_routes_endpoint(cr, "rest_set_client_routes");

    rapidjson::Document root;
    auto content = co_await util::read_entire_stream_contiguous(*req->content_stream);
    root.Parse(content.c_str());
    const auto route_entries = parse_set_client_array(root);

    co_await cr.local().set_client_routes(route_entries);
    co_return seastar::json::json_void();
}

static std::vector<service::client_routes_service::client_route_key> parse_delete_client_array(const rapidjson::Document& root) {
    if (!root.IsArray()) {
        throw bad_param_exception("Body must be a JSON array");
    }

    std::vector<service::client_routes_service::client_route_key> v;
    v.reserve(root.GetArray().Size());
    for (const auto& element : root.GetArray()) {
        v.emplace_back(
            parse_string("connection_id", element),
            utils::UUID{parse_string("host_id", element)}
        );
    }

    return v;
}

static
future<json::json_return_type>
rest_delete_client_routes(http_context& ctx, sharded<service::client_routes_service>& cr, std::unique_ptr<http::request> req) {
    validate_client_routes_endpoint(cr, "delete_client_routes");

    rapidjson::Document root;
    auto content = co_await util::read_entire_stream_contiguous(*req->content_stream);
    root.Parse(content.c_str());

    const auto route_keys = parse_delete_client_array(root);
    co_await cr.local().delete_client_routes(route_keys);
    co_return seastar::json::json_void();
}

static
future<json::json_return_type>
rest_get_client_routes(http_context& ctx, sharded<service::client_routes_service>& cr, std::unique_ptr<http::request> req) {
    validate_client_routes_endpoint(cr, "get_client_routes");

    co_return co_await cr.invoke_on(0, [] (service::client_routes_service& cr) -> future<json::json_return_type> {
        co_return json::json_return_type(stream_range_as_array(co_await cr.get_client_routes(), [](const service::client_routes_service::client_route_entry & entry) {
            seastar::httpd::client_routes_json::client_routes_entry obj;
            obj.connection_id = entry.connection_id;
            obj.host_id = fmt::to_string(entry.host_id);
            obj.address = entry.address;
            if (entry.port.has_value()) { obj.port = entry.port.value(); }
            if (entry.tls_port.has_value()) { obj.tls_port = entry.tls_port.value(); }
            if (entry.alternator_port.has_value()) { obj.alternator_port = entry.alternator_port.value(); }
            if (entry.alternator_https_port.has_value()) { obj.alternator_https_port = entry.alternator_https_port.value(); }
            return obj;
        }));
    });
}

void set_client_routes(http_context& ctx, routes& r, sharded<service::client_routes_service>& cr) {
    seastar::httpd::client_routes_json::set_client_routes.set(r, [&ctx, &cr] (std::unique_ptr<seastar::http::request> req) {
        return rest_set_client_routes(ctx, cr, std::move(req));
    });
    seastar::httpd::client_routes_json::delete_client_routes.set(r, [&ctx, &cr] (std::unique_ptr<seastar::http::request> req) {
        return rest_delete_client_routes(ctx, cr, std::move(req));
    });
    seastar::httpd::client_routes_json::get_client_routes.set(r, [&ctx, &cr] (std::unique_ptr<seastar::http::request> req) {
        return rest_get_client_routes(ctx, cr, std::move(req));
    });
}

void unset_client_routes(http_context& ctx, routes& r) {
    seastar::httpd::client_routes_json::set_client_routes.unset(r);
    seastar::httpd::client_routes_json::delete_client_routes.unset(r);
    seastar::httpd::client_routes_json::get_client_routes.unset(r);
}

}
