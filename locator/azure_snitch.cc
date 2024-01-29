/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2021-present ScyllaDB
 */

#include "locator/azure_snitch.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/http/response_parser.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>
#include <seastar/util/closeable.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <fmt/format.h>

#include "utils/class_registrator.hh"

namespace locator {

const std::string azure_snitch::REGION_NAME_QUERY_PATH = fmt::format(AZURE_QUERY_PATH_TEMPLATE, "location");
const std::string azure_snitch::ZONE_NAME_QUERY_PATH = fmt::format(AZURE_QUERY_PATH_TEMPLATE, "zone");

azure_snitch::azure_snitch(const snitch_config& cfg) : production_snitch_base(cfg) {
    if (this_shard_id() == cfg.io_cpu_id) {
        io_cpu_id() = cfg.io_cpu_id;
    }
}

future<> azure_snitch::load_config() {
    if (this_shard_id() != io_cpu_id()) {
        co_return;
    }

    sstring region = co_await azure_api_call(REGION_NAME_QUERY_PATH);
    sstring azure_zone = co_await azure_api_call(ZONE_NAME_QUERY_PATH);

    sstring datacenter_suffix = co_await read_property_file();

    sstring azure_region = region + datacenter_suffix;

    logger().info("AzureSnitch using region: {}, zone: {}.", azure_region, azure_zone);

    // Zoneless regions return empty zone
    sstring my_rack = (azure_zone != "" ? azure_zone : azure_region);
    sstring my_dc = azure_region;

    co_return co_await container().invoke_on_all([my_dc, my_rack] (snitch_ptr& local_s) {
        local_s->set_my_dc_and_rack(my_dc, my_rack);
    });
}

future<> azure_snitch::start() {
    _state = snitch_state::initializing;

    return load_config().then([this] {
        set_snitch_ready();
    });
}

future<sstring> azure_snitch::azure_api_call(sstring path) {
    return seastar::async([path = std::move(path)] () -> sstring {
        using namespace boost::algorithm;

        net::inet_address a = seastar::net::dns::resolve_name(AZURE_SERVER_ADDR, net::inet_address::family::INET).get();
        connected_socket sd(connect(socket_address(a, 80)).get());
        input_stream<char> in(sd.input());
        output_stream<char> out(sd.output());
        auto close_in = deferred_close(in);
        auto close_out = deferred_close(out);
        sstring request(seastar::format("GET {} HTTP/1.1\r\nHost: {}\r\nMetadata: True\r\n\r\n", path, AZURE_SERVER_ADDR));

        out.write(request).get();
        out.flush().get();

        http_response_parser parser;
        parser.init();
        in.consume(parser).get();

        if (parser.eof()) {
            throw std::runtime_error("Bad HTTP response");
        }

        // Read HTTP response header first
        auto rsp = parser.get_parsed_response();
        if (rsp->_status != http::reply::status_type::ok) {
            throw std::runtime_error(format("Error: HTTP response status {}", rsp->_status));
        }

        auto it = rsp->_headers.find("Content-Length");
        if (it == rsp->_headers.end()) {
            throw std::runtime_error("Error: HTTP response does not contain: Content-Length\n");
        }

        auto content_len = std::stoi(it->second);

        // Read HTTP response body
        temporary_buffer<char> buf = in.read_exactly(content_len).get();

        return sstring(buf.get(), buf.size());
    });
}

future<sstring> azure_snitch::read_property_file() {
    return load_property_file().then([this] {
        sstring dc_suffix;

        if (_prop_values.contains(dc_suffix_property_key)) {
            dc_suffix = _prop_values[dc_suffix_property_key];
        }

        return dc_suffix;
    });
}

using registry_default = class_registrator<i_endpoint_snitch, azure_snitch, const snitch_config&>;
static registry_default registrator_default("org.apache.cassandra.locator.AzureSnitch");
static registry_default registrator_default_short_name("AzureSnitch");

} // namespace locator
