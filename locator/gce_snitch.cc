/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2018-present ScyllaDB
 */

#include <seastar/net/api.hh>
#include <seastar/net/dns.hh>
#include <seastar/core/seastar.hh>
#include "locator/gce_snitch.hh"
#include <seastar/http/reply.hh>
#include <seastar/util/closeable.hh>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "utils/class_registrator.hh"

namespace locator {

gce_snitch::gce_snitch(const snitch_config& cfg) : production_snitch_base(cfg) {
    if (this_shard_id() == cfg.io_cpu_id) {
        io_cpu_id() = cfg.io_cpu_id;
        _meta_server_url = cfg.gce_meta_server_url;
    }
}

/**
 * Read GCE and property file configuration and distribute it among other shards
 *
 * @return
 */
future<> gce_snitch::load_config() {
    using namespace boost::algorithm;

    if (this_shard_id() == io_cpu_id()) {
        sstring meta_server_url(GCE_QUERY_SERVER_ADDR);
        if (!_meta_server_url.empty()) {
            meta_server_url = _meta_server_url;
        }

        return gce_api_call(meta_server_url, ZONE_NAME_QUERY_REQ).then([this, meta_server_url] (sstring az) {
            if (az.empty()) {
                return make_exception_future(std::runtime_error(format("Got an empty zone name from the GCE meta server {}", meta_server_url)));
            }

            std::vector<std::string> splits;

            // Split "us-central1-a" or "asia-east1-a" into "us-central1"/"a" and "asia-east1"/"a".
            split(splits, az, is_any_of("-"));
            if (splits.size() <= 1) {
                return make_exception_future(std::runtime_error(format("Bad GCE zone format: {}", az)));
            }

            sstring my_rack = splits[splits.size() - 1];
            sstring my_dc = az.substr(0, az.size() - 1 - my_rack.size());

            return read_property_file().then([this, my_dc, my_rack] (sstring datacenter_suffix) mutable {
                my_dc += datacenter_suffix;
                logger().info("GCESnitch using region: {}, zone: {}.", my_dc, my_rack);
                return container().invoke_on_all([my_dc, my_rack] (snitch_ptr& local_s) {
                    local_s->set_my_dc_and_rack(my_dc, my_rack);
                });
            });
        });
    }

    return make_ready_future<>();
}

future<> gce_snitch::start() {
    _state = snitch_state::initializing;

    return load_config().then([this] {
        set_snitch_ready();
    });
}

future<sstring> gce_snitch::gce_api_call(sstring addr, sstring cmd) {
    return seastar::async([addr = std::move(addr), cmd = std::move(cmd)] () -> sstring {
        using namespace boost::algorithm;

        net::inet_address a = seastar::net::dns::resolve_name(addr, net::inet_address::family::INET).get();
        connected_socket sd(connect(socket_address(a, 80)).get());
        input_stream<char> in(sd.input());
        output_stream<char> out(sd.output());
        auto close_in = deferred_close(in);
        auto close_out = deferred_close(out);
        sstring zone_req(seastar::format("GET {} HTTP/1.1\r\nHost: metadata\r\nMetadata-Flavor: Google\r\n\r\n", cmd));

        out.write(zone_req).get();
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

        sstring res(buf.get(), buf.size());
        std::vector<std::string> splits;
        split(splits, res, is_any_of("/"));

        return sstring(splits[splits.size() - 1]);
    });
}

future<sstring> gce_snitch::read_property_file() {
    return load_property_file().then([this] {
        sstring dc_suffix;

        if (_prop_values.contains(dc_suffix_property_key)) {
            dc_suffix = _prop_values[dc_suffix_property_key];
        }

        return dc_suffix;
    });
}

using registry_default = class_registrator<i_endpoint_snitch, gce_snitch, const snitch_config&>;
static registry_default registrator_default("org.apache.cassandra.locator.GoogleCloudSnitch");
static registry_default registrator_default_short_name("GoogleCloudSnitch");

} // namespace locator
