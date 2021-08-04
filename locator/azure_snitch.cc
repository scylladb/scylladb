/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2021-present ScyllaDB
 */

#include "locator/azure_snitch.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/http/response_parser.hh>
#include <seastar/net/dns.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <fmt/format.h>

namespace locator {

const auto azure_snitch::REGION_NAME_QUERY_PATH = fmt::format(AZURE_QUERY_PATH_TEMPLATE, "location");
const auto azure_snitch::ZONE_NAME_QUERY_PATH = fmt::format(AZURE_QUERY_PATH_TEMPLATE, "zone");

azure_snitch::azure_snitch(const sstring& fname, unsigned io_cpuid) : production_snitch_base(fname) {
    if (this_shard_id() == io_cpuid) {
        io_cpu_id() = io_cpuid;
    }
}

future<> azure_snitch::load_config() {
    sstring region = co_await azure_api_call(REGION_NAME_QUERY_PATH);
    sstring azure_zone = co_await azure_api_call(ZONE_NAME_QUERY_PATH);

    sstring datacenter_suffix = co_await read_property_file();

    sstring azure_region = region + datacenter_suffix;

    logger().info("AzureSnitch using region: {}, zone: {}.", azure_region, azure_zone);

    _my_rack = azure_zone;
    _my_dc = azure_region;

    co_return co_await _my_distributed->invoke_on_all([this] (snitch_ptr& local_s) {
        // Distribute the new values on all CPUs but the current one
        if (this_shard_id() != io_cpu_id()) {
            local_s->set_my_dc(_my_dc);
            local_s->set_my_rack(_my_rack);
        }
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

        net::inet_address a = seastar::net::dns::resolve_name(AZURE_SERVER_ADDR, net::inet_address::family::INET).get0();
        connected_socket sd(connect(socket_address(a, 80)).get0());
        input_stream<char> in(sd.input());
        output_stream<char> out(sd.output());
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
        auto it = rsp->_headers.find("Content-Length");
        if (it == rsp->_headers.end()) {
            throw std::runtime_error("Error: HTTP response does not contain: Content-Length\n");
        }

        auto content_len = std::stoi(it->second);

        // Read HTTP response body
        temporary_buffer<char> buf = in.read_exactly(content_len).get0();

        sstring res(buf.get(), buf.size());

        // Close streams
        out.close().get();
        in.close().get();

        return res;
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

using registry_2_params = class_registrator<i_endpoint_snitch, azure_snitch, const sstring&, const unsigned&>;
static registry_2_params registrator2("org.apache.cassandra.locator.AzureSnitch");
static registry_2_params registrator2_short_name("AzureSnitch");

using registry_1_param = class_registrator<i_endpoint_snitch, azure_snitch, const sstring&>;
static registry_1_param registrator1("org.apache.cassandra.locator.AzureSnitch");
static registry_1_param registrator1_short_name("AzureSnitch");

using registry_default = class_registrator<i_endpoint_snitch, azure_snitch>;
static registry_default registrator_default("org.apache.cassandra.locator.AzureSnitch");
static registry_default registrator_default_short_name("AzureSnitch");

} // namespace locator
