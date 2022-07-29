/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */
#pragma once

#include "locator/production_snitch_base.hh"
#include <seastar/http/response_parser.hh>
#include <seastar/net/api.hh>

namespace locator {
class ec2_snitch : public production_snitch_base {
public:
    static constexpr const char* ZONE_NAME_QUERY_REQ = "/latest/meta-data/placement/availability-zone";
    static constexpr const char* AWS_QUERY_SERVER_ADDR = "169.254.169.254";
    static constexpr uint16_t AWS_QUERY_SERVER_PORT = 80;

    ec2_snitch(const snitch_config&, gms::gossiper&);
    virtual future<> start() override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.Ec2Snitch";
    }
protected:
    future<> load_config();
    future<sstring> aws_api_call(sstring addr, uint16_t port, const sstring cmd);
    future<sstring> read_property_file();
private:
    connected_socket _sd;
    input_stream<char> _in;
    output_stream<char> _out;
    http_response_parser _parser;
    sstring _zone_req;
};
} // namespace locator
