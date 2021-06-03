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
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */
#pragma once

#include "locator/production_snitch_base.hh"
#include <seastar/http/response_parser.hh>

namespace locator {
class ec2_snitch : public production_snitch_base {
public:
    static constexpr const char* ZONE_NAME_QUERY_REQ = "/latest/meta-data/placement/availability-zone";
    static constexpr const char* AWS_QUERY_SERVER_ADDR = "169.254.169.254";
    static constexpr uint16_t AWS_QUERY_SERVER_PORT = 80;

    ec2_snitch(const sstring& fname = "", unsigned io_cpu_id = 0);
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
