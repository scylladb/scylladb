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
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include "locator/ec2_snitch.hh"

namespace locator {
class ec2_multi_region_snitch : public ec2_snitch {
public:
    ec2_multi_region_snitch(const sstring& fname = "", unsigned io_cpu_id = 0);
    virtual future<> gossiper_starting() override;
    virtual future<> start() override;
    virtual void set_local_private_addr(const sstring& addr_str) override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.Ec2MultiRegionSnitch";
    }
private:
    static constexpr const char* PUBLIC_IP_QUERY_REQ  = "/latest/meta-data/public-ipv4";
    static constexpr const char* PRIVATE_IP_QUERY_REQ = "/latest/meta-data/local-ipv4";
    sstring _local_private_address;
};
} // namespace locator
