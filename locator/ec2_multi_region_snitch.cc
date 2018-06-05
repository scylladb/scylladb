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

#include "locator/ec2_multi_region_snitch.hh"
#include "locator/reconnectable_snitch_helper.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"

namespace locator {
ec2_multi_region_snitch::ec2_multi_region_snitch(const sstring& fname, unsigned io_cpu_id)
    : ec2_snitch(fname, io_cpu_id) {}

future<> ec2_multi_region_snitch::start() {
    _state = snitch_state::initializing;

    return ec2_snitch::load_config().then([this] {
        if (engine().cpu_id() == io_cpu_id()) {
            return aws_api_call(AWS_QUERY_SERVER_ADDR, PUBLIC_IP_QUERY_REQ).then([this](sstring pub_addr){
                inet_address local_public_address = inet_address(pub_addr);
                logger().info("Ec2MultiRegionSnitch using publicIP as identifier: {}", local_public_address);

                //
                // Use the Public IP to broadcast Address to other nodes.
                //
                // Cassandra 2.1 manual explicitly instructs to set broadcast_address
                // value to a public address in cassandra.yaml.
                //
                utils::fb_utilities::set_broadcast_address(local_public_address);
                utils::fb_utilities::set_broadcast_rpc_address(local_public_address);

                return aws_api_call(AWS_QUERY_SERVER_ADDR, PRIVATE_IP_QUERY_REQ).then(
                        [this] (sstring priv_addr) {
                    _local_private_address = priv_addr;
                });
            }).then([this] {
                //
                // Gossiper main instance is currently running on CPU0 -
                // therefore we need to make sure the _local_private_address is
                // set on the shard0 so that it may be used when Gossiper is
                // going to invoke gossiper_starting() method.
                //
                return _my_distributed->invoke_on(0,
                    [this] (snitch_ptr& local_s) {

                    if (engine().cpu_id() != io_cpu_id()) {
                        local_s->set_local_private_addr(_local_private_address);
                    }
                });
            }).then([this] {
                set_snitch_ready();
            });
        }

        set_snitch_ready();
        return make_ready_future<>();
    });
}

void ec2_multi_region_snitch::set_local_private_addr(const sstring& addr_str) {
    _local_private_address = addr_str;
}

future<> ec2_multi_region_snitch::gossiper_starting() {
    //
    // Note: currently gossiper "main" instance always runs on CPU0 therefore
    // this function will be executed on CPU0 only.
    //

    using namespace gms;
    auto& g = get_local_gossiper();
    auto& ss = service::get_local_storage_service();

    return g.add_local_application_state(application_state::INTERNAL_IP,
        ss.value_factory.internal_ip(_local_private_address)).then([this] {
        if (!_gossip_started) {
            gms::get_local_gossiper().register_(make_shared<reconnectable_snitch_helper>(_my_dc));
            _gossip_started = true;
        }
    });

}

using registry_2_params = class_registrator<i_endpoint_snitch, ec2_multi_region_snitch, const sstring&, unsigned>;
static registry_2_params registrator2("org.apache.cassandra.locator.Ec2MultiRegionSnitch");
static registry_2_params registrator2_short_name("Ec2MultiRegionSnitch");


using registry_1_param = class_registrator<i_endpoint_snitch, ec2_multi_region_snitch, const sstring&>;
static registry_1_param registrator1("org.apache.cassandra.locator.Ec2MultiRegionSnitch");
static registry_1_param registrator1_short_name("Ec2MultiRegionSnitch");

using registry_default = class_registrator<i_endpoint_snitch, ec2_multi_region_snitch>;
static registry_default registrator_default("org.apache.cassandra.locator.Ec2MultiRegionSnitch");
static registry_default registrator_default_short_name("Ec2MultiRegionSnitch");

} // namespace locator
