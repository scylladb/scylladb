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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "core/sstring.hh"
#include "gms/inet_address.hh"
#include "locator/abstract_endpoint_snitch.hh"

namespace locator {

using inet_address = gms::inet_address;

/**
 * An endpoint snitch tells Cassandra information about network topology that it can use to route
 * requests more efficiently.
 */
class abstract_network_topology_snitch : public abstract_endpoint_snitch {
public:
    /**
     * Return the rack for which an endpoint resides in
     * @param endpoint a specified endpoint
     * @return string of rack
     */
    // virtual sstring get_rack(inet_address endpoint) = 0;

    /**
     * Return the data center for which an endpoint resides in
     * @param endpoint a specified endpoint
     * @return string of data center
     */
    // virtual sstring get_datacenter(inet_address endpoint) = 0;

    virtual int compare_endpoints(inet_address& address, inet_address& a1, inet_address& a2) override {
        if (address == a1 && !(address == a2)) {
            return -1;
        }
        if (address == a2 && !(address == a1)) {
            return 1;
        }

        sstring address_datacenter = get_datacenter(address);
        sstring a1_datacenter = get_datacenter(a1);
        sstring a2_datacenter = get_datacenter(a2);
        if (address_datacenter == a1_datacenter && !(address_datacenter == a2_datacenter)) {
            return -1;
        }
        if (address_datacenter == a2_datacenter && !(address_datacenter == a1_datacenter)) {
            return 1;
        }

        sstring address_rack = get_rack(address);
        sstring a1_rack = get_rack(a1);
        sstring a2_rack = get_rack(a2);
        if (address_rack == a1_rack && !(address_rack == a2_rack)) {
            return -1;
        }
        if (address_rack == a2_rack && !(address_rack == a1_rack)) {
            return 1;
        }
        return 0;
    }
};

} // namespace locator
