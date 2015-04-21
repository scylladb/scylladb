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

#pragma once
#include "abstract_endpoint_snitch.hh"

namespace locator {

/**
 * A simple endpoint snitch implementation that treats Strategy order as proximity,
 * allowing non-read-repaired reads to prefer a single endpoint, which improves
 * cache locality.
 */
struct simple_snitch : public abstract_endpoint_snitch
{
    sstring get_rack(inet_address endpoint) override
    {
        return "rack1";
    }

    sstring get_datacenter(inet_address endpoint) override
    {
        return "datacenter1";
    }

    void sort_by_proximity(inet_address address, std::vector<inet_address>& addresses) override
    {
        // Optimization to avoid walking the list
    }

    int compare_endpoints(inet_address& target, inet_address& a1, inet_address& a2) override
    {
        // Making all endpoints equal ensures we won't change the original ordering (since
        // Collections.sort is guaranteed to be stable)
        return 0;
    }
};

}
