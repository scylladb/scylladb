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
#include "snitch_base.hh"
#include "utils/fb_utilities.hh"
#include <memory>

namespace locator {

/**
 * A simple endpoint snitch implementation that treats Strategy order as
 * proximity, allowing non-read-repaired reads to prefer a single endpoint,
 * which improves cache locality.
 */
struct simple_snitch : public snitch_base {
    simple_snitch() {
        _my_dc = get_datacenter(utils::fb_utilities::get_broadcast_address());
        _my_rack = get_rack(utils::fb_utilities::get_broadcast_address());

        // This snitch is ready on creation
        set_snitch_ready();
    }

    virtual sstring get_rack(inet_address endpoint) override {
        return "rack1";
    }

    virtual sstring get_datacenter(inet_address endpoint) override {
        return "datacenter1";
    }

    virtual void sort_by_proximity(
        inet_address address, inet_address_vector_replica_set& addresses) override {
        // Optimization to avoid walking the list
    }

    virtual int compare_endpoints(inet_address& target, inet_address& a1,
                                  inet_address& a2) override {
        //
        // "Making all endpoints equal ensures we won't change the original
        // ordering." - quote from C* code.
        //
        // Effectively this would return 0 even in the following case:
        //
        // compare_endpoints(NodeA, NodeA, NodeB) // -1 should be returned
        //
        // The snitch_base implementation would handle the above case correctly.
        //
        // I'm leaving the this implementation anyway since it's the C*'s
        // implementation and some installations may depend on it.
        //
        return 0;
    }

    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.SimpleSnitch";
    }
};

}
