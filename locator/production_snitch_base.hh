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

#include <unordered_map>
#include <utility>
#include <experimental/optional>

#include "gms/endpoint_state.hh"
#include "gms/gossiper.hh"
#include "utils/fb_utilities.hh"
#include "locator/token_metadata.hh"
#include "db/system_keyspace.hh"
#include "core/sstring.hh"
#include "snitch_base.hh"

namespace locator {

class production_snitch_base : public snitch_base {
public:
    // map of inet address to (datacenter, rack) pair
    typedef std::unordered_map<inet_address, endpoint_dc_rack> addr2dc_rack_map;

    static constexpr const char* default_dc   = "UNKNOWN_DC";
    static constexpr const char* default_rack = "UNKNOWN_RACK";

    virtual sstring get_rack(inet_address endpoint) {
        if (endpoint == utils::fb_utilities::get_broadcast_address()) {
            return _my_rack;
        }

        return get_endpoint_info(endpoint,
                                 gms::application_state::RACK,
                                 default_rack);
    }

    virtual sstring get_datacenter(inet_address endpoint) {
        if (endpoint == utils::fb_utilities::get_broadcast_address()) {
            return _my_dc;
        }

        return get_endpoint_info(endpoint,
                                 gms::application_state::DC,
                                 default_dc);
    }

    virtual void set_my_distributed(distributed<snitch_ptr>* d) override {
        _my_distributed = d;
    }

    void reset_io_state() {
        //
        // Reset the promise to allow repeating
        // start()+stop()/pause_io()+resume_io() call sequences.
        //
        _io_is_stopped = promise<>();
    }

private:
    sstring get_endpoint_info(inet_address endpoint, gms::application_state key,
                              const sstring& default_val) {
        gms::gossiper& local_gossiper = gms::get_local_gossiper();
        auto state = local_gossiper.get_endpoint_state_for_endpoint(endpoint);

        // First, look in the gossiper::endpoint_state_map...
        if (state) {
            auto ep_state = state->get_application_state(key);
            if (ep_state) {
                return ep_state->value;
            }
        }

        // ...if not found - look in the SystemTable...
        if (!_saved_endpoints) {
            _saved_endpoints = db::system_keyspace::load_dc_rack_info();
        }

        auto it = _saved_endpoints->find(endpoint);

        if (it != _saved_endpoints->end()) {
            if (key == gms::application_state::RACK) {
                return it->second.rack;
            } else { // gms::application_state::DC
                return it->second.dc;
            }
        }

        // ...if still not found - return a default value
        return default_val;
    }

    virtual void set_my_dc(const sstring& new_dc) override {
        _my_dc = new_dc;
    }

    virtual void set_my_rack(const sstring& new_rack) override {
        _my_rack = new_rack;
    }

protected:
    promise<> _io_is_stopped;
    std::experimental::optional<addr2dc_rack_map> _saved_endpoints;
    distributed<snitch_ptr>* _my_distributed = nullptr;
};
} // namespace locator
