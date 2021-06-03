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
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2018-present ScyllaDB
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
#include "locator/production_snitch_base.hh"
#include "reconnectable_snitch_helper.hh"
#include "db/system_keyspace.hh"
#include "gms/gossiper.hh"
#include "message/messaging_service.hh"
#include "utils/fb_utilities.hh"
#include "db/config.hh"

#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace locator {

production_snitch_base::production_snitch_base(const sstring& prop_file_name)
        : allowed_property_keys({ dc_property_key,
                          rack_property_key,
                          prefer_local_property_key,
                          dc_suffix_property_key }) {
    if (!prop_file_name.empty()) {
        _prop_file_name = prop_file_name;
    } else {
        _prop_file_name = db::config::get_conf_sub(snitch_properties_filename).string();
    }
}


sstring production_snitch_base::get_rack(inet_address endpoint) {
    if (endpoint == utils::fb_utilities::get_broadcast_address()) {
        return _my_rack;
    }

    return get_endpoint_info(endpoint,
                             gms::application_state::RACK,
                             default_rack);
}

sstring production_snitch_base::get_datacenter(inet_address endpoint) {
    if (endpoint == utils::fb_utilities::get_broadcast_address()) {
        return _my_dc;
    }

    return get_endpoint_info(endpoint,
                             gms::application_state::DC,
                             default_dc);
}

void production_snitch_base::set_my_distributed(distributed<snitch_ptr>* d) {
    _my_distributed = d;
}

void production_snitch_base::reset_io_state() {
    //
    // Reset the promise to allow repeating
    // start()+stop()/pause_io()+resume_io() call sequences.
    //
    _io_is_stopped = promise<>();
}

sstring production_snitch_base::get_endpoint_info(inet_address endpoint, gms::application_state key,
                                                  const sstring& default_val) {
    auto val = snitch_base::get_endpoint_info(endpoint, key);
    if (val) {
        return *val;
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

void production_snitch_base::set_my_dc(const sstring& new_dc) {
    _my_dc = new_dc;
}

void production_snitch_base::set_my_rack(const sstring& new_rack) {
    _my_rack = new_rack;
}

void production_snitch_base::set_prefer_local(bool prefer_local) {
    _prefer_local = prefer_local;
}

future<> production_snitch_base::load_property_file() {
    return open_file_dma(_prop_file_name, open_flags::ro)
    .then([this] (file f) {
        return do_with(std::move(f), [this] (file& f) {
            return f.size().then([this, &f] (size_t s) {
                _prop_file_size = s;

                return f.dma_read_exactly<char>(0, s);
            });
        }).then([this] (temporary_buffer<char> tb) {
            _prop_file_contents = std::move(std::string(tb.get(), _prop_file_size));
            parse_property_file();

            return make_ready_future<>();
        });
    });
}

void production_snitch_base::parse_property_file() {
    using namespace boost::algorithm;

    std::string line;
    std::istringstream istrm(_prop_file_contents);
    std::vector<std::string> split_line;
    _prop_values.clear();

    while (std::getline(istrm, line)) {
        trim(line);

        // Skip comments or empty lines
        if (!line.size() || line.at(0) == '#') {
            continue;
        }

        split_line.clear();
        split(split_line, line, is_any_of("="));

        if (split_line.size() != 2) {
            throw_bad_format(line);
        }

        auto key = split_line[0]; trim(key);
        auto val = split_line[1]; trim(val);

        if (val.empty() || !allowed_property_keys.contains(key)) {
            throw_bad_format(line);
        }

        if (_prop_values.contains(key)) {
            throw_double_declaration(key);
        }

        _prop_values[key] = val;
    }
}

[[noreturn]]
void production_snitch_base::throw_double_declaration(const sstring& key) const {
    logger().error("double \"{}\" declaration in {}", key, _prop_file_name);
    throw bad_property_file_error();
}

[[noreturn]]
void production_snitch_base::throw_bad_format(const sstring& line) const {
    logger().error("Bad format in properties file {}: {}", _prop_file_name, line);
    throw bad_property_file_error();
}

[[noreturn]]
void production_snitch_base::throw_incomplete_file() const {
    logger().error("Property file {} is incomplete. Some obligatory fields are missing.", _prop_file_name);
    throw bad_property_file_error();
}

logging::logger& reconnectable_snitch_helper::logger() {
    static logging::logger _logger("reconnectable_snitch_helper");
    return _logger;
}


void reconnectable_snitch_helper::reconnect(gms::inet_address public_address, const gms::versioned_value& local_address_value) {
    reconnect(public_address, gms::inet_address(local_address_value.value));
}

void reconnectable_snitch_helper::reconnect(gms::inet_address public_address, gms::inet_address local_address) {
    netw::messaging_service& ms = gms::get_local_gossiper().get_local_messaging();
    auto& sn_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();

    if (sn_ptr->get_datacenter(public_address) == _local_dc &&
        ms.get_preferred_ip(public_address) != local_address) {
        //
        // First, store the local address in the system_table...
        //
        db::system_keyspace::update_preferred_ip(public_address, local_address).get();

        //
        // ...then update messaging_service cache and reset the currently
        // open connections to this endpoint on all shards...
        //
        ms.container().invoke_on_all([public_address, local_address] (auto& local_ms) {
            local_ms.cache_preferred_ip(public_address, local_address);
            local_ms.remove_rpc_client(netw::msg_addr(public_address));
        }).get();

        logger().debug("Initiated reconnect to an Internal IP {} for the {}", local_address, public_address);
    }
}

reconnectable_snitch_helper::reconnectable_snitch_helper(sstring local_dc)
        : _local_dc(local_dc) {}

void reconnectable_snitch_helper::before_change(gms::inet_address endpoint, gms::endpoint_state cs, gms::application_state new_state_key, const gms::versioned_value& new_value) {
    // do nothing.
}

void reconnectable_snitch_helper::on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) {
    auto* internal_ip_state = ep_state.get_application_state_ptr(gms::application_state::INTERNAL_IP);
    if (internal_ip_state) {
        reconnect(endpoint, *internal_ip_state);
    }
}

void reconnectable_snitch_helper::on_change(gms::inet_address endpoint, gms::application_state state, const gms::versioned_value& value) {
    if (state == gms::application_state::INTERNAL_IP) {
        reconnect(endpoint, value);
    }
}

void reconnectable_snitch_helper::on_alive(gms::inet_address endpoint, gms::endpoint_state ep_state) {
    on_join(std::move(endpoint), std::move(ep_state));
}

void reconnectable_snitch_helper::on_dead(gms::inet_address endpoint, gms::endpoint_state ep_state) {
    // do nothing.
}

void reconnectable_snitch_helper::on_remove(gms::inet_address endpoint) {
    // do nothing.
}

void reconnectable_snitch_helper::on_restart(gms::inet_address endpoint, gms::endpoint_state state) {
    // do nothing.
}

} // namespace locator
