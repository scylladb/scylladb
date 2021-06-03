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

#include <unordered_map>
#include <utility>
#include <optional>
#include <unordered_set>

#include "gms/endpoint_state.hh"
#include "locator/token_metadata.hh"
#include <seastar/core/sstring.hh>
#include "snitch_base.hh"

namespace locator {

class bad_property_file_error : public std::exception {};

class production_snitch_base : public snitch_base {
public:
    // map of inet address to (datacenter, rack) pair
    typedef std::unordered_map<inet_address, endpoint_dc_rack> addr2dc_rack_map;

    static constexpr const char* default_dc   = "UNKNOWN_DC";
    static constexpr const char* default_rack = "UNKNOWN_RACK";
    static constexpr const char* snitch_properties_filename = "cassandra-rackdc.properties";

    // only these property values are supported
    static constexpr const char* dc_property_key           = "dc";
    static constexpr const char* rack_property_key         = "rack";
    static constexpr const char* prefer_local_property_key = "prefer_local";
    static constexpr const char* dc_suffix_property_key    = "dc_suffix";
    const std::unordered_set<sstring> allowed_property_keys;

    production_snitch_base(const sstring& prop_file_name = "");

    virtual sstring get_rack(inet_address endpoint);
    virtual sstring get_datacenter(inet_address endpoint);
    virtual void set_my_distributed(distributed<snitch_ptr>* d) override;

    void reset_io_state();

private:
    sstring get_endpoint_info(inet_address endpoint, gms::application_state key,
                              const sstring& default_val);
    virtual void set_my_dc(const sstring& new_dc) override;
    virtual void set_my_rack(const sstring& new_rack) override;
    virtual void set_prefer_local(bool prefer_local) override;
    void parse_property_file();

protected:
    /**
     * Loads the contents of the property file into the map
     *
     * @return ready future when the file contents has been loaded.
     */
    future<> load_property_file();

    [[noreturn]]
    void throw_double_declaration(const sstring& key) const;

    [[noreturn]]
    void throw_bad_format(const sstring& line) const;

    [[noreturn]]
    void throw_incomplete_file() const;

protected:
    promise<> _io_is_stopped;
    std::optional<addr2dc_rack_map> _saved_endpoints;
    distributed<snitch_ptr>* _my_distributed = nullptr;
    std::string _prop_file_contents;
    sstring _prop_file_name;
    std::unordered_map<sstring, sstring> _prop_values;

private:
    size_t _prop_file_size;
};
} // namespace locator
