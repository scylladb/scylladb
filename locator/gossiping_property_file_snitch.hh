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

#include <sstream>
#include <string>
#include <chrono>
#include <boost/algorithm/string.hpp>
#include <experimental/optional>
#include "production_snitch_base.hh"
#include "exceptions/exceptions.hh"
#include "service/storage_service.hh"
#include "core/file.hh"
#include "log.hh"

namespace locator {

class bad_property_file_error : public std::exception {};

/**
 * cassandra-rackdc.properties file has the following format:
 *
 * dc=<Data Center name>
 * rack=<Rack name>
 * prefer_local=<true|false>
 */
class gossiping_property_file_snitch : public production_snitch_base {
public:
    static constexpr const char* snitch_properties_filename = "cassandra-rackdc.properties";
    // Check the property file for changes every 60s.
    static constexpr timer<>::duration reload_property_file_period() {
        return std::chrono::seconds(60);
    }

    virtual void gossiper_starting() override;
    virtual future<> stop() override;
    virtual future<> start() override;
    virtual future<> pause_io() override;
    virtual void resume_io() override;

    gossiping_property_file_snitch(
        const sstring& fname = snitch_properties_filename,
        unsigned io_cpu_id = 0);

    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.GossipingPropertyFileSnitch";
    }

private:
    static logging::logger& logger() {
        return i_endpoint_snitch::snitch_logger;
    }

    template <typename... Args>
    void err(const char* fmt, Args&&... args) const {
        logger().error(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    void warn(const char* fmt, Args&&... args) const {
        logger().warn(fmt, std::forward<Args>(args)...);
    }

    void throw_double_declaration(const sstring& key) const {
        err("double \"{}\" declaration in {}", key, _fname);
        throw bad_property_file_error();
    }

    void throw_bad_format(const sstring& line) const {
        err("Bad format in properties file {}: {}", _fname, line);
        throw bad_property_file_error();
    }

    void throw_incomplete_file() const {
        err("Property file {} is incomplete. Both \"dc\" and \"rack\" "
            "labels have to be defined.", _fname);
        throw bad_property_file_error();
    }

    void periodic_reader_callback();

    /**
     * Parse the property file and indicate the StorageService and a Gossiper if
     * there was a configuration change.
     *
     * @return a ready-future when we are done
     */
    future<> reload_configuration();

    /**
     * Check if the property file has been modified since the last time we
     * parsed it.
     *
     * @return TRUE if property file has been modified
     */
    future<bool> property_file_was_modified();

    /**
     * Read the propery file if it has changed since the last time we read it.
     */
    future<> read_property_file();

    /**
     * TODO: this function is expected to trigger a Gossiper to reconnect
     * according to the new "prefer_local" value, namely use either an internal
     * or extenal IP address.
     *
     * This is currently relevant to EC2/GCE(?) only.
     */
    void reload_gossiper_state();

    /**
     * Indicate that the snitch has stopped its I/O.
     */
    void set_stopped();

    future<> stop_io();
    void start_io();

private:
    sstring _fname;
    timer<> _file_reader;
    lw_shared_ptr<file> _sf;
    std::experimental::optional<timespec> _last_file_mod;
    size_t _fsize;
    std::string _srting_buf;
    std::istringstream _istrm;
    bool _gossip_started = false;
    bool _prefer_local = false;
    bool _file_reader_runs = false;
    unsigned _file_reader_cpu_id;
};
} // namespace locator
