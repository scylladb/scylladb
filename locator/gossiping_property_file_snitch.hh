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

#include <sstream>
#include <string>
#include <chrono>
#include <optional>
#include "production_snitch_base.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/file.hh>
#include "log.hh"
#include "locator/reconnectable_snitch_helper.hh"

namespace locator {

/**
 * cassandra-rackdc.properties file has the following format:
 *
 * dc=<Data Center name>
 * rack=<Rack name>
 * prefer_local=<true|false>
 */
class gossiping_property_file_snitch : public production_snitch_base {
public:
    // Check the property file for changes every 60s.
    static constexpr timer<lowres_clock>::duration reload_property_file_period() {
        return std::chrono::seconds(60);
    }

    virtual future<> gossiper_starting() override;
    virtual future<> stop() override;
    virtual future<> start() override;
    virtual future<> pause_io() override;
    virtual void resume_io() override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.GossipingPropertyFileSnitch";
    }

    gossiping_property_file_snitch(
        const sstring& fname = "",
        unsigned io_cpuid = 0);

    virtual snitch_signal_connection_t when_reconfigured(snitch_signal_slot_t& slot) override {
        return _reconfigured.connect([slot = std::move(slot)] () mutable {
            // the signal is fired inside thread context
            slot().get();
        });
    }

    /**
     * This function register a Gossiper subscriber to reconnect according to
     * the new "prefer_local" value, namely use either an internal or extenal IP
     * address.
     *
     * @note Currently in order to be backward compatible we are mimicking the C*
     *       behavior, which is a bit strange: while allowing the change of
     *       prefer_local value during the same run it won't actually trigger
     *       disconnect from all remote nodes as would be logical (in order to
     *       connect using a new configuration). On the contrary, if the new
     *       prefer_local value is TRUE, it will trigger the reconnect only when
     *       there is a corresponding gossip event (e.g. on_change()) from the
     *       corresponding node has been accepted. If the new value is FALSE
     *       then it won't trigger disconnect at all! And in any case a remote
     *       node will be reconnected using the PREFERED_IP value stored in the
     *       system_table.peer.
     *
     * This is currently relevant to EC2/GCE(?) only.
     */
    virtual future<> reload_gossiper_state() override;

private:
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
     * Indicate that the snitch has stopped its I/O.
     */
    void set_stopped();

    future<> stop_io();
    void start_io();

private:
    timer<lowres_clock> _file_reader;
    std::optional<timespec> _last_file_mod;
    std::istringstream _istrm;
    bool _file_reader_runs = false;
    unsigned _file_reader_cpu_id;
    shared_ptr<reconnectable_snitch_helper> _reconnectable_helper;
    snitch_signal_t _reconfigured;
};
} // namespace locator
