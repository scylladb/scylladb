/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <sstream>
#include <chrono>
#include <optional>
#include "production_snitch_base.hh"
#include <seastar/core/file.hh>

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

    virtual gms::application_state_map get_app_states() const override;
    virtual future<> stop() override;
    virtual future<> start() override;
    virtual future<> pause_io() override;
    virtual void resume_io() override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.GossipingPropertyFileSnitch";
    }

    gossiping_property_file_snitch(const snitch_config&);

    virtual snitch_signal_connection_t when_reconfigured(snitch_signal_slot_t& slot) override {
        return _reconfigured.connect([slot = std::move(slot)] () mutable {
            // the signal is fired inside thread context
            slot().get();
        });
    }

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
     * Read the property file if it has changed since the last time we read it.
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
    snitch_signal_t _reconfigured;
    promise<> _io_is_stopped;
    std::optional<gms::inet_address> _listen_address;

    void reset_io_state() {
        // Reset the promise to allow repeating
        // start()+stop()/pause_io()+resume_io() call sequences.
        _io_is_stopped = promise<>();
    }
};
} // namespace locator
