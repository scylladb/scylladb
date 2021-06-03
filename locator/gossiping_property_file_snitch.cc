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

#include "locator/gossiping_property_file_snitch.hh"
#include "gms/versioned_value.hh"
#include "message/msg_addr.hh"
#include "message/messaging_service.hh"
#include "gms/gossiper.hh"

namespace locator {
future<bool> gossiping_property_file_snitch::property_file_was_modified() {
    return open_file_dma(_prop_file_name, open_flags::ro)
    .then([this](file f) {
        return do_with(std::move(f), [] (file& f) {
            return f.stat();
        });
    }).then_wrapped([this] (auto&& f) {
        try {
            auto st = f.get0();

            if (!_last_file_mod ||
                _last_file_mod->tv_sec != st.st_mtim.tv_sec) {
                _last_file_mod = st.st_mtim;
                return true;
            } else {
                return false;
            }
        } catch (...) {
            logger().error("Failed to open {} for read or to get stats", _prop_file_name);
            throw;
        }
    });
}

gossiping_property_file_snitch::gossiping_property_file_snitch(
    const sstring& fname, unsigned io_cpuid)
: production_snitch_base(fname), _file_reader_cpu_id(io_cpuid) {
    if (this_shard_id() == _file_reader_cpu_id) {
        io_cpu_id() = _file_reader_cpu_id;
    }
}

future<> gossiping_property_file_snitch::start() {
    using namespace std::chrono_literals;

    _state = snitch_state::initializing;

    reset_io_state();

    // Run a timer only on specific CPU
    if (this_shard_id() == _file_reader_cpu_id) {
        //
        // Here we will create a timer that will read the properties file every
        // minute and load its contents into the gossiper.endpoint_state_map
        //
        _file_reader.set_callback([this] {
            periodic_reader_callback();
        });

        return read_property_file().then([this] {
            start_io();
            set_snitch_ready();
            return make_ready_future<>();
        });
    }

    set_snitch_ready();
    return make_ready_future<>();
}

void gossiping_property_file_snitch::periodic_reader_callback() {
    _file_reader_runs = true;
    //FIXME: discarded future.
    (void)property_file_was_modified().then([this] (bool was_modified) {

        if (was_modified) {
            return read_property_file();
        }

        return make_ready_future<>();
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
        } catch (...) {
            logger().error("Exception has been thrown when parsing the property file.");
        }

        if (_state == snitch_state::stopping || _state == snitch_state::io_pausing) {
            this->set_stopped();
        } else if (_state != snitch_state::stopped) {
            _file_reader.arm(reload_property_file_period());
        }

        _file_reader_runs = false;
    });
}

future<> gossiping_property_file_snitch::gossiper_starting() {
    using namespace gms;
    using namespace service;
    //
    // Note: currently gossiper "main" instance always runs on CPU0 therefore
    // this function will be executed on CPU0 only.
    //
    auto& g = get_local_gossiper();

    auto local_internal_addr = g.get_local_messaging().listen_address();
    std::ostringstream ostrm;

    ostrm<<local_internal_addr<<std::flush;

    return gossip_snitch_info({
        { application_state::INTERNAL_IP, versioned_value::internal_ip(ostrm.str()) },
    }).then([this] {
        _gossip_started = true;
        return reload_gossiper_state();
    });
}

future<> gossiping_property_file_snitch::read_property_file() {
    using namespace exceptions;

    return load_property_file().then([this] {
        return reload_configuration();
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            return make_ready_future<>();
        } catch (...) {
            //
            // In case of an error:
            //    - Halt if in the constructor.
            //    - Print an error when reloading.
            //
            if (_state == snitch_state::initializing) {
                logger().error("Failed to parse a properties file ({}). Halting...", _prop_file_name);
                throw;
            } else {
                logger().warn("Failed to reload a properties file ({}). Using previous values.", _prop_file_name);
                return make_ready_future<>();
            }
        }
    });
}

future<> gossiping_property_file_snitch::reload_configuration() {
    // "prefer_local" is FALSE by default
    bool new_prefer_local = false;
    sstring new_dc;
    sstring new_rack;

    // Rack and Data Center have to be defined in the properties file!
    if (!_prop_values.contains(dc_property_key) || !_prop_values.contains(rack_property_key)) {
        throw_incomplete_file();
    }

    new_dc   = _prop_values[dc_property_key];
    new_rack = _prop_values[rack_property_key];

    if (_prop_values.contains(prefer_local_property_key)) {
        if (_prop_values[prefer_local_property_key] == "false") {
            new_prefer_local = false;
        } else if (_prop_values[prefer_local_property_key] == "true") {
            new_prefer_local = true;
        } else {
            throw_bad_format("prefer_local configuration is malformed");
        }
    }

    if (_state == snitch_state::initializing || _my_dc != new_dc ||
        _my_rack != new_rack || _prefer_local != new_prefer_local) {

        _my_dc = new_dc;
        _my_rack = new_rack;
        _prefer_local = new_prefer_local;

        assert(_my_distributed);

        return _my_distributed->invoke_on_all(
            [this] (snitch_ptr& local_s) {

            // Distribute the new values on all CPUs but the current one
            if (this_shard_id() != _file_reader_cpu_id) {
                local_s->set_my_dc(_my_dc);
                local_s->set_my_rack(_my_rack);
                local_s->set_prefer_local(_prefer_local);
            }
        }).then([this] {
            return seastar::async([this] {
                // reload Gossiper state (executed on CPU0 only)
                smp::submit_to(0, [] {
                    auto& local_snitch_ptr = get_local_snitch_ptr();
                    return local_snitch_ptr->reload_gossiper_state();
                }).get();

                _reconfigured();

                // spread the word...
                smp::submit_to(0, [] {
                    auto& local_snitch_ptr = get_local_snitch_ptr();
                    if (local_snitch_ptr->local_gossiper_started()) {
                        return local_snitch_ptr->gossip_snitch_info({});
                    }

                    return make_ready_future<>();
                }).get();
            });
        });
    }

    return make_ready_future<>();
}

void gossiping_property_file_snitch::set_stopped() {
    if (_state == snitch_state::stopping) {
        _state = snitch_state::stopped;
    } else {
        _state = snitch_state::io_paused;
    }

    _io_is_stopped.set_value();
}

future<> gossiping_property_file_snitch::stop_io() {
    if (this_shard_id() == _file_reader_cpu_id) {
        _file_reader.cancel();

        // If timer is not running then set the STOPPED state right away.
        if (!_file_reader_runs) {
            set_stopped();
        }
    } else {
        set_stopped();
    }

    return _io_is_stopped.get_future();
}

void gossiping_property_file_snitch::resume_io() {
    reset_io_state();
    start_io();
    set_snitch_ready();
}

void gossiping_property_file_snitch::start_io() {
    // Run a timer only on specific CPU
    if (this_shard_id() == _file_reader_cpu_id) {
        _file_reader.arm(reload_property_file_period());
    }
}

future<> gossiping_property_file_snitch::stop() {
    if (_state == snitch_state::stopped || _state == snitch_state::io_paused) {
        return make_ready_future<>();
    }

    _state = snitch_state::stopping;

    return stop_io();
}

future<> gossiping_property_file_snitch::pause_io() {
    if (_state == snitch_state::stopped || _state == snitch_state::io_paused) {
        return make_ready_future<>();
    }

    _state = snitch_state::io_pausing;

    return stop_io();
}

// should be invoked of CPU0 only
future<> gossiping_property_file_snitch::reload_gossiper_state() {
    if (!_gossip_started) {
        return make_ready_future<>();
    }

    future<> ret = make_ready_future<>();
    if (_reconnectable_helper) {
        ret = gms::get_local_gossiper().unregister_(_reconnectable_helper);
    }

    if (!_prefer_local) {
        return ret;
    }

    return ret.then([this] {
        _reconnectable_helper = ::make_shared<reconnectable_snitch_helper>(_my_dc);
        gms::get_local_gossiper().register_(_reconnectable_helper);
    });
}

using registry_2_params = class_registrator<i_endpoint_snitch,
                                   gossiping_property_file_snitch,
                                   const sstring&, unsigned>;
static registry_2_params registrator2("org.apache.cassandra.locator.GossipingPropertyFileSnitch");

using registry_1_param = class_registrator<i_endpoint_snitch,
                                   gossiping_property_file_snitch,
                                   const sstring&>;
static registry_1_param registrator1("org.apache.cassandra.locator.GossipingPropertyFileSnitch");

using registry_default = class_registrator<i_endpoint_snitch,
                                           gossiping_property_file_snitch>;
static registry_default registrator_default("org.apache.cassandra.locator.GossipingPropertyFileSnitch");
static registry_default registrator_default_short_name("GossipingPropertyFileSnitch");
} // namespace locator
