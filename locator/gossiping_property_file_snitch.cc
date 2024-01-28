/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "locator/gossiping_property_file_snitch.hh"

#include <seastar/core/seastar.hh>
#include "gms/versioned_value.hh"
#include "gms/gossiper.hh"
#include "utils/class_registrator.hh"

namespace locator {
future<bool> gossiping_property_file_snitch::property_file_was_modified() {
    return open_file_dma(_prop_file_name, open_flags::ro)
    .then([](file f) {
        return do_with(std::move(f), [] (file& f) {
            return f.stat();
        });
    }).then_wrapped([this] (auto&& f) {
        try {
            auto st = f.get();

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

gossiping_property_file_snitch::gossiping_property_file_snitch(const snitch_config& cfg)
        : production_snitch_base(cfg)
        , _file_reader_cpu_id(cfg.io_cpu_id)
        , _listen_address(cfg.listen_address) {
    if (this_shard_id() == _file_reader_cpu_id) {
        io_cpu_id() = _file_reader_cpu_id;
    }
    if (_listen_address->addr().is_addr_any()) {
        logger().warn("Not gossiping INADDR_ANY as internal IP");
        _listen_address.reset();
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

gms::application_state_map gossiping_property_file_snitch::get_app_states() const {
    gms::application_state_map ret = {
        {gms::application_state::DC, gms::versioned_value::datacenter(_my_dc)},
        {gms::application_state::RACK, gms::versioned_value::rack(_my_rack)},
    };
    if (_listen_address.has_value()) {
        sstring ip = format("{}", *_listen_address);
        ret.emplace(gms::application_state::INTERNAL_IP, gms::versioned_value::internal_ip(std::move(ip)));
    }
    return ret;
}

future<> gossiping_property_file_snitch::read_property_file() {
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

    if (_state == snitch_state::initializing || _my_dc != new_dc || _my_rack != new_rack || _prefer_local != new_prefer_local) {
        return container().invoke_on_all([new_dc, new_rack, new_prefer_local] (snitch_ptr& local_s) {
            local_s->set_my_dc_and_rack(new_dc, new_rack);
            local_s->set_prefer_local(new_prefer_local);
        }).then([this] {
            return seastar::async([this] {
                _reconfigured();
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

using registry_default = class_registrator<i_endpoint_snitch, gossiping_property_file_snitch, const snitch_config&>;
static registry_default registrator_default("org.apache.cassandra.locator.GossipingPropertyFileSnitch");
static registry_default registrator_default_short_name("GossipingPropertyFileSnitch");
} // namespace locator
