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

#include "locator/gossiping_property_file_snitch.hh"

namespace locator {
future<bool> gossiping_property_file_snitch::property_file_was_modified() {
    return engine().open_file_dma(_fname, open_flags::ro)
    .then([this](file f) {
        _sf = make_lw_shared(std::move(f));

        return _sf->stat();
    }).then_wrapped([this] (auto&& f) {
        try {
            auto st = std::get<0>(f.get());

            if (!_last_file_mod ||
                _last_file_mod->tv_sec != st.st_mtim.tv_sec) {
                _last_file_mod = st.st_mtim;
                return true;
            } else {
                return false;
            }
        } catch (...) {
            this->err("Failed to open {} for read or to get stats",
                      _fname);
            throw;
        }
    });
}

gossiping_property_file_snitch::gossiping_property_file_snitch(
    const sstring& fname, unsigned io_cpu_id)
: _fname(fname), _file_reader_cpu_id(io_cpu_id) {

    _state = snitch_state::initializing;
}

future<> gossiping_property_file_snitch::start() {
    using namespace std::chrono_literals;

    // Run a timer only on specific CPU
    if (engine().cpu_id() == _file_reader_cpu_id) {
        //
        // Here we will create a timer that will read the properties file every
        // minute and load its contents into the gossiper.endpoint_state_map
        //
        _file_reader.set_callback([this] {
            periodic_reader_callback();
        });

        io_cpu_id() = _file_reader_cpu_id;

        return read_property_file().then([this] {
            _file_reader.arm(reload_property_file_period());
            set_snitch_ready();
            return make_ready_future<>();
        });
    }

    set_snitch_ready();
    return make_ready_future<>();
}

void gossiping_property_file_snitch::periodic_reader_callback() {
    _file_reader_runs = true;
    property_file_was_modified().then([this] (bool was_modified) {

        if (was_modified) {
            return read_property_file();
        }

        return make_ready_future<>();
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
        } catch (...) {
            this->err("Exception has been thrown when parsing the property "
                      "file.");
        }

        if (_state == snitch_state::stopping) {
            this->set_stopped();
        } else if (_state != snitch_state::stopped) {
            _file_reader.arm(reload_property_file_period());
        }

        _file_reader_runs = false;
    });
}

void gossiping_property_file_snitch::gossiper_starting() {
    using namespace gms;
    using namespace service;

    get_gossiper().invoke_on(0, [&] (gossiper& local_gossiper) {
#if 0   // Uncomment when versioned_vlaue_factory class gets more code (e.g. constructor)
        auto internal_addr = storage_service_instance.value_factory.internal_ip(fb_utilities::get_local_address());

            local_gossiper.add_local_application_state(application_state.INTERNAL_IP, internal_addr);
#endif
        }).then([&] {
        reload_gossiper_state();
        _gossip_started = true;
    });
}

future<> gossiping_property_file_snitch::read_property_file() {
    using namespace exceptions;

    return engine().open_file_dma(_fname, open_flags::ro).then([this](file f) {
        _sf = make_lw_shared(std::move(f));

        return _sf->size();
    }).then([this](size_t s) {
        _fsize = s;

        return _sf->dma_read_exactly<char>(0, _fsize);
    }).then([this](temporary_buffer<char> tb) {

        _srting_buf = std::move(std::string(tb.get(), _fsize));

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
                this->err("Failed to parse a properties file ({}). "
                          "Halting...", _fname);
                throw;
            } else {
                this->warn("Failed to reload a properties file ({}). "
                           "Using previous values.", _fname);
                return make_ready_future<>();
            }
        }
    });
}

future<> gossiping_property_file_snitch::reload_configuration() {
    using namespace boost::algorithm;

    std::string line;
    //
    // Using two bool variables instead of std::experimental::optional<bool>
    // since there is a bug in gcc causing it to report "'new_prefer_local' may
    // be used uninitialized in this function" if we do.
    //
    bool new_prefer_local;
    bool read_prefer_local = false;
    std::experimental::optional<sstring> new_dc;
    std::experimental::optional<sstring> new_rack;
    std::istringstream istrm(_srting_buf);
    std::vector<std::string> split_line;

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

        if (!val.size()) {
            throw_bad_format(line);
        }

        if (!key.compare("dc")) {
            if (new_dc) {
                throw_double_declaration("dc");
            }

            new_dc = sstring(val);
        } else if (!key.compare("rack")) {
            if (new_rack) {
                throw_double_declaration("rack");
            }

            new_rack = sstring(val);
        } else if (!key.compare("prefer_local")) {
            if (read_prefer_local) {
                throw_double_declaration("prefer_local");
            }

            if (!val.compare("false")) {
                new_prefer_local = false;
            } else if (!val.compare("true")) {
                new_prefer_local = true;
            } else {
                throw_bad_format(line);
            }

            read_prefer_local = true;
        } else {
            throw_bad_format(line);
        }
    }

    // Rack and Data Center have to be defined in the properties file!
    if (!new_dc || !new_rack) {
        throw_incomplete_file();
    }

    // "prefer_local" is FALSE by default
    if (!read_prefer_local) {
        new_prefer_local = false;
    }

    if (_state == snitch_state::initializing || _my_dc != *new_dc ||
        _my_rack != *new_rack || _prefer_local != new_prefer_local) {

        _my_dc = *new_dc;
        _my_rack = *new_rack;
        _prefer_local = new_prefer_local;

        assert(_my_distributed);

        return _my_distributed->invoke_on_all(
            [this] (snitch_ptr& local_s) {

            // Distribute the new values on all CPUs but the current one
            if (engine().cpu_id() != _file_reader_cpu_id) {
                local_s->set_my_dc(_my_dc);
                local_s->set_my_rack(_my_rack);
            }
        }).then([this] {
            reload_gossiper_state();

            return service::get_storage_service().invoke_on_all(
                    [] (service::storage_service& l) {
                l.get_token_metadata().invalidate_cached_rings();
            }).then([this] {
                if (_gossip_started) {
                    service::get_local_storage_service().gossip_snitch_info();
                }
            });
        });
    }

    return make_ready_future<>();
}

void gossiping_property_file_snitch::set_stopped() {
    _state = snitch_state::stopped;
    _snitch_is_stopped.set_value();
}

future<> gossiping_property_file_snitch::stop() {
    if (_state == snitch_state::stopped) {
        return make_ready_future<>();
    }

    _state = snitch_state::stopping;

    if (engine().cpu_id() == _file_reader_cpu_id) {
        _file_reader.cancel();

        // If timer is not running then set the STOPPED state right away.
        if (!_file_reader_runs) {
            set_stopped();
        }
    } else {
        set_stopped();
    }

    return _snitch_is_stopped.get_future();
}

void gossiping_property_file_snitch::reload_gossiper_state()
{
    #if 0 // TODO - needed to EC2 only
    ReconnectableSnitchHelper pendingHelper = new ReconnectableSnitchHelper(this, myDC, preferLocal);
    Gossiper.instance.register(pendingHelper);

    pendingHelper = snitchHelperReference.getAndSet(pendingHelper);
    if (pendingHelper != null)
        Gossiper.instance.unregister(pendingHelper);
    #endif
    // else this will eventually rerun at gossiperStarting()
}

namespace locator {
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
}
} // namespace locator
