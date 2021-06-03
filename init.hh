/*
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

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include "log.hh"
#include "seastarx.hh"

namespace db {
class extensions;
class seed_provider_type;
class config;
namespace view {
class view_update_generator;
}
}

namespace gms {
class feature_service;
class gossiper;
}

extern logging::logger startlog;

class bad_configuration_error : public std::exception {};

void init_gossiper(sharded<gms::gossiper>& gossiper
                , db::config& cfg
                , sstring listen_address
                , db::seed_provider_type seed_provider
                , sstring cluster_name = "Test Cluster");

/**
 * Very simplistic config registry. Allows hooking in a config object
 * to the "main" sequence.
 */
class configurable {
public:
    configurable() {
        // We auto register. Not that like cycle is assumed to be forever
        // and scope should be managed elsewhere.
        register_configurable(*this);
    }
    virtual ~configurable()
    {}
    // Hook to add command line options and/or add main config options
    virtual void append_options(db::config&, boost::program_options::options_description_easy_init&)
    {};
    // Called after command line is parsed and db/config populated.
    // Hooked config can for example take this oppurtunity to load any file(s).
    virtual future<> initialize(const boost::program_options::variables_map&) {
        return make_ready_future();
    }
    virtual future<> initialize(const boost::program_options::variables_map& map, const db::config& cfg, db::extensions& exts) {
        return initialize(map);
    }

    // visible for testing
    static std::vector<std::reference_wrapper<configurable>>& configurables();
    static future<> init_all(const boost::program_options::variables_map&, const db::config&, db::extensions&);
    static future<> init_all(const db::config&, db::extensions&);
    static void append_all(db::config&, boost::program_options::options_description_easy_init&);
private:
    static void register_configurable(configurable &);
};
