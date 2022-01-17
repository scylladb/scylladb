/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include "log.hh"
#include "seastarx.hh"
#include <boost/program_options.hpp>

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
class inet_address;
}

extern logging::logger startlog;

class bad_configuration_error : public std::exception {};

std::set<gms::inet_address> get_seeds_from_db_config(const db::config& cfg);

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
