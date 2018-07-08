/*
 * Copyright (C) 2015 ScyllaDB
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
#include "auth/service.hh"
#include "db/config.hh"
#include "db/system_distributed_keyspace.hh"
#include "database.hh"
#include "log.hh"

namespace db {
class extensions;
}

extern logging::logger startlog;

class bad_configuration_error : public std::exception {};

void init_storage_service(distributed<database>& db, sharded<auth::service>&, sharded<db::system_distributed_keyspace>&);

struct init_scheduling_config {
    scheduling_group streaming;
    scheduling_group statement;
    scheduling_group gossip;
};

void init_ms_fd_gossiper(sstring listen_address
                , uint16_t storage_port
                , uint16_t ssl_storage_port
                , bool tcp_nodelay_inter_dc
                , sstring ms_encrypt_what
                , sstring ms_trust_store
                , sstring ms_cert
                , sstring ms_key
                , sstring ms_tls_prio
                , bool ms_client_auth
                , sstring ms_compress
                , db::seed_provider_type seed_provider
                , size_t available_memory
                , init_scheduling_config scheduling_config
                , sstring cluster_name = "Test Cluster"
                , double phi = 8
                , bool sltba = false);

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
