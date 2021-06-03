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

#include "init.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "to_string.hh"
#include "gms/inet_address.hh"
#include "seastarx.hh"
#include "db/config.hh"

#include <boost/algorithm/string/trim.hpp>

logging::logger startlog("init");

void init_gossiper(sharded<gms::gossiper>& gossiper
                , db::config& cfg
                , sstring listen_address_in
                , db::seed_provider_type seed_provider
                , sstring cluster_name) {
    auto preferred = cfg.listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
    const auto listen = gms::inet_address::lookup(listen_address_in, family).get0();

    // Init gossiper
    std::set<gms::inet_address> seeds;
    if (seed_provider.parameters.contains("seeds")) {
        size_t begin = 0;
        size_t next = 0;
        sstring seeds_str = seed_provider.parameters.find("seeds")->second;
        while (begin < seeds_str.length() && begin != (next=seeds_str.find(",",begin))) {
            auto seed = boost::trim_copy(seeds_str.substr(begin,next-begin));
            try {
                seeds.emplace(gms::inet_address::lookup(seed, family, preferred).get0());
            } catch (...) {
                startlog.error("Bad configuration: invalid value in 'seeds': '{}': {}", seed, std::current_exception());
                throw bad_configuration_error();
            }
            begin = next+1;
        }
    }
    if (seeds.empty()) {
        seeds.emplace(gms::inet_address("127.0.0.1"));
    }
    auto broadcast_address = utils::fb_utilities::get_broadcast_address();
    startlog.info("seeds={}, listen_address={}, broadcast_address={}",
            to_string(seeds), listen_address_in, broadcast_address);
    if (broadcast_address != listen && seeds.contains(listen)) {
        startlog.error("Use broadcast_address instead of listen_address for seeds list");
        throw std::runtime_error("Use broadcast_address for seeds list");
    }
    if ((!cfg.replace_address_first_boot().empty() || !cfg.replace_address().empty()) && seeds.contains(broadcast_address)) {
        startlog.error("Bad configuration: replace-address and replace-address-first-boot are not allowed for seed nodes");
        throw bad_configuration_error();
    }
    gossiper.local().set_seeds(seeds);
    // Do it in the background.
    (void)gossiper.invoke_on_all([cluster_name](gms::gossiper& g) {
        g.set_cluster_name(cluster_name);
    }).handle_exception([] (std::exception_ptr e) {
        startlog.error("Unexpected exception while setting cluster name: {}", e);
    });
}


std::vector<std::reference_wrapper<configurable>>& configurable::configurables() {
    static std::vector<std::reference_wrapper<configurable>> configurables;
    return configurables;
}

void configurable::register_configurable(configurable & c) {
    configurables().emplace_back(std::ref(c));
}

void configurable::append_all(db::config& cfg, boost::program_options::options_description_easy_init& init) {
    for (configurable& c : configurables()) {
        c.append_options(cfg, init);
    }
}

future<> configurable::init_all(const boost::program_options::variables_map& opts, const db::config& cfg, db::extensions& exts) {
    return do_for_each(configurables(), [&](configurable& c) {
        return c.initialize(opts, cfg, exts);
    });
}

future<> configurable::init_all(const db::config& cfg, db::extensions& exts) {
    return do_with(boost::program_options::variables_map{}, [&](auto& opts) {
        return init_all(opts, cfg, exts);
    });
}
