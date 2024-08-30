/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "init.hh"
#include "gms/inet_address.hh"
#include "seastarx.hh"
#include "db/config.hh"

#include <boost/algorithm/string/trim.hpp>
#include <seastar/core/coroutine.hh>

logging::logger startlog("init");

std::set<gms::inet_address> get_seeds_from_db_config(const db::config& cfg,
                                                     const gms::inet_address broadcast_address,
                                                     const bool fail_on_lookup_error) {
    auto preferred = cfg.listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
    const auto listen = gms::inet_address::lookup(cfg.listen_address(), family).get();
    auto seed_provider = cfg.seed_provider();

    std::set<gms::inet_address> seeds;
    if (seed_provider.parameters.contains("seeds")) {
        size_t begin = 0;
        size_t next = 0;
        sstring seeds_str = seed_provider.parameters.find("seeds")->second;
        while (begin < seeds_str.length() && begin != (next=seeds_str.find(",",begin))) {
            auto seed = boost::trim_copy(seeds_str.substr(begin,next-begin));
            try {
                seeds.emplace(gms::inet_address::lookup(seed, family, preferred).get());
            } catch (...) {
                if (fail_on_lookup_error) {
                    startlog.error("Bad configuration: invalid value in 'seeds': '{}': {}", seed, std::current_exception());
                    throw bad_configuration_error();
                }
                startlog.warn("Bad configuration: invalid value in 'seeds': '{}': {}. Node will continue booting since already bootstrapped.",
                               seed,
                               std::current_exception());
            }
            begin = next+1;
        }
    }
    if (seeds.empty()) {
        seeds.emplace(gms::inet_address("127.0.0.1"));
    }
    startlog.info("seeds={{{}}}, listen_address={}, broadcast_address={}",
            fmt::join(seeds, ", "), listen, broadcast_address);
    if (broadcast_address != listen && seeds.contains(listen)) {
        startlog.error("Use broadcast_address instead of listen_address for seeds list");
        throw std::runtime_error("Use broadcast_address for seeds list");
    }
    if (!cfg.replace_node_first_boot().empty() && seeds.contains(broadcast_address)) {
        startlog.error("Bad configuration: replace-node-first-boot is not allowed for seed nodes");
        throw bad_configuration_error();
    }
    if ((!cfg.replace_address_first_boot().empty() || !cfg.replace_address().empty()) && seeds.contains(broadcast_address)) {
        startlog.error("Bad configuration: replace-address and replace-address-first-boot are not allowed for seed nodes");
        throw bad_configuration_error();
    }

    return seeds;
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

future<configurable::notify_set> configurable::init_all(const boost::program_options::variables_map& opts, const db::config& cfg, db::extensions& exts, const service_set& services) {
    notify_set res;
    for (auto& c : configurables()) {
        auto f = co_await c.get().initialize_ex(opts, cfg, exts, services);
        if (f) {
            res._listeners.emplace_back(std::move(f));
        }
    }
    co_return res;
}

future<configurable::notify_set> configurable::init_all(const db::config& cfg, db::extensions& exts, const service_set& services) {
    return do_with(boost::program_options::variables_map{}, [&](auto& opts) {
        return init_all(opts, cfg, exts, services);
    });
}

future<> configurable::notify_set::notify_all(system_state e) {
    co_return co_await do_for_each(_listeners, [e](const notify_func& c) {
        return c(e);
    });
}

class service_set::impl {
public:
    void add(std::any value) {
        _services.emplace(value.type(), value);
    }
    std::any find(const std::type_info& type) const {
        return _services.at(type);
    }
private:
    std::unordered_map<std::type_index, std::any> _services;
};

service_set::service_set()
    : _impl(std::make_unique<impl>())
{}
service_set::~service_set() = default;

void service_set::add(std::any value) {
    _impl->add(std::move(value));
} 

std::any service_set::find(const std::type_info& type) const {
    return _impl->find(type);
}
