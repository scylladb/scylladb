/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "init.hh"
#include "gms/inet_address.hh"
#include "seastarx.hh"
#include "db/config.hh"

#include <boost/algorithm/string/trim.hpp>
#include <seastar/core/coroutine.hh>
#include "sstables/sstable_compressor_factory.hh"
#include "gms/feature_service.hh"

logging::logger startlog("init");

using namespace std::string_literals;

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

std::set<sstring> get_disabled_features_from_db_config(const db::config& cfg, std::set<sstring> disabled) {
    switch (sstables::version_from_string(cfg.sstable_format())) {
    case sstables::sstable_version_types::md:
        startlog.warn("sstable_format must be 'me', '{}' is specified", cfg.sstable_format());
        break;
    case sstables::sstable_version_types::me:
        break;
    default:
        SCYLLA_ASSERT(false && "Invalid sstable_format");
    }

    if (!cfg.enable_user_defined_functions()) {
        disabled.insert("UDF");
    } else {
        if (!cfg.check_experimental(db::experimental_features_t::feature::UDF)) {
            throw std::runtime_error(
                    "You must use both enable_user_defined_functions and experimental_features:udf "
                    "to enable user-defined functions");
        }
    }

    if (!cfg.check_experimental(db::experimental_features_t::feature::ALTERNATOR_STREAMS)) {
        disabled.insert("ALTERNATOR_STREAMS"s);
    }
    if (!cfg.check_experimental(db::experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS)) {
        disabled.insert("KEYSPACE_STORAGE_OPTIONS"s);
    }
    if (!cfg.check_experimental(db::experimental_features_t::feature::VIEWS_WITH_TABLETS)) {
        disabled.insert("VIEWS_WITH_TABLETS"s);
    }
    if (cfg.force_gossip_topology_changes()) {
        if (cfg.enable_tablets_by_default()) {
            throw std::runtime_error("Tablets cannot be enabled with gossip topology changes.  Use either --tablets-mode-for-new-keyspaces=enabled|enforced or --force-gossip-topology-changes, but not both.");
        }
        startlog.warn("The tablets feature is disabled due to forced gossip topology changes");
        disabled.insert("TABLETS"s);
    }
    if (!cfg.table_digest_insensitive_to_expiry()) {
        disabled.insert("TABLE_DIGEST_INSENSITIVE_TO_EXPIRY"s);
    }
    if (!cfg.commitlog_use_fragmented_entries()) {
        disabled.insert("FRAGMENTED_COMMITLOG_ENTRIES"s);
    }

    if (!gms::is_test_only_feature_enabled()) {
        disabled.insert("TEST_ONLY_FEATURE"s);
    }

    return disabled;
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

// Placed here to avoid dependency on db::config in compress.cc,
// where the rest of default_sstable_compressor_factory_config is.
auto default_sstable_compressor_factory_config::from_db_config(
    const db::config& cfg,
    std::span<const unsigned> numa_config) -> self
{
    return self {
        .register_metrics = true,
        .enable_writing_dictionaries = cfg.sstable_compression_dictionaries_enable_writing,
        .memory_fraction_starting_at_which_we_stop_writing_dicts = cfg.sstable_compression_dictionaries_memory_budget_fraction,
        .numa_config{numa_config.begin(), numa_config.end()},
    };
}
