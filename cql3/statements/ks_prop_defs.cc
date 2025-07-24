/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "locator/token_metadata.hh"
#include "locator/abstract_replication_strategy.hh"
#include "exceptions/exceptions.hh"
#include "gms/feature_service.hh"
#include "db/config.hh"
#include <random>

namespace cql3 {

namespace statements {

static logging::logger logger("ks_prop_defs");

static locator::replication_strategy_config_options prepare_options(
        const sstring& strategy_class,
        const locator::token_metadata& tm,
        bool rf_rack_valid_keyspaces,
        locator::replication_strategy_config_options options,
        const locator::replication_strategy_config_options& old_options = {}) {
    options.erase(ks_prop_defs::REPLICATION_STRATEGY_CLASS_KEY);

    auto is_nts = locator::abstract_replication_strategy::to_qualified_class_name(strategy_class) == "org.apache.cassandra.locator.NetworkTopologyStrategy";
    const auto& all_dcs = tm.get_topology().get_datacenter_racks();

    logger.info("prepare_options: {}: is_nts={} rf_rack_valid_keyspaces={} {{{}}}: all_dcs={}", strategy_class, is_nts, rf_rack_valid_keyspaces,
            fmt::join(options | std::views::transform([] (auto& x) {
                            return fmt::format("{}:{}", x.first, x.second);
                    }),
                    ","), all_dcs);

    if (!is_nts) {
        return options;
    }

    // For users' convenience, expand the 'replication_factor' option into a replication factor for each DC.
    // If the user simply switches from another strategy without providing any options,
    // but the other strategy used the 'replication_factor' option, it will also be expanded.
    // See issue CASSANDRA-14303.
    std::optional<sstring> rf;
    auto it = options.find(ks_prop_defs::REPLICATION_FACTOR_KEY);
    if (it != options.end()) {
        // Expand: the user explicitly provided a 'replication_factor'.
        try {
            rf = std::get<sstring>(it->second);
        } catch (...) {
            throw exceptions::configuration_exception(fmt::format("Invalid replication factor: {}: must be a string holding a numerical value", it->second));
        }
        options.erase(it);
    } else if (options.empty()) {
        auto it = old_options.find(ks_prop_defs::REPLICATION_FACTOR_KEY);
        if (it != old_options.end()) {
            // Expand: the user switched from another strategy that specified a 'replication_factor'
            // and didn't provide any additional options.
            rf = std::get<sstring>(it->second);
        }
    }

    auto expand_dc_racks = [&] (const sstring& dc, const locator::replication_strategy_config_option& rf, bool rf_rack_valid_keyspaces) {
        logger.info("expand_dc_racks: dc={} rf={} rf_rack_valid_keyspaces={} all_dcs={}", dc, rf, rf_rack_valid_keyspaces, all_dcs);
        auto it = all_dcs.find(dc);
        if (it == all_dcs.end()) {
            return;
        }
        auto opt = options.find(dc);
        if (opt != options.end() && (std::holds_alternative<locator::rack_list>(opt->second) || !rf_rack_valid_keyspaces)) {
            return;
        }
        auto dc_racks = it->second | std::views::keys | std::ranges::to<std::vector<sstring>>();
        auto data = locator::abstract_replication_strategy::parse_replication_factor(rf, dc_racks | std::ranges::to<std::unordered_set<sstring>>());

        if (data.is_rack_based()) {
            options[dc] = data.get_rack_list();
        } else if (rf_rack_valid_keyspaces) {
            // If the replication factor is less than the number of racks, pick rf racks at random.
            if (data.count() < dc_racks.size()) {
                static thread_local auto gen = std::default_random_engine(std::random_device{}());
                std::ranges::shuffle(dc_racks, gen);
                dc_racks.resize(data.count());
            }
            options[dc] = dc_racks;
        } else {
            options.emplace(dc, std::get<sstring>(rf));
        }
    };

    if (rf.has_value()) {

        // We keep previously specified DC factors for safety.
        for (const auto& opt : old_options) {
            if (opt.first != ks_prop_defs::REPLICATION_FACTOR_KEY) {
                options.insert(opt);
            }
        }

        for (const auto& dc : all_dcs | std::views::keys) {
            expand_dc_racks(dc, *rf, rf_rack_valid_keyspaces);
        }
    } else if (rf_rack_valid_keyspaces) {
        for (const auto& [dc, dc_rf] : options) {
            expand_dc_racks(dc, dc_rf, true);
        }
    }

    // #22688 / #20039 - check for illegal, empty options (after above expand)
    // moved to here. We want to be able to remove dc:s once rf=0, 
    // in which case, the options actually serialized in result mutations
    // will in extreme cases in fact be empty -> cannot do this check in 
    // verify_options. We only want to apply this constraint on the input
    // provided by the user
    if (options.empty() && !tm.get_topology().get_datacenters().empty()) {
        throw exceptions::configuration_exception("Configuration for at least one datacenter must be present");
    }

    return options;
}

ks_prop_defs::ks_prop_defs(std::map<sstring, sstring> options) {
    extended_map_type replication_opts;
    map_type storage_opts, tablets_opts, durable_writes_opts;

    auto read_property_into = [] (auto& map, const sstring& name, const sstring& value, const sstring& tag) {
        map[name.substr(sstring(tag).size() + 1)] = value;
    };

    auto read_extended_property_into = [] (extended_map_type& map, const sstring& name, const sstring& value, const sstring& tag) {
        auto key = name.substr(tag.size() + 1);
        auto pos = key.find(':');
        if (pos == sstring::npos) {
            map[key] = value;
        } else {
            key.resize(pos);
            if (auto it = map.find(key); it != map.end()) {
                // If the key already exists, we append the value to the existing one.
                std::get<std::vector<sstring>>(it->second).emplace_back(value);
            } else {
                // Otherwise, we create a new entry.
                map.emplace(key, std::vector<sstring>{value});
            }
        }
};

    for (const auto& [name, value] : options) {
        if (name.starts_with(KW_DURABLE_WRITES)) {
            read_property_into(durable_writes_opts, name, value, KW_DURABLE_WRITES);
        } else if (name.starts_with(KW_REPLICATION)) {
            read_extended_property_into(replication_opts, name, value, KW_REPLICATION);
        } else if (name.starts_with(KW_TABLETS)) {
            read_property_into(tablets_opts, name, value, KW_TABLETS);
        } else if (name.starts_with(KW_STORAGE)) {
            read_property_into(storage_opts, name, value, KW_STORAGE);
        }
    }

    if (!replication_opts.empty())
        add_property(KW_REPLICATION, replication_opts);
    if (!storage_opts.empty())
        add_property(KW_STORAGE, storage_opts);
    if (!tablets_opts.empty())
        add_property(KW_TABLETS, tablets_opts);
    if (!durable_writes_opts.empty())
        add_property(KW_DURABLE_WRITES, durable_writes_opts.begin()->second);
}

void ks_prop_defs::validate() {
    // Skip validation if the strategy class is already set as it means we've already
    // prepared (and redoing it would set strategyClass back to null, which we don't want)
    if (_strategy_class) {
        return;
    }

    static std::set<sstring> keywords({ sstring(KW_DURABLE_WRITES), sstring(KW_REPLICATION), sstring(KW_STORAGE), sstring(KW_TABLETS) });
    property_definitions::validate(keywords);

    auto replication_options = get_replication_options();
    if (replication_options.contains(REPLICATION_STRATEGY_CLASS_KEY)) {
        const auto& class_name = replication_options[REPLICATION_STRATEGY_CLASS_KEY];
        if (!std::holds_alternative<sstring>(class_name)) {
            throw exceptions::configuration_exception(seastar::format("Invalid replication strategy class: {}", class_name));
        }
        _strategy_class = std::get<sstring>(class_name);
    }
}

locator::replication_strategy_config_options ks_prop_defs::get_replication_options() const {
    auto replication_options = get_extended_map(KW_REPLICATION);
    if (replication_options) {
        return replication_options.value();
    }
    return {};
}

data_dictionary::storage_options ks_prop_defs::get_storage_options() const {
    data_dictionary::storage_options opts;
    auto options_map = get_map(KW_STORAGE);
    if (options_map) {
        auto it = options_map->find("type");
        if (it != options_map->end()) {
            sstring storage_type = it->second;
            options_map->erase(it);
            opts.value = data_dictionary::storage_options::from_map(storage_type, std::move(*options_map));
        }
    }
    return opts;
}

std::optional<unsigned> ks_prop_defs::get_initial_tablets(std::optional<unsigned> default_value, bool enforce_tablets) const {
    auto tablets_options = get_map(KW_TABLETS);
    if (!tablets_options) {
        return default_value;
    }

    unsigned initial_count = 0;
    auto it = tablets_options->find("enabled");
    if (it != tablets_options->end()) {
        auto enabled = it->second;
        tablets_options->erase(it);

        if (enabled == "true") {
            // nothing
        } else if (enabled == "false") {
            if (enforce_tablets) {
                throw exceptions::configuration_exception("Cannot disable tablets for keyspace since tablets are enforced using the `tablets_mode_for_new_keyspaces: enforced` config option.");
            }
            return std::nullopt;
        } else {
            throw exceptions::configuration_exception(sstring("Tablets enabled value must be true or false; found: ") + enabled);
        }
    }

    it = tablets_options->find("initial");
    if (it != tablets_options->end()) {
        try {
            initial_count = std::stol(it->second);
        } catch (...) {
            throw exceptions::configuration_exception(sstring("Initial tablets value should be numeric; found ") + it->second);
        }
        tablets_options->erase(it);
    }

    if (!tablets_options->empty()) {
        throw exceptions::configuration_exception(sstring("Unrecognized tablets option ") + tablets_options->begin()->first);
    }

    return initial_count;
}

std::optional<sstring> ks_prop_defs::get_replication_strategy_class() const {
    return _strategy_class;
}

bool ks_prop_defs::get_durable_writes() const {
    return get_boolean(KW_DURABLE_WRITES, true);
}

lw_shared_ptr<data_dictionary::keyspace_metadata> ks_prop_defs::as_ks_metadata(sstring ks_name, const locator::token_metadata& tm, const gms::feature_service& feat, const db::config& cfg) {
    auto sc = get_replication_strategy_class().value();
    // if tablets options have not been specified, but tablets are globally enabled, set the value to 0 for N.T.S. only
    auto enable_tablets = feat.tablets && cfg.enable_tablets_by_default();
    std::optional<unsigned> default_initial_tablets = enable_tablets && locator::abstract_replication_strategy::to_qualified_class_name(sc) == "org.apache.cassandra.locator.NetworkTopologyStrategy"
            ? std::optional<unsigned>(0) : std::nullopt;
    auto initial_tablets = get_initial_tablets(default_initial_tablets, cfg.enforce_tablets());
    auto options = prepare_options(sc, tm, cfg.rf_rack_valid_keyspaces(), get_replication_options());
    return data_dictionary::keyspace_metadata::new_keyspace(ks_name, sc,
            std::move(options), initial_tablets, get_boolean(KW_DURABLE_WRITES, true), get_storage_options());
}

lw_shared_ptr<data_dictionary::keyspace_metadata> ks_prop_defs::as_ks_metadata_update(lw_shared_ptr<data_dictionary::keyspace_metadata> old, const locator::token_metadata& tm, const gms::feature_service& feat, const db::config& cfg) {
    locator::replication_strategy_config_options options;
    const auto& old_options = old->strategy_options();
    auto sc = get_replication_strategy_class();
    if (sc) {
        options = prepare_options(*sc, tm, cfg.rf_rack_valid_keyspaces(), get_replication_options(), old_options);
    } else {
        sc = old->strategy_name();
        options = old_options;
    }
    // if tablets options have not been specified, inherit them if it's tablets-enabled KS
    auto initial_tablets = get_initial_tablets(old->initial_tablets());
    return data_dictionary::keyspace_metadata::new_keyspace(old->name(), *sc, options, initial_tablets, get_boolean(KW_DURABLE_WRITES, true), get_storage_options());
}


}

}
