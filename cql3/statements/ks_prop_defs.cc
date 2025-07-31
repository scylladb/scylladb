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
#include "cql3/statements/request_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "locator/token_metadata.hh"
#include "locator/abstract_replication_strategy.hh"
#include "exceptions/exceptions.hh"
#include "gms/feature_service.hh"
#include "db/config.hh"

namespace cql3 {

namespace statements {

static logging::logger logger("ks_prop_defs");

static locator::replication_strategy_config_options prepare_options(
        const sstring& strategy_class,
        const locator::token_metadata& tm,
        locator::replication_strategy_config_options options,
        const locator::replication_strategy_config_options& old_options,
        bool rack_list_enabled,
        bool uses_tablets) {
    options.erase(ks_prop_defs::REPLICATION_STRATEGY_CLASS_KEY);

    auto is_nts = locator::abstract_replication_strategy::to_qualified_class_name(strategy_class) == "org.apache.cassandra.locator.NetworkTopologyStrategy";

    logger.debug("prepare_options: {}: is_nts={} old_options={} new_options={}", strategy_class, is_nts, old_options, options);

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

    // Validate options.
    for (auto&& [dc, opt] : options) {
        locator::replication_factor_data rf(opt);

        std::optional<locator::replication_factor_data> old_rf;
        auto i = old_options.find(dc);
        if (i != old_options.end()) {
            old_rf = locator::replication_factor_data(i->second);
        }

        if (!rf.is_rack_based()) {
            if (old_rf && old_rf->is_rack_based() && rf.count() != 0) {
                if (old_rf->count() != rf.count()) {
                    throw exceptions::configuration_exception(fmt::format(
                            "Cannot change replication factor for '{}' from {} to {} when the old value was a rack list",
                            dc, old_options.at(dc), opt));
                } else {
                    options[dc] = i->second; // Preserve rack list.
                }
            }
            continue;
        }
        if (!rack_list_enabled) {
            throw exceptions::configuration_exception(fmt::format(
                    "Using rack list for '{}' is not allowed because the 'rf_rack_list' feature is disabled", dc));
        }
        if (!uses_tablets) {
            throw exceptions::configuration_exception(fmt::format(
                    "Using rack list for '{}' is not allowed because the keyspace is not using tablets", dc));
        }
        auto& racks = rf.get_rack_list();
        if (std::unordered_set<sstring>(racks.begin(), racks.end()).size() != rf.count()) {
            throw exceptions::configuration_exception(fmt::format(
                    "Rack list for '{}' contains duplicate entries", dc));
        }
        if (old_rf && !old_rf->is_rack_based() && old_rf->count() != 0) {
            // FIXME: Allow this if replicas already conform to the given rack list.
            // FIXME: Implement automatic colocation to allow transition to rack list.
            throw exceptions::configuration_exception(fmt::format(
                    "Cannot change replication factor from numeric to rack list for '{}'", dc));
        }
    }

    if (rf.has_value()) {
        locator::replication_factor_data::parse(*rf);

        // We keep previously specified DC factors for safety.
        for (const auto& opt : old_options) {
            if (opt.first != ks_prop_defs::REPLICATION_FACTOR_KEY) {
                options.insert(opt);
            }
        }

        for (const auto& dc : tm.get_topology().get_datacenters()) {
            options.emplace(dc, *rf);
        }
    } else if (options.empty() && old_options.empty()) {
        // For default replication factor consider only racks with nodes that are NOT zero-token only nodes,
        auto dc_racks = tm.get_datacenter_racks_token_owners();
        if (dc_racks.empty()) {
            throw request_validations::invalid_request("No data centers found in the cluster, cannot determine replication factor");
        }
        for (const auto& [dc, racks_map] : dc_racks) {
            if (racks_map.empty()) {
                continue;
            }
            options.emplace(dc, std::to_string(racks_map.size()));
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
    std::map<sstring, sstring> replication_opts, storage_opts, tablets_opts, durable_writes_opts;

    auto read_property_into = [] (auto& map, const sstring& name, const sstring& value, const sstring& tag) {
        auto prefix = sstring(tag) + ":";
        if (!name.starts_with(prefix)) {
            throw std::runtime_error(seastar::format("ks_prop_defs: Expected name to start with \"{}\", but got: \"{}\"", prefix, name));
        }
        map[name.substr(prefix.size())] = value;
    };

    for (const auto& [name, value] : options) {
        if (name.starts_with(KW_DURABLE_WRITES)) {
            read_property_into(durable_writes_opts, name, value, KW_DURABLE_WRITES);
        } else if (name.starts_with(KW_REPLICATION)) {
            read_property_into(replication_opts, name, value, KW_REPLICATION);
        } else if (name.starts_with(KW_TABLETS)) {
            read_property_into(tablets_opts, name, value, KW_TABLETS);
        } else if (name.starts_with(KW_STORAGE)) {
            read_property_into(storage_opts, name, value, KW_STORAGE);
        }
    }

    if (!replication_opts.empty())
        add_property(KW_REPLICATION, from_flattened_map(replication_opts));
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

void ks_prop_defs::set_default_replication_strategy_class_option() {
    auto options = get_replication_options();
    if (!options.contains(REPLICATION_STRATEGY_CLASS_KEY)) {
        options[REPLICATION_STRATEGY_CLASS_KEY] = DEFAULT_REPLICATION_STRATEGY_CLASS;
        remove_property(KW_REPLICATION);
        add_property(KW_REPLICATION, std::move(options));
    }
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
    bool uses_tablets = initial_tablets.has_value();
    bool rack_list_enabled = feat.rack_list_rf;
    auto options = prepare_options(sc, tm, get_replication_options(), {}, rack_list_enabled, uses_tablets);
    return data_dictionary::keyspace_metadata::new_keyspace(ks_name, sc,
            std::move(options), initial_tablets, get_boolean(KW_DURABLE_WRITES, true), get_storage_options());
}

lw_shared_ptr<data_dictionary::keyspace_metadata> ks_prop_defs::as_ks_metadata_update(lw_shared_ptr<data_dictionary::keyspace_metadata> old, const locator::token_metadata& tm, const gms::feature_service& feat) {
    locator::replication_strategy_config_options options;
    const auto& old_options = old->strategy_options();
    // if tablets options have not been specified, inherit them if it's tablets-enabled KS
    auto initial_tablets = get_initial_tablets(old->initial_tablets());
    auto uses_tablets = initial_tablets.has_value();
    if (old->uses_tablets() != uses_tablets) {
        throw exceptions::invalid_request_exception("Cannot alter replication strategy vnode/tablets flavor");
    }
    auto sc = get_replication_strategy_class();
    bool rack_list_enabled = feat.rack_list_rf;
    if (sc) {
        options = prepare_options(*sc, tm, get_replication_options(), old_options, rack_list_enabled, uses_tablets);
    } else {
        sc = old->strategy_name();
        options = old_options;
    }
    return data_dictionary::keyspace_metadata::new_keyspace(old->name(), *sc, options, initial_tablets, get_boolean(KW_DURABLE_WRITES, true), get_storage_options());
}


}

}
