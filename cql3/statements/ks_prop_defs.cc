/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/statements/ks_prop_defs.hh"
#include "data_dictionary/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "locator/token_metadata.hh"
#include "locator/abstract_replication_strategy.hh"
#include "log.hh"

namespace cql3 {

logging::logger logger("keyspace_options");

namespace statements {

static std::map<sstring, sstring> prepare_options(
        const sstring& ks_name,
        const sstring& strategy_class,
        const locator::token_metadata& tm,
        std::map<sstring, sstring> options,
        const std::map<sstring, sstring>& old_options = {}) {
    options.erase(ks_prop_defs::REPLICATION_STRATEGY_CLASS_KEY);

    if (locator::abstract_replication_strategy::to_qualified_class_name(strategy_class) != "org.apache.cassandra.locator.NetworkTopologyStrategy") {
        return options;
    }

    // For users' convenience, expand the 'replication_factor' option into a replication factor for each DC.
    // If the user simply switches from another strategy without providing any options,
    // but the other strategy used the 'replication_factor' option, it will also be expanded.
    // See issue CASSANDRA-14303.

    std::optional<sstring> rf;
    bool old_rf_option = false;
    auto it = options.find(ks_prop_defs::REPLICATION_FACTOR_KEY);
    if (it != options.end()) {
        // Expand: the user explicitly provided a 'replication_factor'.
        rf = it->second;
        options.erase(it);
        logger.warn("Using the \"replication_factor\" option is not recommended, but was used for keyspace \"{}\". "
                "Use per-datacenter replication factor options instead.", ks_name);
    } else if (options.empty()) {
        auto it = old_options.find(ks_prop_defs::REPLICATION_FACTOR_KEY);
        if (it != old_options.end()) {
            // Expand: the user switched from another strategy that specified a 'replication_factor'
            // and didn't provide any additional options.
            rf = it->second;
            old_rf_option = true;
        }
    }

    if (rf.has_value()) {
        // The code below may end up not using "rf" at all (if all the DCs
        // already have rf settings), so let's validate it once (#8880).
        locator::abstract_replication_strategy::validate_replication_factor(*rf);

        const auto& dcs = tm.get_topology().get_datacenters();
        size_t old_dc_rfs = 0;
        size_t new_dc_rfs = 0;

        // We keep previously specified DC factors for safety.
        for (const auto& opt : old_options) {
            if (opt.first != ks_prop_defs::REPLICATION_FACTOR_KEY) {
                options.insert(opt);
                if (dcs.contains(opt.first)) {
                    ++old_dc_rfs;
                }
            }
        }

        for (const auto& dc : dcs) {
            if (options.emplace(dc, *rf).second) {
                ++new_dc_rfs;
            }
        }

        // Warn about non-trivial cases
        if (new_dc_rfs) {
            if (old_dc_rfs) {
                logger.warn("The \"replication_factor\": {} option was applied only to newly added datacenters. "
                        "Consider using explicit per-datacenter options instead. Current replication options are {}", *rf, options);
            } else if (old_rf_option) {
                const auto& dc_racks = tm.get_topology().get_datacenter_racks();
                // Warn if topology has multiple datacenters, or multiple racks in a single dc.
                // In this case the SimpleStrategy replication factor was applied globally, while
                // now it will apply per dc/rack and secondary replica ownerships change -
                // requiring repair and cleanup.
                if (dcs.size() > 1 || (!dc_racks.empty() && dc_racks.begin()->second.size() != 1)) {
                    logger.warn("\"replication_factor\": {} was inherited from a previous replication strategy, where it may have been applied globally in the cluster. "
                            "It now applies to the following topology: {}. Current replication options are: {}. "
                            "Run repair to rebuild the new token replicas and then cleanup to remove the stale replicas",
                            *rf, dc_racks, options);
                }
            }
        } else if (!old_rf_option) {
            logger.warn("The \"replication_factor\": {} option did not apply to any existing datacenters. "
                    "Consider using explicit per-datacenter options instead. Current replication options are {}", *rf, options);
        }
    }

    return options;
}

void ks_prop_defs::validate() {
    // Skip validation if the strategy class is already set as it means we've alreayd
    // prepared (and redoing it would set strategyClass back to null, which we don't want)
    if (_strategy_class) {
        return;
    }

    static std::set<sstring> keywords({ sstring(KW_DURABLE_WRITES), sstring(KW_REPLICATION), sstring(KW_STORAGE) });
    property_definitions::validate(keywords);

    auto replication_options = get_replication_options();
    if (replication_options.contains(REPLICATION_STRATEGY_CLASS_KEY)) {
        _strategy_class = replication_options[REPLICATION_STRATEGY_CLASS_KEY];
    }
}

std::map<sstring, sstring> ks_prop_defs::get_replication_options() const {
    auto replication_options = get_map(KW_REPLICATION);
    if (replication_options) {
        return replication_options.value();
    }
    return std::map<sstring, sstring>{};
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

std::optional<sstring> ks_prop_defs::get_replication_strategy_class() const {
    return _strategy_class;
}

lw_shared_ptr<data_dictionary::keyspace_metadata> ks_prop_defs::as_ks_metadata(sstring ks_name, const locator::token_metadata& tm) {
    auto sc = get_replication_strategy_class().value();
    return data_dictionary::keyspace_metadata::new_keyspace(ks_name, sc,
            prepare_options(ks_name, sc, tm, get_replication_options()), get_boolean(KW_DURABLE_WRITES, true), std::vector<schema_ptr>{}, get_storage_options());
}

lw_shared_ptr<data_dictionary::keyspace_metadata> ks_prop_defs::as_ks_metadata_update(lw_shared_ptr<data_dictionary::keyspace_metadata> old, const locator::token_metadata& tm) {
    std::map<sstring, sstring> options;
    const auto& old_options = old->strategy_options();
    auto sc = get_replication_strategy_class();
    if (sc) {
        options = prepare_options(old->name(), *sc, tm, get_replication_options(), old_options);
    } else {
        sc = old->strategy_name();
        options = old_options;
    }

    return data_dictionary::keyspace_metadata::new_keyspace(old->name(), *sc, options, get_boolean(KW_DURABLE_WRITES, true), std::vector<schema_ptr>{}, get_storage_options());
}


}

}
