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
#include <random>

namespace cql3 {

namespace statements {

static logging::logger logger("ks_prop_defs");

static locator::replication_strategy_config_options prepare_options(
        const sstring& strategy_class,
        const locator::token_metadata& tm,
        bool rf_rack_valid_keyspaces,
        locator::replication_strategy_config_options options,
        const locator::replication_strategy_config_options& old_options,
        bool rack_list_enabled,
        bool uses_tablets) {
    options.erase(ks_prop_defs::REPLICATION_STRATEGY_CLASS_KEY);

    auto is_nts = locator::abstract_replication_strategy::to_qualified_class_name(strategy_class) == "org.apache.cassandra.locator.NetworkTopologyStrategy";
    const auto& all_dcs = tm.get_datacenter_racks_token_owners();
    auto force_racks = uses_tablets && rf_rack_valid_keyspaces && rack_list_enabled;

    logger.debug("prepare_options: {}: is_nts={} force_racks={} rack_list_enabled={} old_options={} new_options={} all_dcs={}",
                 strategy_class, is_nts, force_racks, rack_list_enabled, old_options, options, all_dcs);

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

    auto expand_dc_racks = [&] (const sstring& dc, const locator::replication_strategy_config_option& rf) {
        logger.debug("expand_dc_racks: dc={} rf={} all_dcs={}", dc, rf, all_dcs);

        std::unordered_set<sstring> allowed_racks;
        std::vector<sstring> dc_racks;
        auto it = all_dcs.find(dc);
        if (it != all_dcs.end()) {
            dc_racks = it->second | std::views::keys | std::ranges::to<std::vector<sstring>>();
            allowed_racks = std::ranges::to<std::unordered_set<sstring>>(dc_racks);
        } else if (!tm.get_topology().get_datacenters().contains(dc)) {
            throw exceptions::configuration_exception(fmt::format("Unrecognized datacenter name '{}'", dc));
        }

        auto& topo = tm.get_topology();
        auto normal_nodes = [&] (const sstring& rack) {
            int count = 0;
            for (auto n : it->second.at(rack)) {
                count += int(topo.get_node(n).is_normal());
            }
            return count;
        };

        auto data = locator::abstract_replication_strategy::parse_replication_factor(rf);
        data.validate(allowed_racks);

        if (data.is_rack_based()) {
            options[dc] = data.get_rack_list();
        } else if (data.count() == 0) {
            options.emplace(dc, "0");
        } else if (force_racks) {
            if (data.count() > dc_racks.size()) {
                throw exceptions::configuration_exception(fmt::format(
                        "Replication factor {} exceeds the number of racks ({}) in dc {}", data.count(), dc_racks.size(), dc));
            }
            if (old_options.count(dc)) {
                auto& old_rf_val = old_options.at(dc);
                auto old_rf = locator::abstract_replication_strategy::parse_replication_factor(old_rf_val);
                if (old_rf.is_rack_based()) {
                    if (old_rf.count() == data.count()) {
                        options[dc] = old_rf_val;
                        return;
                    } else if (old_rf.count() > 0) {
                        throw exceptions::configuration_exception(fmt::format(
                                "Cannot change replication factor for '{}' from {} to numeric {}, use rack list instead",
                                dc, old_rf_val, data.count()));
                    }
                } else if (old_rf.count() > 0) {
                    throw exceptions::configuration_exception(fmt::format(
                            "Cannot change replication factor for '{}' from {} to {}, only rack list is allowed",
                            dc, old_rf_val, data.count()));
                }
            }
            // If the replication factor is less than the number of racks, pick rf racks at random.
            if (data.count() < dc_racks.size()) {
                static thread_local auto gen = std::default_random_engine(std::random_device{}());
                std::ranges::shuffle(dc_racks, gen);
                // Prefer placing into racks which have more normal nodes.
                std::sort(dc_racks.begin(), dc_racks.end(), [&](const sstring& a, const sstring& b) {
                    return normal_nodes(a) > normal_nodes(b);
                });
                dc_racks.resize(data.count());
            }
            options[dc] = dc_racks;
        } else {
            options.emplace(dc, std::get<sstring>(rf));
        }
    };

    // Validate options.
    for (auto&& [dc, opt] : options) {
        locator::replication_factor_data rf(opt);
        if (!rf.is_rack_based()) {
            if (old_options.contains(dc)) {
                auto old_rf = locator::replication_factor_data(old_options.at(dc));
                if (old_rf.is_rack_based() && rf.count() != 0 && old_rf.count() != rf.count()) {
                    throw exceptions::configuration_exception(fmt::format(
                            "Cannot change replication factor for '{}' from {} to {} when the old value was a rack list",
                            dc, old_options.at(dc), opt));
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
        if (old_options.contains(dc)) {
            auto old_rf = locator::replication_factor_data(old_options.at(dc));
            if (!old_rf.is_rack_based() && old_rf.count() != 0) {
                // FIXME: Allow this if replicas already conform to the given rack list.
                // FIXME: Implement automatic colocation to allow transition to rack list.
                throw exceptions::configuration_exception(fmt::format(
                        "Cannot change replication factor from numeric to rack list for '{}'", dc));
            }
        }
    }

    if (!rf && options.empty() && old_options.empty()) {
        if (all_dcs.empty()) {
            throw request_validations::invalid_request("No data centers found in the cluster, cannot determine replication factor");
        }
        for (const auto& [dc, racks_map] : all_dcs) {
            if (racks_map.empty()) {
                continue;
            }
            options.emplace(dc, std::to_string(racks_map.size()));
        }
    }

    // #22688 / #20039 - check for illegal, empty options
    // moved to here. We want to be able to remove dc:s once rf=0,
    // in which case, the options actually serialized in result mutations
    // will in extreme cases in fact be empty -> cannot do this check in
    // verify_options. We only want to apply this constraint on the input
    // provided by the user
    if (!rf && options.empty() && !tm.get_topology().get_datacenters().empty()) {
        throw exceptions::configuration_exception("Configuration for at least one datacenter must be present");
    }

    if (rf.has_value()) {
        locator::replication_factor_data::parse(*rf);

        for (const auto& dc : tm.get_topology().get_datacenters()) {
            auto i = options.find(dc);
            if (i != options.end()) {
                expand_dc_racks(dc, i->second);
            } else if (!old_options.contains(dc)) {
                expand_dc_racks(dc, *rf);
            }
        }
        // We keep previously specified DC factors for safety.
        for (const auto& opt : old_options) {
            if (opt.first != ks_prop_defs::REPLICATION_FACTOR_KEY) {
                options.insert(opt);
            }
        }
    } else if (force_racks) {
        for (const auto& [dc, dc_rf] : options) {
            expand_dc_racks(dc, dc_rf);
        }
    }

    if (uses_tablets) {
        // We keep previously specified DC factors for safety.
        for (const auto& opt: old_options) {
            if (opt.first != ks_prop_defs::REPLICATION_FACTOR_KEY) {
                options.insert(opt);
            }
        }
    }

    // #22688 - filter out any dc*:0 entries - consider these
    // null and void (removed).
    std::erase_if(options, [] (const auto& e) {
        auto& [dc, rf] = e;
        return locator::replication_factor_data(rf).count() == 0;
    });

    return options;
}

ks_prop_defs::ks_prop_defs(property_definitions::map_type options) {
    map_type replication_opts, storage_opts, tablets_opts, durable_writes_opts;

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
    auto options = prepare_options(sc, tm, cfg.rf_rack_valid_keyspaces(), get_replication_options(), {}, rack_list_enabled, uses_tablets);
    return data_dictionary::keyspace_metadata::new_keyspace(ks_name, sc,
            std::move(options), initial_tablets, get_boolean(KW_DURABLE_WRITES, true), get_storage_options());
}

lw_shared_ptr<data_dictionary::keyspace_metadata> ks_prop_defs::as_ks_metadata_update(lw_shared_ptr<data_dictionary::keyspace_metadata> old, const locator::token_metadata& tm, const gms::feature_service& feat, const db::config& cfg) {
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
        options = prepare_options(*sc, tm, cfg.rf_rack_valid_keyspaces(), get_replication_options(), old_options, rack_list_enabled, uses_tablets);
    } else {
        sc = old->strategy_name();
        options = old_options;
    }
    return data_dictionary::keyspace_metadata::new_keyspace(old->name(), *sc, options, initial_tablets, get_boolean(KW_DURABLE_WRITES, true), get_storage_options());
}

namespace {

void add_prefixed_key(const sstring& prefix, const property_definitions::map_type& in, property_definitions::map_type& out) {
    for (const auto& [in_key, in_value]: in) {
        out[fmt::format("{}:{}", prefix, in_key)] = in_value;
    }
}

void add_prefixed_key(const sstring& prefix, const property_definitions::extended_map_type& in, property_definitions::map_type& out) {
    add_prefixed_key(prefix, to_flattened_map(in), out);
}

} // namespace

property_definitions::map_type ks_prop_defs::flattened() const {
    map_type result;

    for (auto kw : {
            ks_prop_defs::KW_REPLICATION,
            ks_prop_defs::KW_STORAGE,
            ks_prop_defs::KW_TABLETS}) {
        if (auto val_opt = get_extended_map(kw)) {
            add_prefixed_key(kw, *val_opt, result);
        }
    }

    for (auto kw : {ks_prop_defs::KW_DURABLE_WRITES}) {
        if (auto val_opt = get_simple(kw)) {
            // Use nested map for backwards compatibility, ks_prop_defs() constructor expects this.
            add_prefixed_key(kw, std::map<sstring, sstring>({{sstring(kw), to_sstring(*val_opt)}}), result);
        }
    }

    return {result};
}

}

}
