/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/cluster_config_db_config_bridge.hh"

#include <limits>
#include <optional>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include <fmt/format.h>

#include <seastar/core/smp.hh>

#include "db/cluster_config_manager.hh"
#include "db/cluster_config_registry.hh"
#include "db/config.hh"
#include "utils/log.hh"
#include "utils/on_internal_error.hh"

namespace db {

namespace {

logging::logger bridge_logger("cluster_config_db_config_bridge");

using config_source = utils::config_file::config_source;
using registry_option = cluster_config_registry::option;
using value_type = cluster_config_registry::value_type;

// The registry type a db::config entry of native type T is declared with.
template <typename T>
constexpr value_type registry_type_for() {
    if constexpr (std::is_same_v<T, bool>) {
        return value_type::boolean;
    } else if constexpr (std::is_integral_v<T>) {
        return value_type::integer;
    } else if constexpr (std::is_floating_point_v<T>) {
        return value_type::floating_point;
    } else {
        static_assert(std::is_same_v<T, sstring>, "unsupported db::config entry type");
        return value_type::text;
    }
}

template <typename T>
class db_config_binding final : public cluster_config_db_config_bridge::binding {
    utils::config_file::named_value<T>& _entry;
    // A copy: the registry hands out pointers into storage that tests may replace.
    const registry_option _opt;
    struct baseline {
        T value;
        config_source source;
    };
    // This shard's pre-cluster value, present while an override is applied. The source is
    // recorded on shard 0 only.
    std::optional<baseline> _baseline;
    cluster_config_manager::config_callback_registration _registration;

    // on_change() checked the stored text with validate_value(), and for an integral T bind()
    // checked that the option's valid_range fits T, so the registry converters cannot fall back
    // to the default and the cast cannot wrap.
    T parse(const std::optional<sstring>& value) const {
        if constexpr (std::is_same_v<T, bool>) {
            return cluster_config_registry::to_boolean(_opt, value);
        } else if constexpr (std::is_integral_v<T>) {
            return static_cast<T>(cluster_config_registry::to_integer(_opt, value));
        } else if constexpr (std::is_floating_point_v<T>) {
            return static_cast<T>(cluster_config_registry::to_floating_point(_opt, value));
        } else {
            return cluster_config_registry::to_text(_opt, value);
        }
    }

    // Synchronous on purpose: a callback that suspended could resume after this binding is
    // destroyed, since stop() only unregisters.
    //
    // A named_value holds one value per shard but a single source shared by all shards, so
    // only shard 0 records, sets and restores the source; the other shards touch their value
    // only. The shards' callbacks run in no particular order, and a shard that snapshotted
    // the shared source after shard 0 had already set it would restore ClusterConfig.
    void on_change(const std::optional<sstring>& value) {
        const bool owns_source = this_shard_id() == 0;
        if (value) {
            // ALTER validated the value on its coordinator, whose binary may have other rules
            // during an upgrade or rollback. Keep the current value rather than apply it.
            if (auto error = cluster_config_registry::validate_value(_opt, *value)) {
                if (owns_source) {
                    bridge_logger.warn("Ignoring cluster config override {} = {}: {}", _opt.name, *value, *error);
                }
                return;
            }
            const T parsed = parse(value);
            // Recorded only once the apply succeeded, so a rejected value leaves no baseline.
            std::optional<baseline> snapshot;
            if (!_baseline) {
                snapshot = baseline{_entry(), owns_source ? _entry.source() : config_source::None};
            }
            try {
                _entry(parsed, owns_source ? config_source::ClusterConfig : config_source::None);
            } catch (...) {
                // The entry rejected the value (allowed_values), or an observer threw after the
                // value was set. Neither fails the pass, so it is not retried; the restore path
                // lets an observer exception propagate so that the retry resets the source.
                if (owns_source) {
                    bridge_logger.error("Cannot apply cluster config override {} = {}: {}", _opt.name, parsed, std::current_exception());
                }
                return;
            }
            if (snapshot) {
                _baseline = std::move(snapshot);
            }
            if (owns_source) {
                bridge_logger.info("Applied cluster config override {} = {} (baseline {} from {})",
                        _opt.name, parsed, _baseline->value, utils::config_source_name(_baseline->source));
            }
        } else if (_baseline) {
            const auto restored = *_baseline;
            if (owns_source) {
                _entry.restore(restored.value, restored.source);
                bridge_logger.info("Removed cluster config override for {}, restored {} from {}",
                        _opt.name, restored.value, utils::config_source_name(restored.source));
            } else {
                _entry(restored.value, config_source::None);
            }
            // Cleared only after the restore: an observer that throws fails the pass, and
            // the retry must restore again to reset the source.
            _baseline.reset();
        }
    }

public:
    db_config_binding(cluster_config_manager& manager, utils::config_file::named_value<T>& entry, const registry_option& opt)
        : _entry(entry)
        , _opt(opt)
        , _registration(manager.register_config_callback(sstring(opt.name),
                [this] (const cluster_config_manager::lookup_context&, std::optional<sstring> value) {
                    on_change(value);
                    return make_ready_future<>();
                })) {
    }
};

}

template <typename T>
void cluster_config_db_config_bridge::bind(cluster_config_manager& manager, utils::config_file::named_value<T>& entry) {
    const auto* opt = cluster_config_registry::find(entry.name());
    if (!opt || opt->kind != cluster_config_registry::option_kind::db_config) {
        utils::on_internal_error(fmt::format("db::config entry '{}' is bound to cluster config but not registered as a db_config option", entry.name()));
    }
    if (entry.get_liveness() != utils::config_file::liveness::LiveUpdate) {
        utils::on_internal_error(fmt::format("db::config entry '{}' is bound to cluster config but is not live-updateable", entry.name()));
    }
    if (opt->type() != registry_type_for<T>()) {
        utils::on_internal_error(fmt::format("cluster config option '{}' is registered with a type that does not match its db::config entry", entry.name()));
    }
    if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
        // Every value ALTER accepts must fit the entry, or an override could wrap when applied.
        // Without a valid_range ALTER accepts any int64_t.
        const auto range = opt->valid_range.value_or(cluster_config_registry::integer_range{
                .min = std::numeric_limits<int64_t>::min(), .max = std::numeric_limits<int64_t>::max()});
        if (std::cmp_less(range.min, std::numeric_limits<T>::min()) || std::cmp_greater(range.max, std::numeric_limits<T>::max())) {
            utils::on_internal_error(fmt::format("cluster config option '{}' needs a valid_range within the range of its db::config entry", entry.name()));
        }
    }
    _bindings.push_back(std::make_unique<db_config_binding<T>>(manager, entry, *opt));
}

cluster_config_db_config_bridge::cluster_config_db_config_bridge(sharded<cluster_config_manager>& manager, db::config& cfg) {
    std::unordered_set<std::string_view> bound;
    auto bind_entry = [&] (auto& entry) {
        bind(manager.local(), entry);
        bound.insert(entry.name());
    };

    bind_entry(cfg.tablets_per_shard_goal);

    // Every db_config option needs a binding above, or an accepted ALTER would do nothing.
    for (const auto& opt : cluster_config_registry::options()) {
        if (opt.kind == cluster_config_registry::option_kind::db_config && !bound.contains(opt.name)) {
            utils::on_internal_error(fmt::format("cluster config option '{}' is registered as db_config but has no binding", opt.name));
        }
    }
}

future<> cluster_config_db_config_bridge::stop() {
    _bindings.clear();
    return make_ready_future<>();
}

}
