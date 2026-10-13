/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/cluster_config/throughput_limits.hh"

#include <limits>

#include <fmt/format.h>

#include <seastar/core/smp.hh>

#include "db/cluster_config/cluster_config_registry.hh"
#include "db/config.hh"
#include "utils/log.hh"
#include "utils/on_internal_error.hh"

namespace db {

static logging::logger tllog("throughput_limits");

throughput_limits::limit::limit(const utils::config_file::named_value<uint32_t>& legacy)
        : legacy(legacy)
        , effective(legacy())
{}

std::string_view throughput_limits::limit::name() const {
    return legacy.name();
}

bool throughput_limits::limit::legacy_is_set() const {
    return legacy_changed || legacy.is_set();
}

throughput_limits::throughput_limits(const db::config& cfg)
        : _limits{
            limit(cfg.compaction_throughput_mb_per_sec),
            limit(cfg.stream_io_throughput_mb_per_sec),
            limit(cfg.maintenance_io_throughput_mb_per_sec),
            limit(cfg.backup_io_throughput_mb_per_sec),
        }
{
    for (auto& l : _limits) {
        // The config is the same on every shard, so warn once.
        if (this_shard_id() == 0 && l.legacy.is_set()) {
            tllog.warn("The {} configuration option is deprecated and is used only when the cluster configuration "
                    "does not set it. Use ALTER CLUSTER|DATACENTER|RACK|NODE ... WITH {} = <value> instead",
                    l.name(), l.name());
        }
        l.legacy_observer = l.legacy.observe([this, &l] (const uint32_t&) {
            on_legacy_change(l);
        });
    }
    update_effective_values();
}

void throughput_limits::attach(cluster_config_manager& ccm) {
    for (auto& l : _limits) {
        const auto* opt = cluster_config_registry::find(l.name());
        if (!opt) {
            utils::on_internal_error(fmt::format("cluster config option {} is not registered", l.name()));
        }
        l.registration = ccm.register_config_callback(sstring(l.name()),
                [this, &l, opt] (const cluster_config_manager::lookup_context&, std::optional<sstring> value) {
            // Range-checked when stored, so an out-of-range value can only come from a row
            // written out-of-band; treat it like no override rather than wrapping it around.
            std::optional<uint32_t> cluster_value;
            if (value) {
                const auto v = cluster_config_registry::to_integer(*opt, value);
                if (v < 0 || v > std::numeric_limits<uint32_t>::max()) {
                    tllog.warn("Ignoring out-of-range cluster configuration value {} for {}", v, l.name());
                } else {
                    cluster_value = v;
                }
            }
            on_cluster_change(l, cluster_value);
            return make_ready_future<>();
        });
    }
}

void throughput_limits::detach() noexcept {
    for (auto& l : _limits) {
        l.registration.unregister();
    }
}

future<> throughput_limits::stop() {
    detach();
    return make_ready_future<>();
}

void throughput_limits::on_legacy_change(limit& l) {
    l.legacy_changed = true;
    if (this_shard_id() == 0) {
        if (l.cluster_value) {
            tllog.warn("The deprecated {} configuration option was set to {}, which has no effect while the cluster "
                    "configuration sets it to {}", l.name(), l.legacy(), *l.cluster_value);
        } else {
            tllog.warn("The deprecated {} configuration option was set to {}. Use ALTER CLUSTER|DATACENTER|RACK|NODE "
                    "... WITH {} = <value> instead", l.name(), l.legacy(), l.name());
        }
    }
    update_effective_values();
}

void throughput_limits::on_cluster_change(limit& l, std::optional<uint32_t> cluster_value) {
    if (cluster_value == l.cluster_value) {
        return;
    }
    l.cluster_value = cluster_value;
    if (this_shard_id() == 0) {
        if (cluster_value) {
            tllog.info("Cluster configuration sets {} to {}", l.name(), *cluster_value);
            if (l.legacy_is_set()) {
                tllog.warn("The deprecated {} configuration option ({}) is overridden by the cluster configuration ({})",
                        l.name(), l.legacy(), *cluster_value);
            }
        } else {
            tllog.info("Cluster configuration no longer sets {}, using the {} configuration option ({})",
                    l.name(), l.legacy_is_set() ? "deprecated" : "default", l.legacy());
        }
    }
    update_effective_values();
}

void throughput_limits::update_effective_values() {
    for (auto id : {limit_id::compaction, limit_id::stream_io, limit_id::backup_io}) {
        auto& l = get(id);
        l.effective.set(l.cluster_value.value_or(l.legacy()));
    }
    // The maintenance supergroup used stream_io_throughput_mb_per_sec unless
    // maintenance_io_throughput_mb_per_sec was explicitly set, so a maintenance limit set
    // neither here nor there follows the effective streaming limit.
    auto& maintenance = get(limit_id::maintenance_io);
    if (maintenance.cluster_value) {
        maintenance.effective.set(*maintenance.cluster_value);
    } else if (maintenance.legacy_is_set()) {
        maintenance.effective.set(maintenance.legacy());
    } else {
        maintenance.effective.set(get(limit_id::stream_io).effective());
    }
}

throughput_limits::limit& throughput_limits::get(limit_id id) {
    return _limits[size_t(id)];
}

const throughput_limits::limit& throughput_limits::get(limit_id id) const {
    return _limits[size_t(id)];
}

utils::updateable_value<uint32_t> throughput_limits::compaction_throughput_mb_per_sec() const {
    return utils::updateable_value<uint32_t>(get(limit_id::compaction).effective);
}

utils::updateable_value<uint32_t> throughput_limits::stream_io_throughput_mb_per_sec() const {
    return utils::updateable_value<uint32_t>(get(limit_id::stream_io).effective);
}

utils::updateable_value<uint32_t> throughput_limits::maintenance_io_throughput_mb_per_sec() const {
    return utils::updateable_value<uint32_t>(get(limit_id::maintenance_io).effective);
}

utils::updateable_value<uint32_t> throughput_limits::backup_io_throughput_mb_per_sec() const {
    return utils::updateable_value<uint32_t>(get(limit_id::backup_io).effective);
}

}
