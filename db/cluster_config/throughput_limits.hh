/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string_view>

#include <seastar/core/sharded.hh>

#include "db/cluster_config/cluster_config_manager.hh"
#include "seastarx.hh"
#include "utils/config_file.hh"
#include "utils/observable.hh"
#include "utils/updateable_value.hh"

namespace db {

class config;

// The effective I/O throughput limits of the local node, in MiB/s, where 0 means unlimited.
//
// Each limit is a node-oriented cluster-config option (resolved node -> rack -> datacenter ->
// cluster). When no scope stores an override for it, the deprecated db::config option of the
// same name applies instead, so existing scylla.yaml settings and the REST API setters keep
// working until the cluster is configured through CQL.
//
// maintenance_io_throughput_mb_per_sec additionally falls back to the effective
// stream_io_throughput_mb_per_sec when it is set neither in cluster config nor in db::config,
// which preserves the legacy behavior of the maintenance supergroup.
//
// One instance per shard. Every shard's values follow the same sources, so the updateable
// values handed out on a shard are only ever updated on that shard.
class throughput_limits {
public:
    explicit throughput_limits(const db::config& cfg);

    // Starts following the cluster-config overrides; until then only the db::config values
    // apply. Call on every shard, and call detach() before the manager stops. After detach()
    // the last cluster-config values stay in effect.
    void attach(cluster_config_manager& ccm);
    void detach() noexcept;

    future<> stop();

    utils::updateable_value<uint32_t> compaction_throughput_mb_per_sec() const;
    utils::updateable_value<uint32_t> stream_io_throughput_mb_per_sec() const;
    utils::updateable_value<uint32_t> maintenance_io_throughput_mb_per_sec() const;
    utils::updateable_value<uint32_t> backup_io_throughput_mb_per_sec() const;

private:
    // Indexes _limits, in the order the constructor initializes it.
    enum class limit_id : size_t {
        compaction,
        stream_io,
        maintenance_io,
        backup_io,
    };
    static constexpr size_t limit_count = 4;

    struct limit {
        const utils::config_file::named_value<uint32_t>& legacy;
        // The value resolved from cluster config, or nullopt when no scope stores one.
        std::optional<uint32_t> cluster_value;
        utils::updateable_value_source<uint32_t> effective;
        utils::observer<uint32_t> legacy_observer = utils::dummy_observer<uint32_t>();
        // Set once the legacy value changes at runtime. named_value notifies its observers
        // before it records the new source, so is_set() alone misses the first such change.
        bool legacy_changed = false;
        cluster_config_manager::config_callback_registration registration;

        explicit limit(const utils::config_file::named_value<uint32_t>& legacy);
        std::string_view name() const;
        bool legacy_is_set() const;
    };

    limit& get(limit_id id);
    const limit& get(limit_id id) const;
    void on_legacy_change(limit& l);
    void on_cluster_change(limit& l, std::optional<uint32_t> cluster_value);
    void update_effective_values();

    std::array<limit, limit_count> _limits;
};

}
