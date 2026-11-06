/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "seastarx.hh"
#include "utils/config_file.hh"

namespace db {

class config;
class cluster_config_manager;

// Applies db::config-backed cluster config options (registry option_kind::db_config) to
// this shard's db::config value with config_source::ClusterConfig. When the option stops
// resolving to an override, the shard's pre-cluster value and source are restored.
//
// One instance per shard, started after the cluster config manager and stopped before it.
// Each shard registers its own callbacks and applies values synchronously on that shard,
// so nothing is in flight when stop() unregisters them. The baseline is captured on the
// first applied override and kept until the override is removed, so a later override does
// not replace it.
class cluster_config_db_config_bridge {
public:
    class binding {
    public:
        virtual ~binding() = default;
    };

    cluster_config_db_config_bridge(sharded<cluster_config_manager>& manager, db::config& cfg);
    future<> stop();

private:
    template <typename T>
    void bind(cluster_config_manager& manager, utils::config_file::named_value<T>& entry);

    std::vector<std::unique_ptr<binding>> _bindings;
};

}
