/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

/// Forward declarations and lightweight pointer typedefs for
/// locator/abstract_replication_strategy.hh.
/// Include this instead of abstract_replication_strategy.hh when only
/// pointer types or forward references are needed.

namespace locator {

class abstract_replication_strategy;
class static_effective_replication_map;
class effective_replication_map_factory;

using replication_strategy_ptr = seastar::shared_ptr<const abstract_replication_strategy>;
using mutable_replication_strategy_ptr = seastar::shared_ptr<abstract_replication_strategy>;
using static_effective_replication_map_ptr = seastar::shared_ptr<const static_effective_replication_map>;
using mutable_static_effective_replication_map_ptr = seastar::shared_ptr<static_effective_replication_map>;
using static_erm_ptr = static_effective_replication_map_ptr;
using mutable_static_erm_ptr = mutable_static_effective_replication_map_ptr;

} // namespace locator
