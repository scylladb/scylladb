/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <map>
#include <variant>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "locator/types.hh"

/// Forward declarations, lightweight pointer typedefs and replication
/// strategy option types for locator/abstract_replication_strategy.hh.
/// Include this instead of abstract_replication_strategy.hh when only
/// these types or forward references are needed.

namespace locator {

class abstract_replication_strategy;
class effective_replication_map;
class static_effective_replication_map;
class effective_replication_map_factory;

using replication_strategy_ptr = seastar::shared_ptr<const abstract_replication_strategy>;
using mutable_replication_strategy_ptr = seastar::shared_ptr<abstract_replication_strategy>;
using effective_replication_map_ptr = seastar::shared_ptr<const effective_replication_map>;
using mutable_effective_replication_map_ptr = seastar::shared_ptr<effective_replication_map>;
using static_effective_replication_map_ptr = seastar::shared_ptr<const static_effective_replication_map>;
using mutable_static_effective_replication_map_ptr = seastar::shared_ptr<static_effective_replication_map>;
using static_erm_ptr = static_effective_replication_map_ptr;
using mutable_static_erm_ptr = mutable_static_effective_replication_map_ptr;

using replication_strategy_config_option = std::variant<seastar::sstring, rack_list>;
using replication_strategy_config_options = std::map<seastar::sstring, replication_strategy_config_option>;

} // namespace locator
