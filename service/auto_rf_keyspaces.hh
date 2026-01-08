/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <string_view>

// The system keyspaces whose replication the topology coordinator manages (auto-RF),
// with the number of racks per DC each aims for.
namespace service {

inline constexpr std::string_view audit_keyspace_name = "audit";
inline constexpr size_t audit_rf_goal_per_dc = 3;

inline constexpr std::string_view system_traces_keyspace_name = "system_traces";
inline constexpr size_t system_traces_rf_goal_per_dc = 2;

}
