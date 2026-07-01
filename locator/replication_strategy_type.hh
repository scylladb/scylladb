/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <fmt/core.h>

namespace locator {

enum class replication_strategy_type {
    simple,
    local,
    network_topology,
    everywhere_topology,
};

} // namespace locator

template <>
struct fmt::formatter<locator::replication_strategy_type> : fmt::formatter<string_view> {
    auto format(locator::replication_strategy_type, fmt::format_context& ctx) const -> decltype(ctx.out());
};
