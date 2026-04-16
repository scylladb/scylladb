/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

namespace locator {

enum class replication_strategy_type {
    simple,
    local,
    network_topology,
    everywhere_topology,
};

} // namespace locator
