/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "index/secondary_index_manager.hh"
#include "replica/database.hh"

#include <seastar/core/lowres_clock.hh>

namespace cql3::statements {

template <typename Func>
auto measure_index_latency(const schema& schema, const secondary_index::index& index, Func&& func) -> std::invoke_result_t<Func> {
    auto start_time = lowres_system_clock::now();
    auto result = co_await func();
    auto duration = lowres_system_clock::now() - start_time;

    auto stats = schema.table().get_index_manager().get_index_stats(index.metadata().name());
    if (stats) {
        stats->add_latency(duration);
    }

    co_return result;
}

} // namespace cql3::statements
