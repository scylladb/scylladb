/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "coordinator.hh"

namespace service::strong_consistency {

coordinator::coordinator(groups_manager& groups_manager, replica::database& db)
    : _groups_manager(groups_manager)
    , _db(db)
{
}

future<value_or_redirect<>> coordinator::mutate(schema_ptr schema,
        const dht::token& token,
        mutation_gen&& mutation_gen)
{
    (void)_groups_manager;
    (void)_db;
    throw std::runtime_error("not implemented");
}

auto coordinator::query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout
    ) -> future<query_result_type>
{
    (void)_groups_manager;
    (void)_db;
    throw std::runtime_error("not implemented");
}

}
