/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sc_storage_proxy.hh"

namespace service {

sc_storage_proxy::sc_storage_proxy(sc_groups_manager& sc_groups, replica::database& db)
    : _sc_groups(sc_groups)
    , _db(db)
{
}

future<sc_operation_result<>> sc_storage_proxy::mutate(schema_ptr schema,
        const dht::token& token,
        mutatation_gen&& mutatation_gen)
{
    (void)_sc_groups;
    (void)_db;
    throw std::runtime_error("not implemented");
}

auto sc_storage_proxy::query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout
    ) -> future<query_result_type>
{
    throw std::runtime_error("not implemented");
}
}
