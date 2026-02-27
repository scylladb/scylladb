/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "mutation/mutation.hh"
#include "query/query-result.hh"

namespace service::strong_consistency {

class groups_manager;

struct need_redirect {
    locator::tablet_replica target;
};
template <typename T = std::monostate>
using value_or_redirect = std::variant<T, need_redirect>;

class coordinator : public peering_sharded_service<coordinator> {
    groups_manager& _groups_manager;
    replica::database& _db;

    struct operation_ctx;
    future<value_or_redirect<operation_ctx>> create_operation_ctx(const schema& schema, const dht::token& token);
public:
    coordinator(groups_manager& groups_manager, replica::database& db);

    using mutation_gen = noncopyable_function<mutation(api::timestamp_type)>;
    future<value_or_redirect<>> mutate(schema_ptr schema, 
        const dht::token& token,
        mutation_gen&& mutation_gen);

    using query_result_type = value_or_redirect<lw_shared_ptr<query::result>>;
    future<query_result_type> query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout);
};

}
