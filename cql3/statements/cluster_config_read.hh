/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <iterator>
#include <map>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/consistency_level_type.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "service_permit.hh"
#include "types/types.hh"

namespace cql3::statements {

// Reads and parses the `configs` map<text,text> column for a single
// cluster-config row. `cql` is a "SELECT configs FROM ..." query and `params`
// binds its WHERE clause. A missing row or NULL/absent configs column yields an
// empty map. Centralizing this keeps every scope reader (and ALTER TABLE's
// read-modify-write) in agreement about what "no stored overrides" means.
//
// `params` is taken by value so it outlives the internal query: query_data_params
// is a non-owning span, so the bound values must stay alive across the co_await.
inline seastar::future<std::map<seastar::sstring, seastar::sstring>>
read_configs_column(query_processor& qp, seastar::sstring cql, std::vector<data_value_or_unset> params) {
    auto& internal_client_state = service::client_state::for_internal_calls();
    service::query_state internal_query_state(internal_client_state, empty_service_permit());
    auto rows = co_await qp.execute_internal(
            cql,
            db::consistency_level::ONE,
            internal_query_state,
            query_data_params(params),
            query_processor::cache_internal::yes);

    std::map<seastar::sstring, seastar::sstring> configs;
    if (!rows->empty() && rows->one().has("configs")) {
        rows->one().get_map_data<seastar::sstring, seastar::sstring>("configs", std::inserter(configs, configs.end()));
    }
    co_return configs;
}

}
