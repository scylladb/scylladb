/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <vector>
#include <seastar/core/shared_ptr.hh>

#include "schema/schema_fwd.hh"
#include "query-result.hh"
#include "query-request.hh"
#include "service/query_state.hh"
#include "cql3/selection/selection.hh"
#include "cql3/query_options.hh"
#include "query_pager.hh"

namespace service {

class storage_proxy;

namespace pager {

class query_pagers {
public:
    static bool may_need_paging(const schema& s, uint32_t page_size, const query::read_command&,
            const dht::partition_range_vector&);
    static std::unique_ptr<query_pager> pager(service::storage_proxy& p, schema_ptr,
            shared_ptr<const cql3::selection::selection>,
            service::query_state&,
            const cql3::query_options&,
            lw_shared_ptr<query::read_command>,
            dht::partition_range_vector,
            ::shared_ptr<const cql3::restrictions::statement_restrictions> filtering_restrictions = nullptr,
            query_function query_function_override = {});
    static ::shared_ptr<query_pager> ghost_row_deleting_pager(schema_ptr,
            shared_ptr<const cql3::selection::selection>,
            service::query_state&,
            const cql3::query_options&,
            lw_shared_ptr<query::read_command>,
            dht::partition_range_vector,
            cql3::cql_stats& stats,
            storage_proxy& proxy,
            db::timeout_clock::duration timeout_duration);
};

}
}
