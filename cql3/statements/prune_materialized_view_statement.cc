/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

/* Copyright 2022-present ScyllaDB */

#include "cql3/statements/prune_materialized_view_statement.hh"
#include "transport/messages/result_message.hh"
#include "cql3/selection/selection.hh"
#include "service/pager/query_pagers.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include <boost/range/adaptors.hpp>
#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals;

namespace cql3 {

namespace statements {

static future<> delete_ghost_rows(dht::partition_range_vector partition_ranges, std::vector<query::clustering_range> clustering_bounds, view_ptr view,
        service::storage_proxy& proxy, service::query_state& state, const query_options& options, cql_stats& stats, db::timeout_clock::duration timeout_duration) {
    auto key_columns = boost::copy_range<std::vector<const column_definition*>>(
        view->all_columns()
        | boost::adaptors::filtered([] (const column_definition& cdef) { return cdef.is_primary_key(); })
        | boost::adaptors::transformed([] (const column_definition& cdef) { return &cdef; } ));
    auto selection = cql3::selection::selection::for_columns(view, key_columns);

    query::partition_slice partition_slice(std::move(clustering_bounds), {},  {}, selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(view->id(), view->version(), partition_slice, proxy.get_max_result_size(partition_slice),
            query::tombstone_limit(proxy.get_tombstone_limit()));

    tracing::trace(state.get_trace_state(), "Deleting ghost rows from partition ranges {}", partition_ranges);

    auto p = service::pager::query_pagers::ghost_row_deleting_pager(schema_ptr(view), selection, state,
            options, std::move(command), std::move(partition_ranges), stats, proxy, timeout_duration);

    int32_t page_size = std::max(options.get_page_size(), 1000);
    auto now = gc_clock::now();

    while (!p->is_exhausted()) {
        tracing::trace(state.get_trace_state(), "Fetching a page for ghost row deletion");
        auto timeout = db::timeout_clock::now() + timeout_duration;
        cql3::selection::result_set_builder builder(*selection, now);
        co_await p->fetch_page(builder, page_size, now, timeout);
    }
}

future<::shared_ptr<cql_transport::messages::result_message>> prune_materialized_view_statement::do_execute(query_processor& qp,
                                                                                 service::query_state& state, const query_options& options) const {
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());

    if (_restrictions->need_filtering()) {
        throw exceptions::invalid_request_exception("Deleting ghost rows does not support filtering");
    }
    if (!_schema->is_view()) {
        throw exceptions::invalid_request_exception("Ghost rows can only be deleted from materialized views");
    }

    auto timeout_duration = get_timeout(state.get_client_state(), options);
    dht::partition_range_vector key_ranges = _restrictions->get_partition_key_ranges(options);
    std::vector<query::clustering_range> clustering_bounds = _restrictions->get_clustering_bounds(options);
    return delete_ghost_rows(std::move(key_ranges), std::move(clustering_bounds), view_ptr(_schema), qp.proxy(), state, options, _stats, timeout_duration).then([] {
        return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(::make_shared<cql_transport::messages::result_message::void_message>());
    });
}

}

}
