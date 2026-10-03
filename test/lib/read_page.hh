/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "replica/read_page.hh"

namespace tests {

namespace detail {

struct plain_page_context {
    std::monostate start_page() {
        return {};
    }
    future<> before_new_querier(const dht::partition_range&) {
        return make_ready_future<>();
    }
};

struct plain_data_page_context : plain_page_context {
    query::result_memory_accounter accounter;

    future<query::result_memory_accounter> make_accounter() {
        return make_ready_future<query::result_memory_accounter>(std::move(accounter));
    }
    future<> before_read() {
        return make_ready_future<>();
    }
};

} // namespace detail

/// See replica::read_data_page(). The caller creates `accounter`.
inline future<lw_shared_ptr<query::result>> read_data_page(mutation_source source,
        schema_ptr query_schema,
        reader_permit permit,
        const query::read_command& cmd,
        query::result_options opts,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        query::result_memory_accounter accounter,
        tombstone_gc_state gc_state,
        replica::querier_base::querier_config config,
        std::optional<replica::querier>* saved_querier) {
    return replica::read_data_page(detail::plain_data_page_context{{}, std::move(accounter)}, std::move(source), std::move(query_schema),
            std::move(permit), cmd, opts, ranges, std::move(trace_state), std::move(gc_state), std::move(config), saved_querier);
}

/// See replica::read_mutation_page().
inline future<reconcilable_result> read_mutation_page(mutation_source source,
        schema_ptr query_schema,
        reader_permit permit,
        const query::read_command& cmd,
        const dht::partition_range& range,
        tracing::trace_state_ptr trace_state,
        query::result_memory_accounter accounter,
        tombstone_gc_state gc_state,
        replica::querier_base::querier_config config,
        std::optional<replica::querier>* saved_querier) {
    return replica::read_mutation_page(detail::plain_page_context{}, std::move(source), std::move(query_schema), std::move(permit), cmd, range,
            std::move(trace_state), std::move(accounter), std::move(gc_state), std::move(config), saved_querier);
}

} // namespace tests
