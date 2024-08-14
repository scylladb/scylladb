/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "paging_state.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/selection.hh"
#include "service/query_state.hh"
#include "exceptions/coordinator_result.hh"

namespace service {

class storage_proxy;
class storage_proxy_coordinator_query_options;
class storage_proxy_coordinator_query_result;

namespace pager {

using query_function = std::function<future<exceptions::coordinator_result<service::storage_proxy_coordinator_query_result>>(
        service::storage_proxy& sp,
        schema_ptr schema,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector&& partition_ranges,
        db::consistency_level cl,
        service::storage_proxy_coordinator_query_options optional_params)>;

/**
 * Perform a query, paging it by page of a given size.
 *
 * This is essentially an iterator of pages. Each call to fetchPage() will
 * return the next page (i.e. the next list of rows) and isExhausted()
 * indicates whether there is more page to fetch. The pageSize will
 * either be in term of cells or in term of CQL3 row, depending on the
 * parameters of the command we page.
 *
 * Please note that the pager might page within rows, so there is no guarantee
 * that successive pages won't return the same row (though with different
 * columns every time).
 *
 * Also, there is no guarantee that fetchPage() won't return an empty list,
 * even if isExhausted() return false (but it is guaranteed to return an empty
 * list *if* isExhausted() return true). Indeed, isExhausted() does *not*
 * trigger a query so in some (fairly rare) case we might not know the paging
 * is done even though it is.
 */
class query_pager {
public:
    template<typename T = void>
    using result = exceptions::coordinator_result<T>;

    struct stats {
        // Total number of rows read by this pager, based on all pages it fetched
        size_t rows_read_total = 0;
    };

protected:
    // remember if we use clustering. if not, each partition == one row
    const bool _has_clustering_keys;
    bool _exhausted = false;
    uint64_t _max;
    uint64_t _per_partition_limit;

    std::optional<partition_key> _last_pkey;
    position_in_partition _last_pos;
    std::optional<query_id> _query_uuid;

    shared_ptr<service::storage_proxy> _proxy;
    schema_ptr _query_schema;
    shared_ptr<const cql3::selection::selection> _selection;
    service::query_state& _state;
    const cql3::query_options& _options;
    lw_shared_ptr<query::read_command> _cmd;
    dht::partition_range_vector _ranges;
    paging_state::replicas_per_token_range _last_replicas;
    std::optional<db::read_repair_decision> _query_read_repair_decision;
    uint64_t _rows_fetched_for_last_partition = 0;
    stats _stats;

    query_function _query_function;

public:
    query_pager(service::storage_proxy& p, schema_ptr query_schema, shared_ptr<const cql3::selection::selection> selection,
                service::query_state& state,
                const cql3::query_options& options,
                lw_shared_ptr<query::read_command> cmd,
                dht::partition_range_vector ranges,
                query_function query_function_override = {});
    virtual ~query_pager() {}

    /**
     * Fetches the next page.
     *
     * @param pageSize the maximum number of elements to return in the next page.
     * @return the page of result.
     */
    future<std::unique_ptr<cql3::result_set>> fetch_page(uint32_t page_size, gc_clock::time_point, db::timeout_clock::time_point timeout);
    future<result<std::unique_ptr<cql3::result_set>>> fetch_page_result(uint32_t page_size, gc_clock::time_point, db::timeout_clock::time_point timeout);

    /**
     * For more than one page.
     */
    future<> fetch_page(cql3::selection::result_set_builder&, uint32_t page_size, gc_clock::time_point, db::timeout_clock::time_point timeout);
    virtual future<result<>> fetch_page_result(cql3::selection::result_set_builder&, uint32_t page_size, gc_clock::time_point, db::timeout_clock::time_point timeout);

    future<cql3::result_generator> fetch_page_generator(uint32_t page_size, gc_clock::time_point now, db::timeout_clock::time_point timeout, cql3::cql_stats& stats);
    future<result<cql3::result_generator>> fetch_page_generator_result(uint32_t page_size, gc_clock::time_point now, db::timeout_clock::time_point timeout, cql3::cql_stats& stats);

    /**
     * Whether or not this pager is exhausted, i.e. whether or not a call to
     * fetchPage may return more result.
     *
     * @return whether the pager is exhausted.
     */
    bool is_exhausted() const {
        return _exhausted;
    }

    /**
     * The maximum number of cells/CQL3 row that we may still have to return.
     * In other words, that's the initial user limit minus what we've already
     * returned (note that it's not how many we *will* return, just the upper
     * limit on it).
     */
    uint64_t max_remaining() const {
        return _max;
    }

    /**
     * Get the current state (snapshot) of the pager. The state can allow to restart the
     * paging on another host from where we are at this point.
     *
     * @return the current paging state. If the pager is exhausted, the result is a valid pointer
     * to a paging_state instance which will return 0 on calling get_remaining() on it.
     */
    lw_shared_ptr<const paging_state> state() const;

    const stats& stats() const {
        return _stats;
    }

protected:
    template<typename Base>
    class query_result_visitor;
    
    future<result<service::storage_proxy_coordinator_query_result>>
    do_fetch_page(uint32_t page_size, gc_clock::time_point now, db::timeout_clock::time_point timeout);

    template<typename Visitor>
    requires query::ResultVisitor<Visitor>
    void handle_result(Visitor&& visitor,
                      const foreign_ptr<lw_shared_ptr<query::result>>& results,
                      uint32_t page_size, gc_clock::time_point now);

    virtual uint64_t max_rows_to_fetch(uint32_t page_size) {
        return std::min(_max, static_cast<uint64_t>(page_size));
    }

    virtual void maybe_adjust_per_partition_limit(uint32_t page_size) const { }
};

}
}

