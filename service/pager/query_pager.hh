/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "paging_state.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/selection.hh"
#include "service/query_state.hh"

namespace service {

class storage_proxy_coordinator_query_result;

namespace pager {

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
    std::optional<clustering_key> _last_ckey;

    schema_ptr _schema;
    shared_ptr<const cql3::selection::selection> _selection;
    service::query_state& _state;
    const cql3::query_options& _options;
    lw_shared_ptr<query::read_command> _cmd;
    dht::partition_range_vector _ranges;
    paging_state::replicas_per_token_range _last_replicas;
    std::optional<db::read_repair_decision> _query_read_repair_decision;
    uint64_t _rows_fetched_for_last_partition = 0;
    stats _stats;
public:
    query_pager(schema_ptr s, shared_ptr<const cql3::selection::selection> selection,
                service::query_state& state,
                const cql3::query_options& options,
                lw_shared_ptr<query::read_command> cmd,
                dht::partition_range_vector ranges);
    virtual ~query_pager() {}

    /**
     * Fetches the next page.
     *
     * @param pageSize the maximum number of elements to return in the next page.
     * @return the page of result.
     */
    future<std::unique_ptr<cql3::result_set>> fetch_page(uint32_t page_size, gc_clock::time_point, db::timeout_clock::time_point timeout);

    /**
     * For more than one page.
     */
    virtual future<> fetch_page(cql3::selection::result_set_builder&, uint32_t page_size, gc_clock::time_point, db::timeout_clock::time_point timeout);

    future<cql3::result_generator> fetch_page_generator(uint32_t page_size, gc_clock::time_point now, db::timeout_clock::time_point timeout, cql3::cql_stats& stats);

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
    
    future<service::storage_proxy_coordinator_query_result>
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

