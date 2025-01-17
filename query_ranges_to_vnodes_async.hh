/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "query_ranges_to_vnodes.hh"
#include "dht/i_partitioner_fwd.hh"
#include "service/pager/paging_state.hh"
#include "exceptions/coordinator_result.hh"

#include <seastar/coroutine/generator.hh>

using partition_ranges_generator = coroutine::experimental::generator<exceptions::coordinator_result<std::tuple<dht::partition_range_vector, lw_shared_ptr<const service::pager::paging_state>>>>;

// Asynchronous ranges-to-vnodes generator
//
// Extends the synchronous generator by generating partition range vectors
// dynamically through a given generator coroutine instead of a static vector.
//
// The partition range vector generator executes queries internally, so it may
// also return a paging state and an error, if the query failed.
// The ranges-to-vnodes generator caches these values internally and makes them
// available to the caller via getters.
// An error is indicated to the caller via a non-engaged optional as a return
// value from the call operator.
class query_ranges_to_vnodes_generator_async : private query_ranges_to_vnodes_generator {
    partition_ranges_generator& _gen;
    bool _gen_exhausted;
    exceptions::coordinator_exception_container _error;
    mutable lw_shared_ptr<const service::pager::paging_state> _paging_state;
public:
    query_ranges_to_vnodes_generator_async(std::unique_ptr<locator::token_range_splitter> splitter, schema_ptr s, partition_ranges_generator& gen, bool local = false);
    query_ranges_to_vnodes_generator_async(const query_ranges_to_vnodes_generator_async&) = delete;
    query_ranges_to_vnodes_generator_async(query_ranges_to_vnodes_generator_async&&) = default;
    // generate next 'n' vnodes, may return less than requested number of ranges
    // which means either that there are no more ranges
    // (in which case empty() == true), or too many ranges
    // are requested
    //
    // A non-engaged optional indicates an error from the partition range vector
    // generator. The error can be obtained with `get_error()`.
    //
    // A side-effect of this function is the update of the internal paging state,
    // if the generator was used. The latest paging state is available with
    // `get_paging_state()`.
    future<std::optional<dht::partition_range_vector>> operator()(size_t n);
    future<void> prefetch();
    bool empty() const;
    lw_shared_ptr<const service::pager::paging_state> get_paging_state();
    exceptions::coordinator_exception_container get_error();
};