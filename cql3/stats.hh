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

#include "cql3/statements/statement_type.hh"

namespace cql3 {

/** Enums for selecting counters in `cql_stats', like:
 *      stats.query_cnt(source_selector::USER, ks_selector::NONSYSTEM, cond_selector::NO_CONDITIONS, statement_type::INSERT)
 */
enum class source_selector : size_t {
    INTERNAL = 0u,
    USER,

    SIZE  // Keep me as the last entry
};
enum class ks_selector : size_t {
    SYSTEM = 0u,
    NONSYSTEM,

    SIZE  // Keep me as the last entry
};
enum class cond_selector : size_t {
    NO_CONDITIONS = 0u,
    WITH_CONDITIONS,

    SIZE  // Keep me as the last entry
};

// Shard-local CQL statistics
// @sa cql3/query_processor.cc explains the meaning of each counter
struct cql_stats {
    uint64_t& query_cnt(source_selector ss, ks_selector kss, cond_selector cs, statements::statement_type st) {
        return _query_cnt[(size_t)ss][(size_t)kss][(size_t)cs][(size_t)st];
    }
    const uint64_t& query_cnt(source_selector ss, ks_selector kss, cond_selector cs, statements::statement_type st) const {
        return _query_cnt[(size_t)ss][(size_t)kss][(size_t)cs][(size_t)st];
    }

    uint64_t& unpaged_select_queries(ks_selector kss) {
        return _unpaged_select_queries[(size_t)kss];
    }
    const uint64_t& unpaged_select_queries(ks_selector kss) const {
        return _unpaged_select_queries[(size_t)kss];
    }

    uint64_t batches = 0;
    uint64_t cas_batches = 0;
    uint64_t statements_in_batches = 0;
    uint64_t statements_in_cas_batches = 0;
    uint64_t batches_pure_logged = 0;
    uint64_t batches_pure_unlogged = 0;
    uint64_t batches_unlogged_from_logged = 0;
    uint64_t rows_read = 0;
    uint64_t reverse_queries = 0;

    int64_t secondary_index_creates = 0;
    int64_t secondary_index_drops = 0;
    int64_t secondary_index_reads = 0;
    int64_t secondary_index_rows_read = 0;

    int64_t filtered_reads = 0;
    int64_t filtered_rows_matched_total = 0;
    int64_t filtered_rows_read_total = 0;

    int64_t select_bypass_caches = 0;
    int64_t select_allow_filtering = 0;
    int64_t select_partition_range_scan = 0;
    int64_t select_partition_range_scan_no_bypass_cache = 0;

private:
    uint64_t _unpaged_select_queries[(size_t)ks_selector::SIZE] = {0ul};
    uint64_t _query_cnt[(size_t)source_selector::SIZE]
            [(size_t)ks_selector::SIZE]
            [(size_t)cond_selector::SIZE]
            [statements::statement_type::MAX_VALUE + 1] = {0ul};
};

}
