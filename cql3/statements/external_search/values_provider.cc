/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "values_provider.hh"

#include <cmath>

#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

namespace cql3::statements::external_search {

namespace {

/// Matches each row the result set will be built from to the external result that names it.
///
/// Which rows those are, and in what order, is decided by the walk of the base-table read: one
/// row per clustered row, plus one for a partition holding nothing but a static row. This visitor
/// makes that same walk, so the joined rows are exactly the rows of the result set, in order.
class joining_visitor {
    const schema& _schema;

    // The external results to match the rows to, or null when the rows are not matched.
    const vector_search::vector_store_client::primary_keys* _external_results;

    // The results naming rows of the partition being walked, settled when it opens while its key
    // is in hand, so the key itself need not be kept.
    size_t _next_result = 0;
    size_t _partition_end = 0;

    uint64_t _rows_in_partition = 0;

    std::vector<joined_row> _rows;

    // Steps over this partition's results whose row the base table no longer has.
    std::optional<size_t> match(const clustering_key_prefix& row_ck) {
        for (; _next_result < _partition_end; ++_next_result) {
            if (_schema.clustering_key_size() == 0 || (*_external_results)[_next_result].clustering.equal(_schema, row_ck)) {
                return _next_result++;
            }
        }
        return std::nullopt;
    }

public:
    joining_visitor(const schema& schema, const vector_search::vector_store_client::primary_keys* external_results)
        : _schema(schema)
        , _external_results(external_results) {
    }

    std::vector<joined_row> rows() && {
        return std::move(_rows);
    }

    // query::ResultVisitor, called by query::result_view::consume().

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _rows_in_partition = row_count;
        _partition_end = _next_result;
        if (!_external_results || row_count == 0) {
            // The index names rows, so nothing can name a partition that has none. Claiming no
            // results here is what leaves them for the partitions that follow.
            return;
        }
        // The rows arrive in the order of the results, and the read gives a partition its own
        // entry per contiguous run of results naming it, so this entry's run begins at the cursor.
        const auto& results = *_external_results;
        const auto names_this_partition = [&] (size_t i) { return results[i].partition.key().equal(_schema, key); };
        while (_next_result < results.size() && !names_this_partition(_next_result)) {
            ++_next_result;
        }
        for (_partition_end = _next_result; _partition_end < results.size() && names_this_partition(_partition_end); ++_partition_end) {
        }
    }

    void accept_new_partition(uint64_t row_count) {
        // Called when the slice left out the partition key, which matching compares.
        throwing_assert(!_external_results);
        _rows_in_partition = row_count;
        _partition_end = _next_result;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view&, const query::result_row_view&) {
        _rows.push_back(joined_row{.external_result = match(key)});
    }

    void accept_new_row(const query::result_row_view&, const query::result_row_view&) {
        // Called when the slice left out the clustering key, which matching compares unless the
        // table has none.
        throwing_assert(!_external_results || _schema.clustering_key_size() == 0);
        _rows.push_back(joined_row{.external_result = match(clustering_key_prefix::make_empty())});
    }

    void accept_partition_end(const query::result_row_view&) {
        if (_rows_in_partition == 0) {
            _rows.push_back(joined_row{});
        }
    }
};

} // anonymous namespace

std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const vector_search::vector_store_client::primary_keys* external_results) {
    auto visitor = joining_visitor(schema, external_results);
    query::result_view::consume(table_results, slice, visitor);
    return std::move(visitor).rows();
}

values_provider::values_provider(const vector_search::vector_store_client::primary_keys& results, size_t score_slot,
        const schema& schema)
    : _results(results)
    , _next_result(0)
    , _score_slot(score_slot)
    , _schema(schema) {
}

bool values_provider::try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key,
        std::span<const bytes> clustering_key, const query::result_row_view&, const query::result_row_view*) const {
    const auto row_pk = ::partition_key::from_range(partition_key);
    const auto row_ck = (_schema.clustering_key_size() > 0) ? ::clustering_key_prefix::from_range(clustering_key) : ::clustering_key_prefix{};

    // Base-table results are merged in Vector Store primary-key order by
    // external_index_select_statement. Consume the matching score in that order,
    // passing over results with no matching row - the index may be stale and
    // return keys of rows that are no longer in the base table.
    while (_next_result < _results.size()) {
        const auto& vs_result = _results[_next_result];

        if (!vs_result.partition.key().equal(_schema, row_pk)) {
            ++_next_result;
            continue;
        }

        if (_schema.clustering_key_size() > 0) {
            if (!vs_result.clustering.equal(_schema, row_ck)) {
                ++_next_result;
                continue;
            }
        }

        float score = vs_result.similarity;
        ++_next_result;

        // Vector store can't return Inf over JSON API.
        // It also shouldn't return NaN (null in JSON),
        // but if it does, we treat it as an error and skip the row.
        if (!std::isfinite(score)) {
            return false;
        }

        temporaries[_score_slot] = cql3::raw_value::make_value(float_type->decompose(score));
        return true;
    }

    return false;
}

} // namespace cql3::statements::external_search
