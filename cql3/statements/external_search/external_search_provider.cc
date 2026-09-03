/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_search_provider.hh"

#include <cmath>

#include "cql3/result_set.hh"
#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

namespace cql3::statements::external_search {

namespace {

/// A filter for result_set_builder::visitor that records a joined_row for every row the visitor
/// offers it, and rejects the row so that the builder emits nothing. Running the visitor with this
/// filter walks exactly the rows the result set will be built from, in the same order.
class joining_filter {
    const schema& _schema;

    // The external results to match the rows to, or null when the rows are not matched. The cursor
    // only moves forward: a result stepped over names a row that is gone.
    const vector_search::vector_store_client::primary_keys* _external_results;
    mutable size_t _next_result = 0;

    std::vector<joined_row>& _rows;

    // Whether the external result names the row with the given key.
    bool names(const vector_search::primary_key& result, const partition_key& row_pk, const clustering_key_prefix& row_ck) const {
        return result.partition.key().equal(_schema, row_pk)
                && (_schema.clustering_key_size() == 0 || result.clustering.equal(_schema, row_ck));
    }

    std::optional<size_t> match(const std::vector<bytes>& pk, const std::vector<bytes>& ck) const {
        if (!_external_results) {
            return std::nullopt;
        }
        const auto row_pk = partition_key::from_range(pk);
        const auto row_ck = _schema.clustering_key_size() > 0 ? clustering_key_prefix::from_range(ck) : clustering_key_prefix::make_empty();
        for (; _next_result < _external_results->size(); ++_next_result) {
            if (names((*_external_results)[_next_result], row_pk, row_ck)) {
                return _next_result++;
            }
        }
        return std::nullopt;
    }

public:
    joining_filter(const schema& schema, const vector_search::vector_store_client::primary_keys* external_results, std::vector<joined_row>& rows)
        : _schema(schema)
        , _external_results(external_results)
        , _rows(rows) {
    }

    // The result_set_builder::visitor filter interface. `row` is null for the row emitted for a
    // partition holding only a static row; no external result can name such a row, so the cursor
    // stays put.
    bool operator()(const selection::selection&, const std::vector<bytes>& pk, const std::vector<bytes>& ck,
            const query::result_row_view&, const query::result_row_view* row) const {
        _rows.push_back(joined_row{.external_result = row ? match(pk, ck) : std::nullopt});
        return false;
    }

    void reset(const partition_key* = nullptr) {
    }

    uint64_t get_rows_dropped() const {
        return 0;
    }
};

} // anonymous namespace

std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const selection::selection& selection, const vector_search::vector_store_client::primary_keys* external_results) {
    auto rows = std::vector<joined_row>{};
    // The filter rejects every row, so the builder builds nothing and is discarded.
    auto builder = selection::result_set_builder(selection, gc_clock::now());
    query::result_view::consume(table_results, slice, selection::result_set_builder::visitor<joining_filter>(
            builder, schema, selection, joining_filter(schema, external_results, rows)));
    return rows;
}

external_search_provider::external_search_provider(const vector_search::vector_store_client::primary_keys& results, size_t score_slot,
        const schema& schema)
    : _results(results)
    , _next_result(0)
    , _score_slot(score_slot)
    , _schema(schema) {
}

bool external_search_provider::try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key,
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
