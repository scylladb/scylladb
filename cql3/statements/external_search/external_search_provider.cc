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

namespace {

// The score of the external result that names `row`, or nothing if `row` has no usable score.
// That is the case in two situations: no external result names the row (its key was not in the
// search's reply), or the result's score is NaN or infinite. Vector Store cannot send Inf over
// JSON and should not send NaN, so the second case is a malformed reply.
std::optional<float> similarity_of(const joined_row& row, const vector_search::vector_store_client::primary_keys& external_results) {
    if (!row.external_result) {
        return std::nullopt;
    }
    const auto similarity = external_results[*row.external_result].similarity;
    return std::isfinite(similarity) ? std::optional(similarity) : std::nullopt;
}

} // anonymous namespace

void drop_unscored_rows(std::span<joined_row> rows, const vector_search::vector_store_client::primary_keys& external_results) {
    for (auto& row : rows) {
        if (!similarity_of(row, external_results)) {
            row.dropped = true;
        }
    }
}

std::vector<cql3::raw_value> similarities_of(
        std::span<const joined_row> rows, const vector_search::vector_store_client::primary_keys& external_results) {
    auto values = std::vector<cql3::raw_value>{};
    values.reserve(rows.size());
    for (const auto& row : rows) {
        const auto similarity = row.dropped ? std::nullopt : similarity_of(row, external_results);
        values.push_back(similarity ? cql3::raw_value::make_value(float_type->decompose(*similarity)) : cql3::raw_value::make_null());
    }
    return values;
}

external_search_provider::external_search_provider(std::vector<external_values> values, std::span<const joined_row> rows)
    : _values(std::move(values)) {
    _dropped.reserve(rows.size());
    for (const auto& row : rows) {
        _dropped.push_back(row.dropped);
    }
}

bool external_search_provider::try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes>, std::span<const bytes>,
        const query::result_row_view&, const query::result_row_view*) const {
    // Advanced for every row offered, dropped ones included: the values were computed for the
    // same rows in the same order.
    const auto row = _next_row++;
    throwing_assert(row < _dropped.size());

    if (_dropped[row]) {
        return false;
    }

    for (const auto& [temporary_index, values] : _values) {
        throwing_assert(row < values.size());
        // Nothing clears a temporary between rows, so every row is given an explicit value.
        temporaries[temporary_index] = values[row];
    }
    return true;
}

} // namespace cql3::statements::external_search
