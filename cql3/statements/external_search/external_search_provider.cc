/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_search_provider.hh"

#include <cmath>

#include "cql3/expr/expr-utils.hh"
#include "cql3/result_set.hh"
#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <algorithm>

namespace cql3::statements::external_search {

namespace {

/// Index of `column` in the selection's column list, which is also its index in the vector
/// expr::get_non_pk_values() returns.
size_t column_index_in(const selection::selection& selection, const column_definition& column) {
    const auto& columns = selection.get_columns();
    auto it = std::ranges::find(columns, &column);
    throwing_assert(it != columns.end());
    return std::distance(columns.begin(), it);
}

/// A filter for result_set_builder::visitor that records a joined_row for every row the visitor
/// offers it, and rejects the row so that the builder emits nothing. Running the visitor with this
/// filter walks exactly the rows the result set will be built from, in the same order.
class joining_filter {
    const schema& _schema;
    const selection::selection& _selection;

    // The external results to match the rows to, or null when the rows are not matched. The cursor
    // only moves forward: a result stepped over names a row that is gone.
    const vector_search::vector_store_client::primary_keys* _external_results;
    mutable size_t _next_result = 0;

    // The columns to read out of every row. `_column_indexes` gives each one's index in the vector
    // get_non_pk_values() returns; unused for a key column, which is read from the key instead.
    std::span<const column_definition* const> _columns;
    std::vector<size_t> _column_indexes;
    bool _reads_non_key_column = false;

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

    std::vector<managed_bytes_opt> read_columns(const std::vector<bytes>& pk, const std::vector<bytes>& ck,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        if (_columns.empty()) {
            return {};
        }

        auto non_key = _reads_non_key_column ? expr::get_non_pk_values(_selection, static_row, row) : std::vector<managed_bytes_opt>{};
        auto values = std::vector<managed_bytes_opt>{};
        values.reserve(_columns.size());
        for (size_t i = 0; i < _columns.size(); ++i) {
            const auto& column = *_columns[i];
            switch (column.kind) {
            case column_kind::partition_key:
                values.push_back(managed_bytes(pk[column.component_index()]));
                break;
            case column_kind::clustering_key:
                // The clustering key is empty for the row emitted for a partition holding only a
                // static row.
                if (ck.size() > column.component_index()) {
                    values.push_back(managed_bytes(ck[column.component_index()]));
                } else {
                    values.push_back(std::nullopt);
                }
                break;
            default:
                values.push_back(std::move(non_key[_column_indexes[i]]));
                break;
            }
        }
        return values;
    }

public:
    joining_filter(const schema& schema, const selection::selection& selection,
            const vector_search::vector_store_client::primary_keys* external_results, std::span<const column_definition* const> columns,
            std::vector<joined_row>& rows)
        : _schema(schema)
        , _selection(selection)
        , _external_results(external_results)
        , _columns(columns)
        , _rows(rows) {
        _column_indexes.reserve(columns.size());
        for (const auto* column : columns) {
            const auto is_key_column = column->is_primary_key();
            _column_indexes.push_back(is_key_column ? 0 : column_index_in(selection, *column));
            _reads_non_key_column = _reads_non_key_column || !is_key_column;
        }
    }

    // The result_set_builder::visitor filter interface. `row` is null for the row emitted for a
    // partition holding only a static row; no external result can name such a row, so the cursor
    // stays put.
    bool operator()(const selection::selection&, const std::vector<bytes>& pk, const std::vector<bytes>& ck,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        auto external_result = row ? match(pk, ck) : std::nullopt;
        _rows.push_back(joined_row{.external_result = external_result, .columns = read_columns(pk, ck, static_row, row)});
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
        const selection::selection& selection, const vector_search::vector_store_client::primary_keys* external_results,
        std::span<const column_definition* const> columns) {
    auto rows = std::vector<joined_row>{};
    // The filter rejects every row, so the builder builds nothing and is discarded.
    auto builder = selection::result_set_builder(selection, gc_clock::now());
    query::result_view::consume(table_results, slice, selection::result_set_builder::visitor<joining_filter>(
            builder, schema, selection, joining_filter(schema, selection, external_results, columns, rows)));
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
        const auto similarity = similarity_of(row, external_results);
        values.push_back(similarity ? cql3::raw_value::make_value(float_type->decompose(*similarity)) : cql3::raw_value::make_null());
    }
    return values;
}

std::vector<cql3::raw_value> ranks_of(
        std::span<const joined_row> rows, const vector_search::vector_store_client::primary_keys& external_results) {
    auto values = std::vector<cql3::raw_value>{};
    values.reserve(rows.size());
    for (const auto& row : rows) {
        // Same rows as similarities_of(): a row without a usable similarity has no rank either.
        values.push_back(similarity_of(row, external_results)
                        ? cql3::raw_value::make_value(int32_type->decompose(static_cast<int32_t>(*row.external_result + 1)))
                        : cql3::raw_value::make_null());
    }
    return values;
}

std::vector<external_values> search_values_of(const search_temporaries& temporaries, std::span<const joined_row> rows,
        const vector_search::vector_store_client::primary_keys& external_results) {
    std::vector<external_values> values;
    if (temporaries.score) {
        values.push_back({.temporary_index = *temporaries.score, .values = similarities_of(rows, external_results)});
    }
    if (temporaries.rank) {
        values.push_back({.temporary_index = *temporaries.rank, .values = ranks_of(rows, external_results)});
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

bool external_search_provider::try_fill(std::vector<cql3::raw_value>& temporaries) const {
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
