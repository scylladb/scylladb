/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_search_provider.hh"

#include "cql3/expr/expr-utils.hh"
#include "cql3/result_set.hh"
#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <algorithm>
#include <map>
#include <ranges>

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

using vector_search::serialized_primary_key;
using vector_search::serialize_primary_key;

/// Where each candidate sits, by the key it names. The candidates name distinct keys, the join
/// having merged the searches' answers.
std::map<serialized_primary_key, size_t> index_by_key(const std::vector<vector_search::hybrid_candidate>& candidates, const schema& schema) {
    auto by_key = std::map<serialized_primary_key, size_t>{};
    for (size_t i = 0; i < candidates.size(); ++i) {
        by_key.emplace(serialize_primary_key(schema, candidates[i].partition.key(), candidates[i].clustering), i);
    }
    return by_key;
}

/// A filter for result_set_builder::visitor that records a joined_row for every row the visitor
/// offers it, and rejects the row so that the builder emits nothing. Running the visitor with this
/// filter walks exactly the rows the result set will be built from, in the same order.
class joining_filter {
    const schema& _schema;
    const selection::selection& _selection;

    // Nothing when the rows are not matched.
    std::optional<std::map<serialized_primary_key, size_t>> _candidates_by_key;

    // The columns to read out of every row. `_column_indexes` gives each one's index in the vector
    // get_non_pk_values() returns; unused for a key column, which is read from the key instead.
    std::span<const column_definition* const> _columns;
    std::vector<size_t> _column_indexes;
    bool _reads_non_key_column = false;

    std::vector<joined_row>& _rows;


    std::optional<size_t> match(const std::vector<bytes>& pk, const std::vector<bytes>& ck) const {
        if (!_candidates_by_key) {
            return std::nullopt;
        }
        const auto key = serialize_primary_key(_schema, partition_key::from_range(pk), clustering_key_prefix::from_range(ck));
        auto it = _candidates_by_key->find(key);
        return it != _candidates_by_key->end() ? std::optional<size_t>(it->second) : std::nullopt;
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
            const std::vector<vector_search::hybrid_candidate>* candidates, std::span<const column_definition* const> columns,
            std::vector<joined_row>& rows)
        : _schema(schema)
        , _selection(selection)
        , _candidates_by_key(candidates ? std::optional(index_by_key(*candidates, schema)) : std::nullopt)
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
    // partition holding only a static row; no candidate can name such a row.
    bool operator()(const selection::selection&, const std::vector<bytes>& pk, const std::vector<bytes>& ck,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        _rows.push_back(joined_row{.candidate = row ? match(pk, ck) : std::nullopt, .columns = read_columns(pk, ck, static_row, row)});
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
        const selection::selection& selection, const std::vector<vector_search::hybrid_candidate>* candidates,
        std::span<const column_definition* const> columns) {
    auto rows = std::vector<joined_row>{};
    // The filter rejects every row, so the builder builds nothing and is discarded.
    auto builder = selection::result_set_builder(selection, gc_clock::now());
    query::result_view::consume(table_results, slice, selection::result_set_builder::visitor<joining_filter>(
            builder, schema, selection, joining_filter(schema, selection, candidates, columns, rows)));
    return rows;
}

namespace {

// What `search` said about `row`, or nothing: no candidate names the row (it is the pseudo-row of a
// partition holding only a static row), or that search did not hit its candidate.
std::optional<vector_search::search_hit> hit_of(
        const joined_row& row, size_t search, const std::vector<vector_search::hybrid_candidate>& candidates) {
    if (!row.candidate) {
        return std::nullopt;
    }
    return candidates[*row.candidate].hits[search];
}

} // anonymous namespace

void drop_unscored_rows(std::span<joined_row> rows, const std::vector<vector_search::hybrid_candidate>* candidates) {
    if (!candidates) {
        return;
    }
    for (auto& row : rows) {
        const auto scored = row.candidate && std::ranges::any_of((*candidates)[*row.candidate].hits, [] (const auto& hit) {
            return hit.has_value();
        });
        if (!scored) {
            row.dropped = true;
        }
    }
}

std::vector<cql3::raw_value> scores_of(
        std::span<const joined_row> rows, size_t search, const std::vector<vector_search::hybrid_candidate>& candidates) {
    auto values = std::vector<cql3::raw_value>{};
    values.reserve(rows.size());
    for (const auto& row : rows) {
        const auto hit = hit_of(row, search, candidates);
        values.push_back(hit ? cql3::raw_value::make_value(float_type->decompose(hit->score)) : cql3::raw_value::make_null());
    }
    return values;
}

std::vector<cql3::raw_value> ranks_of(
        std::span<const joined_row> rows, size_t search, const std::vector<vector_search::hybrid_candidate>& candidates) {
    auto values = std::vector<cql3::raw_value>{};
    values.reserve(rows.size());
    for (const auto& row : rows) {
        const auto hit = hit_of(row, search, candidates);
        values.push_back(hit ? cql3::raw_value::make_value(int32_type->decompose(static_cast<int32_t>(hit->rank))) : cql3::raw_value::make_null());
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
