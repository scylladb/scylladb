/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_search_provider.hh"

#include <cmath>

#include "cql3/expr/expr-utils.hh"
#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/on_internal_error.hh"

#include <seastar/core/format.hh>

#include <algorithm>

namespace cql3::statements {

namespace {

/// Where in the values get_non_pk_values() hands back the given column sits. That vector is aligned
/// with the selection's columns, which is the order the slice asks the replicas for them in too.
size_t column_index_in(const selection::selection& selection, const column_definition& column) {
    const auto& columns = selection.get_columns();
    auto it = std::ranges::find(columns, &column);
    if (it == columns.end()) {
        utils::on_internal_error(seastar::format("column {} is wanted from every row but was not asked for", column.name_as_text()));
    }
    return std::distance(columns.begin(), it);
}

/// The component a key column holds in the key it is part of, or nothing where the key does not
/// reach that far - a prefix, or a key the read was not asked to send back.
template <typename Key>
managed_bytes_opt key_component(const Key& key, const schema& schema, const column_definition& column) {
    auto components = key.explode(schema);
    if (components.size() <= column.component_index()) {
        return std::nullopt;
    }
    return managed_bytes(components[column.component_index()]);
}

/// Walks the rows of a base-table read, joining an external search's answers to them.
///
/// It has to visit exactly the rows result_set_builder::visitor emits, in the same order, since what
/// it produces is handed back out by position. The one rule that does not follow from walking the
/// same result with the same slice - a partition holding nothing but a static row - is repeated below.
class row_joiner {
    const schema& _schema;
    const selection::selection& _selection;

    // The cursor only moves forward, which is what makes an answer stepped over a stale one.
    const vector_search::vector_store_client::primary_keys& _external_results;
    size_t _next_result = 0;

    // What to read out of every row, and where each column sits in the values get_non_pk_values()
    // hands back - unused for a key column, which is read from the key itself.
    std::span<const column_definition* const> _columns;
    std::vector<size_t> _column_indexes;
    bool _reads_non_key_column = false;

    std::vector<joined_row>& _rows;

    partition_key _partition_key = partition_key::make_empty();
    clustering_key_prefix _clustering_key = clustering_key_prefix::make_empty();
    uint64_t _row_count = 0;

    std::optional<size_t> match() {
        while (_next_result < _external_results.size()) {
            const auto& result = _external_results[_next_result++];

            if (!result.partition.key().equal(_schema, _partition_key)) {
                continue;
            }
            if (_schema.clustering_key_size() > 0 && !result.clustering.equal(_schema, _clustering_key)) {
                continue;
            }
            return _next_result - 1;
        }
        return std::nullopt;
    }

    std::vector<managed_bytes_opt> read_columns(const query::result_row_view& static_row, const query::result_row_view* row) const {
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
                values.push_back(key_component(_partition_key, _schema, column));
                break;
            case column_kind::clustering_key:
                values.push_back(key_component(_clustering_key, _schema, column));
                break;
            default:
                values.push_back(std::move(non_key[_column_indexes[i]]));
                break;
            }
        }
        return values;
    }

    void visit(const query::result_row_view& static_row, const query::result_row_view* row) {
        auto answer = match();
        _rows.push_back(joined_row{.answer = answer, .columns = read_columns(static_row, row)});
    }

public:
    row_joiner(const schema& schema, const selection::selection& selection,
            const vector_search::vector_store_client::primary_keys& external_results, std::span<const column_definition* const> columns,
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

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _partition_key = key;
        // Stale from the previous partition, exactly as the builder's visitor treats it.
        _clustering_key = clustering_key_prefix::make_empty();
        _row_count = row_count;
    }

    void accept_new_partition(uint64_t row_count) {
        _clustering_key = clustering_key_prefix::make_empty();
        _row_count = row_count;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
        _clustering_key = key;
        visit(static_row, &row);
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        visit(static_row, &row);
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        if (_row_count == 0) {
            // The builder emits one row for a partition holding only a static row. No answer can
            // name it - the index names rows and this partition has none - so the cursor stays put.
            _rows.push_back(joined_row{.answer = std::nullopt, .columns = read_columns(static_row, nullptr)});
        }
    }
};

} // anonymous namespace

std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const selection::selection& selection, const vector_search::vector_store_client::primary_keys& external_results,
        std::span<const column_definition* const> columns) {
    auto rows = std::vector<joined_row>{};
    query::result_view::consume(table_results, slice, row_joiner(schema, selection, external_results, columns, rows));
    return rows;
}

std::vector<cql3::raw_value> similarities_of(std::span<const joined_row> rows,
        const vector_search::vector_store_client::primary_keys& external_results, std::vector<bool>& dropped) {
    throwing_assert(dropped.size() == rows.size());

    auto values = std::vector<cql3::raw_value>{};
    values.reserve(rows.size());

    for (size_t row = 0; row < rows.size(); ++row) {
        // Vector Store cannot return Inf over its JSON API, and should not return NaN (null in JSON),
        // but if it does then the row has no relevance to report - and neither has a row the index no
        // longer names.
        const auto answer = rows[row].answer;
        if (dropped[row] || !answer || !std::isfinite(external_results[*answer].similarity)) {
            values.push_back(cql3::raw_value::make_null());
            dropped[row] = true;
            continue;
        }
        values.push_back(cql3::raw_value::make_value(float_type->decompose(external_results[*answer].similarity)));
    }
    return values;
}

external_search_provider::external_search_provider(std::vector<external_values> values, std::vector<bool> dropped)
    : _values(std::move(values))
    , _dropped(std::move(dropped)) {
}

bool external_search_provider::try_fill(std::vector<cql3::raw_value>& temporaries) const {
    // Advanced for every row offered, dropped ones included: the values were computed for the same
    // rows in the same order, so the position has to move with them.
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

} // namespace cql3::statements
