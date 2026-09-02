/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "external_search_provider.hh"

#include <cmath>

#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

namespace cql3::statements {

namespace {

/// Walks the rows of a base-table read, joining an external search's answers to them.
///
/// It has to visit exactly the rows result_set_builder::visitor emits, in the same order, since what
/// it produces is handed back out by position. The one rule that does not follow from walking the
/// same result with the same slice - a partition holding nothing but a static row - is repeated below.
class row_joiner {
    const schema& _schema;

    // The cursor only moves forward, which is what makes an answer stepped over a stale one.
    const vector_search::vector_store_client::primary_keys& _external_results;
    size_t _next_result = 0;

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

    void visit() {
        _rows.push_back(joined_row{.answer = match()});
    }

public:
    row_joiner(const schema& schema, const vector_search::vector_store_client::primary_keys& external_results, std::vector<joined_row>& rows)
        : _schema(schema)
        , _external_results(external_results)
        , _rows(rows) {
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

    void accept_new_row(const clustering_key& key, const query::result_row_view&, const query::result_row_view&) {
        _clustering_key = key;
        visit();
    }

    void accept_new_row(const query::result_row_view&, const query::result_row_view&) {
        visit();
    }

    void accept_partition_end(const query::result_row_view&) {
        if (_row_count == 0) {
            // The builder emits one row for a partition holding only a static row. No answer can
            // name it - the index names rows and this partition has none - so the cursor stays put.
            _rows.push_back(joined_row{.answer = std::nullopt});
        }
    }
};

} // anonymous namespace

std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const vector_search::vector_store_client::primary_keys& external_results) {
    auto rows = std::vector<joined_row>{};
    query::result_view::consume(table_results, slice, row_joiner(schema, external_results, rows));
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
