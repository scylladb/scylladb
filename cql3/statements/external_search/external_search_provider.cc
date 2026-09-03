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

} // namespace cql3::statements
