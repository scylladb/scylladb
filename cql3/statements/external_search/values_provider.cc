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

// The score an external result gave, or nothing if it is not one to report. JSON carries no literal
// for an infinity, but the score is parsed into a float, so a magnitude too large for one arrives
// as one; a NaN would mean a malformed reply. Neither is a score anything can be told.
std::optional<float> similarity_at(const vector_search::vector_store_client::primary_keys& external_results, size_t result) {
    const auto similarity = external_results[result].similarity;
    return std::isfinite(similarity) ? std::optional(similarity) : std::nullopt;
}

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

    // Whether the result matched to a row has a similarity to report; see joined_row::dropped. A
    // row no result names has none. Nothing is dropped when the rows are not matched.
    bool has_similarity(std::optional<size_t> result) const {
        if (!_external_results) {
            return true;
        }
        return result && similarity_at(*_external_results, *result).has_value();
    }

    void add_row(std::optional<size_t> result) {
        _rows.push_back(joined_row{.external_result = result, .dropped = !has_similarity(result)});
    }

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
        add_row(match(key));
    }

    void accept_new_row(const query::result_row_view&, const query::result_row_view&) {
        // Called when the slice left out the clustering key, which matching compares unless the
        // table has none.
        throwing_assert(!_external_results || _schema.clustering_key_size() == 0);
        add_row(match(clustering_key_prefix::make_empty()));
    }

    void accept_partition_end(const query::result_row_view&) {
        if (_rows_in_partition == 0) {
            add_row(std::nullopt);
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

namespace {

// The score of the external result that names `row`, or nothing if it has none to report: either no
// result names the row, its key not having been in the search's reply, or the score is not one
// similarity_at() gives back. These are the rows the join marked dropped.
std::optional<float> similarity_of(const joined_row& row, const vector_search::vector_store_client::primary_keys& external_results) {
    if (!row.external_result) {
        return std::nullopt;
    }
    return similarity_at(external_results, *row.external_result);
}

} // anonymous namespace

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

values_provider::values_provider(std::vector<external_values> values, std::span<const joined_row> rows)
    : _values(std::move(values)) {
    _dropped.reserve(rows.size());
    for (const auto& row : rows) {
        _dropped.push_back(row.dropped);
    }
}

bool values_provider::try_fill(std::vector<cql3::raw_value>& temporaries) const {
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
