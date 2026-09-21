/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "values_provider.hh"

#include <algorithm>

#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

namespace cql3::statements::external_search {

namespace {

/// Matches each row the result set will be built from to the candidate that names it.
///
/// Which rows those are, and in what order, is decided by the walk of the base-table read: one
/// row per clustered row, plus one for a partition holding nothing but a static row. This visitor
/// makes that same walk, so the joined rows are exactly the rows of the result set, in order.
class joining_visitor {
    const schema& _schema;

    // The candidates to match the rows to, or null when the rows are not matched. One list serves
    // every search: their answers were joined by key before the read.
    const std::vector<vector_search::search_candidate>* _candidates;

    // The candidates naming rows of the partition being walked, settled when it opens while its key
    // is in hand, so the key itself need not be kept.
    size_t _next_result = 0;
    size_t _partition_end = 0;

    uint64_t _rows_in_partition = 0;

    std::vector<joined_row> _rows;

    // Whether any search scored the candidate matched to a row; see joined_row::dropped. A row no
    // candidate names has no score at all. Nothing is dropped when the rows are not matched.
    bool has_score(std::optional<size_t> candidate) const {
        if (!_candidates) {
            return true;
        }
        return candidate && std::ranges::any_of((*_candidates)[*candidate].hits,
                [] (const std::optional<vector_search::search_hit>& hit) { return hit.has_value(); });
    }

    void add_row(std::optional<size_t> candidate) {
        _rows.push_back(joined_row{.candidate = candidate, .dropped = !has_score(candidate)});
    }

    // Steps over this partition's candidates whose row the base table no longer has.
    std::optional<size_t> match(const clustering_key_prefix& row_ck) {
        for (; _next_result < _partition_end; ++_next_result) {
            if (_schema.clustering_key_size() == 0 || (*_candidates)[_next_result].clustering.equal(_schema, row_ck)) {
                return _next_result++;
            }
        }
        return std::nullopt;
    }

public:
    joining_visitor(const schema& schema, const std::vector<vector_search::search_candidate>* candidates)
        : _schema(schema)
        , _candidates(candidates) {
    }

    std::vector<joined_row> rows() && {
        return std::move(_rows);
    }

    // query::ResultVisitor, called by query::result_view::consume().

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _rows_in_partition = row_count;
        _partition_end = _next_result;
        if (!_candidates || row_count == 0) {
            // The index names rows, so nothing can name a partition that has none. Claiming no
            // results here is what leaves them for the partitions that follow.
            return;
        }
        // The rows arrive in the order of the results, and the read gives a partition its own
        // entry per contiguous run of results naming it, so this entry's run begins at the cursor.
        const auto& results = *_candidates;
        const auto names_this_partition = [&] (size_t i) { return results[i].partition.key().equal(_schema, key); };
        while (_next_result < results.size() && !names_this_partition(_next_result)) {
            ++_next_result;
        }
        for (_partition_end = _next_result; _partition_end < results.size() && names_this_partition(_partition_end); ++_partition_end) {
        }
    }

    void accept_new_partition(uint64_t row_count) {
        // Called when the slice left out the partition key, which matching compares.
        throwing_assert(!_candidates);
        _rows_in_partition = row_count;
        _partition_end = _next_result;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view&, const query::result_row_view&) {
        add_row(match(key));
    }

    void accept_new_row(const query::result_row_view&, const query::result_row_view&) {
        // Called when the slice left out the clustering key, which matching compares unless the
        // table has none.
        throwing_assert(!_candidates || _schema.clustering_key_size() == 0);
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
        const std::vector<vector_search::search_candidate>* candidates) {
    auto visitor = joining_visitor(schema, candidates);
    query::result_view::consume(table_results, slice, visitor);
    return std::move(visitor).rows();
}

namespace {

/// What one search said about `row`, or nothing where it said nothing: either no candidate names
/// the row, its key not having been in any search's answer, or that search did not return the key.
/// A hit whose score was not a finite number was recorded as absent when the answers were joined.
std::optional<vector_search::search_hit> hit_of(
        const joined_row& row, size_t search, const std::vector<vector_search::search_candidate>& candidates) {
    if (!row.candidate) {
        return std::nullopt;
    }
    return candidates[*row.candidate].hits[search];
}

} // anonymous namespace

std::vector<cql3::raw_value> scores_of(
        std::span<const joined_row> rows, size_t search, const std::vector<vector_search::search_candidate>& candidates) {
    auto values = std::vector<cql3::raw_value>{};
    values.reserve(rows.size());
    for (const auto& row : rows) {
        const auto hit = hit_of(row, search, candidates);
        values.push_back(hit ? cql3::raw_value::make_value(float_type->decompose(hit->score)) : cql3::raw_value::make_null());
    }
    return values;
}

std::vector<cql3::raw_value> ranks_of(
        std::span<const joined_row> rows, size_t search, const std::vector<vector_search::search_candidate>& candidates) {
    auto values = std::vector<cql3::raw_value>{};
    values.reserve(rows.size());
    for (const auto& row : rows) {
        // Same rows as scores_of(): a row a search did not return has no rank from it either.
        const auto hit = hit_of(row, search, candidates);
        values.push_back(
                hit ? cql3::raw_value::make_value(int32_type->decompose(static_cast<int32_t>(hit->rank))) : cql3::raw_value::make_null());
    }
    return values;
}

std::vector<external_values> search_values_of(const search_temporaries& temporaries, std::span<const joined_row> rows, size_t search,
        const std::vector<vector_search::search_candidate>& candidates) {
    std::vector<external_values> values;
    if (temporaries.score) {
        values.push_back({.temporary_index = *temporaries.score, .values = scores_of(rows, search, candidates)});
    }
    if (temporaries.rank) {
        values.push_back({.temporary_index = *temporaries.rank, .values = ranks_of(rows, search, candidates)});
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
