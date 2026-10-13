/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "values_provider.hh"

#include <algorithm>
#include <span>
#include <utility>

#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

namespace cql3::statements::external_search {

namespace {

/// The next cell of `iter`, as a value or nothing where the row has none for the column. The
/// iterator moves past the cell either way, so it stays aligned with the columns it is read
/// against; a cell whose value is not wanted has to be stepped over rather than left unread.
managed_bytes_opt next_value(query::result_row_view::iterator_type& iter, const column_definition& column) {
    if (column.type->is_multi_cell()) {
        auto cell = iter.next_collection_cell();
        return cell ? managed_bytes_opt(managed_bytes(*cell)) : std::nullopt;
    }
    auto cell = iter.next_atomic_cell();
    return cell ? managed_bytes_opt(managed_bytes(cell->value())) : std::nullopt;
}

/// What the searches said about a row: the hits of the candidate naming it, or none where no
/// candidate does.
using row_hits = std::span<const std::optional<vector_search::search_hit>>;

/// Reads a fixed list of columns out of every row walked, in the order asked; see
/// joined_row::columns. Unlike the result set builder, which reads every selected column, this
/// reads a few columns out of what can be a large selection, so where each of them is found is
/// worked out once per read. Walking a row then does no matching, and stops after the last value
/// wanted.
class column_reader {
    const schema& _schema;
    const query::partition_slice& _slice;

    /// A column asked for: where its value is found in a row - a component of the key, or a cell
    /// among the cells of its kind the slice asked for - the slot of the row's values it fills, and
    /// the search it is read for, if any; see column_read.
    struct wanted_column {
        column_kind kind;
        size_t position;
        size_t slot;
        std::optional<size_t> for_search;
    };

    /// Whether `wanted` is read for a row with `hits`: always, or, for a search, only where it
    /// returned the row.
    static bool read_for_row(const wanted_column& wanted, row_hits hits) {
        return !wanted.for_search || (*wanted.for_search < hits.size() && hits[*wanted.for_search]);
    }

    /// One entry per column asked for, ordered by kind and then by position, which is the order a
    /// row's values are met in.
    std::vector<wanted_column> _wanted;

    /// The entries of one kind, in position order.
    std::span<const wanted_column> wanted_of(column_kind kind) const {
        const auto [begin, end] = std::ranges::equal_range(_wanted, kind, {}, &wanted_column::kind);
        return {begin, end};
    }

    /// The position of `column` among the cells the slice asked for, counting only its own kind,
    /// which is the order they arrive in.
    size_t cell_position_of(const column_definition& column) const {
        const auto& ids = column.is_static() ? _slice.static_columns : _slice.regular_columns;
        const auto it = std::ranges::find(ids, column.id);
        // A column is read from the cells the slice asked for, so it has to be one of them:
        // whoever built the read has to have asked for it, the way the highlighted column is asked
        // for with add_column_for_post_processing().
        throwing_assert(it != ids.end());
        return std::distance(ids.begin(), it);
    }

    /// Reads the cells `ids` names, in the order they arrive, keeping the ones of `kind` asked for
    /// that are read out of a row with `hits` and stepping over the rest - the iterator only moves forward, so
    /// every cell up to the last one wanted has to be passed.
    void read_cells(query::result_row_view::iterator_type iter, const query::column_id_vector& ids, column_kind kind,
            std::vector<managed_bytes_opt>& values, row_hits hits) const {
        const auto wanted = wanted_of(kind);
        auto next = wanted.begin();
        for (size_t position = 0; next != wanted.end(); ++position) {
            const auto& column = _schema.column_at(kind, ids[position]);
            const bool is_wanted = position == next->position;
            if (is_wanted && read_for_row(*next, hits)) {
                values[next->slot] = next_value(iter, column);
            } else {
                iter.skip(column);
            }
            if (is_wanted) {
                ++next;
            }
        }
    }

    /// The key's components are views over its own storage, so walking them copies nothing but the
    /// values wanted. A component the key does not have - the clustering key of a partition
    /// holding nothing but a static row, say - is never reached, and its value is left absent.
    template <typename Key>
    void read_key_components(const Key& key, column_kind kind, std::vector<managed_bytes_opt>& values) const {
        const auto wanted = wanted_of(kind);
        auto next = wanted.begin();
        size_t position = 0;
        for (const managed_bytes_view component : key.components()) {
            if (next == wanted.end()) {
                return;
            }
            if (position == next->position) {
                values[next->slot] = managed_bytes(component);
                ++next;
            }
            ++position;
        }
    }

public:
    column_reader(const schema& schema, const query::partition_slice& slice, std::span<const column_read> columns)
        : _schema(schema)
        , _slice(slice) {
        _wanted.reserve(columns.size());
        for (size_t slot = 0; slot < columns.size(); ++slot) {
            const auto& column = *columns[slot].column;
            const auto position = column.is_primary_key() ? column.component_index() : cell_position_of(column);
            _wanted.push_back(wanted_column{.kind = column.kind, .position = position, .slot = slot, .for_search = columns[slot].for_search});
        }
        const auto location = [] (const wanted_column& w) { return std::pair(w.kind, w.position); };
        std::ranges::sort(_wanted, {}, location);
        // One position fills one slot: the walk reads each value once, so a column asked for twice
        // would leave one of its slots unfilled.
        throwing_assert(std::ranges::adjacent_find(_wanted, {}, location) == _wanted.end());
    }

    bool reads_partition_key() const {
        return !wanted_of(column_kind::partition_key).empty();
    }

    bool reads_clustering_key() const {
        return !wanted_of(column_kind::clustering_key).empty();
    }

    /// The values the rows of a partition start from: the partition-key columns asked for, which
    /// every row of it shares, and nothing else yet.
    std::vector<managed_bytes_opt> partition_values(const partition_key* key) const {
        if (_wanted.empty()) {
            return {};
        }
        auto values = std::vector<managed_bytes_opt>(_wanted.size());
        if (reads_partition_key()) {
            throwing_assert(key);
            read_key_components(*key, column_kind::partition_key, values);
        }
        return values;
    }

    /// Adds what a row of that partition gives: its clustering key and its cells, only the ones read
    /// out of a row with `hits`. `key` is null where the slice left the clustering key out,
    /// `row` for the row emitted for a partition holding nothing but a static row.
    void read_row(std::vector<managed_bytes_opt>& values, row_hits hits, const clustering_key_prefix* key,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        if (values.empty()) {
            return;
        }
        if (key) {
            read_key_components(*key, column_kind::clustering_key, values);
        }
        read_cells(static_row.iterator(), _slice.static_columns, column_kind::static_column, values, hits);
        if (row) {
            read_cells(row->iterator(), _slice.regular_columns, column_kind::regular_column, values, hits);
        }
        // A cell not read was never copied; a key component, which is small, was, and is dropped
        // here.
        for (const auto& wanted : _wanted) {
            if (!read_for_row(wanted, hits)) {
                values[wanted.slot] = std::nullopt;
            }
        }
    }
};

/// Matches each row the result set will be built from to the external result that names it.
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

    // Reads the columns asked for out of every row. The values of the partition being walked are
    // what its rows start from: a partition-key column has one value for all of them.
    column_reader _columns;
    std::vector<managed_bytes_opt> _partition_values;

    // What the searches said about the row `candidate` names, or nothing where none does.
    row_hits hits_of(std::optional<size_t> candidate) const {
        return candidate ? row_hits((*_candidates)[*candidate].hits) : row_hits();
    }

    std::vector<managed_bytes_opt> read_columns(row_hits hits, const clustering_key_prefix* key,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        auto values = _partition_values;
        _columns.read_row(values, hits, key, static_row, row);
        return values;
    }

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

    void add_row(std::optional<size_t> candidate, std::vector<managed_bytes_opt> columns) {
        _rows.push_back(joined_row{.candidate = candidate, .dropped = !has_score(candidate), .columns = std::move(columns)});
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
    joining_visitor(const schema& schema, const query::partition_slice& slice,
            const std::vector<vector_search::search_candidate>* candidates, std::span<const column_read> columns)
        : _schema(schema)
        , _candidates(candidates)
        , _columns(schema, slice, columns) {
        throwing_assert(_candidates || std::ranges::none_of(columns, [] (const column_read& c) { return c.for_search.has_value(); }));
    }

    std::vector<joined_row> rows() && {
        return std::move(_rows);
    }

    // query::ResultVisitor, called by query::result_view::consume().

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _partition_values = _columns.partition_values(&key);
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
        // Called when the slice left out the partition key, which matching compares and a
        // partition-key column is read from.
        throwing_assert(!_candidates);
        throwing_assert(!_columns.reads_partition_key());
        _partition_values = _columns.partition_values(nullptr);
        _rows_in_partition = row_count;
        _partition_end = _next_result;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
        const auto candidate = match(key);
        add_row(candidate, read_columns(hits_of(candidate), &key, static_row, &row));
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        // Called when the slice left out the clustering key, which matching compares unless the
        // table has none.
        throwing_assert(!_candidates || _schema.clustering_key_size() == 0);
        throwing_assert(!_columns.reads_clustering_key());
        const auto candidate = match(clustering_key_prefix::make_empty());
        add_row(candidate, read_columns(hits_of(candidate), nullptr, static_row, &row));
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        if (_rows_in_partition == 0) {
            // The row emitted for a partition holding nothing but a static row: it has no cells of
            // its own, so only the static columns and the partition key can be read from it.
            add_row(std::nullopt, read_columns({}, nullptr, static_row, nullptr));
        }
    }
};

} // anonymous namespace

std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const std::vector<vector_search::search_candidate>* candidates, std::span<const column_read> columns) {
    auto visitor = joining_visitor(schema, slice, candidates, columns);
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
