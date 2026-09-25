/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "values_provider.hh"

#include <algorithm>
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

/// Reads a fixed list of columns out of every row walked, in the order asked; see
/// joined_row::columns. Where each value comes from is settled once, up front: every column is
/// given the position it is read from - a component of the key, or a cell of the row - and the
/// slot it fills. Reading a row is then a walk of those positions in order, with no searching.
class column_reader {
    const schema& _schema;
    const query::partition_slice& _slice;
    /// How many values a row is read into, which is how many columns were asked for. The columns
    /// themselves are only needed to settle the slots below.
    size_t _column_count;

    /// For each position, the slot the value there fills, or nothing where nobody asked for it.
    /// A list ends after the last position asked for, so what lies beyond it is never read, and an
    /// empty one means nothing is read from that source at all.
    using slots = std::vector<std::optional<size_t>>;
    slots _partition_slots;
    slots _clustering_slots;
    slots _static_slots;
    slots _regular_slots;

    static void fill_slot(slots& s, size_t position, size_t slot) {
        if (s.size() <= position) {
            s.resize(position + 1);
        }
        // One position fills one slot: asking for the same column twice would leave all but the
        // last of them unread.
        throwing_assert(!s[position]);
        s[position] = slot;
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

    /// Reads the cells `ids` names, in the order they arrive, keeping the ones `s` asks for and
    /// stepping over the rest - the iterator only moves forward, so every cell up to the last one
    /// wanted has to be passed.
    void read_cells(query::result_row_view::iterator_type iter, const query::column_id_vector& ids, const slots& s, column_kind kind,
            std::vector<managed_bytes_opt>& values) const {
        for (size_t position = 0; position < s.size(); ++position) {
            const auto& column = _schema.column_at(kind, ids[position]);
            if (const auto slot = s[position]) {
                values[*slot] = next_value(iter, column);
            } else {
                iter.skip(column);
            }
        }
    }

    /// The key's components are views over its own storage, so walking them copies nothing but the
    /// values wanted. A component the key does not have - the clustering key of a partition
    /// holding nothing but a static row, say - is never reached, and its value is left absent.
    template <typename Key>
    void read_key_components(const Key& key, const slots& s, std::vector<managed_bytes_opt>& values) const {
        size_t position = 0;
        for (const managed_bytes_view component : key.components()) {
            if (position >= s.size()) {
                return;
            }
            if (const auto slot = s[position]) {
                values[*slot] = managed_bytes(component);
            }
            ++position;
        }
    }

public:
    column_reader(const schema& schema, const query::partition_slice& slice, std::span<const column_definition* const> columns)
        : _schema(schema)
        , _slice(slice)
        , _column_count(columns.size()) {
        for (size_t slot = 0; slot < columns.size(); ++slot) {
            const auto& column = *columns[slot];
            switch (column.kind) {
            case column_kind::partition_key:
                fill_slot(_partition_slots, column.component_index(), slot);
                break;
            case column_kind::clustering_key:
                fill_slot(_clustering_slots, column.component_index(), slot);
                break;
            case column_kind::static_column:
                fill_slot(_static_slots, cell_position_of(column), slot);
                break;
            case column_kind::regular_column:
                fill_slot(_regular_slots, cell_position_of(column), slot);
                break;
            }
        }
    }

    bool reads_partition_key() const {
        return !_partition_slots.empty();
    }

    bool reads_clustering_key() const {
        return !_clustering_slots.empty();
    }

    /// The values the rows of a partition start from: the partition-key columns asked for, which
    /// every row of it shares, and nothing else yet.
    std::vector<managed_bytes_opt> partition_values(const partition_key* key) const {
        if (_column_count == 0) {
            return {};
        }
        auto values = std::vector<managed_bytes_opt>(_column_count);
        if (reads_partition_key()) {
            throwing_assert(key);
            read_key_components(*key, _partition_slots, values);
        }
        return values;
    }

    /// Adds what a row of that partition gives: its clustering key and its cells. `key` is null
    /// where the slice left the clustering key out, `row` for the row emitted for a partition
    /// holding nothing but a static row.
    void read_row(std::vector<managed_bytes_opt>& values, const clustering_key_prefix* key,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        if (values.empty()) {
            return;
        }
        if (key) {
            read_key_components(*key, _clustering_slots, values);
        }
        read_cells(static_row.iterator(), _slice.static_columns, _static_slots, column_kind::static_column, values);
        if (row) {
            read_cells(row->iterator(), _slice.regular_columns, _regular_slots, column_kind::regular_column, values);
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

    // The external results to match the rows to, or null when the rows are not matched.
    const vector_search::vector_store_client::primary_keys* _external_results;

    // The results naming rows of the partition being walked, settled when it opens while its key
    // is in hand, so the key itself need not be kept.
    size_t _next_result = 0;
    size_t _partition_end = 0;

    uint64_t _rows_in_partition = 0;

    // Reads the columns asked for out of every row. The values of the partition being walked are
    // what its rows start from: a partition-key column has one value for all of them.
    column_reader _columns;
    std::vector<managed_bytes_opt> _partition_values;

    std::vector<managed_bytes_opt> read_columns(const clustering_key_prefix* key,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        auto values = _partition_values;
        _columns.read_row(values, key, static_row, row);
        return values;
    }

    std::vector<joined_row> _rows;

    // Whether the result matched to a row has a similarity to report; see joined_row::dropped. A
    // row no result names has none. Nothing is dropped when the rows are not matched.
    bool has_similarity(std::optional<size_t> result) const {
        if (!_external_results) {
            return true;
        }
        return result && similarity_at(*_external_results, *result).has_value();
    }

    void add_row(std::optional<size_t> result, std::vector<managed_bytes_opt> columns) {
        _rows.push_back(joined_row{.external_result = result, .dropped = !has_similarity(result), .columns = std::move(columns)});
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
    joining_visitor(const schema& schema, const query::partition_slice& slice,
            const vector_search::vector_store_client::primary_keys* external_results, std::span<const column_definition* const> columns)
        : _schema(schema)
        , _external_results(external_results)
        , _columns(schema, slice, columns) {
    }

    std::vector<joined_row> rows() && {
        return std::move(_rows);
    }

    // query::ResultVisitor, called by query::result_view::consume().

    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        _partition_values = _columns.partition_values(&key);
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
        // Called when the slice left out the partition key, which matching compares and a
        // partition-key column is read from.
        throwing_assert(!_external_results);
        throwing_assert(!_columns.reads_partition_key());
        _partition_values = _columns.partition_values(nullptr);
        _rows_in_partition = row_count;
        _partition_end = _next_result;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
        add_row(match(key), read_columns(&key, static_row, &row));
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        // Called when the slice left out the clustering key, which matching compares unless the
        // table has none.
        throwing_assert(!_external_results || _schema.clustering_key_size() == 0);
        throwing_assert(!_columns.reads_clustering_key());
        add_row(match(clustering_key_prefix::make_empty()), read_columns(nullptr, static_row, &row));
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        if (_rows_in_partition == 0) {
            // The row emitted for a partition holding nothing but a static row: it has no cells of
            // its own, so only the static columns and the partition key can be read from it.
            add_row(std::nullopt, read_columns(nullptr, static_row, nullptr));
        }
    }
};

} // anonymous namespace

std::vector<joined_row> join_table_results(const query::result& table_results, const query::partition_slice& slice, const schema& schema,
        const vector_search::vector_store_client::primary_keys* external_results, std::span<const column_definition* const> columns) {
    auto visitor = joining_visitor(schema, slice, external_results, columns);
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
