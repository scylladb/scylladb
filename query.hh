#pragma once

#include "schema.hh"
#include "types.hh"
#include "atomic_cell.hh"

namespace query {

// A range which can have inclusive, exclusive or open-ended bounds on each end.
class range {
    bytes _start; // empty if range is open on this end
    bytes _end; // empty if range is open on this end (_end_inclusive == true) or same as start (_end_inclusive == false)
    bool _start_inclusive;
    bool _end_inclusive;
public:
    range(bytes start, bytes end, bool start_inclusive, bool end_inclusive)
        : _start(std::move(start))
        , _end(std::move(end))
        , _start_inclusive(start_inclusive)
        , _end_inclusive(end_inclusive)
    { }
public:
    static range make_open_ended_both_sides() {
        return {{}, {}, true, true};
    }
    static range make_singular(bytes key) {
        return {std::move(key), {}, true, false};
    }
    static range make_starting_with(bytes key, bool inclusive = true) {
        assert(!key.empty());
        return {std::move(key), {}, inclusive, true};
    }
    static range make_ending_with(bytes key, bool inclusive = true) {
        assert(!key.empty());
        return {{}, std::move(key), true, inclusive};
    }
    static range make_both_inclusive(bytes start, bytes end) {
        assert(!start.empty());
        assert(!end.empty());
        return {std::move(start), std::move(end), true, true};
    }
    static range make_inclusive_exclusive(bytes start, bytes end) {
        assert(!start.empty());
        assert(!end.empty());
        return {std::move(start), std::move(end), true, false};
    }
    static range make_exclusive_inclusive(bytes start, bytes end) {
        assert(!start.empty());
        assert(!end.empty());
        return {std::move(start), std::move(end), false, true};
    }
    static range make_both_exclusive(bytes start, bytes end) {
        assert(!start.empty());
        assert(!end.empty());
        return {std::move(start), std::move(end), false, false};
    }
    bool is_singular() const {
        return _end.empty() && !_end_inclusive;
    }
    bool is_full() const {
        return _start.empty() && _end.empty();
    }
    void reverse() {
        if (!is_singular()) {
            std::swap(_start, _end);
            std::swap(_end_inclusive, _start_inclusive);
        }
    }
    const bytes& start() const {
        return _start;
    }
    const bytes& end() const {
        return _end;
    }
};

class result {
public:
    class partition;
    class row;

    // TODO: Optimize for singular partition range. In such case the caller
    // knows the partition key, no need to send it back.

    // std::pair::first is a serialized partition key.
    std::vector<std::pair<bytes, partition>> partitions;
};

class result::row {
public:
    // contains cells in the same order as requested by partition_slice
    std::vector<std::experimental::optional<atomic_cell_or_collection>> cells;
public:
    bool empty() const { return cells.empty(); }
    explicit operator bool() const { return !empty(); }
};

class result::partition {
public:
    row static_row; // when serializing, make present only when queried for static columns

    // TODO: for some queries we could avoid sending keys back, because the client knows
    // what the key is (single row query for instance).
    //
    // std::pair::first is a serialized clustering row key.
    std::vector<std::pair<bytes, row>> rows;
public:
    // Returns row count in this result. If there is a static row and no clustering rows, that counts as one row.
    // Otherwise, if there are some clustering rows, the static row doesn't count.
    size_t row_count() {
        return rows.empty() ? !static_row.empty() : rows.size();
    }
};

class partition_slice {
public:
    std::vector<range> row_ranges;
    std::vector<column_id> static_columns; // TODO: consider using bitmap
    std::vector<column_id> regular_columns;  // TODO: consider using bitmap
public:
    partition_slice(std::vector<range> row_ranges, std::vector<column_id> static_columns,
            std::vector<column_id> regular_columns)
        : row_ranges(std::move(row_ranges))
        , static_columns(std::move(static_columns))
        , regular_columns(std::move(regular_columns))
    { }
};

class read_command {
public:
    sstring keyspace;
    sstring column_family;
    std::vector<range> partition_ranges; // ranges must be non-overlapping
    partition_slice slice;
    uint32_t row_limit;
public:
    read_command(const sstring& keyspace, const sstring& column_family, std::vector<range> partition_ranges,
            partition_slice slice, uint32_t row_limit)
        : keyspace(keyspace)
        , column_family(column_family)
        , partition_ranges(std::move(partition_ranges))
        , slice(std::move(slice))
        , row_limit(row_limit)
    { }
};

}
