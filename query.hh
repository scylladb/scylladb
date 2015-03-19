#pragma once

#include <experimental/optional>
#include "schema.hh"
#include "types.hh"
#include "atomic_cell.hh"
#include "keys.hh"

namespace query {

// A range which can have inclusive, exclusive or open-ended bounds on each end.
template<typename T>
class range {
    template <typename U>
    using optional = std::experimental::optional<U>;
public:
    class bound {
        T _value;
        bool _inclusive;
    public:
        bound(T value, bool inclusive = true)
            : _value(std::move(value))
            , _inclusive(inclusive)
        { }
        const T& value() const { return _value; }
        bool is_inclusive() const { return _inclusive; }
    };
private:
    optional<bound> _start;
    optional<bound> _end;
    bool _singular;
public:
    range(optional<bound> start, optional<bound> end)
        : _start(std::move(start))
        , _end(std::move(end))
        , _singular(false)
    { }
    range(T value)
        : _start(bound(std::move(value), true))
        , _end()
        , _singular(true)
    { }
public:
    static range make(bound start, bound end) {
        return range({std::move(start)}, {std::move(end)});
    }
    static range make_open_ended_both_sides() {
        return {{}, {}};
    }
    static range make_singular(T value) {
        return {std::move(value)};
    }
    static range make_starting_with(bound b) {
        return {{std::move(b)}, {}};
    }
    static range make_ending_with(bound b) {
        return {{}, {std::move(b)}};
    }
    bool is_singular() const {
        return _singular;
    }
    bool is_full() const {
        return !_start && !_end;
    }
    void reverse() {
        if (!_singular) {
            std::swap(_start, _end);
        }
    }
    const T& start_value() const {
        return _start->value();
    }
    const T& end_value() const {
        return _end->value();
    }
};

using partition_range = range<partition_key>;
using clustering_range = range<clustering_key_prefix>;

class result {
public:
    class partition;
    class row;

    // TODO: Optimize for singular partition range. In such case the caller
    // knows the partition key, no need to send it back.
    std::vector<std::pair<partition_key, partition>> partitions;
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
    std::vector<std::pair<clustering_key, row>> rows;
public:
    // Returns row count in this result. If there is a static row and no clustering rows, that counts as one row.
    // Otherwise, if there are some clustering rows, the static row doesn't count.
    size_t row_count() {
        return rows.empty() ? !static_row.empty() : rows.size();
    }
};

class partition_slice {
public:
    std::vector<clustering_range> row_ranges;
    std::vector<column_id> static_columns; // TODO: consider using bitmap
    std::vector<column_id> regular_columns;  // TODO: consider using bitmap
public:
    partition_slice(std::vector<clustering_range> row_ranges, std::vector<column_id> static_columns,
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
    std::vector<partition_range> partition_ranges; // ranges must be non-overlapping
    partition_slice slice;
    uint32_t row_limit;
public:
    read_command(const sstring& keyspace, const sstring& column_family, std::vector<partition_range> partition_ranges,
            partition_slice slice, uint32_t row_limit)
        : keyspace(keyspace)
        , column_family(column_family)
        , partition_ranges(std::move(partition_ranges))
        , slice(std::move(slice))
        , row_limit(row_limit)
    { }
};

}
