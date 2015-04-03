#pragma once

#include <experimental/optional>
#include "schema.hh"
#include "types.hh"
#include "atomic_cell.hh"
#include "keys.hh"
#include "enum_set.hh"
#include "bytes_ostream.hh"
#include "utils/data_input.hh"

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
    range() : range({}, {}) {}
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

    const optional<bound>& start() const {
        return _start;
    }

    const optional<bound>& end() const {
        return _end;
    }

    template<typename U>
    friend std::ostream& operator<<(std::ostream& out, const range<U>& r);
};

template<typename U>
std::ostream& operator<<(std::ostream& out, const range<U>& r) {
    if (r.is_singular()) {
        return out << "==" << r.start_value();
    }

    if (!r.start()) {
        out << "(-inf, ";
    } else {
        if (r.start()->is_inclusive()) {
            out << "[";
        } else {
            out << "(";
        }
        out << r.start()->value() << ", ";
    }

    if (!r.end()) {
        out << "+inf)";
    } else {
        out << r.end()->value();
        if (r.end()->is_inclusive()) {
            out << "]";
        } else {
            out << ")";
        }
    }

    return out;
}

using partition_range = range<partition_key>;
using clustering_range = range<clustering_key_prefix>;

class partition_slice {
public:
    enum class option { send_clustering_key, send_partition_key, send_timestamp_and_ttl };
    using option_set = enum_set<super_enum<option,
        option::send_clustering_key,
        option::send_partition_key,
        option::send_timestamp_and_ttl>>;
public:
    std::vector<clustering_range> row_ranges;
    std::vector<column_id> static_columns; // TODO: consider using bitmap
    std::vector<column_id> regular_columns;  // TODO: consider using bitmap
    option_set options;
public:
    partition_slice(std::vector<clustering_range> row_ranges, std::vector<column_id> static_columns,
                    std::vector<column_id> regular_columns, option_set options)
        : row_ranges(std::move(row_ranges))
        , static_columns(std::move(static_columns))
        , regular_columns(std::move(regular_columns))
        , options(options)
    { }
    friend std::ostream& operator<<(std::ostream& out, const partition_slice& ps);
};

//
// Query results are serialized to the following form:
//
// <result>          ::= <partition>*
// <partition>       ::= <row-count> [ <partition-key> ] [ <static-row> ] <row>*
// <static-row>      ::= <row>
// <row>             ::= <row-length> <cell>+
// <cell>            ::= <atomic-cell> | <collection-cell>
// <atomic-cell>     ::= <present-byte> [ <timestamp> <ttl> ] <value>
// <collection-cell> ::= <blob>
//
// <value>           ::= <blob>
// <blob>            ::= <blob-length> <uint8_t>*
// <timestamp>       ::= <uint64_t>
// <ttl>             ::= <int32_t>
// <present-byte>    ::= <int8_t>
// <row-length>      ::= <uint32_t>
// <row-count>       ::= <uint32_t>
// <blob-length>     ::= <uint32_t>
//
// Optional elements are present according to partition_slice
//

class result_atomic_cell_view {
    api::timestamp_type _timestamp;
    ttl_opt _ttl;
    bytes_view _value;
public:
    result_atomic_cell_view(api::timestamp_type timestamp, ttl_opt ttl, bytes_view value)
        : _timestamp(timestamp), _ttl(ttl), _value(value) { }

    api::timestamp_type timestamp() const {
        return _timestamp;
    }

    ttl_opt ttl() const {
        return _ttl;
    }

    bytes_view value() const {
        return _value;
    }
};

// Contains cells in the same order as requested by partition_slice.
// Contains only live cells.
class result_row_view {
    bytes_view _v;
    const partition_slice& _slice;
public:
    result_row_view(bytes_view v, const partition_slice& slice) : _v(v), _slice(slice) {}

    class iterator_type {
        data_input _in;
        const partition_slice& _slice;
    public:
        iterator_type(bytes_view v, const partition_slice& slice)
            : _in(v)
            , _slice(slice)
        { }
        std::experimental::optional<result_atomic_cell_view> next_atomic_cell() {
            auto present = _in.read<int8_t>();
            if (!present) {
                return {};
            }
            api::timestamp_type timestamp = api::missing_timestamp;
            ttl_opt ttl_;
            if (_slice.options.contains<partition_slice::option::send_timestamp_and_ttl>()) {
                timestamp = _in.read <api::timestamp_type> ();
                auto ttl_rep = _in.read<gc_clock::rep>();
                if (ttl_rep != std::numeric_limits<gc_clock::rep>::max()) {
                    ttl_ = gc_clock::time_point(gc_clock::duration(ttl_rep));
                }
            }
            auto value = _in.read_view_to_blob();
            return {result_atomic_cell_view(timestamp, ttl_, value)};
        }
        std::experimental::optional<collection_mutation::view> next_collection_cell() {
            auto present = _in.read<int8_t>();
            if (!present) {
                return {};
            }
            return collection_mutation::view{_in.read_view_to_blob()};
        };
        void skip(const column_definition& def) {
            if (def.is_atomic()) {
                next_atomic_cell();
            } else {
                next_collection_cell();
            }
        }
    };

    iterator_type iterator() const {
        return iterator_type(_v, _slice);
    }

    bool empty() const {
        return _v.empty();
    }
};

// Describes expectations about the ResultVisitor concept.
//
// Interaction flow:
//   -> accept_new_partition()
//   -> accept_new_row()
//   -> accept_new_row()
//   -> accept_partition_end()
//   -> accept_new_partition()
//   -> accept_new_row()
//   -> accept_new_row()
//   -> accept_new_row()
//   -> accept_partition_end()
//   ...
//
struct result_visitor {
    void accept_new_partition(
        const partition_key& key, // FIXME: use view for the key
        uint32_t row_count) {}

    void accept_new_partition(uint32_t row_count) {}

    void accept_new_row(
        const clustering_key& key, // FIXME: use view for the key
        const result_row_view& static_row,
        const result_row_view& row) {}

    void accept_new_row(const result_row_view& static_row, const result_row_view& row) {}

    void accept_partition_end(const result_row_view& static_row) {}
};

class result_view {
    bytes_view _v;
public:
    result_view(bytes_view v) : _v(v) {}

    template <typename ResultVisitor>
    void consume(const partition_slice& slice, ResultVisitor&& visitor) {
        data_input in(_v);
        while (in.has_next()) {
            auto row_count = in.read<uint32_t>();
            if (slice.options.contains<partition_slice::option::send_partition_key>()) {
                auto key = partition_key::from_bytes(to_bytes(in.read_view_to_blob()));
                visitor.accept_new_partition(key, row_count);
            } else {
                visitor.accept_new_partition(row_count);
            }

            bytes_view static_row_view;
            if (!slice.static_columns.empty()) {
                static_row_view = in.read_view_to_blob();
            }
            result_row_view static_row(static_row_view, slice);

            while (row_count--) {
                if (slice.options.contains<partition_slice::option::send_clustering_key>()) {
                    auto key = clustering_key::from_bytes(to_bytes(in.read_view_to_blob()));
                    result_row_view row(in.read_view_to_blob(), slice);
                    visitor.accept_new_row(key, static_row, row);
                } else {
                    result_row_view row(in.read_view_to_blob(), slice);
                    visitor.accept_new_row(static_row, row);
                }
            }

            visitor.accept_partition_end(static_row);
        }
    }
};

class result {
    bytes_ostream _w;
public:
    class builder;
    class partition_writer;
    class row_writer;
    friend class result_merger;

    result() {}
    result(bytes_ostream&& w) : _w(std::move(w)) {}

    const bytes_ostream& buf() const {
        return _w;
    }
};

class result::row_writer {
    bytes_ostream& _w;
    const partition_slice& _slice;
    bytes_ostream::place_holder<uint32_t> _size_ph;
    size_t _start_pos;
    bool _finished = false;
public:
    row_writer(
        const partition_slice& slice,
        bytes_ostream& w,
        bytes_ostream::place_holder<uint32_t> size_ph)
            : _w(w)
            , _slice(slice)
            , _size_ph(size_ph)
            , _start_pos(w.size())
    { }

    ~row_writer() {
        assert(_finished);
    }

    void add_empty() {
        // FIXME: store this in a bitmap
        _w.write<int8_t>(false);
    }

    void add(::atomic_cell_view c) {
        // FIXME: store this in a bitmap
        _w.write<int8_t>(true);
        assert(c.is_live());
        if (_slice.options.contains<partition_slice::option::send_timestamp_and_ttl>()) {
            _w.write(c.timestamp());
            if (c.ttl()) {
                _w.write<gc_clock::rep>(c.ttl()->time_since_epoch().count());
            } else {
                _w.write<gc_clock::rep>(std::numeric_limits<gc_clock::rep>::max());
            }
        }
        _w.write_blob(c.value());
    }

    void add(collection_mutation::view v) {
        // FIXME: store this in a bitmap
        _w.write<int8_t>(true);
        _w.write_blob(v.data);
    }

    void finish() {
        auto row_size = _w.size() - _start_pos;
        assert((uint32_t)row_size == row_size);
        _w.set(_size_ph, (uint32_t)row_size);
        _finished = true;
    }
};

class result::partition_writer {
    bytes_ostream& _w;
    const partition_slice& _slice;
    bytes_ostream::place_holder<uint32_t> _count_ph;
    uint32_t _row_count = 0;
    bool _static_row_added = false;
    bool _finished = false;
public:
    partition_writer(
        const partition_slice& slice,
        bytes_ostream::place_holder<uint32_t> count_ph,
        bytes_ostream& w)
            : _w(w)
            , _slice(slice)
            , _count_ph(count_ph)
    { }

    ~partition_writer() {
        assert(_finished);
    }

    row_writer add_row(const clustering_key& key) {
        if (_slice.options.contains<partition_slice::option::send_clustering_key>()) {
            _w.write_blob(key);
        }
        ++_row_count;
        auto size_placeholder = _w.write_place_holder<uint32_t>();
        return row_writer(_slice, _w, size_placeholder);
    }

    // Call before any add_row()
    row_writer add_static_row() {
        assert(!_static_row_added); // Static row can be added only once
        assert(!_row_count); // Static row must be added before clustered rows
        _static_row_added = true;
        auto size_placeholder = _w.write_place_holder<uint32_t>();
        return row_writer(_slice, _w, size_placeholder);
    }

    uint32_t row_count() const {
        return std::max(_row_count, (uint32_t)_static_row_added);
    }

    void finish() {
        _w.set(_count_ph, _row_count);
        _finished = true;
    }
};

class result::builder {
    bytes_ostream _w;
    const partition_slice& _slice;
public:
    builder(const partition_slice& slice) : _slice(slice) { }

    // Starts new partition and returns a builder for its contents.
    // Invalidates all previously obtained builders
    partition_writer add_partition(const partition_key& key) {
        auto count_place_holder = _w.write_place_holder<uint32_t>();
        if (_slice.options.contains<partition_slice::option::send_partition_key>()) {
            _w.write_blob(key);
        }
        return partition_writer(_slice, count_place_holder, _w);
    }

    result build() {
        return result(std::move(_w));
    };
};

class read_command {
public:
    utils::UUID cf_id;
    std::vector<partition_range> partition_ranges; // ranges must be non-overlapping
    partition_slice slice;
    uint32_t row_limit;
public:
    read_command(const utils::UUID& cf_id, std::vector<partition_range> partition_ranges,
            partition_slice slice, uint32_t row_limit)
        : cf_id(cf_id)
        , partition_ranges(std::move(partition_ranges))
        , slice(std::move(slice))
        , row_limit(row_limit)
    { }
    friend std::ostream& operator<<(std::ostream& out, const read_command& r);
};

}
