/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "query-request.hh"
#include "query-result.hh"
#include "utils/data_input.hh"

// Refer to query-result.hh for the query result format

namespace query {

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

}
