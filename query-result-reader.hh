/*
 * Copyright 2015 Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "query-request.hh"
#include "query-result.hh"
#include "utils/data_input.hh"

// Refer to query-result.hh for the query result format

namespace query {

class result_atomic_cell_view {
    api::timestamp_type _timestamp;
    expiry_opt _expiry;
    bytes_view _value;
public:
    result_atomic_cell_view(api::timestamp_type timestamp, expiry_opt expiry, bytes_view value)
        : _timestamp(timestamp), _expiry(expiry), _value(value) { }

    api::timestamp_type timestamp() const {
        return _timestamp;
    }

    expiry_opt expiry() const {
        return _expiry;
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
            expiry_opt expiry_;
            if (_slice.options.contains<partition_slice::option::send_timestamp>()) {
                timestamp = _in.read<api::timestamp_type>();
            }
            if (_slice.options.contains<partition_slice::option::send_expiry>()) {
                auto expiry_rep = _in.read<gc_clock::rep>();
                if (expiry_rep != std::numeric_limits<gc_clock::rep>::max()) {
                    expiry_ = gc_clock::time_point(gc_clock::duration(expiry_rep));
                }
            }
            auto value = _in.read_view_to_blob<uint32_t>();
            return {result_atomic_cell_view(timestamp, expiry_, value)};
        }
        std::experimental::optional<collection_mutation_view> next_collection_cell() {
            auto present = _in.read<int8_t>();
            if (!present) {
                return {};
            }
            return collection_mutation_view{_in.read_view_to_blob<uint32_t>()};
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
    static void consume(const bytes_ostream& buf, const partition_slice& slice, ResultVisitor&& visitor) {
        // FIXME: This special casing saves us the cost of copying an already
        // linearized response. When we switch views to scattered_reader this will go away.
        if (buf.is_linearized()) {
            result_view view(buf.view());
            view.consume(slice, std::forward<ResultVisitor>(visitor));
        } else {
            bytes_ostream w(buf);
            result_view view(w.linearize());
            view.consume(slice, std::forward<ResultVisitor>(visitor));
        }
    }

    template <typename ResultVisitor>
    void consume(const partition_slice& slice, ResultVisitor&& visitor) {
        data_input in(_v);
        while (in.has_next()) {
            auto row_count = in.read<uint32_t>();
            if (slice.options.contains<partition_slice::option::send_partition_key>()) {
                auto key = partition_key::from_bytes(in.read_view_to_blob<uint32_t>());
                visitor.accept_new_partition(key, row_count);
            } else {
                visitor.accept_new_partition(row_count);
            }

            bytes_view static_row_view;
            if (!slice.static_columns.empty()) {
                static_row_view = in.read_view_to_blob<uint32_t>();
            }
            result_row_view static_row(static_row_view, slice);

            while (row_count--) {
                if (slice.options.contains<partition_slice::option::send_clustering_key>()) {
                    auto key = clustering_key::from_bytes(in.read_view_to_blob<uint32_t>());
                    result_row_view row(in.read_view_to_blob<uint32_t>(), slice);
                    visitor.accept_new_row(key, static_row, row);
                } else {
                    result_row_view row(in.read_view_to_blob<uint32_t>(), slice);
                    visitor.accept_new_row(static_row, row);
                }
            }

            visitor.accept_partition_end(static_row);
        }
    }
};

}
