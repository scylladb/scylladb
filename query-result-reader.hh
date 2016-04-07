/*
 * Copyright (C) 2015 ScyllaDB
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

#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/query.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/query.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/uuid.dist.impl.hh"

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
    ser::qr_row_view _v;
public:
    result_row_view(ser::qr_row_view v) : _v(v) {}

    class iterator_type {
        using cells_vec = std::vector<std::experimental::optional<ser::qr_cell_view>>;
        cells_vec _cells;
        cells_vec::iterator _i;
        bytes _tmp_value;
    public:
        iterator_type(ser::qr_row_view v)
            : _cells(v.cells())
            , _i(_cells.begin())
        { }
        std::experimental::optional<result_atomic_cell_view> next_atomic_cell() {
            auto cell_opt = *_i++;
            if (!cell_opt) {
                return {};
            }
            ser::qr_cell_view v = *cell_opt;
            api::timestamp_type timestamp = v.timestamp().value_or(api::missing_timestamp);
            expiry_opt expiry = v.expiry();
            _tmp_value = v.value();
            return {result_atomic_cell_view(timestamp, expiry, _tmp_value)};
        }
        std::experimental::optional<bytes_view> next_collection_cell() {
            auto cell_opt = *_i++;
            if (!cell_opt) {
                return {};
            }
            ser::qr_cell_view v = *cell_opt;
            _tmp_value = v.value();
            return {bytes_view(_tmp_value)};
        };
        void skip(const column_definition& def) {
            ++_i;
        }
    };

    iterator_type iterator() const {
        return iterator_type(_v);
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
    ser::query_result_view _v;
    friend class result_merger;
public:
    result_view(bytes_view v) : _v(ser::query_result_view{ser::as_input_stream(v)}) {}
    result_view(ser::query_result_view v) : _v(v) {}

    template <typename Func>
    static auto do_with(const query::result& res, Func&& func) {
        const bytes_ostream& buf = res.buf();
        // FIXME: This special casing saves us the cost of copying an already
        // linearized response. When we switch views to scattered_reader this will go away.
        if (buf.is_linearized()) {
            result_view view(buf.view());
            return func(view);
        } else {
            bytes_ostream w(buf);
            result_view view(w.linearize());
            return func(view);
        }
    }

    template <typename ResultVisitor>
    static void consume(const query::result& res, const partition_slice& slice, ResultVisitor&& visitor) {
        do_with(res, [&] (result_view v) {
            v.consume(slice, visitor);
        });
    }

    template <typename ResultVisitor>
    void consume(const partition_slice& slice, ResultVisitor&& visitor) {
        for (auto&& p : _v.partitions()) {
            auto rows = p.rows();
            auto row_count = rows.size();
            if (slice.options.contains<partition_slice::option::send_partition_key>()) {
                auto key = *p.key();
                visitor.accept_new_partition(key, row_count);
            } else {
                visitor.accept_new_partition(row_count);
            }

            result_row_view static_row(p.static_row());

            for (auto&& row : rows) {
                result_row_view view(row.cells());
                if (slice.options.contains<partition_slice::option::send_clustering_key>()) {
                    visitor.accept_new_row(*row.key(), static_row, view);
                } else {
                    visitor.accept_new_row(static_row, view);
                }
            }

            visitor.accept_partition_end(static_row);
        }
    }
};

}
