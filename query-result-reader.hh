/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/numeric.hpp>

#include "query-request.hh"
#include "query-result.hh"
#include "utils/data_input.hh"
#include "digest_algorithm.hh"

#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/query.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/query.dist.impl.hh"

namespace query {

using result_bytes_view = ser::buffer_view<bytes_ostream::fragment_iterator>;

class result_atomic_cell_view {
    ser::qr_cell_view _view;
public:
    result_atomic_cell_view(ser::qr_cell_view view)
        : _view(view) { }

    api::timestamp_type timestamp() const {
        return _view.timestamp().value_or(api::missing_timestamp);
    }

    expiry_opt expiry() const {
        return _view.expiry();
    }

    ttl_opt ttl() const {
        return _view.ttl();
    }

    result_bytes_view value() const {
        return _view.value().view();
    }
};

// Contains cells in the same order as requested by partition_slice.
// Contains only live cells.
class result_row_view {
    ser::qr_row_view _v;
public:
    result_row_view(ser::qr_row_view v) : _v(v) {}

    class iterator_type {
        using cells_vec = std::vector<std::optional<ser::qr_cell_view>>;
        cells_vec _cells;
        cells_vec::iterator _i;
    public:
        iterator_type(ser::qr_row_view v)
            : _cells(v.cells())
            , _i(_cells.begin())
        { }
        std::optional<result_atomic_cell_view> next_atomic_cell() {
            auto cell_opt = *_i++;
            if (!cell_opt) {
                return {};
            }
            return {result_atomic_cell_view(*cell_opt)};
        }
        std::optional<result_bytes_view> next_collection_cell() {
            auto cell_opt = *_i++;
            if (!cell_opt) {
                return {};
            }
            ser::qr_cell_view v = *cell_opt;
            return {v.value().view()};
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
        uint64_t row_count) {}

    void accept_new_partition(uint64_t row_count) {}

    void accept_new_row(
        const clustering_key& key, // FIXME: use view for the key
        const result_row_view& static_row,
        const result_row_view& row) {}

    void accept_new_row(const result_row_view& static_row, const result_row_view& row) {}

    void accept_partition_end(const result_row_view& static_row) {}
};

template<typename Visitor>
concept ResultVisitor = requires(Visitor visitor, const partition_key& pkey,
                                      uint64_t row_count, const clustering_key& ckey,
                                      const result_row_view& static_row, const result_row_view& row)
{
    visitor.accept_new_partition(pkey, row_count);
    visitor.accept_new_partition(row_count);
    visitor.accept_new_row(ckey, static_row, row);
    visitor.accept_new_row(static_row, row);
    visitor.accept_partition_end(static_row);
};

class result_view {
    ser::query_result_view _v;
    friend class result_merger;
public:
    result_view(const bytes_ostream& v) : _v(ser::query_result_view{ser::as_input_stream(v)}) {}
    result_view(ser::query_result_view v) : _v(v) {}
    explicit result_view(const query::result& res) : result_view(res.buf()) { }

    template <typename Func>
    static auto do_with(const query::result& res, Func&& func) {
        result_view view(res.buf());
        return func(view);
    }

    template <typename ResultVisitor>
    static void consume(const query::result& res, const partition_slice& slice, ResultVisitor&& visitor) {
        result_view(res).consume(slice, visitor);
    }

    template <typename Visitor>
    requires ResultVisitor<Visitor>
    void consume(const partition_slice& slice, Visitor&& visitor) const {
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

    std::tuple<uint32_t, uint64_t> count_partitions_and_rows() const {
        auto&& ps = _v.partitions();
        auto rows = boost::accumulate(ps | boost::adaptors::transformed([] (auto& p) {
            return std::max(p.rows().size(), size_t(1));
        }), uint64_t(0));
        return std::make_tuple(ps.size(), rows);
    }

    std::tuple<partition_key, std::optional<clustering_key>>
    get_last_partition_and_clustering_key() const {
        auto ps = _v.partitions();
        assert(!ps.empty());
        auto& p = ps.back();
        auto rs = p.rows();
        return { p.key().value(), !rs.empty() ? rs.back().key() : std::optional<clustering_key>() };
    }
};

}
