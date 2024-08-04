/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/numeric.hpp>

#include "query-result.hh"
#include "full_position.hh"

#include "idl/query.dist.hh"
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
        using cells_deserializer = ser::vector_deserializer<std::optional<ser::qr_cell_view>>;
        cells_deserializer _cells;
        cells_deserializer::iterator _i;
    public:
        iterator_type(const ser::qr_row_view& v)
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
        auto ps = _v.partitions();
        uint64_t rows = 0;
        for (auto p : ps) {
            rows += std::max(p.rows().size(), size_t(1));
        }
        return std::make_tuple(ps.size(), rows);
    }

    full_position calculate_last_position() const {
        auto ps = _v.partitions();
        SCYLLA_ASSERT(!ps.empty());
        auto pit = ps.begin();
        auto pnext = pit;
        while (++pnext != ps.end()) {
            pit = pnext;
        }
        auto p = *pit;
        auto rs = p.rows();
        auto pos = position_in_partition::for_partition_start();
        if (!rs.empty()) {
            auto rit = rs.begin();
            auto rnext = rit;
            while (++rnext != rs.end()) {
                rit = rnext;
            }
            const auto& key_opt = (*rit).key();
            if (key_opt) {
                pos = position_in_partition::for_key(*key_opt);
            }
        }
        return { p.key().value(), std::move(pos) };
    }
};

}
