/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "selection/selection.hh"
#include "stats.hh"

namespace cql3 {
class untyped_result_set;

class result_generator {
    schema_ptr _schema;
    foreign_ptr<lw_shared_ptr<query::result>> _result;
    lw_shared_ptr<const query::read_command> _command;
    shared_ptr<const selection::selection> _selection;
    cql_stats* _stats;
private:
    friend class untyped_result_set;
    template<typename Visitor>
    class query_result_visitor {
        const schema& _schema;
        std::vector<bytes> _partition_key;
        std::vector<bytes> _clustering_key;
        uint64_t _partition_row_count = 0;
        uint64_t _total_row_count = 0;
        Visitor& _visitor;
        const selection::selection& _selection;
    private:
        void accept_cell_value(const column_definition& def, query::result_row_view::iterator_type& i) {
            if (def.is_multi_cell()) {
                _visitor.accept_value(i.next_collection_cell());
            } else {
                auto cell = i.next_atomic_cell();
                _visitor.accept_value(cell ? std::optional<query::result_bytes_view>(cell->value()) : std::optional<query::result_bytes_view>());
            }
        }
    public:
        query_result_visitor(const schema& s, Visitor& visitor, const selection::selection& select)
            : _schema(s), _visitor(visitor), _selection(select) { }

        void accept_new_partition(const partition_key& key, uint64_t row_count) {
            _partition_key = key.explode(_schema);
            accept_new_partition(row_count);
        }
        void accept_new_partition(uint64_t row_count) {
            _partition_row_count = row_count;
            _total_row_count += row_count;
        }

        void accept_new_row(const clustering_key& key, query::result_row_view static_row,
                            query::result_row_view row) {
            _clustering_key = key.explode(_schema);
            accept_new_row(static_row, row);
        }
        void accept_new_row(query::result_row_view static_row, query::result_row_view row) {
            auto static_row_iterator = static_row.iterator();
            auto row_iterator = row.iterator();
            _visitor.start_row();
            for (auto&& def : _selection.get_columns()) {
                switch (def->kind) {
                case column_kind::partition_key:
                    _visitor.accept_value(query::result_bytes_view(bytes_view(_partition_key[def->component_index()])));
                    break;
                case column_kind::clustering_key:
                    if (_clustering_key.size() > def->component_index()) {
                        _visitor.accept_value(query::result_bytes_view(bytes_view(_clustering_key[def->component_index()])));
                    } else {
                        _visitor.accept_value(std::nullopt);
                    }
                    break;
                case column_kind::regular_column:
                    accept_cell_value(*def, row_iterator);
                    break;
                case column_kind::static_column:
                    accept_cell_value(*def, static_row_iterator);
                    break;
                }
            }
            _visitor.end_row();
        }

        void accept_partition_end(const query::result_row_view& static_row) {
            if (_partition_row_count == 0) {
                _total_row_count++;
                _visitor.start_row();
                auto static_row_iterator = static_row.iterator();
                for (auto&& def : _selection.get_columns()) {
                    if (def->is_partition_key()) {
                        _visitor.accept_value(query::result_bytes_view(bytes_view(_partition_key[def->component_index()])));
                    } else if (def->is_static()) {
                        accept_cell_value(*def, static_row_iterator);
                    } else {
                        _visitor.accept_value(std::nullopt);
                    }
                }
                _visitor.end_row();
            }
        }

        uint64_t rows_read() const { return _total_row_count; }
    };
public:
    result_generator() = default;

    result_generator(schema_ptr s, foreign_ptr<lw_shared_ptr<query::result>> result, lw_shared_ptr<const query::read_command> cmd,
                     ::shared_ptr<const selection::selection> select, cql_stats& stats)
        : _schema(std::move(s))
        , _result(std::move(result))
        , _command(std::move(cmd))
        , _selection(std::move(select))
        , _stats(&stats)
    { }

    template<typename Visitor>
    void visit(Visitor&& visitor) const {
        query_result_visitor<Visitor> v(*_schema, visitor, *_selection);
        query::result_view::consume(*_result, _command->slice, v);
        _stats->rows_read += v.rows_read();
    }
};

}
