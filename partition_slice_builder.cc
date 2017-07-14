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

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>

#include "partition_slice_builder.hh"

partition_slice_builder::partition_slice_builder(const schema& schema)
    : _schema(schema)
{
    _options.set<query::partition_slice::option::send_partition_key>();
    _options.set<query::partition_slice::option::send_clustering_key>();
    _options.set<query::partition_slice::option::send_timestamp>();
    _options.set<query::partition_slice::option::send_expiry>();
}

query::partition_slice
partition_slice_builder::build() {
    std::vector<query::clustering_range> ranges;
    if (_row_ranges) {
        ranges = std::move(*_row_ranges);
    } else {
        ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
    }

    std::vector<column_id> static_columns;
    if (_static_columns) {
        static_columns = std::move(*_static_columns);
    } else {
        boost::range::push_back(static_columns,
            _schema.static_columns() | boost::adaptors::transformed(std::mem_fn(&column_definition::id)));
    }

    std::vector<column_id> regular_columns;
    if (_regular_columns) {
        regular_columns = std::move(*_regular_columns);
    } else {
        boost::range::push_back(regular_columns,
            _schema.regular_columns() | boost::adaptors::transformed(std::mem_fn(&column_definition::id)));
    }

    return {
        std::move(ranges),
        std::move(static_columns),
        std::move(regular_columns),
        std::move(_options)
    };
}

partition_slice_builder&
partition_slice_builder::with_range(query::clustering_range range) {
    if (!_row_ranges) {
        _row_ranges = std::vector<query::clustering_range>();
    }
    _row_ranges->emplace_back(std::move(range));
    return *this;
}

partition_slice_builder&
partition_slice_builder::with_ranges(std::vector<query::clustering_range> ranges) {
    if (!_row_ranges) {
        _row_ranges = std::move(ranges);
    } else {
        for (auto&& r : ranges) {
            with_range(std::move(r));
        }
    }
    return *this;
}

partition_slice_builder&
partition_slice_builder::with_no_regular_columns() {
    _regular_columns = std::vector<column_id>();
    return *this;
}

partition_slice_builder&
partition_slice_builder::with_regular_column(bytes name) {
    if (!_regular_columns) {
        _regular_columns = std::vector<column_id>();
    }

    const column_definition* def = _schema.get_column_definition(name);
    if (!def) {
        throw std::runtime_error(sprint("No such column: %s", _schema.regular_column_name_type()->to_string(name)));
    }
    if (!def->is_regular()) {
        throw std::runtime_error(sprint("Column is not regular: %s", _schema.column_name_type(*def)->to_string(name)));
    }
    _regular_columns->push_back(def->id);
    return *this;
}

partition_slice_builder&
partition_slice_builder::with_no_static_columns() {
    _static_columns = std::vector<column_id>();
    return *this;
}

partition_slice_builder&
partition_slice_builder::with_static_column(bytes name) {
    if (!_static_columns) {
        _static_columns = std::vector<column_id>();
    }

    const column_definition* def = _schema.get_column_definition(name);
    if (!def) {
        throw std::runtime_error(sprint("No such column: %s", utf8_type->to_string(name)));
    }
    if (!def->is_static()) {
        throw std::runtime_error(sprint("Column is not static: %s", utf8_type->to_string(name)));
    }
    _static_columns->push_back(def->id);
    return *this;
}

partition_slice_builder&
partition_slice_builder::reversed() {
    _options.set<query::partition_slice::option::reversed>();
    return *this;
}

partition_slice_builder&
partition_slice_builder::without_partition_key_columns() {
    _options.remove<query::partition_slice::option::send_partition_key>();
    return *this;
}

partition_slice_builder&
partition_slice_builder::without_clustering_key_columns() {
    _options.remove<query::partition_slice::option::send_clustering_key>();
    return *this;
}
