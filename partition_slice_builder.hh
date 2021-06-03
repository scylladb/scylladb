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

#include <optional>
#include <vector>

#include "query-request.hh"
#include "schema_fwd.hh"

//
// Fluent builder for query::partition_slice.
//
// Selects everything by default, unless restricted. Each property can be
// restricted separately. For example, by default all static columns are
// selected, but if with_static_column() is called then only that column will
// be included. Still, all regular columns and the whole clustering range will
// be selected (unless restricted).
//
class partition_slice_builder {
    std::optional<query::column_id_vector> _regular_columns;
    std::optional<query::column_id_vector> _static_columns;
    std::optional<std::vector<query::clustering_range>> _row_ranges;
    const schema& _schema;
    query::partition_slice::option_set _options;
public:
    partition_slice_builder(const schema& schema);

    partition_slice_builder& with_static_column(bytes name);
    partition_slice_builder& with_no_static_columns();
    partition_slice_builder& with_regular_column(bytes name);
    partition_slice_builder& with_no_regular_columns();
    partition_slice_builder& with_range(query::clustering_range range);
    partition_slice_builder& with_ranges(std::vector<query::clustering_range>);
    partition_slice_builder& without_partition_key_columns();
    partition_slice_builder& without_clustering_key_columns();
    partition_slice_builder& reversed();
    template <query::partition_slice::option OPTION>
    partition_slice_builder& with_option() {
        _options.set<OPTION>();
        return *this;
    }

    query::partition_slice build();
};
