/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <vector>

#include "query-request.hh"
#include "schema/schema_fwd.hh"

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
    std::unique_ptr<query::specific_ranges> _specific_ranges;
    const schema& _schema;
    query::partition_slice::option_set _options;
    uint64_t _partition_row_limit = query::partition_max_rows;
public:
    partition_slice_builder(const schema& schema);
    partition_slice_builder(const schema& schema, query::partition_slice slice);

    partition_slice_builder& with_static_column(bytes name);
    partition_slice_builder& with_no_static_columns();
    partition_slice_builder& with_regular_column(bytes name);
    partition_slice_builder& with_no_regular_columns();
    partition_slice_builder& with_range(query::clustering_range range);
    partition_slice_builder& with_ranges(std::vector<query::clustering_range>);
    // noop if no ranges have been set yet
    partition_slice_builder& mutate_ranges(std::function<void(std::vector<query::clustering_range>&)>);
    // noop if no specific ranges have been set yet
    partition_slice_builder& mutate_specific_ranges(std::function<void(query::specific_ranges&)>);
    partition_slice_builder& set_specific_ranges(query::specific_ranges);
    partition_slice_builder& without_partition_key_columns();
    partition_slice_builder& without_clustering_key_columns();
    partition_slice_builder& reversed();
    template <query::partition_slice::option OPTION>
    partition_slice_builder& with_option() {
        _options.set<OPTION>();
        return *this;
    }
    template <query::partition_slice::option OPTION>
    partition_slice_builder& with_option_toggled() {
        _options.toggle<OPTION>();
        return *this;
    }

    partition_slice_builder& with_partition_row_limit(uint64_t limit);

    query::partition_slice build();
};
