/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <experimental/optional>
#include <vector>

#include "query-request.hh"
#include "schema.hh"

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
    std::experimental::optional<std::vector<column_id>> _regular_columns;
    std::experimental::optional<std::vector<column_id>> _static_columns;
    std::experimental::optional<std::vector<query::clustering_range>> _row_ranges;
    const schema& _schema;
    query::partition_slice::option_set _options;
public:
    partition_slice_builder(const schema& schema);

    partition_slice_builder& with_static_column(bytes name);
    partition_slice_builder& with_no_static_columns();
    partition_slice_builder& with_regular_column(bytes name);
    partition_slice_builder& with_no_regular_columns();
    partition_slice_builder& with_range(query::clustering_range range);

    query::partition_slice build();
};
