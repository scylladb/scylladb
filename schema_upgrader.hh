/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/mutation_fragment.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "converting_mutation_partition_applier.hh"

// A StreamedMutationTransformer which transforms the stream to a different schema
class schema_upgrader {
    schema_ptr _prev;
    schema_ptr _new;
    std::optional<reader_permit> _permit;
private:
    row transform(row&& r, column_kind kind) {
        row new_row;
        r.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) {
            const column_definition& col = _prev->column_at(kind, id);
            const column_definition* new_col = _new->get_column_definition(col.name());
            if (new_col) {
                converting_mutation_partition_applier::append_cell(new_row, kind, *new_col, col, std::move(cell));
            }
        });
        return new_row;
    }
public:
    schema_upgrader(schema_ptr s)
        : _new(std::move(s))
    { }
    schema_ptr operator()(schema_ptr old) {
        _prev = std::move(old);
        return _new;
    }
    mutation_fragment consume(static_row&& row) {
        return mutation_fragment(*_new, std::move(*_permit), static_row(transform(std::move(row.cells()), column_kind::static_column)));
    }
    mutation_fragment consume(clustering_row&& row) {
        return mutation_fragment(*_new, std::move(*_permit), clustering_row(row.key(), row.tomb(), row.marker(),
            transform(std::move(row.cells()), column_kind::regular_column)));
    }
    mutation_fragment consume(range_tombstone&& rt) {
        return mutation_fragment(*_new, std::move(*_permit), std::move(rt));
    }
    mutation_fragment consume(partition_start&& ph) {
        return mutation_fragment(*_new, std::move(*_permit), std::move(ph));
    }
    mutation_fragment consume(partition_end&& eop) {
        return mutation_fragment(*_new, std::move(*_permit), std::move(eop));
    }
    mutation_fragment operator()(mutation_fragment&& mf) {
        _permit = mf.permit();
        return std::move(mf).consume(*this);
    }
};

// A StreamedMutationTransformer which transforms the stream to a different schema
class schema_upgrader_v2 {
    schema_ptr _prev;
    schema_ptr _new;
    std::optional<reader_permit> _permit;
private:
    row transform(row&& r, column_kind kind) {
        row new_row;
        r.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) {
            const column_definition& col = _prev->column_at(kind, id);
            const column_definition* new_col = _new->get_column_definition(col.name());
            if (new_col) {
                converting_mutation_partition_applier::append_cell(new_row, kind, *new_col, col, std::move(cell));
            }
        });
        return new_row;
    }
public:
    schema_upgrader_v2(schema_ptr s)
        : _new(std::move(s))
    { }
    schema_ptr operator()(schema_ptr old) {
        _prev = std::move(old);
        return _new;
    }
    mutation_fragment_v2 consume(static_row&& row) {
        return mutation_fragment_v2(*_new, std::move(*_permit), static_row(transform(std::move(row.cells()), column_kind::static_column)));
    }
    mutation_fragment_v2 consume(clustering_row&& row) {
        return mutation_fragment_v2(*_new, std::move(*_permit), clustering_row(row.key(), row.tomb(), row.marker(),
            transform(std::move(row.cells()), column_kind::regular_column)));
    }
    mutation_fragment_v2 consume(range_tombstone_change&& rt) {
        return mutation_fragment_v2(*_new, std::move(*_permit), std::move(rt));
    }
    mutation_fragment_v2 consume(partition_start&& ph) {
        return mutation_fragment_v2(*_new, std::move(*_permit), std::move(ph));
    }
    mutation_fragment_v2 consume(partition_end&& eop) {
        return mutation_fragment_v2(*_new, std::move(*_permit), std::move(eop));
    }
    mutation_fragment_v2 operator()(mutation_fragment_v2&& mf) {
        _permit = mf.permit();
        return std::move(mf).consume(*this);
    }
};

static_assert(StreamedMutationTranformer<schema_upgrader>);
static_assert(StreamedMutationTranformerV2<schema_upgrader_v2>);
