/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "mutation_fragment.hh"
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

static_assert(StreamedMutationTranformer<schema_upgrader>);
