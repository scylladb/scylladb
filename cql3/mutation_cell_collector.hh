/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>

#include "mutation/mutation.hh"

namespace cql3 {

// Collects the cells which the operations of a statement write to a row of a
// mutation, and adds them to the mutation on finish(), a block of cells at a
// time, rather than one cell at a time.
//
// Cells are expected in column order (static columns, then regular ones), as
// written by operations sorted by column (see modification_statement::add_operation()).
// Cells which come out of order, or for a column already written, are added to
// the mutation directly.
//
// Provides the subset of mutation's interface used by operations.
class mutation_cell_collector {
    mutation& _m;
    // The clustering prefix of the row the regular cells belong to.
    const clustering_key_prefix& _prefix;
    row _static_cells;
    row _regular_cells;
    row::cell_appender _static_appender{_static_cells};
    row::cell_appender _regular_appender{_regular_cells};
    std::optional<column_id> _last_static_column;
    std::optional<column_id> _last_regular_column;

    // Returns the appender to append the cell of def to, or nullptr if
    // the cell should be applied to the mutation directly.
    row::cell_appender* appender_for(const clustering_key_prefix& prefix, const column_definition& def);
public:
    // Operations are expected to write regular cells to the row at `prefix`, which
    // must stay alive until finish().
    mutation_cell_collector(mutation& m, const clustering_key_prefix& prefix) noexcept : _m(m), _prefix(prefix) {}
    mutation_cell_collector(const mutation_cell_collector&) = delete;
    mutation_cell_collector& operator=(const mutation_cell_collector&) = delete;

    const partition_key& key() const { return _m.key(); }

    // Like mutation::set_cell(); the cell is added to the mutation by finish().
    void set_cell(const clustering_key_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value);

    // Like set_cell(), for a cell whose serialized form of `size` bytes is written by
    // write(managed_bytes_mutable_view), avoiding a separate allocation for it.
    template <std::invocable<managed_bytes_mutable_view> Writer>
    void set_serialized_cell(const clustering_key_prefix& prefix, const column_definition& def, size_t size, Writer&& write) {
        if (auto* appender = appender_for(prefix, def)) {
            appender->append_serialized(def.id, size, write);
        } else {
            managed_bytes cell(managed_bytes::initialized_later(), size);
            write(managed_bytes_mutable_view(cell));
            _m.set_cell(prefix, def, atomic_cell_or_collection::from_serialized(std::move(cell)));
        }
    }

    // Adds the collected cells to the mutation. Must be called before the mutation is used.
    void finish();
};

}
