/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/mutation_cell_collector.hh"

namespace cql3 {

row::cell_appender* mutation_cell_collector::appender_for(const clustering_key_prefix& prefix, const column_definition& def) {
    std::optional<column_id>* last;
    row::cell_appender* appender;
    if (def.is_regular()) {
        if (&prefix != &_prefix && !prefix.equal(*_m.schema(), _prefix)) {
            // Not expected, all regular cells written by a statement's operations
            // belong to the same row.
            return nullptr;
        }
        last = &_last_regular_column;
        appender = &_regular_appender;
    } else if (def.is_static()) {
        last = &_last_static_column;
        appender = &_static_appender;
    } else {
        throw std::runtime_error("attempting to store into a key cell");
    }
    if (*last && def.id <= **last) {
        // Out of order, or a column written again (e.g. a tombstone and then
        // the new elements of a collection); cells merge in any order.
        return nullptr;
    }
    *last = def.id;
    return appender;
}

void mutation_cell_collector::set_cell(const clustering_key_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value) {
    if (auto* appender = appender_for(prefix, def)) {
        appender->append(def.id, std::move(value));
    } else {
        _m.set_cell(prefix, def, std::move(value));
    }
}

void mutation_cell_collector::finish() {
    _static_appender.finish();
    _regular_appender.finish();
    const auto& s = *_m.schema();
    if (!_static_cells.empty()) {
        _m.partition().static_row().apply(s, column_kind::static_column, std::move(_static_cells));
    }
    if (!_regular_cells.empty()) {
        _m.partition().clustered_row(s, _prefix).cells().apply(s, column_kind::regular_column, std::move(_regular_cells));
    }
}

}
