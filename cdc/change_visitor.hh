/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "mutation.hh"

/*
 * This file contains a general abstraction for walking over mutations,
 * deconstructing them into ``atomic'' pieces, and consuming these pieces.
 *
 * The pieces considered atomic are:
 * - atomic_cells, either in collections or in atomic columns
 *   (see `live_collection_cell`, `dead_collection_cell`, `live_atomic_cell`, `dead_atomic_cell`),
 * - collection tombstones (see `collection_tombstone`)
 * - row markers (see `marker`)
 * - row tombstones (see `clustered_row_delete`),
 * - range tombstones (see `range_delete`),
 * - partition tombstones (see `partition_delete`).
 * We use the term ``changes'' to refer to these atomic pieces, hence the name ``ChangeVisitor''.
 *
 * IMPORTANT: this doesn't understand all possible states that a mutation can have, e.g. it doesn't understand
 * the concept of ``continuity''. However, it is sufficient for analyzing mutations created by a write coordinator,
 * e.g. obtained by parsing a CQL statement.
 *
 * To analyze a mutation, create a visitor (described by the `ChangeVisitor` concept below) and pass it
 * together with the mutation to `inspect_mutation`.
 *
 * To analyze certain fragments of the mutation, the inspecting code requires further visitors to be passed.
 * For example, when it encounters a clustered row update, it calls `clustered_row_cells` on the visitor,
 * passing it the row's key and the callback. The visitor can then decide:
 * - if it's not interested in the row's cells, it can simply not call the callback,
 * - otherwise, it can call the callback with a value of type that satisfies the ``RowCellsVisitor'' concept.
 * If the callback is called, the inspector walks over the row and passes the changes into the ``row cells visitor''.
 * In either case, it will then proceed to analyze further parts of the mutation, if any.
 *
 * Note that the type passed to the callbacks provided by the inspector (such as in the example above)
 * can be decided at runtime. This can be especially useful with the callback passed to `collection_column`
 * in RowCellsVisitor, if different collection types require different logic to handle.
 *
 * The dummy visitors below are there only to define the concepts.
 * For example, in the RowCellsVisitor concept I wanted to express that `visit_collection` in RowCellsVisitor
 * is a function that handles *any* type which satisfies CollectionVisitor. I didn't find a way to do that
 * other than providing a ``most generic'' concrete type which satisfies the interface (`dummy_collection_visitor`).
 * Unfortunately C++ is still not Haskell.
 *
 * The inspector calls `finished()` after visiting each change, and sometimes before (e.g. when it starts
 * visiting a static row, but before it visits any of its cells). If it returns true, the inspector
 * will stop the visitation. Thus, if at any point during the walk the visitor decides it's not interested
 * in any more changes, it can inform the inspector by returning `true` from `finished()`.
 *
 * IMPORTANT: if the visitor returns `true` from `finished()`, it should keep returning `true`. This is because
 * the inspector may call `finished()` multiple times when exiting some nested loops.
 *
 * The order of visitation is as follows:
 * - First the static row is visited, if it has any cells.
 *   Within the row, its columns are visited in order of increasing column IDs.
 *
 * - Then, for each clustering key, if a change (row marker, cell, or tombstone) exists for this key:
 *   - The row marker is visited, if there is one.
 *   - Columns are visited in order of increasing column IDs.
 *   - The row tombstone is visited, if there is one.
 *
 * For both the static row and a clustering row, for each column:
 * - If the column is atomic, a corresponding atomic_cell is visited (if there is one).
 * - Otherwise (the column is non-atomic):
 *   - The collection tombstone is visited first.
 *   - Cells are visited in order of increasing keys
 *     (assuming that the mutation was correctly constructed, i.e. it stores cells in key order).
 *
 * WARNING: visited collection tombstone and cells
 * are guaranteed to live only for the duration of `collection_column` call.
 *
 * - Then range tombstones are visited. The order is unspecified
 *   (more accurately: if it's specified, I don't know what it is)
 *
 * - Finally, the partition tombstone is visited, if it exists.
 */

namespace cdc {

template <typename V>
concept CollectionVisitor = requires(V v,
        const tombstone& t,
        bytes_view key,
        const atomic_cell_view& cell) {

    { v.collection_tombstone(t) }         -> std::same_as<void>;
    { v.live_collection_cell(key, cell) } -> std::same_as<void>;
    { v.dead_collection_cell(key, cell) } -> std::same_as<void>;
    { v.finished() } -> std::same_as<bool>;
};

struct dummy_collection_visitor {
    void collection_tombstone(const tombstone&) {}
    void live_collection_cell(bytes_view, const atomic_cell_view&) {}
    void dead_collection_cell(bytes_view, const atomic_cell_view&) {}
    bool finished() { return false; }
};

template <typename V>
concept RowCellsVisitor = requires(V v,
        const column_definition& cdef,
        const atomic_cell_view& cell,
        noncopyable_function<void(dummy_collection_visitor&)> visit_collection) {

    { v.live_atomic_cell(cdef, cell) }                         -> std::same_as<void>;
    { v.dead_atomic_cell(cdef, cell) }                         -> std::same_as<void>;
    { v.collection_column(cdef, std::move(visit_collection)) } -> std::same_as<void>;
    { v.finished() }                                           -> std::same_as<bool>;
};

struct dummy_row_cells_visitor {
    void live_atomic_cell(const column_definition&, const atomic_cell_view&) {}
    void dead_atomic_cell(const column_definition&, const atomic_cell_view&) {}
    void collection_column(const column_definition&, auto&& visit_collection) {
        dummy_collection_visitor v;
        visit_collection(v);
    }
    bool finished() { return false; }
};

template <typename V>
concept ClusteredRowCellsVisitor = requires(V v,
        const row_marker& rm) {
    requires RowCellsVisitor<V>;
    { v.marker(rm) } -> std::same_as<void>;
};

struct dummy_clustered_row_cells_visitor : public dummy_row_cells_visitor {
    void marker(const row_marker&) {}
};

template <typename V>
concept ChangeVisitor = requires(V v,
        api::timestamp_type ts,
        const clustering_key& ckey,
        const range_tombstone& rt,
        const tombstone& t,
        noncopyable_function<void(dummy_clustered_row_cells_visitor&)> visit_clustered_row_cells,
        noncopyable_function<void(dummy_row_cells_visitor&)> visit_row_cells) {

    { v.static_row_cells(std::move(visit_row_cells)) }                    -> std::same_as<void>;
    { v.clustered_row_cells(ckey, std::move(visit_clustered_row_cells)) } -> std::same_as<void>;
    { v.clustered_row_delete(ckey, t) }                                   -> std::same_as<void>;
    { v.range_delete(rt) }                                                -> std::same_as<void>;
    { v.partition_delete(t) }                                             -> std::same_as<void>;
    { v.finished() }                                                      -> std::same_as<bool>;
};

template <RowCellsVisitor V>
void inspect_row_cells(const schema& s, column_kind ckind, const row& r, V& v) {
    r.for_each_cell_until([&s, ckind, &v] (column_id id, const atomic_cell_or_collection& acoc) {
        auto& cdef = s.column_at(ckind, id);

        if (cdef.is_atomic()) {
            auto cell = acoc.as_atomic_cell(cdef);
            if (cell.is_live()) {
                v.live_atomic_cell(cdef, cell);
            } else {
                v.dead_atomic_cell(cdef, cell);
            }

            return stop_iteration(v.finished());
        }

        acoc.as_collection_mutation().with_deserialized(*cdef.type, [&v, &cdef] (collection_mutation_view_description view) {
            v.collection_column(cdef, [&view] (CollectionVisitor auto& cv) {
                if (cv.finished()) {
                    return;
                }

                if (view.tomb) {
                    cv.collection_tombstone(view.tomb);
                    if (cv.finished()) {
                        return;
                    }
                }

                for (auto& [key, cell]: view.cells) {
                    if (cell.is_live()) {
                        cv.live_collection_cell(key, cell);
                    } else {
                        cv.dead_collection_cell(key, cell);
                    }

                    if (cv.finished()) {
                        return;
                    }
                }
            });
        });

        return stop_iteration(v.finished());
    });
}

template <ChangeVisitor V>
void inspect_mutation(const mutation& m, V& v) {
    auto& p = m.partition();
    auto& s = *m.schema();

    if (!p.static_row().empty()) {
        v.static_row_cells([&s, &p] (RowCellsVisitor auto& srv) {
            if (srv.finished()) {
                return;
            }
            inspect_row_cells(s, column_kind::static_column, p.static_row().get(), srv);
        });

        if (v.finished()) {
            return;
        }
    }

    for (auto& cr: p.clustered_rows()) {
        auto& r = cr.row();

        if (r.marker().is_live() || !r.cells().empty()) {
            v.clustered_row_cells(cr.key(), [&s, &r] (ClusteredRowCellsVisitor auto& crv) {
                if (crv.finished()) {
                    return;
                }

                auto& rm = r.marker();
                if (rm.is_live()) {
                    crv.marker(rm);

                    if (crv.finished()) {
                        return;
                    }
                }

                inspect_row_cells(s, column_kind::regular_column, r.cells(), crv);
            });

            if (v.finished()) {
                return;
            }
        }

        if (r.deleted_at()) {
            auto t = r.deleted_at().tomb();
            assert(t.timestamp != api::missing_timestamp);
            v.clustered_row_delete(cr.key(), t);
            if (v.finished()) {
                return;
            }
        }
    }

    for (auto& rt: p.row_tombstones()) {
        assert(rt.tomb.timestamp != api::missing_timestamp);
        v.range_delete(rt);
        if (v.finished()) {
            return;
        }
    }

    if (p.partition_tombstone()) {
        v.partition_delete(p.partition_tombstone());
    }
}

} // namespace cdc
