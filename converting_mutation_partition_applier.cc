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

#include "converting_mutation_partition_applier.hh"
#include "concrete_types.hh"

#include "mutation_partition_view.hh"
#include "mutation_partition.hh"
#include "schema.hh"

bool
converting_mutation_partition_applier::is_compatible(const column_definition& new_def, const abstract_type& old_type, column_kind kind) {
    return ::is_compatible(new_def.kind, kind) && new_def.type->is_value_compatible_with(old_type);
}

atomic_cell
converting_mutation_partition_applier::upgrade_cell(const abstract_type& new_type, const abstract_type& old_type, atomic_cell_view cell,
                                atomic_cell::collection_member cm) {
    if (cell.is_live() && !old_type.is_counter()) {
        if (cell.is_live_and_has_ttl()) {
            return atomic_cell::make_live(new_type, cell.timestamp(), cell.value(), cell.expiry(), cell.ttl(), cm);
        }
        return atomic_cell::make_live(new_type, cell.timestamp(), cell.value(), cm);
    } else {
        return atomic_cell(new_type, cell);
    }
}

void
converting_mutation_partition_applier::accept_cell(row& dst, column_kind kind, const column_definition& new_def, const abstract_type& old_type, atomic_cell_view cell) {
    if (!is_compatible(new_def, old_type, kind) || cell.timestamp() <= new_def.dropped_at()) {
        return;
    }
    dst.apply(new_def, upgrade_cell(*new_def.type, old_type, cell));
}

void
converting_mutation_partition_applier::accept_cell(row& dst, column_kind kind, const column_definition& new_def, const abstract_type& old_type, collection_mutation_view cell) {
    if (!is_compatible(new_def, old_type, kind)) {
        return;
    }

  cell.with_deserialized(old_type, [&] (collection_mutation_view_description old_view) {
    collection_mutation_description new_view;
    if (old_view.tomb.timestamp > new_def.dropped_at()) {
        new_view.tomb = old_view.tomb;
    }

    visit(old_type, make_visitor(
        [&] (const collection_type_impl& old_ctype) {
            assert(new_def.type->is_collection()); // because is_compatible
            auto& new_ctype = static_cast<const collection_type_impl&>(*new_def.type);

            auto& new_value_type = *new_ctype.value_comparator();
            auto& old_value_type = *old_ctype.value_comparator();

            for (auto& c : old_view.cells) {
                if (c.second.timestamp() > new_def.dropped_at()) {
                    new_view.cells.emplace_back(c.first, upgrade_cell(
                            new_value_type, old_value_type, c.second, atomic_cell::collection_member::yes));
                }
            }
        },
        [&] (const user_type_impl& old_utype) {
            assert(new_def.type->is_user_type()); // because is_compatible
            auto& new_utype = static_cast<const user_type_impl&>(*new_def.type);

            for (auto& c : old_view.cells) {
                if (c.second.timestamp() > new_def.dropped_at()) {
                    auto idx = deserialize_field_index(c.first);
                    assert(idx < new_utype.size() && idx < old_utype.size());

                    new_view.cells.emplace_back(c.first, upgrade_cell(
                            *new_utype.type(idx), *old_utype.type(idx), c.second, atomic_cell::collection_member::yes));
                }
            }
        },
        [&] (const abstract_type& o) {
            throw std::runtime_error(format("not a multi-cell type: {}", o.name()));
        }
    ));

    if (new_view.tomb || !new_view.cells.empty()) {
        dst.apply(new_def, new_view.serialize(*new_def.type));
    }
  });
}

converting_mutation_partition_applier::converting_mutation_partition_applier(
        const column_mapping& visited_column_mapping,
        const schema& target_schema,
        mutation_partition& target)
    : _p_schema(target_schema)
    , _p(target)
    , _visited_column_mapping(visited_column_mapping)
{ }


void
converting_mutation_partition_applier::accept_partition_tombstone(tombstone t) {
    _p.apply(t);
}

void
converting_mutation_partition_applier::accept_static_cell(column_id id, atomic_cell cell) {
    return accept_static_cell(id, atomic_cell_view(cell));
}

void
converting_mutation_partition_applier::accept_static_cell(column_id id, atomic_cell_view cell) {
    const column_mapping_entry& col = _visited_column_mapping.static_column_at(id);
    const column_definition* def = _p_schema.get_column_definition(col.name());
    if (def) {
        accept_cell(_p._static_row.maybe_create(), column_kind::static_column, *def, *col.type(), cell);
    }
}

void
converting_mutation_partition_applier::accept_static_cell(column_id id, collection_mutation_view collection) {
    const column_mapping_entry& col = _visited_column_mapping.static_column_at(id);
    const column_definition* def = _p_schema.get_column_definition(col.name());
    if (def) {
        accept_cell(_p._static_row.maybe_create(), column_kind::static_column, *def, *col.type(), collection);
    }
}

void
converting_mutation_partition_applier::accept_row_tombstone(const range_tombstone& rt) {
    _p.apply_row_tombstone(_p_schema, rt);
}

void
converting_mutation_partition_applier::accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) {
    deletable_row& r = _p.clustered_row(_p_schema, key, dummy, continuous);
    r.apply(rm);
    r.apply(deleted_at);
    _current_row = &r;
}

void
converting_mutation_partition_applier::accept_row_cell(column_id id, atomic_cell cell) {
    return accept_row_cell(id, atomic_cell_view(cell));
}

void
converting_mutation_partition_applier::accept_row_cell(column_id id, atomic_cell_view cell) {
    const column_mapping_entry& col = _visited_column_mapping.regular_column_at(id);
    const column_definition* def = _p_schema.get_column_definition(col.name());
    if (def) {
        accept_cell(_current_row->cells(), column_kind::regular_column, *def, *col.type(), cell);
    }
}

void
converting_mutation_partition_applier::accept_row_cell(column_id id, collection_mutation_view collection) {
    const column_mapping_entry& col = _visited_column_mapping.regular_column_at(id);
    const column_definition* def = _p_schema.get_column_definition(col.name());
    if (def) {
        accept_cell(_current_row->cells(), column_kind::regular_column, *def, *col.type(), collection);
    }
}

void
converting_mutation_partition_applier::append_cell(row& dst, column_kind kind, const column_definition& new_def, const column_definition& old_def, const atomic_cell_or_collection& cell) {
    if (new_def.is_atomic()) {
        accept_cell(dst, kind, new_def, *old_def.type, cell.as_atomic_cell(old_def));
    } else {
        accept_cell(dst, kind, new_def, *old_def.type, cell.as_collection_mutation());
    }
}
