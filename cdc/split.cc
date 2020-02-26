/*
 * Copyright (C) 2020 ScyllaDB
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

#include "mutation.hh"
#include "schema.hh"

#include "split.hh"

namespace cdc {

bool should_split(const mutation& base_mutation, const schema& base_schema) {
    auto& p = base_mutation.partition();

    api::timestamp_type found_ts = api::missing_timestamp;
    std::optional<gc_clock::duration> found_ttl; // 0 = "no ttl"

    auto check_or_set = [&] (api::timestamp_type ts, gc_clock::duration ttl) {
        if (found_ts != api::missing_timestamp && found_ts != ts) {
            return true;
        }
        found_ts = ts;

        if (found_ttl && *found_ttl != ttl) {
            return true;
        }
        found_ttl = ttl;

        return false;
    };

    bool had_static_row = false;

    bool should_split = false;
    p.static_row().get().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        had_static_row = true;

        auto& cdef = base_schema.column_at(column_kind::static_column, id);
        if (cdef.is_atomic()) {
            auto view = cell.as_atomic_cell(cdef);
            if (check_or_set(view.timestamp(), view.is_live_and_has_ttl() ? view.ttl() : gc_clock::duration(0))) {
                should_split = true;
            }
            return;
        }

        cell.as_collection_mutation().with_deserialized(*cdef.type, [&] (collection_mutation_view_description mview) {
            auto desc = mview.materialize(*cdef.type);
            for (auto& [k, v]: desc.cells) {
                if (check_or_set(v.timestamp(), v.is_live_and_has_ttl() ? v.ttl() : gc_clock::duration(0))) {
                    should_split = true;
                    return;
                }
            }

            if (desc.tomb) {
                if (check_or_set(desc.tomb.timestamp, gc_clock::duration(0))) {
                    should_split = true;
                    return;
                }
            }
        });
    });

    if (should_split) {
        return true;
    }

    bool had_clustered_row = false;

    if (!p.clustered_rows().empty() && had_static_row) {
        return true;
    }
    for (const rows_entry& cr : p.clustered_rows()) {
        had_clustered_row = true;

        const auto& marker = cr.row().marker();
        if (marker.is_live() && check_or_set(marker.timestamp(), marker.is_expiring() ? marker.ttl() : gc_clock::duration(0))) {
            return true;
        }

        bool is_insert = marker.is_live();

        bool had_cells = false;
        cr.row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            had_cells = true;

            auto& cdef = base_schema.column_at(column_kind::regular_column, id);
            if (cdef.is_atomic()) {
                auto view = cell.as_atomic_cell(cdef);
                if (check_or_set(view.timestamp(), view.is_live_and_has_ttl() ? view.ttl() : gc_clock::duration(0))) {
                    should_split = true;
                }
                return;
            }

            cell.as_collection_mutation().with_deserialized(*cdef.type, [&] (collection_mutation_view_description mview) {
                for (auto& [k, v]: mview.cells) {
                    if (check_or_set(v.timestamp(), v.is_live_and_has_ttl() ? v.ttl() : gc_clock::duration(0))) {
                        should_split = true;
                        return;
                    }

                    if (is_insert) {
                        // nonatomic updates cannot be expressed with an INSERT.
                        should_split = true;
                        return;
                    }
                }

                if (mview.tomb) {
                    if (check_or_set(mview.tomb.timestamp, gc_clock::duration(0))) {
                        should_split = true;
                        return;
                    }
                }
            });
        });

        if (should_split) {
            return true;
        }

        auto row_tomb = cr.row().deleted_at().regular();
        if (row_tomb) {
            if (had_cells) {
                return true;
            }

            // there were no cells, so no ttl
            assert(!found_ttl);
            if (found_ts != api::missing_timestamp && found_ts != row_tomb.timestamp) {
                return true;
            }

            found_ts = row_tomb.timestamp;
        }
    }

    if (!p.row_tombstones().empty() && (had_static_row || had_clustered_row)) {
        return true;
    }

    for (const auto& rt: p.row_tombstones()) {
        if (rt.tomb) {
            if (found_ts != api::missing_timestamp && found_ts != rt.tomb.timestamp) {
                return true;
            }

            found_ts = rt.tomb.timestamp;
        }
    }

    if (p.partition_tombstone().timestamp != api::missing_timestamp
            && (!p.row_tombstones().empty() || had_static_row || had_clustered_row)) {
        return true;
    }

    return false;
}

} // namespace cdc
