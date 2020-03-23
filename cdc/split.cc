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
#include "log.hh"

struct atomic_column_update {
    column_id id;
    atomic_cell cell;
};

// see the comment inside `clustered_row_insert` for motivation for separating
// nonatomic deletions from nonatomic updates
struct nonatomic_column_deletion {
    column_id id;
    tombstone t;
};

struct nonatomic_column_update {
    column_id id;
    utils::chunked_vector<std::pair<bytes, atomic_cell>> cells;
};

struct static_row_update {
    gc_clock::duration ttl;
    std::vector<atomic_column_update> atomic_entries;
    std::vector<nonatomic_column_deletion> nonatomic_deletions;
    std::vector<nonatomic_column_update> nonatomic_updates;
};

struct clustered_row_insert {
    gc_clock::duration ttl;
    clustering_key key;
    row_marker marker;
    std::vector<atomic_column_update> atomic_entries;
    std::vector<nonatomic_column_deletion> nonatomic_deletions;
    // INSERTs can't express updates of individual cells inside a non-atomic
    // (without deleting the entire field first), so no `nonatomic_updates` field
    // overwriting a nonatomic column inside an INSERT will be split into two changes:
    // one with a nonatomic deletion, and one with a nonatomic update
};

struct clustered_row_update {
    gc_clock::duration ttl;
    clustering_key key;
    std::vector<atomic_column_update> atomic_entries;
    std::vector<nonatomic_column_deletion> nonatomic_deletions;
    std::vector<nonatomic_column_update> nonatomic_updates;
};

struct clustered_row_deletion {
    clustering_key key;
    tombstone t;
};

struct clustered_range_deletion {
    range_tombstone rt;
};

struct partition_deletion {
    tombstone t;
};

struct batch {
    std::vector<static_row_update> static_updates;
    std::vector<clustered_row_insert> clustered_inserts;
    std::vector<clustered_row_update> clustered_updates;
    std::vector<clustered_row_deletion> clustered_row_deletions;
    std::vector<clustered_range_deletion> clustered_range_deletions;
    std::optional<partition_deletion> partition_deletions;
};

using set_of_changes = std::map<api::timestamp_type, batch>;

struct row_update {
    std::vector<atomic_column_update> atomic_entries;
    std::vector<nonatomic_column_deletion> nonatomic_deletions;
    std::vector<nonatomic_column_update> nonatomic_updates;
};

static
std::map<std::pair<api::timestamp_type, gc_clock::duration>, row_update>
extract_row_updates(const row& r, column_kind ckind, const schema& schema) {
    std::map<std::pair<api::timestamp_type, gc_clock::duration>, row_update> result;
    r.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        auto& cdef = schema.column_at(ckind, id);
        if (cdef.is_atomic()) {
            auto view = cell.as_atomic_cell(cdef);
            auto timestamp_and_ttl = std::pair(
                    view.timestamp(),
                    view.is_live_and_has_ttl() ? view.ttl() : gc_clock::duration(0)
                );
            result[timestamp_and_ttl].atomic_entries.push_back({id, atomic_cell(*cdef.type, view)});
            return;
        }

        cell.as_collection_mutation().with_deserialized(*cdef.type, [&] (collection_mutation_view_description mview) {
            auto desc = mview.materialize(*cdef.type);
            for (auto& [k, v]: desc.cells) {
                auto timestamp_and_ttl = std::pair(
                        v.timestamp(),
                        v.is_live_and_has_ttl() ? v.ttl() : gc_clock::duration(0)
                    );
                auto& updates = result[timestamp_and_ttl].nonatomic_updates;
                if (updates.empty() || updates.back().id != id) {
                    updates.push_back({id, {}});
                }
                updates.back().cells.push_back({std::move(k), std::move(v)});
            }

            if (desc.tomb) {
                auto timestamp_and_ttl = std::pair(desc.tomb.timestamp, gc_clock::duration(0));
                result[timestamp_and_ttl].nonatomic_deletions.push_back({id, desc.tomb});
            }
        });
    });
    return result;
};

set_of_changes extract_changes(const mutation& base_mutation, const schema& base_schema) {
    set_of_changes res;
    auto& p = base_mutation.partition();

    auto sr_updates = extract_row_updates(p.static_row().get(), column_kind::static_column, base_schema);
    for (auto& [k, up]: sr_updates) {
        auto [timestamp, ttl] = k;
        res[timestamp].static_updates.push_back({
                ttl,
                std::move(up.atomic_entries),
                std::move(up.nonatomic_deletions),
                std::move(up.nonatomic_updates)
            });
    }

    for (const rows_entry& cr : p.clustered_rows()) {
        auto cr_updates = extract_row_updates(cr.row().cells(), column_kind::regular_column, base_schema);

        const auto& marker = cr.row().marker();
        auto marker_timestamp = marker.timestamp();
        auto marker_ttl = marker.is_expiring() ? marker.ttl() : gc_clock::duration(0);
        if (marker.is_live()) {
            // make sure that an entry corresponding to the row marker's timestamp and ttl is in the map
            (void)cr_updates[std::pair(marker_timestamp, marker_ttl)];
        }

        auto is_insert = [&] (api::timestamp_type timestamp, gc_clock::duration ttl) {
            if (!marker.is_live()) {
                return false;
            }

            return timestamp == marker_timestamp && ttl == marker_ttl;
        };

        for (auto& [k, up]: cr_updates) {
            auto [timestamp, ttl] = k;

            if (is_insert(timestamp, ttl)) {
                res[timestamp].clustered_inserts.push_back({
                        ttl,
                        cr.key(),
                        marker,
                        std::move(up.atomic_entries),
                        std::move(up.nonatomic_deletions)
                    });
                if (!up.nonatomic_updates.empty()) {
                    // nonatomic updates cannot be expressed with an INSERT.
                    res[timestamp].clustered_updates.push_back({
                            ttl,
                            cr.key(),
                            {},
                            {},
                            std::move(up.nonatomic_updates)
                        });
                }
            } else {
                res[timestamp].clustered_updates.push_back({
                        ttl,
                        cr.key(),
                        std::move(up.atomic_entries),
                        std::move(up.nonatomic_deletions),
                        std::move(up.nonatomic_updates)
                    });
            }
        }

        auto row_tomb = cr.row().deleted_at().regular();
        if (row_tomb) {
            res[row_tomb.timestamp].clustered_row_deletions.push_back({cr.key(), row_tomb});
        }
    }

    for (const auto& rt: p.row_tombstones()) {
        if (rt.tomb.timestamp != api::missing_timestamp) {
            res[rt.tomb.timestamp].clustered_range_deletions.push_back({rt});
        }
    }

    auto partition_tomb_timestamp = p.partition_tombstone().timestamp;
    if (partition_tomb_timestamp != api::missing_timestamp) {
        res[partition_tomb_timestamp].partition_deletions = {p.partition_tombstone()};
    }

    return res;
}

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

    // A mutation with no timestamp will be split into 0 mutations
    return found_ts == api::missing_timestamp;
}

void for_each_change(const mutation& base_mutation, const schema_ptr& base_schema,
        seastar::noncopyable_function<void(mutation, api::timestamp_type, bytes, int&)> f) {
    auto changes = extract_changes(base_mutation, *base_schema);
    auto pk = base_mutation.key();

    for (auto& [change_ts, btch] : changes) {
        auto tuuid = timeuuid_type->decompose(generate_timeuuid(change_ts));
        int batch_no = 0;

        for (auto& sr_update : btch.static_updates) {
            mutation m(base_schema, pk);
            for (auto& atomic_update : sr_update.atomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::static_column, atomic_update.id);
                m.set_static_cell(cdef, std::move(atomic_update.cell));
            }
            for (auto& nonatomic_delete : sr_update.nonatomic_deletions) {
                auto& cdef = base_schema->column_at(column_kind::static_column, nonatomic_delete.id);
                m.set_static_cell(cdef, collection_mutation_description{nonatomic_delete.t, {}}.serialize(*cdef.type));
            }
            for (auto& nonatomic_update : sr_update.nonatomic_updates) {
                auto& cdef = base_schema->column_at(column_kind::static_column, nonatomic_update.id);
                m.set_static_cell(cdef, collection_mutation_description{{}, std::move(nonatomic_update.cells)}.serialize(*cdef.type));
            }
            f(std::move(m), change_ts, tuuid, batch_no);
        }

        for (auto& cr_insert : btch.clustered_inserts) {
            mutation m(base_schema, pk);

            auto& row = m.partition().clustered_row(*base_schema, cr_insert.key);
            for (auto& atomic_update : cr_insert.atomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, atomic_update.id);
                row.cells().apply(cdef, std::move(atomic_update.cell));
            }
            for (auto& nonatomic_delete : cr_insert.nonatomic_deletions) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, nonatomic_delete.id);
                row.cells().apply(cdef, collection_mutation_description{nonatomic_delete.t, {}}.serialize(*cdef.type));
            }
            row.apply(cr_insert.marker);

            f(std::move(m), change_ts, tuuid, batch_no);
        }

        for (auto& cr_update : btch.clustered_updates) {
            mutation m(base_schema, pk);

            auto& row = m.partition().clustered_row(*base_schema, cr_update.key).cells();
            for (auto& atomic_update : cr_update.atomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, atomic_update.id);
                row.apply(cdef, std::move(atomic_update.cell));
            }
            for (auto& nonatomic_delete : cr_update.nonatomic_deletions) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, nonatomic_delete.id);
                row.apply(cdef, collection_mutation_description{nonatomic_delete.t, {}}.serialize(*cdef.type));
            }
            for (auto& nonatomic_update : cr_update.nonatomic_updates) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, nonatomic_update.id);
                row.apply(cdef, collection_mutation_description{{}, std::move(nonatomic_update.cells)}.serialize(*cdef.type));
            }

            f(std::move(m), change_ts, tuuid, batch_no);
        }

        for (auto& cr_delete : btch.clustered_row_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply_delete(*base_schema, cr_delete.key, cr_delete.t);
            f(std::move(m), change_ts, tuuid, batch_no);
        }

        for (auto& crange_delete : btch.clustered_range_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply_delete(*base_schema, crange_delete.rt);
            f(std::move(m), change_ts, tuuid, batch_no);
        }

        if (btch.partition_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply(btch.partition_deletions->t);
            f(std::move(m), change_ts, tuuid, batch_no);
        }
    }
}

} // namespace cdc
