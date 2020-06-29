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

struct nonatomic_column_update {
    column_id id;
    tombstone t; // optional
    utils::chunked_vector<std::pair<bytes, atomic_cell>> cells;
};

struct static_row_update {
    gc_clock::duration ttl;
    std::vector<atomic_column_update> atomic_entries;
    std::vector<nonatomic_column_update> nonatomic_entries;
};

struct clustered_row_insert {
    gc_clock::duration ttl;
    clustering_key key;
    row_marker marker;
    std::vector<atomic_column_update> atomic_entries;
    std::vector<nonatomic_column_update> nonatomic_entries;
};

struct clustered_row_update {
    gc_clock::duration ttl;
    clustering_key key;
    std::vector<atomic_column_update> atomic_entries;
    std::vector<nonatomic_column_update> nonatomic_entries;
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
    std::vector<nonatomic_column_update> nonatomic_entries;
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
                auto& updates = result[timestamp_and_ttl].nonatomic_entries;
                if (updates.empty() || updates.back().id != id) {
                    updates.push_back({id, {}});
                }
                updates.back().cells.push_back({std::move(k), std::move(v)});
            }

            if (desc.tomb) {
                auto timestamp_and_ttl = std::pair(desc.tomb.timestamp + 1, gc_clock::duration(0));
                auto& updates = result[timestamp_and_ttl].nonatomic_entries;
                if (updates.empty() || updates.back().id != id) {
                    updates.push_back({id, {}});
                }
                updates.back().t = std::move(desc.tomb);
            }
        });
    });
    return result;
};

set_of_changes extract_changes(const mutation& base_mutation) {
    set_of_changes res;
    auto& p = base_mutation.partition();
    const auto& base_schema = *base_mutation.schema();

    auto sr_updates = extract_row_updates(p.static_row().get(), column_kind::static_column, base_schema);
    for (auto& [k, up]: sr_updates) {
        auto [timestamp, ttl] = k;
        res[timestamp].static_updates.push_back({
                ttl,
                std::move(up.atomic_entries),
                std::move(up.nonatomic_entries)
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
            // It is important that changes in the resulting `set_of_changes` are listed
            // in increasing TTL order. The reason is explained in a comment in cdc/log.cc,
            // search for "#6070".
            auto [timestamp, ttl] = k;

            if (is_insert(timestamp, ttl)) {
                res[timestamp].clustered_inserts.push_back({
                        ttl,
                        cr.key(),
                        marker,
                        std::move(up.atomic_entries),
                        {}
                    });

                auto& cr_insert = res[timestamp].clustered_inserts.back();
                bool clustered_update_exists = false;
                for (auto& nonatomic_up: up.nonatomic_entries) {
                    // Updating a collection column with an INSERT statement implies inserting a tombstone.
                    //
                    // For example, suppose that we have:
                    //     CREATE TABLE t (a int primary key, b map<int, int>);
                    // Then the following statement:
                    //     INSERT INTO t (a, b) VALUES (0, {0:0}) USING TIMESTAMP T;
                    // creates a tombstone in column b with timestamp T-1.
                    // It also creates a cell (0, 0) with timestamp T.
                    //
                    // There is no way to create just the cell using an INSERT statement.
                    // This can only be done using an UPDATE, as follows:
                    //     UPDATE t USING TIMESTAMP T SET b = b + {0:0} WHERE a = 0;
                    // note that this is different  than
                    //     UPDATE t USING TIMESTAMP T SET b = {0:0} WHERE a = 0;
                    // which also creates a tombstone with timestamp T-1.
                    //
                    // It follows that:
                    // - if `nonatomic_up` has a tombstone, it can be made merged with our `cr_insert`,
                    //   which represents an INSERT change.
                    // - but if `nonatomic_up` only has cells, we must create a separate UPDATE change
                    //   for the cells alone.
                    if (nonatomic_up.t) {
                        cr_insert.nonatomic_entries.push_back(std::move(nonatomic_up));
                    } else {
                        if (!clustered_update_exists) {
                            res[timestamp].clustered_updates.push_back({
                                ttl,
                                cr.key(),
                                {},
                                {}
                            });

                            // Multiple iterations of this `for` loop (for different collection columns)
                            // might want to put their `nonatomic_up`s into an UPDATE change;
                            // but we don't want to create a separate change for each of them, reusing one instead.
                            //
                            // Example:
                            // CREATE TABLE t (a int primary key, b map<int, int>, c map <int, int>) with cdc = {'enabled':true};
                            // insert into t (a, b, c) values (0, {1:1}, {2:2}) USING TTL 5;
                            //
                            // this should create 3 delta rows:
                            // 1. one for the row marker (indicating an INSERT), with TTL 5
                            // 2. one for the b and c tombstones, without TTL (cdc$ttl = null)
                            // 3. one for the b and c cells, with TTL 5
                            // This logic takes care that b cells and c cells are put into a single change (3. above).
                            clustered_update_exists = true;
                        }

                        auto& cr_update = res[timestamp].clustered_updates.back();
                        cr_update.nonatomic_entries.push_back(std::move(nonatomic_up));
                    }
                }
            } else {
                res[timestamp].clustered_updates.push_back({
                        ttl,
                        cr.key(),
                        std::move(up.atomic_entries),
                        std::move(up.nonatomic_entries)
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

/* Find some timestamp inside the given mutation.
 *
 * If this mutation was created using a single insert/update/delete statement, then it will have a single,
 * well-defined timestamp (even if this timestamp occurs multiple times, e.g. in a cell and row_marker).
 *
 * This function shouldn't be used for mutations that have multiple different timestamps: the function
 * would only find one of them. When dealing with such mutations, the caller should first split the mutation
 * into multiple ones, each with a single timestamp.
 */
// TODO: We need to
// - in the code that calls `augument_mutation_call`, or inside `augument_mutation_call`,
//   split each mutation to a set of mutations, each with a single timestamp.
// - optionally: here, throw error if multiple timestamps are encountered (may degrade performance).
// external linkage for testing
api::timestamp_type find_timestamp(const mutation& m) {
    auto& p = m.partition();
    const auto& s = *m.schema();
    api::timestamp_type t = api::missing_timestamp;

    t = p.partition_tombstone().timestamp;
    if (t != api::missing_timestamp) {
        return t;
    }

    for (auto& rt: p.row_tombstones()) {
        t = rt.tomb.timestamp;
        if (t != api::missing_timestamp) {
            return t;
        }
    }

    auto walk_row = [&t, &s] (const row& r, column_kind ckind) {
        r.for_each_cell_until([&t, &s, ckind] (column_id id, const atomic_cell_or_collection& cell) {
            auto& cdef = s.column_at(ckind, id);

            if (cdef.is_atomic()) {
                t = cell.as_atomic_cell(cdef).timestamp();
                if (t != api::missing_timestamp) {
                    return stop_iteration::yes;
                }
                return stop_iteration::no;
            }

            return cell.as_collection_mutation().with_deserialized(*cdef.type,
                    [&] (collection_mutation_view_description mview) {
                t = mview.tomb.timestamp;
                if (t != api::missing_timestamp) {
                    // A collection tombstone with timestamp T can be created with:
                    // UPDATE ks.t USING TIMESTAMP T + 1 SET X = null WHERE ...
                    // where X is a non-atomic column.
                    // This is, among others, the reason why we show it in the CDC log
                    // with cdc$time using timestamp T + 1 instead of T.
                    t += 1;
                    return stop_iteration::yes;
                }

                for (auto& kv : mview.cells) {
                    t = kv.second.timestamp();
                    if (t != api::missing_timestamp) {
                        return stop_iteration::yes;
                    }
                }

                return stop_iteration::no;
            });
        });
    };

    walk_row(p.static_row().get(), column_kind::static_column);
    if (t != api::missing_timestamp) {
        return t;
    }

    for (const rows_entry& cr : p.clustered_rows()) {
        const deletable_row& r = cr.row();

        t = r.deleted_at().regular().timestamp;
        if (t != api::missing_timestamp) {
            return t;
        }

        t = r.deleted_at().shadowable().tomb().timestamp;
        if (t != api::missing_timestamp) {
            return t;
        }

        t = r.created_at();
        if (t != api::missing_timestamp) {
            return t;
        }

        walk_row(r.cells(), column_kind::regular_column);
        if (t != api::missing_timestamp) {
            return t;
        }
    }

    throw std::runtime_error("cdc: could not find timestamp of mutation");
}

bool should_split(const mutation& base_mutation) {
    const auto& base_schema = *base_mutation.schema();
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
                if (check_or_set(desc.tomb.timestamp + 1, gc_clock::duration(0))) {
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
                    if (check_or_set(mview.tomb.timestamp + 1, gc_clock::duration(0))) {
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

void process_changes_with_splitting(const mutation& base_mutation, change_processor& processor) {
    const auto base_schema = base_mutation.schema();
    auto changes = extract_changes(base_mutation);
    auto pk = base_mutation.key();

    for (auto& [change_ts, btch] : changes) {
        int batch_no = 0;

        processor.begin_timestamp(change_ts);

        for (auto& sr_update : btch.static_updates) {
            mutation m(base_schema, pk);
            for (auto& atomic_update : sr_update.atomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::static_column, atomic_update.id);
                m.set_static_cell(cdef, std::move(atomic_update.cell));
            }
            for (auto& nonatomic_update : sr_update.nonatomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::static_column, nonatomic_update.id);
                m.set_static_cell(cdef, collection_mutation_description{nonatomic_update.t, std::move(nonatomic_update.cells)}.serialize(*cdef.type));
            }
            processor.process_change(m, batch_no);
        }

        for (auto& cr_insert : btch.clustered_inserts) {
            mutation m(base_schema, pk);

            auto& row = m.partition().clustered_row(*base_schema, cr_insert.key);
            for (auto& atomic_update : cr_insert.atomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, atomic_update.id);
                row.cells().apply(cdef, std::move(atomic_update.cell));
            }
            for (auto& nonatomic_update : cr_insert.nonatomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, nonatomic_update.id);
                row.cells().apply(cdef, collection_mutation_description{nonatomic_update.t, std::move(nonatomic_update.cells)}.serialize(*cdef.type));
            }
            row.apply(cr_insert.marker);

            processor.process_change(m, batch_no);
        }

        for (auto& cr_update : btch.clustered_updates) {
            mutation m(base_schema, pk);

            auto& row = m.partition().clustered_row(*base_schema, cr_update.key).cells();
            for (auto& atomic_update : cr_update.atomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, atomic_update.id);
                row.apply(cdef, std::move(atomic_update.cell));
            }
            for (auto& nonatomic_update : cr_update.nonatomic_entries) {
                auto& cdef = base_schema->column_at(column_kind::regular_column, nonatomic_update.id);
                row.apply(cdef, collection_mutation_description{nonatomic_update.t, std::move(nonatomic_update.cells)}.serialize(*cdef.type));
            }

            processor.process_change(m, batch_no);
        }

        for (auto& cr_delete : btch.clustered_row_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply_delete(*base_schema, cr_delete.key, cr_delete.t);
            processor.process_change(m, batch_no);
        }

        for (auto& crange_delete : btch.clustered_range_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply_delete(*base_schema, crange_delete.rt);
            processor.process_change(m, batch_no);
        }

        if (btch.partition_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply(btch.partition_deletions->t);
            processor.process_change(m, batch_no);
        }
    }
}

void process_changes_without_splitting(const mutation& base_mutation, change_processor& processor) {
    int batch_no = 0;
    auto ts = find_timestamp(base_mutation);
    processor.begin_timestamp(ts);
    processor.process_change(base_mutation, batch_no);
}

} // namespace cdc
