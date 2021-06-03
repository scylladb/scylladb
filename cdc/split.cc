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

#include "mutation.hh"
#include "schema.hh"

#include "concrete_types.hh"
#include "types/user.hh"

#include "split.hh"
#include "log.hh"
#include "change_visitor.hh"

#include <type_traits>

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

using clustered_column_set = std::map<clustering_key, cdc::one_kind_column_set, clustering_key::less_compare>;

template<typename Container>
concept EntryContainer = requires(Container& container) {
    // Parenthesized due to https://bugs.llvm.org/show_bug.cgi?id=45088
    { (container.atomic_entries) } -> std::same_as<std::vector<atomic_column_update>&>;
    { (container.nonatomic_entries) } -> std::same_as<std::vector<nonatomic_column_update>&>;
};

template<EntryContainer Container>
static void add_columns_affected_by_entries(cdc::one_kind_column_set& cset, const Container& cont) {
    for (const auto& entry : cont.atomic_entries) {
        cset.set(entry.id);
    }
    for (const auto& entry : cont.nonatomic_entries) {
        cset.set(entry.id);
    }
}

/* Given a mutation with multiple timestamps/ttl/types of changes, we split it into multiple mutations
 * before passing it into `process_change` (see comment above `should_split_visitor` for more details).
 *
 * The first step of the splitting is to walk over the mutation and put each change into an appropriate bucket
 * (see `batch`). The buckets are sorted by timestamps (see `set_of_changes`), and within each bucket,
 * the changes are split according to their types (`static_updates`, `clustered_inserts`, and so on).
 * Within each type, the changes are sorted w.r.t TTLs. Changes without a TTL are treated as if they had TTL = 0.
 *
 * The function that puts changes into bucket is called `extract_changes`. Underneath, it uses
 * `extract_changes_visitor`, `extract_collection_visitor` and `extract_row_visitor`.
 */

struct batch {
    std::vector<static_row_update> static_updates;
    std::vector<clustered_row_insert> clustered_inserts;
    std::vector<clustered_row_update> clustered_updates;
    std::vector<clustered_row_deletion> clustered_row_deletions;
    std::vector<clustered_range_deletion> clustered_range_deletions;
    std::optional<partition_deletion> partition_deletions;

    clustered_column_set get_affected_clustered_columns_per_row(const schema& s) const {
        clustered_column_set ret{clustering_key::less_compare(s)};

        if (!clustered_row_deletions.empty()) {
            // When deleting a row, all columns are affected
            cdc::one_kind_column_set all_columns{s.regular_columns_count()};
            all_columns.set(0, s.regular_columns_count(), true);
            for (const auto& change : clustered_row_deletions) {
                ret.insert(std::make_pair(change.key, all_columns));
            }
        }

        auto process_change_type = [&] (const auto& changes) {
            for (const auto& change : changes) {
                auto& cset = ret[change.key];
                cset.resize(s.regular_columns_count());
                add_columns_affected_by_entries(cset, change);
            }
        };

        process_change_type(clustered_inserts);
        process_change_type(clustered_updates);

        return ret;
    }

    cdc::one_kind_column_set get_affected_static_columns(const schema& s) const {
        cdc::one_kind_column_set ret{s.static_columns_count()};
        for (const auto& change : static_updates) {
            add_columns_affected_by_entries(ret, change);
        }
        return ret;
    }
};

using set_of_changes = std::map<api::timestamp_type, batch>;

struct row_update {
    std::vector<atomic_column_update> atomic_entries;
    std::vector<nonatomic_column_update> nonatomic_entries;
};

static gc_clock::duration get_ttl(const atomic_cell_view& acv) {
    return acv.is_live_and_has_ttl() ? acv.ttl() : gc_clock::duration(0);
}

static gc_clock::duration get_ttl(const row_marker& rm) {
    return rm.is_expiring() ? rm.ttl() : gc_clock::duration(0);
}

using change_key_t = std::pair<api::timestamp_type, gc_clock::duration>;

/* Visits the cells and tombstone of a collection, putting the encountered changes into buckets
 * sorted by timestamp first and ttl second (see `_updates`).
 */
template <typename V>
struct extract_collection_visitor {
private:
    const column_id _id;
    std::map<change_key_t, row_update>& _updates;

    nonatomic_column_update& get_or_append_entry(api::timestamp_type ts, gc_clock::duration ttl) {
        auto& updates = this->_updates[std::pair(ts, ttl)].nonatomic_entries;
        if (updates.empty() || updates.back().id != _id) {
            updates.push_back({_id});
        }
        return updates.back();
    }

    /* To copy a value from a collection/non-frozen UDT (in order to put it into a bucket) we need to know the value's type.
     * The method of obtaining the type depends on the collection type; in particular, for non-frozen UDT, each value
     * might have a different type, thus in general we need a method that, given a key (identifying the value in the collection),
     * returns the value' type.
     *
     * We use the `Curiously Recurring Template Pattern' to avoid performing a dynamic dispatch on the collection's type for each visited cell.
     * Instead we perform a single dynamic dispatch at the beginning, when encountering the collection column;
     * the dispatch provides us with a correct `get_value_type` method.
     * See `extract_row_visitor::collection_column` where the dispatch is done.

    data_type get_value_type(bytes_view);
    */

    void cell(bytes_view key, const atomic_cell_view& c) {
        auto& entry = get_or_append_entry(c.timestamp(), get_ttl(c));
        entry.cells.emplace_back(to_bytes(key), atomic_cell(*static_cast<V&>(*this).get_value_type(key), c));
    }

public:
    extract_collection_visitor(column_id id, std::map<change_key_t, row_update>& updates)
        : _id(id), _updates(updates) {}

    void collection_tombstone(const tombstone& t) {
        auto& entry = get_or_append_entry(t.timestamp + 1, gc_clock::duration(0));
        entry.t = t;
    }

    void live_collection_cell(bytes_view key, const atomic_cell_view& c) {
        cell(key, c);
    }

    void dead_collection_cell(bytes_view key, const atomic_cell_view& c) {
        cell(key, c);
    }

    constexpr bool finished() const { return false; }
};

/* Visits all cells and tombstones in a row, putting the encountered changes into buckets
 * sorted by timestamp first and ttl second (see `_updates`).
 */
struct extract_row_visitor {
    std::map<change_key_t, row_update> _updates;

    void cell(const column_definition& cdef, const atomic_cell_view& cell) {
        _updates[std::pair(cell.timestamp(), get_ttl(cell))].atomic_entries.push_back({cdef.id, atomic_cell(*cdef.type, cell)});
    }

    void live_atomic_cell(const column_definition& cdef, const atomic_cell_view& c) {
        cell(cdef, c);
    }

    void dead_atomic_cell(const column_definition& cdef, const atomic_cell_view& c) {
        cell(cdef, c);
    }

    void collection_column(const column_definition& cdef, auto&& visit_collection) {
        visit(*cdef.type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            struct collection_visitor : public extract_collection_visitor<collection_visitor> {
                data_type _value_type;

                collection_visitor(column_id id, std::map<change_key_t, row_update>& updates, const collection_type_impl& ctype)
                    : extract_collection_visitor<collection_visitor>(id, updates), _value_type(ctype.value_comparator()) {}

                data_type get_value_type(bytes_view) {
                    return _value_type;
                }
            } v(cdef.id, _updates, ctype);

            visit_collection(v);
        },
        [&] (const user_type_impl& utype) {
            struct udt_visitor : public extract_collection_visitor<udt_visitor> {
                const user_type_impl& _utype;

                udt_visitor(column_id id, std::map<change_key_t, row_update>& updates, const user_type_impl& utype)
                    : extract_collection_visitor<udt_visitor>(id, updates), _utype(utype) {}

                data_type get_value_type(bytes_view key) {
                    return _utype.type(deserialize_field_index(key));
                }
            } v(cdef.id, _updates, utype);

            visit_collection(v);
        },
        [&] (const abstract_type& o) {
            throw std::runtime_error(format("extract_changes: unknown collection type:", o.name()));
        }
        ));
    }

    constexpr bool finished() const { return false; }
};

struct extract_changes_visitor {
    set_of_changes _result;

    void static_row_cells(auto&& visit_row_cells) {
        extract_row_visitor v;
        visit_row_cells(v);

        for (auto& [ts_ttl, row_update]: v._updates) {
            _result[ts_ttl.first].static_updates.push_back({
                ts_ttl.second,
                std::move(row_update.atomic_entries),
                std::move(row_update.nonatomic_entries)
            });
        }
    }

    void clustered_row_cells(const clustering_key& ckey, auto&& visit_row_cells) {
        struct clustered_cells_visitor : public extract_row_visitor {
            api::timestamp_type _marker_ts;
            gc_clock::duration _marker_ttl;
            std::optional<row_marker> _marker;

            void marker(const row_marker& rm) {
                _marker_ts = rm.timestamp();
                _marker_ttl = get_ttl(rm);
                _marker = rm;

                // make sure that an entry corresponding to the row marker's timestamp and ttl is in the map
                (void)_updates[std::pair(_marker_ts, _marker_ttl)];
            }
        } v;
        visit_row_cells(v);

        for (auto& [ts_ttl, row_update]: v._updates) {
            // It is important that changes in the resulting `set_of_changes` are listed
            // in increasing TTL order. The reason is explained in a comment in cdc/log.cc,
            // search for "#6070".
            auto [ts, ttl] = ts_ttl;

            if (v._marker && ts == v._marker_ts && ttl == v._marker_ttl) {
                _result[ts].clustered_inserts.push_back({
                        ttl,
                        ckey,
                        *v._marker,
                        std::move(row_update.atomic_entries),
                        {}
                    });

                auto& cr_insert = _result[ts].clustered_inserts.back();
                bool clustered_update_exists = false;
                for (auto& nonatomic_up: row_update.nonatomic_entries) {
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
                            _result[ts].clustered_updates.push_back({
                                ttl,
                                ckey,
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

                        auto& cr_update = _result[ts].clustered_updates.back();
                        cr_update.nonatomic_entries.push_back(std::move(nonatomic_up));
                    }
                }
            } else {
                _result[ts].clustered_updates.push_back({
                        ttl,
                        ckey,
                        std::move(row_update.atomic_entries),
                        std::move(row_update.nonatomic_entries)
                    });
            }
        }
    }

    void clustered_row_delete(const clustering_key& ckey, const tombstone& t) {
        _result[t.timestamp].clustered_row_deletions.push_back({ckey, t});
    }

    void range_delete(const range_tombstone& rt) {
        _result[rt.tomb.timestamp].clustered_range_deletions.push_back({rt});
    }

    void partition_delete(const tombstone& t) {
        _result[t.timestamp].partition_deletions = {t};
    }

    constexpr bool finished() const { return false; }
};

set_of_changes extract_changes(const mutation& m) {
    extract_changes_visitor v;
    cdc::inspect_mutation(m, v);
    return std::move(v._result);
}

namespace cdc {

struct find_timestamp_visitor {
    api::timestamp_type _ts = api::missing_timestamp;

    bool finished() const { return _ts != api::missing_timestamp; }

    void visit(api::timestamp_type ts) { _ts = ts; }
    void visit(const atomic_cell_view& cell) { visit(cell.timestamp()); }

    void live_atomic_cell(const column_definition&, const atomic_cell_view& cell) { visit(cell); }
    void dead_atomic_cell(const column_definition&, const atomic_cell_view& cell) { visit(cell); }
    void collection_tombstone(const tombstone& t) {
        // A collection tombstone with timestamp T can be created with:
        // UPDATE ks.t USING TIMESTAMP T + 1 SET X = null WHERE ...
        // (where X is a collection column).
        // This is, among others, the reason why we show it in the CDC log
        // with cdc$time using timestamp T + 1 instead of T.
        visit(t.timestamp + 1);
    }
    void live_collection_cell(bytes_view, const atomic_cell_view& cell) { visit(cell); }
    void dead_collection_cell(bytes_view, const atomic_cell_view& cell) { visit(cell); }
    void collection_column(const column_definition&, auto&& visit_collection) { visit_collection(*this); }
    void marker(const row_marker& rm) { visit(rm.timestamp()); }
    void static_row_cells(auto&& visit_row_cells) { visit_row_cells(*this); }
    void clustered_row_cells(const clustering_key&, auto&& visit_row_cells) { visit_row_cells(*this); }
    void clustered_row_delete(const clustering_key&, const tombstone& t) { visit(t.timestamp); }
    void range_delete(const range_tombstone& t) { visit(t.tomb.timestamp); }
    void partition_delete(const tombstone& t) { visit(t.timestamp); }
};

/* Find some timestamp inside the given mutation.
 *
 * If this mutation was created using a single insert/update/delete statement, then it will have a single,
 * well-defined timestamp (even if this timestamp occurs multiple times, e.g. in a cell and row_marker).
 *
 * This function shouldn't be used for mutations that have multiple different timestamps: the function
 * would only find one of them. When dealing with such mutations, the caller should first split the mutation
 * into multiple ones, each with a single timestamp.
 */
api::timestamp_type find_timestamp(const mutation& m) {
    find_timestamp_visitor v;

    cdc::inspect_mutation(m, v);

    if (v._ts == api::missing_timestamp) {
        throw std::runtime_error("cdc: could not find timestamp of mutation");
    }

    return v._ts;
}

/* If a mutation contains multiple timestamps, multiple ttls, or multiple types of changes
 * (e.g. it was created from a batch that both updated a clustered row and deleted a clustered row),
 * we split it into multiple mutations, each with exactly one timestamp, at most one ttl, and a single type of change.
 * We also split if we find both a change with no ttl (e.g. a cell tombstone) and a change with ttl (e.g. a ttled cell update).
 *
 * The `should_split` function checks whether the mutation requires such splitting, using `should_split_visitor`.
 * The visitor uses the order in which the mutation is being visited (see the documentation of ChangeVisitor),
 * remembers a bunch of state based on whatever was visited until now (e.g. was there a static row update?
 * Was there a clustered row update? Was there a clustered row delete? Was there a TTL?)
 * and tells the caller to stop on the first occurence of a second timestamp/ttl/type of change.
 */
struct should_split_visitor {
    bool _had_static_row = false;
    bool _had_clustered_row = false;
    bool _had_upsert = false;
    bool _had_row_marker = false;
    bool _had_range_delete = false;

    bool _result = false;

    // This becomes a valid (non-missing) timestamp after visiting the first change.
    // Then, if we encounter any different timestamp, it means that we should split.
    api::timestamp_type _ts = api::missing_timestamp;

    // This becomes non-null after visiting the fist change.
    // If the change did not have a ttl (e.g. a non-ttled cell, or a tombstone), we store gc_clock::duration(0) there,
    // because specifying ttl = 0 is equivalent to not specifying a TTL.
    // Otherwise we store the change's ttl.
    std::optional<gc_clock::duration> _ttl = std::nullopt;

    inline bool finished() const { return _result; }
    inline void stop() { _result = true; }

    void visit(api::timestamp_type ts, gc_clock::duration ttl = gc_clock::duration(0)) {
        if (_ts != api::missing_timestamp && _ts != ts) {
            return stop();
        }
        _ts = ts;

        if (_ttl && *_ttl != ttl) {
            return stop();
        }
        _ttl = { ttl };
    }

    void visit(const atomic_cell_view& cell) { visit(cell.timestamp(), get_ttl(cell)); }

    void live_atomic_cell(const column_definition&, const atomic_cell_view& cell) { visit(cell); }
    void dead_atomic_cell(const column_definition&, const atomic_cell_view& cell) { visit(cell); }

    void collection_tombstone(const tombstone& t) { visit(t.timestamp + 1); }

    void live_collection_cell(bytes_view, const atomic_cell_view& cell) {
        if (_had_row_marker) {
            // nonatomic updates cannot be expressed with an INSERT.
            return stop();
        }
        visit(cell);
    }
    void dead_collection_cell(bytes_view, const atomic_cell_view& cell) { visit(cell); }
    void collection_column(const column_definition&, auto&& visit_collection) { visit_collection(*this); }

    void marker(const row_marker& rm) {
        _had_row_marker = true;
        visit(rm.timestamp(), get_ttl(rm));
    }

    void static_row_cells(auto&& visit_row_cells) {
        _had_static_row = true;
        visit_row_cells(*this);
    }

    void clustered_row_cells(const clustering_key&, auto&& visit_row_cells) {
        if (_had_static_row) {
            return stop();
        }
        _had_clustered_row = _had_upsert = true;
        visit_row_cells(*this);
    }

    void clustered_row_delete(const clustering_key&, const tombstone& t) {
        if (_had_static_row || _had_upsert) {
            return stop();
        }
        _had_clustered_row = true;
        visit(t.timestamp);
    }

    void range_delete(const range_tombstone& t) {
        if (_had_static_row || _had_clustered_row) {
            return stop();
        }
        _had_range_delete = true;
        visit(t.tomb.timestamp);
    }

    void partition_delete(const tombstone&) {
        if (_had_range_delete || _had_static_row || _had_clustered_row) {
            return stop();
        }
    }
};

bool should_split(const mutation& m) {
    should_split_visitor v;

    cdc::inspect_mutation(m, v);

    return v._result
    // A mutation with no timestamp will be split into 0 mutations:
        || v._ts == api::missing_timestamp;
}

void process_changes_with_splitting(const mutation& base_mutation, change_processor& processor,
        bool enable_preimage, bool enable_postimage) {
    const auto base_schema = base_mutation.schema();
    auto changes = extract_changes(base_mutation);
    auto pk = base_mutation.key();

    if (changes.empty()) {
        return;
    }

    const auto last_timestamp = changes.rbegin()->first;

    for (auto& [change_ts, btch] : changes) {
        const bool is_last = change_ts == last_timestamp;
        processor.begin_timestamp(change_ts, is_last);

        clustered_column_set affected_clustered_columns_per_row{clustering_key::less_compare(*base_schema)};
        one_kind_column_set affected_static_columns{base_schema->static_columns_count()};

        if (enable_preimage || enable_postimage) {
            affected_static_columns = btch.get_affected_static_columns(*base_schema);
            affected_clustered_columns_per_row = btch.get_affected_clustered_columns_per_row(*base_mutation.schema());
        }

        if (enable_preimage) {
            if (affected_static_columns.count() > 0) {
                processor.produce_preimage(nullptr, affected_static_columns);
            }
            for (const auto& [ck, affected_row_cells] : affected_clustered_columns_per_row) {
                processor.produce_preimage(&ck, affected_row_cells);
            }
        }

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
            processor.process_change(m);
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

            processor.process_change(m);
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

            processor.process_change(m);
        }

        for (auto& cr_delete : btch.clustered_row_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply_delete(*base_schema, cr_delete.key, cr_delete.t);
            processor.process_change(m);
        }

        for (auto& crange_delete : btch.clustered_range_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply_delete(*base_schema, crange_delete.rt);
            processor.process_change(m);
        }

        if (btch.partition_deletions) {
            mutation m(base_schema, pk);
            m.partition().apply(btch.partition_deletions->t);
            processor.process_change(m);
        }

        if (enable_postimage) {
            if (affected_static_columns.count() > 0) {
                processor.produce_postimage(nullptr);
            }
            for (const auto& [ck, crow] : affected_clustered_columns_per_row) {
                processor.produce_postimage(&ck);
            }
        }

        processor.end_record();
    }
}

void process_changes_without_splitting(const mutation& base_mutation, change_processor& processor,
        bool enable_preimage, bool enable_postimage) {
    auto ts = find_timestamp(base_mutation);
    processor.begin_timestamp(ts, true);

    const auto base_schema = base_mutation.schema();

    if (enable_preimage) {
        const auto& p = base_mutation.partition();

        one_kind_column_set columns{base_schema->static_columns_count()};
        if (!p.static_row().empty()) {
            p.static_row().get().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
                columns.set(id);
            });
            processor.produce_preimage(nullptr, columns);
        }

        columns.resize(base_schema->regular_columns_count());
        for (const rows_entry& cr : p.clustered_rows()) {
            columns.reset();
            if (cr.row().deleted_at().regular()) {
                // Row deleted - include all columns in preimage
                columns.set(0, base_schema->regular_columns_count(), true);
            } else {
                cr.row().cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
                    columns.set(id);
                });
            }
            processor.produce_preimage(&cr.key(), columns);
        }
    }

    processor.process_change(base_mutation);

    if (enable_postimage) {
        const auto& p = base_mutation.partition();
        if (!p.static_row().empty()) {
            processor.produce_postimage(nullptr);
        }
        for (const rows_entry& cr : p.clustered_rows()) {
            processor.produce_postimage(&cr.key());
        }
    }

    processor.end_record();
}

} // namespace cdc
