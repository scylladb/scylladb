/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include <boost/range/adaptor/reversed.hpp>
#include "mutation_partition.hh"
#include "mutation_partition_applier.hh"

mutation_partition::mutation_partition(const mutation_partition& x)
        : _tombstone(x._tombstone)
        , _static_row(x._static_row)
        , _rows(x._rows.value_comp())
        , _row_tombstones(x._row_tombstones.value_comp()) {
    auto cloner = [] (const auto& x) {
        return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
    };
    _rows.clone_from(x._rows, cloner, current_deleter<rows_entry>());
    _row_tombstones.clone_from(x._row_tombstones, cloner, current_deleter<row_tombstones_entry>());
}

mutation_partition::~mutation_partition() {
    _rows.clear_and_dispose(current_deleter<rows_entry>());
    _row_tombstones.clear_and_dispose(current_deleter<row_tombstones_entry>());
}

mutation_partition&
mutation_partition::operator=(const mutation_partition& x) {
    mutation_partition n(x);
    std::swap(*this, n);
    return *this;
}

void
mutation_partition::apply(const schema& schema, const mutation_partition& p) {
    _tombstone.apply(p._tombstone);

    for (auto&& e : p._row_tombstones) {
        apply_row_tombstone(schema, e.prefix(), e.t());
    }

    static auto merge_cells = [] (row& old_row, const row& new_row, auto&& find_column_def) {
        for (auto&& new_column : new_row) {
            old_row.apply(new_column.id(), new_column.cell(), find_column_def);
        }
    };

    auto find_static_column_def = [&schema] (auto col) -> const column_definition& { return schema.static_column_at(col); };
    auto find_regular_column_def = [&schema] (auto col) -> const column_definition& { return schema.regular_column_at(col); };

    merge_cells(_static_row, p._static_row, find_static_column_def);

    for (auto&& entry : p._rows) {
        auto& key = entry.key();
        auto i = _rows.find(key, rows_entry::compare(schema));
        if (i == _rows.end()) {
            auto e = current_allocator().construct<rows_entry>(entry);
            _rows.insert(i, *e);
        } else {
            i->row().apply(entry.row().deleted_at());
            i->row().apply(entry.row().marker());
            merge_cells(i->row().cells(), entry.row().cells(), find_regular_column_def);
        }
    }
}

void
mutation_partition::apply(const schema& schema, mutation_partition_view p) {
    mutation_partition_applier applier(schema, *this);
    p.accept(schema, applier);
}

tombstone
mutation_partition::range_tombstone_for_row(const schema& schema, const clustering_key& key) const {
    tombstone t = _tombstone;

    if (_row_tombstones.empty()) {
        return t;
    }

    auto c = row_tombstones_entry::key_comparator(
        clustering_key::prefix_view_type::less_compare_with_prefix(schema));

    // _row_tombstones contains only strict prefixes
    for (unsigned prefix_len = 1; prefix_len < schema.clustering_key_size(); ++prefix_len) {
        auto i = _row_tombstones.find(key.prefix_view(schema, prefix_len), c);
        if (i != _row_tombstones.end()) {
            t.apply(i->t());
        }
    }

    return t;
}

tombstone
mutation_partition::tombstone_for_row(const schema& schema, const clustering_key& key) const {
    tombstone t = range_tombstone_for_row(schema, key);

    auto j = _rows.find(key, rows_entry::compare(schema));
    if (j != _rows.end()) {
        t.apply(j->row().deleted_at());
    }

    return t;
}

tombstone
mutation_partition::tombstone_for_row(const schema& schema, const rows_entry& e) const {
    tombstone t = range_tombstone_for_row(schema, e.key());
    t.apply(e.row().deleted_at());
    return t;
}

void
mutation_partition::apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t) {
    assert(!prefix.is_full(schema));
    auto i = _row_tombstones.lower_bound(prefix, row_tombstones_entry::compare(schema));
    if (i == _row_tombstones.end() || !prefix.equal(schema, i->prefix())) {
        auto e = current_allocator().construct<row_tombstones_entry>(std::move(prefix), t);
        _row_tombstones.insert(i, *e);
    } else {
        i->apply(t);
    }
}

void
mutation_partition::apply_delete(const schema& schema, const exploded_clustering_prefix& prefix, tombstone t) {
    if (!prefix) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        apply_delete(schema, clustering_key::from_clustering_prefix(schema, prefix), t);
    } else {
        apply_row_tombstone(schema, clustering_key_prefix::from_clustering_prefix(schema, prefix), t);
    }
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key&& key, tombstone t) {
    clustered_row(schema, std::move(key)).apply(t);
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key_view key, tombstone t) {
    clustered_row(schema, key).apply(t);
}

void
mutation_partition::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at) {
    clustered_row(s, key).apply(created_at);
}

const rows_entry*
mutation_partition::find_entry(const schema& schema, const clustering_key_prefix& key) const {
    auto i = _rows.find(key, rows_entry::key_comparator(clustering_key::less_compare_with_prefix(schema)));
    if (i == _rows.end()) {
        return nullptr;
    }
    return &*i;
}

const row*
mutation_partition::find_row(const clustering_key& key) const {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells();
}

deletable_row&
mutation_partition::clustered_row(clustering_key&& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(std::move(key));
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const clustering_key& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(key);
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const schema& s, const clustering_key_view& key) {
    auto i = _rows.find(key, rows_entry::compare(s));
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(key);
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

boost::iterator_range<mutation_partition::rows_type::const_iterator>
mutation_partition::range(const schema& schema, const query::range<clustering_key_prefix>& r) const {
    auto cmp = rows_entry::key_comparator(clustering_key::prefix_equality_less_compare(schema));
    auto i1 = r.start() ? (r.start()->is_inclusive()
            ? _rows.lower_bound(r.start()->value(), cmp)
            : _rows.upper_bound(r.start()->value(), cmp)) : _rows.cbegin();
    auto i2 = r.end() ? (r.end()->is_inclusive()
            ? _rows.upper_bound(r.end()->value(), cmp)
            : _rows.lower_bound(r.end()->value(), cmp)) : _rows.cend();
    return boost::make_iterator_range(i1, i2);
}


template<typename Func>
void mutation_partition::for_each_row(const schema& schema, const query::range<clustering_key_prefix>& row_range, bool reversed, Func&& func) const
{
    auto r = range(schema, row_range);
    if (!reversed) {
        for (const auto& e : r) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    } else {
        for (const auto& e : r | boost::adaptors::reversed) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    }
}

template <typename ColumnDefResolver>
static void get_row_slice(const row& cells, const std::vector<column_id>& columns, tombstone tomb,
        ColumnDefResolver&& id_to_def, gc_clock::time_point now, query::result::row_writer& writer) {
    for (auto id : columns) {
        const atomic_cell_or_collection* cell = cells.find_cell(id);
        if (!cell) {
            writer.add_empty();
        } else {
            auto&& def = id_to_def(id);
            if (def.is_atomic()) {
                auto c = cell->as_atomic_cell();
                if (!c.is_live(tomb, now)) {
                    writer.add_empty();
                } else {
                    writer.add(cell->as_atomic_cell());
                }
            } else {
                auto&& mut = cell->as_collection_mutation();
                auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
                auto m_view = ctype->deserialize_mutation_form(mut);
                m_view.tomb.apply(tomb);
                auto m_ser = ctype->serialize_mutation_form_only_live(m_view, now);
                if (ctype->is_empty(m_ser)) {
                    writer.add_empty();
                } else {
                    writer.add(m_ser);
                }
            }
        }
    }
}

template <typename ColumnDefResolver>
bool has_any_live_data(const row& cells, tombstone tomb, ColumnDefResolver&& id_to_def, gc_clock::time_point now) {
    for (auto&& e : cells) {
        auto&& cell_or_collection = e.cell();
        const column_definition& def = id_to_def(e.id());
        if (def.is_atomic()) {
            auto&& c = cell_or_collection.as_atomic_cell();
            if (c.is_live(tomb, now)) {
                return true;
            }
        } else {
            auto&& cell = cell_or_collection.as_collection_mutation();
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            if (ctype->is_any_live(cell, tomb, now)) {
                return true;
            }
        }
    }
    return false;
}

void
mutation_partition::query(query::result::partition_writer& pw,
    const schema& s,
    gc_clock::time_point now,
    uint32_t limit) const
{
    const query::partition_slice& slice = pw.slice();

    auto regular_column_resolver = [&s] (column_id id) -> const column_definition& {
        return s.regular_column_at(id);
    };

    // To avoid retraction of the partition entry in case of limit == 0.
    assert(limit > 0);

    auto static_column_resolver = [&s] (column_id id) -> const column_definition& {
        return s.static_column_at(id);
    };

    bool any_live = has_any_live_data(static_row(), _tombstone, static_column_resolver, now);

    if (!slice.static_columns.empty()) {
        auto row_builder = pw.add_static_row();
        get_row_slice(static_row(), slice.static_columns, partition_tombstone(),
            static_column_resolver, now, row_builder);
        row_builder.finish();
    }

    auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);
    for (auto&& row_range : slice.row_ranges) {
        if (limit == 0) {
            break;
        }

        // FIXME: Optimize for a full-tuple singular range. mutation_partition::range()
        // does two lookups to form a range, even for singular range. We need
        // only one lookup for a full-tuple singular range though.
        for_each_row(s, row_range, is_reversed, [&] (const rows_entry& e) {
            auto& row = e.row();
            auto row_tombstone = tombstone_for_row(s, e);

            if (row.is_live(s, row_tombstone, now)) {
                any_live = true;
                auto row_builder = pw.add_row(e.key());
                get_row_slice(row.cells(), slice.regular_columns, row_tombstone, regular_column_resolver, now, row_builder);
                row_builder.finish();
                if (--limit == 0) {
                    return stop_iteration::yes;
                }
            }
            return stop_iteration::no;
        });
    }

    if (!any_live) {
        pw.retract();
    } else {
        pw.finish();
    }
}

std::ostream&
operator<<(std::ostream& os, const row::value_type& rv) {
    return fprint(os, "{column: %s %s}", rv.id(), rv.cell());
}

std::ostream&
operator<<(std::ostream& os, const row& r) {
    return fprint(os, "{row: %s}", ::join(", ", r));
}

std::ostream&
operator<<(std::ostream& os, const row_marker& rm) {
    if (rm.is_missing()) {
        return fprint(os, "{missing row_marker}");
    } else if (rm._ttl == row_marker::dead) {
        return fprint(os, "{dead row_marker %s %s}", rm._timestamp, rm._expiry.time_since_epoch().count());
    } else {
        return fprint(os, "{row_marker %s %s %s}", rm._timestamp, rm._ttl.count(),
            rm._ttl != row_marker::no_ttl ? rm._expiry.time_since_epoch().count() : 0);
    }
}

std::ostream&
operator<<(std::ostream& os, const deletable_row& dr) {
    return fprint(os, "{deletable_row: %s %s %s}", dr._marker, dr._deleted_at, dr._cells);
}

std::ostream&
operator<<(std::ostream& os, const rows_entry& re) {
    return fprint(os, "{rows_entry: %s %s}", re._key, re._row);
}

std::ostream&
operator<<(std::ostream& os, const row_tombstones_entry& rte) {
    return fprint(os, "{row_tombstone_entry: %s %s}", rte._prefix, rte._t);
}

std::ostream&
operator<<(std::ostream& os, const mutation_partition& mp) {
    return fprint(os, "{mutation_partition: %s (%s) static %s clustered %s}",
                  mp._tombstone, ::join(", ", mp._row_tombstones), mp._static_row,
                  ::join(", ", mp._rows));
}

constexpr gc_clock::duration row_marker::no_ttl;
constexpr gc_clock::duration row_marker::dead;

static bool
rows_equal(const schema& s, const row& r1, const row& r2) {
    return std::equal(r1.begin(), r1.end(), r2.begin(), r2.end(),
        [] (const row::value_type& c1, const row::value_type& c2) {
            return c1.id() == c2.id() && c1.cell().serialize() == c2.cell().serialize();
        });
}

bool
deletable_row::equal(const schema& s, const deletable_row& other) const {
    if (_deleted_at != other._deleted_at || _marker != other._marker) {
        return false;
    }
    return rows_equal(s, _cells, other._cells);
}

bool
rows_entry::equal(const schema& s, const rows_entry& other) const {
    return key().equal(s, other.key()) && row().equal(s, other.row());
}

bool
row_tombstones_entry::equal(const schema& s, const row_tombstones_entry& other) const {
    return prefix().equal(s, other.prefix()) && t() == other.t();
}

bool mutation_partition::equal(const schema& s, const mutation_partition& p) const {
    if (_tombstone != p._tombstone) {
        return false;
    }

    if (!std::equal(_rows.begin(), _rows.end(), p._rows.begin(), p._rows.end(),
        [&s] (const rows_entry& e1, const rows_entry& e2) { return e1.equal(s, e2); }
    )) {
        return false;
    }

    if (!std::equal(_row_tombstones.begin(), _row_tombstones.end(),
        p._row_tombstones.begin(), p._row_tombstones.end(),
        [&s] (const row_tombstones_entry& e1, const row_tombstones_entry& e2) { return e1.equal(s, e2); }
    )) {
        return false;
    }

    return rows_equal(s, _static_row, p._static_row);
}

void
merge_column(const column_definition& def,
             atomic_cell_or_collection& old,
             atomic_cell_or_collection&& neww) {
    if (def.is_atomic()) {
        if (compare_atomic_cell_for_merge(old.as_atomic_cell(), neww.as_atomic_cell()) < 0) {
            old = std::move(neww);
        }
    } else {
        auto ct = static_pointer_cast<const collection_type_impl>(def.type);
        old = ct->merge(old.as_collection_mutation(), neww.as_collection_mutation());
    }
}

void
row::apply(const column_definition& column, atomic_cell_or_collection value) {
    // our mutations are not yet immutable
    auto id = column.id;
    auto i = _cells.lower_bound(id, cell_entry::compare());
    if (i == _cells.end() || i->id() != id) {
        auto e = current_allocator().construct<cell_entry>(id, std::move(value));
        _cells.insert(i, *e);
    } else {
        merge_column(column, i->cell(), std::move(value));
    }
}

void
row::append_cell(column_id id, atomic_cell_or_collection value) {
    auto e = current_allocator().construct<cell_entry>(id, std::move(value));
    _cells.insert(_cells.end(), *e);
}

const atomic_cell_or_collection*
row::find_cell(column_id id) const {
    auto i = _cells.find(id, cell_entry::compare());
    if (i == _cells.end()) {
        return nullptr;
    }
    return &i->cell();
}

uint32_t
mutation_partition::compact_for_query(
    const schema& s,
    gc_clock::time_point query_time,
    const std::vector<query::clustering_range>& row_ranges,
    uint32_t row_limit)
{
    assert(row_limit > 0);

    // FIXME: drop GC'able tombstones

    bool static_row_live = _static_row.compact_and_expire(
        _tombstone, query_time, std::bind1st(std::mem_fn(&schema::static_column_at), &s));

    uint32_t row_count = 0;

    auto last = _rows.begin();
    for (auto&& row_range : row_ranges) {
        if (row_count == row_limit) {
            break;
        }

        auto it_range = range(s, row_range);
        last = _rows.erase_and_dispose(last, it_range.begin(), current_deleter<rows_entry>());

        while (last != it_range.end()) {
            rows_entry& e = *last;
            deletable_row& row = e.row();

            tombstone tomb = tombstone_for_row(s, e);

            bool is_live = row.cells().compact_and_expire(
                tomb, query_time, std::bind1st(std::mem_fn(&schema::regular_column_at), &s));

            is_live |= row.marker().compact_and_expire(tomb, query_time);

            ++last;

            if (is_live) {
                ++row_count;
                if (row_count == row_limit) {
                    break;
                }
            }
        }
    }

    if (row_count == 0 && static_row_live) {
        ++row_count;
    }

    _rows.erase_and_dispose(last, _rows.end(), current_deleter<rows_entry>());

    // FIXME: purge unneeded prefix tombstones based on row_ranges

    return row_count;
}

bool
deletable_row::is_live(const schema& s, tombstone base_tombstone, gc_clock::time_point query_time = gc_clock::time_point::min()) const {
    auto regular_column_resolver = [&s] (column_id id) -> const column_definition& {
        return s.regular_column_at(id);
    };

    // _created_at corresponds to the row marker cell, present for rows
    // created with the 'insert' statement. If row marker is live, we know the
    // row is live. Otherwise, a row is considered live if it has any cell
    // which is live.
    base_tombstone.apply(_deleted_at);
    return _marker.is_live(base_tombstone, query_time)
           || has_any_live_data(_cells, base_tombstone, regular_column_resolver, query_time);
}

bool
mutation_partition::is_static_row_live(const schema& s, gc_clock::time_point query_time) const {
    auto static_column_resolver = [&s] (column_id id) -> const column_definition& {
        return s.static_column_at(id);
    };

    return has_any_live_data(static_row(), _tombstone, static_column_resolver, query_time);
}

size_t
mutation_partition::live_row_count(const schema& s, gc_clock::time_point query_time) const {
    size_t count = 0;

    for (const rows_entry& e : _rows) {
        tombstone base_tombstone = range_tombstone_for_row(s, e.key());
        if (e.row().is_live(s, base_tombstone, query_time)) {
            ++count;
        }
    }

    if (count == 0 && is_static_row_live(s, query_time)) {
        return 1;
    }

    return count;
}

rows_entry::rows_entry(rows_entry&& o) noexcept
    : _link(std::move(o._link))
    , _key(std::move(o._key))
    , _row(std::move(o._row))
{
    using container_type = mutation_partition::rows_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

row_tombstones_entry::row_tombstones_entry(row_tombstones_entry&& o) noexcept
    : _link()
    , _prefix(std::move(o._prefix))
    , _t(std::move(o._t))
{
    using container_type = mutation_partition::row_tombstones_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

row::row(const row& o) {
    auto cloner = [] (const auto& x) {
        return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
    };
    _cells.clone_from(o._cells, cloner, current_deleter<cell_entry>());
}

row::~row() {
    _cells.clear_and_dispose(current_deleter<cell_entry>());
}

row::cell_entry::cell_entry(const cell_entry& o) noexcept
    : _id(o._id)
    , _cell(o._cell)
{ }

row::cell_entry::cell_entry(cell_entry&& o) noexcept
    : _link()
    , _id(o._id)
    , _cell(std::move(o._cell))
{
    using container_type = row::map_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

const atomic_cell_or_collection& row::cell_at(column_id id) const {
    auto&& cell = find_cell(id);
    if (!cell) {
        throw std::out_of_range(sprint("Column not found for id = %d", id));
    }
    return *cell;
}
