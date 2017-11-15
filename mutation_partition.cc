/*
 * Copyright (C) 2014 ScyllaDB
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

#include <boost/range/adaptor/reversed.hpp>
#include <seastar/util/defer.hh>
#include "mutation_partition.hh"
#include "mutation_partition_applier.hh"
#include "converting_mutation_partition_applier.hh"
#include "partition_builder.hh"
#include "query-result-writer.hh"
#include "atomic_cell_hash.hh"
#include "reversibly_mergeable.hh"
#include "streamed_mutation.hh"
#include "mutation_query.hh"
#include "service/priority_manager.hh"
#include "mutation_compactor.hh"
#include "intrusive_set_external_comparator.hh"
#include "counters.hh"
#include <seastar/core/execution_stage.hh>

template<bool reversed>
struct reversal_traits;

template<>
struct reversal_traits<false> {
    template <typename Container>
    static auto begin(Container& c) {
        return c.begin();
    }

    template <typename Container>
    static auto end(Container& c) {
        return c.end();
    }

    template <typename Container, typename Disposer>
    static typename Container::iterator erase_and_dispose(Container& c,
        typename Container::iterator begin,
        typename Container::iterator end,
        Disposer disposer)
    {
        return c.erase_and_dispose(begin, end, std::move(disposer));
    }

    template<typename Container, typename Disposer>
    static typename Container::iterator erase_dispose_and_update_end(Container& c,
         typename Container::iterator it, Disposer&& disposer,
         typename Container::iterator&)
    {
        return c.erase_and_dispose(it, std::forward<Disposer>(disposer));
    }

    template <typename Container>
    static boost::iterator_range<typename Container::iterator> maybe_reverse(
        Container& c, boost::iterator_range<typename Container::iterator> r)
    {
        return r;
    }

    template <typename Container>
    static typename Container::iterator maybe_reverse(Container&, typename Container::iterator r) {
        return r;
    }
};

template<>
struct reversal_traits<true> {
    template <typename Container>
    static auto begin(Container& c) {
        return c.rbegin();
    }

    template <typename Container>
    static auto end(Container& c) {
        return c.rend();
    }

    template <typename Container, typename Disposer>
    static typename Container::reverse_iterator erase_and_dispose(Container& c,
        typename Container::reverse_iterator begin,
        typename Container::reverse_iterator end,
        Disposer disposer)
    {
        return typename Container::reverse_iterator(
            c.erase_and_dispose(end.base(), begin.base(), disposer)
        );
    }

    // Erases element pointed to by it and makes sure than iterator end is not
    // invalidated.
    template<typename Container, typename Disposer>
    static typename Container::reverse_iterator erase_dispose_and_update_end(Container& c,
        typename Container::reverse_iterator it, Disposer&& disposer,
        typename Container::reverse_iterator& end)
    {
        auto to_erase = std::next(it).base();
        bool update_end = end.base() == to_erase;
        auto ret = typename Container::reverse_iterator(
            c.erase_and_dispose(to_erase, std::forward<Disposer>(disposer))
        );
        if (update_end) {
            end = ret;
        }
        return ret;
    }

    template <typename Container>
    static boost::iterator_range<typename Container::reverse_iterator> maybe_reverse(
        Container& c, boost::iterator_range<typename Container::iterator> r)
    {
        using reverse_iterator = typename Container::reverse_iterator;
        return boost::make_iterator_range(reverse_iterator(r.end()), reverse_iterator(r.begin()));
    }

    template <typename Container>
    static typename Container::reverse_iterator maybe_reverse(Container&, typename Container::iterator r) {
        return typename Container::reverse_iterator(r);
    }
};


//
// apply_reversibly_intrusive_set() and revert_intrusive_set() implement ReversiblyMergeable
// for a rows_type container of ReversiblyMergeable entries.
//
// See reversibly_mergeable.hh
//
// Requirements:
//  - entry has distinct key and value states
//  - entries are ordered only by key in the container
//  - entry can have an empty value
//  - presence of an entry with an empty value doesn't affect equality of the containers
//  - E::empty() returns true iff the value is empty
//  - E(e.key()) creates an entry with empty value but the same key as that of e.
//
// Implementation of ReversiblyMergeable for the entry's value is provided via Apply and Revert functors.
//
// ReversiblyMergeable is constructed assuming the following properties of the 'apply' operation
// on containers:
//
//  apply([{k1, v1}], [{k1, v2}]) = [{k1, apply(v1, v2)}]
//  apply([{k1, v1}], [{k2, v2}]) = [{k1, v1}, {k2, v2}]
//

// revert for apply_reversibly_intrusive_set()
void revert_intrusive_set_range(const schema& s, mutation_partition::rows_type& dst, mutation_partition::rows_type& src,
    mutation_partition::rows_type::iterator start,
    mutation_partition::rows_type::iterator end) noexcept
{
    auto deleter = current_deleter<rows_entry>();
    while (start != end) {
        auto& e = *start;
        // lower_bound() can allocate if linearization is required but it should have
        // been already performed by the lower_bound() invocation in apply_reversibly_intrusive_set() and
        // stored in the linearization context.
        auto i = dst.find(e, rows_entry::compare(s));
        assert(i != dst.end());
        rows_entry& dst_e = *i;

        if (e.erased()) {
            dst.erase(i);
            start = src.erase_and_dispose(start, deleter);
            start = src.insert_before(start, dst_e);
        } else {
            dst_e.revert(s, e);
        }

        ++start;
    }
}

void revert_intrusive_set(const schema& s, mutation_partition::rows_type& dst, mutation_partition::rows_type& src) noexcept {
    revert_intrusive_set_range(s, dst, src, src.begin(), src.end());
}

// Applies src onto dst. See comment above revert_intrusive_set_range() for more details.
//
// Returns an object which upon going out of scope, unless cancel() is called on it,
// reverts the applicaiton by calling revert_intrusive_set(). The references to containers
// must be stable as long as the returned object is live.
auto apply_reversibly_intrusive_set(const schema& s, mutation_partition::rows_type& dst, mutation_partition::rows_type& src) {
    auto src_i = src.begin();
    try {
        rows_entry::compare cmp(s);
        while (src_i != src.end()) {
            rows_entry& src_e = *src_i;

            auto i = dst.lower_bound(src_e, cmp);
            if (i == dst.end() || cmp(src_e, *i)) {
                // Construct erased entry which will represent missing dst entry for revert.
                rows_entry* empty_e = current_allocator().construct<rows_entry>(rows_entry::erased_tag{}, src_e);
                [&] () noexcept {
                    src_i = src.erase(src_i);
                    src_i = src.insert_before(src_i, *empty_e);
                    dst.insert_before(i, src_e);
                }();
            } else {
                i->apply_reversibly(s, src_e);
            }
            ++src_i;
        }
        return defer([&s, &dst, &src] { revert_intrusive_set(s, dst, src); });
    } catch (...) {
        revert_intrusive_set_range(s, dst, src, src.begin(), src_i);
        throw;
    }
}

mutation_partition::mutation_partition(const mutation_partition& x)
        : _tombstone(x._tombstone)
        , _static_row(x._static_row)
        , _static_row_continuous(x._static_row_continuous)
        , _rows()
        , _row_tombstones(x._row_tombstones) {
    auto cloner = [] (const auto& x) {
        return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
    };
    _rows.clone_from(x._rows, cloner, current_deleter<rows_entry>());
}

mutation_partition::mutation_partition(const mutation_partition& x, const schema& schema,
        query::clustering_key_filter_ranges ck_ranges)
        : _tombstone(x._tombstone)
        , _static_row(x._static_row)
        , _static_row_continuous(x._static_row_continuous)
        , _rows()
        , _row_tombstones(x._row_tombstones, range_tombstone_list::copy_comparator_only()) {
    try {
        for(auto&& r : ck_ranges) {
            for (const rows_entry& e : x.range(schema, r)) {
                _rows.insert(_rows.end(), *current_allocator().construct<rows_entry>(e), rows_entry::compare(schema));
            }
            for (auto&& rt : x._row_tombstones.slice(schema, r)) {
                _row_tombstones.apply(schema, rt);
            }
        }
    } catch (...) {
        _rows.clear_and_dispose(current_deleter<rows_entry>());
        throw;
    }
}

mutation_partition::mutation_partition(mutation_partition&& x, const schema& schema,
    query::clustering_key_filter_ranges ck_ranges)
    : _tombstone(x._tombstone)
    , _static_row(std::move(x._static_row))
    , _static_row_continuous(x._static_row_continuous)
    , _rows(std::move(x._rows))
    , _row_tombstones(std::move(x._row_tombstones))
{
    {
        auto deleter = current_deleter<rows_entry>();
        auto it = _rows.begin();
        for (auto&& range : ck_ranges.ranges()) {
            _rows.erase_and_dispose(it, lower_bound(schema, range), deleter);
            it = upper_bound(schema, range);
        }
        _rows.erase_and_dispose(it, _rows.end(), deleter);
    }
    {
        range_tombstone_list::const_iterator it = _row_tombstones.begin();
        for (auto&& range : ck_ranges.ranges()) {
            auto rt_range = _row_tombstones.slice(schema, range);
            // upper bound for previous range may be after lower bound for the next range
            // if both ranges are connected through a range tombstone. In this case the
            // erase range would be invalid.
            if (rt_range.begin() == _row_tombstones.end() || std::next(rt_range.begin()) != it) {
                _row_tombstones.erase(it, rt_range.begin());
            }
            it = rt_range.end();
        }
        _row_tombstones.erase(it, _row_tombstones.end());
    }
}

mutation_partition::~mutation_partition() {
    _rows.clear_and_dispose(current_deleter<rows_entry>());
}

mutation_partition&
mutation_partition::operator=(const mutation_partition& x) {
    mutation_partition n(x);
    std::swap(*this, n);
    return *this;
}

mutation_partition&
mutation_partition::operator=(mutation_partition&& x) noexcept {
    if (this != &x) {
        this->~mutation_partition();
        new (this) mutation_partition(std::move(x));
    }
    return *this;
}

void mutation_partition::ensure_last_dummy(const schema& s) {
    if (_rows.empty() || !_rows.rbegin()->position().is_after_all_clustered_rows(s)) {
        _rows.insert_before(_rows.end(),
            *current_allocator().construct<rows_entry>(s, position_in_partition_view::after_all_clustered_rows(), is_dummy::yes, is_continuous::yes));
    }
}

void
mutation_partition::apply(const schema& s, const mutation_partition& p, const schema& p_schema) {
    if (s.version() != p_schema.version()) {
        auto p2 = p;
        p2.upgrade(p_schema, s);
        apply(s, std::move(p2));
        return;
    }

    mutation_partition tmp(p);
    apply(s, std::move(tmp));
}

struct mutation_fragment_applier {
    const schema& _s;
    mutation_partition& _mp;

    void operator()(tombstone t) {
        _mp.apply(t);
    }

    void operator()(range_tombstone rt) {
        _mp.apply_row_tombstone(_s, std::move(rt));
    }

    void operator()(static_row sr) {
        _mp.static_row().apply(_s, column_kind::static_column, std::move(sr.cells()));
    }

    void operator()(partition_start ps) {
        _mp.apply(ps.partition_tombstone());
    }

    void operator()(partition_end ps) {
    }

    void operator()(clustering_row cr) {
        auto& dr = _mp.clustered_row(_s, std::move(cr.key()));
        dr.apply(cr.tomb());
        dr.apply(cr.marker());
        dr.cells().apply(_s, column_kind::regular_column, std::move(cr.cells()));
    }
};

void
mutation_partition::apply(const schema& s, const mutation_fragment& mf) {
    mutation_fragment_applier applier{s, *this};
    mf.visit(applier);
}

void
mutation_partition::apply(const schema& s, mutation_partition&& p, const schema& p_schema) {
    if (s.version() != p_schema.version()) {
        // We can't upgrade p in-place due to exception guarantees
        apply(s, p, p_schema);
        return;
    }

    apply(s, std::move(p));
}

void
mutation_partition::apply(const schema& s, mutation_partition&& p) {
    auto revert_row_tombstones = _row_tombstones.apply_reversibly(s, p._row_tombstones);

    _static_row.apply_reversibly(s, column_kind::static_column, p._static_row);
    auto revert_static_row = defer([&] {
        _static_row.revert(s, column_kind::static_column, p._static_row);
    });

    auto revert_rows = apply_reversibly_intrusive_set(s, _rows, p._rows);

    _tombstone.apply(p._tombstone); // noexcept

    revert_rows.cancel();
    revert_row_tombstones.cancel();
    revert_static_row.cancel();
}

void
mutation_partition::apply(const schema& s, mutation_partition_view p, const schema& p_schema) {
    if (p_schema.version() == s.version()) {
        mutation_partition p2(*this, copy_comparators_only{});
        partition_builder b(s, p2);
        p.accept(s, b);
        apply(s, std::move(p2));
    } else {
        mutation_partition p2(*this, copy_comparators_only{});
        partition_builder b(p_schema, p2);
        p.accept(p_schema, b);
        p2.upgrade(p_schema, s);
        apply(s, std::move(p2));
    }
}

tombstone
mutation_partition::range_tombstone_for_row(const schema& schema, const clustering_key& key) const {
    tombstone t = _tombstone;
    if (!_row_tombstones.empty()) {
        auto found = _row_tombstones.search_tombstone_covering(schema, key);
        t.apply(found);
    }
    return t;
}

row_tombstone
mutation_partition::tombstone_for_row(const schema& schema, const clustering_key& key) const {
    row_tombstone t = row_tombstone(range_tombstone_for_row(schema, key));

    auto j = _rows.find(key, rows_entry::compare(schema));
    if (j != _rows.end()) {
        t.apply(j->row().deleted_at(), j->row().marker());
    }

    return t;
}

row_tombstone
mutation_partition::tombstone_for_row(const schema& schema, const rows_entry& e) const {
    row_tombstone t = e.row().deleted_at();
    t.apply(range_tombstone_for_row(schema, e.key()));
    return t;
}

void
mutation_partition::apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t) {
    assert(!prefix.is_full(schema));
    auto start = prefix;
    _row_tombstones.apply(schema, {std::move(start), std::move(prefix), std::move(t)});
}

void
mutation_partition::apply_row_tombstone(const schema& schema, range_tombstone rt) {
    _row_tombstones.apply(schema, std::move(rt));
}

void
mutation_partition::apply_delete(const schema& schema, const clustering_key_prefix& prefix, tombstone t) {
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, prefix).apply(t);
    } else {
        apply_row_tombstone(schema, prefix, t);
    }
}

void
mutation_partition::apply_delete(const schema& schema, range_tombstone rt) {
    if (range_tombstone::is_single_clustering_row_tombstone(schema, rt.start, rt.start_kind, rt.end, rt.end_kind)) {
        apply_delete(schema, std::move(rt.start), std::move(rt.tomb));
        return;
    }
    apply_row_tombstone(schema, std::move(rt));
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key&& prefix, tombstone t) {
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, std::move(prefix)).apply(t);
    } else {
        apply_row_tombstone(schema, std::move(prefix), t);
    }
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key_prefix_view prefix, tombstone t) {
    if (prefix.is_empty(schema)) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        clustered_row(schema, prefix).apply(t);
    } else {
        apply_row_tombstone(schema, prefix, t);
    }
}

void
mutation_partition::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at) {
    clustered_row(s, key).apply(row_marker(created_at));
}

void mutation_partition::insert_row(const schema& s, const clustering_key& key, deletable_row&& row) {
    auto e = current_allocator().construct<rows_entry>(key, std::move(row));
    _rows.insert(_rows.end(), *e, rows_entry::compare(s));
}

void mutation_partition::insert_row(const schema& s, const clustering_key& key, const deletable_row& row) {
    auto e = current_allocator().construct<rows_entry>(key, row);
    _rows.insert(_rows.end(), *e, rows_entry::compare(s));
}

const row*
mutation_partition::find_row(const schema& s, const clustering_key& key) const {
    auto i = _rows.find(key, rows_entry::compare(s));
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells();
}

deletable_row&
mutation_partition::clustered_row(const schema& s, clustering_key&& key) {
    auto i = _rows.find(key, rows_entry::compare(s));
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(std::move(key));
        _rows.insert(i, *e, rows_entry::compare(s));
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const schema& s, const clustering_key& key) {
    auto i = _rows.find(key, rows_entry::compare(s));
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(key);
        _rows.insert(i, *e, rows_entry::compare(s));
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const schema& s, clustering_key_view key) {
    auto i = _rows.find(key, rows_entry::compare(s));
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(key);
        _rows.insert(i, *e, rows_entry::compare(s));
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous) {
    auto i = _rows.find(pos, rows_entry::compare(s));
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(s, pos, dummy, continuous);
        _rows.insert(i, *e, rows_entry::compare(s));
        return e->row();
    }
    return i->row();
}

mutation_partition::rows_type::const_iterator
mutation_partition::lower_bound(const schema& schema, const query::clustering_range& r) const {
    if (!r.start()) {
        return std::cbegin(_rows);
    }
    return _rows.lower_bound(position_in_partition_view::for_range_start(r), rows_entry::compare(schema));
}

mutation_partition::rows_type::const_iterator
mutation_partition::upper_bound(const schema& schema, const query::clustering_range& r) const {
    if (!r.end()) {
        return std::cend(_rows);
    }
    return _rows.lower_bound(position_in_partition_view::for_range_end(r), rows_entry::compare(schema));
}

boost::iterator_range<mutation_partition::rows_type::const_iterator>
mutation_partition::range(const schema& schema, const query::clustering_range& r) const {
    return boost::make_iterator_range(lower_bound(schema, r), upper_bound(schema, r));
}

template <typename Container>
boost::iterator_range<typename Container::iterator>
unconst(Container& c, boost::iterator_range<typename Container::const_iterator> r) {
    return boost::make_iterator_range(
        c.erase(r.begin(), r.begin()),
        c.erase(r.end(), r.end())
    );
}

template <typename Container>
typename Container::iterator
unconst(Container& c, typename Container::const_iterator i) {
    return c.erase(i, i);
}

boost::iterator_range<mutation_partition::rows_type::iterator>
mutation_partition::range(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->range(schema, r));
}

mutation_partition::rows_type::iterator
mutation_partition::lower_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->lower_bound(schema, r));
}

mutation_partition::rows_type::iterator
mutation_partition::upper_bound(const schema& schema, const query::clustering_range& r) {
    return unconst(_rows, static_cast<const mutation_partition*>(this)->upper_bound(schema, r));
}

template<typename Func>
void mutation_partition::for_each_row(const schema& schema, const query::clustering_range& row_range, bool reversed, Func&& func) const
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

template<typename RowWriter>
void write_cell(RowWriter& w, const query::partition_slice& slice, ::atomic_cell_view c) {
    assert(c.is_live());
    auto wr = w.add().write();
    auto after_timestamp = [&, wr = std::move(wr)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_timestamp>()) {
            return std::move(wr).write_timestamp(c.timestamp());
        } else {
            return std::move(wr).skip_timestamp();
        }
    }();
    auto after_value = [&, wr = std::move(after_timestamp)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_expiry>() && c.is_live_and_has_ttl()) {
            return std::move(wr).write_expiry(c.expiry());
        } else {
            return std::move(wr).skip_expiry();
        }
    }().write_value(c.value());
    [&, wr = std::move(after_value)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_ttl>() && c.is_live_and_has_ttl()) {
            return std::move(wr).write_ttl(c.ttl());
        } else {
            return std::move(wr).skip_ttl();
        }
    }().end_qr_cell();
}

template<typename RowWriter>
void write_cell(RowWriter& w, const query::partition_slice& slice, const data_type& type, collection_mutation_view v) {
    auto ctype = static_pointer_cast<const collection_type_impl>(type);
    if (slice.options.contains<query::partition_slice::option::collections_as_maps>()) {
        ctype = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
    }
    w.add().write().skip_timestamp()
        .skip_expiry()
        .write_value(ctype->to_value(v, slice.cql_format()))
        .skip_ttl()
        .end_qr_cell();
}

template<typename RowWriter>
void write_counter_cell(RowWriter& w, const query::partition_slice& slice, ::atomic_cell_view c) {
    assert(c.is_live());
    auto wr = w.add().write();
    [&, wr = std::move(wr)] () mutable {
        if (slice.options.contains<query::partition_slice::option::send_timestamp>()) {
            return std::move(wr).write_timestamp(c.timestamp());
        } else {
            return std::move(wr).skip_timestamp();
        }
    }().skip_expiry()
            .write_value(counter_cell_view::total_value_type()->decompose(counter_cell_view(c).total_value()))
            .skip_ttl()
            .end_qr_cell();
}

// returns the timestamp of a latest update to the row
static api::timestamp_type hash_row_slice(md5_hasher& hasher,
    const schema& s,
    column_kind kind,
    const row& cells,
    const std::vector<column_id>& columns)
{
    api::timestamp_type max = api::missing_timestamp;
    for (auto id : columns) {
        const atomic_cell_or_collection* cell = cells.find_cell(id);
        if (!cell) {
            continue;
        }
        feed_hash(hasher, id);
        auto&& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            feed_hash(hasher, cell->as_atomic_cell(), def);
            max = std::max(max, cell->as_atomic_cell().timestamp());
        } else {
            auto&& cm = cell->as_collection_mutation();
            feed_hash(hasher, cm, def);
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            max = std::max(max, ctype->last_update(cm));
        }
    }
    return max;
}

template<typename RowWriter>
static void get_compacted_row_slice(const schema& s,
    const query::partition_slice& slice,
    column_kind kind,
    const row& cells,
    const std::vector<column_id>& columns,
    RowWriter& writer)
{
    for (auto id : columns) {
        const atomic_cell_or_collection* cell = cells.find_cell(id);
        if (!cell) {
            writer.add().skip();
        } else {
            auto&& def = s.column_at(kind, id);
            if (def.is_atomic()) {
                auto c = cell->as_atomic_cell();
                if (!c.is_live()) {
                    writer.add().skip();
                } else if (def.is_counter()) {
                    write_counter_cell(writer, slice, cell->as_atomic_cell());
                } else {
                    write_cell(writer, slice, cell->as_atomic_cell());
                }
            } else {
                auto&& mut = cell->as_collection_mutation();
                auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
                if (!ctype->is_any_live(mut)) {
                    writer.add().skip();
                } else {
                    write_cell(writer, slice, def.type, mut);
                }
            }
        }
    }
}

bool has_any_live_data(const schema& s, column_kind kind, const row& cells, tombstone tomb = tombstone(),
                       gc_clock::time_point now = gc_clock::time_point::min()) {
    bool any_live = false;
    cells.for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& cell_or_collection) {
        const column_definition& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            auto&& c = cell_or_collection.as_atomic_cell();
            if (c.is_live(tomb, now, def.is_counter())) {
                any_live = true;
                return stop_iteration::yes;
            }
        } else {
            auto&& cell = cell_or_collection.as_collection_mutation();
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            if (ctype->is_any_live(cell, tomb, now)) {
                any_live = true;
                return stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    });
    return any_live;
}

void
mutation_partition::query_compacted(query::result::partition_writer& pw, const schema& s, uint32_t limit) const {
    const query::partition_slice& slice = pw.slice();

    if (limit == 0) {
        pw.retract();
        return;
    }

    auto static_cells_wr = pw.start().start_static_row().start_cells();

    if (!slice.static_columns.empty()) {
        if (pw.requested_result()) {
            get_compacted_row_slice(s, slice, column_kind::static_column, static_row(), slice.static_columns, static_cells_wr);
        }
        if (pw.requested_digest()) {
            auto pt = partition_tombstone();
            ::feed_hash(pw.digest(), pt);
            auto t = hash_row_slice(pw.digest(), s, column_kind::static_column, static_row(), slice.static_columns);
            pw.last_modified() = std::max({pw.last_modified(), pt.timestamp, t});
        }
    }

    auto rows_wr = std::move(static_cells_wr).end_cells()
            .end_static_row()
            .start_rows();

    uint32_t row_count = 0;

    auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);
    auto send_ck = slice.options.contains(query::partition_slice::option::send_clustering_key);
    for_each_row(s, query::clustering_range::make_open_ended_both_sides(), is_reversed, [&] (const rows_entry& e) {
        if (e.dummy()) {
            return stop_iteration::no;
        }
        auto& row = e.row();
        auto row_tombstone = tombstone_for_row(s, e);

        if (pw.requested_digest()) {
            e.key().feed_hash(pw.digest(), s);
            ::feed_hash(pw.digest(), row_tombstone);
            auto t = hash_row_slice(pw.digest(), s, column_kind::regular_column, row.cells(), slice.regular_columns);
            pw.last_modified() = std::max({pw.last_modified(), row_tombstone.tomb().timestamp, t});
        }

        if (row.is_live(s)) {
            if (pw.requested_result()) {
                auto cells_wr = [&] {
                    if (send_ck) {
                        return rows_wr.add().write_key(e.key()).start_cells().start_cells();
                    } else {
                        return rows_wr.add().skip_key().start_cells().start_cells();
                    }
                }();
                get_compacted_row_slice(s, slice, column_kind::regular_column, row.cells(), slice.regular_columns, cells_wr);
                std::move(cells_wr).end_cells().end_cells().end_qr_clustered_row();
            }
            ++row_count;
            if (--limit == 0) {
                return stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    });

    // If we got no rows, but have live static columns, we should only
    // give them back IFF we did not have any CK restrictions.
    // #589
    // If ck:s exist, and we do a restriction on them, we either have maching
    // rows, or return nothing, since cql does not allow "is null".
    if (row_count == 0
            && (has_ck_selector(pw.ranges())
                    || !has_any_live_data(s, column_kind::static_column, static_row()))) {
        pw.retract();
    } else {
        pw.row_count() += row_count ? : 1;
        pw.partition_count() += 1;
        std::move(rows_wr).end_rows().end_qr_partition();
    }
}

std::ostream&
operator<<(std::ostream& os, const std::pair<column_id, const atomic_cell_or_collection&>& c) {
    return fprint(os, "{column: %s %s}", c.first, c.second);
}

std::ostream&
operator<<(std::ostream& os, const row& r) {
    sstring cells;
    switch (r._type) {
    case row::storage_type::set:
        cells = ::join(", ", r.get_range_set());
        break;
    case row::storage_type::vector:
        cells = ::join(", ", r.get_range_vector());
        break;
    }
    return fprint(os, "{row: %s}", cells);
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
    return fprint(os, "{rows_entry: cont=%d dummy=%d %s %s}", re.continuous(), re.dummy(), re.position(), re._row);
}

std::ostream&
operator<<(std::ostream& os, const mutation_partition& mp) {
    return fprint(os, "{mutation_partition: %s (%s) static cont=%d %s clustered %s}",
                  mp._tombstone, ::join(", ", mp._row_tombstones), mp._static_row_continuous, mp._static_row,
                  ::join(", ", mp._rows));
}

constexpr gc_clock::duration row_marker::no_ttl;
constexpr gc_clock::duration row_marker::dead;

int compare_row_marker_for_merge(const row_marker& left, const row_marker& right) noexcept {
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() > right.timestamp() ? 1 : -1;
    }
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? -1 : 1;
    }
    if (left.is_live()) {
        if (left.is_expiring() != right.is_expiring()) {
            // prefer expiring cells.
            return left.is_expiring() ? 1 : -1;
        }
        if (left.is_expiring() && left.expiry() != right.expiry()) {
            return left.expiry() < right.expiry() ? -1 : 1;
        }
    } else {
        // Both are deleted
        if (left.deletion_time() != right.deletion_time()) {
            // Origin compares big-endian serialized deletion time. That's because it
            // delegates to AbstractCell.reconcile() which compares values after
            // comparing timestamps, which in case of deleted cells will hold
            // serialized expiry.
            return (uint32_t) left.deletion_time().time_since_epoch().count()
                   < (uint32_t) right.deletion_time().time_since_epoch().count() ? -1 : 1;
        }
    }
    return 0;
}

bool
deletable_row::equal(column_kind kind, const schema& s, const deletable_row& other, const schema& other_schema) const {
    if (_deleted_at != other._deleted_at || _marker != other._marker) {
        return false;
    }
    return _cells.equal(kind, s, other._cells, other_schema);
}

void deletable_row::apply_reversibly(const schema& s, deletable_row& src) {
    _cells.apply_reversibly(s, column_kind::regular_column, src._cells);
    _marker.apply_reversibly(src._marker); // noexcept
    _deleted_at.apply_reversibly(src._deleted_at, _marker); // noexcept
}

void deletable_row::revert(const schema& s, deletable_row& src) {
    _cells.revert(s, column_kind::regular_column, src._cells);
    _deleted_at.revert(src._deleted_at);
    _marker.revert(src._marker);
}

void deletable_row::apply(const schema& s, deletable_row&& src) {
    _cells.apply(s, column_kind::regular_column, std::move(src._cells));
    _marker.apply(src._marker);
    _deleted_at.apply(src._deleted_at, _marker);
}

bool
rows_entry::equal(const schema& s, const rows_entry& other) const {
    return equal(s, other, s);
}

bool
rows_entry::equal(const schema& s, const rows_entry& other, const schema& other_schema) const {
    position_in_partition::equal_compare eq(s);
    return eq(position(), other.position())
           && row().equal(column_kind::regular_column, s, other.row(), other_schema);
}

bool mutation_partition::equal(const schema& s, const mutation_partition& p) const {
    return equal(s, p, s);
}

bool mutation_partition::equal(const schema& this_schema, const mutation_partition& p, const schema& p_schema) const {
    if (_tombstone != p._tombstone) {
        return false;
    }

    if (!boost::equal(non_dummy_rows(), p.non_dummy_rows(),
        [&] (const rows_entry& e1, const rows_entry& e2) {
            return e1.equal(this_schema, e2, p_schema);
        }
    )) {
        return false;
    }

    if (!std::equal(_row_tombstones.begin(), _row_tombstones.end(),
        p._row_tombstones.begin(), p._row_tombstones.end(),
        [&] (const range_tombstone& rt1, const range_tombstone& rt2) { return rt1.equal(this_schema, rt2); }
    )) {
        return false;
    }

    return _static_row.equal(column_kind::static_column, this_schema, p._static_row, p_schema);
}

bool mutation_partition::equal_continuity(const schema& s, const mutation_partition& p) const {
    return _static_row_continuous == p._static_row_continuous
        && boost::equal(_rows, p._rows, [&] (const rows_entry& e1, const rows_entry& e2) {
            position_in_partition::equal_compare eq(s);
            return eq(e1.position(), e2.position())
                   && e1.continuous() == e2.continuous()
                   && e1.dummy() == e2.dummy();
        });
}

void
apply_reversibly(const column_definition& def, atomic_cell_or_collection& dst,  atomic_cell_or_collection& src) {
    // Must be run via with_linearized_managed_bytes() context, but assume it is
    // provided via an upper layer
    if (def.is_atomic()) {
        auto&& src_ac = src.as_atomic_cell_ref();
        if (def.is_counter()) {
            auto did_apply = counter_cell_view::apply_reversibly(dst, src);
            src_ac.set_revert(did_apply);
        } else {
            if (compare_atomic_cell_for_merge(dst.as_atomic_cell(), src.as_atomic_cell()) < 0) {
                std::swap(dst, src);
                src_ac.set_revert(true);
            } else {
                src_ac.set_revert(false);
            }
        }
    } else {
        auto ct = static_pointer_cast<const collection_type_impl>(def.type);
        src = ct->merge(dst.as_collection_mutation(), src.as_collection_mutation());
        std::swap(dst, src);
    }
}

void
revert(const column_definition& def, atomic_cell_or_collection& dst, atomic_cell_or_collection& src) noexcept {
    static_assert(std::is_nothrow_move_constructible<atomic_cell_or_collection>::value
                  && std::is_nothrow_move_assignable<atomic_cell_or_collection>::value,
                  "for std::swap() to be noexcept");
    if (def.is_atomic()) {
        auto&& ac = src.as_atomic_cell_ref();
        if (ac.is_revert_set()) {
            ac.set_revert(false);
            if (def.is_counter()) {
                counter_cell_view::revert_apply(dst, src);
            } else {
                std::swap(dst, src);
            }
        }
    } else {
        std::swap(dst, src);
    }
}

void
row::apply(const column_definition& column, const atomic_cell_or_collection& value) {
    atomic_cell_or_collection tmp(value);
    apply(column, std::move(tmp));
}

void
row::apply(const column_definition& column, atomic_cell_or_collection&& value) {
    apply_reversibly(column, value);
}

template<typename Func, typename Rollback>
void row::for_each_cell(Func&& func, Rollback&& rollback) {
    static_assert(noexcept(rollback(std::declval<column_id>(), std::declval<atomic_cell_or_collection&>())),
                           "rollback must be noexcept");

    if (_type == storage_type::vector) {
        unsigned i = 0;
        try {
            for (; i < _storage.vector.v.size(); i++) {
                if (_storage.vector.present.test(i)) {
                    func(i, _storage.vector.v[i]);
                }
            }
        } catch (...) {
            while (i) {
                --i;
                if (_storage.vector.present.test(i)) {
                    rollback(i, _storage.vector.v[i]);
                }
            }
            throw;
        }
    } else {
        auto i = _storage.set.begin();
        try {
            while (i != _storage.set.end()) {
                func(i->id(), i->cell());
                ++i;
            }
        } catch (...) {
            while (i != _storage.set.begin()) {
                --i;
                rollback(i->id(), i->cell());
            }
            throw;
        }
    }
}

void
row::apply_reversibly(const column_definition& column, atomic_cell_or_collection& value) {
    static_assert(std::is_nothrow_move_constructible<atomic_cell_or_collection>::value
                  && std::is_nothrow_move_assignable<atomic_cell_or_collection>::value,
                  "noexcept required for atomicity");

    // our mutations are not yet immutable
    auto id = column.id;
    if (_type == storage_type::vector && id < max_vector_size) {
        if (id >= _storage.vector.v.size()) {
            _storage.vector.v.resize(id);
            _storage.vector.v.emplace_back(std::move(value));
            _storage.vector.present.set(id);
            _size++;
        } else if (!bool(_storage.vector.v[id])) {
            _storage.vector.v[id] = std::move(value);
            _storage.vector.present.set(id);
            _size++;
        } else {
            ::apply_reversibly(column, _storage.vector.v[id], value);
        }
    } else {
        if (_type == storage_type::vector) {
            vector_to_set();
        }
        auto i = _storage.set.lower_bound(id, cell_entry::compare());
        if (i == _storage.set.end() || i->id() != id) {
            cell_entry* e = current_allocator().construct<cell_entry>(id);
            std::swap(e->_cell, value);
            _storage.set.insert(i, *e);
            _size++;
        } else {
            ::apply_reversibly(column, i->cell(), value);
        }
    }
}

void
row::revert(const column_definition& column, atomic_cell_or_collection& src) noexcept {
    auto id = column.id;
    if (_type == storage_type::vector) {
        auto& dst = _storage.vector.v[id];
        if (!src) {
            std::swap(dst, src);
            _storage.vector.present.reset(id);
            --_size;
        } else {
            ::revert(column, dst, src);
        }
    } else {
        auto i = _storage.set.find(id, cell_entry::compare());
        auto& dst = i->cell();
        if (!src) {
            std::swap(dst, src);
            _storage.set.erase_and_dispose(i, current_deleter<cell_entry>());
            --_size;
        } else {
            ::revert(column, dst, src);
        }
    }
}

void
row::append_cell(column_id id, atomic_cell_or_collection value) {
    if (_type == storage_type::vector && id < max_vector_size) {
        _storage.vector.v.resize(id);
        _storage.vector.v.emplace_back(std::move(value));
        _storage.vector.present.set(id);
    } else {
        if (_type == storage_type::vector) {
            vector_to_set();
        }
        auto e = current_allocator().construct<cell_entry>(id, std::move(value));
        _storage.set.insert(_storage.set.end(), *e);
    }
    _size++;
}

const atomic_cell_or_collection*
row::find_cell(column_id id) const {
    if (_type == storage_type::vector) {
        if (id >= _storage.vector.v.size() || !_storage.vector.present.test(id)) {
            return nullptr;
        }
        return &_storage.vector.v[id];
    } else {
        auto i = _storage.set.find(id, cell_entry::compare());
        if (i == _storage.set.end()) {
            return nullptr;
        }
        return &i->cell();
    }
}

size_t row::external_memory_usage() const {
    size_t mem = 0;
    if (_type == storage_type::vector) {
        mem += _storage.vector.v.external_memory_usage();
        for (auto&& ac_o_c : _storage.vector.v) {
            mem += ac_o_c.external_memory_usage();
        }
    } else {
        for (auto&& ce : _storage.set) {
            mem += sizeof(cell_entry) + ce.cell().external_memory_usage();
        }
    }
    return mem;
}

size_t rows_entry::memory_usage() const {
    size_t size = 0;
    if (!dummy()) {
        size += key().external_memory_usage();
    }
    return size +
           row().cells().external_memory_usage() +
           sizeof(rows_entry);
}

size_t mutation_partition::external_memory_usage() const {
    size_t sum = 0;
    auto& s = static_row();
    sum += s.external_memory_usage();
    for (auto& clr : clustered_rows()) {
        sum += clr.memory_usage();
    }

    for (auto& rtb : row_tombstones()) {
        sum += rtb.memory_usage();
    }

    return sum;
}

template<bool reversed, typename Func>
void mutation_partition::trim_rows(const schema& s,
    const std::vector<query::clustering_range>& row_ranges,
    Func&& func)
{
    static_assert(std::is_same<stop_iteration, std::result_of_t<Func(rows_entry&)>>::value, "Bad func signature");

    stop_iteration stop = stop_iteration::no;
    auto last = reversal_traits<reversed>::begin(_rows);
    auto deleter = current_deleter<rows_entry>();

    auto range_begin = [this, &s] (const query::clustering_range& range) {
        return reversed ? upper_bound(s, range) : lower_bound(s, range);
    };

    auto range_end = [this, &s] (const query::clustering_range& range) {
        return reversed ? lower_bound(s, range) : upper_bound(s, range);
    };

    for (auto&& row_range : row_ranges) {
        if (stop) {
            break;
        }

        last = reversal_traits<reversed>::erase_and_dispose(_rows, last,
            reversal_traits<reversed>::maybe_reverse(_rows, range_begin(row_range)), deleter);

        auto end = reversal_traits<reversed>::maybe_reverse(_rows, range_end(row_range));
        while (last != end && !stop) {
            rows_entry& e = *last;
            stop = func(e);
            if (e.empty()) {
                last = reversal_traits<reversed>::erase_dispose_and_update_end(_rows, last, deleter, end);
            } else {
                ++last;
            }
        }
    }

    reversal_traits<reversed>::erase_and_dispose(_rows, last, reversal_traits<reversed>::end(_rows), deleter);
}

uint32_t mutation_partition::do_compact(const schema& s,
    gc_clock::time_point query_time,
    const std::vector<query::clustering_range>& row_ranges,
    bool reverse,
    uint32_t row_limit,
    can_gc_fn& can_gc)
{
    assert(row_limit > 0);

    auto gc_before = saturating_subtract(query_time, s.gc_grace_seconds());

    auto should_purge_tombstone = [&] (const tombstone& t) {
        return t.deletion_time < gc_before && can_gc(t);
    };
    auto should_purge_row_tombstone = [&] (const row_tombstone& t) {
        return t.max_deletion_time() < gc_before && can_gc(t.tomb());
    };

    bool static_row_live = _static_row.compact_and_expire(s, column_kind::static_column, row_tombstone(_tombstone),
        query_time, can_gc, gc_before);

    uint32_t row_count = 0;

    auto row_callback = [&] (rows_entry& e) {
        if (e.dummy()) {
            return stop_iteration::no;
        }
        deletable_row& row = e.row();
        row_tombstone tomb = tombstone_for_row(s, e);

        bool is_live = row.cells().compact_and_expire(s, column_kind::regular_column, tomb, query_time, can_gc, gc_before);
        is_live |= row.marker().compact_and_expire(tomb.tomb(), query_time, can_gc, gc_before);

        if (should_purge_row_tombstone(row.deleted_at())) {
            row.remove_tombstone();
        }

        return stop_iteration(is_live && ++row_count == row_limit);
    };

    if (reverse) {
        trim_rows<true>(s, row_ranges, row_callback);
    } else {
        trim_rows<false>(s, row_ranges, row_callback);
    }

    // #589 - Do not add extra row for statics unless we did a CK range-less query.
    // See comment in query
    if (row_count == 0 && static_row_live && !has_ck_selector(row_ranges)) {
        ++row_count;
    }

    _row_tombstones.erase_where([&] (auto&& rt) {
        return should_purge_tombstone(rt.tomb) || rt.tomb.timestamp <= _tombstone.timestamp;
    });
    if (should_purge_tombstone(_tombstone)) {
        _tombstone = tombstone();
    }

    // FIXME: purge unneeded prefix tombstones based on row_ranges

    return row_count;
}

uint32_t
mutation_partition::compact_for_query(
    const schema& s,
    gc_clock::time_point query_time,
    const std::vector<query::clustering_range>& row_ranges,
    bool reverse,
    uint32_t row_limit)
{
    return do_compact(s, query_time, row_ranges, reverse, row_limit, always_gc);
}

void mutation_partition::compact_for_compaction(const schema& s,
    can_gc_fn& can_gc, gc_clock::time_point compaction_time)
{
    static const std::vector<query::clustering_range> all_rows = {
        query::clustering_range::make_open_ended_both_sides()
    };

    do_compact(s, compaction_time, all_rows, false, query::max_rows, can_gc);
}

// Returns true if there is no live data or tombstones.
bool mutation_partition::empty() const
{
    if (_tombstone.timestamp != api::missing_timestamp) {
        return false;
    }
    return !_static_row.size() && _rows.empty() && _row_tombstones.empty();
}

bool
deletable_row::is_live(const schema& s, tombstone base_tombstone, gc_clock::time_point query_time) const {
    // _created_at corresponds to the row marker cell, present for rows
    // created with the 'insert' statement. If row marker is live, we know the
    // row is live. Otherwise, a row is considered live if it has any cell
    // which is live.
    base_tombstone.apply(_deleted_at.tomb());
    return _marker.is_live(base_tombstone, query_time)
           || has_any_live_data(s, column_kind::regular_column, _cells, base_tombstone, query_time);
}

bool
mutation_partition::is_static_row_live(const schema& s, gc_clock::time_point query_time) const {
    return has_any_live_data(s, column_kind::static_column, static_row(), _tombstone, query_time);
}

size_t
mutation_partition::live_row_count(const schema& s, gc_clock::time_point query_time) const {
    size_t count = 0;

    for (const rows_entry& e : non_dummy_rows()) {
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
    , _flags(std::move(o._flags))
{ }

row::row(const row& o)
    : _type(o._type)
    , _size(o._size)
{
    if (_type == storage_type::vector) {
        new (&_storage.vector) vector_storage(o._storage.vector);
    } else {
        auto cloner = [] (const auto& x) {
            return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
        };
        new (&_storage.set) map_type;
        try {
            _storage.set.clone_from(o._storage.set, cloner, current_deleter<cell_entry>());
        } catch (...) {
            _storage.set.~map_type();
            throw;
        }
    }
}

row::~row() {
    if (_type == storage_type::vector) {
        _storage.vector.~vector_storage();
    } else {
        _storage.set.clear_and_dispose(current_deleter<cell_entry>());
        _storage.set.~map_type();
    }
}

row::cell_entry::cell_entry(const cell_entry& o)
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

void row::vector_to_set()
{
    assert(_type == storage_type::vector);
    map_type set;
    try {
    for (auto i : bitsets::for_each_set(_storage.vector.present)) {
        auto& c = _storage.vector.v[i];
        auto e = current_allocator().construct<cell_entry>(i, std::move(c));
        set.insert(set.end(), *e);
    }
    } catch (...) {
        set.clear_and_dispose([this, del = current_deleter<cell_entry>()] (cell_entry* ce) noexcept {
            _storage.vector.v[ce->id()] = std::move(ce->cell());
            del(ce);
        });
        throw;
    }
    _storage.vector.~vector_storage();
    new (&_storage.set) map_type(std::move(set));
    _type = storage_type::set;
}

void row::reserve(column_id last_column)
{
    if (_type == storage_type::vector && last_column >= internal_count) {
        if (last_column >= max_vector_size) {
            vector_to_set();
        } else {
            _storage.vector.v.reserve(last_column);
        }
    }
}

template<typename Func>
auto row::with_both_ranges(const row& other, Func&& func) const {
    if (_type == storage_type::vector) {
        if (other._type == storage_type::vector) {
            return func(get_range_vector(), other.get_range_vector());
        } else {
            return func(get_range_vector(), other.get_range_set());
        }
    } else {
        if (other._type == storage_type::vector) {
            return func(get_range_set(), other.get_range_vector());
        } else {
            return func(get_range_set(), other.get_range_set());
        }
    }
}

bool row::operator==(const row& other) const {
    if (size() != other.size()) {
        return false;
    }

    auto cells_equal = [] (std::pair<column_id, const atomic_cell_or_collection&> c1,
                           std::pair<column_id, const atomic_cell_or_collection&> c2) {
        return c1.first == c2.first && c1.second == c2.second;
    };
    return with_both_ranges(other, [&] (auto r1, auto r2) {
        return boost::equal(r1, r2, cells_equal);
    });
}

bool row::equal(column_kind kind, const schema& this_schema, const row& other, const schema& other_schema) const {
    if (size() != other.size()) {
        return false;
    }

    auto cells_equal = [&] (std::pair<column_id, const atomic_cell_or_collection&> c1,
                            std::pair<column_id, const atomic_cell_or_collection&> c2) {
        static_assert(schema::row_column_ids_are_ordered_by_name::value, "Relying on column ids being ordered by name");
        return this_schema.column_at(kind, c1.first).name() == other_schema.column_at(kind, c2.first).name()
               && c1.second == c2.second;
    };
    return with_both_ranges(other, [&] (auto r1, auto r2) {
        return boost::equal(r1, r2, cells_equal);
    });
}

row::row() {
    new (&_storage.vector) vector_storage;
}

row::row(row&& other) noexcept
    : _type(other._type), _size(other._size) {
    if (_type == storage_type::vector) {
        new (&_storage.vector) vector_storage(std::move(other._storage.vector));
    } else {
        new (&_storage.set) map_type(std::move(other._storage.set));
    }
}

row& row::operator=(row&& other) noexcept {
    if (this != &other) {
        this->~row();
        new (this) row(std::move(other));
    }
    return *this;
}

void row::apply_reversibly(const schema& s, column_kind kind, row& other) {
    if (other.empty()) {
        return;
    }
    if (other._type == storage_type::vector) {
        reserve(other._storage.vector.v.size() - 1);
    } else {
        reserve(other._storage.set.rbegin()->id());
    }
    other.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) {
        apply_reversibly(s.column_at(kind, id), cell);
    }, [&] (column_id id, atomic_cell_or_collection& cell) noexcept {
        revert(s.column_at(kind, id), cell);
    });
}

void row::apply(const schema& s, column_kind kind, const row& other) {
    if (other.empty()) {
        return;
    }
    if (other._type == storage_type::vector) {
        reserve(other._storage.vector.v.size() - 1);
    } else {
        reserve(other._storage.set.rbegin()->id());
    }
    other.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        apply(s.column_at(kind, id), cell);
    });
}

void row::apply(const schema& s, column_kind kind, row&& other) {
    if (other.empty()) {
        return;
    }
    if (other._type == storage_type::vector) {
        reserve(other._storage.vector.v.size() - 1);
    } else {
        reserve(other._storage.set.rbegin()->id());
    }
    other.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) {
        apply(s.column_at(kind, id), std::move(cell));
    });
}

void row::revert(const schema& s, column_kind kind, row& other) noexcept {
    other.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) noexcept {
        revert(s.column_at(kind, id), cell);
    });
}

bool row::compact_and_expire(const schema& s, column_kind kind, row_tombstone tomb, gc_clock::time_point query_time,
    can_gc_fn& can_gc, gc_clock::time_point gc_before)
{
    bool any_live = false;
    remove_if([&] (column_id id, atomic_cell_or_collection& c) {
        bool erase = false;
        const column_definition& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            atomic_cell_view cell = c.as_atomic_cell();
            auto can_erase_cell = [&] {
                return cell.deletion_time() < gc_before && can_gc(tombstone(cell.timestamp(), cell.deletion_time()));
            };

            if (cell.is_covered_by(tomb.regular(), def.is_counter())) {
                erase = true;
            } else if (cell.has_expired(query_time)) {
                erase = can_erase_cell();
                if (!erase) {
                    c = atomic_cell::make_dead(cell.timestamp(), cell.deletion_time());
                }
            } else if (!cell.is_live()) {
                erase = can_erase_cell();
            } else if (cell.is_covered_by(tomb.shadowable().tomb(), def.is_counter())) {
                erase = true;
            } else {
                any_live = true;
            }
        } else {
            auto&& cell = c.as_collection_mutation();
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            auto m_view = ctype->deserialize_mutation_form(cell);
            collection_type_impl::mutation m = m_view.materialize();
            any_live |= m.compact_and_expire(tomb, query_time, can_gc, gc_before);
            if (m.cells.empty() && m.tomb <= tomb.tomb()) {
                erase = true;
            } else {
                c = ctype->serialize_mutation_form(m);
            }
        }
        return erase;
    });
    return any_live;
}

deletable_row deletable_row::difference(const schema& s, column_kind kind, const deletable_row& other) const
{
    deletable_row dr;
    if (_deleted_at > other._deleted_at) {
        dr.apply(_deleted_at);
    }
    if (compare_row_marker_for_merge(_marker, other._marker) > 0) {
        dr.apply(_marker);
    }
    dr._cells = _cells.difference(s, kind, other._cells);
    return dr;
}

row row::difference(const schema& s, column_kind kind, const row& other) const
{
    row r;
    with_both_ranges(other, [&] (auto this_range, auto other_range) {
        auto it = other_range.begin();
        for (auto&& c : this_range) {
            while (it != other_range.end() && it->first < c.first) {
                ++it;
            }
            auto& cdef = s.column_at(kind, c.first);
            if (it == other_range.end() || it->first != c.first) {
                r.append_cell(c.first, c.second);
            } else if (cdef.is_counter()) {
                auto cell = counter_cell_view::difference(c.second.as_atomic_cell(), it->second.as_atomic_cell());
                if (cell) {
                    r.append_cell(c.first, std::move(*cell));
                }
            } else if (s.column_at(kind, c.first).is_atomic()) {
                if (compare_atomic_cell_for_merge(c.second.as_atomic_cell(), it->second.as_atomic_cell()) > 0) {
                    r.append_cell(c.first, c.second);
                }
            } else {
                auto ct = static_pointer_cast<const collection_type_impl>(s.column_at(kind, c.first).type);
                auto diff = ct->difference(c.second.as_collection_mutation(), it->second.as_collection_mutation());
                if (!ct->is_empty(diff)) {
                    r.append_cell(c.first, std::move(diff));
                }
            }
        }
    });
    return r;
}

mutation_partition mutation_partition::difference(schema_ptr s, const mutation_partition& other) const
{
    mutation_partition mp(s);
    if (_tombstone > other._tombstone) {
        mp.apply(_tombstone);
    }
    mp._static_row = _static_row.difference(*s, column_kind::static_column, other._static_row);

    mp._row_tombstones = _row_tombstones.difference(*s, other._row_tombstones);

    auto it_r = other._rows.begin();
    rows_entry::compare cmp_r(*s);
    for (auto&& r : _rows) {
        if (r.dummy()) {
            continue;
        }
        while (it_r != other._rows.end() && (it_r->dummy() || cmp_r(*it_r, r))) {
            ++it_r;
        }
        if (it_r == other._rows.end() || !it_r->key().equal(*s, r.key())) {
            mp.insert_row(*s, r.key(), r.row());
        } else {
            auto dr = r.row().difference(*s, column_kind::regular_column, it_r->row());
            if (!dr.empty()) {
                mp.insert_row(*s, r.key(), std::move(dr));
            }
        }
    }
    return mp;
}

void mutation_partition::accept(const schema& s, mutation_partition_visitor& v) const {
    v.accept_partition_tombstone(_tombstone);
    _static_row.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        const column_definition& def = s.static_column_at(id);
        if (def.is_atomic()) {
            v.accept_static_cell(id, cell.as_atomic_cell());
        } else {
            v.accept_static_cell(id, cell.as_collection_mutation());
        }
    });
    for (const range_tombstone& rt : _row_tombstones) {
        v.accept_row_tombstone(rt);
    }
    for (const rows_entry& e : _rows) {
        const deletable_row& dr = e.row();
        v.accept_row(e.position(), dr.deleted_at(), dr.marker(), e.dummy(), e.continuous());
        dr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
            const column_definition& def = s.regular_column_at(id);
            if (def.is_atomic()) {
                v.accept_row_cell(id, cell.as_atomic_cell());
            } else {
                v.accept_row_cell(id, cell.as_collection_mutation());
            }
        });
    }
}

void
mutation_partition::upgrade(const schema& old_schema, const schema& new_schema) {
    // We need to copy to provide strong exception guarantees.
    mutation_partition tmp(new_schema.shared_from_this());
    converting_mutation_partition_applier v(old_schema.get_column_mapping(), new_schema, tmp);
    accept(old_schema, v);
    *this = std::move(tmp);
}

void row_marker::apply_reversibly(row_marker& rm) noexcept {
    if (compare_row_marker_for_merge(*this, rm) < 0) {
        std::swap(*this, rm);
    } else {
        rm = *this;
    }
}

void row_marker::revert(row_marker& rm) noexcept {
    std::swap(*this, rm);
}

// Adds mutation to query::result.
class mutation_querier {
    const schema& _schema;
    query::result_memory_accounter& _memory_accounter;
    query::result::partition_writer& _pw;
    ser::qr_partition__static_row__cells<bytes_ostream> _static_cells_wr;
    bool _live_data_in_static_row{};
    uint32_t _live_clustering_rows = 0;
    stdx::optional<ser::qr_partition__rows<bytes_ostream>> _rows_wr;
    bool _short_reads_allowed;
private:
    void query_static_row(const row& r, tombstone current_tombstone);
    void prepare_writers();
public:
    mutation_querier(const schema& s, query::result::partition_writer& pw,
                     query::result_memory_accounter& memory_accounter);
    void consume(tombstone) { }
    // Requires that sr.has_any_live_data()
    stop_iteration consume(static_row&& sr, tombstone current_tombstone);
    // Requires that cr.has_any_live_data()
    stop_iteration consume(clustering_row&& cr, row_tombstone current_tombstone);
    stop_iteration consume(range_tombstone&&) { return stop_iteration::no; }
    uint32_t consume_end_of_stream();
};

mutation_querier::mutation_querier(const schema& s, query::result::partition_writer& pw,
                                   query::result_memory_accounter& memory_accounter)
    : _schema(s)
    , _memory_accounter(memory_accounter)
    , _pw(pw)
    , _static_cells_wr(pw.start().start_static_row().start_cells())
    , _short_reads_allowed(pw.slice().options.contains<query::partition_slice::option::allow_short_read>())
{
}

void mutation_querier::query_static_row(const row& r, tombstone current_tombstone)
{
    const query::partition_slice& slice = _pw.slice();
    if (!slice.static_columns.empty()) {
        if (_pw.requested_result()) {
            auto start = _static_cells_wr._out.size();
            get_compacted_row_slice(_schema, slice, column_kind::static_column,
                                    r, slice.static_columns, _static_cells_wr);
            _memory_accounter.update(_static_cells_wr._out.size() - start);
        } else if (_short_reads_allowed) {
            seastar::measuring_output_stream stream;
            ser::qr_partition__static_row__cells<seastar::measuring_output_stream> out(stream, { });
            get_compacted_row_slice(_schema, slice, column_kind::static_column,
                                    r, slice.static_columns, _static_cells_wr);
            _memory_accounter.update(stream.size());
        }
        if (_pw.requested_digest()) {
            ::feed_hash(_pw.digest(), current_tombstone);
            auto t = hash_row_slice(_pw.digest(), _schema, column_kind::static_column,
                                    r, slice.static_columns);
            _pw.last_modified() = std::max({_pw.last_modified(), current_tombstone.timestamp, t});
        }
    }
    _rows_wr.emplace(std::move(_static_cells_wr).end_cells().end_static_row().start_rows());
}

stop_iteration mutation_querier::consume(static_row&& sr, tombstone current_tombstone) {
    query_static_row(sr.cells(), current_tombstone);
    _live_data_in_static_row = true;
    return stop_iteration::no;
}

void mutation_querier::prepare_writers() {
    if (!_rows_wr) {
        row empty_row;
        query_static_row(empty_row, { });
        _live_data_in_static_row = false;
    }
}

stop_iteration mutation_querier::consume(clustering_row&& cr, row_tombstone current_tombstone) {
    prepare_writers();

    const query::partition_slice& slice = _pw.slice();

    if (_pw.requested_digest()) {
        cr.key().feed_hash(_pw.digest(), _schema);
        ::feed_hash(_pw.digest(), current_tombstone);
        auto t = hash_row_slice(_pw.digest(), _schema, column_kind::regular_column, cr.cells(), slice.regular_columns);
        _pw.last_modified() = std::max({_pw.last_modified(), current_tombstone.tomb().timestamp, t});
    }

    auto write_row = [&] (auto& rows_writer) {
        auto cells_wr = [&] {
            if (slice.options.contains(query::partition_slice::option::send_clustering_key)) {
                return rows_writer.add().write_key(cr.key()).start_cells().start_cells();
            } else {
                return rows_writer.add().skip_key().start_cells().start_cells();
            }
        }();
        get_compacted_row_slice(_schema, slice, column_kind::regular_column, cr.cells(), slice.regular_columns, cells_wr);
        std::move(cells_wr).end_cells().end_cells().end_qr_clustered_row();
    };

    auto stop = stop_iteration::no;
    if (_pw.requested_result()) {
        auto start = _rows_wr->_out.size();
        write_row(*_rows_wr);
        stop = _memory_accounter.update_and_check(_rows_wr->_out.size() - start);
    } else if (_short_reads_allowed) {
        seastar::measuring_output_stream stream;
        ser::qr_partition__rows<seastar::measuring_output_stream> out(stream, { });
        write_row(out);
        stop = _memory_accounter.update_and_check(stream.size());
    }

    _live_clustering_rows++;
    return stop && stop_iteration(_short_reads_allowed);
}

uint32_t mutation_querier::consume_end_of_stream() {
    prepare_writers();

    // If we got no rows, but have live static columns, we should only
    // give them back IFF we did not have any CK restrictions.
    // #589
    // If ck:s exist, and we do a restriction on them, we either have maching
    // rows, or return nothing, since cql does not allow "is null".
    if (!_live_clustering_rows
        && (has_ck_selector(_pw.ranges()) || !_live_data_in_static_row)) {
        _pw.retract();
        return 0;
    } else {
        auto live_rows = std::max(_live_clustering_rows, uint32_t(1));
        _pw.row_count() += live_rows;
        _pw.partition_count() += 1;
        std::move(*_rows_wr).end_rows().end_qr_partition();
        return live_rows;
    }
}

class query_result_builder {
    const schema& _schema;
    query::result::builder& _rb;
    stdx::optional<query::result::partition_writer> _pw;
    stdx::optional<mutation_querier> _mutation_consumer;
    stop_iteration _stop;
    stop_iteration _short_read_allowed;
public:
    query_result_builder(const schema& s, query::result::builder& rb)
        : _schema(s), _rb(rb)
        , _short_read_allowed(_rb.slice().options.contains<query::partition_slice::option::allow_short_read>())
    { }

    void consume_new_partition(const dht::decorated_key& dk) {
        _pw.emplace(_rb.add_partition(_schema, dk.key()));
        _mutation_consumer.emplace(mutation_querier(_schema, *_pw, _rb.memory_accounter()));
    }

    void consume(tombstone t) {
        _mutation_consumer->consume(t);
    }
    stop_iteration consume(static_row&& sr, tombstone t, bool) {
        _stop = _mutation_consumer->consume(std::move(sr), t) && _short_read_allowed;
        return _stop;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone t,  bool) {
        _stop = _mutation_consumer->consume(std::move(cr), t) && _short_read_allowed;
        return _stop;
    }
    stop_iteration consume(range_tombstone&& rt) {
        _stop = _mutation_consumer->consume(std::move(rt)) && _short_read_allowed;
        return _stop;
    }

    stop_iteration consume_end_of_partition() {
        auto live_rows_in_partition = _mutation_consumer->consume_end_of_stream();
        if (_short_read_allowed && live_rows_in_partition > 0 && !_stop) {
            _stop = _rb.memory_accounter().check();
        }
        if (_stop) {
            _rb.mark_as_short_read();
        }
        return _stop;
    }

    void consume_end_of_stream() {
    }
};

future<> data_query(
        schema_ptr s,
        const mutation_source& source,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        uint32_t row_limit,
        uint32_t partition_limit,
        gc_clock::time_point query_time,
        query::result::builder& builder,
        tracing::trace_state_ptr trace_ptr)
{
    if (row_limit == 0 || slice.partition_row_limit() == 0 || partition_limit == 0) {
        return make_ready_future<>();
    }

    auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);

    auto qrb = query_result_builder(*s, builder);
    auto cfq = make_stable_flattened_mutations_consumer<compact_for_query<emit_only_live_rows::yes, query_result_builder>>(
            *s, query_time, slice, row_limit, partition_limit, std::move(qrb));

    return do_with(source.make_flat_mutation_reader(s, range, slice, service::get_local_sstable_query_read_priority(), std::move(trace_ptr),
                                                    streamed_mutation::forwarding::no, mutation_reader::forwarding::no),
                   [cfq = std::move(cfq), is_reversed] (flat_mutation_reader& reader) mutable {
        return reader.consume(std::move(cfq), flat_mutation_reader::consume_reversed_partitions(is_reversed));
    });
}

class reconcilable_result_builder {
    const schema& _schema;
    const query::partition_slice& _slice;

    std::vector<partition> _result;
    uint32_t _live_rows;

    bool _has_ck_selector{};
    bool _static_row_is_alive{};
    uint32_t _total_live_rows = 0;
    query::result_memory_accounter _memory_accounter;
    stop_iteration _stop;
    bool _short_read_allowed;
    stdx::optional<streamed_mutation_freezer> _mutation_consumer;
public:
    reconcilable_result_builder(const schema& s, const query::partition_slice& slice,
                                query::result_memory_accounter&& accounter)
        : _schema(s), _slice(slice)
        , _memory_accounter(std::move(accounter))
        , _short_read_allowed(slice.options.contains<query::partition_slice::option::allow_short_read>())
    { }

    void consume_new_partition(const dht::decorated_key& dk) {
        _has_ck_selector = has_ck_selector(_slice.row_ranges(_schema, dk.key()));
        _static_row_is_alive = false;
        _live_rows = 0;
        auto is_reversed = _slice.options.contains(query::partition_slice::option::reversed);
        _mutation_consumer.emplace(streamed_mutation_freezer(_schema, dk.key(), is_reversed));
    }

    void consume(tombstone t) {
        _mutation_consumer->consume(t);
    }
    stop_iteration consume(static_row&& sr, tombstone, bool is_alive) {
        _static_row_is_alive = is_alive;
        _memory_accounter.update(sr.memory_usage());
        return _mutation_consumer->consume(std::move(sr));
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool is_alive) {
        _live_rows += is_alive;
        auto stop = _memory_accounter.update_and_check(cr.memory_usage());
        if (is_alive) {
            // We are considering finishing current read only after consuming a
            // live clustering row. While sending a single live row is enough to
            // guarantee progress, not ending the result on a live row would
            // mean that the next page fetch will read all tombstones after the
            // last live row again.
            _stop = stop && stop_iteration(_short_read_allowed);
        }
        return _mutation_consumer->consume(std::move(cr)) || _stop;
    }
    stop_iteration consume(range_tombstone&& rt) {
        _memory_accounter.update(rt.memory_usage());
        return _mutation_consumer->consume(std::move(rt));
    }

    stop_iteration consume_end_of_partition() {
        if (_live_rows == 0 && _static_row_is_alive && !_has_ck_selector) {
            ++_live_rows;
            // Normally we count only live clustering rows, to guarantee that
            // the next page fetch won't ask for the same range. However,
            // if we return just a single static row we can stop the result as
            // well. Next page fetch will ask for the next partition and if we
            // don't do that we could end up with an unbounded number of
            // partitions with only a static row.
            _stop = _stop || (_memory_accounter.check() && stop_iteration(_short_read_allowed));
        }
        _total_live_rows += _live_rows;
        _result.emplace_back(partition { _live_rows, _mutation_consumer->consume_end_of_stream() });
        return _stop;
    }

    reconcilable_result consume_end_of_stream() {
        return reconcilable_result(_total_live_rows, std::move(_result),
                                   query::short_read(bool(_stop)),
                                   std::move(_memory_accounter).done());
    }
};

future<reconcilable_result>
static do_mutation_query(schema_ptr s,
               mutation_source source,
               const dht::partition_range& range,
               const query::partition_slice& slice,
               uint32_t row_limit,
               uint32_t partition_limit,
               gc_clock::time_point query_time,
               query::result_memory_accounter&& accounter,
               tracing::trace_state_ptr trace_ptr)
{
    if (row_limit == 0 || slice.partition_row_limit() == 0 || partition_limit == 0) {
        return make_ready_future<reconcilable_result>(reconcilable_result());
    }

    auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);

    auto rrb = reconcilable_result_builder(*s, slice, std::move(accounter));
    auto cfq = make_stable_flattened_mutations_consumer<compact_for_query<emit_only_live_rows::no, reconcilable_result_builder>>(
            *s, query_time, slice, row_limit, partition_limit, std::move(rrb));

    return do_with(source.make_flat_mutation_reader(s, range, slice, service::get_local_sstable_query_read_priority(), std::move(trace_ptr),
                                                    streamed_mutation::forwarding::no, mutation_reader::forwarding::no),
                   [cfq = std::move(cfq), is_reversed] (flat_mutation_reader& reader) mutable {
        return reader.consume(std::move(cfq), flat_mutation_reader::consume_reversed_partitions(is_reversed));
    });
}

static thread_local auto mutation_query_stage = seastar::make_execution_stage("mutation_query", do_mutation_query);

future<reconcilable_result>
mutation_query(schema_ptr s,
               mutation_source source,
               const dht::partition_range& range,
               const query::partition_slice& slice,
               uint32_t row_limit,
               uint32_t partition_limit,
               gc_clock::time_point query_time,
               query::result_memory_accounter&& accounter,
               tracing::trace_state_ptr trace_ptr)
{
    return mutation_query_stage(std::move(s), std::move(source), seastar::cref(range), seastar::cref(slice),
                                row_limit, partition_limit, query_time, std::move(accounter), std::move(trace_ptr));
}

deletable_row::deletable_row(clustering_row&& cr)
    : _deleted_at(cr.tomb())
    , _marker(std::move(cr.marker()))
    , _cells(std::move(cr.cells()))
{ }

class counter_write_query_result_builder {
    const schema& _schema;
    mutation_opt _mutation;
public:
    counter_write_query_result_builder(const schema& s) : _schema(s) { }
    void consume_new_partition(const dht::decorated_key& dk) {
        _mutation = mutation(dk, _schema.shared_from_this());
    }
    void consume(tombstone) { }
    stop_iteration consume(static_row&& sr, tombstone, bool) {
        _mutation->partition().static_row() = std::move(sr.cells());
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone,  bool) {
        _mutation->partition().insert_row(_schema, cr.key(), deletable_row(std::move(cr)));
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone&& rt) {
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        return stop_iteration::no;
    }
    mutation_opt consume_end_of_stream() {
        return std::move(_mutation);
    }
};

mutation_partition::mutation_partition(mutation_partition::incomplete_tag, const schema& s, tombstone t)
    : _tombstone(t)
    , _static_row_continuous(!s.has_static_columns())
    , _rows()
    , _row_tombstones(s)
{
    _rows.insert_before(_rows.end(),
        *current_allocator().construct<rows_entry>(s, position_in_partition_view::after_all_clustered_rows(), is_dummy::yes, is_continuous::no));
}

bool mutation_partition::is_fully_continuous() const {
    if (!_static_row_continuous) {
        return false;
    }
    for (auto&& row : _rows) {
        if (!row.continuous()) {
            return false;
        }
    }
    return true;
}

void mutation_partition::make_fully_continuous() {
    _static_row_continuous = true;
    auto i = _rows.begin();
    while (i != _rows.end()) {
        if (i->dummy()) {
            i = _rows.erase_and_dispose(i, alloc_strategy_deleter<rows_entry>());
        } else {
            i->set_continuous(true);
            ++i;
        }
    }
}

void mutation_partition::evict() noexcept {
    if (!_rows.empty()) {
        // We need to keep the last entry to mark the range containing all evicted rows as discontinuous.
        // No rows would mean it is continuous.
        auto i = _rows.erase_and_dispose(_rows.begin(), std::prev(_rows.end()), current_deleter<rows_entry>());
        rows_entry& e = *i;
        e._flags._before_ck = false;
        e._flags._after_ck = true;
        e._flags._dummy = true;
        e._flags._continuous = false;
        e._row = {};
    }
    _row_tombstones.clear();
    _static_row_continuous = false;
    _static_row = {};
}

bool
mutation_partition::check_continuity(const schema& s, const position_range& r, is_continuous cont) {
    auto less = rows_entry::compare(s);
    auto i = _rows.lower_bound(r.start(), less);
    auto end = _rows.lower_bound(r.end(), less);
    if (!less(r.start(), r.end())) {
        return bool(cont);
    }
    if (i != end) {
        if (no_clustering_row_between(s, r.start(), i->position())) {
            ++i;
        }
        while (i != end) {
            if (i->continuous() != cont) {
                return false;
            }
            ++i;
        }
        if (end != _rows.begin() && no_clustering_row_between(s, std::prev(end)->position(), r.end())) {
            return true;
        }
    }
    return (end == _rows.end() ? is_continuous::yes : end->continuous()) == cont;
}

bool
mutation_partition::fully_continuous(const schema& s, const position_range& r) {
    return check_continuity(s, r, is_continuous::yes);
}

bool
mutation_partition::fully_discontinuous(const schema& s, const position_range& r) {
    return check_continuity(s, r, is_continuous::no);
}

future<mutation_opt> counter_write_query(schema_ptr s, const mutation_source& source,
                                         const dht::decorated_key& dk,
                                         const query::partition_slice& slice,
                                         tracing::trace_state_ptr trace_ptr)
{
    struct range_and_reader {
        dht::partition_range range;
        flat_mutation_reader reader;

        range_and_reader(range_and_reader&&) = delete;
        range_and_reader(const range_and_reader&) = delete;

        range_and_reader(schema_ptr s, const mutation_source& source,
                         const dht::decorated_key& dk,
                         const query::partition_slice& slice,
                         tracing::trace_state_ptr trace_ptr)
            : range(dht::partition_range::make_singular(dk))
            , reader(source.make_flat_mutation_reader(s, range, slice, service::get_local_sstable_query_read_priority(),
                                                      std::move(trace_ptr), streamed_mutation::forwarding::no,
                                                      mutation_reader::forwarding::no))
        { }
    };

    // do_with() doesn't support immovable objects
    auto r_a_r = std::make_unique<range_and_reader>(s, source, dk, slice, std::move(trace_ptr));
    auto cwqrb = counter_write_query_result_builder(*s);
    auto cfq = make_stable_flattened_mutations_consumer<compact_for_query<emit_only_live_rows::yes, counter_write_query_result_builder>>(
            *s, gc_clock::now(), slice, query::max_rows, query::max_rows, std::move(cwqrb));
    auto f = r_a_r->reader.consume(std::move(cfq), flat_mutation_reader::consume_reversed_partitions::no);
    return f.finally([r_a_r = std::move(r_a_r)] { });
}
