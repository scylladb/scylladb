/*
 * Copyright (C) 2016 ScyllaDB
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

#include <boost/range/algorithm/heap_algorithm.hpp>

#include "partition_version.hh"

static void remove_or_mark_as_unique_owner(partition_version* current)
{
    while (current && !current->is_referenced()) {
        auto next = current->next();
        current_allocator().destroy(current);
        current = next;
    }
    if (current) {
        current->back_reference().mark_as_unique_owner();
    }
}

partition_version::partition_version(partition_version&& pv) noexcept
    : anchorless_list_base_hook(std::move(pv))
    , _backref(pv._backref)
    , _partition(std::move(pv._partition))
{
    if (_backref) {
        _backref->_version = this;
    }
    pv._backref = nullptr;
}

partition_version& partition_version::operator=(partition_version&& pv) noexcept
{
    if (this != &pv) {
        this->~partition_version();
        new (this) partition_version(std::move(pv));
    }
    return *this;
}

partition_version::~partition_version()
{
    if (_backref) {
        _backref->_version = nullptr;
    }
}

partition_snapshot::~partition_snapshot() {
    if (_version && _version.is_unique_owner()) {
        auto v = &*_version;
        _version = {};
        remove_or_mark_as_unique_owner(v);
    } else if (_entry) {
        _entry->_snapshot = nullptr;
    }
}

void partition_snapshot::merge_partition_versions() {
    if (_version && !_version.is_unique_owner()) {
        auto v = &*_version;
        _version = { };
        auto first_used = v;
        while (first_used->prev() && !first_used->is_referenced()) {
            first_used = first_used->prev();
        }

        auto current = first_used->next();
        while (current && !current->is_referenced()) {
            auto next = current->next();
            try {
                first_used->partition().apply(*_schema, std::move(current->partition()));
                current_allocator().destroy(current);
            } catch (...) {
                // Set _version so that the merge can be retried.
                _version = partition_version_ref(*current);
                throw;
            }
            current = next;
        }
    }
}

unsigned partition_snapshot::version_count()
{
    unsigned count = 0;
    for (auto&& v : versions()) {
        (void)v;
        count++;
    }
    return count;
}

partition_entry::partition_entry(mutation_partition mp)
{
    auto new_version = current_allocator().construct<partition_version>(std::move(mp));
    _version = partition_version_ref(*new_version);
}

partition_entry::~partition_entry() {
    if (!_version) {
        return;
    }
    if (_snapshot) {
        _snapshot->_version = std::move(_version);
        _snapshot->_version.mark_as_unique_owner();
        _snapshot->_entry = nullptr;
    } else {
        auto v = &*_version;
        _version = { };
        remove_or_mark_as_unique_owner(v);
    }
}

void partition_entry::set_version(partition_version* new_version)
{
    if (_snapshot) {
        _snapshot->_version = std::move(_version);
        _snapshot->_entry = nullptr;
    }

    _snapshot = nullptr;
    _version = partition_version_ref(*new_version);
}

void partition_entry::apply(const schema& s, partition_version* pv, const schema& pv_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, std::move(pv->partition()), pv_schema);
        current_allocator().destroy(pv);
    } else {
        if (s.version() != pv_schema.version()) {
            pv->partition().upgrade(pv_schema, s);
        }
        pv->insert_before(*_version);
        set_version(pv);
    }
}

void partition_entry::apply(const schema& s, const mutation_partition& mp, const schema& mp_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, mp, mp_schema);
    } else {
        mutation_partition mp1 = mp;
        if (s.version() != mp_schema.version()) {
            mp1.upgrade(mp_schema, s);
        }
        auto new_version = current_allocator().construct<partition_version>(std::move(mp1));
        new_version->insert_before(*_version);

        set_version(new_version);
    }
}

void partition_entry::apply(const schema& s, mutation_partition&& mp, const schema& mp_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, std::move(mp), mp_schema);
    } else {
        if (s.version() != mp_schema.version()) {
            apply(s, mp, mp_schema);
        } else {
            auto new_version = current_allocator().construct<partition_version>(std::move(mp));
            new_version->insert_before(*_version);

            set_version(new_version);
        }
    }
}

void partition_entry::apply(const schema& s, mutation_partition_view mpv, const schema& mp_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, mpv, mp_schema);
    } else {
        mutation_partition mp(s.shared_from_this());
        mp.apply(s, mpv, mp_schema);
        auto new_version = current_allocator().construct<partition_version>(std::move(mp));
        new_version->insert_before(*_version);

        set_version(new_version);
    }
}

void partition_entry::apply(const schema& s, partition_entry&& pe, const schema& mp_schema)
{
    auto begin = &*pe._version;
    auto snapshot = pe._snapshot;
    if (pe._snapshot) {
        pe._snapshot->_version = std::move(pe._version);
        pe._snapshot->_entry = nullptr;
        pe._snapshot = nullptr;
    }
    pe._version = { };

    auto current = begin;
    if (!current->next() && !current->is_referenced()) {
        try {
            apply(s, current, mp_schema);
        } catch (...) {
            pe._version = partition_version_ref(*current);
            throw;
        }
        return;
    }

    try {
        while (current && !current->is_referenced()) {
            auto next = current->next();
            apply(s, std::move(current->partition()), mp_schema);
            // Leave current->partition() valid (albeit empty) in case we throw later.
            current->partition() = mutation_partition(mp_schema.shared_from_this());
            current = next;
        }
        while (current) {
            auto next = current->next();
            apply(s, current->partition(), mp_schema);
            current = next;
        }
    } catch (...) {
        if (snapshot) {
            pe._snapshot = snapshot;
            snapshot->_entry = &pe;
            pe._version = std::move(snapshot->_version);
        } else {
            pe._version = partition_version_ref(*begin);
        }
        throw;
    }

    current = begin;
    while (current && !current->is_referenced()) {
        auto next = current->next();
        current_allocator().destroy(current);
        current = next;
    }
    if (current) {
        current->back_reference().mark_as_unique_owner();
    }
}

mutation_partition partition_entry::squashed(schema_ptr from, schema_ptr to)
{
    mutation_partition mp(to);
    for (auto&& v : _version->all_elements()) {
        mp.apply(*to, v.partition(), *from);
    }
    return mp;
}

void partition_entry::upgrade(schema_ptr from, schema_ptr to)
{
    auto new_version = current_allocator().construct<partition_version>(mutation_partition(to));
    try {
        for (auto&& v : _version->all_elements()) {
            new_version->partition().apply(*to, v.partition(), *from);
        }
    } catch (...) {
        current_allocator().destroy(new_version);
        throw;
    }

    auto old_version = &*_version;
    set_version(new_version);
    remove_or_mark_as_unique_owner(old_version);
}

lw_shared_ptr<partition_snapshot> partition_entry::read(schema_ptr entry_schema)
{
    if (_snapshot) {
        return _snapshot->shared_from_this();
    } else {
        auto snp = make_lw_shared<partition_snapshot>(entry_schema, this);
        _snapshot = snp.get();
        return snp;
    }
}

partition_snapshot_reader::partition_snapshot_reader(schema_ptr s, dht::decorated_key dk,
    lw_shared_ptr<partition_snapshot> snp, query::clustering_key_filtering_context fc,
    const query::clustering_row_ranges& crr, logalloc::region& region,
    logalloc::allocating_section& read_section, boost::any pointer_to_container)
    : streamed_mutation::impl(s, std::move(dk), tomb(*snp))
    , _container_guard(std::move(pointer_to_container))
    , _filtering_context(fc)
    , _current_ck_range(crr.begin())
    , _ck_range_end(crr.end())
    , _cmp(*s)
    , _eq(*s)
    , _snapshot(snp)
    , _range_tombstones(*s)
    , _lsa_region(region)
    , _read_section(read_section)
{
    for (auto&& v : _snapshot->versions()) {
        _range_tombstones.apply(v.partition().row_tombstones());
    }
    do_fill_buffer();
}

partition_snapshot_reader::~partition_snapshot_reader()
{
    if (!_snapshot.owned()) {
        return;
    }
    // If no one else is using this particular snapshot try to merge partition
    // versions.
    with_allocator(_lsa_region.allocator(), [this] {
        return with_linearized_managed_bytes([this] {
            try {
                _read_section(_lsa_region, [this] {
                    _snapshot->merge_partition_versions();
                });
            } catch (...) { }
            _snapshot = {};
        });
    });
}

tombstone partition_snapshot_reader::tomb(partition_snapshot& snp)
{
    tombstone t;
    for (auto& v : snp.versions()) {
        t.apply(v.partition().partition_tombstone());
    }
    return t;
}

mutation_fragment_opt partition_snapshot_reader::read_static_row()
{
    _last_entry = position_in_partition(position_in_partition::static_row_tag_t());
    mutation_fragment_opt sr;
    for (auto&& v : _snapshot->versions()) {
        if (!v.partition().static_row().empty()) {
            if (!sr) {
                sr = mutation_fragment(static_row(v.partition().static_row()));
            } else {
                sr->as_static_row().apply(*_schema, v.partition().static_row());
            }
        }
    }
    return sr;
}

void partition_snapshot_reader::refresh_iterators()
{
    _clustering_rows.clear();

    if (!_in_ck_range && _current_ck_range == _ck_range_end) {
        return;
    }

    for (auto&& v : _snapshot->versions()) {
        auto cr_end = v.partition().upper_bound(*_schema, *_current_ck_range);
        auto cr = [&] () -> mutation_partition::rows_type::const_iterator {
            if (_in_ck_range) {
                return v.partition().clustered_rows().upper_bound(*_last_entry, _cmp);
            } else {
                return v.partition().lower_bound(*_schema, *_current_ck_range);
            }
        }();

        if (cr != cr_end) {
            _clustering_rows.emplace_back(rows_position { cr, cr_end });
        }
    }

    _in_ck_range = true;
    boost::range::make_heap(_clustering_rows, heap_compare(_cmp));
}

void partition_snapshot_reader::pop_clustering_row()
{
    auto& current = _clustering_rows.back();
    current._position = std::next(current._position);
    if (current._position == current._end) {
        _clustering_rows.pop_back();
    } else {
        boost::range::push_heap(_clustering_rows, heap_compare(_cmp));
    }
}

mutation_fragment_opt partition_snapshot_reader::read_next()
{
    if (!_clustering_rows.empty()) {
        auto mf = _range_tombstones.get_next(*_clustering_rows.front()._position);
        if (mf) {
            return mf;
        }

        boost::range::pop_heap(_clustering_rows, heap_compare(_cmp));
        clustering_row result = *_clustering_rows.back()._position;
        pop_clustering_row();
        while (!_clustering_rows.empty() && _eq(*_clustering_rows.front()._position, result)) {
            boost::range::pop_heap(_clustering_rows, heap_compare(_cmp));
            auto& current = _clustering_rows.back();
            result.apply(*_schema, *current._position);
            pop_clustering_row();
        }
        _last_entry = result.position();
        return mutation_fragment(std::move(result));
    }
    return _range_tombstones.get_next();
}

void partition_snapshot_reader::do_fill_buffer()
{
    if (!_last_entry) {
        auto mfopt = read_static_row();
        if (mfopt) {
            _buffer.emplace_back(std::move(*mfopt));
        }
    }

    if (!_in_ck_range || _lsa_region.reclaim_counter() != _reclaim_counter || _snapshot->version_count() != _version_count) {
        refresh_iterators();
        _reclaim_counter = _lsa_region.reclaim_counter();
        _version_count = _snapshot->version_count();
    }

    while (!is_end_of_stream() && !is_buffer_full()) {
        if (_in_ck_range && _clustering_rows.empty()) {
            _in_ck_range = false;
            _current_ck_range = std::next(_current_ck_range);
            refresh_iterators();
            continue;
        }

        auto mfopt = read_next();
        if (mfopt) {
            _buffer.emplace_back(std::move(*mfopt));
        } else {
            _end_of_stream = true;
        }
    }
}

future<> partition_snapshot_reader::fill_buffer()
{
    return _read_section(_lsa_region, [&] {
        return with_linearized_managed_bytes([&] {
            do_fill_buffer();
            return make_ready_future<>();
        });
    });
}

streamed_mutation make_partition_snapshot_reader(schema_ptr s, dht::decorated_key dk,
    query::clustering_key_filtering_context fc, const query::clustering_row_ranges& crr,
    lw_shared_ptr<partition_snapshot> snp, logalloc::region& region,
    logalloc::allocating_section& read_section, boost::any pointer_to_container)
{
    return make_streamed_mutation<partition_snapshot_reader>(s, std::move(dk), 
        snp, fc, crr, region, read_section, std::move(pointer_to_container));
}
