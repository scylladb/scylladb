/*
 * Copyright (C) 2017 ScyllaDB
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

#include "partition_version.hh"
#include "flat_mutation_reader.hh"

struct partition_snapshot_reader_dummy_accounter {
   void operator()(const clustering_row& cr) {}
   void operator()(const static_row& sr) {}
   void operator()(const range_tombstone& rt) {}
   void operator()(const partition_start& ph) {}
   void operator()(const partition_end& eop) {}
};
extern partition_snapshot_reader_dummy_accounter no_accounter;

inline void maybe_merge_versions(lw_shared_ptr<partition_snapshot>& snp,
                                 logalloc::region& lsa_region,
                                 logalloc::allocating_section& read_section) {
    if (!snp.owned()) {
        return;
    }
    // If no one else is using this particular snapshot try to merge partition
    // versions.
    with_allocator(lsa_region.allocator(), [&snp, &lsa_region, &read_section] {
        return with_linearized_managed_bytes([&snp, &lsa_region, &read_section] {
            try {
                // Allocating sections require the region to be reclaimable
                // which means that they cannot be nested.
                // It is, however, possible, that if the snapshot is taken
                // inside an allocating section and then an exception is thrown
                // this function will be called to clean up even though we
                // still will be in the context of the allocating section.
                if (lsa_region.reclaiming_enabled()) {
                    read_section(lsa_region, [&snp] {
                        snp->merge_partition_versions();
                    });
                }
            } catch (...) { }
            snp = {};
        });
    });
}

template <typename MemoryAccounter = partition_snapshot_reader_dummy_accounter>
class partition_snapshot_reader : public streamed_mutation::impl, public MemoryAccounter {
    struct rows_position {
        mutation_partition::rows_type::const_iterator _position;
        mutation_partition::rows_type::const_iterator _end;
    };

    class heap_compare {
        rows_entry::compare _cmp;
    public:
        explicit heap_compare(const schema& s) : _cmp(s) { }
        bool operator()(const rows_position& a, const rows_position& b) {
            return _cmp(*b._position, *a._position);
        }
    };
private:
    // Keeps shared pointer to the container we read mutation from to make sure
    // that its lifetime is appropriately extended.
    boost::any _container_guard;

    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _current_ck_range;
    query::clustering_row_ranges::const_iterator _ck_range_end;
    bool _in_ck_range = false;

    rows_entry::compare _cmp;
    position_in_partition::equal_compare _eq;
    heap_compare _heap_cmp;

    lw_shared_ptr<partition_snapshot> _snapshot;
    stdx::optional<position_in_partition> _last_entry;

    std::vector<rows_position> _clustering_rows;

    range_tombstone_stream _range_tombstones;

    logalloc::region& _lsa_region;
    logalloc::allocating_section& _read_section;

    MemoryAccounter& mem_accounter() {
        return *this;
    }

    partition_snapshot::change_mark _change_mark;
private:
    void refresh_iterators() {
        _clustering_rows.clear();

        if (!_in_ck_range) {
            if (_current_ck_range == _ck_range_end) {
                _end_of_stream = true;
                return;
            }
            for (auto&& v : _snapshot->versions()) {
                _range_tombstones.apply(v.partition().row_tombstones(), *_current_ck_range);
            }
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
        boost::range::make_heap(_clustering_rows, _heap_cmp);
    }

    // Valid if has_more_rows()
    const rows_entry& pop_clustering_row() {
        boost::range::pop_heap(_clustering_rows, _heap_cmp);
        auto& current = _clustering_rows.back();
        const rows_entry& e = *current._position;
        current._position = std::next(current._position);
        if (current._position == current._end) {
            _clustering_rows.pop_back();
        } else {
            boost::range::push_heap(_clustering_rows, _heap_cmp);
        }
        return e;
    }

    // Valid if has_more_rows()
    const rows_entry& peek_row() const {
        return *_clustering_rows.front()._position;
    }

    bool has_more_rows() const {
        return !_clustering_rows.empty();
    }

    mutation_fragment_opt read_static_row() {
        _last_entry = position_in_partition(position_in_partition::static_row_tag_t());
        mutation_fragment_opt sr;
        for (auto&& v : _snapshot->versions()) {
            if (!v.partition().static_row().empty()) {
                if (!sr) {
                    sr = mutation_fragment(static_row(v.partition().static_row()));
                } else {
                    sr->as_mutable_static_row().apply(*_schema, v.partition().static_row());
                }
            }
        }
        return sr;
    }

    mutation_fragment_opt read_next() {
        while (has_more_rows()) {
            auto mf = _range_tombstones.get_next(peek_row());
            if (mf) {
                return mf;
            }
            const rows_entry& e = pop_clustering_row();
            if (e.dummy()) {
                continue;
            }
            clustering_row result = e;
            while (has_more_rows() && _eq(peek_row().position(), result.position())) {
                result.apply(*_schema, pop_clustering_row());
            }
            _last_entry = position_in_partition(result.position());
            return mutation_fragment(std::move(result));
        }
        return _range_tombstones.get_next();
    }

    void emplace_mutation_fragment(mutation_fragment&& mfopt) {
        mfopt.visit(mem_accounter());
        push_mutation_fragment(std::move(mfopt));
    }

    void do_fill_buffer() {
        if (!_last_entry) {
            auto mfopt = read_static_row();
            if (mfopt) {
                emplace_mutation_fragment(std::move(*mfopt));
            }
        }

        auto mark = _snapshot->get_change_mark();
        if (!_in_ck_range || mark != _change_mark) {
            refresh_iterators();
            _change_mark = mark;
        }

        while (!is_end_of_stream() && !is_buffer_full()) {
            auto mfopt = read_next();
            if (mfopt) {
                emplace_mutation_fragment(std::move(*mfopt));
            } else {
                _in_ck_range = false;
                _current_ck_range = std::next(_current_ck_range);
                refresh_iterators();
            }
        }
    }

    static tombstone tomb(partition_snapshot& snp) {
        tombstone t;
        for (auto& v : snp.versions()) {
            t.apply(v.partition().partition_tombstone());
        }
        return t;
    }
public:
    template <typename... Args>
    partition_snapshot_reader(schema_ptr s, dht::decorated_key dk, lw_shared_ptr<partition_snapshot> snp,
        query::clustering_key_filter_ranges crr,
        logalloc::region& region, logalloc::allocating_section& read_section,
        boost::any pointer_to_container, Args&&... args)
    : streamed_mutation::impl(s, std::move(dk), tomb(*snp))
    , MemoryAccounter(std::forward<Args>(args)...)
    , _container_guard(std::move(pointer_to_container))
    , _ck_ranges(std::move(crr))
    , _current_ck_range(_ck_ranges.begin())
    , _ck_range_end(_ck_ranges.end())
    , _cmp(*s)
    , _eq(*s)
    , _heap_cmp(*s)
    , _snapshot(snp)
    , _range_tombstones(*s)
    , _lsa_region(region)
    , _read_section(read_section) {
        do_fill_buffer();
    }

    ~partition_snapshot_reader() {
        maybe_merge_versions(_snapshot, _lsa_region, _read_section);
    }

    virtual future<> fill_buffer() override {
        return _read_section(_lsa_region, [&] {
            return with_linearized_managed_bytes([&] {
                do_fill_buffer();
                return make_ready_future<>();
            });
        });
    }
};

template <typename MemoryAccounter, typename... Args>
inline streamed_mutation
make_partition_snapshot_reader(schema_ptr s,
    dht::decorated_key dk,
    query::clustering_key_filter_ranges crr,
    lw_shared_ptr<partition_snapshot> snp,
    logalloc::region& region,
    logalloc::allocating_section& read_section,
    boost::any pointer_to_container,
    streamed_mutation::forwarding fwd,
    Args&&... args)
{
    auto sm = make_streamed_mutation<partition_snapshot_reader<MemoryAccounter>>(s, std::move(dk),
           snp, std::move(crr), region, read_section, std::move(pointer_to_container), std::forward<Args>(args)...);
    if (fwd) {
        return make_forwardable(std::move(sm)); // FIXME: optimize
    } else {
        return std::move(sm);
    }
}

inline streamed_mutation
make_partition_snapshot_reader(schema_ptr s,
    dht::decorated_key dk,
    query::clustering_key_filter_ranges crr,
    lw_shared_ptr<partition_snapshot> snp,
    logalloc::region& region,
    logalloc::allocating_section& read_section,
    boost::any pointer_to_container,
    streamed_mutation::forwarding fwd)
{
    return make_partition_snapshot_reader<partition_snapshot_reader_dummy_accounter>(std::move(s),
        std::move(dk), std::move(crr), std::move(snp), region, read_section, std::move(pointer_to_container), fwd);
}

template <typename MemoryAccounter = partition_snapshot_reader_dummy_accounter>
class partition_snapshot_flat_reader : public flat_mutation_reader::impl, public MemoryAccounter {
    struct rows_position {
        mutation_partition::rows_type::const_iterator _position;
        mutation_partition::rows_type::const_iterator _end;
    };

    class heap_compare {
        rows_entry::compare _cmp;
    public:
        explicit heap_compare(const schema& s) : _cmp(s) { }
        bool operator()(const rows_position& a, const rows_position& b) {
            return _cmp(*b._position, *a._position);
        }
    };
private:
    // Keeps shared pointer to the container we read mutation from to make sure
    // that its lifetime is appropriately extended.
    boost::any _container_guard;

    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _current_ck_range;
    query::clustering_row_ranges::const_iterator _ck_range_end;
    bool _in_ck_range = false;

    rows_entry::compare _cmp;
    position_in_partition::equal_compare _eq;
    heap_compare _heap_cmp;

    lw_shared_ptr<partition_snapshot> _snapshot;
    stdx::optional<position_in_partition> _last_entry;

    std::vector<rows_position> _clustering_rows;

    range_tombstone_stream _range_tombstones;

    logalloc::region& _lsa_region;
    logalloc::allocating_section& _read_section;

    MemoryAccounter& mem_accounter() {
        return *this;
    }

    partition_snapshot::change_mark _change_mark;
private:
    void refresh_iterators() {
        _clustering_rows.clear();

        if (!_in_ck_range) {
            if (_current_ck_range == _ck_range_end) {
                _end_of_stream = true;
                return;
            }
            for (auto&& v : _snapshot->versions()) {
                _range_tombstones.apply(v.partition().row_tombstones(), *_current_ck_range);
            }
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
        boost::range::make_heap(_clustering_rows, _heap_cmp);
    }

    // Valid if has_more_rows()
    const rows_entry& pop_clustering_row() {
        boost::range::pop_heap(_clustering_rows, _heap_cmp);
        auto& current = _clustering_rows.back();
        const rows_entry& e = *current._position;
        current._position = std::next(current._position);
        if (current._position == current._end) {
            _clustering_rows.pop_back();
        } else {
            boost::range::push_heap(_clustering_rows, _heap_cmp);
        }
        return e;
    }

    // Valid if has_more_rows()
    const rows_entry& peek_row() const {
        return *_clustering_rows.front()._position;
    }

    bool has_more_rows() const {
        return !_clustering_rows.empty();
    }

    mutation_fragment_opt read_static_row() {
        _last_entry = position_in_partition(position_in_partition::static_row_tag_t());
        mutation_fragment_opt sr;
        for (auto&& v : _snapshot->versions()) {
            if (!v.partition().static_row().empty()) {
                if (!sr) {
                    sr = mutation_fragment(static_row(v.partition().static_row()));
                } else {
                    sr->as_mutable_static_row().apply(*_schema, v.partition().static_row());
                }
            }
        }
        return sr;
    }

    mutation_fragment_opt read_next() {
        while (has_more_rows()) {
            auto mf = _range_tombstones.get_next(peek_row());
            if (mf) {
                return mf;
            }
            const rows_entry& e = pop_clustering_row();
            if (e.dummy()) {
                continue;
            }
            clustering_row result = e;
            while (has_more_rows() && _eq(peek_row().position(), result.position())) {
                result.apply(*_schema, pop_clustering_row());
            }
            _last_entry = position_in_partition(result.position());
            return mutation_fragment(std::move(result));
        }
        return _range_tombstones.get_next();
    }

    void emplace_mutation_fragment(mutation_fragment&& mfopt) {
        mfopt.visit(mem_accounter());
        push_mutation_fragment(std::move(mfopt));
    }

    void do_fill_buffer() {
        if (!_last_entry) {
            auto mfopt = read_static_row();
            if (mfopt) {
                emplace_mutation_fragment(std::move(*mfopt));
            }
        }

        auto mark = _snapshot->get_change_mark();
        if (!_in_ck_range || mark != _change_mark) {
            refresh_iterators();
            _change_mark = mark;
        }

        while (!is_end_of_stream() && !is_buffer_full()) {
            auto mfopt = read_next();
            if (mfopt) {
                emplace_mutation_fragment(std::move(*mfopt));
            } else {
                _in_ck_range = false;
                _current_ck_range = std::next(_current_ck_range);
                refresh_iterators();
            }
        }
        if (is_end_of_stream()) {
            push_mutation_fragment(partition_end());
        }
    }

    static tombstone tomb(partition_snapshot& snp) {
        tombstone t;
        for (auto& v : snp.versions()) {
            t.apply(v.partition().partition_tombstone());
        }
        return t;
    }
public:
    template <typename... Args>
    partition_snapshot_flat_reader(schema_ptr s, dht::decorated_key dk, lw_shared_ptr<partition_snapshot> snp,
                              query::clustering_key_filter_ranges crr,
                              logalloc::region& region, logalloc::allocating_section& read_section,
                              boost::any pointer_to_container, Args&&... args)
        : impl(std::move(s))
        , MemoryAccounter(std::forward<Args>(args)...)
        , _container_guard(std::move(pointer_to_container))
        , _ck_ranges(std::move(crr))
        , _current_ck_range(_ck_ranges.begin())
        , _ck_range_end(_ck_ranges.end())
        , _cmp(*_schema)
        , _eq(*_schema)
        , _heap_cmp(*_schema)
        , _snapshot(snp)
        , _range_tombstones(*_schema)
        , _lsa_region(region)
        , _read_section(read_section) {
        push_mutation_fragment(partition_start(std::move(dk), tomb(*snp)));
        do_fill_buffer();
    }

    ~partition_snapshot_flat_reader() {
        maybe_merge_versions(_snapshot, _lsa_region, _read_section);
    }

    virtual future<> fill_buffer() override {
        return _read_section(_lsa_region, [&] {
            return with_linearized_managed_bytes([&] {
                do_fill_buffer();
                return make_ready_future<>();
            });
        });
    }
    virtual void next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = true;
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        throw std::runtime_error("This reader can't be fast forwarded to another partition.");
    };
    virtual future<> fast_forward_to(position_range cr) override {
        throw std::runtime_error("This reader can't be fast forwarded to another position.");
    };
};

template <typename MemoryAccounter, typename... Args>
inline flat_mutation_reader
make_partition_snapshot_flat_reader(schema_ptr s,
                                    dht::decorated_key dk,
                                    query::clustering_key_filter_ranges crr,
                                    lw_shared_ptr<partition_snapshot> snp,
                                    logalloc::region& region,
                                    logalloc::allocating_section& read_section,
                                    boost::any pointer_to_container,
                                    streamed_mutation::forwarding fwd,
                                    Args&&... args)
{
    auto res = make_flat_mutation_reader<partition_snapshot_flat_reader<MemoryAccounter>>(std::move(s), std::move(dk),
            snp, std::move(crr), region, read_section, std::move(pointer_to_container), std::forward<Args>(args)...);
    if (fwd) {
        return make_forwardable(std::move(res)); // FIXME: optimize
    } else {
        return std::move(res);
    }
}

inline flat_mutation_reader
make_partition_snapshot_flat_reader(schema_ptr s,
                                    dht::decorated_key dk,
                                    query::clustering_key_filter_ranges crr,
                                    lw_shared_ptr<partition_snapshot> snp,
                                    logalloc::region& region,
                                    logalloc::allocating_section& read_section,
                                    boost::any pointer_to_container,
                                    streamed_mutation::forwarding fwd)
{
    return make_partition_snapshot_flat_reader<partition_snapshot_reader_dummy_accounter>(std::move(s),
            std::move(dk), std::move(crr), std::move(snp), region, read_section, std::move(pointer_to_container), fwd);
}
