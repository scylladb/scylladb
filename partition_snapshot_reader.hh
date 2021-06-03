/*
 * Copyright (C) 2017-present ScyllaDB
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
#include <boost/range/algorithm/heap_algorithm.hpp>

struct partition_snapshot_reader_dummy_accounter {
   void operator()(const clustering_row& cr) {}
   void operator()(const static_row& sr) {}
   void operator()(const range_tombstone& rt) {}
   void operator()(const partition_start& ph) {}
   void operator()(const partition_end& eop) {}
};
extern partition_snapshot_reader_dummy_accounter no_accounter;

template <typename MemoryAccounter = partition_snapshot_reader_dummy_accounter>
class partition_snapshot_flat_reader : public flat_mutation_reader::impl, public MemoryAccounter {
    struct rows_position {
        mutation_partition::rows_type::const_iterator _position;
        mutation_partition::rows_type::const_iterator _end;
    };

    class heap_compare {
        position_in_partition::less_compare _less;
    public:
        explicit heap_compare(const schema& s) : _less(s) { }
        bool operator()(const rows_position& a, const rows_position& b) {
            return _less(b._position->position(), a._position->position());
        }
        bool operator()(const range_tombstone_list::iterator_range& a, const range_tombstone_list::iterator_range& b) {
            return _less(b.front().position(), a.front().position());
        }
    };

    // The part of the reader that accesses LSA memory directly and works
    // with reclamation disabled. The state is either immutable (comparators,
    // snapshot, references to region and alloc section) or dropped on any
    // allocation section retry (_clustering_rows).
    class lsa_partition_reader {
        const schema& _schema;
        reader_permit _permit;
        heap_compare _heap_cmp;

        partition_snapshot_ptr _snapshot;

        logalloc::region& _region;
        logalloc::allocating_section& _read_section;

        partition_snapshot::change_mark _change_mark;
        std::vector<rows_position> _clustering_rows;
        std::vector<range_tombstone_list::iterator_range> _range_tombstones;

        range_tombstone_stream _rt_stream;

        bool _digest_requested;
    private:
        template<typename Function>
        decltype(auto) in_alloc_section(Function&& fn) {
            return _read_section.with_reclaiming_disabled(_region, [&] {
                return fn();
            });
        }
        void maybe_refresh_state(const query::clustering_range& ck_range,
                           const std::optional<position_in_partition>& last_row,
                           const std::optional<position_in_partition>& last_rts) {
            auto mark = _snapshot->get_change_mark();
            if (mark != _change_mark) {
                do_refresh_state(ck_range, last_row, last_rts);
                _change_mark = mark;
            }
        }

        void do_refresh_state(const query::clustering_range& ck_range,
                           const std::optional<position_in_partition>& last_row,
                           const std::optional<position_in_partition>& last_rts) {
            _clustering_rows.clear();
            _range_tombstones.clear();

            rows_entry::tri_compare rows_cmp(_schema);
            for (auto&& v : _snapshot->versions()) {
                mutation_partition::rows_type::const_iterator cr_end = v.partition().upper_bound(_schema, ck_range);
                auto cr = [&] () -> mutation_partition::rows_type::const_iterator {
                    if (last_row) {
                        return v.partition().clustered_rows().upper_bound(*last_row, rows_cmp);
                    } else {
                        return v.partition().lower_bound(_schema, ck_range);
                    }
                }();

                if (cr != cr_end) {
                    _clustering_rows.emplace_back(rows_position { cr, cr_end });
                }

                range_tombstone_list::iterator_range rt_slice = [&] () {
                    if (last_rts) {
                        return v.partition().row_tombstones().upper_slice(_schema, *last_rts, bound_view::from_range_end(ck_range));
                    } else {
                        return v.partition().row_tombstones().slice(_schema, ck_range);
                    }
                }();
                if (rt_slice.begin() != rt_slice.end()) {
                    _range_tombstones.emplace_back(std::move(rt_slice));
                }
            }

            boost::range::make_heap(_clustering_rows, _heap_cmp);
            boost::range::make_heap(_range_tombstones, _heap_cmp);
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

        const range_tombstone& pop_range_tombstone() {
            boost::range::pop_heap(_range_tombstones, _heap_cmp);
            auto& current = _range_tombstones.back();
            const range_tombstone& rt = *current.begin();
            current.advance_begin(1);
            if (current.begin() == current.end()) {
                _range_tombstones.pop_back();
            } else {
                boost::range::push_heap(_range_tombstones, _heap_cmp);
            }
            return rt;
        }

        // Valid if has_more_rows()
        const rows_entry& peek_row() const {
            return *_clustering_rows.front()._position;
        }
        bool has_more_rows() const {
            return !_clustering_rows.empty();
        }

        const range_tombstone& peek_range_tombstone() const {
            return *_range_tombstones.front().begin();
        }
        bool has_more_range_tombstones() const {
            return !_range_tombstones.empty();
        }
    public:
        explicit lsa_partition_reader(const schema& s, reader_permit permit, partition_snapshot_ptr snp,
                                      logalloc::region& region, logalloc::allocating_section& read_section,
                                      bool digest_requested)
            : _schema(s)
            , _permit(permit)
            , _heap_cmp(s)
            , _snapshot(std::move(snp))
            , _region(region)
            , _read_section(read_section)
            , _rt_stream(s, permit)
            , _digest_requested(digest_requested)
        { }

        void reset_state(const query::clustering_range& ck_range) {
            do_refresh_state(ck_range, {}, {});
        }

        template<typename Function>
        decltype(auto) with_reserve(Function&& fn) {
            return _read_section.with_reserve(std::forward<Function>(fn));
        }

        tombstone partition_tombstone() {
            logalloc::reclaim_lock guard(_region);
            return _snapshot->partition_tombstone();
        }

        static_row get_static_row() {
            return in_alloc_section([&] {
                return _snapshot->static_row(_digest_requested);
            });
        }

        // Returns next clustered row in the range.
        // If the ck_range is the same as the one used previously last_row needs
        // to be engaged and equal the position of the row returned last time.
        // If the ck_range is different or this is the first call to this
        // function last_row has to be disengaged. Additionally, when entering
        // new range _rt_stream will be populated with all relevant
        // tombstones.
        mutation_fragment_opt next_row(const query::clustering_range& ck_range,
                                       const std::optional<position_in_partition>& last_row,
                                       const std::optional<position_in_partition>& last_rts) {
            return in_alloc_section([&] () -> mutation_fragment_opt {
                maybe_refresh_state(ck_range, last_row, last_rts);

                position_in_partition::equal_compare rows_eq(_schema);
                while (has_more_rows()) {
                    const rows_entry& e = pop_clustering_row();
                    if (e.dummy()) {
                        continue;
                    }
                    if (_digest_requested) {
                        e.row().cells().prepare_hash(_schema, column_kind::regular_column);
                    }
                    auto result = mutation_fragment(mutation_fragment::clustering_row_tag_t(), _schema, _permit, _schema, e);
                    while (has_more_rows() && rows_eq(peek_row().position(), result.as_clustering_row().position())) {
                        const rows_entry& e = pop_clustering_row();
                        if (_digest_requested) {
                            e.row().cells().prepare_hash(_schema, column_kind::regular_column);
                        }
                        result.mutate_as_clustering_row(_schema, [&] (clustering_row& cr) mutable {
                            cr.apply(_schema, e);
                        });
                    }
                    return result;
                }
                return { };
            });
        }

        mutation_fragment_opt next_range_rombstone(const query::clustering_range& ck_range,
                const std::optional<position_in_partition>& last_row,
                const std::optional<position_in_partition>& last_rts,
                position_in_partition_view pos) {
            return in_alloc_section([&] () -> mutation_fragment_opt {
                maybe_refresh_state(ck_range, last_row, last_rts);

                position_in_partition::less_compare rt_less(_schema);
                while (has_more_range_tombstones() && !rt_less(pos, peek_range_tombstone().position())) {
                    range_tombstone rt = pop_range_tombstone();
                    rt.trim_front(_schema, position_in_partition_view::for_range_start(ck_range));
                    _rt_stream.apply(std::move(rt));
                }

                return _rt_stream.get_next(std::move(pos));
            });
        }
    };
private:
    // Keeps shared pointer to the container we read mutation from to make sure
    // that its lifetime is appropriately extended.
    boost::any _container_guard;

    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _current_ck_range;
    query::clustering_row_ranges::const_iterator _ck_range_end;

    std::optional<position_in_partition> _last_entry;
    std::optional<position_in_partition> _last_rts;
    mutation_fragment_opt _next_row;

    lsa_partition_reader _reader;
    bool _static_row_done = false;
    bool _no_more_rows_in_current_range = false;

    MemoryAccounter& mem_accounter() {
        return *this;
    }
private:
    void push_static_row() {
        auto sr = _reader.get_static_row();
        if (!sr.empty()) {
            emplace_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(sr)));
        }
    }

    mutation_fragment_opt read_next() {
        if (!_next_row && !_no_more_rows_in_current_range) {
            _next_row = _reader.next_row(*_current_ck_range, _last_entry, _last_rts);
        }
        if (_next_row) {
            auto pos_view = _next_row->as_clustering_row().position();
            _last_entry = position_in_partition(pos_view);

            auto mf = _reader.next_range_rombstone(*_current_ck_range, _last_entry, _last_rts,  pos_view);
            if (mf) {
                _last_rts = mf->as_range_tombstone().position();
                return mf;
            }
            return std::exchange(_next_row, {});
        } else {
            _no_more_rows_in_current_range = true;
            auto mf = _reader.next_range_rombstone(*_current_ck_range, _last_entry, _last_rts, position_in_partition_view::for_range_end(*_current_ck_range));
            if (mf) {
                _last_rts = mf->as_range_tombstone().position();
            }
            return mf;
        }
    }

    void emplace_mutation_fragment(mutation_fragment&& mfopt) {
        mfopt.visit(mem_accounter());
        push_mutation_fragment(std::move(mfopt));
    }

    void on_new_range() {
        if (_current_ck_range == _ck_range_end) {
            _end_of_stream = true;
            push_mutation_fragment(mutation_fragment(*_schema, _permit, partition_end()));
        } else {
            _reader.reset_state(*_current_ck_range);
        }
        _no_more_rows_in_current_range = false;
    }

    void do_fill_buffer(db::timeout_clock::time_point timeout) {
        while (!is_end_of_stream() && !is_buffer_full()) {
            auto mfopt = read_next();
            if (mfopt) {
                emplace_mutation_fragment(std::move(*mfopt));
            } else {
                _last_entry = std::nullopt;
                _last_rts = std::nullopt;
                _current_ck_range = std::next(_current_ck_range);
                on_new_range();
            }
            if (need_preempt()) {
                break;
            }
        }
    }
public:
    template <typename... Args>
    partition_snapshot_flat_reader(schema_ptr s, reader_permit permit, dht::decorated_key dk, partition_snapshot_ptr snp,
                              query::clustering_key_filter_ranges crr, bool digest_requested,
                              logalloc::region& region, logalloc::allocating_section& read_section,
                              boost::any pointer_to_container, Args&&... args)
        : impl(std::move(s), std::move(permit))
        , MemoryAccounter(std::forward<Args>(args)...)
        , _container_guard(std::move(pointer_to_container))
        , _ck_ranges(std::move(crr))
        , _current_ck_range(_ck_ranges.begin())
        , _ck_range_end(_ck_ranges.end())
        , _reader(*_schema, _permit, std::move(snp), region, read_section, digest_requested)
    {
        _reader.with_reserve([&] {
            push_mutation_fragment(*_schema, _permit, partition_start(std::move(dk), _reader.partition_tombstone()));
        });
    }

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
      // FIXME: indentation
      return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this, timeout] {
        _reader.with_reserve([&] {
            if (!_static_row_done) {
                push_static_row();
                on_new_range();
                _static_row_done = true;
            }
            do_fill_buffer(timeout);
        });
        return make_ready_future<>();
      });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = true;
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        throw std::runtime_error("This reader can't be fast forwarded to another partition.");
    };
    virtual future<> fast_forward_to(position_range cr, db::timeout_clock::time_point timeout) override {
        throw std::runtime_error("This reader can't be fast forwarded to another position.");
    };
    virtual future<> close() noexcept override {
        return make_ready_future<>();
    }
};

template <typename MemoryAccounter, typename... Args>
inline flat_mutation_reader
make_partition_snapshot_flat_reader(schema_ptr s,
                                    reader_permit permit,
                                    dht::decorated_key dk,
                                    query::clustering_key_filter_ranges crr,
                                    partition_snapshot_ptr snp,
                                    bool digest_requested,
                                    logalloc::region& region,
                                    logalloc::allocating_section& read_section,
                                    boost::any pointer_to_container,
                                    streamed_mutation::forwarding fwd,
                                    Args&&... args)
{
    auto res = make_flat_mutation_reader<partition_snapshot_flat_reader<MemoryAccounter>>(std::move(s), std::move(permit), std::move(dk),
            snp, std::move(crr), digest_requested, region, read_section, std::move(pointer_to_container), std::forward<Args>(args)...);
    if (fwd) {
        return make_forwardable(std::move(res)); // FIXME: optimize
    } else {
        return res;
    }
}

inline flat_mutation_reader
make_partition_snapshot_flat_reader(schema_ptr s,
                                    reader_permit permit,
                                    dht::decorated_key dk,
                                    query::clustering_key_filter_ranges crr,
                                    partition_snapshot_ptr snp,
                                    bool digest_requested,
                                    logalloc::region& region,
                                    logalloc::allocating_section& read_section,
                                    boost::any pointer_to_container,
                                    streamed_mutation::forwarding fwd)
{
    return make_partition_snapshot_flat_reader<partition_snapshot_reader_dummy_accounter>(std::move(s), std::move(permit),
            std::move(dk), std::move(crr), std::move(snp), digest_requested, region, read_section, std::move(pointer_to_container), fwd);
}
