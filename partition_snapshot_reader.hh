/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "partition_version.hh"
#include "readers/flat_mutation_reader_fwd.hh"
#include "readers/flat_mutation_reader_v2.hh"
#include "clustering_key_filter.hh"
#include "query-request.hh"
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <any>

template <bool Reversing, typename Accounter>
class partition_snapshot_flat_reader : public flat_mutation_reader_v2::impl, public Accounter {
    using rows_iter_type = std::conditional_t<Reversing,
          mutation_partition::rows_type::const_reverse_iterator,
          mutation_partition::rows_type::const_iterator>;
    struct rows_position {
        rows_iter_type _position, _end;
    };

    static rows_iter_type make_iterator(mutation_partition::rows_type::const_iterator it) {
        if constexpr (Reversing) {
            return std::make_reverse_iterator(it);
        } else {
            return it;
        }
    }

    class heap_compare {
        position_in_partition::less_compare _less;
    public:
        // `s` shall be native to the query clustering order.
        explicit heap_compare(const schema& s) : _less(s) { }
        bool operator()(const rows_position& a, const rows_position& b) {
            if constexpr (Reversing) {
                // Here `reversed()` doesn't change anything, but keep it for consistency.
                return _less(b._position->position().reversed(), a._position->position().reversed());
            } else {
                return _less(b._position->position(), a._position->position());
            }
        }
        bool operator()(const range_tombstone_list::iterator_range& a, const range_tombstone_list::iterator_range& b) {
            if constexpr (Reversing) {
                return _less(b.back().end_position().reversed(), a.back().end_position().reversed());
            } else {
                return _less(b.front().position(), a.front().position());
            }
        }
    };

    // The part of the reader that accesses LSA memory directly and works
    // with reclamation disabled. The state is either immutable (comparators,
    // snapshot, references to region and alloc section) or dropped on any
    // allocation section retry (_clustering_rows).
    class lsa_partition_reader {
        // _query_schema can be used to retrieve the clustering key order which is used
        // for result ordering. This schema is passed from the query and is reversed iff
        // the query was reversed (i.e. `Reversing==true`).
        const schema& _query_schema;
        // _snapshot_schema is a schema that induces the same clustering key order as the
        // schema from the underlying snapshot. The schemas mentioned might differ, for
        // instance, if a query used newer version of the schema.
        const schema_ptr _snapshot_schema;
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
        void maybe_refresh_state(const query::clustering_range& ck_range_snapshot,
                           const std::optional<position_in_partition>& last_row,
                           const std::optional<position_in_partition>& last_rts) {
            if (_snapshot->get_change_mark() != _change_mark) {
                do_refresh_state(ck_range_snapshot, last_row, last_rts);
            }
        }

        // In reversing mode, upper and lower bounds still need to be executed against
        // snapshot schema and ck_range, however we need them to search from "opposite" direction.
        template<typename T, typename... Args>
        static rows_iter_type lower_bound(const T& t, Args&&... args) {
            if constexpr (Reversing) {
                return make_iterator(t.upper_bound(std::forward<Args>(args)...));
            } else {
                return make_iterator(t.lower_bound(std::forward<Args>(args)...));
            }
        }
        template<typename T, typename... Args>
        static rows_iter_type upper_bound(const T& t, Args&&... args) {
            if constexpr (Reversing) {
                return make_iterator(t.lower_bound(std::forward<Args>(args)...));
            } else {
                return make_iterator(t.upper_bound(std::forward<Args>(args)...));
            }
        }

        void do_refresh_state(const query::clustering_range& ck_range_snapshot,
                           const std::optional<position_in_partition>& last_row,
                           const std::optional<position_in_partition>& last_rts) {
            _clustering_rows.clear();
            _range_tombstones.clear();

            rows_entry::tri_compare rows_cmp(*_snapshot_schema);
            for (auto&& v : _snapshot->versions()) {
                auto cr = [&] () {
                    if (last_row) {
                        return upper_bound(v.partition().clustered_rows(), *last_row, rows_cmp);
                    } else {
                        return lower_bound(v.partition(), *_snapshot_schema, ck_range_snapshot);
                    }
                }();
                auto cr_end = upper_bound(v.partition(), *_snapshot_schema, ck_range_snapshot);

                if (cr != cr_end) {
                    _clustering_rows.emplace_back(rows_position { cr, cr_end });
                }

                range_tombstone_list::iterator_range rt_slice = [&] () {
                    const auto& tombstones = v.partition().row_tombstones();
                    if (last_rts) {
                        if constexpr (Reversing) {
                            return tombstones.lower_slice(*_snapshot_schema, bound_view::from_range_start(ck_range_snapshot), *last_rts);
                        } else {
                            return tombstones.upper_slice(*_snapshot_schema, *last_rts, bound_view::from_range_end(ck_range_snapshot));
                        }
                    } else {
                        return tombstones.slice(*_snapshot_schema, ck_range_snapshot);
                    }
                }();
                if (rt_slice.begin() != rt_slice.end()) {
                    _range_tombstones.emplace_back(std::move(rt_slice));
                }
            }

            boost::range::make_heap(_clustering_rows, _heap_cmp);
            boost::range::make_heap(_range_tombstones, _heap_cmp);
            _change_mark = _snapshot->get_change_mark();
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

        range_tombstone pop_range_tombstone() {
            boost::range::pop_heap(_range_tombstones, _heap_cmp);
            auto& current = _range_tombstones.back();
            range_tombstone rt = (Reversing ? std::prev(current.end()) : current.begin())->tombstone();
            if constexpr (Reversing) {
                current.advance_end(-1);
                rt.reverse();
            } else {
                current.advance_begin(1);
            }
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

        // Let's not lose performance when not Reversing.
        using peeked_range_tombstone = std::conditional_t<Reversing, range_tombstone, const range_tombstone&>;

        peeked_range_tombstone peek_range_tombstone() const {
            if constexpr (Reversing) {
                range_tombstone rt = std::prev(_range_tombstones.front().end())->tombstone();
                rt.reverse();
                return rt;
            } else {
                return _range_tombstones.front().begin()->tombstone();
            }
        }
        bool has_more_range_tombstones() const {
            return !_range_tombstones.empty();
        }
    public:
        explicit lsa_partition_reader(const schema& s, reader_permit permit, partition_snapshot_ptr snp,
                                      logalloc::region& region, logalloc::allocating_section& read_section,
                                      bool digest_requested)
            : _query_schema(s)
            , _snapshot_schema(Reversing ? s.make_reversed() : s.shared_from_this())
            , _permit(permit)
            , _heap_cmp(s)
            , _snapshot(std::move(snp))
            , _region(region)
            , _read_section(read_section)
            , _rt_stream(s, permit)
            , _digest_requested(digest_requested)
        { }

        void reset_state(const query::clustering_range& ck_range_snapshot) {
            return in_alloc_section([&] {
                do_refresh_state(ck_range_snapshot, {}, {});
            });
        }

        template<typename Function>
        decltype(auto) with_reserve(Function&& fn) {
            return _read_section.with_reserve(_region, std::forward<Function>(fn));
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
        // If the ck_range_snapshot is the same as the one used previously last_row needs
        // to be engaged and equal the position of the row returned last time.
        // If the ck_range_snapshot is different or this is the first call to this
        // function last_row has to be disengaged. Additionally, when entering
        // new range _rt_stream will be populated with all relevant
        // tombstones.
        mutation_fragment_opt next_row(const query::clustering_range& ck_range_snapshot,
                                       const std::optional<position_in_partition>& last_row,
                                       const std::optional<position_in_partition>& last_rts) {
            return in_alloc_section([&] () -> mutation_fragment_opt {
                maybe_refresh_state(ck_range_snapshot, last_row, last_rts);

                position_in_partition::equal_compare rows_eq(_query_schema);
                while (has_more_rows()) {
                    const rows_entry& e = pop_clustering_row();
                    if (e.dummy()) {
                        continue;
                    }
                    if (_digest_requested) {
                        e.row().cells().prepare_hash(_query_schema, column_kind::regular_column);
                    }
                    auto result = mutation_fragment(mutation_fragment::clustering_row_tag_t(), _query_schema, _permit, _query_schema, e);
                    // TODO: Ideally this should be position() or position().reversed(), depending on Reversing.
                    while (has_more_rows() && rows_eq(peek_row().position(), result.as_clustering_row().position())) {
                        const rows_entry& e = pop_clustering_row();
                        if (_digest_requested) {
                            e.row().cells().prepare_hash(_query_schema, column_kind::regular_column);
                        }
                        result.mutate_as_clustering_row(_query_schema, [&] (clustering_row& cr) mutable {
                            cr.apply(_query_schema, e);
                        });
                    }
                    return result;
                }
                return { };
            });
        }

        mutation_fragment_opt next_range_tombstone(const query::clustering_range& ck_range_snapshot,
                const query::clustering_range& ck_range_query,
                const std::optional<position_in_partition>& last_row,
                const std::optional<position_in_partition>& last_rts,
                position_in_partition_view pos) {
            return in_alloc_section([&] () -> mutation_fragment_opt {
                maybe_refresh_state(ck_range_snapshot, last_row, last_rts);

                position_in_partition::less_compare rt_less(_query_schema);

                // The while below moves range tombstones from partition versions
                // into _rt_stream, just enough to produce the next range tombstone
                // The main goal behind moving to _rt_stream is to deoverlap range tombstones
                // which have the same starting position. This is not in order to satisfy
                // flat_mutation_reader stream requirements, the reader can emit range tombstones
                // which have the same position incrementally. This is to guarantee forward
                // progress in the case iterators get invalidated and maybe_refresh_state()
                // above needs to restore them. It does so using last_rts, which tracks
                // the position of the last emitted range tombstone. All range tombstones
                // with positions <= than last_rts are skipped on refresh. To make progress,
                // we need to make sure that all range tombstones with duplicated positions
                // are emitted before maybe_refresh_state().
                while (has_more_range_tombstones()
                        && !rt_less(pos, peek_range_tombstone().position())
                        && (_rt_stream.empty() || !rt_less(_rt_stream.peek_next().position(), peek_range_tombstone().position()))) {
                    range_tombstone rt = pop_range_tombstone();

                    if (rt.trim(_query_schema,
                                position_in_partition_view::for_range_start(ck_range_query),
                                position_in_partition_view::for_range_end(ck_range_query))) {
                        _rt_stream.apply(std::move(rt));
                    }
                }
                return _rt_stream.get_next(std::move(pos));
            });
        }
    };
private:
    // Keeps shared pointer to the container we read mutation from to make sure
    // that its lifetime is appropriately extended.
    std::any _container_guard;

    // Each range from _ck_ranges are taken to be in snapshot clustering key
    // order, i.e. given a comparator derived from snapshot schema, for each ck_range from
    // _ck_ranges, begin(ck_range) <= end(ck_range).
    query::clustering_key_filter_ranges _ck_ranges;
    query::clustering_row_ranges::const_iterator _current_ck_range;
    query::clustering_row_ranges::const_iterator _ck_range_end;

    // Holds reversed current clustering key range, if Reversing was needed.
    std::optional<query::clustering_range> opt_reversed_range;

    std::optional<position_in_partition> _last_entry;
    // When not Reversing, it's .position() of last emitted range tombstone.
    // When Reversing, it's .position().reversed() of last emitted range tombstone,
    // so that it is usable from functions expecting position in snapshot domain.
    std::optional<position_in_partition> _last_rts;
    mutation_fragment_opt _next_row;

    range_tombstone_change_generator _rtc_gen;

    lsa_partition_reader _reader;
    bool _static_row_done = false;
    bool _no_more_rows_in_current_range = false;

    Accounter& accounter() {
        return *this;
    }
private:
    void push_static_row() {
        auto sr = _reader.get_static_row();
        if (!sr.empty()) {
            emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
        }
    }

    // If `Reversing`, when we pop_range_tombstone(), a reversed rt is returned (the correct
    // one in query clustering order). In order to save progress of reading from range_tombstone_list,
    // we need to save the end position of rt (as it was stored in the list). This corresponds to
    // the start position, with reversed bound weigth.
    static position_in_partition rt_position_in_snapshot_order(const range_tombstone& rt) {
        position_in_partition pos(rt.position());
        if constexpr (Reversing) {
            pos = pos.reversed();
        }
        return pos;
    }

    mutation_fragment_opt read_next() {
        // We use the names ck_range_snapshot and ck_range_query to denote clustering order.
        // ck_range_snapshot uses the snapshot order, while ck_range_query uses the
        // query order. These two differ if the query was reversed (`Reversing==true`).
        const auto& ck_range_snapshot = *_current_ck_range;
        const auto& ck_range_query = opt_reversed_range ? *opt_reversed_range : ck_range_snapshot;

        if (!_next_row && !_no_more_rows_in_current_range) {
            _next_row = _reader.next_row(ck_range_snapshot, _last_entry, _last_rts);
        }

        if (_next_row) {
            auto pos_view = _next_row->as_clustering_row().position();
            _last_entry = position_in_partition(pos_view);

            auto mf = _reader.next_range_tombstone(ck_range_snapshot, ck_range_query, _last_entry, _last_rts,  pos_view);
            if (mf) {
                _last_rts = rt_position_in_snapshot_order(mf->as_range_tombstone());
                return mf;
            }
            return std::exchange(_next_row, {});
        } else {
            _no_more_rows_in_current_range = true;
            auto mf = _reader.next_range_tombstone(ck_range_snapshot, ck_range_query, _last_entry, _last_rts, position_in_partition_view::for_range_end(ck_range_query));
            if (mf) {
                _last_rts = rt_position_in_snapshot_order(mf->as_range_tombstone());
            }
            return mf;
        }
    }

    void emplace_mutation_fragment(mutation_fragment&& mf) {
        _rtc_gen.flush(mf.position(), [this] (range_tombstone_change&& rtc) {
            emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rtc)));
        });
        if (mf.is_clustering_row()) {
            emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(mf).as_clustering_row()));
        } else {
            assert(mf.is_range_tombstone());
            _rtc_gen.consume(std::move(mf).as_range_tombstone());
        }
    }

    void emplace_mutation_fragment(mutation_fragment_v2&& mfopt) {
        mfopt.visit(accounter());
        push_mutation_fragment(std::move(mfopt));
    }

    void on_new_range() {
        if (_current_ck_range == _ck_range_end) {
            _end_of_stream = true;
            _rtc_gen.flush(position_in_partition::after_all_clustered_rows(), [this] (range_tombstone_change&& rtc) {
                emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rtc)));
            });
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end()));
        } else {
            _reader.reset_state(*_current_ck_range);
        }
        _no_more_rows_in_current_range = false;
    }

    void fill_opt_reversed_range() {
        opt_reversed_range = std::nullopt;
        if (_current_ck_range != _ck_range_end) {
            if constexpr (Reversing) {
                opt_reversed_range = query::reverse(*_current_ck_range);
            }
        }
    }

    void do_fill_buffer() {
        while (!is_end_of_stream() && !is_buffer_full()) {
            auto mfopt = read_next();
            if (mfopt) {
                emplace_mutation_fragment(std::move(*mfopt));
            } else {
                _last_entry = std::nullopt;
                _last_rts = std::nullopt;
                _current_ck_range = std::next(_current_ck_range);
                fill_opt_reversed_range();
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
                              std::any pointer_to_container, Args&&... args)
        : impl(std::move(s), std::move(permit))
        , Accounter(std::forward<Args>(args)...)
        , _container_guard(std::move(pointer_to_container))
        , _ck_ranges(std::move(crr))
        , _current_ck_range(_ck_ranges.begin())
        , _ck_range_end(_ck_ranges.end())
        , _rtc_gen(*_schema)
        , _reader(*_schema, _permit, std::move(snp), region, read_section, digest_requested)
    {
        fill_opt_reversed_range();
        _reader.with_reserve([&] {
            push_mutation_fragment(*_schema, _permit, partition_start(std::move(dk), _reader.partition_tombstone()));
        });
    }

    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            _reader.with_reserve([&] {
                if (!_static_row_done) {
                    push_static_row();
                    on_new_range();
                    _static_row_done = true;
                }
                do_fill_buffer();
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
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        throw std::runtime_error("This reader can't be fast forwarded to another partition.");
    };
    virtual future<> fast_forward_to(position_range cr) override {
        throw std::runtime_error("This reader can't be fast forwarded to another position.");
    };
    virtual future<> close() noexcept override {
        return make_ready_future<>();
    }
};

template <bool Reversing, typename Accounter, typename... Args>
inline flat_mutation_reader_v2
make_partition_snapshot_flat_reader(schema_ptr s,
                                    reader_permit permit,
                                    dht::decorated_key dk,
                                    query::clustering_key_filter_ranges crr,
                                    partition_snapshot_ptr snp,
                                    bool digest_requested,
                                    logalloc::region& region,
                                    logalloc::allocating_section& read_section,
                                    std::any pointer_to_container,
                                    streamed_mutation::forwarding fwd,
                                    Args&&... args)
{
    auto res = make_flat_mutation_reader_v2<partition_snapshot_flat_reader<Reversing, Accounter>>(std::move(s), std::move(permit), std::move(dk),
            snp, std::move(crr), digest_requested, region, read_section, std::move(pointer_to_container), std::forward<Args>(args)...);
    if (fwd) {
        return make_forwardable(std::move(res)); // FIXME: optimize
    } else {
        return res;
    }
}
