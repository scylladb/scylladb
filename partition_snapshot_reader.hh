/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/partition_version.hh"
#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "readers/range_tombstone_change_merger.hh"
#include "clustering_key_filter.hh"
#include "query-request.hh"
#include "partition_snapshot_row_cursor.hh"
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <any>

extern seastar::logger mplog;

template <bool Reversing, typename Accounter>
class partition_snapshot_flat_reader : public mutation_reader::impl, public Accounter {
    struct row_info {
        mutation_fragment_v2 row;
        tombstone rt_for_row;
    };

    // Represents a subset of mutations for some clustering key range.
    //
    // The range of the interval starts at the upper bound of the previous
    // interval and its end depends on the contents of info:
    //    - position_in_partition: holds the upper bound of the interval
    //    - row_info: after_key(row_info::row.as_clustering_row().key())
    //    - monostate: upper bound is the end of the current clustering key range
    //
    // All positions in query schema domain.
    struct interval_info {
        // Applies to the whole range of the interval.
        tombstone range_tombstone;

        // monostate means no more rows (end of range).
        // position_in_partition means there is no row, it is the upper bound of the interval.
        // if row_info, the upper bound is after_key(row_info::row.as_clustering_row().key()).
        std::variant<row_info, position_in_partition, std::monostate> info;
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
        reader_permit _permit;
        partition_snapshot_ptr _snapshot;
        logalloc::region& _region;
        logalloc::allocating_section& _read_section;
        partition_snapshot_row_cursor _cursor;
        bool _digest_requested;
        bool _done = false;
    private:
        template<typename Function>
        decltype(auto) in_alloc_section(Function&& fn) {
            return _read_section.with_reclaiming_disabled(_region, [&] {
                return fn();
            });
        }
    public:
        explicit lsa_partition_reader(const schema& s, reader_permit permit, partition_snapshot_ptr snp,
                                      logalloc::region& region, logalloc::allocating_section& read_section,
                                      bool digest_requested)
            : _query_schema(s)
            , _permit(permit)
            , _snapshot(std::move(snp))
            , _region(region)
            , _read_section(read_section)
            , _cursor(s, *_snapshot, false, Reversing, digest_requested)
            , _digest_requested(digest_requested)
        { }

        void on_new_range(position_in_partition_view lower_bound) {
            in_alloc_section([&] {
                _done = false;
                _cursor.advance_to(lower_bound);
                mplog.trace("on_new_range({}): {}", lower_bound, _cursor);
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

        // Returns mutations for the next interval in the range.
        interval_info next_interval(const query::clustering_range& ck_range_query) {
            return in_alloc_section([&]() -> interval_info {
                position_in_partition::tri_compare cmp(_query_schema);

                // Result is ignored because memtables don't lose information. If the entry is missing,
                // it must have been redundant, and we can as well look at the next entry.
                _cursor.maybe_refresh();

                auto rt_before_row = _cursor.range_tombstone();
                mplog.trace("next_interval(): range={}, rt={}, cursor={}", ck_range_query, rt_before_row, _cursor);

                if (_done || cmp(_cursor.position(), position_in_partition::for_range_end(ck_range_query)) >= 0) {
                    mplog.trace("next_interval(): done");
                    return interval_info{rt_before_row, std::monostate{}};
                }

                if (_cursor.dummy()) {
                    mplog.trace("next_interval(): pos={}, rt={}", _cursor.position(), rt_before_row);
                    auto res = interval_info{rt_before_row, position_in_partition(_cursor.position())};
                    _done = !_cursor.next();
                    return res;
                }

                tombstone rt_for_row = _cursor.range_tombstone_for_row();
                mplog.trace("next_interval(): row, pos={}, rt={}, rt_for_row={}", _cursor.position(), rt_before_row, rt_for_row);
                auto result = mutation_fragment_v2(_query_schema, _permit, _cursor.row());
                _done = !_cursor.next();
                return interval_info{rt_before_row, row_info{std::move(result), rt_for_row}};
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

    std::optional<position_in_partition> _lower_bound;

    // Last emitted range_tombstone_change.
    tombstone _current_tombstone;

    lsa_partition_reader _reader;
    bool _static_row_done = false;

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

    // We use the names ck_range_snapshot and ck_range_query to denote clustering order.
    // ck_range_snapshot uses the snapshot order, while ck_range_query uses the
    // query order. These two differ if the query was reversed (`Reversing==true`).
    const query::clustering_range& current_ck_range_query() {
        return *_current_ck_range;
    }

    void emit_next_interval() {
        interval_info next = _reader.next_interval(current_ck_range_query());

        if (next.range_tombstone != _current_tombstone) {
            _current_tombstone = next.range_tombstone;
            emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                range_tombstone_change(*_lower_bound, _current_tombstone)));
        }

        std::visit(make_visitor([&] (row_info&& info) {
            auto pos_view = info.row.as_clustering_row().position();
            _lower_bound = position_in_partition::after_key(*_schema, pos_view);
            if (info.rt_for_row != _current_tombstone) {
                _current_tombstone = info.rt_for_row;
                emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                    range_tombstone_change(
                        position_in_partition::before_key(info.row.as_clustering_row().key()), _current_tombstone)));
            }
            emplace_mutation_fragment(std::move(info.row));
        }, [&] (position_in_partition&& pos) {
            _lower_bound = std::move(pos);
        }, [&] (std::monostate) {
            if (_current_tombstone) {
                _current_tombstone = {};
                emplace_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                    range_tombstone_change(position_in_partition_view::for_range_end(current_ck_range_query()), _current_tombstone)));
            }
            _current_ck_range = std::next(_current_ck_range);
            on_new_range();
        }), std::move(next.info));
    }

    void emplace_mutation_fragment(mutation_fragment_v2&& mfopt) {
        mfopt.visit(accounter());
        push_mutation_fragment(std::move(mfopt));
    }

    void on_new_range() {
        if (_current_ck_range == _ck_range_end) {
            _end_of_stream = true;
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end()));
        } else {
            _lower_bound = position_in_partition_view::for_range_start(current_ck_range_query());
            _reader.on_new_range(*_lower_bound);
        }
    }

    void do_fill_buffer() {
        while (!is_end_of_stream() && !is_buffer_full()) {
            emit_next_interval();
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
        , _reader(*_schema, _permit, std::move(snp), region, read_section, digest_requested)
    {
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
inline mutation_reader
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
    auto res = make_mutation_reader<partition_snapshot_flat_reader<Reversing, Accounter>>(std::move(s), std::move(permit), std::move(dk),
            snp, std::move(crr), digest_requested, region, read_section, std::move(pointer_to_container), std::forward<Args>(args)...);
    if (fwd) {
        return make_forwardable(std::move(res)); // FIXME: optimize
    } else {
        return res;
    }
}
