/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "clustering_key_filter.hh"
#include "clustering_ranges_walker.hh"
#include "dht/i_partitioner.hh"
#include "mutation.hh"
#include "mutation_partition.hh"
#include "mutation_compactor.hh"
#include "range_tombstone_assembler.hh"
#include "range_tombstone_splitter.hh"
#include "readers/combined.hh"
#include "readers/delegating.hh"
#include "readers/delegating_v2.hh"
#include "readers/empty.hh"
#include "readers/empty_v2.hh"
#include "readers/flat_mutation_reader.hh"
#include "readers/flat_mutation_reader_v2.hh"
#include "readers/forwardable.hh"
#include "readers/forwardable_v2.hh"
#include "readers/from_fragments_v2.hh"
#include "readers/from_mutations.hh"
#include "readers/from_mutations_v2.hh"
#include "readers/generating_v2.hh"
#include "readers/multi_range.hh"
#include "readers/mutation_source.hh"
#include "readers/nonforwardable.hh"
#include "readers/queue.hh"
#include "readers/reversing.hh"
#include "readers/slice_mutations.hh"
#include "readers/upgrading_consumer.hh"
#include <seastar/core/coroutine.hh>
#include <stack>

extern logging::logger fmr_logger;

flat_mutation_reader make_delegating_reader(flat_mutation_reader& r) {
    return make_flat_mutation_reader<delegating_reader>(r);
}

future<> delegating_reader::fill_buffer() {
    if (is_buffer_full()) {
        return make_ready_future<>();
    }
    return _underlying->fill_buffer().then([this] {
        _end_of_stream = _underlying->is_end_of_stream();
        _underlying->move_buffer_content_to(*this);
    });
}

future<> delegating_reader::fast_forward_to(position_range pr) {
    _end_of_stream = false;
    forward_buffer_to(pr.start());
    return _underlying->fast_forward_to(std::move(pr));
}

future<> delegating_reader::next_partition() {
    clear_buffer_to_next_partition();
    auto maybe_next_partition = make_ready_future<>();
    if (is_buffer_empty()) {
        maybe_next_partition = _underlying->next_partition();
    }
    return maybe_next_partition.then([this] {
        _end_of_stream = _underlying->is_end_of_stream() && _underlying->is_buffer_empty();
    });
}

future<> delegating_reader::fast_forward_to(const dht::partition_range& pr) {
    _end_of_stream = false;
    clear_buffer();
    return _underlying->fast_forward_to(pr);
}

future<> delegating_reader::close() noexcept {
    return _underlying_holder ? _underlying_holder->close() : make_ready_future<>();
}

flat_mutation_reader_v2 make_delegating_reader_v2(flat_mutation_reader_v2& r) {
    return make_flat_mutation_reader_v2<delegating_reader_v2>(r);
}

class empty_flat_reader final : public flat_mutation_reader::impl {
public:
    empty_flat_reader(schema_ptr s, reader_permit permit) : impl(std::move(s), std::move(permit)) { _end_of_stream = true; }
    virtual future<> fill_buffer() override { return make_ready_future<>(); }
    virtual future<> next_partition() override { return make_ready_future<>(); }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override { return make_ready_future<>(); };
    virtual future<> fast_forward_to(position_range cr) override { return make_ready_future<>(); };
    virtual future<> close() noexcept override { return make_ready_future<>(); }
};

flat_mutation_reader make_empty_flat_reader(schema_ptr s, reader_permit permit) {
    return make_flat_mutation_reader<empty_flat_reader>(std::move(s), std::move(permit));
}

class empty_flat_reader_v2 final : public flat_mutation_reader_v2::impl {
public:
    empty_flat_reader_v2(schema_ptr s, reader_permit permit) : impl(std::move(s), std::move(permit)) { _end_of_stream = true; }
    virtual future<> fill_buffer() override { return make_ready_future<>(); }
    virtual future<> next_partition() override { return make_ready_future<>(); }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override { return make_ready_future<>(); };
    virtual future<> fast_forward_to(position_range cr) override { return make_ready_future<>(); };
    virtual future<> close() noexcept override { return make_ready_future<>(); }
};

flat_mutation_reader_v2 make_empty_flat_reader_v2(schema_ptr s, reader_permit permit) {
    return make_flat_mutation_reader_v2<empty_flat_reader_v2>(std::move(s), std::move(permit));
}

flat_mutation_reader make_forwardable(flat_mutation_reader m) {
    class reader : public flat_mutation_reader::impl {
        flat_mutation_reader _underlying;
        position_range _current;
        mutation_fragment_opt _next;
        // When resolves, _next is engaged or _end_of_stream is set.
        future<> ensure_next() {
            if (_next) {
                co_return;
            }
            _next = co_await _underlying();
            if (!_next) {
                _end_of_stream = true;
            }
        }
    public:
        reader(flat_mutation_reader r) : impl(r.schema(), r.permit()), _underlying(std::move(r)), _current({
            position_in_partition(position_in_partition::partition_start_tag_t()),
            position_in_partition(position_in_partition::after_static_row_tag_t())
        }) { }
        virtual future<> fill_buffer() override {
            while (!is_buffer_full()) {
                co_await ensure_next();
                if (is_end_of_stream()) {
                    break;
                }
                position_in_partition::less_compare cmp(*_schema);
                if (!cmp(_next->position(), _current.end())) {
                    _end_of_stream = true;
                    // keep _next, it may be relevant for next range
                    break;
                }
                if (_next->relevant_for_range(*_schema, _current.start())) {
                    push_mutation_fragment(std::move(*_next));
                }
                _next = {};
            }
        }
        virtual future<> fast_forward_to(position_range pr) override {
            _current = std::move(pr);
            _end_of_stream = false;
            forward_buffer_to(_current.start());
            return make_ready_future<>();
        }
        virtual future<> next_partition() override {
            _end_of_stream = false;
            if (!_next || !_next->is_partition_start()) {
                co_await _underlying.next_partition();
                _next = {};
            }
            clear_buffer_to_next_partition();
            _current = {
                position_in_partition(position_in_partition::partition_start_tag_t()),
                position_in_partition(position_in_partition::after_static_row_tag_t())
            };
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            _end_of_stream = false;
            clear_buffer();
            _next = {};
            _current = {
                position_in_partition(position_in_partition::partition_start_tag_t()),
                position_in_partition(position_in_partition::after_static_row_tag_t())
            };
            return _underlying.fast_forward_to(pr);
        }
        virtual future<> close() noexcept override {
            return _underlying.close();
        }
    };
    return make_flat_mutation_reader<reader>(std::move(m));
}

flat_mutation_reader_v2 make_forwardable(flat_mutation_reader_v2 m) {
    class reader : public flat_mutation_reader_v2::impl {
        flat_mutation_reader_v2 _underlying;
        position_range _current;
        mutation_fragment_v2_opt _next;
        tombstone _active_tombstone;
        bool _current_has_content = false;
        // When resolves, _next is engaged or _end_of_stream is set.
        future<> ensure_next() {
            if (_next) {
                co_return;
            }
            _next = co_await _underlying();
            if (!_next) {
                maybe_emit_end_tombstone();
                _end_of_stream = true;
            }
        }
        void maybe_emit_start_tombstone() {
            if (!_current_has_content && _active_tombstone) {
                _current_has_content = true;
                push_mutation_fragment(*_schema, _permit, range_tombstone_change(position_in_partition_view::before_key(_current.start()), _active_tombstone));
            }
        }
        void maybe_emit_end_tombstone() {
            if (_active_tombstone) {
                push_mutation_fragment(*_schema, _permit, range_tombstone_change(position_in_partition_view::after_key(_current.end()), {}));
            }
        }
    public:
        reader(flat_mutation_reader_v2 r) : impl(r.schema(), r.permit()), _underlying(std::move(r)), _current({
            position_in_partition(position_in_partition::partition_start_tag_t()),
            position_in_partition(position_in_partition::after_static_row_tag_t())
        }) { }
        virtual future<> fill_buffer() override {
            while (!is_buffer_full()) {
                co_await ensure_next();
                if (is_end_of_stream()) {
                    break;
                }
                position_in_partition::tri_compare cmp(*_schema);
                if (cmp(_next->position(), _current.end()) >= 0) {
                    maybe_emit_start_tombstone();
                    maybe_emit_end_tombstone();
                    _end_of_stream = true;
                    // keep _next, it may be relevant for next range
                    break;
                }
                if (_next->relevant_for_range(*_schema, _current.start())) {
                    if (!_current_has_content && (!_next->is_range_tombstone_change() || cmp(_next->position(), _current.start()) != 0)) {
                        maybe_emit_start_tombstone();
                    }
                    if (_next->is_range_tombstone_change()) {
                        _active_tombstone = _next->as_range_tombstone_change().tombstone();
                    }
                    _current_has_content = true;
                    push_mutation_fragment(std::move(*_next));
                } else if (_next->is_range_tombstone_change()) {
                    _active_tombstone = _next->as_range_tombstone_change().tombstone();
                }
                _next = {};
            }
        }
        virtual future<> fast_forward_to(position_range pr) override {
            _current = std::move(pr);
            _end_of_stream = false;
            _current_has_content = false;
            forward_buffer_to(_current.start());
            return make_ready_future<>();
        }
        virtual future<> next_partition() override {
            _end_of_stream = false;
            if (!_next || !_next->is_partition_start()) {
                co_await _underlying.next_partition();
                _next = {};
            }
            clear_buffer_to_next_partition();
            _current = {
                position_in_partition(position_in_partition::partition_start_tag_t()),
                position_in_partition(position_in_partition::after_static_row_tag_t())
            };
            _active_tombstone = {};
            _current_has_content = false;
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            _end_of_stream = false;
            clear_buffer();
            _next = {};
            _current = {
                position_in_partition(position_in_partition::partition_start_tag_t()),
                position_in_partition(position_in_partition::after_static_row_tag_t())
            };
            _active_tombstone = {};
            _current_has_content = false;
            return _underlying.fast_forward_to(pr);
        }
        virtual future<> close() noexcept override {
            return _underlying.close();
        }
    };
    return make_flat_mutation_reader_v2<reader>(std::move(m));
}

flat_mutation_reader make_slicing_filtering_reader(flat_mutation_reader rd, const dht::partition_range& pr, const query::partition_slice& slice) {
    class reader : public flat_mutation_reader::impl {
        flat_mutation_reader _rd;
        const dht::partition_range* _pr;
        const query::partition_slice* _slice;
        dht::ring_position_comparator _cmp;
        std::optional<clustering_ranges_walker> _ranges_walker;
        std::optional<range_tombstone_splitter> _splitter;

    public:
        reader(flat_mutation_reader rd, const dht::partition_range& pr, const query::partition_slice& slice)
            : flat_mutation_reader::impl(rd.schema(), rd.permit())
            , _rd(std::move(rd))
            , _pr(&pr)
            , _slice(&slice)
            , _cmp(*_schema) {
        }

        virtual future<> fill_buffer() override {
            const auto consume_fn = [this] (mutation_fragment mf) {
                push_mutation_fragment(std::move(mf));
            };

            while (!is_buffer_full() && !is_end_of_stream()) {
                co_await _rd.fill_buffer();
                while (!_rd.is_buffer_empty()) {
                    auto mf = _rd.pop_mutation_fragment();
                    switch (mf.mutation_fragment_kind()) {
                    case mutation_fragment::kind::partition_start: {
                        auto& dk = mf.as_partition_start().key();
                        if (!_pr->contains(dk, _cmp)) {
                            co_return co_await _rd.next_partition();
                        } else {
                            _ranges_walker.emplace(*_schema, _slice->row_ranges(*_schema, dk.key()), false);
                            _splitter.emplace(*_schema, _permit, *_ranges_walker);
                        }
                        // fall-through
                    }

                    case mutation_fragment::kind::static_row:
                        consume_fn(std::move(mf));
                        break;

                    case mutation_fragment::kind::partition_end:
                        _splitter->flush(position_in_partition::after_all_clustered_rows(), consume_fn);
                        consume_fn(std::move(mf));
                        break;

                    case mutation_fragment::kind::clustering_row:
                        _splitter->flush(mf.position(), consume_fn);
                        if (_ranges_walker->advance_to(mf.position())) {
                            consume_fn(std::move(mf));
                        }
                        break;

                    case mutation_fragment::kind::range_tombstone:
                        auto&& rt = mf.as_range_tombstone();
                        _splitter->consume(rt, consume_fn);
                        break;
                    }
                }
                
                _end_of_stream = _rd.is_end_of_stream();
                co_return;
            }
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
                _end_of_stream = false;
                return _rd.next_partition();
            }

            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            return _rd.fast_forward_to(pr);
        }

        virtual future<> fast_forward_to(position_range pr) override {
            forward_buffer_to(pr.start());
            _end_of_stream = false;
            return _rd.fast_forward_to(std::move(pr));
        }

        virtual future<> close() noexcept override {
            return _rd.close();
        }
    };

    return make_flat_mutation_reader<reader>(std::move(rd), pr, slice);
}

std::vector<mutation> slice_mutations(schema_ptr schema, std::vector<mutation> ms, const query::partition_slice& slice) {
    std::vector<mutation> sliced_ms;
    for (auto& m : ms) {
        auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*schema, slice, m.key());
        auto mp = mutation_partition(std::move(m.partition()), *schema, std::move(ck_ranges));
        sliced_ms.emplace_back(schema, m.decorated_key(), std::move(mp));
    }
    return sliced_ms;
}

flat_mutation_reader make_reversing_reader(flat_mutation_reader original, query::max_result_size max_size, std::unique_ptr<query::partition_slice> slice) {
    class partition_reversing_mutation_reader final : public flat_mutation_reader::impl {
        flat_mutation_reader _source;
        range_tombstone_list _range_tombstones;
        std::stack<mutation_fragment> _mutation_fragments;
        mutation_fragment_opt _partition_end;
        size_t _stack_size = 0;
        const query::max_result_size _max_size;
        bool _below_soft_limit = true;
        std::unique_ptr<query::partition_slice> _slice; // only stored, not used
    private:
        stop_iteration emit_partition() {
            auto emit_range_tombstone = [&] {
                // _range_tombstones uses the reverse schema already, so we can use `begin()`
                auto it = _range_tombstones.begin();
                push_mutation_fragment(*_schema, _permit, _range_tombstones.pop(it));
            };
            position_in_partition::tri_compare cmp(*_schema);
            while (!_mutation_fragments.empty() && !is_buffer_full()) {
                auto& mf = _mutation_fragments.top();
                if (!_range_tombstones.empty() && cmp(_range_tombstones.begin()->position(), mf.position()) <= 0) {
                    emit_range_tombstone();
                } else {
                    _stack_size -= mf.memory_usage();
                    push_mutation_fragment(std::move(mf));
                    _mutation_fragments.pop();
                }
            }
            while (!_range_tombstones.empty() && !is_buffer_full()) {
                emit_range_tombstone();
            }
            if (is_buffer_full()) {
                return stop_iteration::yes;
            }
            push_mutation_fragment(std::move(*std::exchange(_partition_end, std::nullopt)));
            return stop_iteration::no;
        }
        future<stop_iteration> consume_partition_from_source() {
            if (_source.is_buffer_empty()) {
                if (_source.is_end_of_stream()) {
                    _end_of_stream = true;
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return _source.fill_buffer().then([] { return stop_iteration::no; });
            }
            while (!_source.is_buffer_empty() && !is_buffer_full()) {
                auto mf = _source.pop_mutation_fragment();
                if (mf.is_partition_start() || mf.is_static_row()) {
                    push_mutation_fragment(std::move(mf));
                } else if (mf.is_end_of_partition()) {
                    _partition_end = std::move(mf);
                    if (emit_partition()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                } else if (mf.is_range_tombstone()) {
                    auto&& rt = std::move(mf).as_range_tombstone();
                    rt.reverse();
                    _range_tombstones.apply(*_schema, std::move(rt));
                } else {
                    _mutation_fragments.emplace(std::move(mf));
                    _stack_size += _mutation_fragments.top().memory_usage();
                    if (_stack_size > _max_size.hard_limit || (_stack_size > _max_size.soft_limit && _below_soft_limit)) {
                        const partition_key* key = nullptr;
                        auto it = buffer().end();
                        --it;
                        if (it->is_partition_start()) {
                            key = &it->as_partition_start().key().key();
                        } else {
                            --it;
                            key = &it->as_partition_start().key().key();
                        }

                        if (_stack_size > _max_size.hard_limit) {
                            return make_exception_future<stop_iteration>(std::runtime_error(fmt::format(
                                    "Memory usage of reversed read exceeds hard limit of {} (configured via max_memory_for_unlimited_query_hard_limit), while reading partition {}",
                                    _max_size.hard_limit,
                                    key->with_schema(*_schema))));
                        } else {
                            fmr_logger.warn(
                                    "Memory usage of reversed read exceeds soft limit of {} (configured via max_memory_for_unlimited_query_soft_limit), while reading partition {}",
                                    _max_size.soft_limit,
                                    key->with_schema(*_schema));
                            _below_soft_limit = false;
                        }
                    }
                }
            }
            return make_ready_future<stop_iteration>(is_buffer_full());
        }
    public:
        explicit partition_reversing_mutation_reader(flat_mutation_reader mr, query::max_result_size max_size, std::unique_ptr<query::partition_slice> slice)
            : flat_mutation_reader::impl(mr.schema()->make_reversed(), mr.permit())
            , _source(std::move(mr))
            , _range_tombstones(*_schema)
            , _max_size(max_size)
            , _slice(std::move(slice))
        { }

        virtual future<> fill_buffer() override {
            return repeat([&] {
                if (_partition_end) {
                    // We have consumed full partition from source, now it is
                    // time to emit it.
                    auto stop = emit_partition();
                    if (stop) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                }
                return consume_partition_from_source();
            });
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty() && !is_end_of_stream()) {
                while (!_mutation_fragments.empty()) {
                    _stack_size -= _mutation_fragments.top().memory_usage();
                    _mutation_fragments.pop();
                }
                _range_tombstones.clear();
                _partition_end = std::nullopt;
                return _source.next_partition();
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            while (!_mutation_fragments.empty()) {
                _mutation_fragments.pop();
            }
            _stack_size = 0;
            _partition_end = std::nullopt;
            _end_of_stream = false;
            return _source.fast_forward_to(pr);
        }

        virtual future<> fast_forward_to(position_range) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }

        virtual future<> close() noexcept override {
            return _source.close();
        }
    };

    return make_flat_mutation_reader<partition_reversing_mutation_reader>(std::move(original), max_size, std::move(slice));
}

flat_mutation_reader make_nonforwardable(flat_mutation_reader r, bool single_partition) {
    class reader : public flat_mutation_reader::impl {
        flat_mutation_reader _underlying;
        bool _single_partition;
        bool _static_row_done = false;
        bool is_end_end_of_underlying_stream() const {
            return _underlying.is_buffer_empty() && _underlying.is_end_of_stream();
        }
        future<> on_end_of_underlying_stream() {
            if (!_static_row_done) {
                _static_row_done = true;
                return _underlying.fast_forward_to(position_range::all_clustered_rows());
            }
            push_mutation_fragment(*_schema, _permit, partition_end());
            if (_single_partition) {
                _end_of_stream = true;
                return make_ready_future<>();
            }
          return _underlying.next_partition().then([this] {
            _static_row_done = false;
            return _underlying.fill_buffer().then([this] {
                _end_of_stream = is_end_end_of_underlying_stream();
            });
          });
        }
    public:
        reader(flat_mutation_reader r, bool single_partition)
            : impl(r.schema(), r.permit())
            , _underlying(std::move(r))
            , _single_partition(single_partition)
        { }
        virtual future<> fill_buffer() override {
            return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
                return fill_buffer_from(_underlying).then([this] (bool underlying_finished) {
                    if (underlying_finished) {
                        return on_end_of_underlying_stream();
                    }
                    return make_ready_future<>();
                });
            });
        }
        virtual future<> fast_forward_to(position_range pr) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            auto maybe_next_partition = make_ready_future<>();;
            if (is_buffer_empty()) {
                maybe_next_partition = _underlying.next_partition();
            }
          return maybe_next_partition.then([this] {
            _end_of_stream = is_end_end_of_underlying_stream();
          });
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            _end_of_stream = false;
            clear_buffer();
            return _underlying.fast_forward_to(pr);
        }
        virtual future<> close() noexcept override {
            return _underlying.close();
        }
    };
    return make_flat_mutation_reader<reader>(std::move(r), single_partition);
}

template<typename Generator>
class flat_multi_range_mutation_reader : public flat_mutation_reader_v2::impl {
    std::optional<Generator> _generator;
    flat_mutation_reader_v2 _reader;

    const dht::partition_range* next() {
        if (!_generator) {
            return nullptr;
        }
        return (*_generator)();
    }

public:
    flat_multi_range_mutation_reader(
            schema_ptr s,
            reader_permit permit,
            mutation_source source,
            const dht::partition_range& first_range,
            Generator generator,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state)
        : impl(s, std::move(permit))
        , _generator(std::move(generator))
        , _reader(source.make_reader_v2(s, _permit, first_range, slice, pc, trace_state, streamed_mutation::forwarding::no, mutation_reader::forwarding::yes))
    {
    }

    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || !is_buffer_empty(); }, [this] {
            return _reader.fill_buffer().then([this] () {
                while (!_reader.is_buffer_empty()) {
                    push_mutation_fragment(_reader.pop_mutation_fragment());
                }
                if (!_reader.is_end_of_stream()) {
                    return make_ready_future<>();
                }
                if (auto r = next()) {
                    return _reader.fast_forward_to(*r);
                } else {
                    _end_of_stream = true;
                    return make_ready_future<>();
                }
            });
        });
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        return _reader.fast_forward_to(pr).then([this] {
            _generator.reset();
        });
    }

    virtual future<> fast_forward_to(position_range pr) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }

    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            return _reader.next_partition();
        }
        return make_ready_future<>();
    }

    virtual future<> close() noexcept override {
        return _reader.close();
    }
};

/// A reader that is empty when created but can be fast-forwarded.
///
/// Useful when a reader has to be created without an initial read-range and it
/// has to be fast-forwardable.
/// Delays the creation of the underlying reader until it is first
/// fast-forwarded and thus a range is available.
class forwardable_empty_mutation_reader : public flat_mutation_reader_v2::impl {
    mutation_source _source;
    const query::partition_slice& _slice;
    const io_priority_class& _pc;
    tracing::trace_state_ptr _trace_state;
    flat_mutation_reader_v2_opt _reader;
public:
    forwardable_empty_mutation_reader(schema_ptr s,
            reader_permit permit,
            mutation_source source,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state)
        : impl(s, std::move(permit))
        , _source(std::move(source))
        , _slice(slice)
        , _pc(pc)
        , _trace_state(std::move(trace_state)) {
        _end_of_stream = true;
    }
    virtual future<> fill_buffer() override {
        if (!_reader) {
            return make_ready_future<>();
        }
        if (_reader->is_buffer_empty()) {
            if (_reader->is_end_of_stream()) {
                _end_of_stream = true;
                return make_ready_future<>();
            } else {
                return _reader->fill_buffer().then([this] { return fill_buffer(); });
            }
        }
        _reader->move_buffer_content_to(*this);
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        if (!_reader) {
            _reader = _source.make_reader_v2(_schema, _permit, pr, _slice, _pc, std::move(_trace_state), streamed_mutation::forwarding::no,
                    mutation_reader::forwarding::yes);
            _end_of_stream = false;
            return make_ready_future<>();
        }

        clear_buffer();
        _end_of_stream = false;
        return _reader->fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> next_partition() override {
        if (!_reader) {
            return make_ready_future<>();
        }
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            return _reader->next_partition();
        }
        return make_ready_future<>();
    }
    virtual future<> close() noexcept override {
        return _reader ? _reader->close() : make_ready_future<>();
    }
};
flat_mutation_reader_v2
make_flat_multi_range_reader(schema_ptr s, reader_permit permit, mutation_source source, const dht::partition_range_vector& ranges,
                        const query::partition_slice& slice, const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state,
                        mutation_reader::forwarding fwd_mr)
{
    class adapter {
        dht::partition_range_vector::const_iterator _it;
        dht::partition_range_vector::const_iterator _end;

    public:
        adapter(dht::partition_range_vector::const_iterator begin, dht::partition_range_vector::const_iterator end) : _it(begin), _end(end) {
        }
        const dht::partition_range* operator()() {
            if (_it == _end) {
                return nullptr;
            }
            return &*_it++;
        }
    };

    if (ranges.empty()) {
        if (fwd_mr) {
            return make_flat_mutation_reader_v2<forwardable_empty_mutation_reader>(std::move(s), std::move(permit), std::move(source), slice, pc,
                    std::move(trace_state));
        } else {
            return make_empty_flat_reader_v2(std::move(s), std::move(permit));
        }
    } else if (ranges.size() == 1) {
        return source.make_reader_v2(std::move(s), std::move(permit), ranges.front(), slice, pc, std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr);
    } else {
        return make_flat_mutation_reader_v2<flat_multi_range_mutation_reader<adapter>>(std::move(s), std::move(permit), std::move(source),
                ranges.front(), adapter(std::next(ranges.cbegin()), ranges.cend()), slice, pc, std::move(trace_state));
    }
}

flat_mutation_reader_v2
make_flat_multi_range_reader(
        schema_ptr s,
        reader_permit permit,
        mutation_source source,
        std::function<std::optional<dht::partition_range>()> generator,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    class adapter {
        std::function<std::optional<dht::partition_range>()> _generator;
        std::unique_ptr<dht::partition_range> _previous;
        std::unique_ptr<dht::partition_range> _current;

    public:
        explicit adapter(std::function<std::optional<dht::partition_range>()> generator)
            : _generator(std::move(generator))
            , _previous(std::make_unique<dht::partition_range>(dht::partition_range::make_singular({dht::token{}, partition_key::make_empty()})))
            , _current(std::make_unique<dht::partition_range>(dht::partition_range::make_singular({dht::token{}, partition_key::make_empty()}))) {
        }
        const dht::partition_range* operator()() {
            std::swap(_current, _previous);
            if (auto next = _generator()) {
                *_current = std::move(*next);
                return _current.get();
            } else {
                return nullptr;
            }
        }
    };

    auto adapted_generator = adapter(std::move(generator));
    auto* first_range = adapted_generator();
    if (!first_range) {
        if (fwd_mr) {
            return make_flat_mutation_reader_v2<forwardable_empty_mutation_reader>(std::move(s), std::move(permit), std::move(source), slice, pc, std::move(trace_state));
        } else {
            return make_empty_flat_reader_v2(std::move(s), std::move(permit));
        }
    } else {
        return make_flat_mutation_reader_v2<flat_multi_range_mutation_reader<adapter>>(std::move(s), std::move(permit), std::move(source),
                *first_range, std::move(adapted_generator), slice, pc, std::move(trace_state));
    }
}


/*
 * This reader takes a get_next_fragment generator that produces mutation_fragment_opt which is returned by
 * generating_reader.
 */
class generating_reader_v2 final : public flat_mutation_reader_v2::impl {
    noncopyable_function<future<mutation_fragment_v2_opt> ()> _get_next_fragment;
public:
    generating_reader_v2(schema_ptr s, reader_permit permit, noncopyable_function<future<mutation_fragment_v2_opt> ()> get_next_fragment)
        : impl(std::move(s), std::move(permit)), _get_next_fragment(std::move(get_next_fragment))
    { }
    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return _get_next_fragment().then([this] (mutation_fragment_v2_opt mopt) {
                if (!mopt) {
                    _end_of_stream = true;
                } else {
                    push_mutation_fragment(std::move(*mopt));
                }
            });
        });
    }
    virtual future<> next_partition() override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(const dht::partition_range&) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(position_range) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        return make_ready_future<>();
    }
};

flat_mutation_reader_v2 make_generating_reader_v2(schema_ptr s, reader_permit permit, noncopyable_function<future<mutation_fragment_v2_opt> ()> get_next_fragment) {
    return make_flat_mutation_reader_v2<generating_reader_v2>(std::move(s), std::move(permit), std::move(get_next_fragment));
}

flat_mutation_reader_v2 make_generating_reader_v1(schema_ptr s, reader_permit permit, noncopyable_function<future<mutation_fragment_opt> ()> get_next_fragment) {
    class adaptor {
        struct consumer {
            circular_buffer<mutation_fragment_v2>* buf;
            void operator()(mutation_fragment_v2&& mf) {
                buf->emplace_back(std::move(mf));
            }
        };
        std::unique_ptr<circular_buffer<mutation_fragment_v2>> _buffer;
        upgrading_consumer<consumer> _upgrading_consumer;
        noncopyable_function<future<mutation_fragment_opt> ()> _get_next_fragment;
    public:
        adaptor(schema_ptr schema, reader_permit permit, noncopyable_function<future<mutation_fragment_opt> ()> get_next_fragment)
            : _buffer(std::make_unique<circular_buffer<mutation_fragment_v2>>())
            , _upgrading_consumer(*schema, std::move(permit), consumer{_buffer.get()})
            , _get_next_fragment(std::move(get_next_fragment))
        { }
        future<mutation_fragment_v2_opt> operator()() {
            while (_buffer->empty()) {
                auto mfopt = co_await _get_next_fragment();
                if (!mfopt) {
                    break;
                }
                _upgrading_consumer.consume(std::move(*mfopt));
            }
            if (_buffer->empty()) {
                co_return std::nullopt;
            }
            auto mf = std::move(_buffer->front());
            _buffer->pop_front();
            co_return mf;
        }
    };
    return make_flat_mutation_reader_v2<generating_reader_v2>(s, permit, adaptor(s, permit, std::move(get_next_fragment)));
}

flat_mutation_reader_v2
make_flat_mutation_reader_from_mutations_v2(schema_ptr schema, reader_permit permit, std::vector<mutation> ms, const query::partition_slice& slice, streamed_mutation::forwarding fwd) {
    return make_flat_mutation_reader_from_mutations_v2(std::move(schema), std::move(permit), std::move(ms), query::full_partition_range, slice, fwd);
}

flat_mutation_reader_v2
make_flat_mutation_reader_from_mutations_v2(schema_ptr s, reader_permit permit, std::vector<mutation> mutations, const dht::partition_range& pr,
        const query::partition_slice& query_slice, streamed_mutation::forwarding fwd) {
    class reader final : public flat_mutation_reader_v2::impl {
        std::vector<mutation> _mutations;
        const dht::partition_range* _pr;
        bool _reversed;
        const dht::decorated_key* _dk = nullptr;
        std::optional<mutation_consume_cookie> _cookie;

    private:
        void maybe_emit_partition_start() {
            if (_dk) {
                consume(tombstone{}); // flush partition-start
            }
        }
    public:
        void consume_new_partition(const dht::decorated_key& dk) {
            _dk = &dk;
        }
        void consume(tombstone t) {
            push_mutation_fragment(*_schema, _permit, partition_start(*_dk, t));
            _dk = nullptr;
        }
        stop_iteration consume(static_row&& sr) {
            maybe_emit_partition_start();
            push_mutation_fragment(*_schema, _permit, std::move(sr));
            return stop_iteration(is_buffer_full());
        }
        stop_iteration consume(clustering_row&& cr) {
            maybe_emit_partition_start();
            push_mutation_fragment(*_schema, _permit, std::move(cr));
            return stop_iteration(is_buffer_full());
        }
        stop_iteration consume(range_tombstone_change&& rtc) {
            maybe_emit_partition_start();
            push_mutation_fragment(*_schema, _permit, std::move(rtc));
            return stop_iteration(is_buffer_full());
        }
        stop_iteration consume_end_of_partition() {
            if (is_buffer_full()) {
                return stop_iteration::yes;
            }
            maybe_emit_partition_start();
            push_mutation_fragment(*_schema, _permit, partition_end{});
            return stop_iteration::no;
        }
        void consume_end_of_stream() { }

    public:
        reader(schema_ptr schema, reader_permit permit, std::vector<mutation> mutations, const dht::partition_range& pr, bool reversed)
            : impl(std::move(schema), std::move(permit))
            , _mutations(std::move(mutations))
            , _pr(&pr)
            , _reversed(reversed)
        {
            std::reverse(_mutations.begin(), _mutations.end());
        }
        virtual future<> fill_buffer() override {
            if (_mutations.empty()) {
                _end_of_stream = true;
                return make_ready_future<>();
            }

            dht::ring_position_comparator cmp{*_schema};
            while (!_mutations.empty()) {
                auto& mut = _mutations.back();
                if (_pr->before(mut.decorated_key(), cmp)) {
                    _mutations.pop_back();
                    continue;
                }
                if (_pr->after(mut.decorated_key(), cmp)) {
                    _end_of_stream = true;
                    break;
                }
                if (!_cookie) {
                    _cookie.emplace();
                }
                auto res = std::move(mut).consume(*this, _reversed ? consume_in_reverse::yes : consume_in_reverse::no, std::move(*_cookie));
                if (res.stop == stop_iteration::yes) {
                    _cookie = std::move(res.cookie);
                    break;
                } else {
                    _cookie.reset();
                    _mutations.pop_back();
                }
            }
            return make_ready_future<>();
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty() && _cookie) {
                _cookie.reset();
                _mutations.pop_back();
            }
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            _cookie.reset();
            _pr = &pr;
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(position_range pr) override {
            return make_exception_future<>(std::bad_function_call{});
        }
        virtual future<> close() noexcept override {
            return make_ready_future<>();
        }
    };
    if (mutations.empty()) {
        return make_empty_flat_reader_v2(std::move(s), std::move(permit));
    }
    const auto reversed = query_slice.is_reversed();
    std::vector<mutation> sliced_mutations;
    if (reversed) {
        sliced_mutations = slice_mutations(s->make_reversed(), std::move(mutations), query::half_reverse_slice(*s, query_slice));
    } else {
        sliced_mutations = slice_mutations(s, std::move(mutations), query_slice);
    }
    auto res = make_flat_mutation_reader_v2<reader>(s, std::move(permit), std::move(sliced_mutations), pr, reversed);
    if (fwd) {
        return make_forwardable(std::move(res));
    }
    return res;
}

flat_mutation_reader_v2
make_flat_mutation_reader_from_mutations_v2(schema_ptr s, reader_permit permit, std::vector<mutation> mutations, const dht::partition_range& pr, streamed_mutation::forwarding fwd) {
    return make_flat_mutation_reader_from_mutations_v2(s, std::move(permit), std::move(mutations), pr, s->full_slice(), fwd);
}

flat_mutation_reader_v2 make_flat_mutation_reader_from_mutations_v2(
    schema_ptr schema,
    reader_permit permit,
    std::vector<mutation> ms,
    streamed_mutation::forwarding fwd) {
    return make_flat_mutation_reader_from_mutations_v2(std::move(schema), std::move(permit), std::move(ms), query::full_partition_range, fwd);
}

// All mutations should have the same schema.
flat_mutation_reader make_flat_mutation_reader_from_mutations(schema_ptr schema, reader_permit permit, std::vector<mutation> ms, streamed_mutation::forwarding fwd) {
    return make_flat_mutation_reader_from_mutations(std::move(schema), std::move(permit), std::move(ms), query::full_partition_range, fwd);
}

flat_mutation_reader
make_flat_mutation_reader_from_mutations(schema_ptr schema,
                                    reader_permit permit,
                                    std::vector<mutation> ms,
                                    const dht::partition_range& pr,
                                    const query::partition_slice& query_slice,
                                    streamed_mutation::forwarding fwd) {
    const auto reversed = query_slice.is_reversed();
    if (reversed) {
        schema = schema->make_reversed();
    }
    auto slice = reversed
            ? query::half_reverse_slice(*schema, query_slice)
            : query_slice;
    auto sliced_ms = slice_mutations(schema, std::move(ms), slice);
    auto rd = make_flat_mutation_reader_from_mutations(schema, permit, sliced_ms, pr, reversed ? streamed_mutation::forwarding::no : fwd);
    if (reversed) {
        rd = make_reversing_reader(std::move(rd), permit.max_result_size());
        if (fwd) {
            rd = make_forwardable(std::move(rd));
        }
    }
    return rd;
}

flat_mutation_reader
make_flat_mutation_reader_from_mutations(schema_ptr schema, reader_permit permit, std::vector<mutation> ms, const query::partition_slice& slice, streamed_mutation::forwarding fwd) {
    return make_flat_mutation_reader_from_mutations(std::move(schema), std::move(permit), std::move(ms), query::full_partition_range, slice, fwd);
}

flat_mutation_reader
make_flat_mutation_reader_from_mutations(schema_ptr s, reader_permit permit, std::vector<mutation> mutations, const dht::partition_range& pr, streamed_mutation::forwarding fwd) {
    class reader final : public flat_mutation_reader::impl {
        std::vector<mutation> _mutations;
        std::vector<mutation>::iterator _cur;
        std::vector<mutation>::iterator _end;
        position_in_partition::less_compare _cmp;
        bool _static_row_done = false;
        mutation_fragment_opt _rt;
        mutation_fragment_opt _cr;
    private:
        void prepare_next_clustering_row() {
            auto& crs = _cur->partition().mutable_clustered_rows();
            while (true) {
                auto re = crs.unlink_leftmost_without_rebalance();
                if (!re) {
                    break;
                }
                auto re_deleter = defer([re] () noexcept { current_deleter<rows_entry>()(re); });
                if (!re->dummy()) {
                    _cr = mutation_fragment(*_schema, _permit, std::move(*re));
                    break;
                }
            }
        }
        void prepare_next_range_tombstone() {
            auto& rts = _cur->partition().mutable_row_tombstones();
            if (!rts.empty()) {
                _rt = mutation_fragment(*_schema, _permit, rts.pop_front_and_lock());
            }
        }
        mutation_fragment_opt read_next() {
            if (_cr && (!_rt || _cmp(_cr->position(), _rt->position()))) {
                auto cr = std::exchange(_cr, { });
                prepare_next_clustering_row();
                return cr;
            } else if (_rt) {
                auto rt = std::exchange(_rt, { });
                prepare_next_range_tombstone();
                return rt;
            }
            return { };
        }
    private:
        void do_fill_buffer() {
            while (!is_end_of_stream() && !is_buffer_full()) {
                if (!_static_row_done) {
                    _static_row_done = true;
                    if (!_cur->partition().static_row().empty()) {
                        push_mutation_fragment(*_schema, _permit, static_row(std::move(_cur->partition().static_row().get_existing())));
                    }
                }
                auto mfopt = read_next();
                if (mfopt) {
                    push_mutation_fragment(std::move(*mfopt));
                } else {
                    push_mutation_fragment(*_schema, _permit, partition_end());
                    ++_cur;
                    if (_cur == _end) {
                        _end_of_stream = true;
                    } else {
                        start_new_partition();
                    }
                }
            }
        }
        void start_new_partition() {
            _static_row_done = false;
            push_mutation_fragment(*_schema, _permit, partition_start(_cur->decorated_key(),
                                                   _cur->partition().partition_tombstone()));

            prepare_next_clustering_row();
            prepare_next_range_tombstone();
        }
        void destroy_current_mutation() {
            auto &crs = _cur->partition().mutable_clustered_rows();
            auto deleter = current_deleter<rows_entry>();
            crs.clear_and_dispose(deleter);

            auto &rts = _cur->partition().mutable_row_tombstones();
            // cannot use .clear() here, pop_front_and_lock leaves
            // boost set in a sate that crashes if being cleared.
            while (!rts.empty()) {
                rts.pop_front_and_lock();
            }
        }
        struct cmp {
            bool operator()(const mutation& m, const dht::ring_position& p) const {
                return m.decorated_key().tri_compare(*m.schema(), p) < 0;
            }
            bool operator()(const dht::ring_position& p, const mutation& m) const {
                return m.decorated_key().tri_compare(*m.schema(), p) > 0;
            }
        };
        static std::vector<mutation>::iterator find_first_partition(std::vector<mutation>& ms, const dht::partition_range& pr) {
            if (!pr.start()) {
                return std::begin(ms);
            }
            if (pr.is_singular()) {
                return std::lower_bound(std::begin(ms), std::end(ms), pr.start()->value(), cmp{});
            } else {
                if (pr.start()->is_inclusive()) {
                    return std::lower_bound(std::begin(ms), std::end(ms), pr.start()->value(), cmp{});
                } else {
                    return std::upper_bound(std::begin(ms), std::end(ms), pr.start()->value(), cmp{});
                }
            }
        }
        static std::vector<mutation>::iterator find_last_partition(std::vector<mutation>& ms, const dht::partition_range& pr) {
            if (!pr.end()) {
                return std::end(ms);
            }
            if (pr.is_singular()) {
                return std::upper_bound(std::begin(ms), std::end(ms), pr.start()->value(), cmp{});
            } else {
                if (pr.end()->is_inclusive()) {
                    return std::upper_bound(std::begin(ms), std::end(ms), pr.end()->value(), cmp{});
                } else {
                    return std::lower_bound(std::begin(ms), std::end(ms), pr.end()->value(), cmp{});
                }
            }
        }
    public:
        reader(schema_ptr s, reader_permit permit, std::vector<mutation>&& mutations, const dht::partition_range& pr)
            : impl(s, std::move(permit))
            , _mutations(std::move(mutations))
            , _cur(find_first_partition(_mutations, pr))
            , _end(find_last_partition(_mutations, pr))
            , _cmp(*s)
        {
            _end_of_stream = _cur == _end;
            if (!_end_of_stream) {
                auto mutation_destroyer = defer([this] () noexcept { destroy_mutations(); });
                start_new_partition();

                do_fill_buffer();

                mutation_destroyer.cancel();
            }
        }
        void destroy_mutations() noexcept {
            // After unlink_leftmost_without_rebalance() was called on a bi::set
            // we need to complete destroying the tree using that function.
            // clear_and_dispose() used by mutation_partition destructor won't
            // work properly.

            _cur = _mutations.begin();
            while (_cur != _end) {
                destroy_current_mutation();
                ++_cur;
            }
        }
        ~reader() {
            destroy_mutations();
        }
        virtual future<> fill_buffer() override {
            do_fill_buffer();
            return make_ready_future<>();
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty() && !is_end_of_stream()) {
                destroy_current_mutation();
                ++_cur;
                if (_cur == _end) {
                    _end_of_stream = true;
                } else {
                    start_new_partition();
                }
            }
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _cur = find_first_partition(_mutations, pr);
            _end = find_last_partition(_mutations, pr);
            _static_row_done = false;
            _cr = {};
            _rt = {};
            _end_of_stream = _cur == _end;
            if (!_end_of_stream) {
                start_new_partition();
            }
            return make_ready_future<>();
        };
        virtual future<> fast_forward_to(position_range cr) override {
            return make_exception_future<>(std::runtime_error("This reader can't be fast forwarded to another position."));
        };
        virtual future<> close() noexcept override {
            return make_ready_future<>();
        };
    };
    if (mutations.empty()) {
        return make_empty_flat_reader(std::move(s), std::move(permit));
    }
    auto res = make_flat_mutation_reader<reader>(std::move(s), std::move(permit), std::move(mutations), pr);
    if (fwd) {
        return make_forwardable(std::move(res));
    }
    return res;
}

flat_mutation_reader_v2
make_flat_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments) {
    return make_flat_mutation_reader_from_fragments(std::move(schema), std::move(permit), std::move(fragments), query::full_partition_range);
}

flat_mutation_reader_v2
make_flat_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments, const dht::partition_range& pr) {
    class reader : public flat_mutation_reader_v2::impl {
        std::deque<mutation_fragment_v2> _fragments;
        const dht::partition_range* _pr;
        dht::ring_position_comparator _cmp;

    private:
        bool end_of_range() const {
            return _fragments.empty() ||
                (_fragments.front().is_partition_start() && _pr->after(_fragments.front().as_partition_start().key(), _cmp));
        }

        void do_fast_forward_to(const dht::partition_range& pr) {
            clear_buffer();
            _pr = &pr;
            _fragments.erase(_fragments.begin(), std::find_if(_fragments.begin(), _fragments.end(), [this] (const mutation_fragment_v2& mf) {
                return mf.is_partition_start() && !_pr->before(mf.as_partition_start().key(), _cmp);
            }));
            _end_of_stream = end_of_range();
        }

    public:
        reader(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments, const dht::partition_range& pr)
                : flat_mutation_reader_v2::impl(std::move(schema), std::move(permit))
                , _fragments(std::move(fragments))
                , _pr(&pr)
                , _cmp(*_schema) {
            do_fast_forward_to(*_pr);
        }
        virtual future<> fill_buffer() override {
            while (!(_end_of_stream = end_of_range()) && !is_buffer_full()) {
                push_mutation_fragment(std::move(_fragments.front()));
                _fragments.pop_front();
            }
            return make_ready_future<>();
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
                while (!(_end_of_stream = end_of_range()) && !_fragments.front().is_partition_start()) {
                    _fragments.pop_front();
                }
            }
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(position_range pr) override {
            throw std::runtime_error("This reader can't be fast forwarded to another range.");
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            do_fast_forward_to(pr);
            return make_ready_future<>();
        }
        virtual future<> close() noexcept override {
            return make_ready_future<>();
        }
    };
    return make_flat_mutation_reader_v2<reader>(std::move(schema), std::move(permit), std::move(fragments), pr);
}

std::deque<mutation_fragment_v2> reverse_fragments(const schema& schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments) {
    std::deque<mutation_fragment_v2> reversed_fragments;

    auto it = fragments.begin();
    auto end = fragments.end();

    while (it != end) {
        while (it != end && it->position().region() != partition_region::clustered) {
            reversed_fragments.push_back(std::move(*it++));
        }
        // We need to find a partition-end but let's be flexible (tests will sometime use incorrect streams).
        auto partition_end_it = std::find_if(it, end, [] (auto& mf) { return mf.position().region() != partition_region::clustered; });
        auto rit = std::make_reverse_iterator(partition_end_it);
        const auto rend = std::make_reverse_iterator(it);
        for (; rit != rend; ++rit) {
            if (rit->is_range_tombstone_change()) {
                auto next_rtc_it = std::find_if(rit + 1, rend, std::mem_fn(&mutation_fragment_v2::is_range_tombstone_change));
                if (next_rtc_it == rend) {
                    reversed_fragments.emplace_back(schema, permit, range_tombstone_change(rit->position().reversed(), {}));
                } else {
                    reversed_fragments.emplace_back(schema, permit, range_tombstone_change(rit->position().reversed(), next_rtc_it->as_range_tombstone_change().tombstone()));
                }
            } else {
                reversed_fragments.push_back(std::move(*rit));
            }
        }
        it = partition_end_it;
    }

    return reversed_fragments;
}

flat_mutation_reader_v2
make_flat_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments,
        const dht::partition_range& pr, const query::partition_slice& query_slice) {
    const auto reversed = query_slice.is_reversed();
    if (reversed) {
        fragments = reverse_fragments(*schema, permit, std::move(fragments));
    }
    auto slice = reversed ? query::legacy_reverse_slice_to_native_reverse_slice(*schema, query_slice) : query_slice;

    std::optional<clustering_ranges_walker> ranges_walker;
    std::deque<mutation_fragment_v2> filtered;
    std::optional<range_tombstone_change> activa_tombstone;
    for (auto&& mf : fragments) {
        clustering_ranges_walker::progress p{.contained = true};

        switch (mf.mutation_fragment_kind()) {
            case mutation_fragment_v2::kind::partition_start:
                ranges_walker.emplace(*schema, slice.row_ranges(*schema, mf.as_partition_start().key().key()), false);
                [[fallthrough]];
            case mutation_fragment_v2::kind::static_row:
                break;
            case mutation_fragment_v2::kind::clustering_row:
                p = ranges_walker->advance_to(mf.position(), ranges_walker->current_tombstone());
                break;
            case mutation_fragment_v2::kind::range_tombstone_change:
                p = ranges_walker->advance_to(mf.position(), mf.as_range_tombstone_change().tombstone());
                p.contained = false;
                break;
            case mutation_fragment_v2::kind::partition_end:
                p = ranges_walker->advance_to(mf.position(), ranges_walker->current_tombstone());
                p.contained = true;
                break;
        }

        for (auto&& rt : p.rts) {
            filtered.emplace_back(*schema, permit, std::move(rt));
        }
        if (p.contained) {
            filtered.push_back(std::move(mf));
        }
    }
    return make_flat_mutation_reader_from_fragments(std::move(schema), permit, std::move(filtered), pr);
}

flat_mutation_reader downgrade_to_v1(flat_mutation_reader_v2 r) {
    class transforming_reader : public flat_mutation_reader::impl {
        flat_mutation_reader_v2 _reader;
        struct consumer {
            transforming_reader* _owner;
            stop_iteration operator()(mutation_fragment_v2&& mf) {
                std::move(mf).consume(*_owner);
                return stop_iteration(_owner->is_buffer_full());
            }
        };
        range_tombstone_assembler _rt_assembler;
    public:
        void consume(static_row mf) {
            push_mutation_fragment(*_schema, _permit, std::move(mf));
        }
        void consume(clustering_row mf) {
            if (_rt_assembler.needs_flush()) {
                if (auto rt_opt = _rt_assembler.flush(*_schema, position_in_partition::after_key(mf.position()))) {
                    push_mutation_fragment(*_schema, _permit, std::move(*rt_opt));
                }
            }
            push_mutation_fragment(*_schema, _permit, std::move(mf));
        }
        void consume(range_tombstone_change mf) {
            if (auto rt_opt = _rt_assembler.consume(*_schema, std::move(mf))) {
                push_mutation_fragment(*_schema, _permit, std::move(*rt_opt));
            }
        }
        void consume(partition_start mf) {
            _rt_assembler.reset();
            push_mutation_fragment(*_schema, _permit, std::move(mf));
        }
        void consume(partition_end mf) {
            _rt_assembler.on_end_of_stream();
            push_mutation_fragment(*_schema, _permit, std::move(mf));
        }
        transforming_reader(flat_mutation_reader_v2&& r)
                : impl(r.schema(), r.permit())
                , _reader(std::move(r))
        {}
        virtual flat_mutation_reader_v2* get_original() override {
            if (is_buffer_empty() && _rt_assembler.discardable()) {
                return &_reader;
            }
            return nullptr;
        }
        virtual future<> fill_buffer() override {
            if (_end_of_stream) {
                return make_ready_future<>();
            }
            return _reader.consume_pausable(consumer{this}).then([this] {
                if (_reader.is_end_of_stream()) {
                    _rt_assembler.on_end_of_stream();
                    _end_of_stream = true;
                }
            });
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();

            if (is_buffer_empty()) {
                // If there is an active tombstone at the moment of doing next_partition(), simply clearing the buffer
                // means that the closing range_tombstone_change will not be processed by the assembler. This violates
                // an assumption of the assembler. Since the client executed next_partition(), they aren't interested
                // in currently active range_tombstone_change (if any). Therefore in both cases, next partition should
                // start with the assembler state reset. This is relevant only if the the producer still hasn't
                // advanced to a next partition.
                _rt_assembler.reset();
                _end_of_stream = false;
                return _reader.next_partition();
            }
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();

            // As in next_partition(), current partitions' state of having an active range tombstone is irrelevant for the
            // partition range that we are forwarding to. Here, it is guaranteed that is_buffer_empty().
            _rt_assembler.reset();
            _end_of_stream = false;
            return _reader.fast_forward_to(pr);
        }
        virtual future<> fast_forward_to(position_range pr) override {
            clear_buffer();

            // It is guaranteed that at the beginning of `pr`, all the `range_tombstone`s active at the beginning of `pr`
            // will have emitted appropriate range_tombstone_change. Therefore we can simply reset the state of assembler.
            // Here, it is guaranteed that is_buffer_empty().
            _rt_assembler.reset();
            _end_of_stream = false;
            return _reader.fast_forward_to(std::move(pr));
        }
        virtual future<> close() noexcept override {
            return _reader.close();
        }
    };

    if (auto original_opt = r.get_original()) {
        auto original = std::move(*original_opt);
        r._impl.reset();
        return original;
    } else {
        return make_flat_mutation_reader<transforming_reader>(std::move(r));
    }
}

flat_mutation_reader_v2 upgrade_to_v2(flat_mutation_reader r) {
    class transforming_reader : public flat_mutation_reader_v2::impl {
        flat_mutation_reader _reader;
        struct consumer {
            transforming_reader* _owner;
            stop_iteration operator()(mutation_fragment&& mf) {
                std::move(mf).consume(_owner->_upgrading_consumer);
                return stop_iteration(_owner->is_buffer_full());
            }
        };
        struct mutation_fragment_pusher {
            transforming_reader* owner;
            void operator()(mutation_fragment_v2&& mf) {
                owner->push_mutation_fragment(std::move(mf));
            }
        };
        upgrading_consumer<mutation_fragment_pusher> _upgrading_consumer;
    public:
        explicit transforming_reader(flat_mutation_reader&& reader)
            : impl(reader.schema(), reader.permit())
            , _reader(std::move(reader))
            , _upgrading_consumer(*_schema, _permit, mutation_fragment_pusher{this})
        { }
        virtual flat_mutation_reader* get_original() override {
            if (is_buffer_empty() && _upgrading_consumer.discardable()) {
                return &_reader;
            }
            return nullptr;
        }
        virtual future<> fill_buffer() override {
            if (_end_of_stream) {
                return make_ready_future<>();
            }
            return _reader.consume_pausable(consumer{this}).then([this] {
                if (_reader.is_end_of_stream() && _reader.is_buffer_empty()) {
                    _upgrading_consumer.on_end_of_stream();
                    _end_of_stream = true;
                }
            });
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
                _end_of_stream = false;
                return _reader.next_partition();
            }
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            return _reader.fast_forward_to(pr);
        }
        virtual future<> fast_forward_to(position_range pr) override {
            clear_buffer();
            // r is used to trim range tombstones and range_tombstone:s can be trimmed only to positions
            // which are !is_clustering_row(). Replace with equivalent ranges.
            // Long-term we should guarantee this on position_range.
            if (pr.start().is_clustering_row()) {
                pr.set_start(position_in_partition::before_key(pr.start().key()));
            }
            if (pr.end().is_clustering_row()) {
                pr.set_end(position_in_partition::before_key(pr.end().key()));
            }
            _upgrading_consumer.set_position_range(pr);
            _end_of_stream = false;
            return _reader.fast_forward_to(std::move(pr));
        }
        virtual future<> close() noexcept override {
            return _reader.close();
        }
    };

    if (auto original_opt = r.get_original()) {
        auto original = std::move(*original_opt);
        r._impl.reset();
        return original;
    } else {
        return make_flat_mutation_reader_v2<transforming_reader>(std::move(r));
    }
}

flat_mutation_reader_v2 make_next_partition_adaptor(flat_mutation_reader_v2&& rd) {
    class adaptor : public flat_mutation_reader_v2::impl {
        flat_mutation_reader_v2 _underlying;
    public:
        adaptor(flat_mutation_reader_v2 underlying) : impl(underlying.schema(), underlying.permit()), _underlying(std::move(underlying))
        { }
        virtual future<> fill_buffer() override {
            co_await _underlying.fill_buffer();
            _underlying.move_buffer_content_to(*this);
            _end_of_stream = _underlying.is_end_of_stream();
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) {
                co_return;
            }
            auto* next = co_await _underlying.peek();
            while (next && !next->is_partition_start()) {
                co_await _underlying();
                next = co_await _underlying.peek();
            }
        }
        virtual future<> fast_forward_to(const dht::partition_range&) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }
        virtual future<> fast_forward_to(position_range) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }
        virtual future<> close() noexcept override {
            return _underlying.close();
        }
    };
    return make_flat_mutation_reader_v2<adaptor>(std::move(rd));
}

snapshot_source make_empty_snapshot_source() {
    return snapshot_source([] {
        return make_empty_mutation_source();
    });
}

mutation_source make_empty_mutation_source() {
    return mutation_source([](schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding) {
        return make_empty_flat_reader(s, std::move(permit));
    }, [] {
        return [] (const dht::decorated_key& key) {
            return partition_presence_checker_result::definitely_doesnt_exist;
        };
    });
}

mutation_source make_combined_mutation_source(std::vector<mutation_source> addends) {
    return mutation_source([addends = std::move(addends)] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd_sm,
            mutation_reader::forwarding fwd_mr) {
        std::vector<flat_mutation_reader_v2> rd;
        rd.reserve(addends.size());
        for (auto&& ms : addends) {
            rd.emplace_back(ms.make_reader_v2(s, permit, pr, slice, pc, tr, fwd_sm, fwd_mr));
        }
        return make_combined_reader(s, std::move(permit), std::move(rd), fwd_sm, fwd_mr);
    });
}

class queue_reader final : public flat_mutation_reader::impl {
    friend class queue_reader_handle;

private:
    queue_reader_handle* _handle = nullptr;
    std::optional<promise<>> _not_full;
    std::optional<promise<>> _full;
    std::exception_ptr _ex;

private:
    void push_and_maybe_notify(mutation_fragment&& mf) {
        push_mutation_fragment(std::move(mf));
        if (_full && is_buffer_full()) {
            _full->set_value();
            _full.reset();
        }
    }

public:
    explicit queue_reader(schema_ptr s, reader_permit permit)
        : impl(std::move(s), std::move(permit)) {
    }
    virtual future<> fill_buffer() override {
        if (_ex) {
            return make_exception_future<>(_ex);
        }
        if (_end_of_stream || !is_buffer_empty()) {
            return make_ready_future<>();
        }
        if (_not_full) {
            _not_full->set_value();
            _not_full.reset();
        }
        _full.emplace();
        return _full->get_future();
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            return fill_buffer().then([this] {
                return next_partition();
            });
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(position_range) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        // wake up any waiters to prevent broken_promise errors
        if (_full) {
            _full->set_value();
            _full.reset();
        } else if (_not_full) {
            _not_full->set_value();
            _not_full.reset();
        }
        // detach from the queue_reader_handle
        // since it should never access the reader after close.
        if (_handle) {
            _handle->_reader = nullptr;
            _handle = nullptr;
        }
        return make_ready_future<>();
    }
    future<> push(mutation_fragment&& mf) {
        push_and_maybe_notify(std::move(mf));
        if (!is_buffer_full()) {
            return make_ready_future<>();
        }
        _not_full.emplace();
        return _not_full->get_future();
    }
    void push_end_of_stream() {
        _end_of_stream = true;
        if (_full) {
            _full->set_value();
            _full.reset();
        }
    }
    void abort(std::exception_ptr ep) noexcept {
        _ex = std::move(ep);
        if (_full) {
            _full->set_exception(_ex);
            _full.reset();
        } else if (_not_full) {
            _not_full->set_exception(_ex);
            _not_full.reset();
        }
    }
};

void queue_reader_handle::abandon() noexcept {
    std::exception_ptr ex;
    try {
        ex = std::make_exception_ptr<std::runtime_error>(std::runtime_error("Abandoned queue_reader_handle"));
    } catch (...) {
        ex = std::current_exception();
    }
    abort(std::move(ex));
}

queue_reader_handle::queue_reader_handle(queue_reader& reader) noexcept : _reader(&reader) {
    _reader->_handle = this;
}

queue_reader_handle::queue_reader_handle(queue_reader_handle&& o) noexcept
        : _reader(std::exchange(o._reader, nullptr))
        , _ex(std::exchange(o._ex, nullptr))
{
    if (_reader) {
        _reader->_handle = this;
    }
}

queue_reader_handle::~queue_reader_handle() {
    abandon();
}

queue_reader_handle& queue_reader_handle::operator=(queue_reader_handle&& o) {
    abandon();
    _reader = std::exchange(o._reader, nullptr);
    _ex = std::exchange(o._ex, {});
    if (_reader) {
        _reader->_handle = this;
    }
    return *this;
}

future<> queue_reader_handle::push(mutation_fragment mf) {
    if (!_reader) {
        if (_ex) {
            return make_exception_future<>(_ex);
        }
        return make_exception_future<>(std::runtime_error("Dangling queue_reader_handle"));
    }
    return _reader->push(std::move(mf));
}

void queue_reader_handle::push_end_of_stream() {
    if (!_reader) {
        throw std::runtime_error("Dangling queue_reader_handle");
    }
    _reader->push_end_of_stream();
    _reader->_handle = nullptr;
    _reader = nullptr;
}

bool queue_reader_handle::is_terminated() const {
    return _reader == nullptr;
}

void queue_reader_handle::abort(std::exception_ptr ep) {
    _ex = std::move(ep);
    if (_reader) {
        _reader->abort(_ex);
        _reader->_handle = nullptr;
        _reader = nullptr;
    }
}

std::exception_ptr queue_reader_handle::get_exception() const noexcept {
    return _ex;
}

std::pair<flat_mutation_reader, queue_reader_handle> make_queue_reader(schema_ptr s, reader_permit permit) {
    auto impl = std::make_unique<queue_reader>(std::move(s), std::move(permit));
    auto handle = queue_reader_handle(*impl);
    return {flat_mutation_reader(std::move(impl)), std::move(handle)};
}

class queue_reader_v2 final : public flat_mutation_reader_v2::impl {
    friend class queue_reader_handle_v2;

private:
    queue_reader_handle_v2* _handle = nullptr;
    std::optional<promise<>> _not_full;
    std::optional<promise<>> _full;
    std::exception_ptr _ex;

private:
    void push_and_maybe_notify(mutation_fragment_v2&& mf) {
        push_mutation_fragment(std::move(mf));
        if (_full && is_buffer_full()) {
            _full->set_value();
            _full.reset();
        }
    }

public:
    explicit queue_reader_v2(schema_ptr s, reader_permit permit)
        : impl(std::move(s), std::move(permit)) {
    }
    virtual future<> fill_buffer() override {
        if (_ex) {
            return make_exception_future<>(_ex);
        }
        if (_end_of_stream || !is_buffer_empty()) {
            return make_ready_future<>();
        }
        if (_not_full) {
            _not_full->set_value();
            _not_full.reset();
        }
        _full.emplace();
        return _full->get_future();
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            return fill_buffer().then([this] {
                return next_partition();
            });
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(position_range) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        // wake up any waiters to prevent broken_promise errors
        if (_full) {
            _full->set_value();
            _full.reset();
        } else if (_not_full) {
            _not_full->set_value();
            _not_full.reset();
        }
        // detach from the queue_reader_handle
        // since it should never access the reader after close.
        if (_handle) {
            _handle->_reader = nullptr;
            _handle = nullptr;
        }
        return make_ready_future<>();
    }
    future<> push(mutation_fragment_v2&& mf) {
        push_and_maybe_notify(std::move(mf));
        if (!is_buffer_full()) {
            return make_ready_future<>();
        }
        _not_full.emplace();
        return _not_full->get_future();
    }
    void push_end_of_stream() {
        _end_of_stream = true;
        if (_full) {
            _full->set_value();
            _full.reset();
        }
    }
    void abort(std::exception_ptr ep) noexcept {
        _ex = std::move(ep);
        if (_full) {
            _full->set_exception(_ex);
            _full.reset();
        } else if (_not_full) {
            _not_full->set_exception(_ex);
            _not_full.reset();
        }
    }
};

void queue_reader_handle_v2::abandon() noexcept {
    std::exception_ptr ex;
    try {
        ex = std::make_exception_ptr<std::runtime_error>(std::runtime_error("Abandoned queue_reader_handle_v2"));
    } catch (...) {
        ex = std::current_exception();
    }
    abort(std::move(ex));
}

queue_reader_handle_v2::queue_reader_handle_v2(queue_reader_v2& reader) noexcept : _reader(&reader) {
    _reader->_handle = this;
}

queue_reader_handle_v2::queue_reader_handle_v2(queue_reader_handle_v2&& o) noexcept
        : _reader(std::exchange(o._reader, nullptr))
        , _ex(std::exchange(o._ex, nullptr))
{
    if (_reader) {
        _reader->_handle = this;
    }
}

queue_reader_handle_v2::~queue_reader_handle_v2() {
    abandon();
}

queue_reader_handle_v2& queue_reader_handle_v2::operator=(queue_reader_handle_v2&& o) {
    abandon();
    _reader = std::exchange(o._reader, nullptr);
    _ex = std::exchange(o._ex, {});
    if (_reader) {
        _reader->_handle = this;
    }
    return *this;
}

future<> queue_reader_handle_v2::push(mutation_fragment_v2 mf) {
    if (!_reader) {
        if (_ex) {
            return make_exception_future<>(_ex);
        }
        return make_exception_future<>(std::runtime_error("Dangling queue_reader_handle_v2"));
    }
    return _reader->push(std::move(mf));
}

void queue_reader_handle_v2::push_end_of_stream() {
    if (!_reader) {
        throw std::runtime_error("Dangling queue_reader_handle_v2");
    }
    _reader->push_end_of_stream();
    _reader->_handle = nullptr;
    _reader = nullptr;
}

bool queue_reader_handle_v2::is_terminated() const {
    return _reader == nullptr;
}

void queue_reader_handle_v2::abort(std::exception_ptr ep) {
    _ex = std::move(ep);
    if (_reader) {
        _reader->abort(_ex);
        _reader->_handle = nullptr;
        _reader = nullptr;
    }
}

std::exception_ptr queue_reader_handle_v2::get_exception() const noexcept {
    return _ex;
}

std::pair<flat_mutation_reader_v2, queue_reader_handle_v2> make_queue_reader_v2(schema_ptr s, reader_permit permit) {
    auto impl = std::make_unique<queue_reader_v2>(std::move(s), std::move(permit));
    auto handle = queue_reader_handle_v2(*impl);
    return {flat_mutation_reader_v2(std::move(impl)), std::move(handle)};
}

namespace {

class compacting_reader : public flat_mutation_reader_v2::impl {
    friend class compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::yes>;

private:
    flat_mutation_reader_v2 _reader;
    compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::yes> _compactor;
    noop_compacted_fragments_consumer _gc_consumer;

    // Uncompacted stream
    partition_start _last_uncompacted_partition_start;
    mutation_fragment_v2::kind _last_uncompacted_kind = mutation_fragment_v2::kind::partition_end;

    // Compacted stream
    bool _has_compacted_partition_start = false;
    bool _ignore_partition_end = false;

private:
    void maybe_push_partition_start() {
        if (_has_compacted_partition_start) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(_last_uncompacted_partition_start)));
            _has_compacted_partition_start = false;
        }
    }
    void maybe_inject_partition_end() {
        // The compactor needs a valid stream, but downstream doesn't care about
        // the injected partition end, so ignore it.
        if (_last_uncompacted_kind != mutation_fragment_v2::kind::partition_end) {
            _ignore_partition_end = true;
            _compactor.consume_end_of_partition(*this, _gc_consumer);
            _ignore_partition_end = false;
        }
    }
    void consume_new_partition(const dht::decorated_key& dk) {
        _has_compacted_partition_start = true;
        // We need to reset the partition's tombstone here. If the tombstone is
        // compacted away, `consume(tombstone)` below is simply not called. If
        // it is not compacted away, `consume(tombstone)` below will restore it.
        _last_uncompacted_partition_start.partition_tombstone() = {};
    }
    void consume(tombstone t) {
        _last_uncompacted_partition_start.partition_tombstone() = t;
        maybe_push_partition_start();
    }
    stop_iteration consume(static_row&& sr, tombstone, bool) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        // The compactor will close the active tombstone (if any) on partition
        // end. We ignore this when we don't care about the partition-end.
        if (_ignore_partition_end) {
            return stop_iteration::no;
        }
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rtc)));
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        maybe_push_partition_start();
        if (!_ignore_partition_end) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end{}));
        }
        return stop_iteration::no;
    }
    void consume_end_of_stream() {
    }
    streamed_mutation::forwarding _fwd;

public:
    compacting_reader(flat_mutation_reader_v2 source, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no)
        : impl(source.schema(), source.permit())
        , _reader(std::move(source))
        , _compactor(*_schema, compaction_time, get_max_purgeable)
        , _last_uncompacted_partition_start(dht::decorated_key(dht::minimum_token(), partition_key::make_empty()), tombstone{})
        , _fwd(fwd) {
    }
    virtual future<> fill_buffer() override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return _reader.fill_buffer().then([this] {
                if (_reader.is_buffer_empty()) {
                    _end_of_stream = _reader.is_end_of_stream();
                    if (_end_of_stream && _fwd) {
                        maybe_push_partition_start();
                    }
                }
                // It is important to not consume more than we actually need.
                // Doing so leads to corner cases around `next_partition()`. The
                // fragments consumed after our buffer is full might not be
                // emitted by the compactor, so on a following `next_partition()`
                // call we won't be able to determine whether we are at a
                // partition boundary or not and thus whether we need to forward
                // it to the underlying reader or not.
                // This problem doesn't exist when we want more fragments, in this
                // case we'll keep reading until the compactor emits something or
                // we read EOS, and thus we'll know where we are.
                while (!_reader.is_buffer_empty() && !is_buffer_full()) {
                    auto mf = _reader.pop_mutation_fragment();
                    _last_uncompacted_kind = mf.mutation_fragment_kind();
                    switch (mf.mutation_fragment_kind()) {
                    case mutation_fragment_v2::kind::static_row:
                        _compactor.consume(std::move(mf).as_static_row(), *this, _gc_consumer);
                        break;
                    case mutation_fragment_v2::kind::clustering_row:
                        _compactor.consume(std::move(mf).as_clustering_row(), *this, _gc_consumer);
                        break;
                    case mutation_fragment_v2::kind::range_tombstone_change:
                        _compactor.consume(std::move(mf).as_range_tombstone_change(), *this, _gc_consumer);
                        break;
                    case mutation_fragment_v2::kind::partition_start:
                        _last_uncompacted_partition_start = std::move(mf).as_partition_start();
                        _compactor.consume_new_partition(_last_uncompacted_partition_start.key());
                        if (_last_uncompacted_partition_start.partition_tombstone()) {
                            _compactor.consume(_last_uncompacted_partition_start.partition_tombstone(), *this, _gc_consumer);
                        }
                        if (_fwd) {
                            _compactor.force_partition_not_empty(*this);
                        }
                        break;
                    case mutation_fragment_v2::kind::partition_end:
                        _compactor.consume_end_of_partition(*this, _gc_consumer);
                        break;
                    }
                }
            });
        });
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            return make_ready_future<>();
        }
        _end_of_stream = false;
        maybe_inject_partition_end();
        return _reader.next_partition();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        maybe_inject_partition_end();
        return _reader.fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        forward_buffer_to(pr.start());
        _end_of_stream = false;
        return _reader.fast_forward_to(std::move(pr));
    }
    virtual future<> close() noexcept override {
        return _reader.close();
    }
};

} // anonymous namespace

flat_mutation_reader_v2 make_compacting_reader(flat_mutation_reader_v2 source, gc_clock::time_point compaction_time,
        std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable, streamed_mutation::forwarding fwd) {
    return make_flat_mutation_reader_v2<compacting_reader>(std::move(source), compaction_time, get_max_purgeable, fwd);
}
