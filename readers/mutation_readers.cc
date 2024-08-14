/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "clustering_key_filter.hh"
#include "clustering_ranges_walker.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition.hh"
#include "mutation/mutation_compactor.hh"
#include "mutation/range_tombstone_assembler.hh"
#include "mutation/range_tombstone_splitter.hh"
#include "readers/combined.hh"
#include "readers/delegating_v2.hh"
#include "readers/empty_v2.hh"
#include "readers/mutation_reader.hh"
#include "readers/forwardable_v2.hh"
#include "readers/from_fragments_v2.hh"
#include "readers/from_mutations_v2.hh"
#include "readers/generating_v2.hh"
#include "readers/multi_range.hh"
#include "readers/mutation_source.hh"
#include "readers/nonforwardable.hh"
#include "readers/queue.hh"
#include "readers/reversing_v2.hh"
#include "readers/upgrading_consumer.hh"
#include "tombstone_gc.hh"
#include <seastar/core/coroutine.hh>
#include <stack>

extern logging::logger mrlog;

mutation_reader make_delegating_reader(mutation_reader& r) {
    return make_mutation_reader<delegating_reader_v2>(r);
}

namespace {
class partition_slicer {
public:
    using fragment_consumer = std::function<void(mutation_fragment_v2)>;
private:
    schema_ptr _schema;
    reader_permit _permit;
    clustering_ranges_walker _ranges_walker;
    fragment_consumer _consume;
    clustering_ranges_walker::progress _p{.contained = true};
public:
    partition_slicer(schema_ptr schema, reader_permit permit, const query::clustering_row_ranges& row_ranges, fragment_consumer consume)
        : _schema(schema)
        , _permit(permit)
        , _ranges_walker(*schema, row_ranges, false)
        , _consume(std::move(consume)) { }

    void apply(mutation_fragment_v2 mf) {
        switch (mf.mutation_fragment_kind()) {
            case mutation_fragment_v2::kind::partition_start:
                // can't happen
                SCYLLA_ASSERT(false);
                break;
            case mutation_fragment_v2::kind::static_row:
                break;
            case mutation_fragment_v2::kind::clustering_row:
                _p = _ranges_walker.advance_to(mf.position(), _ranges_walker.current_tombstone());
                break;
            case mutation_fragment_v2::kind::range_tombstone_change:
                _p = _ranges_walker.advance_to(mf.position(), mf.as_range_tombstone_change().tombstone());
                _p.contained = false;
                break;
            case mutation_fragment_v2::kind::partition_end:
                _p = _ranges_walker.advance_to(mf.position(), _ranges_walker.current_tombstone());
                _p.contained = true;
                break;
        }

        for (auto&& rt : _p.rts) {
            _consume(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
        if (_p.contained) {
            _consume(std::move(mf));
        }
    }
};
} //anon namespace

class empty_flat_reader_v2 final : public mutation_reader::impl {
public:
    empty_flat_reader_v2(schema_ptr s, reader_permit permit) : impl(std::move(s), std::move(permit)) { _end_of_stream = true; }
    virtual future<> fill_buffer() override { return make_ready_future<>(); }
    virtual future<> next_partition() override { return make_ready_future<>(); }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override { return make_ready_future<>(); };
    virtual future<> fast_forward_to(position_range cr) override { return make_ready_future<>(); };
    virtual future<> close() noexcept override { return make_ready_future<>(); }
};

mutation_reader make_empty_flat_reader_v2(schema_ptr s, reader_permit permit) {
    return make_mutation_reader<empty_flat_reader_v2>(std::move(s), std::move(permit));
}

mutation_reader make_forwardable(mutation_reader m) {
    class reader : public mutation_reader::impl {
        mutation_reader _underlying;
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
                push_mutation_fragment(*_schema, _permit, range_tombstone_change(position_in_partition_view::before_key(_current.end()), {}));
            }
        }
    public:
        reader(mutation_reader r) : impl(r.schema(), r.permit()), _underlying(std::move(r)), _current({
            position_in_partition::for_partition_start(),
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
            clear_buffer();
            return make_ready_future<>();
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) {
                co_return;
            }
            _end_of_stream = false;
            if (!_next || !_next->is_partition_start()) {
                co_await _underlying.next_partition();
                _next = {};
            }
            _current = {
                position_in_partition::for_partition_start(),
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
                position_in_partition::for_partition_start(),
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
    return make_mutation_reader<reader>(std::move(m));
}

mutation_reader make_slicing_filtering_reader(mutation_reader rd, const dht::partition_range& pr, const query::partition_slice& slice) {
    class reader : public mutation_reader::impl {
        mutation_reader _rd;
        const dht::partition_range* _pr;
        const query::partition_slice* _slice;
        dht::ring_position_comparator _cmp;
        std::optional<partition_slicer> _partition_slicer;

    public:
        reader(mutation_reader rd, const dht::partition_range& pr, const query::partition_slice& slice)
            : mutation_reader::impl(rd.schema(), rd.permit())
            , _rd(std::move(rd))
            , _pr(&pr)
            , _slice(&slice)
            , _cmp(*_schema) {
        }

        virtual future<> fill_buffer() override {
            while (!is_buffer_full() && !is_end_of_stream()) {
                co_await _rd.fill_buffer();
                while (!_rd.is_buffer_empty()) {
                    auto mf = _rd.pop_mutation_fragment();
                    switch (mf.mutation_fragment_kind()) {
                        case mutation_fragment_v2::kind::partition_start: {
                            auto& dk = mf.as_partition_start().key();
                            if (!_pr->contains(dk, _cmp)) {
                                co_return co_await _rd.next_partition();
                            }
                            _partition_slicer.emplace(_schema, _permit, _slice->row_ranges(*_schema, dk.key()),
                                                      [this] (mutation_fragment_v2 mf) {
                                                          push_mutation_fragment(std::move(mf));
                                                      });
                            push_mutation_fragment(std::move(mf));
                            break;
                        }

                        default:
                            _partition_slicer->apply(std::move(mf));
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
            clear_buffer();
            _end_of_stream = false;
            return _rd.fast_forward_to(std::move(pr));
        }

        virtual future<> close() noexcept override {
            return _rd.close();
        }
    };

    return make_mutation_reader<reader>(std::move(rd), pr, slice);
}

static mutation slice_mutation(schema_ptr schema, mutation&& m, const query::partition_slice& slice) {
    auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*schema, slice, m.key());
    auto&& mp = mutation_partition(std::move(m.partition()), *m.schema(), std::move(ck_ranges));
    return mutation(schema, m.decorated_key(), std::move(mp));
}

static std::vector<mutation> slice_mutations(schema_ptr schema, std::vector<mutation> ms, const query::partition_slice& slice) {
    std::vector<mutation> sliced_ms;
    sliced_ms.reserve(ms.size());
    for (auto& m : ms) {
        sliced_ms.emplace_back(slice_mutation(schema, std::move(m), slice));
    }
    return sliced_ms;
}

mutation_reader make_reversing_reader(mutation_reader original, query::max_result_size max_size, std::unique_ptr<query::partition_slice> slice) {
    class partition_reversing_mutation_reader final : public mutation_reader::impl {
        mutation_reader _source;
        tombstone _current_tombstone;
        query::max_result_size _max_size;
        bool _below_soft_limit = true;
        std::unique_ptr<query::partition_slice> _slice; // only stored, not used
    private:
        void check_buffer_size(const partition_key& key) {
            const auto sz = buffer_size();
            if (sz > _max_size.hard_limit || (sz > _max_size.soft_limit && _below_soft_limit)) [[unlikely]] {
                if (buffer_size() > _max_size.hard_limit) {
                    throw std::runtime_error(fmt::format(
                            "Memory usage of reversed read exceeds hard limit of {} (configured via max_memory_for_unlimited_query_hard_limit), while reading partition {}",
                            _max_size.hard_limit,
                            key.with_schema(*_schema)));
                } else {
                    mrlog.warn(
                            "Memory usage of reversed read exceeds soft limit of {} (configured via max_memory_for_unlimited_query_soft_limit), while reading partition {}",
                            _max_size.soft_limit,
                            key.with_schema(*_schema));
                    _below_soft_limit = false;
                }
            }
        }
        void push_front(mutation_fragment_v2&& mf) {
            unpop_mutation_fragment(std::move(mf));
        }
        void push_back(mutation_fragment_v2&& mf) {
            push_mutation_fragment(std::move(mf));
        }
    public:
        explicit partition_reversing_mutation_reader(mutation_reader mr, query::max_result_size max_size, std::unique_ptr<query::partition_slice> slice)
            : mutation_reader::impl(mr.schema()->make_reversed(), mr.permit())
            , _source(std::move(mr))
            , _max_size(max_size)
            , _slice(std::move(slice))
        { }

        virtual future<> fill_buffer() override {
            if (!is_buffer_empty()) {
                co_return;
            }
            mutation_fragment_v2_opt ps, sr, pe;
            const partition_key* pk = nullptr;
            bool have_partition = false;

            while (!have_partition) {
                auto mf_opt = co_await _source();
                if (!mf_opt) {
                    break;
                }
                switch (mf_opt->mutation_fragment_kind()) {
                    case mutation_fragment_v2::kind::partition_start:
                        ps = std::move(mf_opt);
                        pk = &ps->as_partition_start().key().key();
                        break;
                    case mutation_fragment_v2::kind::partition_end:
                        pe = std::move(mf_opt);
                        have_partition = true;
                        break;
                    case mutation_fragment_v2::kind::static_row:
                        sr = std::move(mf_opt);
                        break;
                    case mutation_fragment_v2::kind::range_tombstone_change:
                        mf_opt->mutate_as_range_tombstone_change(*_schema, [this] (range_tombstone_change& rtc) {
                            rtc.set_position(std::move(rtc).position().reversed());
                            rtc.set_tombstone(std::exchange(_current_tombstone, rtc.tombstone()));
                        });
                        [[fallthrough]];
                    case mutation_fragment_v2::kind::clustering_row:
                        push_front(std::move(*mf_opt));
                        check_buffer_size(*pk);
                        break;
                }
            }

            if (have_partition) {
                if (sr) {
                    push_front(std::move(*sr));
                }
                push_front(std::move(*ps));
                push_back(std::move(*pe));
            }
            _end_of_stream = _source.is_end_of_stream();
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            _end_of_stream = false;
            return _source.next_partition();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
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

    return make_mutation_reader<partition_reversing_mutation_reader>(std::move(original), max_size, std::move(slice));
}

mutation_reader make_nonforwardable(mutation_reader r, bool single_partition) {
    class reader : public mutation_reader::impl {
        mutation_reader _underlying;
        bool _single_partition;
        bool _static_row_done = false;
        bool _partition_is_open = false;
        bool is_end_end_of_underlying_stream() const {
            return _underlying.is_buffer_empty() && _underlying.is_end_of_stream();
        }
        future<> on_end_of_underlying_stream() {
            if (_partition_is_open) {
                if (!_static_row_done) {
                    _static_row_done = true;
                    return _underlying.fast_forward_to(position_range::all_clustered_rows());
                }
                push_mutation_fragment(*_schema, _permit, partition_end());
                reset_partition();
            }
            if (_single_partition) {
                _end_of_stream = true;
                return make_ready_future<>();
            }
            return _underlying.next_partition().then([this] {
                return _underlying.fill_buffer().then([this] {
                    _end_of_stream = is_end_end_of_underlying_stream();
                });
            });
        }
        void reset_partition() {
            _partition_is_open = false;
            _static_row_done = false;
        }
    public:
        reader(mutation_reader r, bool single_partition)
            : impl(r.schema(), r.permit())
            , _underlying(std::move(r))
            , _single_partition(single_partition)
        { }
        virtual future<> fill_buffer() override {
            return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
                return fill_buffer_from(_underlying).then([this] (bool underlying_finished) {
                    if (!_partition_is_open && !is_buffer_empty()) {
                        _partition_is_open = true;
                    }
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
            auto maybe_next_partition = make_ready_future<>();
            if (is_buffer_empty()) {
                if (_end_of_stream || (_partition_is_open && _single_partition)) {
                    _end_of_stream = true;
                    return maybe_next_partition;
                }
                reset_partition();
                maybe_next_partition = _underlying.next_partition();
            }
            return maybe_next_partition.then([this] {
                _end_of_stream = is_end_end_of_underlying_stream();
            });
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            if (_single_partition) {
                _end_of_stream = true;
                return make_ready_future<>();
            }
            reset_partition();
            _end_of_stream = false;
            return _underlying.fast_forward_to(pr);
        }
        virtual future<> close() noexcept override {
            return _underlying.close();
        }
    };
    return make_mutation_reader<reader>(std::move(r), single_partition);
}

template<typename Generator>
class flat_multi_range_mutation_reader : public mutation_reader::impl {
    std::optional<Generator> _generator;
    mutation_reader _reader;

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
            tracing::trace_state_ptr trace_state)
        : impl(s, std::move(permit))
        , _generator(std::move(generator))
        , _reader(source.make_reader_v2(s, _permit, first_range, slice, trace_state, streamed_mutation::forwarding::no, mutation_reader::forwarding::yes))
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
class forwardable_empty_mutation_reader : public mutation_reader::impl {
    mutation_source _source;
    const query::partition_slice& _slice;
    tracing::trace_state_ptr _trace_state;
    mutation_reader_opt _reader;
public:
    forwardable_empty_mutation_reader(schema_ptr s,
            reader_permit permit,
            mutation_source source,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state)
        : impl(s, std::move(permit))
        , _source(std::move(source))
        , _slice(slice)
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
            _reader = _source.make_reader_v2(_schema, _permit, pr, _slice, std::move(_trace_state), streamed_mutation::forwarding::no,
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
mutation_reader
make_flat_multi_range_reader(schema_ptr s, reader_permit permit, mutation_source source, const dht::partition_range_vector& ranges,
                        const query::partition_slice& slice,
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
            return make_mutation_reader<forwardable_empty_mutation_reader>(std::move(s), std::move(permit), std::move(source), slice,
                    std::move(trace_state));
        } else {
            return make_empty_flat_reader_v2(std::move(s), std::move(permit));
        }
    } else if (ranges.size() == 1) {
        return source.make_reader_v2(std::move(s), std::move(permit), ranges.front(), slice, std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr);
    } else {
        return make_mutation_reader<flat_multi_range_mutation_reader<adapter>>(std::move(s), std::move(permit), std::move(source),
                ranges.front(), adapter(std::next(ranges.cbegin()), ranges.cend()), slice, std::move(trace_state));
    }
}

mutation_reader
make_flat_multi_range_reader(
        schema_ptr s,
        reader_permit permit,
        mutation_source source,
        std::function<std::optional<dht::partition_range>()> generator,
        const query::partition_slice& slice,
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
            return make_mutation_reader<forwardable_empty_mutation_reader>(std::move(s), std::move(permit), std::move(source), slice, std::move(trace_state));
        } else {
            return make_empty_flat_reader_v2(std::move(s), std::move(permit));
        }
    } else {
        return make_mutation_reader<flat_multi_range_mutation_reader<adapter>>(std::move(s), std::move(permit), std::move(source),
                *first_range, std::move(adapted_generator), slice, std::move(trace_state));
    }
}


/*
 * This reader takes a get_next_fragment generator that produces mutation_fragment_opt which is returned by
 * generating_reader.
 */
class generating_reader_v2 final : public mutation_reader::impl {
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

mutation_reader make_generating_reader_v2(schema_ptr s, reader_permit permit, noncopyable_function<future<mutation_fragment_v2_opt> ()> get_next_fragment) {
    return make_mutation_reader<generating_reader_v2>(std::move(s), std::move(permit), std::move(get_next_fragment));
}

mutation_reader make_generating_reader_v1(schema_ptr s, reader_permit permit, noncopyable_function<future<mutation_fragment_opt> ()> get_next_fragment) {
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
    return make_mutation_reader<generating_reader_v2>(s, permit, adaptor(s, permit, std::move(get_next_fragment)));
}

class reader_from_mutation_base : public mutation_reader::impl {
    const dht::decorated_key* _dk = nullptr;

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
    reader_from_mutation_base(schema_ptr schema, reader_permit permit) noexcept
        : impl(std::move(schema), std::move(permit))
    { }
    virtual future<> fast_forward_to(position_range pr) override {
        return make_exception_future<>(std::bad_function_call{});
    }
    virtual future<> close() noexcept override {
        return make_ready_future<>();
    }
};

// Reader optimized for a single mutation.
mutation_reader
make_mutation_reader_from_mutations_v2(
        schema_ptr s,
        reader_permit permit,
        mutation m,
        streamed_mutation::forwarding fwd,
        bool reversed) {
    class reader final : public reader_from_mutation_base {
        mutation _mutation;
        bool _reversed;
        std::optional<mutation_consume_cookie> _cookie;

    public:
        reader(schema_ptr schema, reader_permit permit, mutation&& m, bool reversed) noexcept
            : reader_from_mutation_base(std::move(schema), std::move(permit))
            , _mutation(std::move(m))
            , _reversed(reversed)
        {}
        virtual future<> fill_buffer() override {
            if (_end_of_stream) {
                return make_ready_future<>();
            }

            auto res = std::move(_mutation).consume(*this, consume_in_reverse(_reversed), _cookie ? std::move(*_cookie) : mutation_consume_cookie());
            if (res.stop == stop_iteration::yes) {
                _cookie.emplace(std::move(res.cookie));
            } else {
                _end_of_stream = true;
            }
            return make_ready_future<>();
        }
        virtual future<> next_partition() override {
            // Next partition may be called before fill_buffer
            // so set _end_of_stream only if stopped mid-fill.
            if (_cookie) {
                clear_buffer();
                _end_of_stream = true;
            }
            return make_ready_future<>();
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            // We assume that the reader is always created within
            // the initial partition_range.
            clear_buffer();
            _end_of_stream = true;
            return make_ready_future<>();
        }
    };
    auto res = make_mutation_reader<reader>(s, std::move(permit), std::move(m), reversed);
    if (fwd) {
        return make_forwardable(std::move(res));
    }
    return res;
}

// Reader optimized for a single mutation.
mutation_reader
make_mutation_reader_from_mutations_v2(
        schema_ptr s,
        reader_permit permit,
        mutation m,
        const query::partition_slice& slice,
        streamed_mutation::forwarding fwd) {
    const auto reversed = slice.is_reversed();
    auto sliced_mutation = reversed
        ? slice_mutation(s->make_reversed(), std::move(m), query::reverse_slice(*s, slice))
        : slice_mutation(s, std::move(m), slice);
    return make_mutation_reader_from_mutations_v2(std::move(s), std::move(permit), std::move(sliced_mutation), fwd, reversed);
}

mutation_reader
make_mutation_reader_from_mutations_v2(schema_ptr s, reader_permit permit, std::vector<mutation> mutations, const dht::partition_range& pr,
        const query::partition_slice& query_slice, streamed_mutation::forwarding fwd) {
    class reader final : public reader_from_mutation_base {
        std::vector<mutation> _mutations;
        const dht::partition_range* _pr;
        bool _reversed;
        std::optional<mutation_consume_cookie> _cookie;

    public:
        reader(schema_ptr schema, reader_permit permit, std::vector<mutation> mutations, const dht::partition_range& pr, bool reversed)
            : reader_from_mutation_base(std::move(schema), std::move(permit))
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
    };
    if (mutations.empty()) {
        return make_empty_flat_reader_v2(std::move(s), std::move(permit));
    }
    const auto reversed = query_slice.is_reversed();
    std::vector<mutation> sliced_mutations;
    if (reversed) {
        sliced_mutations = slice_mutations(s->make_reversed(), std::move(mutations), query::reverse_slice(*s, query_slice));
    } else {
        sliced_mutations = slice_mutations(s, std::move(mutations), query_slice);
    }
    auto res = make_mutation_reader<reader>(s, std::move(permit), std::move(sliced_mutations), pr, reversed);
    if (fwd) {
        return make_forwardable(std::move(res));
    }
    return res;
}

mutation_reader
make_mutation_reader_from_mutations_v2(schema_ptr s, reader_permit permit, std::vector<mutation> mutations, const dht::partition_range& pr, streamed_mutation::forwarding fwd) {
    if (mutations.size() == 1) {
        dht::ring_position_comparator cmp{*s};
        auto& m = mutations.back();
        auto& dk = m.decorated_key();
        if (pr.before(dk, cmp)) {
            return make_empty_flat_reader_v2(std::move(s), std::move(permit));
        }
        if (!pr.after(dk, cmp)) {
            return make_mutation_reader_from_mutations_v2(std::move(s), std::move(permit), std::move(m), fwd);
        }
        // fallthrough to multi-partition reader
        // since it may be fast_forwarded to include this mutation.
    }
    return make_mutation_reader_from_mutations_v2(s, std::move(permit), std::move(mutations), pr, s->full_slice(), fwd);
}

static mutation_reader
make_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments, const dht::partition_range* pr) {
    class reader : public mutation_reader::impl {
        std::deque<mutation_fragment_v2> _fragments;
        const dht::partition_range* _pr = nullptr;
        dht::ring_position_comparator _cmp;

    private:
        bool end_of_range() const {
            return _fragments.empty() ||
                (_pr && _fragments.front().is_partition_start() && _pr->after(_fragments.front().as_partition_start().key(), _cmp));
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
        reader(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments, const dht::partition_range* pr)
                : mutation_reader::impl(std::move(schema), std::move(permit))
                , _fragments(std::move(fragments))
                , _cmp(*_schema) {
            if (pr) {
                do_fast_forward_to(*pr);
            }
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
    return make_mutation_reader<reader>(std::move(schema), std::move(permit), std::move(fragments), pr);
}

mutation_reader
make_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments, const dht::partition_range& pr) {
    return make_mutation_reader_from_fragments(std::move(schema), std::move(permit), std::move(fragments), &pr);
}

mutation_reader
make_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments) {
    return make_mutation_reader_from_fragments(std::move(schema), std::move(permit), std::move(fragments), nullptr);
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

mutation_reader
make_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment_v2> fragments,
        const dht::partition_range& pr, const query::partition_slice& slice) {
    const auto reversed = slice.is_reversed();
    if (reversed) {
        fragments = reverse_fragments(*schema, permit, std::move(fragments));
    }

    std::deque<mutation_fragment_v2> filtered;
    for (auto it = fragments.begin(); it != fragments.end(); ) {
        auto&& mf = *it++;
        auto kind = mf.mutation_fragment_kind();
        SCYLLA_ASSERT(kind == mutation_fragment_v2::kind::partition_start);
        partition_slicer slicer(schema, permit, slice.row_ranges(*schema, mf.as_partition_start().key().key()),
                                [&filtered] (mutation_fragment_v2 mf) {
                                    filtered.push_back(std::move(mf));
                                });
        filtered.push_back(std::move(mf));
        do {
            auto&& mf = *it++;
            kind = mf.mutation_fragment_kind();
            slicer.apply(std::move(mf));
        } while (kind != mutation_fragment_v2::kind::partition_end);
    }

    return make_mutation_reader_from_fragments(std::move(schema), permit, std::move(filtered), pr);
}

mutation_reader make_next_partition_adaptor(mutation_reader&& rd) {
    class adaptor : public mutation_reader::impl {
        mutation_reader _underlying;
    public:
        adaptor(mutation_reader underlying) : impl(underlying.schema(), underlying.permit()), _underlying(std::move(underlying))
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
    return make_mutation_reader<adaptor>(std::move(rd));
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
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding) {
        return make_empty_flat_reader_v2(s, std::move(permit));
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
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd_sm,
            mutation_reader::forwarding fwd_mr) {
        std::vector<mutation_reader> rd;
        rd.reserve(addends.size());
        for (auto&& ms : addends) {
            rd.emplace_back(ms.make_reader_v2(s, permit, pr, slice, tr, fwd_sm, fwd_mr));
        }
        return make_combined_reader(s, std::move(permit), std::move(rd), fwd_sm, fwd_mr);
    });
}

class queue_reader_v2 final : public mutation_reader::impl {
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
    virtual ~queue_reader_v2() {
        if (_handle) {
            _handle->_reader = nullptr;
        }
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

std::pair<mutation_reader, queue_reader_handle_v2> make_queue_reader_v2(schema_ptr s, reader_permit permit) {
    auto impl = std::make_unique<queue_reader_v2>(std::move(s), std::move(permit));
    auto handle = queue_reader_handle_v2(*impl);
    return {mutation_reader(std::move(impl)), std::move(handle)};
}

namespace {

class compacting_reader : public mutation_reader::impl {
    friend class compact_mutation_state<compact_for_sstables::yes>;

private:
    mutation_reader _reader;
    compact_mutation_state<compact_for_sstables::yes> _compactor;
    noop_compacted_fragments_consumer _gc_consumer;

    // Uncompacted stream
    partition_start _last_uncompacted_partition_start;
    mutation_fragment_v2::kind _last_uncompacted_kind = mutation_fragment_v2::kind::partition_end;

    // Compacted stream
    bool _has_compacted_partition_start = false;

private:
    void maybe_push_partition_start() {
        if (_has_compacted_partition_start) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(_last_uncompacted_partition_start)));
            _has_compacted_partition_start = false;
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
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rtc)));
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end{}));
        return stop_iteration::no;
    }
    void consume_end_of_stream() {
    }
    streamed_mutation::forwarding _fwd;

public:
    compacting_reader(mutation_reader source, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable,
            const tombstone_gc_state& gc_state,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no)
        : impl(source.schema(), source.permit())
        , _reader(std::move(source))
        , _compactor(*_schema, compaction_time, get_max_purgeable, gc_state)
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
        _compactor.abandon_current_partition();
        return _reader.next_partition();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        _compactor.abandon_current_partition();
        return _reader.fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        clear_buffer();
        _end_of_stream = false;
        return _reader.fast_forward_to(std::move(pr));
    }
    virtual future<> close() noexcept override {
        return _reader.close();
    }
};

} // anonymous namespace

mutation_reader make_compacting_reader(mutation_reader source, gc_clock::time_point compaction_time,
        std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable,
        const tombstone_gc_state& gc_state, streamed_mutation::forwarding fwd) {
    return make_mutation_reader<compacting_reader>(std::move(source), compaction_time, get_max_purgeable, gc_state, fwd);
}
