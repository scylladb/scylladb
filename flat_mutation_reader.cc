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

#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "seastar/util/reference_wrapper.hh"
#include <algorithm>

#include <boost/range/adaptor/transformed.hpp>
#include <seastar/util/defer.hh>

static size_t compute_buffer_size(const schema& s, circular_buffer<mutation_fragment>& buffer)
{
    return boost::accumulate(
        buffer
        | boost::adaptors::transformed([&s] (const mutation_fragment& mf) {
            return mf.memory_usage(s);
        }), size_t(0)
    );
}

void flat_mutation_reader::impl::forward_buffer_to(const position_in_partition& pos) {
    _buffer.erase(std::remove_if(_buffer.begin(), _buffer.end(), [this, &pos] (mutation_fragment& f) {
        return !f.relevant_for_range_assuming_after(*_schema, pos);
    }), _buffer.end());

    _buffer_size = compute_buffer_size(*_schema, _buffer);
}

void flat_mutation_reader::impl::clear_buffer_to_next_partition() {
    auto next_partition_start = std::find_if(_buffer.begin(), _buffer.end(), [] (const mutation_fragment& mf) {
        return mf.is_partition_start();
    });
    _buffer.erase(_buffer.begin(), next_partition_start);

    _buffer_size = compute_buffer_size(*_schema, _buffer);
}

flat_mutation_reader flat_mutation_reader::impl::reverse_partitions(flat_mutation_reader::impl& original) {
    // FIXME: #1413 Full partitions get accumulated in memory.

    class partition_reversing_mutation_reader final : public flat_mutation_reader::impl {
        flat_mutation_reader::impl* _source;
        range_tombstone_list _range_tombstones;
        std::stack<mutation_fragment> _mutation_fragments;
        mutation_fragment_opt _partition_end;
    private:
        stop_iteration emit_partition() {
            auto emit_range_tombstone = [&] {
                auto it = std::prev(_range_tombstones.tombstones().end());
                auto& rt = *it;
                _range_tombstones.tombstones().erase(it);
                auto rt_owner = alloc_strategy_unique_ptr<range_tombstone>(&rt);
                push_mutation_fragment(mutation_fragment(std::move(rt)));
            };
            position_in_partition::less_compare cmp(*_source->_schema);
            while (!_mutation_fragments.empty() && !is_buffer_full()) {
                auto& mf = _mutation_fragments.top();
                if (!_range_tombstones.empty() && !cmp(_range_tombstones.tombstones().rbegin()->end_position(), mf.position())) {
                    emit_range_tombstone();
                } else {
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
            push_mutation_fragment(std::move(*std::exchange(_partition_end, stdx::nullopt)));
            return stop_iteration::no;
        }
        future<stop_iteration> consume_partition_from_source(db::timeout_clock::time_point timeout) {
            if (_source->is_buffer_empty()) {
                if (_source->is_end_of_stream()) {
                    _end_of_stream = true;
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return _source->fill_buffer(timeout).then([] { return stop_iteration::no; });
            }
            while (!_source->is_buffer_empty() && !is_buffer_full()) {
                auto mf = _source->pop_mutation_fragment();
                if (mf.is_partition_start() || mf.is_static_row()) {
                    push_mutation_fragment(std::move(mf));
                } else if (mf.is_end_of_partition()) {
                    _partition_end = std::move(mf);
                    if (emit_partition()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                } else if (mf.is_range_tombstone()) {
                    _range_tombstones.apply(*_source->_schema, std::move(mf.as_range_tombstone()));
                } else {
                    _mutation_fragments.emplace(std::move(mf));
                }
            }
            return make_ready_future<stop_iteration>(is_buffer_full());
        }
    public:
        explicit partition_reversing_mutation_reader(flat_mutation_reader::impl& mr)
            : flat_mutation_reader::impl(mr._schema)
            , _source(&mr)
            , _range_tombstones(*mr._schema)
        { }

        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            return repeat([&, timeout] {
                if (_partition_end) {
                    // We have consumed full partition from source, now it is
                    // time to emit it.
                    auto stop = emit_partition();
                    if (stop) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                }
                return consume_partition_from_source(timeout);
            });
        }

        virtual void next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty() && !is_end_of_stream()) {
                while (!_mutation_fragments.empty()) {
                    _mutation_fragments.pop();
                }
                _range_tombstones.clear();
                _partition_end = stdx::nullopt;
                _source->next_partition();
            }
        }

        virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override {
            throw std::bad_function_call();
        }

        virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override {
            throw std::bad_function_call();
        }
        virtual size_t buffer_size() const override {
            return flat_mutation_reader::impl::buffer_size() + _source->buffer_size();
        }
    };

    return make_flat_mutation_reader<partition_reversing_mutation_reader>(original);
}

template<typename Source>
future<bool> flat_mutation_reader::impl::fill_buffer_from(Source& source, db::timeout_clock::time_point timeout) {
    if (source.is_buffer_empty()) {
        if (source.is_end_of_stream()) {
            return make_ready_future<bool>(true);
        }
        return source.fill_buffer(timeout).then([this, &source, timeout] {
            return fill_buffer_from(source, timeout);
        });
    } else {
        while (!source.is_buffer_empty() && !is_buffer_full()) {
            push_mutation_fragment(source.pop_mutation_fragment());
        }
        return make_ready_future<bool>(source.is_end_of_stream() && source.is_buffer_empty());
    }
}

template future<bool> flat_mutation_reader::impl::fill_buffer_from<flat_mutation_reader>(flat_mutation_reader&, db::timeout_clock::time_point);

flat_mutation_reader& to_reference(reference_wrapper<flat_mutation_reader>& wrapper) {
    return wrapper.get();
}

flat_mutation_reader make_delegating_reader(flat_mutation_reader& r) {
    return make_flat_mutation_reader<delegating_reader<reference_wrapper<flat_mutation_reader>>>(ref(r));
}

flat_mutation_reader make_forwardable(flat_mutation_reader m) {
    class reader : public flat_mutation_reader::impl {
        flat_mutation_reader _underlying;
        position_range _current;
        mutation_fragment_opt _next;
        // When resolves, _next is engaged or _end_of_stream is set.
        future<> ensure_next(db::timeout_clock::time_point timeout) {
            if (_next) {
                return make_ready_future<>();
            }
            return _underlying(timeout).then([this] (auto&& mfo) {
                _next = std::move(mfo);
                if (!_next) {
                    _end_of_stream = true;
                }
            });
        }
    public:
        reader(flat_mutation_reader r) : impl(r.schema()), _underlying(std::move(r)), _current({
            position_in_partition(position_in_partition::partition_start_tag_t()),
            position_in_partition(position_in_partition::after_static_row_tag_t())
        }) { }
        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            return repeat([this, timeout] {
                if (is_buffer_full()) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return ensure_next(timeout).then([this] {
                    if (is_end_of_stream()) {
                        return stop_iteration::yes;
                    }
                    position_in_partition::less_compare cmp(*_schema);
                    if (!cmp(_next->position(), _current.end())) {
                        _end_of_stream = true;
                        // keep _next, it may be relevant for next range
                        return stop_iteration::yes;
                    }
                    if (_next->relevant_for_range(*_schema, _current.start())) {
                        push_mutation_fragment(std::move(*_next));
                    }
                    _next = {};
                    return stop_iteration::no;
                });
            });
        }
        virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
            _current = std::move(pr);
            _end_of_stream = false;
            forward_buffer_to(_current.start());
            return make_ready_future<>();
        }
        virtual void next_partition() override {
            _end_of_stream = false;
            if (!_next || !_next->is_partition_start()) {
                _underlying.next_partition();
                _next = {};
            }
            clear_buffer_to_next_partition();
            _current = {
                position_in_partition(position_in_partition::partition_start_tag_t()),
                position_in_partition(position_in_partition::after_static_row_tag_t())
            };
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            _end_of_stream = false;
            clear_buffer();
            _next = {};
            _current = {
                position_in_partition(position_in_partition::partition_start_tag_t()),
                position_in_partition(position_in_partition::after_static_row_tag_t())
            };
            return _underlying.fast_forward_to(pr, timeout);
        }
        virtual size_t buffer_size() const override {
            return flat_mutation_reader::impl::buffer_size() + _underlying.buffer_size();
        }
    };
    return make_flat_mutation_reader<reader>(std::move(m));
}

flat_mutation_reader make_nonforwardable(flat_mutation_reader r, bool single_partition) {
    class reader : public flat_mutation_reader::impl {
        flat_mutation_reader _underlying;
        bool _single_partition;
        bool _static_row_done = false;
        bool is_end_end_of_underlying_stream() const {
            return _underlying.is_buffer_empty() && _underlying.is_end_of_stream();
        }
        future<> on_end_of_underlying_stream(db::timeout_clock::time_point timeout) {
            if (!_static_row_done) {
                _static_row_done = true;
                return _underlying.fast_forward_to(position_range::all_clustered_rows(), timeout);
            }
            push_mutation_fragment(partition_end());
            if (_single_partition) {
                _end_of_stream = true;
                return make_ready_future<>();
            }
            _underlying.next_partition();
            _static_row_done = false;
            return _underlying.fill_buffer(timeout).then([this] {
                _end_of_stream = is_end_end_of_underlying_stream();
            });
        }
    public:
        reader(flat_mutation_reader r, bool single_partition)
            : impl(r.schema())
            , _underlying(std::move(r))
            , _single_partition(single_partition)
        { }
        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this, timeout] {
                return fill_buffer_from(_underlying, timeout).then([this, timeout] (bool underlying_finished) {
                    if (underlying_finished) {
                        return on_end_of_underlying_stream(timeout);
                    }
                    return make_ready_future<>();
                });
            });
        }
        virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
            throw std::bad_function_call();
        }
        virtual void next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
                _underlying.next_partition();
            }
            _end_of_stream = is_end_end_of_underlying_stream();
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            _end_of_stream = false;
            clear_buffer();
            return _underlying.fast_forward_to(pr, timeout);
        }
        virtual size_t buffer_size() const override {
            return flat_mutation_reader::impl::buffer_size() + _underlying.buffer_size();
        }
    };
    return make_flat_mutation_reader<reader>(std::move(r), single_partition);
}

class empty_flat_reader final : public flat_mutation_reader::impl {
public:
    empty_flat_reader(schema_ptr s) : impl(std::move(s)) { _end_of_stream = true; }
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override { return make_ready_future<>(); }
    virtual void next_partition() override {}
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override { return make_ready_future<>(); };
    virtual future<> fast_forward_to(position_range cr, db::timeout_clock::time_point timeout) override { return make_ready_future<>(); };
};

flat_mutation_reader make_empty_flat_reader(schema_ptr s) {
    return make_flat_mutation_reader<empty_flat_reader>(std::move(s));
}

flat_mutation_reader
flat_mutation_reader_from_mutations(std::vector<mutation> ms,
                                    const query::partition_slice& slice,
                                    streamed_mutation::forwarding fwd) {
    std::vector<mutation> sliced_ms;
    for (auto& m : ms) {
        auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*m.schema(), slice, m.key());
        auto mp = mutation_partition(std::move(m.partition()), *m.schema(), std::move(ck_ranges));
        sliced_ms.emplace_back(m.schema(), m.decorated_key(), std::move(mp));
    }
    return flat_mutation_reader_from_mutations(sliced_ms, query::full_partition_range, fwd);
}

flat_mutation_reader
flat_mutation_reader_from_mutations(std::vector<mutation> mutations, const dht::partition_range& pr, streamed_mutation::forwarding fwd) {
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
            auto& crs = _cur->partition().clustered_rows();
            while (true) {
                auto re = crs.unlink_leftmost_without_rebalance();
                if (!re) {
                    break;
                }
                auto re_deleter = defer([re] { current_deleter<rows_entry>()(re); });
                if (!re->dummy()) {
                    _cr = mutation_fragment(std::move(*re));
                    break;
                }
            }
        }
        void prepare_next_range_tombstone() {
            auto& rts = _cur->partition().row_tombstones().tombstones();
            auto rt = rts.unlink_leftmost_without_rebalance();
            if (rt) {
                auto rt_deleter = defer([rt] { current_deleter<range_tombstone>()(rt); });
                _rt = mutation_fragment(std::move(*rt));
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
        void do_fill_buffer(db::timeout_clock::time_point timeout) {
            while (!is_end_of_stream() && !is_buffer_full()) {
                if (!_static_row_done) {
                    _static_row_done = true;
                    if (!_cur->partition().static_row().empty()) {
                        push_mutation_fragment(static_row(std::move(_cur->partition().static_row())));
                    }
                }
                auto mfopt = read_next();
                if (mfopt) {
                    push_mutation_fragment(std::move(*mfopt));
                } else {
                    push_mutation_fragment(partition_end());
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
            push_mutation_fragment(partition_start(_cur->decorated_key(),
                                                   _cur->partition().partition_tombstone()));

            prepare_next_clustering_row();
            prepare_next_range_tombstone();
        }
        void destroy_current_mutation() {
            auto &crs = _cur->partition().clustered_rows();
            auto re = crs.unlink_leftmost_without_rebalance();
            while (re) {
                current_deleter<rows_entry>()(re);
                re = crs.unlink_leftmost_without_rebalance();
            }

            auto &rts = _cur->partition().row_tombstones().tombstones();
            auto rt = rts.unlink_leftmost_without_rebalance();
            while (rt) {
                current_deleter<range_tombstone>()(rt);
                rt = rts.unlink_leftmost_without_rebalance();
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
        reader(schema_ptr s, std::vector<mutation>&& mutations, const dht::partition_range& pr)
            : impl(std::move(s))
            , _mutations(std::move(mutations))
            , _cur(find_first_partition(_mutations, pr))
            , _end(find_last_partition(_mutations, pr))
            , _cmp(*_cur->schema())
        {
            _end_of_stream = _cur == _end;
            if (!_end_of_stream) {
                auto mutation_destroyer = defer([this] { destroy_mutations(); });
                start_new_partition();

                do_fill_buffer(db::no_timeout);

                mutation_destroyer.cancel();
            }
        }
        void destroy_mutations() noexcept {
            // After unlink_leftmost_without_rebalance() was called on a bi::set
            // we need to complete destroying the tree using that function.
            // clear_and_dispose() used by mutation_partition destructor won't
            // work properly.

            while (_cur != _end) {
                destroy_current_mutation();
                ++_cur;
            }
        }
        ~reader() {
            destroy_mutations();
        }
        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            do_fill_buffer(timeout);
            return make_ready_future<>();
        }
        virtual void next_partition() override {
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
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
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
        virtual future<> fast_forward_to(position_range cr, db::timeout_clock::time_point timeout) override {
            throw std::runtime_error("This reader can't be fast forwarded to another position.");
        };
    };
    assert(!mutations.empty());
    schema_ptr s = mutations[0].schema();
    auto res = make_flat_mutation_reader<reader>(std::move(s), std::move(mutations), pr);
    if (fwd) {
        return make_forwardable(std::move(res));
    }
    return res;
}

/// A reader that is empty when created but can be fast-forwarded.
///
/// Useful when a reader has to be created without an initial read-range and it
/// has to be fast-forwardable.
/// Delays the creation of the underlying reader until it is first
/// fast-forwarded and thus a range is available.
class forwardable_empty_mutation_reader : public flat_mutation_reader::impl {
    mutation_source _source;
    const query::partition_slice& _slice;
    const io_priority_class& _pc;
    tracing::trace_state_ptr _trace_state;
    flat_mutation_reader_opt _reader;
public:
    forwardable_empty_mutation_reader(schema_ptr s,
            mutation_source source,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state)
        : impl(s)
        , _source(std::move(source))
        , _slice(slice)
        , _pc(pc)
        , _trace_state(std::move(trace_state)) {
        _end_of_stream = true;
    }
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        if (!_reader) {
            return make_ready_future<>();
        }
        if (_reader->is_buffer_empty()) {
            if (_reader->is_end_of_stream()) {
                _end_of_stream = true;
                return make_ready_future<>();
            } else {
                return _reader->fill_buffer(timeout).then([this, timeout] { return fill_buffer(timeout); });
            }
        }
        _reader->move_buffer_content_to(*this);
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        if (!_reader) {
            _reader = _source.make_reader(_schema, pr, _slice, _pc, std::move(_trace_state), streamed_mutation::forwarding::no,
                    mutation_reader::forwarding::yes);
            _end_of_stream = false;
            return make_ready_future<>();
        }

        clear_buffer();
        _end_of_stream = false;
        return _reader->fast_forward_to(pr, timeout);
    }
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        throw std::bad_function_call();
    }
    virtual void next_partition() override {
        if (!_reader) {
            return;
        }
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            _reader->next_partition();
        }
    }
    virtual size_t buffer_size() const override {
        return impl::buffer_size() + (_reader ? _reader->buffer_size() : 0);
    }
};

template<typename Generator>
class flat_multi_range_mutation_reader : public flat_mutation_reader::impl {
    std::optional<Generator> _generator;
    flat_mutation_reader _reader;

    const dht::partition_range* next() {
        if (!_generator) {
            return nullptr;
        }
        return (*_generator)();
    }

public:
    flat_multi_range_mutation_reader(
            schema_ptr s,
            mutation_source source,
            const dht::partition_range& first_range,
            Generator generator,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state)
        : impl(s)
        , _generator(std::move(generator))
        , _reader(source.make_reader(s, first_range, slice, pc, trace_state, streamed_mutation::forwarding::no, mutation_reader::forwarding::yes))
    {
    }

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        return do_until([this] { return is_end_of_stream() || !is_buffer_empty(); }, [this, timeout] {
            return _reader.fill_buffer(timeout).then([this, timeout] () {
                while (!_reader.is_buffer_empty()) {
                    push_mutation_fragment(_reader.pop_mutation_fragment());
                }
                if (!_reader.is_end_of_stream()) {
                    return make_ready_future<>();
                }
                if (auto r = next()) {
                    return _reader.fast_forward_to(*r, timeout);
                } else {
                    _end_of_stream = true;
                    return make_ready_future<>();
                }
            });
        });
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        clear_buffer();
        _end_of_stream = false;
        return _reader.fast_forward_to(pr, timeout).then([this] {
            _generator.reset();
        });
    }

    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        throw std::bad_function_call();
    }

    virtual void next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !is_end_of_stream()) {
            _reader.next_partition();
        }
    }
    virtual size_t buffer_size() const override {
        return flat_mutation_reader::impl::buffer_size() + _reader.buffer_size();
    }
};

flat_mutation_reader
make_flat_multi_range_reader(schema_ptr s, mutation_source source, const dht::partition_range_vector& ranges,
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
            return make_flat_mutation_reader<forwardable_empty_mutation_reader>(std::move(s), std::move(source), slice, pc, std::move(trace_state));
        } else {
            return make_empty_flat_reader(std::move(s));
        }
    } else if (ranges.size() == 1) {
        return source.make_reader(std::move(s), ranges.front(), slice, pc, std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr);
    } else {
        return make_flat_mutation_reader<flat_multi_range_mutation_reader<adapter>>(std::move(s), std::move(source),
                ranges.front(), adapter(std::next(ranges.cbegin()), ranges.cend()), slice, pc, std::move(trace_state));
    }
}

flat_mutation_reader
make_flat_multi_range_reader(
        schema_ptr s,
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
            return make_flat_mutation_reader<forwardable_empty_mutation_reader>(std::move(s), std::move(source), slice, pc, std::move(trace_state));
        } else {
            return make_empty_flat_reader(std::move(s));
        }
    } else {
        return make_flat_mutation_reader<flat_multi_range_mutation_reader<adapter>>(std::move(s), std::move(source),
                *first_range, std::move(adapted_generator), slice, pc, std::move(trace_state));
    }
}

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr schema, std::deque<mutation_fragment> fragments) {
    class reader : public flat_mutation_reader::impl {
        std::deque<mutation_fragment> _fragments;
    public:
        reader(schema_ptr schema, std::deque<mutation_fragment> fragments)
                : flat_mutation_reader::impl(std::move(schema))
                , _fragments(std::move(fragments)) {
        }
        virtual future<> fill_buffer(db::timeout_clock::time_point) override {
            while (!(_end_of_stream = _fragments.empty()) && !is_buffer_full()) {
                push_mutation_fragment(std::move(_fragments.front()));
                _fragments.pop_front();
            }
            return make_ready_future<>();
        }
        virtual void next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
                while (!(_end_of_stream = _fragments.empty()) && !_fragments.front().is_partition_start()) {
                    _fragments.pop_front();
                }
            }
        }
        virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
            throw std::runtime_error("This reader can't be fast forwarded to another range.");
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            throw std::runtime_error("This reader can't be fast forwarded to another position.");
        }
    };
    return make_flat_mutation_reader<reader>(std::move(schema), std::move(fragments));
}
