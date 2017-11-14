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
#include <algorithm>

#include <boost/range/adaptor/transformed.hpp>
#include <seastar/util/defer.hh>

void flat_mutation_reader::impl::forward_buffer_to(const position_in_partition& pos) {
    _buffer.erase(std::remove_if(_buffer.begin(), _buffer.end(), [this, &pos] (mutation_fragment& f) {
        return !f.relevant_for_range_assuming_after(*_schema, pos);
    }), _buffer.end());

    _buffer_size = boost::accumulate(_buffer | boost::adaptors::transformed(std::mem_fn(&mutation_fragment::memory_usage)), size_t(0));
}

void flat_mutation_reader::impl::clear_buffer_to_next_partition() {
    auto next_partition_start = std::find_if(_buffer.begin(), _buffer.end(), [] (const mutation_fragment& mf) {
        return mf.is_partition_start();
    });
    _buffer.erase(_buffer.begin(), next_partition_start);

    _buffer_size = boost::accumulate(_buffer | boost::adaptors::transformed(std::mem_fn(&mutation_fragment::memory_usage)), size_t(0));
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
            push_mutation_fragment(*std::exchange(_partition_end, stdx::nullopt));
            return stop_iteration::no;
        }
        future<stop_iteration> consume_partition_from_source() {
            if (_source->is_buffer_empty()) {
                if (_source->is_end_of_stream()) {
                    _end_of_stream = true;
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return _source->fill_buffer().then([] { return stop_iteration::no; });
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

        virtual future<> fast_forward_to(const dht::partition_range&) override {
            throw std::bad_function_call();
        }

        virtual future<> fast_forward_to(position_range) override {
            throw std::bad_function_call();
        }
    };

    return make_flat_mutation_reader<partition_reversing_mutation_reader>(original);
}

flat_mutation_reader flat_mutation_reader_from_mutation_reader(schema_ptr s, mutation_reader&& legacy_reader, streamed_mutation::forwarding fwd) {
    class converting_reader final : public flat_mutation_reader::impl {
        mutation_reader _legacy_reader;
        streamed_mutation_opt _sm;
        streamed_mutation::forwarding _fwd;

        future<> get_next_sm() {
            return _legacy_reader().then([this] (auto&& sm) {
                if (bool(sm)) {
                    _sm = std::move(sm);
                    this->push_mutation_fragment(
                            mutation_fragment(partition_start(_sm->decorated_key(), _sm->partition_tombstone())));
                } else {
                    _end_of_stream = true;
                }
            });
        }
        void on_sm_finished() {
            if (_fwd == streamed_mutation::forwarding::yes) {
                _end_of_stream = true;
            } else {
                this->push_mutation_fragment(mutation_fragment(partition_end()));
                _sm = {};
            }
        }
    public:
        converting_reader(schema_ptr s, mutation_reader&& legacy_reader, streamed_mutation::forwarding fwd)
            : impl(std::move(s)), _legacy_reader(std::move(legacy_reader)), _fwd(fwd)
        { }
        virtual future<> fill_buffer() override {
            return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
                if (!_sm) {
                    return get_next_sm();
                } else {
                    if (_sm->is_buffer_empty()) {
                        if (_sm->is_end_of_stream()) {
                            on_sm_finished();
                            return make_ready_future<>();
                        }
                        return _sm->fill_buffer();
                    } else {
                        while (!_sm->is_buffer_empty() && !is_buffer_full()) {
                            this->push_mutation_fragment(_sm->pop_mutation_fragment());
                        }
                        if (_sm->is_end_of_stream() && _sm->is_buffer_empty()) {
                            on_sm_finished();
                        }
                        return make_ready_future<>();
                    }
                }
            });
        }
        virtual void next_partition() override {
            if (_fwd == streamed_mutation::forwarding::yes) {
                clear_buffer();
                _sm = {};
                _end_of_stream = false;
            } else {
                clear_buffer_to_next_partition();
                if (_sm && is_buffer_empty()) {
                    _sm = {};
                }
            }
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _sm = { };
            _end_of_stream = false;
            return _legacy_reader.fast_forward_to(pr);
        };
        virtual future<> fast_forward_to(position_range cr) override {
            forward_buffer_to(cr.start());
            _end_of_stream = false;
            if (_sm) {
                return _sm->fast_forward_to(std::move(cr));
            } else {
                _end_of_stream = true;
                return make_ready_future<>();
            }
        };
    };
    return make_flat_mutation_reader<converting_reader>(std::move(s), std::move(legacy_reader), fwd);
}

flat_mutation_reader make_forwardable(flat_mutation_reader m) {
    class reader : public flat_mutation_reader::impl {
        flat_mutation_reader _underlying;
        position_range _current = {
            position_in_partition(position_in_partition::partition_start_tag_t()),
            position_in_partition(position_in_partition::after_static_row_tag_t())
        };
        mutation_fragment_opt _next;
        // When resolves, _next is engaged or _end_of_stream is set.
        future<> ensure_next() {
            if (_next) {
                return make_ready_future<>();
            }
            return _underlying().then([this] (auto&& mfo) {
                _next = std::move(mfo);
                if (!_next) {
                    _end_of_stream = true;
                }
            });
        }
    public:
        reader(flat_mutation_reader r) : impl(r.schema()), _underlying(std::move(r)) { }
        virtual future<> fill_buffer() override {
            return repeat([this] {
                if (is_buffer_full()) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return ensure_next().then([this] {
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
        virtual future<> fast_forward_to(position_range pr) override {
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
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _next = {};
            return _underlying.fast_forward_to(pr);
        }
    };
    return make_flat_mutation_reader<reader>(std::move(m));
}

class empty_flat_reader final : public flat_mutation_reader::impl {
public:
    empty_flat_reader(schema_ptr s) : impl(std::move(s)) { _end_of_stream = true; }
    virtual future<> fill_buffer() override { return make_ready_future<>(); }
    virtual void next_partition() override {}
    virtual future<> fast_forward_to(const dht::partition_range& pr) override { return make_ready_future<>(); };
    virtual future<> fast_forward_to(position_range cr) override { return make_ready_future<>(); };
};

flat_mutation_reader make_empty_flat_reader(schema_ptr s) {
    return make_flat_mutation_reader<empty_flat_reader>(std::move(s));
}

flat_mutation_reader
flat_mutation_reader_from_mutations(std::vector<mutation> mutations, streamed_mutation::forwarding fwd) {
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
                auto cr = move_and_disengage(_cr);
                prepare_next_clustering_row();
                return cr;
            } else if (_rt) {
                auto rt = move_and_disengage(_rt);
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
    public:
        reader(schema_ptr s, std::vector<mutation>&& mutations)
            : impl(std::move(s))
            , _mutations(std::move(mutations))
            , _cur(_mutations.begin())
            , _end(_mutations.end())
            , _cmp(*_cur->schema())
        {
            auto mutation_destroyer = defer([this] { destroy_mutations(); });
            start_new_partition();

            do_fill_buffer();

            mutation_destroyer.cancel();
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
        virtual future<> fill_buffer() override {
            do_fill_buffer();
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
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            throw std::runtime_error("This reader can't be fast forwarded to another partition.");
        };
        virtual future<> fast_forward_to(position_range cr) override {
            throw std::runtime_error("This reader can't be fast forwarded to another position.");
        };
    };
    assert(!mutations.empty());
    schema_ptr s = mutations[0].schema();
    auto res = make_flat_mutation_reader<reader>(std::move(s), std::move(mutations));
    if (fwd) {
        return make_forwardable(std::move(res));
    }
    return res;
}

class flat_multi_range_mutation_reader : public flat_mutation_reader::impl {
public:
    using ranges_vector = dht::partition_range_vector;
private:
    const ranges_vector& _ranges;
    ranges_vector::const_iterator _current_range;
    flat_mutation_reader _reader;
public:
    flat_multi_range_mutation_reader(schema_ptr s, mutation_source source, const ranges_vector& ranges,
                                const query::partition_slice& slice, const io_priority_class& pc,
                                tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd,
                                mutation_reader::forwarding fwd_mr)
        : impl(s)
        , _ranges(ranges)
        , _current_range(_ranges.begin())
        , _reader(source.make_flat_mutation_reader(s, *_current_range, slice, pc, trace_state, fwd,
                                                   _ranges.size() > 1 ? mutation_reader::forwarding::yes : fwd_mr))
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
                ++_current_range;
                if (_current_range == _ranges.end()) {
                    _end_of_stream = true;
                    return make_ready_future<>();
                }
                return _reader.fast_forward_to(*_current_range);
            });
        });
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        // When end of pr is reached, this reader will increment _current_range
        // and notice that it now points to _ranges.end().
        _current_range = std::prev(_ranges.end());
        return _reader.fast_forward_to(pr);
    }

    virtual future<> fast_forward_to(position_range pr) override {
        return _reader.fast_forward_to(std::move(pr));
    }

    virtual void next_partition() override {
        return _reader.next_partition();
    }
};

flat_mutation_reader
make_flat_multi_range_reader(schema_ptr s, mutation_source source, const dht::partition_range_vector& ranges,
                        const query::partition_slice& slice, const io_priority_class& pc,
                        tracing::trace_state_ptr trace_state, streamed_mutation::forwarding fwd,
                        mutation_reader::forwarding fwd_mr)
{
    return make_flat_mutation_reader<flat_multi_range_mutation_reader>(std::move(s), std::move(source), ranges,
                                                             slice, pc, std::move(trace_state), fwd, fwd_mr);
}
