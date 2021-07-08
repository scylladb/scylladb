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

#include "flat_mutation_reader.hh"
#include "flat_mutation_reader_v2.hh"
#include "range_tombstone_assembler.hh"
#include "range_tombstone_change_generator.hh"
#include "mutation_fragment_stream_validator.hh"
#include "mutation_reader.hh"
#include "seastar/util/reference_wrapper.hh"
#include "clustering_ranges_walker.hh"
#include "schema_upgrader.hh"
#include <algorithm>
#include <stack>

#include <boost/range/adaptor/transformed.hpp>
#include <seastar/util/defer.hh>
#include "utils/exceptions.hh"
#include "mutation_rebuilder.hh"
#include "range_tombstone_splitter.hh"
#include <seastar/core/on_internal_error.hh>

#include "clustering_key_filter.hh"

logging::logger fmr_logger("flat_mutation_reader");

flat_mutation_reader& flat_mutation_reader::operator=(flat_mutation_reader&& o) noexcept {
    if (_impl) {
        impl* ip = _impl.get();
        // Abort to enforce calling close() before readers are closed
        // to prevent leaks and potential use-after-free due to background
        // tasks left behind.
        on_internal_error_noexcept(fmr_logger, format("{} [{}]: permit {}: was not closed before overwritten by move-assign", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description()));
        abort();
    }
    _impl = std::move(o._impl);
    return *this;
}

flat_mutation_reader::~flat_mutation_reader() {
    if (_impl) {
        impl* ip = _impl.get();
        // Abort to enforce calling close() before readers are closed
        // to prevent leaks and potential use-after-free due to background
        // tasks left behind.
        on_internal_error_noexcept(fmr_logger, format("{} [{}]: permit {}: was not closed before destruction", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description()));
        abort();
    }
}

static size_t compute_buffer_size(const schema& s, const flat_mutation_reader::tracked_buffer& buffer)
{
    return boost::accumulate(
        buffer
        | boost::adaptors::transformed([&s] (const mutation_fragment& mf) {
            return mf.memory_usage();
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

flat_mutation_reader make_reversing_reader(flat_mutation_reader& original, query::max_result_size max_size) {
    class partition_reversing_mutation_reader final : public flat_mutation_reader::impl {
        flat_mutation_reader* _source;
        range_tombstone_list _range_tombstones;
        std::stack<mutation_fragment> _mutation_fragments;
        mutation_fragment_opt _partition_end;
        size_t _stack_size = 0;
        const query::max_result_size _max_size;
        bool _below_soft_limit = true;
    private:
        stop_iteration emit_partition() {
            auto emit_range_tombstone = [&] {
                auto it = std::prev(_range_tombstones.end());
                push_mutation_fragment(*_schema, _permit, _range_tombstones.pop_as<range_tombstone>(it));
            };
            position_in_partition::less_compare cmp(*_schema);
            while (!_mutation_fragments.empty() && !is_buffer_full()) {
                auto& mf = _mutation_fragments.top();
                if (!_range_tombstones.empty() && !cmp(_range_tombstones.rbegin()->end_position(), mf.position())) {
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
                    _range_tombstones.apply(*_schema, std::move(mf.as_range_tombstone()));
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
                            throw std::runtime_error(fmt::format(
                                    "Memory usage of reversed read exceeds hard limit of {} (configured via max_memory_for_unlimited_query_hard_limit), while reading partition {}",
                                    _max_size.hard_limit,
                                    key->with_schema(*_schema)));
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
        explicit partition_reversing_mutation_reader(flat_mutation_reader& mr, query::max_result_size max_size)
            : flat_mutation_reader::impl(mr.schema(), mr.permit())
            , _source(&mr)
            , _range_tombstones(*_schema)
            , _max_size(max_size)
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

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty() && !is_end_of_stream()) {
                while (!_mutation_fragments.empty()) {
                    _stack_size -= _mutation_fragments.top().memory_usage();
                    _mutation_fragments.pop();
                }
                _range_tombstones.clear();
                _partition_end = std::nullopt;
                return _source->next_partition();
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }

        virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override {
            return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
        }

        virtual future<> close() noexcept override {
            // we don't own _source therefore do not close it
            return make_ready_future<>();
        }
    };

    return make_flat_mutation_reader<partition_reversing_mutation_reader>(original, max_size);
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

flat_mutation_reader make_delegating_reader(flat_mutation_reader& r) {
    return make_flat_mutation_reader<delegating_reader>(r);
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
        reader(flat_mutation_reader r) : impl(r.schema(), r.permit()), _underlying(std::move(r)), _current({
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
        virtual future<> next_partition() override {
            _end_of_stream = false;
            auto maybe_next_partition = make_ready_future<>();
            if (!_next || !_next->is_partition_start()) {
              maybe_next_partition = _underlying.next_partition().then([this] {
                _next = {};
              });
            }
          return maybe_next_partition.then([this] {
            clear_buffer_to_next_partition();
            _current = {
                position_in_partition(position_in_partition::partition_start_tag_t()),
                position_in_partition(position_in_partition::after_static_row_tag_t())
            };
          });
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
        virtual future<> close() noexcept override {
            return _underlying.close();
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
            push_mutation_fragment(*_schema, _permit, partition_end());
            if (_single_partition) {
                _end_of_stream = true;
                return make_ready_future<>();
            }
          return _underlying.next_partition().then([this, timeout] {
            _static_row_done = false;
            return _underlying.fill_buffer(timeout).then([this] {
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
        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            _end_of_stream = false;
            clear_buffer();
            return _underlying.fast_forward_to(pr, timeout);
        }
        virtual future<> close() noexcept override {
            return _underlying.close();
        }
    };
    return make_flat_mutation_reader<reader>(std::move(r), single_partition);
}

class empty_flat_reader final : public flat_mutation_reader::impl {
public:
    empty_flat_reader(schema_ptr s, reader_permit permit) : impl(std::move(s), std::move(permit)) { _end_of_stream = true; }
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override { return make_ready_future<>(); }
    virtual future<> next_partition() override { return make_ready_future<>(); }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override { return make_ready_future<>(); };
    virtual future<> fast_forward_to(position_range cr, db::timeout_clock::time_point timeout) override { return make_ready_future<>(); };
    virtual future<> close() noexcept override { return make_ready_future<>(); }
};

flat_mutation_reader make_empty_flat_reader(schema_ptr s, reader_permit permit) {
    return make_flat_mutation_reader<empty_flat_reader>(std::move(s), std::move(permit));
}

flat_mutation_reader
flat_mutation_reader_from_mutations(reader_permit permit,
                                    std::vector<mutation> ms,
                                    const dht::partition_range& pr,
                                    const query::partition_slice& slice,
                                    streamed_mutation::forwarding fwd) {
    std::vector<mutation> sliced_ms;
    for (auto& m : ms) {
        auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*m.schema(), slice, m.key());
        auto mp = mutation_partition(std::move(m.partition()), *m.schema(), std::move(ck_ranges));
        sliced_ms.emplace_back(m.schema(), m.decorated_key(), std::move(mp));
    }
    return flat_mutation_reader_from_mutations(std::move(permit), sliced_ms, pr, fwd);
}

flat_mutation_reader
flat_mutation_reader_from_mutations(reader_permit permit, std::vector<mutation> ms, const query::partition_slice& slice, streamed_mutation::forwarding fwd) {
    return flat_mutation_reader_from_mutations(std::move(permit), std::move(ms), query::full_partition_range, slice, fwd);
}

flat_mutation_reader
flat_mutation_reader_from_mutations(reader_permit permit, std::vector<mutation> mutations, const dht::partition_range& pr, streamed_mutation::forwarding fwd) {
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
                    _cr = mutation_fragment(*_schema, _permit, std::move(*re));
                    break;
                }
            }
        }
        void prepare_next_range_tombstone() {
            auto& rts = _cur->partition().row_tombstones();
            auto rt = rts.pop_front_and_lock();
            if (rt) {
                auto rt_deleter = defer([rt] { current_deleter<range_tombstone>()(rt); });
                _rt = mutation_fragment(*_schema, _permit, std::move(*rt));
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
            auto &crs = _cur->partition().clustered_rows();
            auto deleter = current_deleter<rows_entry>();
            crs.clear_and_dispose(deleter);

            auto &rts = _cur->partition().row_tombstones();
            auto rt = rts.pop_front_and_lock();
            while (rt) {
                current_deleter<range_tombstone>()(rt);
                rt = rts.pop_front_and_lock();
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

            _cur = _mutations.begin();
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
        virtual future<> close() noexcept override {
            return make_ready_future<>();
        };
    };
    assert(!mutations.empty());
    schema_ptr s = mutations[0].schema();
    auto res = make_flat_mutation_reader<reader>(std::move(s), std::move(permit), std::move(mutations), pr);
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
            _reader = _source.make_reader(_schema, _permit, pr, _slice, _pc, std::move(_trace_state), streamed_mutation::forwarding::no,
                    mutation_reader::forwarding::yes);
            _end_of_stream = false;
            return make_ready_future<>();
        }

        clear_buffer();
        _end_of_stream = false;
        return _reader->fast_forward_to(pr, timeout);
    }
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
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
            reader_permit permit,
            mutation_source source,
            const dht::partition_range& first_range,
            Generator generator,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state)
        : impl(s, std::move(permit))
        , _generator(std::move(generator))
        , _reader(source.make_reader(s, _permit, first_range, slice, pc, trace_state, streamed_mutation::forwarding::no, mutation_reader::forwarding::yes))
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

flat_mutation_reader
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
            return make_flat_mutation_reader<forwardable_empty_mutation_reader>(std::move(s), std::move(permit), std::move(source), slice, pc,
                    std::move(trace_state));
        } else {
            return make_empty_flat_reader(std::move(s), std::move(permit));
        }
    } else if (ranges.size() == 1) {
        return source.make_reader(std::move(s), std::move(permit), ranges.front(), slice, pc, std::move(trace_state), streamed_mutation::forwarding::no, fwd_mr);
    } else {
        return make_flat_mutation_reader<flat_multi_range_mutation_reader<adapter>>(std::move(s), std::move(permit), std::move(source),
                ranges.front(), adapter(std::next(ranges.cbegin()), ranges.cend()), slice, pc, std::move(trace_state));
    }
}

flat_mutation_reader
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
            return make_flat_mutation_reader<forwardable_empty_mutation_reader>(std::move(s), std::move(permit), std::move(source), slice, pc, std::move(trace_state));
        } else {
            return make_empty_flat_reader(std::move(s), std::move(permit));
        }
    } else {
        return make_flat_mutation_reader<flat_multi_range_mutation_reader<adapter>>(std::move(s), std::move(permit), std::move(source),
                *first_range, std::move(adapted_generator), slice, pc, std::move(trace_state));
    }
}

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment> fragments) {
    return make_flat_mutation_reader_from_fragments(std::move(schema), std::move(permit), std::move(fragments), query::full_partition_range);
}

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment> fragments, const dht::partition_range& pr) {
    class reader : public flat_mutation_reader::impl {
        std::deque<mutation_fragment> _fragments;
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
            _fragments.erase(_fragments.begin(), std::find_if(_fragments.begin(), _fragments.end(), [this] (const mutation_fragment& mf) {
                return mf.is_partition_start() && !_pr->before(mf.as_partition_start().key(), _cmp);
            }));
            _end_of_stream = end_of_range();
        }

    public:
        reader(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment> fragments, const dht::partition_range& pr)
                : flat_mutation_reader::impl(std::move(schema), std::move(permit))
                , _fragments(std::move(fragments))
                , _pr(&pr)
                , _cmp(*_schema) {
            do_fast_forward_to(*_pr);
        }
        virtual future<> fill_buffer(db::timeout_clock::time_point) override {
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
        virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
            throw std::runtime_error("This reader can't be fast forwarded to another range.");
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            do_fast_forward_to(pr);
            return make_ready_future<>();
        }
        virtual future<> close() noexcept override {
            return make_ready_future<>();
        }
    };
    return make_flat_mutation_reader<reader>(std::move(schema), std::move(permit), std::move(fragments), pr);
}

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr schema, reader_permit permit, std::deque<mutation_fragment> fragments,
        const dht::partition_range& pr, const query::partition_slice& slice) {
    std::optional<clustering_ranges_walker> ranges_walker;
    std::optional<range_tombstone_splitter> splitter;
    std::deque<mutation_fragment> filtered;
    for (auto&& mf : fragments) {
        switch (mf.mutation_fragment_kind()) {
            case mutation_fragment::kind::partition_start:
                ranges_walker.emplace(*schema, slice.row_ranges(*schema, mf.as_partition_start().key().key()), false);
                splitter.emplace(*schema, permit, *ranges_walker);
                filtered.emplace_back(std::move(mf));
                break;
            case mutation_fragment::kind::static_row:
                filtered.push_back(std::move(mf));
                break;
            case mutation_fragment::kind::partition_end:
                splitter->flush(position_in_partition::after_all_clustered_rows(), [&] (mutation_fragment mf) {
                    filtered.emplace_back(std::move(mf));
                });
                filtered.push_back(std::move(mf));
                break;
            case mutation_fragment::kind::clustering_row:
                splitter->flush(mf.position(), [&] (mutation_fragment mf) {
                    filtered.emplace_back(std::move(mf));
                });
                if (ranges_walker->advance_to(mf.position())) {
                    filtered.push_back(std::move(mf));
                }
                break;
            case mutation_fragment::kind::range_tombstone:
                splitter->consume(std::move(mf).as_range_tombstone(), [&] (mutation_fragment mf) {
                    filtered.emplace_back(std::move(mf));
                });
                break;
        }
    }
    return make_flat_mutation_reader_from_fragments(std::move(schema), std::move(permit), std::move(filtered), pr);
}

/*
 * This reader takes a get_next_fragment generator that produces mutation_fragment_opt which is returned by
 * generating_reader.
 *
 */
class generating_reader final : public flat_mutation_reader::impl {
    std::function<future<mutation_fragment_opt> ()> _get_next_fragment;
public:
    generating_reader(schema_ptr s, reader_permit permit, std::function<future<mutation_fragment_opt> ()> get_next_fragment)
        : impl(std::move(s), std::move(permit)), _get_next_fragment(std::move(get_next_fragment))
    { }
    virtual future<> fill_buffer(db::timeout_clock::time_point) override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return _get_next_fragment().then([this] (mutation_fragment_opt mopt) {
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
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        return make_ready_future<>();
    }
};

flat_mutation_reader make_generating_reader(schema_ptr s, reader_permit permit, std::function<future<mutation_fragment_opt> ()> get_next_fragment) {
    return make_flat_mutation_reader<generating_reader>(std::move(s), std::move(permit), std::move(get_next_fragment));
}

void flat_mutation_reader::do_upgrade_schema(const schema_ptr& s) {
    *this = transform(std::move(*this), schema_upgrader(s));
}

void flat_mutation_reader::on_close_error(std::unique_ptr<impl> i, std::exception_ptr ep) noexcept {
    impl* ip = i.get();
    on_internal_error_noexcept(fmr_logger,
            format("Failed to close {} [{}]: permit {}: {}", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description(), ep));
}

invalid_mutation_fragment_stream::invalid_mutation_fragment_stream(std::runtime_error e) : std::runtime_error(std::move(e)) {
}


mutation_fragment_stream_validator::mutation_fragment_stream_validator(const ::schema& s)
    : _schema(s)
    , _prev_kind(mutation_fragment::kind::partition_end)
    , _prev_pos(position_in_partition::end_of_partition_tag_t{})
    , _prev_partition_key(dht::minimum_token(), partition_key::make_empty()) {
}

bool mutation_fragment_stream_validator::operator()(const dht::decorated_key& dk) {
    if (_prev_partition_key.less_compare(_schema, dk)) {
        _prev_partition_key = dk;
        return true;
    }
    return false;
}

bool mutation_fragment_stream_validator::operator()(dht::token t) {
    if (_prev_partition_key.token() <= t) {
        _prev_partition_key._token = t;
        return true;
    }
    return false;
}

bool mutation_fragment_stream_validator::operator()(mutation_fragment::kind kind, position_in_partition_view pos) {
    if (_prev_kind == mutation_fragment::kind::partition_end) {
        const bool valid = (kind == mutation_fragment::kind::partition_start);
        if (valid) {
            _prev_kind = mutation_fragment::kind::partition_start;
            _prev_pos = pos;
        }
        return valid;
    }
    auto cmp = position_in_partition::tri_compare(_schema);
    auto res = cmp(_prev_pos, pos);
    bool valid = true;
    if (_prev_kind == mutation_fragment::kind::range_tombstone) {
        valid = res <= 0;
    } else {
        valid = res < 0;
    }
    if (valid) {
        _prev_kind = kind;
        _prev_pos = pos;
    }
    return valid;
}

bool mutation_fragment_stream_validator::operator()(const mutation_fragment& mf) {
    return (*this)(mf.mutation_fragment_kind(), mf.position());
}

bool mutation_fragment_stream_validator::operator()(mutation_fragment::kind kind) {
    bool valid = true;
    switch (_prev_kind) {
        case mutation_fragment::kind::partition_start:
            valid = kind != mutation_fragment::kind::partition_start;
            break;
        case mutation_fragment::kind::static_row: // fall-through
        case mutation_fragment::kind::clustering_row: // fall-through
        case mutation_fragment::kind::range_tombstone:
            valid = kind != mutation_fragment::kind::partition_start &&
                    kind != mutation_fragment::kind::static_row;
            break;
        case mutation_fragment::kind::partition_end:
            valid = kind == mutation_fragment::kind::partition_start;
            break;
    }
    if (valid) {
        _prev_kind = kind;
    }
    return valid;
}

bool mutation_fragment_stream_validator::on_end_of_stream() {
    return _prev_kind == mutation_fragment::kind::partition_end;
}

void mutation_fragment_stream_validator::reset(dht::decorated_key dk) {
    _prev_partition_key = dk;
    _prev_pos = position_in_partition::for_partition_start();
    _prev_kind = mutation_fragment::kind::partition_start;
}

void mutation_fragment_stream_validator::reset(const mutation_fragment& mf) {
    _prev_pos = mf.position();
    _prev_kind = mf.mutation_fragment_kind();
}

namespace {

[[noreturn]] void on_validation_error(seastar::logger& l, const seastar::sstring& reason) {
    try {
        on_internal_error(l, reason);
    } catch (std::runtime_error& e) {
        throw invalid_mutation_fragment_stream(e);
    }
}

}

bool mutation_fragment_stream_validating_filter::operator()(const dht::decorated_key& dk) {
    if (_validation_level < mutation_fragment_stream_validation_level::token) {
        return true;
    }
    if (_validation_level == mutation_fragment_stream_validation_level::token) {
        if (_validator(dk.token())) {
            return true;
        }
        on_validation_error(fmr_logger, format("[validator {} for {}] Unexpected token: previous {}, current {}",
                static_cast<void*>(this), _name, _validator.previous_token(), dk.token()));
    } else {
        if (_validator(dk)) {
            return true;
        }
        on_validation_error(fmr_logger, format("[validator {} for {}] Unexpected partition key: previous {}, current {}",
                static_cast<void*>(this), _name, _validator.previous_partition_key(), dk));
    }
}

mutation_fragment_stream_validating_filter::mutation_fragment_stream_validating_filter(sstring_view name, const schema& s,
        mutation_fragment_stream_validation_level level)
    : _validator(s)
    , _name(format("{} ({}.{} {})", name, s.ks_name(), s.cf_name(), s.id()))
    , _validation_level(level)
{
    if (fmr_logger.level() <= log_level::debug) {
        std::string_view what;
        switch (_validation_level) {
            case mutation_fragment_stream_validation_level::partition_region:
                what = "partition region";
                break;
            case mutation_fragment_stream_validation_level::token:
                what = "partition region and token";
                break;
            case mutation_fragment_stream_validation_level::partition_key:
                what = "partition region and partition key";
                break;
            case mutation_fragment_stream_validation_level::clustering_key:
                what = "partition region, partition key and clustering key";
                break;
        }
        fmr_logger.debug("[validator {} for {}] Will validate {} monotonicity.", static_cast<void*>(this), _name, what);
    }
}

bool mutation_fragment_stream_validating_filter::operator()(mutation_fragment::kind kind, position_in_partition_view pos) {
    bool valid = false;

    fmr_logger.debug("[validator {}] {}:{}", static_cast<void*>(this), kind, pos);

    if (_validation_level >= mutation_fragment_stream_validation_level::clustering_key) {
        valid = _validator(kind, pos);
    } else {
        valid = _validator(kind);
    }

    if (__builtin_expect(!valid, false)) {
        if (_validation_level >= mutation_fragment_stream_validation_level::clustering_key) {
            on_validation_error(fmr_logger, format("[validator {} for {}] Unexpected mutation fragment: previous {}:{}, current {}:{}",
                    static_cast<void*>(this), _name, _validator.previous_mutation_fragment_kind(), _validator.previous_position(), kind, pos));
        } else {
            on_validation_error(fmr_logger, format("[validator {} for {}] Unexpected mutation fragment: previous {}, current {}",
                    static_cast<void*>(this), _name, _validator.previous_mutation_fragment_kind(), kind));
        }
    }

    return true;
}

bool mutation_fragment_stream_validating_filter::operator()(const mutation_fragment& mv) {
    return (*this)(mv.mutation_fragment_kind(), mv.position());
}

bool mutation_fragment_stream_validating_filter::on_end_of_partition() {
    return (*this)(mutation_fragment::kind::partition_end, position_in_partition_view(position_in_partition_view::end_of_partition_tag_t()));
}

void mutation_fragment_stream_validating_filter::on_end_of_stream() {
    fmr_logger.debug("[validator {}] EOS", static_cast<const void*>(this));
    if (!_validator.on_end_of_stream()) {
        on_validation_error(fmr_logger, format("[validator {} for {}] Stream ended with unclosed partition: {}", static_cast<const void*>(this), _name,
                _validator.previous_mutation_fragment_kind()));
    }
}

flat_mutation_reader_v2& flat_mutation_reader_v2::operator=(flat_mutation_reader_v2&& o) noexcept {
    if (_impl) {
        impl* ip = _impl.get();
        // Abort to enforce calling close() before readers are closed
        // to prevent leaks and potential use-after-free due to background
        // tasks left behind.
        on_internal_error_noexcept(fmr_logger, format("{} [{}]: permit {}: was not closed before overwritten by move-assign", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description()));
        abort();
    }
    _impl = std::move(o._impl);
    return *this;
}

flat_mutation_reader_v2::~flat_mutation_reader_v2() {
    if (_impl) {
        impl* ip = _impl.get();
        // Abort to enforce calling close() before readers are closed
        // to prevent leaks and potential use-after-free due to background
        // tasks left behind.
        on_internal_error_noexcept(fmr_logger, format("{} [{}]: permit {}: was not closed before destruction", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description()));
        abort();
    }
}

static size_t compute_buffer_size(const schema& s, const flat_mutation_reader_v2::tracked_buffer& buffer)
{
    return boost::accumulate(
        buffer
        | boost::adaptors::transformed([&s] (const mutation_fragment_v2& mf) {
            return mf.memory_usage();
        }), size_t(0)
    );
}

void flat_mutation_reader_v2::impl::forward_buffer_to(const position_in_partition& pos) {
    clear_buffer();
    _buffer_size = compute_buffer_size(*_schema, _buffer);
}

void flat_mutation_reader_v2::impl::clear_buffer_to_next_partition() {
    auto next_partition_start = std::find_if(_buffer.begin(), _buffer.end(), [] (const mutation_fragment_v2& mf) {
        return mf.is_partition_start();
    });
    _buffer.erase(_buffer.begin(), next_partition_start);

    _buffer_size = compute_buffer_size(*_schema, _buffer);
}

template<typename Source>
future<bool> flat_mutation_reader_v2::impl::fill_buffer_from(Source& source, db::timeout_clock::time_point timeout) {
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

template future<bool> flat_mutation_reader_v2::impl::fill_buffer_from<flat_mutation_reader_v2>(flat_mutation_reader_v2&, db::timeout_clock::time_point);

void flat_mutation_reader_v2::do_upgrade_schema(const schema_ptr& s) {
    *this = transform(std::move(*this), schema_upgrader_v2(s));
}

future<mutation_opt> read_mutation_from_flat_mutation_reader(flat_mutation_reader_v2& r, db::timeout_clock::time_point timeout) {
    return r.consume(mutation_rebuilder_v2(r.schema()), timeout);
}

void flat_mutation_reader_v2::on_close_error(std::unique_ptr<impl> i, std::exception_ptr ep) noexcept {
    impl* ip = i.get();
    on_internal_error_noexcept(fmr_logger,
            format("Failed to close {} [{}]: permit {}: {}", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description(), ep));
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
        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            if (_end_of_stream) {
                return make_ready_future<>();
            }
            return _reader.consume_pausable(consumer{this}, timeout).then([this] {
                if (_reader.is_end_of_stream() && _reader.is_buffer_empty()) {
                    _rt_assembler.on_end_of_stream();
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
        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            clear_buffer();
            _end_of_stream = false;
            return _reader.fast_forward_to(pr, timeout);
        }
        virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
            clear_buffer();
            _end_of_stream = false;
            return _reader.fast_forward_to(std::move(pr), timeout);
        }
        virtual future<> close() noexcept override {
            return _reader.close();
        }
    };
    return make_flat_mutation_reader<transforming_reader>(std::move(r));
}

flat_mutation_reader_v2 upgrade_to_v2(flat_mutation_reader r) {
    class transforming_reader : public flat_mutation_reader_v2::impl {
        flat_mutation_reader _reader;
        struct consumer {
            transforming_reader* _owner;
            stop_iteration operator()(mutation_fragment&& mf) {
                std::move(mf).consume(*_owner);
                return stop_iteration(_owner->is_buffer_full());
            }
        };
        range_tombstone_change_generator _rt_gen;
        tombstone _current_rt;
        std::optional<position_range> _pr;
    public:
        void flush_tombstones(position_in_partition_view pos) {
            _rt_gen.flush(pos, [&] (range_tombstone_change rt) {
                _current_rt = rt.tombstone();
                push_mutation_fragment(*_schema, _permit, std::move(rt));
            });
        }
        void consume(static_row mf) {
            push_mutation_fragment(*_schema, _permit, std::move(mf));
        }
        void consume(clustering_row mf) {
            flush_tombstones(mf.position());
            push_mutation_fragment(*_schema, _permit, std::move(mf));
        }
        void consume(range_tombstone rt) {
            if (_pr) {
                if (!rt.trim_front(*_schema, _pr->start())) {
                    return;
                }
            }
            flush_tombstones(rt.position());
            _rt_gen.consume(std::move(rt));
        }
        void consume(partition_start mf) {
            _rt_gen.reset();
            _current_rt = {};
            _pr = std::nullopt;
            push_mutation_fragment(*_schema, _permit, std::move(mf));
        }
        void consume(partition_end mf) {
            flush_tombstones(position_in_partition::after_all_clustered_rows());
            if (_current_rt) {
                assert(!_pr);
                push_mutation_fragment(*_schema, _permit, range_tombstone_change(
                        position_in_partition::after_all_clustered_rows(), {}));
            }
            push_mutation_fragment(*_schema, _permit, std::move(mf));
        }
        transforming_reader(flat_mutation_reader&& r)
                : impl(r.schema(), r.permit())
                , _reader(std::move(r))
                , _rt_gen(*_schema)
        {}
        virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
            if (_end_of_stream) {
                return make_ready_future<>();
            }
            return _reader.consume_pausable(consumer{this}, timeout).then([this] {
                if (_reader.is_end_of_stream() && _reader.is_buffer_empty()) {
                    if (_pr) {
                        // If !_pr we should flush on partition_end
                        flush_tombstones(_pr->end());
                        if (_current_rt) {
                            push_mutation_fragment(*_schema, _permit, range_tombstone_change(_pr->end(), {}));
                        }
                    }
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
        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            clear_buffer();
            _end_of_stream = false;
            return _reader.fast_forward_to(pr, timeout);
        }
        virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
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
            _rt_gen.trim(pr.start());
            _current_rt = {};
            _pr = pr;
            _end_of_stream = false;
            return _reader.fast_forward_to(std::move(pr), timeout);
        }
        virtual future<> close() noexcept override {
            return _reader.close();
        }
    };
    return make_flat_mutation_reader_v2<transforming_reader>(std::move(r));
}
