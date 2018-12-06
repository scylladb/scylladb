/*
 * Copyright (C) 2015 ScyllaDB
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
#include <boost/range/algorithm/reverse.hpp>
#include <boost/move/iterator.hpp>
#include <variant>

#include "mutation_reader.hh"
#include "core/future-util.hh"
#include "stdx.hh"
#include "flat_mutation_reader.hh"


GCC6_CONCEPT(
    template<typename Producer>
    concept bool FragmentProducer = requires(Producer p, dht::partition_range part_range, position_range pos_range,
            db::timeout_clock::time_point timeout) {
        // The returned fragments are expected to have the same
        // position_in_partition. Iterators and references are expected
        // to be valid until the next call to operator()().
        { p(timeout) } -> future<boost::iterator_range<std::vector<mutation_fragment>::iterator>>;
        // These have the same semantics as their
        // flat_mutation_reader counterparts.
        { p.next_partition() };
        { p.fast_forward_to(part_range, timeout) } -> future<>;
        { p.fast_forward_to(pos_range, timeout) } -> future<>;
        { p.buffer_size() } -> size_t;
    };
)

/**
 * Merge mutation-fragments produced by producer.
 *
 * Merge a non-decreasing stream of mutation-fragments into strictly
 * increasing stream. The merger is stateful, it's intended to be kept
 * around *at least* for merging an entire partition. That is, creating
 * a new instance for each batch of fragments will produce incorrect
 * results.
 *
 * Call operator() to get the next mutation fragment. operator() will
 * consume fragments from the producer using operator().
 * Any fast-forwarding has to be communicated to the merger object using
 * fast_forward_to() and next_partition(), as appropriate.
 */
template<class Producer>
GCC6_CONCEPT(
    requires FragmentProducer<Producer>
)
class mutation_fragment_merger {
    using iterator = std::vector<mutation_fragment>::iterator;

    const schema_ptr _schema;
    Producer _producer;
    iterator _it;
    iterator _end;

    future<> fetch(db::timeout_clock::time_point timeout) {
        if (!empty()) {
            return make_ready_future<>();
        }

        return _producer(timeout).then([this] (boost::iterator_range<iterator> fragments) {
            _it = fragments.begin();
            _end = fragments.end();
        });
    }

    bool empty() const {
        return _it == _end;
    }

    const mutation_fragment& top() const {
        return *_it;
    }

    mutation_fragment pop() {
        return std::move(*_it++);
    }

public:
    mutation_fragment_merger(schema_ptr schema, Producer&& producer)
        : _schema(std::move(schema))
        , _producer(std::move(producer)) {
    }

    future<mutation_fragment_opt> operator()(db::timeout_clock::time_point timeout) {
        return fetch(timeout).then([this] () -> mutation_fragment_opt {
            if (empty()) {
                return mutation_fragment_opt();
            }
            auto current = pop();
            while (!empty() && current.mergeable_with(top())) {
                current.apply(*_schema, pop());
            }
            return current;
        });
    }

    void next_partition() {
        _producer.next_partition();
    }

    future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
        return _producer.fast_forward_to(pr, timeout);
    }

    future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
        return _producer.fast_forward_to(std::move(pr), timeout);
    }

    size_t buffer_size() const {
        return _producer.buffer_size();
    }
};

// Merges the output of the sub-readers into a single non-decreasing
// stream of mutation-fragments.
class mutation_reader_merger {
public:
    struct reader_and_fragment {
        flat_mutation_reader* reader;
        mutation_fragment fragment;

        reader_and_fragment(flat_mutation_reader* r, mutation_fragment f)
            : reader(r)
            , fragment(std::move(f)) {
        }
    };

    struct reader_and_last_fragment_kind {
        flat_mutation_reader* reader = nullptr;
        mutation_fragment::kind last_kind = mutation_fragment::kind::partition_end;

        reader_and_last_fragment_kind() = default;

        reader_and_last_fragment_kind(flat_mutation_reader* r, mutation_fragment::kind k)
            : reader(r)
            , last_kind(k) {
        }
    };

    using mutation_fragment_batch = boost::iterator_range<std::vector<mutation_fragment>::iterator>;
private:
    struct reader_heap_compare;
    struct fragment_heap_compare;

    std::unique_ptr<reader_selector> _selector;
    // We need a list because we need stable addresses across additions
    // and removals.
    std::list<flat_mutation_reader> _all_readers;
    // Readers positioned at a partition, different from the one we are
    // reading from now. For these readers the attached fragment is
    // always partition_start. Used to pick the next partition.
    std::vector<reader_and_fragment> _reader_heap;
    // Readers and their current fragments, belonging to the current
    // partition.
    std::vector<reader_and_fragment> _fragment_heap;
    std::vector<reader_and_last_fragment_kind> _next;
    // Readers that reached EOS.
    std::vector<reader_and_last_fragment_kind> _halted_readers;
    std::vector<mutation_fragment> _current;
    // Optimisation for cases where only a single reader emits a particular
    // partition. If _single_reader.reader is not null that reader is
    // guaranteed to be the only one having relevant data until the partition
    // end, a call to next_partition() or a call to
    // fast_forward_to(dht::partition_range).
    reader_and_last_fragment_kind _single_reader;
    const schema_ptr _schema;
    streamed_mutation::forwarding _fwd_sm;
    mutation_reader::forwarding _fwd_mr;
private:
    void maybe_add_readers(const std::optional<dht::ring_position_view>& pos);
    void add_readers(std::vector<flat_mutation_reader> new_readers);
    future<> prepare_next(db::timeout_clock::time_point timeout);
    // Collect all forwardable readers into _next, and remove them from
    // their previous containers (_halted_readers and _fragment_heap).
    void prepare_forwardable_readers();
public:
    mutation_reader_merger(schema_ptr schema,
            std::unique_ptr<reader_selector> selector,
            streamed_mutation::forwarding fwd_sm,
            mutation_reader::forwarding fwd_mr);
    // Produces the next batch of mutation-fragments of the same
    // position.
    future<mutation_fragment_batch> operator()(db::timeout_clock::time_point timeout);
    void next_partition();
    future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout);
    future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout);
    size_t buffer_size() const;
};

// Combines multiple mutation_readers into one.
class combined_mutation_reader : public flat_mutation_reader::impl {
    mutation_fragment_merger<mutation_reader_merger> _producer;
    streamed_mutation::forwarding _fwd_sm;
public:
    // The specified streamed_mutation::forwarding and
    // mutation_reader::forwarding tag must be the same for all included
    // readers.
    combined_mutation_reader(schema_ptr schema,
            std::unique_ptr<reader_selector> selector,
            streamed_mutation::forwarding fwd_sm,
            mutation_reader::forwarding fwd_mr);
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;
    virtual void next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override;
    virtual size_t buffer_size() const override;
};

// Dumb selector implementation for combined_mutation_reader that simply
// forwards it's list of readers.
class list_reader_selector : public reader_selector {
    std::vector<flat_mutation_reader> _readers;

public:
    explicit list_reader_selector(schema_ptr s, std::vector<flat_mutation_reader> readers)
        : reader_selector(s, dht::ring_position_view::min())
        , _readers(std::move(readers)) {
    }

    list_reader_selector(const list_reader_selector&) = delete;
    list_reader_selector& operator=(const list_reader_selector&) = delete;

    list_reader_selector(list_reader_selector&&) = default;
    list_reader_selector& operator=(list_reader_selector&&) = default;

    virtual std::vector<flat_mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>&) override {
        _selector_position = dht::ring_position_view::max();
        return std::exchange(_readers, {});
    }

    virtual std::vector<flat_mutation_reader> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point timeout) override {
        return {};
    }
};

void mutation_reader_merger::maybe_add_readers(const std::optional<dht::ring_position_view>& pos) {
    if (_selector->has_new_readers(pos)) {
        add_readers(_selector->create_new_readers(pos));
    }
}

void mutation_reader_merger::add_readers(std::vector<flat_mutation_reader> new_readers) {
    for (auto&& new_reader : new_readers) {
        _all_readers.emplace_back(std::move(new_reader));
        auto* r = &_all_readers.back();
        _next.emplace_back(r, mutation_fragment::kind::partition_end);
    }
}

struct mutation_reader_merger::reader_heap_compare {
    const schema& s;

    explicit reader_heap_compare(const schema& s)
        : s(s) {
    }

    bool operator()(const mutation_reader_merger::reader_and_fragment& a, const mutation_reader_merger::reader_and_fragment& b) {
        // Invert comparison as this is a max-heap.
        return b.fragment.as_partition_start().key().less_compare(s, a.fragment.as_partition_start().key());
    }
};

struct mutation_reader_merger::fragment_heap_compare {
    position_in_partition::less_compare cmp;

    explicit fragment_heap_compare(const schema& s)
        : cmp(s) {
    }

    bool operator()(const mutation_reader_merger::reader_and_fragment& a, const mutation_reader_merger::reader_and_fragment& b) {
        // Invert comparison as this is a max-heap.
        return cmp(b.fragment.position(), a.fragment.position());
    }
};

future<> mutation_reader_merger::prepare_next(db::timeout_clock::time_point timeout) {
    return parallel_for_each(_next, [this, timeout] (reader_and_last_fragment_kind rk) {
        return (*rk.reader)(timeout).then([this, rk] (mutation_fragment_opt mfo) {
            if (mfo) {
                if (mfo->is_partition_start()) {
                    _reader_heap.emplace_back(rk.reader, std::move(*mfo));
                    boost::push_heap(_reader_heap, reader_heap_compare(*_schema));
                } else {
                    _fragment_heap.emplace_back(rk.reader, std::move(*mfo));
                    boost::range::push_heap(_fragment_heap, fragment_heap_compare(*_schema));
                }
            } else if (_fwd_sm == streamed_mutation::forwarding::yes && rk.last_kind != mutation_fragment::kind::partition_end) {
                // When in streamed_mutation::forwarding mode we need
                // to keep track of readers that returned
                // end-of-stream to know what readers to ff. We can't
                // just ff all readers as we might drop fragments from
                // partitions we haven't even read yet.
                // Readers whoose last emitted fragment was a partition
                // end are out of data for good for the current range.
                _halted_readers.push_back(rk);
            } else if (_fwd_mr == mutation_reader::forwarding::no) {
                _all_readers.remove_if([mr = rk.reader] (auto& r) { return &r == mr; });
            }
        });
    }).then([this] {
        _next.clear();

        // We are either crossing partition boundary or ran out of
        // readers. If there are halted readers then we are just
        // waiting for a fast-forward so there is nothing to do.
        if (_fragment_heap.empty() && _halted_readers.empty()) {
            if (_reader_heap.empty()) {
                maybe_add_readers(std::nullopt);
            } else {
                maybe_add_readers(_reader_heap.front().fragment.as_partition_start().key());
            }
        }
    });
}

void mutation_reader_merger::prepare_forwardable_readers() {
    _next.reserve(_halted_readers.size() + _fragment_heap.size() + _next.size());

    std::move(_halted_readers.begin(), _halted_readers.end(), std::back_inserter(_next));
    if (_single_reader.reader) {
        _next.emplace_back(std::exchange(_single_reader.reader, {}), _single_reader.last_kind);
    }
    for (auto& df : _fragment_heap) {
        _next.emplace_back(df.reader, df.fragment.mutation_fragment_kind());
    }

    _halted_readers.clear();
    _fragment_heap.clear();
}

mutation_reader_merger::mutation_reader_merger(schema_ptr schema,
        std::unique_ptr<reader_selector> selector,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr)
    : _selector(std::move(selector))
    , _schema(std::move(schema))
    , _fwd_sm(fwd_sm)
    , _fwd_mr(fwd_mr) {
    maybe_add_readers(std::nullopt);
}

future<mutation_reader_merger::mutation_fragment_batch> mutation_reader_merger::operator()(db::timeout_clock::time_point timeout) {
    // Avoid merging-related logic if we know that only a single reader owns
    // current partition.
    if (_single_reader.reader) {
        if (_single_reader.reader->is_buffer_empty()) {
            if (_single_reader.reader->is_end_of_stream()) {
                _current.clear();
                return make_ready_future<mutation_fragment_batch>(_current);
            }
            return _single_reader.reader->fill_buffer(timeout).then([this, timeout] { return operator()(timeout); });
        }
        _current.clear();
        _current.emplace_back(_single_reader.reader->pop_mutation_fragment());
        _single_reader.last_kind = _current.back().mutation_fragment_kind();
        if (_current.back().is_end_of_partition()) {
            _next.emplace_back(std::exchange(_single_reader.reader, {}), mutation_fragment::kind::partition_end);
        }
        return make_ready_future<mutation_fragment_batch>(_current);
    }

    if (!_next.empty()) {
        return prepare_next(timeout).then([this, timeout] { return (*this)(timeout); });
    }

    _current.clear();

    // If we ran out of fragments for the current partition, select the
    // readers for the next one.
    if (_fragment_heap.empty()) {
        if (!_halted_readers.empty() || _reader_heap.empty()) {
            return make_ready_future<mutation_fragment_batch>(_current);
        }

        auto key = [] (const std::vector<reader_and_fragment>& heap) -> const dht::decorated_key& {
            return heap.front().fragment.as_partition_start().key();
        };

        do {
            boost::range::pop_heap(_reader_heap, reader_heap_compare(*_schema));
            // All fragments here are partition_start so no need to
            // heap-sort them.
            _fragment_heap.emplace_back(std::move(_reader_heap.back()));
            _reader_heap.pop_back();
        }
        while (!_reader_heap.empty() && key(_fragment_heap).equal(*_schema, key(_reader_heap)));
        if (_fragment_heap.size() == 1) {
            _single_reader = { _fragment_heap.back().reader, mutation_fragment::kind::partition_start };
            _current.emplace_back(std::move(_fragment_heap.back().fragment));
            _fragment_heap.clear();
            return make_ready_future<mutation_fragment_batch>(_current);
        }
    }

    const auto equal = position_in_partition::equal_compare(*_schema);
    do {
        boost::range::pop_heap(_fragment_heap, fragment_heap_compare(*_schema));
        auto& n = _fragment_heap.back();
        const auto kind = n.fragment.mutation_fragment_kind();
        _current.emplace_back(std::move(n.fragment));
        _next.emplace_back(n.reader, kind);
        _fragment_heap.pop_back();
    }
    while (!_fragment_heap.empty() && equal(_current.back().position(), _fragment_heap.front().fragment.position()));

    return make_ready_future<mutation_fragment_batch>(_current);
}

void mutation_reader_merger::next_partition() {
    prepare_forwardable_readers();
    for (auto& rk : _next) {
        rk.last_kind = mutation_fragment::kind::partition_end;
        rk.reader->next_partition();
    }
}

future<> mutation_reader_merger::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    _single_reader = { };
    _next.clear();
    _halted_readers.clear();
    _fragment_heap.clear();
    _reader_heap.clear();

    return parallel_for_each(_all_readers, [this, &pr, timeout] (flat_mutation_reader& mr) {
        _next.emplace_back(&mr, mutation_fragment::kind::partition_end);
        return mr.fast_forward_to(pr, timeout);
    }).then([this, &pr, timeout] {
        add_readers(_selector->fast_forward_to(pr, timeout));
    });
}

future<> mutation_reader_merger::fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
    prepare_forwardable_readers();
    return parallel_for_each(_next, [this, pr = std::move(pr), timeout] (reader_and_last_fragment_kind rk) {
        return rk.reader->fast_forward_to(pr, timeout);
    });
}

size_t mutation_reader_merger::buffer_size() const {
    return boost::accumulate(_all_readers | boost::adaptors::transformed(std::mem_fn(&flat_mutation_reader::buffer_size)), size_t(0));
}

combined_mutation_reader::combined_mutation_reader(schema_ptr schema,
        std::unique_ptr<reader_selector> selector,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr)
    : impl(std::move(schema))
    , _producer(_schema, mutation_reader_merger(_schema, std::move(selector), fwd_sm, fwd_mr))
    , _fwd_sm(fwd_sm) {
}

future<> combined_mutation_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    return repeat([this, timeout] {
        return _producer(timeout).then([this] (mutation_fragment_opt mfo) {
            if (!mfo) {
                _end_of_stream = true;
                return stop_iteration::yes;
            }
            push_mutation_fragment(std::move(*mfo));
            if (is_buffer_full()) {
                return stop_iteration::yes;
            }
            return stop_iteration::no;
        });
    });
}

void combined_mutation_reader::next_partition() {
    if (_fwd_sm == streamed_mutation::forwarding::yes) {
        clear_buffer();
        _end_of_stream = false;
        _producer.next_partition();
    } else {
        clear_buffer_to_next_partition();
        // If the buffer is empty at this point then all fragments in it
        // belonged to the current partition, so either:
        // * All (forwardable) readers are still positioned in the
        // inside of the current partition, or
        // * They are between the current one and the next one.
        // Either way we need to call next_partition on them.
        if (is_buffer_empty()) {
            _producer.next_partition();
        }
    }
}

future<> combined_mutation_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    clear_buffer();
    _end_of_stream = false;
    return _producer.fast_forward_to(pr, timeout);
}

future<> combined_mutation_reader::fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
    forward_buffer_to(pr.start());
    _end_of_stream = false;
    return _producer.fast_forward_to(std::move(pr), timeout);
}

size_t combined_mutation_reader::buffer_size() const {
    return flat_mutation_reader::impl::buffer_size() + _producer.buffer_size();
}

flat_mutation_reader make_combined_reader(schema_ptr schema,
        std::unique_ptr<reader_selector> selectors,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<combined_mutation_reader>(schema,
            std::move(selectors),
            fwd_sm,
            fwd_mr);
}

flat_mutation_reader make_combined_reader(schema_ptr schema,
        std::vector<flat_mutation_reader> readers,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    if (readers.size() == 1) {
        return std::move(readers.front());
    }
    return make_flat_mutation_reader<combined_mutation_reader>(schema,
            std::make_unique<list_reader_selector>(schema, std::move(readers)),
            fwd_sm,
            fwd_mr);
}

flat_mutation_reader make_combined_reader(schema_ptr schema,
        flat_mutation_reader&& a,
        flat_mutation_reader&& b,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    std::vector<flat_mutation_reader> v;
    v.reserve(2);
    v.push_back(std::move(a));
    v.push_back(std::move(b));
    return make_combined_reader(std::move(schema), std::move(v), fwd_sm, fwd_mr);
}

class restricting_mutation_reader : public flat_mutation_reader::impl {
    struct mutation_source_and_params {
        mutation_source _ms;
        schema_ptr _s;
        std::reference_wrapper<const dht::partition_range> _range;
        std::reference_wrapper<const query::partition_slice> _slice;
        std::reference_wrapper<const io_priority_class> _pc;
        tracing::trace_state_ptr _trace_state;
        streamed_mutation::forwarding _fwd;
        mutation_reader::forwarding _fwd_mr;

        flat_mutation_reader operator()(reader_resource_tracker tracker) {
            return _ms.make_reader(std::move(_s), _range.get(), _slice.get(), _pc.get(), std::move(_trace_state), _fwd, _fwd_mr, tracker);
        }
    };

    struct pending_state {
        reader_concurrency_semaphore& semaphore;
        mutation_source_and_params reader_factory;
    };
    struct admitted_state {
        lw_shared_ptr<reader_concurrency_semaphore::reader_permit> permit;
        flat_mutation_reader reader;
    };
    std::variant<pending_state, admitted_state> _state;

    static const ssize_t new_reader_base_cost{16 * 1024};

    template<typename Function>
    GCC6_CONCEPT(
        requires std::is_move_constructible<Function>::value
            && requires(Function fn, flat_mutation_reader& reader) {
                fn(reader);
            }
    )
    decltype(auto) with_reader(Function fn, db::timeout_clock::time_point timeout) {
        if (auto* state = std::get_if<admitted_state>(&_state)) {
            return fn(state->reader);
        }

        return std::get<pending_state>(_state).semaphore.wait_admission(new_reader_base_cost,
                timeout).then([this, fn = std::move(fn)] (lw_shared_ptr<reader_concurrency_semaphore::reader_permit> permit) mutable {
            auto reader_factory = std::move(std::get<pending_state>(_state).reader_factory);
            _state.emplace<admitted_state>(admitted_state{permit, reader_factory(reader_resource_tracker(permit))});
            return fn(std::get<admitted_state>(_state).reader);
        });
    }
public:
    restricting_mutation_reader(reader_concurrency_semaphore& semaphore,
            mutation_source ms,
            schema_ptr s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr)
        : impl(s)
        , _state(pending_state{semaphore,
                mutation_source_and_params{std::move(ms), std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr}}) {
    }

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        return with_reader([this, timeout] (flat_mutation_reader& reader) {
            return reader.fill_buffer(timeout).then([this, &reader] {
                _end_of_stream = reader.is_end_of_stream();
                while (!reader.is_buffer_empty()) {
                    push_mutation_fragment(reader.pop_mutation_fragment());
                }
            });
        }, timeout);
    }
    virtual void next_partition() override {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            return;
        }
        _end_of_stream = false;
        if (auto* state = std::get_if<admitted_state>(&_state)) {
            return state->reader.next_partition();
        }
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        clear_buffer();
        _end_of_stream = false;
        return with_reader([&pr, timeout] (flat_mutation_reader& reader) {
            return reader.fast_forward_to(pr, timeout);
        }, timeout);
    }
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        forward_buffer_to(pr.start());
        _end_of_stream = false;
        return with_reader([pr = std::move(pr), timeout] (flat_mutation_reader& reader) mutable {
            return reader.fast_forward_to(std::move(pr), timeout);
        }, timeout);
    }
    virtual size_t buffer_size() const override {
        if (auto* state = std::get_if<admitted_state>(&_state)) {
            return state->reader.buffer_size();
        }
        return 0;
    }
};

flat_mutation_reader
make_restricted_flat_reader(reader_concurrency_semaphore& semaphore,
                       mutation_source ms,
                       schema_ptr s,
                       const dht::partition_range& range,
                       const query::partition_slice& slice,
                       const io_priority_class& pc,
                       tracing::trace_state_ptr trace_state,
                       streamed_mutation::forwarding fwd,
                       mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<restricting_mutation_reader>(semaphore, std::move(ms), std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
}


snapshot_source make_empty_snapshot_source() {
    return snapshot_source([] {
        return make_empty_mutation_source();
    });
}

mutation_source make_empty_mutation_source() {
    return mutation_source([](schema_ptr s,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding,
            reader_resource_tracker) {
        return make_empty_flat_reader(s);
    }, [] {
        return [] (const dht::decorated_key& key) {
            return partition_presence_checker_result::definitely_doesnt_exist;
        };
    });
}

mutation_source make_combined_mutation_source(std::vector<mutation_source> addends) {
    return mutation_source([addends = std::move(addends)] (schema_ptr s,
            const dht::partition_range& pr,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr tr,
            streamed_mutation::forwarding fwd) {
        std::vector<flat_mutation_reader> rd;
        rd.reserve(addends.size());
        for (auto&& ms : addends) {
            rd.emplace_back(ms.make_reader(s, pr, slice, pc, tr, fwd));
        }
        return make_combined_reader(s, std::move(rd), fwd);
    });
}

/// See make_foreign_reader() for description.
class foreign_reader : public flat_mutation_reader::impl {
    template <typename T>
    using foreign_unique_ptr = foreign_ptr<std::unique_ptr<T>>;

    using fragment_buffer = circular_buffer<mutation_fragment>;

    foreign_unique_ptr<flat_mutation_reader> _reader;
    foreign_unique_ptr<future<>> _read_ahead_future;
    // Set this flag when next_partition() is called.
    // This pending call will be executed the next time we go to the remote
    // reader (a fill_buffer() or a fast_forward_to() call).
    bool _pending_next_partition = false;
    streamed_mutation::forwarding _fwd_sm;

    // Forward an operation to the reader on the remote shard.
    // If the remote reader has an ongoing read-ahead, bring it to the
    // foreground (wait on it) and execute the operation after.
    // After the operation completes, kick off a new read-ahead (fill_buffer())
    // and move it to the background (save it's future but don't wait on it
    // now). If all works well read-aheads complete by the next operation and
    // we don't have to wait on the remote reader filling its buffer.
    template <typename Operation, typename Result = futurize_t<std::result_of_t<Operation()>>>
    Result forward_operation(db::timeout_clock::time_point timeout, Operation op) {
        return smp::submit_to(_reader.get_owner_shard(), [reader = _reader.get(),
                read_ahead_future = std::exchange(_read_ahead_future, nullptr),
                pending_next_partition = std::exchange(_pending_next_partition, false),
                timeout,
                op = std::move(op)] () mutable {
            auto exec_op_and_read_ahead = [=] () mutable {
                if (pending_next_partition) {
                    reader->next_partition();
                }
                return op().then([=] (auto... results) {
                    auto f = reader->is_end_of_stream() ? nullptr : std::make_unique<future<>>(reader->fill_buffer(timeout));
                    return make_ready_future<foreign_unique_ptr<future<>>, decltype(results)...>(
                                make_foreign(std::move(f)), std::move(results)...);
                });
            };
            if (read_ahead_future) {
                return read_ahead_future->then(std::move(exec_op_and_read_ahead));
            } else {
                return exec_op_and_read_ahead();
            }
        }).then([this] (foreign_unique_ptr<future<>> new_read_ahead_future, auto... results) {
            _read_ahead_future = std::move(new_read_ahead_future);
            return make_ready_future<decltype(results)...>(std::move(results)...);
        });
    }

    void update_buffer_with(foreign_unique_ptr<fragment_buffer> buffer, bool end_of_steam);
public:
    foreign_reader(schema_ptr schema,
            foreign_unique_ptr<flat_mutation_reader> reader,
            streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no);

    ~foreign_reader();

    // this is captured.
    foreign_reader(const foreign_reader&) = delete;
    foreign_reader& operator=(const foreign_reader&) = delete;
    foreign_reader(foreign_reader&&) = delete;
    foreign_reader& operator=(foreign_reader&&) = delete;

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;
    virtual void next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override;

    const mutation_fragment& peek_buffer() const { return buffer().front(); }
    const circular_buffer<mutation_fragment>& get_buffer() const { return buffer(); }

    future<foreign_unique_ptr<flat_mutation_reader>> pause();
    void resume(foreign_unique_ptr<flat_mutation_reader> reader);

    future<reader_lifecycle_policy::paused_or_stopped_reader> stop();
};

void foreign_reader::update_buffer_with(foreign_unique_ptr<fragment_buffer> buffer, bool end_of_steam) {
    _end_of_stream = end_of_steam;
    for (const auto& mf : *buffer) {
        // Need a copy since the mf is on the remote shard.
        push_mutation_fragment(mutation_fragment(*_schema, mf));
    }
}

foreign_reader::foreign_reader(schema_ptr schema,
        foreign_unique_ptr<flat_mutation_reader> reader,
        streamed_mutation::forwarding fwd_sm)
    : impl(std::move(schema))
    , _reader(std::move(reader))
    , _fwd_sm(fwd_sm) {
}

foreign_reader::~foreign_reader() {
    if (!_read_ahead_future && !_reader) {
        return;
    }
    smp::submit_to(_reader.get_owner_shard(), [reader = std::move(_reader), read_ahead_future = std::move(_read_ahead_future)] () mutable {
        if (read_ahead_future) {
            return read_ahead_future->finally([r = std::move(reader)] {});
        }
        return make_ready_future<>();
    });
}

future<> foreign_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    if (_end_of_stream || is_buffer_full()) {
        return make_ready_future();
    }

    return forward_operation(timeout, [reader = _reader.get(), timeout] () {
        auto f = reader->is_buffer_empty() ? reader->fill_buffer(timeout) : make_ready_future<>();
        return f.then([=] {
            return make_ready_future<foreign_unique_ptr<fragment_buffer>, bool>(
                    std::make_unique<fragment_buffer>(reader->detach_buffer()),
                    reader->is_end_of_stream());
        });
    }).then([this] (foreign_unique_ptr<fragment_buffer> buffer, bool end_of_stream) mutable {
        update_buffer_with(std::move(buffer), end_of_stream);
    });
}

void foreign_reader::next_partition() {
    if (_fwd_sm == streamed_mutation::forwarding::yes) {
        clear_buffer();
        _end_of_stream = false;
        _pending_next_partition = true;
    } else {
        clear_buffer_to_next_partition();
        if (is_buffer_empty()) {
            _end_of_stream = false;
            _pending_next_partition = true;
        }
    }
}

future<> foreign_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    clear_buffer();
    _end_of_stream = false;
    return forward_operation(timeout, [reader = _reader.get(), &pr, timeout] () {
        return reader->fast_forward_to(pr, timeout);
    });
}

future<> foreign_reader::fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
    forward_buffer_to(pr.start());
    _end_of_stream = false;
    return forward_operation(timeout, [reader = _reader.get(), pr = std::move(pr), timeout] () {
        return reader->fast_forward_to(std::move(pr), timeout);
    });
}

future<reader_lifecycle_policy::paused_or_stopped_reader> foreign_reader::stop() {
    if (_reader && (_read_ahead_future || _pending_next_partition)) {
        const auto owner_shard = _reader.get_owner_shard();
        return smp::submit_to(owner_shard, [reader = _reader.get(),
                read_ahead_future = std::exchange(_read_ahead_future, nullptr),
                pending_next_partition = std::exchange(_pending_next_partition, false)] () mutable {
            auto fut = read_ahead_future ? std::move(*read_ahead_future) : make_ready_future<>();
            return fut.then([=] () mutable {
                if (pending_next_partition) {
                    reader->next_partition();
                }
            });
        }).then([this] {
            return reader_lifecycle_policy::paused_or_stopped_reader{std::move(_reader), detach_buffer(), false};
        });
    } else {
        return make_ready_future<reader_lifecycle_policy::paused_or_stopped_reader>(
                reader_lifecycle_policy::paused_or_stopped_reader{std::move(_reader), detach_buffer(), _pending_next_partition});
    }
}

future<foreign_ptr<std::unique_ptr<flat_mutation_reader>>> foreign_reader::pause() {
    return smp::submit_to(_reader.get_owner_shard(), [reader = _reader.get(),
            read_ahead_future = std::exchange(_read_ahead_future, nullptr),
            pending_next_partition = std::exchange(_pending_next_partition, false)] () mutable {
        auto fut = read_ahead_future ? std::move(*read_ahead_future) : make_ready_future<>();
        return fut.then([=] () mutable {
            if (pending_next_partition) {
                reader->next_partition();
            }
            return make_ready_future<foreign_unique_ptr<fragment_buffer>, bool>(
                    std::make_unique<fragment_buffer>(reader->detach_buffer()),
                    reader->is_end_of_stream());
        });
    }).then([this] (foreign_unique_ptr<fragment_buffer>&& buffer, bool end_of_stream) mutable {
        update_buffer_with(std::move(buffer), end_of_stream);

        // An ongoing pause() might overlap with a next_partition() call.
        // So if there is a pending next partition, try to execute it again
        // after the remote buffer was transferred. This is required for
        // correctness, otherwise some fragments belonging to the to-be-skipped
        // partition can escape the next_partition() call, both on the local and
        // the remote shard.
        if (_pending_next_partition) {
            _pending_next_partition = false;
            next_partition();
        }
        return std::move(_reader);
    });
}

void foreign_reader::resume(foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader) {
    _reader = std::move(reader);
}

flat_mutation_reader make_foreign_reader(schema_ptr schema,
            foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader,
            streamed_mutation::forwarding fwd_sm) {
    if (reader.get_owner_shard() == engine().cpu_id()) {
        return std::move(*reader);
    }
    return make_flat_mutation_reader<foreign_reader>(std::move(schema), std::move(reader), fwd_sm);
}

// See make_multishard_combining_reader() for description.
class multishard_combining_reader : public flat_mutation_reader::impl {
    shared_ptr<reader_lifecycle_policy> _lifecycle_policy;
    const dht::i_partitioner& _partitioner;
    const dht::partition_range* _pr;
    const query::partition_slice& _ps;
    const io_priority_class& _pc;
    tracing::trace_state_ptr _trace_state;
    const mutation_reader::forwarding _fwd_mr;

    // Thin wrapper around a flat_mutation_reader (foreign_reader) that
    // lazy-creates the reader when needed and transparently keeps track
    // of read-ahead.
    // Shard reader instances have to stay alive until all pending read-ahead
    // completes. But at the same time we don't want to do any additional work
    // after the parent reader was destroyed. To solve this we do two things:
    // * Move flat_mutation_reader instance into a struct managed through a
    //   shared pointer. Continuations using this internal state will share
    //   owhership of this struct with the shard reader instance.
    // * Add a stopped flag to the struct which will be set when the shard
    //   reader is destroyed. When this is set don't do any work in the
    //   pending continuations, just "run through them".
    class shard_reader {
        struct state {
            std::unique_ptr<foreign_reader> reader;
            bool stopped = false;
            bool drop_partition_start = false;
            bool drop_static_row = false;
        };
        const multishard_combining_reader& _parent;
        const unsigned _shard;
        lw_shared_ptr<state> _state;
        std::optional<future<>> _read_ahead;
        std::optional<future<>> _pause;

        std::optional<dht::decorated_key> _last_pkey;
        std::optional<position_in_partition> _last_position_in_partition;
        // These are used when the reader has to be recreated (after having been
        // evicted while paused) and the range and/or slice it is recreated with
        // differs from the original ones.
        std::optional<dht::partition_range> _range_override;
        std::optional<query::partition_slice> _slice_override;

    private:
        void update_last_position();
        void adjust_partition_slice();
        future<foreign_ptr<std::unique_ptr<flat_mutation_reader>>> recreate_reader();
        future<> resume();
        future<> do_fill_buffer(db::timeout_clock::time_point timeout);

    public:
        shard_reader(multishard_combining_reader& parent, unsigned shard)
            : _parent(parent)
            , _shard(shard)
            , _state(make_lw_shared<state>()) {
        }

        shard_reader(shard_reader&&) = default;
        shard_reader& operator=(shard_reader&&) = delete;

        shard_reader(const shard_reader&) = delete;
        shard_reader& operator=(const shard_reader&) = delete;

        ~shard_reader();

        // These methods assume the reader is already created.
        bool is_end_of_stream() const {
            return _state->reader->is_end_of_stream();
        }
        bool is_buffer_empty() const {
            return _state->reader->is_buffer_empty();
        }
        mutation_fragment pop_mutation_fragment() {
            return _state->reader->pop_mutation_fragment();
        }
        const mutation_fragment& peek_buffer() const {
            return _state->reader->peek_buffer();
        }
        future<> fill_buffer(db::timeout_clock::time_point timeout);

        // These methods don't assume the reader is already created.
        void next_partition();
        future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout);
        future<> create_reader();
        explicit operator bool() const {
            return bool(_state->reader);
        }
        bool done() const {
            return _state->reader && _state->reader->is_buffer_empty() && _state->reader->is_end_of_stream();
        }
        void read_ahead(db::timeout_clock::time_point timeout);
        bool is_read_ahead_in_progress() const {
            return _read_ahead.has_value();
        }
        void pause();
    };

    std::vector<shard_reader> _shard_readers;
    unsigned _current_shard;
    dht::token _next_token;
    bool _crossed_shards;
    unsigned _concurrency = 1;

    void move_to_next_shard();
    future<> handle_empty_reader_buffer(db::timeout_clock::time_point timeout);

public:
    multishard_combining_reader(
            shared_ptr<reader_lifecycle_policy> lifecycle_policy,
            const dht::i_partitioner& partitioner,
            schema_ptr s,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr);

    // this is captured.
    multishard_combining_reader(const multishard_combining_reader&) = delete;
    multishard_combining_reader& operator=(const multishard_combining_reader&) = delete;
    multishard_combining_reader(multishard_combining_reader&&) = delete;
    multishard_combining_reader& operator=(multishard_combining_reader&&) = delete;

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;
    virtual void next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override;
};

multishard_combining_reader::shard_reader::~shard_reader() {
    // Nothing to do if there was no reader created, nor is there a background
    // read ahead in progress which will create one.
    if (!_state->reader && !_read_ahead) {
        return;
    }

    _state->stopped = true;

    auto f = [this] {
        if (_read_ahead) {
            return std::move(*_read_ahead);
        } else if (_pause) {
            return std::move(*_pause);
        } else {
            return make_ready_future<>();
        }
    }();

    _parent._lifecycle_policy->destroy_reader(_shard, f.then([state = _state.get()] {
        return state->reader->stop();
    }).finally([state = _state] {}));
}

void multishard_combining_reader::shard_reader::update_last_position() {
    auto& reader = *_state->reader;
    if (reader.is_buffer_empty()) {
        return;
    }

    auto rbegin = std::reverse_iterator(reader.get_buffer().end());
    auto rend = std::reverse_iterator(reader.get_buffer().begin());
    if (auto pk_it = std::find_if(rbegin, rend, std::mem_fn(&mutation_fragment::is_partition_start)); pk_it != rend) {
        _last_pkey = pk_it->as_partition_start().key();
    }

    _last_position_in_partition.emplace(reader.get_buffer().back().position());
}

void multishard_combining_reader::shard_reader::adjust_partition_slice() {
    if (!_slice_override) {
        _slice_override = _parent._ps;
    }

    const auto& schema = *_parent._schema;
    _slice_override->clear_range(schema, _last_pkey->key());
    auto& last_ckey = _last_position_in_partition->key();

    auto cmp = bound_view::compare(schema);
    auto eq = clustering_key_prefix::equality(schema);

    auto ranges = _slice_override->default_row_ranges();
    auto it = ranges.begin();
    while (it != ranges.end()) {
        auto range = bound_view::from_range(*it);
        if (cmp(range.second, last_ckey) || eq(range.second.prefix(), last_ckey)) {
            it = ranges.erase(it);
        } else {
            if (cmp(range.first, last_ckey)) {
                assert(cmp(last_ckey, range.second));
                *it = query::clustering_range(query::clustering_range::bound{last_ckey, false}, it->end());
            }
            ++it;
        }
    }

    _slice_override->clear_ranges();
    _slice_override->set_range(schema, _last_pkey->key(), std::move(ranges));
}

future<foreign_ptr<std::unique_ptr<flat_mutation_reader>>> multishard_combining_reader::shard_reader::recreate_reader() {
    const dht::partition_range* range = _parent._pr;
    const query::partition_slice* slice = &_parent._ps;

    if (_last_pkey) {
        bool partition_range_is_inclusive = true;

        if (_last_position_in_partition) {
            switch (_last_position_in_partition->region()) {
            case partition_region::partition_start:
                _state->drop_partition_start = true;
                break;
            case partition_region::static_row:
                _state->drop_partition_start = true;
                _state->drop_static_row = true;
                break;
            case partition_region::clustered:
                _state->drop_partition_start = true;
                _state->drop_static_row = true;
                adjust_partition_slice();
                slice = &*_slice_override;
                break;
            case partition_region::partition_end:
                partition_range_is_inclusive = false;
                break;
            }
        }

        // The original range contained a single partition and we've read it
        // all. We'd have to create a reader with an empty range that would
        // immediately be at EOS. This is not possible so just don't recreate
        // the reader.
        // This should be extremely rare (who'd create a multishard reader to
        // read a single partition) but still, let's make sure we handle it
        // correctly.
        if (_parent._pr->is_singular() && !partition_range_is_inclusive) {
            return make_ready_future<foreign_ptr<std::unique_ptr<flat_mutation_reader>>>();
        }

        _range_override = dht::partition_range({dht::partition_range::bound(*_last_pkey, partition_range_is_inclusive)}, _parent._pr->end());
        range = &*_range_override;
    }

    return _parent._lifecycle_policy->create_reader(
            _shard,
            _parent._schema,
            *range,
            *slice,
            _parent._pc,
            _parent._trace_state,
            _parent._fwd_mr);
}

future<> multishard_combining_reader::shard_reader::resume() {
    return std::exchange(_pause, std::nullopt)->then([this, state = _state] {
        if (state->stopped) {
            return make_ready_future<>();
        }
        return _parent._lifecycle_policy->try_resume(_shard).then(
                [this, state = std::move(state)] (foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader) mutable {
            if (reader) {
                state->reader->resume(std::move(reader));
                return make_ready_future<>();
            } else if (state->stopped) {
                return make_ready_future<>();
            } else {
                return recreate_reader().then([this, state = std::move(state)] (foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader) {
                    state->reader->resume(std::move(reader));
                });
            }
        });
    });
}

future<> multishard_combining_reader::shard_reader::do_fill_buffer(db::timeout_clock::time_point timeout) {
    return _state->reader->fill_buffer(timeout).then([this, state = _state] {
        auto& reader = *state->reader;

        if (reader.is_buffer_empty()) {
            return;
        }
        if (state->drop_partition_start) {
            state->drop_partition_start = false;
            if (reader.peek_buffer().is_partition_start()) {
                reader.pop_mutation_fragment();
            }
        }

        if (reader.is_buffer_empty()) {
            return;
        }
        if (state->drop_static_row) {
            state->drop_static_row = false;
            if (reader.peek_buffer().is_static_row()) {
                reader.pop_mutation_fragment();
            }
        }

        if (!state->stopped) {
            update_last_position();
        }
    });
}

future<> multishard_combining_reader::shard_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    if (_read_ahead) {
        return *std::exchange(_read_ahead, std::nullopt);
    }
    if (!_state->reader->is_buffer_empty()) {
        return make_ready_future<>();
    }
    if (_pause) {
        return resume().then([this, timeout] {
            return fill_buffer(timeout);
        });
    }
    return do_fill_buffer(timeout);
}

void multishard_combining_reader::shard_reader::next_partition() {
    _last_position_in_partition = position_in_partition(position_in_partition::end_of_partition_tag_t{});

    // The only case this can be called with an uncreated reader is when
    // `next_partition()` is called on the multishard reader before the
    // first `fill_buffer()` call. In this case we are right before the first
    // partition so this call has no effect, hence we can ignore it.
    if (_state->reader) {
        _state->reader->next_partition();
    }
}

future<> multishard_combining_reader::shard_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    if (_state->reader) {
        _last_pkey.reset();
        _last_position_in_partition.reset();

        auto do_fast_forward = [this, &pr, timeout] {
            return _state->reader->fast_forward_to(pr, timeout);
        };

        if (_pause) {
            return resume().then(std::move(do_fast_forward));
        }

        if (_read_ahead) {
            return std::exchange(_read_ahead, std::nullopt)->then(std::move(do_fast_forward));
        }

        return do_fast_forward();
    }
    // No need to fast-forward uncreated readers, they will be passed the new
    // range when created.
    return make_ready_future<>();
}

future<> multishard_combining_reader::shard_reader::create_reader() {
    if (_state->reader) {
        return make_ready_future<>();
    }
    if (_read_ahead) {
        return *std::exchange(_read_ahead, std::nullopt);
    }
    return _parent._lifecycle_policy->create_reader(_shard, _parent._schema, *_parent._pr, _parent._ps, _parent._pc, _parent._trace_state,
            _parent._fwd_mr).then(
            [schema = _parent._schema, state = _state] (foreign_ptr<std::unique_ptr<flat_mutation_reader>>&& r) mutable {
        state->reader = std::make_unique<foreign_reader>(std::move(schema), std::move(r));
    });
}

void multishard_combining_reader::shard_reader::read_ahead(db::timeout_clock::time_point timeout) {
    if (_read_ahead || (_state->reader && (_state->reader->is_end_of_stream() || !_state->reader->is_buffer_empty()))) {
        return;
    }

    auto f = _state->reader
        ? (_pause ? resume() : make_ready_future<>())
        : create_reader();

    _read_ahead.emplace(f.then([this, state = _state, timeout] () mutable {
        if (state->stopped) {
            return make_ready_future<>();
        }
        return do_fill_buffer(timeout).then([this, state = std::move(state)] {
            // Read ahead is still in the background, so pause the reader.
            if (!state->stopped && _read_ahead) {
                pause();
            }
        });
    }));
}

void multishard_combining_reader::shard_reader::pause() {
    if (_pause) {
        return;
    }
    auto f = _read_ahead ? *std::exchange(_read_ahead, std::nullopt) : make_ready_future<>();
    _pause = f.then([this, state = _state] () mutable {
        if (state->stopped) {
            return make_ready_future<>();
        }
        return state->reader->pause().then([this, state = std::move(state)] (foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader) {
            if (state->stopped) {
                state->reader->resume(std::move(reader));
                return make_ready_future<>();
            }

            // When pausing, the content of the remote reader's buffer is transferred to
            // the foreign reader, so we might need to update the last position.
            update_last_position();

            return _parent._lifecycle_policy->pause(std::move(reader));
        });
    });
}

void multishard_combining_reader::move_to_next_shard() {
    _crossed_shards = true;
    _current_shard = (_current_shard + 1) % _partitioner.shard_count();
    _next_token = _partitioner.token_for_next_shard(_next_token, _current_shard);
}

future<> multishard_combining_reader::handle_empty_reader_buffer(db::timeout_clock::time_point timeout) {
    auto& reader = _shard_readers[_current_shard];

    if (reader.is_end_of_stream()) {
        if (std::all_of(_shard_readers.begin(), _shard_readers.end(), std::mem_fn(&shard_reader::done))) {
            _end_of_stream = true;
        } else {
            move_to_next_shard();
        }
        reader.pause();
        return make_ready_future<>();
    } else if (reader.is_read_ahead_in_progress()) {
        return reader.fill_buffer(timeout);
    } else {
        // If we crossed shards and the next reader has an empty buffer we
        // double concurrency so the next time we cross shards we will have
        // more chances of hitting the reader's buffer.
        if (_crossed_shards) {
            _concurrency = std::min(_concurrency * 2, _partitioner.shard_count());

            // If concurrency > 1 we kick-off concurrency-1 read-aheads in the
            // background. They will be brought to the foreground when we move
            // to their respective shard.
            for (unsigned i = 1; i < _concurrency; ++i) {
                _shard_readers[(_current_shard + i) % _partitioner.shard_count()].read_ahead(timeout);
            }
        }
        return reader.fill_buffer(timeout);
    }
}

multishard_combining_reader::multishard_combining_reader(
        shared_ptr<reader_lifecycle_policy> lifecycle_policy,
        const dht::i_partitioner& partitioner,
        schema_ptr s,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr)
    : impl(s)
    , _lifecycle_policy(std::move(lifecycle_policy))
    , _partitioner(partitioner)
    , _pr(&pr)
    , _ps(ps)
    , _pc(pc)
    , _trace_state(std::move(trace_state))
    , _fwd_mr(fwd_mr)
    , _current_shard(pr.start() ? _partitioner.shard_of(pr.start()->value().token()) : _partitioner.shard_of_minimum_token())
    , _next_token(_partitioner.token_for_next_shard(pr.start() ? pr.start()->value().token() : dht::minimum_token(),
                (_current_shard + 1) % _partitioner.shard_count())) {
    _shard_readers.reserve(_partitioner.shard_count());
    for (unsigned i = 0; i < _partitioner.shard_count(); ++i) {
        _shard_readers.emplace_back(*this, i);
    }
}

future<> multishard_combining_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    _crossed_shards = false;
    return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this, timeout] {
        auto& reader = _shard_readers[_current_shard];
        if (!reader) {
            return reader.create_reader();
        }

        if (reader.is_buffer_empty()) {
            return handle_empty_reader_buffer(timeout);
        }

        while (!reader.is_buffer_empty() && !is_buffer_full()) {
            if (const auto& mf = reader.peek_buffer(); mf.is_partition_start() && mf.as_partition_start().key().token() >= _next_token) {
                move_to_next_shard();
                reader.pause();
                return make_ready_future<>();
            }
            push_mutation_fragment(reader.pop_mutation_fragment());
        }
        return make_ready_future<>();
    });
}

void multishard_combining_reader::next_partition() {
    clear_buffer_to_next_partition();
    if (is_buffer_empty()) {
        _shard_readers[_current_shard].next_partition();
    }
}

future<> multishard_combining_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    if (pr.start()) {
        auto& t = pr.start()->value().token();
        _current_shard = _partitioner.shard_of(t);
        _next_token = _partitioner.token_for_next_shard(t, (_current_shard + 1) % _partitioner.shard_count());
    } else {
        _current_shard = _partitioner.shard_of_minimum_token();
        _next_token = _partitioner.token_for_next_shard(dht::minimum_token(), (_current_shard + 1) % _partitioner.shard_count());
    }
    _pr = &pr;
    clear_buffer();
    _end_of_stream = false;
    return parallel_for_each(_shard_readers, [this, timeout] (shard_reader& sr) {
        return sr.fast_forward_to(*_pr, timeout);
    });
}

future<> multishard_combining_reader::fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
    return make_exception_future<>(std::bad_function_call());
}

flat_mutation_reader make_multishard_combining_reader(
        shared_ptr<reader_lifecycle_policy> lifecycle_policy,
        const dht::i_partitioner& partitioner,
        schema_ptr schema,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<multishard_combining_reader>(std::move(lifecycle_policy), partitioner, std::move(schema), pr, ps, pc,
            std::move(trace_state), fwd_mr);
}
