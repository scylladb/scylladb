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

#include "mutation_reader.hh"
#include "core/future-util.hh"
#include "stdx.hh"
#include "reader_resource_tracker.hh"
#include "flat_mutation_reader.hh"


GCC6_CONCEPT(
    template<typename Producer>
    concept bool FragmentProducer = requires(Producer p, dht::partition_range part_range, position_range pos_range,
            db::timeout_clock::time_point timeout) {
        // The returned fragments are expected to have the same
        // position_in_partition. Iterators and references are expected
        // to be valid until the next call to operator()().
        { p() } -> future<boost::iterator_range<std::vector<mutation_fragment>::iterator>>;
        // These have the same semantics as their
        // flat_mutation_reader counterparts.
        { p.next_partition() };
        { p.fast_forward_to(part_range, timeout) } -> future<>;
        { p.fast_forward_to(pos_range, timeout) } -> future<>;
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

    future<> fetch() {
        if (!empty()) {
            return make_ready_future<>();
        }

        return _producer().then([this] (boost::iterator_range<iterator> fragments) {
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

    future<mutation_fragment_opt> operator()() {
        return fetch().then([this] () -> mutation_fragment_opt {
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
    dht::decorated_key_opt _key;
    const schema_ptr _schema;
    streamed_mutation::forwarding _fwd_sm;
    mutation_reader::forwarding _fwd_mr;
private:
    const dht::token* current_position() const;
    void maybe_add_readers(const dht::token* const t);
    void add_readers(std::vector<flat_mutation_reader> new_readers);
    future<> prepare_next();
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
    future<mutation_fragment_batch> operator()();
    void next_partition();
    future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout);
    future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout);
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
};

// Dumb selector implementation for combined_mutation_reader that simply
// forwards it's list of readers.
class list_reader_selector : public reader_selector {
    std::vector<flat_mutation_reader> _readers;

public:
    explicit list_reader_selector(schema_ptr s, std::vector<flat_mutation_reader> readers)
        : reader_selector(s, dht::ring_position::min())
        , _readers(std::move(readers)) {
    }

    list_reader_selector(const list_reader_selector&) = delete;
    list_reader_selector& operator=(const list_reader_selector&) = delete;

    list_reader_selector(list_reader_selector&&) = default;
    list_reader_selector& operator=(list_reader_selector&&) = default;

    virtual std::vector<flat_mutation_reader> create_new_readers(const dht::token* const) override {
        _selector_position = dht::ring_position::max();
        return std::exchange(_readers, {});
    }

    virtual std::vector<flat_mutation_reader> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point timeout) override {
        return {};
    }
};

void mutation_reader_merger::maybe_add_readers(const dht::token* const t) {
    if (!_selector->has_new_readers(t)) {
        return;
    }

    add_readers(_selector->create_new_readers(t));
}

void mutation_reader_merger::add_readers(std::vector<flat_mutation_reader> new_readers) {
    for (auto&& new_reader : new_readers) {
        _all_readers.emplace_back(std::move(new_reader));
        auto* r = &_all_readers.back();
        _next.emplace_back(r, mutation_fragment::kind::partition_end);
    }
}

const dht::token* mutation_reader_merger::current_position() const {
    if (!_key) {
        return nullptr;
    }

    return &_key->token();
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

future<> mutation_reader_merger::prepare_next() {
    return parallel_for_each(_next, [this] (reader_and_last_fragment_kind rk) {
        return (*rk.reader)().then([this, rk] (mutation_fragment_opt mfo) {
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
                _key = {};
            } else {
                _key = _reader_heap.front().fragment.as_partition_start().key();
            }

            maybe_add_readers(current_position());
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
    maybe_add_readers(nullptr);
}

future<mutation_reader_merger::mutation_fragment_batch> mutation_reader_merger::operator()() {
    // Avoid merging-related logic if we know that only a single reader owns
    // current partition.
    if (_single_reader.reader) {
        if (_single_reader.reader->is_buffer_empty()) {
            if (_single_reader.reader->is_end_of_stream()) {
                _current.clear();
                return make_ready_future<mutation_fragment_batch>(_current);
            }
            return _single_reader.reader->fill_buffer().then([this] { return operator()(); });
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
        return prepare_next().then([this] { return (*this)(); });
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
            _current.emplace_back(_fragment_heap.back().fragment);
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

combined_mutation_reader::combined_mutation_reader(schema_ptr schema,
        std::unique_ptr<reader_selector> selector,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr)
    : impl(std::move(schema))
    , _producer(_schema, mutation_reader_merger(_schema, std::move(selector), fwd_sm, fwd_mr))
    , _fwd_sm(fwd_sm) {
}

future<> combined_mutation_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    return repeat([this] {
        return _producer().then([this] (mutation_fragment_opt mfo) {
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

class reader_returning final : public mutation_reader::impl {
    streamed_mutation _m;
    bool _done = false;
public:
    reader_returning(streamed_mutation m) : _m(std::move(m)) {
    }
    virtual future<streamed_mutation_opt> operator()() override {
        if (_done) {
            return make_ready_future<streamed_mutation_opt>();
        } else {
            _done = true;
            return make_ready_future<streamed_mutation_opt>(std::move(_m));
        }
    }
};

mutation_reader make_reader_returning(mutation m, streamed_mutation::forwarding fwd) {
    return make_mutation_reader<reader_returning>(streamed_mutation_from_mutation(std::move(m), std::move(fwd)));
}

mutation_reader make_reader_returning(streamed_mutation m) {
    return make_mutation_reader<reader_returning>(std::move(m));
}

class reader_returning_many final : public mutation_reader::impl {
    std::vector<streamed_mutation> _m;
    dht::partition_range _pr;
public:
    reader_returning_many(std::vector<streamed_mutation> m, const dht::partition_range& pr) : _m(std::move(m)), _pr(pr) {
        boost::range::reverse(_m);
    }
    virtual future<streamed_mutation_opt> operator()() override {
        while (!_m.empty()) {
            auto& sm = _m.back();
            dht::ring_position_comparator cmp(*sm.schema());
            if (_pr.before(sm.decorated_key(), cmp)) {
                _m.pop_back();
            } else if (_pr.after(sm.decorated_key(), cmp)) {
                break;
            } else {
                auto m = std::move(sm);
                _m.pop_back();
                return make_ready_future<streamed_mutation_opt>(std::move(m));
            }
        }
        return make_ready_future<streamed_mutation_opt>();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        _pr = pr;
        return make_ready_future<>();
    }
};

mutation_reader make_reader_returning_many(std::vector<mutation> mutations, const query::partition_slice& slice, streamed_mutation::forwarding fwd) {
    std::vector<streamed_mutation> streamed_mutations;
    streamed_mutations.reserve(mutations.size());
    for (auto& m : mutations) {
        auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*m.schema(), slice, m.key());
        auto mp = mutation_partition(std::move(m.partition()), *m.schema(), std::move(ck_ranges));
        auto sm = streamed_mutation_from_mutation(mutation(m.schema(), m.decorated_key(), std::move(mp)), fwd);
        streamed_mutations.emplace_back(std::move(sm));
    }
    return make_mutation_reader<reader_returning_many>(std::move(streamed_mutations), query::full_partition_range);
}

mutation_reader make_reader_returning_many(std::vector<mutation> mutations, const dht::partition_range& pr) {
    std::vector<streamed_mutation> streamed_mutations;
    boost::range::transform(mutations, std::back_inserter(streamed_mutations), [] (auto& m) {
        return streamed_mutation_from_mutation(std::move(m));
    });
    return make_mutation_reader<reader_returning_many>(std::move(streamed_mutations), pr);
}

mutation_reader make_reader_returning_many(std::vector<streamed_mutation> mutations) {
    return make_mutation_reader<reader_returning_many>(std::move(mutations), query::full_partition_range);
}

class empty_reader final : public mutation_reader::impl {
public:
    virtual future<streamed_mutation_opt> operator()() override {
        return make_ready_future<streamed_mutation_opt>();
    }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point timeout) override {
        return make_ready_future<>();
    }
};

mutation_reader make_empty_reader() {
    return make_mutation_reader<empty_reader>();
}

// A file that tracks the memory usage of buffers resulting from read
// operations.
class tracking_file_impl : public file_impl {
    file _tracked_file;
    db::timeout_semaphore* _semaphore;

    // Shouldn't be called if semaphore is NULL.
    temporary_buffer<uint8_t> make_tracked_buf(temporary_buffer<uint8_t> buf) {
        return seastar::temporary_buffer<uint8_t>(buf.get_write(),
                buf.size(),
                make_deleter(buf.release(), std::bind(&db::timeout_semaphore::signal, _semaphore, buf.size())));
    }

public:
    tracking_file_impl(file file, reader_resource_tracker resource_tracker)
        : _tracked_file(std::move(file))
        , _semaphore(resource_tracker.get_semaphore()) {
    }

    tracking_file_impl(const tracking_file_impl&) = delete;
    tracking_file_impl& operator=(const tracking_file_impl&) = delete;
    tracking_file_impl(tracking_file_impl&&) = default;
    tracking_file_impl& operator=(tracking_file_impl&&) = default;

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->write_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->write_dma(pos, std::move(iov), pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->read_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->read_dma(pos, iov, pc);
    }

    virtual future<> flush(void) override {
        return get_file_impl(_tracked_file)->flush();
    }

    virtual future<struct stat> stat(void) override {
        return get_file_impl(_tracked_file)->stat();
    }

    virtual future<> truncate(uint64_t length) override {
        return get_file_impl(_tracked_file)->truncate(length);
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return get_file_impl(_tracked_file)->discard(offset, length);
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return get_file_impl(_tracked_file)->allocate(position, length);
    }

    virtual future<uint64_t> size(void) override {
        return get_file_impl(_tracked_file)->size();
    }

    virtual future<> close() override {
        return get_file_impl(_tracked_file)->close();
    }

    virtual std::unique_ptr<file_handle_impl> dup() override {
        return get_file_impl(_tracked_file)->dup();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return get_file_impl(_tracked_file)->list_directory(std::move(next));
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->dma_read_bulk(offset, range_size, pc).then([this] (temporary_buffer<uint8_t> buf) {
            if (_semaphore) {
                buf = make_tracked_buf(std::move(buf));
                _semaphore->consume(buf.size());
            }
            return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
        });
    }
};


file reader_resource_tracker::track(file f) const {
    return file(make_shared<tracking_file_impl>(f, *this));
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

        flat_mutation_reader operator()() {
            return _ms.make_flat_mutation_reader(std::move(_s), _range.get(), _slice.get(), _pc.get(), std::move(_trace_state), _fwd, _fwd_mr);
        }
    };

    const restricted_mutation_reader_config& _config;
    boost::variant<mutation_source_and_params, flat_mutation_reader> _reader_or_mutation_source;

    static const std::size_t new_reader_base_cost{16 * 1024};

    future<> create_reader(db::timeout_clock::time_point timeout) {
        auto f = timeout != db::no_timeout
                ? _config.resources_sem->wait(timeout, new_reader_base_cost)
                : _config.resources_sem->wait(new_reader_base_cost);

        return f.then([this] {
            flat_mutation_reader reader = boost::get<mutation_source_and_params>(_reader_or_mutation_source)();
            _reader_or_mutation_source = std::move(reader);

            if (_config.active_reads) {
                ++(*_config.active_reads);
            }

            return make_ready_future<>();
        });
    }

    template<typename Function>
    GCC6_CONCEPT(
        requires std::is_move_constructible<Function>::value
            && requires(Function fn, flat_mutation_reader& reader) {
                fn(reader);
            }
    )
    decltype(auto) with_reader(Function fn, db::timeout_clock::time_point timeout) {
        if (auto* reader = boost::get<flat_mutation_reader>(&_reader_or_mutation_source)) {
            return fn(*reader);
        }
        return create_reader(timeout).then([this, fn = std::move(fn)] () mutable {
            return fn(boost::get<flat_mutation_reader>(_reader_or_mutation_source));
        });
    }
public:
    restricting_mutation_reader(const restricted_mutation_reader_config& config,
            mutation_source ms,
            schema_ptr s,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr)
        : impl(s)
        , _config(config)
        , _reader_or_mutation_source(
                mutation_source_and_params{std::move(ms), std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr}) {
        if (_config.resources_sem->waiters() >= _config.max_queue_length) {
            _config.raise_queue_overloaded_exception();
        }
    }
    ~restricting_mutation_reader() {
        if (boost::get<flat_mutation_reader>(&_reader_or_mutation_source)) {
            _config.resources_sem->signal(new_reader_base_cost);
            if (_config.active_reads) {
                --(*_config.active_reads);
            }
        }
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
        if (auto* reader = boost::get<flat_mutation_reader>(&_reader_or_mutation_source)) {
            return reader->next_partition();
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
};

flat_mutation_reader
make_restricted_flat_reader(const restricted_mutation_reader_config& config,
                       mutation_source ms,
                       schema_ptr s,
                       const dht::partition_range& range,
                       const query::partition_slice& slice,
                       const io_priority_class& pc,
                       tracing::trace_state_ptr trace_state,
                       streamed_mutation::forwarding fwd,
                       mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<restricting_mutation_reader>(config, std::move(ms), std::move(s), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
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
            streamed_mutation::forwarding fwd) {
        return make_empty_reader();
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
            rd.emplace_back(ms.make_flat_mutation_reader(s, pr, slice, pc, tr, fwd));
        }
        return make_combined_reader(s, std::move(rd), fwd);
    });
}

mutation_reader mutation_reader_from_flat_mutation_reader(flat_mutation_reader&& mr) {
    class converting_reader final : public mutation_reader::impl {
        lw_shared_ptr<flat_mutation_reader> _mr;

        void move_to_next_partition() {
            _mr->next_partition();
        }
    public:
        converting_reader(flat_mutation_reader&& mr)
            : _mr(make_lw_shared<flat_mutation_reader>(std::move(mr)))
        { }

        virtual future<streamed_mutation_opt> operator()() override {
            class partition_reader final : public streamed_mutation::impl {
                lw_shared_ptr<flat_mutation_reader> _mr;
            public:
                partition_reader(lw_shared_ptr<flat_mutation_reader> mr, schema_ptr s, dht::decorated_key dk, tombstone t)
                    : streamed_mutation::impl(std::move(s), std::move(dk), std::move(t))
                    , _mr(std::move(mr))
                { }

                virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
                    if (_end_of_stream) {
                        return make_ready_future<>();
                    }
                    return _mr->consume_pausable([this] (mutation_fragment_opt&& mfopt) {
                        assert(bool(mfopt));
                        if (mfopt->is_end_of_partition()) {
                            _end_of_stream = true;
                            return stop_iteration::yes;
                        } else {
                            this->push_mutation_fragment(std::move(*mfopt));
                            return is_buffer_full() ? stop_iteration::yes : stop_iteration::no;
                        }
                    }, timeout).then([this] {
                        if (_mr->is_end_of_stream() && _mr->is_buffer_empty()) {
                            _end_of_stream = true;
                        }
                    });
                }

                virtual future<> fast_forward_to(position_range cr, db::timeout_clock::time_point timeout) {
                    forward_buffer_to(cr.start());
                    _end_of_stream = false;
                    return _mr->fast_forward_to(std::move(cr), timeout);
                }
            };
            move_to_next_partition();
            return (*_mr)().then([this] (auto&& mfopt) {
                if (!mfopt) {
                    return make_ready_future<streamed_mutation_opt>();
                }
                assert(mfopt->is_partition_start());
                partition_start& ph = mfopt->as_mutable_partition_start();
                return make_ready_future<streamed_mutation_opt>(
                    make_streamed_mutation<partition_reader>(_mr,
                                                     _mr->schema(),
                                                     std::move(ph.key()),
                                                     std::move(ph.partition_tombstone())));
            });
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
            return _mr->fast_forward_to(pr, timeout);
        }
    };
    return make_mutation_reader<converting_reader>(std::move(mr));
}

future<streamed_mutation_opt> streamed_mutation_from_flat_mutation_reader(flat_mutation_reader&& r) {
    return do_with(mutation_reader_from_flat_mutation_reader(std::move(r)), [] (auto&& rd) {
        return rd();
    });
}
