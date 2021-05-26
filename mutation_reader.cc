/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/closeable.hh>

#include "mutation_reader.hh"
#include "flat_mutation_reader.hh"
#include "schema_registry.hh"
#include "mutation_compactor.hh"
#include "dht/sharder.hh"

logging::logger mrlog("mutation_reader");

static constexpr size_t merger_small_vector_size = 4;

template<typename T>
using merger_vector = utils::small_vector<T, merger_small_vector_size>;

using mutation_fragment_batch = boost::iterator_range<merger_vector<mutation_fragment>::iterator>;

template<typename Producer>
concept FragmentProducer = requires(Producer p, dht::partition_range part_range, position_range pos_range,
        db::timeout_clock::time_point timeout) {
    // The returned fragments are expected to have the same
    // position_in_partition. Iterators and references are expected
    // to be valid until the next call to operator()().
    { p(timeout) } -> std::same_as<future<mutation_fragment_batch>>;

    // The following functions have the same semantics as their
    // flat_mutation_reader counterparts.
    { p.next_partition() } -> std::same_as<future<>>;
    { p.fast_forward_to(part_range, timeout) } -> std::same_as<future<>>;
    { p.fast_forward_to(pos_range, timeout) } -> std::same_as<future<>>;
};

/**
 * Merge mutation-fragments produced by producer.
 *
 * Merge a non-decreasing stream of mutation fragment batches into
 * a non-decreasing stream of mutation fragments.
 *
 * A batch is a sequence of fragments. For each such batch we merge
 * the maximal mergeable subsequences of fragments and emit them
 * as single fragments.
 *
 * For example, a batch <f1, f2, f3, f4, f5>, where f1 and f2 are mergeable,
 * f2 is not mergeable with f3, f3 is not mergeable with f4, and f4 and f5
 * are mergeable, will result in the following sequence:
 * merge(f1, f2), f3, merge(f4, f5).
 *
 * The merger is stateful, it's intended to be kept
 * around *at least* for merging an entire partition. That is, creating
 * a new instance for each batch of fragments will produce incorrect
 * results.
 *
 * Call operator() to get the next mutation fragment. operator() will
 * consume batches from the producer using operator().
 * Any fast-forwarding has to be communicated to the merger object using
 * fast_forward_to() and next_partition(), as appropriate.
 */
template<class Producer>
requires FragmentProducer<Producer>
class mutation_fragment_merger {
    using iterator = merger_vector<mutation_fragment>::iterator;

    const schema_ptr _schema;
    Producer _producer;
    iterator _it{};
    iterator _end{};

    future<> fetch(db::timeout_clock::time_point timeout) {
        if (!empty()) {
            return make_ready_future<>();
        }

        return _producer(timeout).then([this] (mutation_fragment_batch fragments) {
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

    future<> next_partition() {
        return _producer.next_partition();
    }

    future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
        return _producer.fast_forward_to(pr, timeout);
    }

    future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
        return _producer.fast_forward_to(std::move(pr), timeout);
    }

    future<> close() noexcept {
        return _producer.close();
    }
};

// Merges the output of the sub-readers into a single non-decreasing
// stream of mutation-fragments.
class mutation_reader_merger {
public:
    using reader_iterator = std::list<flat_mutation_reader>::iterator;

    struct reader_and_fragment {
        reader_iterator reader{};
        mutation_fragment fragment;

        reader_and_fragment(reader_iterator r, mutation_fragment f)
            : reader(r)
            , fragment(std::move(f)) {
        }
    };

    struct reader_and_last_fragment_kind {
        reader_iterator reader{};
        mutation_fragment::kind last_kind = mutation_fragment::kind::partition_end;

        reader_and_last_fragment_kind() = default;

        reader_and_last_fragment_kind(reader_iterator r, mutation_fragment::kind k)
            : reader(r)
            , last_kind(k) {
        }
    };

    // Determines how many times a fragment should be taken from the same
    // reader in order to enter gallop mode. Must be greater than one.
    static constexpr int gallop_mode_entering_threshold = 3;
private:
    struct reader_heap_compare;
    struct fragment_heap_compare;

    struct needs_merge_tag { };
    using needs_merge = bool_class<needs_merge_tag>;

    struct reader_galloping_tag { };
    using reader_galloping = bool_class<reader_galloping_tag>;

    std::unique_ptr<reader_selector> _selector;
    // We need a list because we need stable addresses across additions
    // and removals.
    std::list<flat_mutation_reader> _all_readers;
    // We remove unneeded readers in batches. Until it is their time they
    // are kept in _to_remove.
    std::list<flat_mutation_reader> _to_remove;
    // Readers positioned at a partition, different from the one we are
    // reading from now. For these readers the attached fragment is
    // always partition_start. Used to pick the next partition.
    merger_vector<reader_and_fragment> _reader_heap;
    // Readers and their current fragments, belonging to the current
    // partition.
    merger_vector<reader_and_fragment> _fragment_heap;
    merger_vector<reader_and_last_fragment_kind> _next;
    // Readers that reached EOS.
    merger_vector<reader_and_last_fragment_kind> _halted_readers;
    merger_vector<mutation_fragment> _current;
    // Optimisation for cases where only a single reader emits a particular
    // partition. If _single_reader.reader is not null that reader is
    // guaranteed to be the only one having relevant data until the partition
    // end, a call to next_partition() or a call to
    // fast_forward_to(dht::partition_range).
    reader_and_last_fragment_kind _single_reader;
    // Holds a reference to the reader that previously contributed a fragment.
    // When a reader consecutively contributes a certain number of fragments,
    // gallop mode becomes enabled. In this mode, it is assumed that
    // the _galloping_reader will keep producing winning fragments.
    reader_and_last_fragment_kind _galloping_reader;
    // Counts how many times the _galloping_reader contributed a fragment
    // before entering the gallop mode. It can also be equal to 0, meaning
    // that the gallop mode was stopped (galloping reader lost to some other reader).
    int _gallop_mode_hits = 0;
    const schema_ptr _schema;
    streamed_mutation::forwarding _fwd_sm;
    mutation_reader::forwarding _fwd_mr;
private:
    void maybe_add_readers_at_partition_boundary();
    void maybe_add_readers(const std::optional<dht::ring_position_view>& pos);
    void add_readers(std::vector<flat_mutation_reader> new_readers);
    bool in_gallop_mode() const;
    future<needs_merge> prepare_one(db::timeout_clock::time_point timeout, reader_and_last_fragment_kind rk, reader_galloping reader_galloping);
    future<needs_merge> advance_galloping_reader(db::timeout_clock::time_point timeout);
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
    future<> next_partition();
    future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout);
    future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout);
    future<> close() noexcept;
};

/* Merge a non-decreasing stream of mutation fragment batches
 * produced by a FragmentProducer into a non-decreasing stream
 * of mutation fragments.
 *
 * See `mutation_fragment_merger` for details.
 *
 * This class is a simple adapter over `mutation_fragment_merger` that provides
 * a `flat_mutation_reader` interface. */
template <FragmentProducer Producer>
class merging_reader : public flat_mutation_reader::impl {
    mutation_fragment_merger<Producer> _merger;
    streamed_mutation::forwarding _fwd_sm;
public:
    merging_reader(schema_ptr schema,
            reader_permit permit,
            streamed_mutation::forwarding fwd_sm,
            Producer&& producer)
        : impl(std::move(schema), std::move(permit))
        , _merger(_schema, std::move(producer))
        , _fwd_sm(fwd_sm) {}

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override;
    virtual future<> close() noexcept override;
};

// Dumb selector implementation for mutation_reader_merger that simply
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
        _next.emplace_back(std::prev(_all_readers.end()), mutation_fragment::kind::partition_end);
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

bool mutation_reader_merger::in_gallop_mode() const {
    return _gallop_mode_hits >= gallop_mode_entering_threshold;
}

void mutation_reader_merger::maybe_add_readers_at_partition_boundary() {
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
}

future<mutation_reader_merger::needs_merge> mutation_reader_merger::advance_galloping_reader(db::timeout_clock::time_point timeout) {
    return prepare_one(timeout, _galloping_reader, reader_galloping::yes).then([this] (needs_merge needs_merge) {
        maybe_add_readers_at_partition_boundary();
        return needs_merge;
    });
}

future<> mutation_reader_merger::prepare_next(db::timeout_clock::time_point timeout) {
    return parallel_for_each(_next, [this, timeout] (reader_and_last_fragment_kind rk) {
        return prepare_one(timeout, rk, reader_galloping::no).discard_result();
    }).then([this] {
        _next.clear();
        maybe_add_readers_at_partition_boundary();
    });
}

future<mutation_reader_merger::needs_merge> mutation_reader_merger::prepare_one(db::timeout_clock::time_point timeout,
        reader_and_last_fragment_kind rk, reader_galloping reader_galloping) {
    return (*rk.reader)(timeout).then([this, rk, reader_galloping] (mutation_fragment_opt mfo) {
        auto to_close = make_ready_future<>();
        if (mfo) {
            if (mfo->is_partition_start()) {
                _reader_heap.emplace_back(rk.reader, std::move(*mfo));
                boost::push_heap(_reader_heap, reader_heap_compare(*_schema));
            } else {
                if (reader_galloping) {
                    // Optimization: assume that galloping reader will keep winning, and compare directly with the heap front.
                    // If this assumption is correct, we do one key comparison instead of pushing to/popping from the heap.
                    if (_fragment_heap.empty() || position_in_partition::less_compare(*_schema)(mfo->position(), _fragment_heap.front().fragment.position())) {
                        _current.clear();
                        _current.push_back(std::move(*mfo));
                        _galloping_reader.last_kind = _current.back().mutation_fragment_kind();
                        return make_ready_future<needs_merge>(needs_merge::no);
                    }

                    _gallop_mode_hits = 0;
                }

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
            _to_remove.splice(_to_remove.end(), _all_readers, rk.reader);
            if (_to_remove.size() >= 4) {
                auto to_remove = std::move(_to_remove);
                to_close = parallel_for_each(to_remove, [] (flat_mutation_reader& r) {
                    return r.close();
                });
                if (reader_galloping) {
                    // Galloping reader iterator may have become invalid at this point, so - to be safe - clear it
                    auto fut = _galloping_reader.reader->close();
                    to_close = when_all_succeed(std::move(to_close), std::move(fut)).discard_result();
                }
            }
        }

        if (reader_galloping) {
            _gallop_mode_hits = 0;
        }
      // to_close is a chain of flat_mutation_reader close futures,
      // therefore it can not fail.
      return to_close.then([] {
        return needs_merge::yes;
      });
    });
}

void mutation_reader_merger::prepare_forwardable_readers() {
    _next.reserve(_halted_readers.size() + _fragment_heap.size() + _next.size());

    std::move(_halted_readers.begin(), _halted_readers.end(), std::back_inserter(_next));
    if (_single_reader.reader != reader_iterator{}) {
        _next.emplace_back(std::exchange(_single_reader.reader, {}), _single_reader.last_kind);
    }
    if (in_gallop_mode()) {
        _next.emplace_back(_galloping_reader);
        _gallop_mode_hits = 0;
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

future<mutation_fragment_batch> mutation_reader_merger::operator()(db::timeout_clock::time_point timeout) {
    // Avoid merging-related logic if we know that only a single reader owns
    // current partition.
    if (_single_reader.reader != reader_iterator{}) {
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

    if (in_gallop_mode()) {
        return advance_galloping_reader(timeout).then([this, timeout] (needs_merge needs_merge) {
            if (!needs_merge) {
                return make_ready_future<mutation_fragment_batch>(_current);
            }
            // Galloping reader may have lost to some other reader. In that case, we should proceed
            // with standard merging logic.
            return (*this)(timeout);
        });
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

        auto key = [] (const merger_vector<reader_and_fragment>& heap) -> const dht::decorated_key& {
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
            _gallop_mode_hits = 0;
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

    if (_next.size() == 1 && _next.front().reader == _galloping_reader.reader) {
        ++_gallop_mode_hits;
        if (in_gallop_mode()) {
            _galloping_reader.last_kind = _next.front().last_kind;
            _next.clear();
        }
    } else {
        _galloping_reader.reader = _next.front().reader;
        _gallop_mode_hits = 1;
    }

    return make_ready_future<mutation_fragment_batch>(_current);
}

future<> mutation_reader_merger::next_partition() {
    // If the last batch of fragments returned by operator() came from partition P,
    // we must forward to the partition immediately following P (as per the `next_partition`
    // contract in `flat_mutation_reader`).
    //
    // The readers in _next are those which returned the last batch of fragments, thus they are
    // currently positioned either inside P or at the end of P, hence we need to forward them.
    // Readers in _fragment_heap (or the _galloping_reader, if we're currently galloping) are obviously still in P,
    // so we also need to forward those. Finally, _halted_readers must have been halted after returning
    // a fragment from P, hence must be forwarded.
    //
    // The only readers that we must not forward are those in _reader_heap, since they already are positioned
    // at the start of the next partition.
    prepare_forwardable_readers();
    for (auto& rk : _next) {
        rk.last_kind = mutation_fragment::kind::partition_end;
        co_await rk.reader->next_partition();
    }
}

future<> mutation_reader_merger::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    _single_reader = { };
    _gallop_mode_hits = 0;
    _next.clear();
    _halted_readers.clear();
    _fragment_heap.clear();
    _reader_heap.clear();

    for (auto it = _all_readers.begin(); it != _all_readers.end(); ++it) {
        _next.emplace_back(it, mutation_fragment::kind::partition_end);
    }
    return parallel_for_each(_all_readers, [this, &pr, timeout] (flat_mutation_reader& mr) {
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

future<> mutation_reader_merger::close() noexcept {
    return parallel_for_each(std::move(_to_remove), [] (flat_mutation_reader& mr) {
        return mr.close();
    }).then([this] {
        return parallel_for_each(std::move(_all_readers), [] (flat_mutation_reader& mr) {
            return mr.close();
        });
    });
}

template <FragmentProducer P>
future<> merging_reader<P>::fill_buffer(db::timeout_clock::time_point timeout) {
    return repeat([this, timeout] {
        return _merger(timeout).then([this] (mutation_fragment_opt mfo) {
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

template <FragmentProducer P>
future<> merging_reader<P>::next_partition() {
    if (_fwd_sm == streamed_mutation::forwarding::yes) {
        clear_buffer();
        _end_of_stream = false;
        return _merger.next_partition();
    } else {
        clear_buffer_to_next_partition();
        // If the buffer is empty at this point then all fragments in it
        // belonged to the current partition, hence the last fragment produced
        // by the producer came from the current partition, meaning that the producer
        // is still inside the current partition.
        // Thus we need to call next_partition on it (see the `next_partition` contract
        // of `flat_mutation_reader`, which `FragmentProducer` follows).
        if (is_buffer_empty()) {
            return _merger.next_partition();
        }
    }
    return make_ready_future<>();
}

template <FragmentProducer P>
future<> merging_reader<P>::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    clear_buffer();
    _end_of_stream = false;
    return _merger.fast_forward_to(pr, timeout);
}

template <FragmentProducer P>
future<> merging_reader<P>::fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
    forward_buffer_to(pr.start());
    _end_of_stream = false;
    return _merger.fast_forward_to(std::move(pr), timeout);
}

template <FragmentProducer P>
future<> merging_reader<P>::close() noexcept {
    return _merger.close();
}

flat_mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        std::unique_ptr<reader_selector> selector,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<merging_reader<mutation_reader_merger>>(schema,
            std::move(permit),
            fwd_sm,
            mutation_reader_merger(schema, std::move(selector), fwd_sm, fwd_mr));
}

flat_mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        std::vector<flat_mutation_reader> readers,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    if (readers.empty()) {
        return make_empty_flat_reader(std::move(schema), std::move(permit));
    }
    if (readers.size() == 1) {
        return std::move(readers.front());
    }
    return make_combined_reader(schema,
            std::move(permit),
            std::make_unique<list_reader_selector>(schema, std::move(readers)),
            fwd_sm,
            fwd_mr);
}

flat_mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        flat_mutation_reader&& a,
        flat_mutation_reader&& b,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    std::vector<flat_mutation_reader> v;
    v.reserve(2);
    v.push_back(std::move(a));
    v.push_back(std::move(b));
    return make_combined_reader(std::move(schema), std::move(permit), std::move(v), fwd_sm, fwd_mr);
}

const ssize_t new_reader_base_cost{16 * 1024};

class restricting_mutation_reader : public flat_mutation_reader::impl {
    struct mutation_source_and_params {
        mutation_source _ms;
        schema_ptr _s;
        reader_permit _permit;
        std::reference_wrapper<const dht::partition_range> _range;
        std::reference_wrapper<const query::partition_slice> _slice;
        std::reference_wrapper<const io_priority_class> _pc;
        tracing::trace_state_ptr _trace_state;
        streamed_mutation::forwarding _fwd;
        mutation_reader::forwarding _fwd_mr;

        flat_mutation_reader operator()() {
            return _ms.make_reader(std::move(_s), std::move(_permit), _range.get(), _slice.get(), _pc.get(), std::move(_trace_state), _fwd, _fwd_mr);
        }
    };

    struct pending_state {
        mutation_source_and_params reader_factory;
    };
    struct admitted_state {
        flat_mutation_reader reader;
        reader_permit::resource_units units;
    };
    std::variant<pending_state, admitted_state> _state;

    template<typename Function>
    requires std::is_move_constructible<Function>::value
        && requires(Function fn, flat_mutation_reader& reader) {
            fn(reader);
        }
    decltype(auto) with_reader(Function fn, db::timeout_clock::time_point timeout) {
        if (auto* state = std::get_if<admitted_state>(&_state)) {
            return fn(state->reader);
        }

        return std::get<pending_state>(_state).reader_factory._permit.wait_admission(new_reader_base_cost,
                timeout).then([this, fn = std::move(fn)] (reader_permit::resource_units units) mutable {
            auto reader_factory = std::move(std::get<pending_state>(_state).reader_factory);
            _state.emplace<admitted_state>(admitted_state{reader_factory(), std::move(units)});
            return fn(std::get<admitted_state>(_state).reader);
        });
    }
public:
    restricting_mutation_reader(
            mutation_source ms,
            schema_ptr s,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr)
        : impl(s, permit)
        , _state(pending_state{
                mutation_source_and_params{std::move(ms), std::move(s), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr}}) {
    }

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        return with_reader([this, timeout] (flat_mutation_reader& reader) {
            return reader.fill_buffer(timeout).then([this, &reader] {
                _end_of_stream = reader.is_end_of_stream();
                reader.move_buffer_content_to(*this);
            });
        }, timeout);
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            return make_ready_future<>();
        }
        _end_of_stream = false;
        if (auto* state = std::get_if<admitted_state>(&_state)) {
            return state->reader.next_partition();
        }
        return make_ready_future<>();
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
    virtual future<> close() noexcept override {
        if (auto* state = std::get_if<admitted_state>(&_state)) {
            return state->reader.close();
        }
        return make_ready_future<>();
    }
};

flat_mutation_reader
make_restricted_flat_reader(
                       mutation_source ms,
                       schema_ptr s,
                       reader_permit permit,
                       const dht::partition_range& range,
                       const query::partition_slice& slice,
                       const io_priority_class& pc,
                       tracing::trace_state_ptr trace_state,
                       streamed_mutation::forwarding fwd,
                       mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<restricting_mutation_reader>(std::move(ms), std::move(s), std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
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
            streamed_mutation::forwarding fwd) {
        std::vector<flat_mutation_reader> rd;
        rd.reserve(addends.size());
        for (auto&& ms : addends) {
            rd.emplace_back(ms.make_reader(s, permit, pr, slice, pc, tr, fwd));
        }
        return make_combined_reader(s, std::move(permit), std::move(rd), fwd);
    });
}

namespace {

struct remote_fill_buffer_result {
    foreign_ptr<std::unique_ptr<const flat_mutation_reader::tracked_buffer>> buffer;
    bool end_of_stream = false;

    remote_fill_buffer_result() = default;
    remote_fill_buffer_result(flat_mutation_reader::tracked_buffer&& buffer, bool end_of_stream)
        : buffer(make_foreign(std::make_unique<const flat_mutation_reader::tracked_buffer>(std::move(buffer))))
        , end_of_stream(end_of_stream) {
    }
};

}

/// See make_foreign_reader() for description.
class foreign_reader : public flat_mutation_reader::impl {
    template <typename T>
    using foreign_unique_ptr = foreign_ptr<std::unique_ptr<T>>;

    using fragment_buffer = flat_mutation_reader::tracked_buffer;

    foreign_unique_ptr<flat_mutation_reader> _reader;
    foreign_unique_ptr<future<>> _read_ahead_future;
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
                timeout,
                op = std::move(op)] () mutable {
            auto exec_op_and_read_ahead = [=] () mutable {
                // Not really variadic, we expect 0 (void) or 1 parameter.
                return op().then([=] (auto... result) {
                    auto f = reader->is_end_of_stream() ? nullptr : std::make_unique<future<>>(reader->fill_buffer(timeout));
                    return make_ready_future<std::tuple<foreign_unique_ptr<future<>>, decltype(result)...>>(
                                std::tuple(make_foreign(std::move(f)), std::move(result)...));
                });
            };
            if (read_ahead_future) {
                return read_ahead_future->then(std::move(exec_op_and_read_ahead));
            } else {
                return exec_op_and_read_ahead();
            }
        }).then([this] (auto fut_and_result) {
            _read_ahead_future = std::get<0>(std::move(fut_and_result));
            static_assert(std::tuple_size<decltype(fut_and_result)>::value <= 2);
            if constexpr (std::tuple_size<decltype(fut_and_result)>::value == 1) {
                return make_ready_future<>();
            } else {
                auto result = std::get<1>(std::move(fut_and_result));
                return make_ready_future<decltype(result)>(std::move(result));
            }
        });
    }
public:
    foreign_reader(schema_ptr schema,
            reader_permit permit,
            foreign_unique_ptr<flat_mutation_reader> reader,
            streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no);

    // this is captured.
    foreign_reader(const foreign_reader&) = delete;
    foreign_reader& operator=(const foreign_reader&) = delete;
    foreign_reader(foreign_reader&&) = delete;
    foreign_reader& operator=(foreign_reader&&) = delete;

    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override;
    virtual future<> close() noexcept override;
};

foreign_reader::foreign_reader(schema_ptr schema,
        reader_permit permit,
        foreign_unique_ptr<flat_mutation_reader> reader,
        streamed_mutation::forwarding fwd_sm)
    : impl(std::move(schema), std::move(permit))
    , _reader(std::move(reader))
    , _fwd_sm(fwd_sm) {
}

future<> foreign_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    if (_end_of_stream || is_buffer_full()) {
        return make_ready_future();
    }

    return forward_operation(timeout, [reader = _reader.get(), timeout] () {
        auto f = reader->is_buffer_empty() ? reader->fill_buffer(timeout) : make_ready_future<>();
        return f.then([=] {
            return make_ready_future<remote_fill_buffer_result>(remote_fill_buffer_result(reader->detach_buffer(), reader->is_end_of_stream()));
        });
    }).then([this] (remote_fill_buffer_result res) mutable {
        _end_of_stream = res.end_of_stream;
        for (const auto& mf : *res.buffer) {
            // Need a copy since the mf is on the remote shard.
            push_mutation_fragment(mutation_fragment(*_schema, _permit, mf));
        }
    });
}

future<> foreign_reader::next_partition() {
    if (_fwd_sm == streamed_mutation::forwarding::yes) {
        clear_buffer();
        _end_of_stream = false;
    } else {
        clear_buffer_to_next_partition();
        if (!is_buffer_empty()) {
            co_return;
        }
        _end_of_stream = false;
    }
    co_await forward_operation(db::no_timeout, [reader = _reader.get()] () {
        return reader->next_partition();
    });
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

future<> foreign_reader::close() noexcept {
    if (!_reader) {
        if (_read_ahead_future) {
            on_internal_error_noexcept(mrlog, "foreign_reader::close can't wait on read_ahead future with disengaged reader");
        }
        return make_ready_future<>();
    }
    return smp::submit_to(_reader.get_owner_shard(),
            [reader = std::move(_reader), read_ahead_future = std::exchange(_read_ahead_future, nullptr)] () mutable {
        auto read_ahead = read_ahead_future ? std::move(*read_ahead_future.get()) : make_ready_future<>();
        return read_ahead.then_wrapped([reader = std::move(reader)] (future<> f) mutable {
            if (f.failed()) {
                auto ex = f.get_exception();
                mrlog.warn("foreign_reader: benign read_ahead failure during close: {}. Ignoring.", ex);
            }
            return reader->close();
        });
    });
}

flat_mutation_reader make_foreign_reader(schema_ptr schema,
            reader_permit permit,
            foreign_ptr<std::unique_ptr<flat_mutation_reader>> reader,
            streamed_mutation::forwarding fwd_sm) {
    if (reader.get_owner_shard() == this_shard_id()) {
        return std::move(*reader);
    }
    return make_flat_mutation_reader<foreign_reader>(std::move(schema), std::move(permit), std::move(reader), fwd_sm);
}

// Encapsulates all data and logic that is local to the remote shard the
// reader lives on.
class evictable_reader : public flat_mutation_reader::impl {
public:
    using auto_pause = bool_class<class auto_pause_tag>;

private:
    auto_pause _auto_pause;
    mutation_source _ms;
    const dht::partition_range* _pr;
    const query::partition_slice& _ps;
    const io_priority_class& _pc;
    tracing::global_trace_state_ptr _trace_state;
    const mutation_reader::forwarding _fwd_mr;
    reader_concurrency_semaphore::inactive_read_handle _irh;
    bool _drop_partition_start = false;
    bool _drop_static_row = false;
    // Trim range tombstones on the start of the buffer to the start of the read
    // range (_next_position_in_partition). Set after reader recreation.
    // Also validate the first not-trimmed mutation fragment's position.
    bool _trim_range_tombstones = false;
    // Validate the partition key of the first emitted partition, set after the
    // reader was recreated.
    bool _validate_partition_key = false;
    position_in_partition::tri_compare _tri_cmp;

    std::optional<dht::decorated_key> _last_pkey;
    position_in_partition _next_position_in_partition = position_in_partition::for_partition_start();
    // These are used when the reader has to be recreated (after having been
    // evicted while paused) and the range and/or slice it is recreated with
    // differs from the original ones.
    std::optional<dht::partition_range> _range_override;
    std::optional<query::partition_slice> _slice_override;

    flat_mutation_reader_opt _reader;

private:
    void do_pause(flat_mutation_reader reader);
    void maybe_pause(flat_mutation_reader reader);
    flat_mutation_reader_opt try_resume();
    void update_next_position(flat_mutation_reader& reader);
    void adjust_partition_slice();
    flat_mutation_reader recreate_reader();
    flat_mutation_reader resume_or_create_reader();
    void maybe_validate_partition_start(const flat_mutation_reader::tracked_buffer& buffer);
    void validate_position_in_partition(position_in_partition_view pos) const;
    bool should_drop_fragment(const mutation_fragment& mf);
    bool maybe_trim_range_tombstone(mutation_fragment& mf) const;
    future<> do_fill_buffer(flat_mutation_reader& reader, db::timeout_clock::time_point timeout);
    future<> fill_buffer(flat_mutation_reader& reader, db::timeout_clock::time_point timeout);

public:
    evictable_reader(
            auto_pause ap,
            mutation_source ms,
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr);
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point timeout) override {
        throw_with_backtrace<std::bad_function_call>();
    }
    virtual future<> close() noexcept override {
        if (_reader) {
            return _reader->close();
        }
        if (auto reader_opt = try_resume()) {
            return reader_opt->close();
        }
        return make_ready_future<>();
    }
    reader_concurrency_semaphore::inactive_read_handle inactive_read_handle() && {
        return std::move(_irh);
    }
    void pause() {
        if (_reader) {
            do_pause(std::move(*_reader));
        }
    }
    reader_permit permit() {
        return _permit;
    }
};

void evictable_reader::do_pause(flat_mutation_reader reader) {
    assert(!_irh);
    _irh = _permit.semaphore().register_inactive_read(std::move(reader));
}

void evictable_reader::maybe_pause(flat_mutation_reader reader) {
    if (_auto_pause) {
        do_pause(std::move(reader));
    } else {
        _reader = std::move(reader);
    }
}

flat_mutation_reader_opt evictable_reader::try_resume() {
    return _permit.semaphore().unregister_inactive_read(std::move(_irh));
}

void evictable_reader::update_next_position(flat_mutation_reader& reader) {
    if (is_buffer_empty()) {
        return;
    }

    auto rbegin = std::reverse_iterator(buffer().end());
    auto rend = std::reverse_iterator(buffer().begin());
    if (auto pk_it = std::find_if(rbegin, rend, std::mem_fn(&mutation_fragment::is_partition_start)); pk_it != rend) {
        _last_pkey = pk_it->as_partition_start().key();
    }

    const auto last_pos = buffer().back().position();
    switch (last_pos.region()) {
        case partition_region::partition_start:
            _next_position_in_partition = position_in_partition::for_static_row();
            break;
        case partition_region::static_row:
            _next_position_in_partition = position_in_partition::before_all_clustered_rows();
            break;
        case partition_region::clustered:
            if (!reader.is_buffer_empty() && reader.peek_buffer().is_end_of_partition()) {
                   push_mutation_fragment(reader.pop_mutation_fragment());
                   _next_position_in_partition = position_in_partition::for_partition_start();
            } else {
                _next_position_in_partition = position_in_partition::after_key(last_pos);
            }
            break;
        case partition_region::partition_end:
           _next_position_in_partition = position_in_partition::for_partition_start();
           break;
    }
}

void evictable_reader::adjust_partition_slice() {
    if (!_slice_override) {
        _slice_override = _ps;
    }

    auto ranges = _slice_override->default_row_ranges();
    query::trim_clustering_row_ranges_to(*_schema, ranges, _next_position_in_partition);

    _slice_override->clear_ranges();
    _slice_override->set_range(*_schema, _last_pkey->key(), std::move(ranges));
}

flat_mutation_reader evictable_reader::recreate_reader() {
    const dht::partition_range* range = _pr;
    const query::partition_slice* slice = &_ps;

    _range_override.reset();
    _slice_override.reset();

    _drop_partition_start = false;
    _drop_static_row = false;

    if (_last_pkey) {
        bool partition_range_is_inclusive = true;

        switch (_next_position_in_partition.region()) {
        case partition_region::partition_start:
            partition_range_is_inclusive = false;
            break;
        case partition_region::static_row:
            _drop_partition_start = true;
            break;
        case partition_region::clustered:
            _drop_partition_start = true;
            _drop_static_row = true;
            _trim_range_tombstones = true;
            adjust_partition_slice();
            slice = &*_slice_override;
            break;
        case partition_region::partition_end:
            partition_range_is_inclusive = false;
            break;
        }

        // The original range contained a single partition and we've read it
        // all. We'd have to create a reader with an empty range that would
        // immediately be at EOS. This is not possible so just create an empty
        // reader instead.
        // This should be extremely rare (who'd create a multishard reader to
        // read a single partition) but still, let's make sure we handle it
        // correctly.
        if (_pr->is_singular() && !partition_range_is_inclusive) {
            return make_empty_flat_reader(_schema, _permit);
        }

        _range_override = dht::partition_range({dht::partition_range::bound(*_last_pkey, partition_range_is_inclusive)}, _pr->end());
        range = &*_range_override;

        _validate_partition_key = true;
    }

    return _ms.make_reader(
            _schema,
            _permit,
            *range,
            *slice,
            _pc,
            _trace_state,
            streamed_mutation::forwarding::no,
            _fwd_mr);
}

flat_mutation_reader evictable_reader::resume_or_create_reader() {
    if (_reader) {
        return std::move(*_reader);
    }
    if (auto reader_opt = try_resume()) {
        return std::move(*reader_opt);
    }
    return recreate_reader();
}

template <typename... Arg>
static void require(bool condition, const char* msg, const Arg&... arg) {
    if (!condition) {
        on_internal_error(mrlog, format(msg, arg...));
    }
}

void evictable_reader::maybe_validate_partition_start(const flat_mutation_reader::tracked_buffer& buffer) {
    if (!_validate_partition_key || buffer.empty()) {
        return;
    }

    // If this is set we can assume the first fragment is a partition-start.
    const auto& ps = buffer.front().as_partition_start();
    const auto tri_cmp = dht::ring_position_comparator(*_schema);
    // If we recreated the reader after fast-forwarding it we won't have
    // _last_pkey set. In this case it is enough to check if the partition
    // is in range.
    if (_last_pkey) {
        const auto cmp_res = tri_cmp(*_last_pkey, ps.key());
        if (_drop_partition_start) { // we expect to continue from the same partition
            // We cannot assume the partition we stopped the read at is still alive
            // when we recreate the reader. It might have been compacted away in the
            // meanwhile, so allow for a larger partition too.
            require(
                    cmp_res <= 0,
                    "{}(): validation failed, expected partition with key larger or equal to _last_pkey {} due to _drop_partition_start being set, but got {}",
                    __FUNCTION__,
                    *_last_pkey,
                    ps.key());
            // Reset drop flags and next pos if we are not continuing from the same partition
            if (cmp_res < 0) {
                // Close previous partition, we are not going to continue it.
                push_mutation_fragment(*_schema, _permit, partition_end{});
                _drop_partition_start = false;
                _drop_static_row = false;
                _next_position_in_partition = position_in_partition::for_partition_start();
                _trim_range_tombstones = false;
            }
        } else { // should be a larger partition
            require(
                    cmp_res < 0,
                    "{}(): validation failed, expected partition with key larger than _last_pkey {} due to _drop_partition_start being unset, but got {}",
                    __FUNCTION__,
                    *_last_pkey,
                    ps.key());
        }
    }
    const auto& prange = _range_override ? *_range_override : *_pr;
    require(
            // TODO: somehow avoid this copy
            prange.contains(ps.key(), tri_cmp),
            "{}(): validation failed, expected partition with key that falls into current range {}, but got {}",
            __FUNCTION__,
            prange,
            ps.key());

    _validate_partition_key = false;
}

void evictable_reader::validate_position_in_partition(position_in_partition_view pos) const {
    require(
            _tri_cmp(_next_position_in_partition, pos) <= 0,
            "{}(): validation failed, expected position in partition that is larger-than-equal than _next_position_in_partition {}, but got {}",
            __FUNCTION__,
            _next_position_in_partition,
            pos);

    if (_slice_override && pos.region() == partition_region::clustered) {
        const auto ranges = _slice_override->row_ranges(*_schema, _last_pkey->key());
        const bool any_contains = std::any_of(ranges.begin(), ranges.end(), [this, &pos] (const query::clustering_range& cr) {
            // TODO: somehow avoid this copy
            auto range = position_range(cr);
            return range.contains(*_schema, pos);
        });
        require(
                any_contains,
                "{}(): validation failed, expected clustering fragment that is included in the slice {}, but got {}",
                __FUNCTION__,
                *_slice_override,
                pos);
    }
}

bool evictable_reader::should_drop_fragment(const mutation_fragment& mf) {
    if (_drop_partition_start && mf.is_partition_start()) {
        _drop_partition_start = false;
        return true;
    }
    // Unlike partition-start above, a partition is not guaranteed to have a
    // static row fragment. So reset the flag regardless of whether we could
    // drop one or not.
    // We are guaranteed to get here only right after dropping a partition-start,
    // so if we are not seeing a static row here, the partition doesn't have one.
    if (_drop_static_row) {
         _drop_static_row = false;
        return mf.is_static_row();
    }
    return false;
}

bool evictable_reader::maybe_trim_range_tombstone(mutation_fragment& mf) const {
    // We either didn't read a partition yet (evicted after fast-forwarding) or
    // didn't stop in a clustering region. We don't need to trim range
    // tombstones in either case.
    if (!_last_pkey || _next_position_in_partition.region() != partition_region::clustered) {
        return false;
    }
    if (!mf.is_range_tombstone()) {
        validate_position_in_partition(mf.position());
        return false;
    }

    if (_tri_cmp(mf.position(), _next_position_in_partition) >= 0) {
        validate_position_in_partition(mf.position());
        return false; // rt in range, no need to trim
    }

    const auto& rt = mf.as_range_tombstone();

    require(
            _tri_cmp(_next_position_in_partition, rt.end_position()) <= 0,
            "{}(): validation failed, expected range tombstone with end pos larger than _next_position_in_partition {}, but got {}",
            __FUNCTION__,
            _next_position_in_partition,
            rt.end_position());

    mf.mutate_as_range_tombstone(*_schema, [this] (range_tombstone& rt) {
        rt.set_start(position_in_partition_view::before_key(_next_position_in_partition));
    });

    return true;
}

future<> evictable_reader::do_fill_buffer(flat_mutation_reader& reader, db::timeout_clock::time_point timeout) {
    if (!_drop_partition_start && !_drop_static_row) {
        auto fill_buf_fut = reader.fill_buffer(timeout);
        if (_validate_partition_key) {
            fill_buf_fut = fill_buf_fut.then([this, &reader] {
                maybe_validate_partition_start(reader.buffer());
            });
        }
        return fill_buf_fut;
    }
    return repeat([this, &reader, timeout] {
        return reader.fill_buffer(timeout).then([this, &reader] {
            maybe_validate_partition_start(reader.buffer());
            while (!reader.is_buffer_empty() && should_drop_fragment(reader.peek_buffer())) {
                reader.pop_mutation_fragment();
            }
            return stop_iteration(reader.is_buffer_full() || reader.is_end_of_stream());
        });
    });
}

future<> evictable_reader::fill_buffer(flat_mutation_reader& reader, db::timeout_clock::time_point timeout) {
    return do_fill_buffer(reader, timeout).then([this, &reader, timeout] {
        if (reader.is_buffer_empty()) {
            return make_ready_future<>();
        }
        while (_trim_range_tombstones && !reader.is_buffer_empty()) {
            auto mf = reader.pop_mutation_fragment();
            _trim_range_tombstones = maybe_trim_range_tombstone(mf);
            push_mutation_fragment(std::move(mf));
        }
        reader.move_buffer_content_to(*this);
        auto stop = [this, &reader] {
            // The only problematic fragment kind is the range tombstone.
            // All other fragment kinds are safe to end the buffer on, and
            // are guaranteed to represent progress vs. the last buffer fill.
            if (!buffer().back().is_range_tombstone()) {
                return true;
            }
            if (reader.is_buffer_empty()) {
                return reader.is_end_of_stream();
            }
            const auto& next_pos = reader.peek_buffer().position();
            // To ensure safe progress we have to ensure the following:
            //
            // _next_position_in_partition < buffer.back().position() < next_pos
            //
            // * The first condition is to ensure we made progress since the
            // last buffer fill. Otherwise we might get into an endless loop if
            // the reader is recreated after each `fill_buffer()` call.
            // * The second condition is to ensure we have seen all fragments
            // with the same position. Otherwise we might jump over those
            // remaining fragments with the same position as the last
            // fragment's in the buffer when the reader is recreated.
            return _tri_cmp(_next_position_in_partition, buffer().back().position()) < 0 && _tri_cmp(buffer().back().position(), next_pos) < 0;
        };
        // Read additional fragments until it is safe to stop, if needed.
        // We have to ensure we stop at a fragment such that if the reader is
        // evicted and recreated later, we won't be skipping any fragments.
        // Practically, range tombstones are the only ones that are
        // problematic to end the buffer on. This is due to the fact range
        // tombstones can have the same position that multiple following range
        // tombstones, or a single following clustering row in the stream has.
        // When a range tombstone is the last in the buffer, we have to continue
        // to read until we are sure we've read all fragments sharing the same
        // position, so that we can safely continue reading from after said
        // position.
        return do_until(stop, [this, &reader, timeout] {
            if (reader.is_buffer_empty()) {
                return do_fill_buffer(reader, timeout);
            }
            if (_trim_range_tombstones) {
                auto mf = reader.pop_mutation_fragment();
                _trim_range_tombstones = maybe_trim_range_tombstone(mf);
                push_mutation_fragment(std::move(mf));
            } else {
                push_mutation_fragment(reader.pop_mutation_fragment());
            }
            return make_ready_future<>();
        });
    }).then([this, &reader] {
        update_next_position(reader);
    });
}

evictable_reader::evictable_reader(
        auto_pause ap,
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr)
    : impl(std::move(schema), std::move(permit))
    , _auto_pause(ap)
    , _ms(std::move(ms))
    , _pr(&pr)
    , _ps(ps)
    , _pc(pc)
    , _trace_state(std::move(trace_state))
    , _fwd_mr(fwd_mr)
    , _tri_cmp(*_schema) {
}

future<> evictable_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    if (is_end_of_stream()) {
        return make_ready_future<>();
    }
    return with_closeable(resume_or_create_reader(), [this, timeout] (flat_mutation_reader& reader) mutable {
        return fill_buffer(reader, timeout).then([this, &reader] {
            _end_of_stream = reader.is_end_of_stream() && reader.is_buffer_empty();
            maybe_pause(std::move(reader));
        });
    });
}

future<> evictable_reader::next_partition() {
    _next_position_in_partition = position_in_partition::for_partition_start();
    clear_buffer_to_next_partition();
    if (!is_buffer_empty()) {
        co_return;
    }
    auto reader = resume_or_create_reader();
    co_await reader.next_partition();
    maybe_pause(std::move(reader));
}

future<> evictable_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    _pr = &pr;
    _last_pkey.reset();
    _next_position_in_partition = position_in_partition::for_partition_start();
    clear_buffer();
    _end_of_stream = false;

    if (_reader) {
        co_await _reader->fast_forward_to(pr, timeout);
        _range_override.reset();
        co_return;
    }
    if (auto reader_opt = try_resume()) {
        co_await reader_opt->fast_forward_to(pr, timeout);
        _range_override.reset();
        maybe_pause(std::move(*reader_opt));
    }
}

evictable_reader_handle::evictable_reader_handle(evictable_reader& r) : _r(&r)
{ }

void evictable_reader_handle::evictable_reader_handle::pause() {
    _r->pause();
}

flat_mutation_reader make_auto_paused_evictable_reader(
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<evictable_reader>(evictable_reader::auto_pause::yes, std::move(ms), std::move(schema), std::move(permit), pr, ps,
            pc, std::move(trace_state), fwd_mr);
}

std::pair<flat_mutation_reader, evictable_reader_handle> make_manually_paused_evictable_reader(
        mutation_source ms,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    auto reader = std::make_unique<evictable_reader>(evictable_reader::auto_pause::no, std::move(ms), std::move(schema), std::move(permit), pr, ps,
            pc, std::move(trace_state), fwd_mr);
    auto handle = evictable_reader_handle(*reader.get());
    return std::pair(flat_mutation_reader(std::move(reader)), handle);
}

namespace {

// A special-purpose shard reader.
//
// Shard reader manages a reader located on a remote shard. It transparently
// supports read-ahead (background fill_buffer() calls).
// This reader is not for general use, it was designed to serve the
// multishard_combining_reader.
// Although it implements the flat_mutation_reader:impl interface it cannot be
// wrapped into a flat_mutation_reader, as it needs to be managed by a shared
// pointer.
class shard_reader : public flat_mutation_reader::impl {
private:
    shared_ptr<reader_lifecycle_policy> _lifecycle_policy;
    const unsigned _shard;
    const dht::partition_range* _pr;
    const query::partition_slice& _ps;
    const io_priority_class& _pc;
    tracing::global_trace_state_ptr _trace_state;
    const mutation_reader::forwarding _fwd_mr;
    std::optional<future<>> _read_ahead;
    foreign_ptr<std::unique_ptr<evictable_reader>> _reader;

private:
    future<> do_fill_buffer(db::timeout_clock::time_point timeout);

public:
    shard_reader(
            schema_ptr schema,
            reader_permit permit,
            shared_ptr<reader_lifecycle_policy> lifecycle_policy,
            unsigned shard,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            mutation_reader::forwarding fwd_mr)
        : impl(std::move(schema), std::move(permit))
        , _lifecycle_policy(std::move(lifecycle_policy))
        , _shard(shard)
        , _pr(&pr)
        , _ps(ps)
        , _pc(pc)
        , _trace_state(std::move(trace_state))
        , _fwd_mr(fwd_mr) {
    }

    shard_reader(shard_reader&&) = delete;
    shard_reader& operator=(shard_reader&&) = delete;

    shard_reader(const shard_reader&) = delete;
    shard_reader& operator=(const shard_reader&) = delete;

    const mutation_fragment& peek_buffer() const {
        return buffer().front();
    }
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point timeout) override;
    virtual future<> close() noexcept override;
    bool done() const {
        return _reader && is_buffer_empty() && is_end_of_stream();
    }
    void read_ahead(db::timeout_clock::time_point timeout);
    bool is_read_ahead_in_progress() const {
        return _read_ahead.has_value();
    }
};

future<> shard_reader::close() noexcept {
    // Nothing to do if there was no reader created, nor is there a background
    // read ahead in progress which will create one.
    if (!_reader && !_read_ahead) {
        co_return;
    }

    try {
        if (_read_ahead) {
            co_await *std::exchange(_read_ahead, std::nullopt);
        }

        co_await smp::submit_to(_shard, [this] {
            auto irh = std::move(*_reader).inactive_read_handle();
            return with_closeable(flat_mutation_reader(_reader.release()), [this] (flat_mutation_reader& reader) mutable {
                auto permit = reader.permit();
                const auto& schema = *reader.schema();

                auto unconsumed_fragments = reader.detach_buffer();
                auto rit = std::reverse_iterator(buffer().cend());
                auto rend = std::reverse_iterator(buffer().cbegin());
                for (; rit != rend; ++rit) {
                    unconsumed_fragments.emplace_front(schema, permit, *rit); // we are copying from the remote shard.
                }

                return unconsumed_fragments;
            }).then([this, irh = std::move(irh)] (flat_mutation_reader::tracked_buffer&& buf) mutable {
                return _lifecycle_policy->destroy_reader({std::move(irh), std::move(buf)});
            });
        });
    } catch (...) {
        mrlog.error("shard_reader::close(): failed to stop reader on shard {}: {}", _shard, std::current_exception());
    }
}

future<> shard_reader::do_fill_buffer(db::timeout_clock::time_point timeout) {
    auto fill_buf_fut = make_ready_future<remote_fill_buffer_result>();

    struct reader_and_buffer_fill_result {
        foreign_ptr<std::unique_ptr<evictable_reader>> reader;
        remote_fill_buffer_result result;
    };

    if (!_reader) {
        fill_buf_fut = smp::submit_to(_shard, [this, gs = global_schema_ptr(_schema), timeout] {
            auto ms = mutation_source([lifecycle_policy = _lifecycle_policy.get()] (
                        schema_ptr s,
                        reader_permit permit,
                        const dht::partition_range& pr,
                        const query::partition_slice& ps,
                        const io_priority_class& pc,
                        tracing::trace_state_ptr ts,
                        streamed_mutation::forwarding,
                        mutation_reader::forwarding fwd_mr) {
                return lifecycle_policy->create_reader(std::move(s), std::move(permit), pr, ps, pc, std::move(ts), fwd_mr);
            });
            auto s = gs.get();
            auto rreader = make_foreign(std::make_unique<evictable_reader>(evictable_reader::auto_pause::yes, std::move(ms),
                        s, _lifecycle_policy->semaphore().make_permit(s.get(), "shard-reader"), *_pr, _ps, _pc, _trace_state, _fwd_mr));
            tracing::trace(_trace_state, "Creating shard reader on shard: {}", this_shard_id());
            reader_permit::used_guard ug{rreader->permit()};
            auto f = rreader->fill_buffer(timeout);
            return f.then([rreader = std::move(rreader), ug = std::move(ug)] () mutable {
                auto res = remote_fill_buffer_result(rreader->detach_buffer(), rreader->is_end_of_stream());
                return make_ready_future<reader_and_buffer_fill_result>(reader_and_buffer_fill_result{std::move(rreader), std::move(res)});
            });
        }).then([this, timeout] (reader_and_buffer_fill_result res) {
            _reader = std::move(res.reader);
            return std::move(res.result);
        });
    } else {
        fill_buf_fut = smp::submit_to(_shard, [this, timeout] () mutable {
            reader_permit::used_guard ug{_reader->permit()};
            return _reader->fill_buffer(timeout).then([this, ug = std::move(ug)] {
                return remote_fill_buffer_result(_reader->detach_buffer(), _reader->is_end_of_stream());
            });
        });
    }

    return fill_buf_fut.then([this] (remote_fill_buffer_result res) mutable {
        _end_of_stream = res.end_of_stream;
        for (const auto& mf : *res.buffer) {
            push_mutation_fragment(mutation_fragment(*_schema, _permit, mf));
        }
    });
}

future<> shard_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    if (_read_ahead) {
        return *std::exchange(_read_ahead, std::nullopt);
    }
    if (!is_buffer_empty()) {
        return make_ready_future<>();
    }
    return do_fill_buffer(timeout);
}

future<> shard_reader::next_partition() {
    if (!_reader) {
        co_return;
    }
    if (_read_ahead) {
        co_await *std::exchange(_read_ahead, std::nullopt);
    }
    clear_buffer_to_next_partition();
    if (!is_buffer_empty()) {
        co_return;
    }
    co_return co_await smp::submit_to(_shard, [this] {
        return _reader->next_partition();
    });
}

future<> shard_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    _pr = &pr;

    if (!_reader && !_read_ahead) {
        // No need to fast-forward uncreated readers, they will be passed the new
        // range when created.
        return make_ready_future<>();
    }

    auto f = _read_ahead ? *std::exchange(_read_ahead, std::nullopt) : make_ready_future<>();
    return f.then([this, &pr, timeout] {
        _end_of_stream = false;
        clear_buffer();

        return smp::submit_to(_shard, [this, &pr, timeout] {
            return _reader->fast_forward_to(pr, timeout);
        });
    });
}

future<> shard_reader::fast_forward_to(position_range, db::timeout_clock::time_point timeout) {
    return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
}

void shard_reader::read_ahead(db::timeout_clock::time_point timeout) {
    if (_read_ahead || is_end_of_stream() || !is_buffer_empty()) {
        return;
    }

    _read_ahead.emplace(do_fill_buffer(timeout));
}

} // anonymous namespace

// See make_multishard_combining_reader() for description.
class multishard_combining_reader : public flat_mutation_reader::impl {
    struct shard_and_token {
        shard_id shard;
        dht::token token;

        bool operator<(const shard_and_token& o) const {
            // Reversed, as we want a min-heap.
            return token > o.token;
        }
    };

    const dht::sharder& _sharder;
    std::vector<std::unique_ptr<shard_reader>> _shard_readers;
    // Contains the position of each shard with token granularity, organized
    // into a min-heap. Used to select the shard with the smallest token each
    // time a shard reader produces a new partition.
    std::vector<shard_and_token> _shard_selection_min_heap;
    unsigned _current_shard;
    bool _crossed_shards;
    unsigned _concurrency = 1;

    void on_partition_range_change(const dht::partition_range& pr);
    bool maybe_move_to_next_shard(const dht::token* const t = nullptr);
    future<> handle_empty_reader_buffer(db::timeout_clock::time_point timeout);

public:
    multishard_combining_reader(
            const dht::sharder& sharder,
            shared_ptr<reader_lifecycle_policy> lifecycle_policy,
            schema_ptr s,
            reader_permit permit,
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
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override;
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override;
    virtual future<> close() noexcept override;
};

void multishard_combining_reader::on_partition_range_change(const dht::partition_range& pr) {
    _shard_selection_min_heap.clear();
    _shard_selection_min_heap.reserve(_sharder.shard_count());

    auto token = pr.start() ? pr.start()->value().token() : dht::minimum_token();
    _current_shard = _sharder.shard_of(token);

    auto sharder = dht::ring_position_range_sharder(_sharder, pr);

    auto next = sharder.next(*_schema);

    // The first value of `next` is thrown away, as it is the ring range of the current shard.
    // We only want to do a full round, until we get back to the shard we started from (`_current_shard`).
    // We stop earlier if the sharder has no ranges for the remaining shards.
    for (next = sharder.next(*_schema); next && next->shard != _current_shard; next = sharder.next(*_schema)) {
        _shard_selection_min_heap.push_back(shard_and_token{next->shard, next->ring_range.start()->value().token()});
        boost::push_heap(_shard_selection_min_heap);
    }
}

bool multishard_combining_reader::maybe_move_to_next_shard(const dht::token* const t) {
    if (_shard_selection_min_heap.empty() || (t && *t < _shard_selection_min_heap.front().token)) {
        return false;
    }

    boost::pop_heap(_shard_selection_min_heap);
    const auto next_shard = _shard_selection_min_heap.back().shard;
    _shard_selection_min_heap.pop_back();

    if (t) {
        _shard_selection_min_heap.push_back(shard_and_token{_current_shard, *t});
        boost::push_heap(_shard_selection_min_heap);
    }

    _crossed_shards = true;
    _current_shard = next_shard;
    return true;
}

future<> multishard_combining_reader::handle_empty_reader_buffer(db::timeout_clock::time_point timeout) {
    auto& reader = *_shard_readers[_current_shard];

    if (reader.is_end_of_stream()) {
        if (_shard_selection_min_heap.empty()) {
            _end_of_stream = true;
        } else {
            maybe_move_to_next_shard();
        }
        return make_ready_future<>();
    } else if (reader.is_read_ahead_in_progress()) {
        return reader.fill_buffer(timeout);
    } else {
        // If we crossed shards and the next reader has an empty buffer we
        // double concurrency so the next time we cross shards we will have
        // more chances of hitting the reader's buffer.
        if (_crossed_shards) {
            _concurrency = std::min(_concurrency * 2, _sharder.shard_count());

            // Read ahead shouldn't change the min selection heap so we work on a local copy.
            auto shard_selection_min_heap_copy = _shard_selection_min_heap;

            // If concurrency > 1 we kick-off concurrency-1 read-aheads in the
            // background. They will be brought to the foreground when we move
            // to their respective shard.
            for (unsigned i = 1; i < _concurrency && !shard_selection_min_heap_copy.empty(); ++i) {
                boost::pop_heap(shard_selection_min_heap_copy);
                const auto next_shard = shard_selection_min_heap_copy.back().shard;
                shard_selection_min_heap_copy.pop_back();
                _shard_readers[next_shard]->read_ahead(timeout);
            }
        }
        return reader.fill_buffer(timeout);
    }
}

multishard_combining_reader::multishard_combining_reader(
        const dht::sharder& sharder,
        shared_ptr<reader_lifecycle_policy> lifecycle_policy,
        schema_ptr s,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr)
    : impl(std::move(s), std::move(permit)), _sharder(sharder) {

    on_partition_range_change(pr);

    _shard_readers.reserve(_sharder.shard_count());
    for (unsigned i = 0; i < _sharder.shard_count(); ++i) {
        _shard_readers.emplace_back(std::make_unique<shard_reader>(_schema, _permit, lifecycle_policy, i, pr, ps, pc, trace_state, fwd_mr));
    }
}

future<> multishard_combining_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    _crossed_shards = false;
    return do_until([this] { return is_buffer_full() || is_end_of_stream(); }, [this, timeout] {
        auto& reader = *_shard_readers[_current_shard];

        if (reader.is_buffer_empty()) {
            return handle_empty_reader_buffer(timeout);
        }

        while (!reader.is_buffer_empty() && !is_buffer_full()) {
            if (const auto& mf = reader.peek_buffer(); mf.is_partition_start() && maybe_move_to_next_shard(&mf.as_partition_start().key().token())) {
                return make_ready_future<>();
            }
            push_mutation_fragment(reader.pop_mutation_fragment());
        }
        return make_ready_future<>();
    });
}

future<> multishard_combining_reader::next_partition() {
    clear_buffer_to_next_partition();
    if (is_buffer_empty()) {
        return _shard_readers[_current_shard]->next_partition();
    }
    return make_ready_future<>();
}

future<> multishard_combining_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    clear_buffer();
    _end_of_stream = false;
    on_partition_range_change(pr);
    return parallel_for_each(_shard_readers, [&pr, timeout] (std::unique_ptr<shard_reader>& sr) {
        return sr->fast_forward_to(pr, timeout);
    });
}

future<> multishard_combining_reader::fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
    return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
}

future<> multishard_combining_reader::close() noexcept {
    return parallel_for_each(_shard_readers, [] (std::unique_ptr<shard_reader>& sr) {
        return sr->close();
    });
}

flat_mutation_reader make_multishard_combining_reader(
        shared_ptr<reader_lifecycle_policy> lifecycle_policy,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    const dht::sharder& sharder = schema->get_sharder();
    return make_flat_mutation_reader<multishard_combining_reader>(sharder, std::move(lifecycle_policy), std::move(schema), std::move(permit), pr, ps, pc,
            std::move(trace_state), fwd_mr);
}

flat_mutation_reader make_multishard_combining_reader_for_tests(
        const dht::sharder& sharder,
        shared_ptr<reader_lifecycle_policy> lifecycle_policy,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        mutation_reader::forwarding fwd_mr) {
    return make_flat_mutation_reader<multishard_combining_reader>(sharder, std::move(lifecycle_policy), std::move(schema), std::move(permit), pr, ps, pc,
            std::move(trace_state), fwd_mr);
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
    virtual future<> fill_buffer(db::timeout_clock::time_point) override {
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
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override {
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
    void abort(std::exception_ptr ep) {
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

void queue_reader_handle::abandon() {
    abort(std::make_exception_ptr<std::runtime_error>(std::runtime_error("Abandoned queue_reader_handle")));
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

namespace {

class compacting_reader : public flat_mutation_reader::impl {
    friend class compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::yes>;

private:
    flat_mutation_reader _reader;
    compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::yes> _compactor;
    noop_compacted_fragments_consumer _gc_consumer;

    // Uncompacted stream
    partition_start _last_uncompacted_partition_start;
    mutation_fragment::kind _last_uncompacted_kind = mutation_fragment::kind::partition_end;

    // Compacted stream
    bool _has_compacted_partition_start = false;
    bool _ignore_partition_end = false;

private:
    void maybe_push_partition_start() {
        if (_has_compacted_partition_start) {
            push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(_last_uncompacted_partition_start)));
            _has_compacted_partition_start = false;
        }
    }
    void maybe_inject_partition_end() {
        // The compactor needs a valid stream, but downstream doesn't care about
        // the injected partition end, so ignore it.
        if (_last_uncompacted_kind != mutation_fragment::kind::partition_end) {
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
        push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(sr)));
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(cr)));
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone&& rt) {
        maybe_push_partition_start();
        push_mutation_fragment(mutation_fragment(*_schema, _permit, std::move(rt)));
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        maybe_push_partition_start();
        if (!_ignore_partition_end) {
            push_mutation_fragment(mutation_fragment(*_schema, _permit, partition_end{}));
        }
        return stop_iteration::no;
    }
    void consume_end_of_stream() {
    }

public:
    compacting_reader(flat_mutation_reader source, gc_clock::time_point compaction_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
        : impl(source.schema(), source.permit())
        , _reader(std::move(source))
        , _compactor(*_schema, compaction_time, get_max_purgeable)
        , _last_uncompacted_partition_start(dht::decorated_key(dht::minimum_token(), partition_key::make_empty()), tombstone{}) {
    }
    virtual future<> fill_buffer(db::timeout_clock::time_point timeout) override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this, timeout] {
            return _reader.fill_buffer(timeout).then([this, timeout] {
                if (_reader.is_buffer_empty()) {
                    _end_of_stream = _reader.is_end_of_stream();
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
                    case mutation_fragment::kind::static_row:
                        _compactor.consume(std::move(mf).as_static_row(), *this, _gc_consumer);
                        break;
                    case mutation_fragment::kind::clustering_row:
                        _compactor.consume(std::move(mf).as_clustering_row(), *this, _gc_consumer);
                        break;
                    case mutation_fragment::kind::range_tombstone:
                        _compactor.consume(std::move(mf).as_range_tombstone(), *this, _gc_consumer);
                        break;
                    case mutation_fragment::kind::partition_start:
                        _last_uncompacted_partition_start = std::move(mf).as_partition_start();
                        _compactor.consume_new_partition(_last_uncompacted_partition_start.key());
                        if (_last_uncompacted_partition_start.partition_tombstone()) {
                            _compactor.consume(_last_uncompacted_partition_start.partition_tombstone(), *this, _gc_consumer);
                        }
                        break;
                    case mutation_fragment::kind::partition_end:
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
    virtual future<> fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) override {
        clear_buffer();
        _end_of_stream = false;
        maybe_inject_partition_end();
        return _reader.fast_forward_to(pr, timeout);
    }
    virtual future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) override {
        return make_exception_future<>(make_backtraced_exception_ptr<std::bad_function_call>());
    }
    virtual future<> close() noexcept override {
        return _reader.close();
    }
};

} // anonymous namespace

flat_mutation_reader make_compacting_reader(flat_mutation_reader source, gc_clock::time_point compaction_time,
        std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable) {
    return make_flat_mutation_reader<compacting_reader>(std::move(source), compaction_time, get_max_purgeable);
}

position_reader_queue::~position_reader_queue() {}

// Merges output of readers opened for a single partition query into a non-decreasing stream of mutation fragments.
//
// Uses `position_reader_queue` to retrieve new readers lazily as the read progresses through the partition.
// A reader is popped from the queue only if we find that it may contain fragments for the currently inspected positions.
//
// Readers are closed as soon as we find that they were exhausted for the given partition query.
//
// Implements the `FragmentProducer` concept. However, `next_partition` and `fast_forward_to(partition_range)`
// are not implemented and throw an error; the reader is only used for single partition queries.
//
// Assumes that:
// - there are no static rows,
// - the returned fragments do not contain partition tombstones,
// - the merged readers return fragments from the same partition (but some or even all of them may be empty).
class clustering_order_reader_merger {
    const schema_ptr _schema;
    const reader_permit _permit;

    // Compares positions using *_schema.
    const position_in_partition::tri_compare _cmp;

    // A queue of readers used to lazily retrieve new readers as we progress through the partition.
    // Before the merger returns a batch for position `p`, it first ensures that all readers containing positions
    // <= `p` are popped from the queue so it can take all of their fragments into account.
    std::unique_ptr<position_reader_queue> _reader_queue;

    // Owning container for the readers popped from _reader_queue.
    // If we are sure that a reader is exhausted (all rows from the queried partition have been returned),
    // we destroy and remove it from the container.
    std::list<reader_and_upper_bound> _all_readers;
    using reader_iterator = std::list<reader_and_upper_bound>::iterator;

    // A min-heap of readers, sorted by the positions of their next fragments.
    // The iterators point to _all_readers.
    // Invariant: every reader in `_peeked_readers` satisfies `!is_buffer_empty()`,
    // so it is safe to call `pop_mutation_fragment()` and `peek_buffer()` on it.
    merger_vector<reader_iterator> _peeked_readers;

    // Used to compare peeked_readers stored in the `_peeked_readers` min-heap.
    struct peeked_reader_cmp {
        const position_in_partition::less_compare _less;

        explicit peeked_reader_cmp(const schema& s) : _less(s) {}

        bool operator()(const reader_iterator& a, const reader_iterator& b) {
            // Boost heaps are max-heaps, but we want a min-heap, so invert the comparison.
            return _less(b->reader.peek_buffer().position(), a->reader.peek_buffer().position());
        }
    };

    const peeked_reader_cmp _peeked_cmp;

    // operator() returns a mutation_fragment_batch, which is a range (a pair of iterators);
    // this is where the actual data is stored, i.e. the range points to _current_batch.
    merger_vector<mutation_fragment> _current_batch;

    // _unpeeked_readers stores readers for which we don't know the next fragment that they'll return.
    // Before we return the next batch of fragments, we must peek all readers here (and move them to
    // the _peeked_readers heap), since they might contain fragments with smaller positions than the
    // currently peeked readers.
    merger_vector<reader_iterator> _unpeeked_readers;

    // In forwarding mode, after a reader returns end-of-stream, if we cannot determine that
    // the reader won't return any more fragments in later position ranges, we save it in
    // _halted_readers and restore it when we get fast-forwaded to a later range.
    // See also comment in `peek_reader` when a reader returns end-of-stream.
    // _halted_readers doesn't serve any purpose when not in forwarding mode, because then
    // readers always return end-of-partition before end-of-stream, which is a signal that
    // we can remove the reader immediately.
    merger_vector<reader_iterator> _halted_readers;

    // In forwarding mode, this is the right-end of the position range being currently queried;
    // initially it's set to `before_all_clustered_rows` and updated on `fast_forward_to`.
    // We use it when popping readers from _reader_queue so that we don't prematurely pop
    // readers that only contain fragments from greater ranges.
    // In non-forwarding mode _pr_end is always equal to `after_all_clustered_rows`.
    position_in_partition_view _pr_end;
    // In forwarding mode, _forwarded_to remembers the last range we were forwarded to.
    // We need this because we're opening new readers in the middle of the partition query:
    // after the new reader returns its initial partition-start, we immediately forward it
    // to this range.
    std::optional<position_range> _forwarded_to;

    // Since we may open new readers when already inside the partition, i.e. after returning `partition_start`,
    // we must ignore `partition_start`s returned by these new readers. The approach we take is to return
    // the `partition_start` fetched from the first reader and ignore all the rest. This flag says whether
    // or not we've already fetched the first `partition_start`.
    bool _partition_start_fetched = false;

    // In non-forwarding mode, remember if we've returned the last fragment, which is always partition-end.
    // We construct the fragment ourselves instead of merging partition-ends returned from the merged readers,
    // because we may close readers in the middle of the partition query.
    // In forwarding mode this is always false.
    bool _should_emit_partition_end;

    // If a single reader wins with other readers (i.e. returns a smaller fragment) multiple times in a row,
    // the reader becomes a ``galloping reader'' (and is pointed to by _galloping_reader).
    // In this galloping mode we stop doing heap operations using the _peeked_readers heap;
    // instead, we keep peeking the _galloping_reader and compare the returned fragment's position directly
    // with the fragment of the reader stored at the heap front (if any), hoping that the galloping reader
    // will keep winning. If he wins, we don't put the fragment on the heap, but immediately return it.
    // If he loses, we go back to normal operation.
    reader_iterator _galloping_reader;

    // Counts how many times a potential galloping reader candidate has won with other readers.
    int _gallop_mode_hits = 0;

    // Determines how many times a fragment should be taken from the same
    // reader in order to enter gallop mode. Must be greater than one.
    static constexpr int _gallop_mode_entering_threshold = 3;

    bool in_gallop_mode() const {
        return _gallop_mode_hits >= _gallop_mode_entering_threshold;
    }

    future<> erase_reader(reader_iterator it) noexcept {
        return std::move(it->reader).close().then([this, it = std::move(it)] {
            _all_readers.erase(it);
        });
    }

    // Retrieve the next fragment from the reader pointed to by `it`.
    // The function assumes that we're not in galloping mode, `it` is in `_unpeeked_readers`,
    // and all fragments previously returned from the reader have already been returned by operator().
    //
    // The peeked reader is pushed onto the _peeked_readers heap.
    future<> peek_reader(reader_iterator it, db::timeout_clock::time_point timeout) {
        return it->reader.peek(timeout).then([this, timeout, it] (mutation_fragment* mf) {
            if (!mf) {
                // The reader returned end-of-stream before returning end-of-partition
                // (otherwise we would have removed it in a previous peek). This means that
                // either the reader was empty from the beginning (not even returning a `partition_start`)
                // or we are in forwarding mode and the reader won't return any more fragments in the current range.
                // If the reader's upper bound is smaller then the end of the current range then it won't
                // return any more fragments in later ranges as well (subsequent fast-forward-to ranges
                // are non-overlapping and strictly increasing), so we can remove it now.
                // Otherwise, if it previously returned a `partition_start`, it may start returning more fragments
                // later (after we fast-forward) so we save it for the moment in _halted_readers and will bring it
                // back when we get fast-forwarded.
                // We also save the reader if it was empty from the beginning (no `partition_start`) since
                // it makes the code simpler (to check for this here we would need additional state); it is a bit wasteful
                // but completely empty readers should be rare.
                if (_cmp(it->upper_bound, _pr_end) < 0) {
                    return erase_reader(std::move(it));
                } else {
                    _halted_readers.push_back(it);
                }
                return make_ready_future<>();
            }

            if (mf->is_partition_start()) {
                // We assume there are no partition tombstones.
                // This should have been checked before opening the reader.
                if (mf->as_partition_start().partition_tombstone()) {
                    on_internal_error(mrlog, format(
                            "clustering_order_reader_merger: partition tombstone encountered for partition {}."
                            " This reader merger cannot be used for readers that return partition tombstones"
                            " or it would give incorrect results.", mf->as_partition_start().key()));
                }
                if (!_partition_start_fetched) {
                    _peeked_readers.emplace_back(it);
                    boost::range::push_heap(_peeked_readers, _peeked_cmp);
                    _partition_start_fetched = true;
                    // there is no _forwarded_to range yet (see `fast_forward_to`)
                    // so no need to forward this reader
                    return make_ready_future<>();
                }

                it->reader.pop_mutation_fragment();
                auto f = _forwarded_to ? it->reader.fast_forward_to(*_forwarded_to, timeout) : make_ready_future<>();
                return f.then([this, timeout, it] { return peek_reader(it, timeout); });
            }

            // We assume that the schema does not have any static columns, so there cannot be any static rows.
            if (mf->is_static_row()) {
                on_internal_error(mrlog,
                        "clustering_order_reader_merger: static row encountered."
                        " This reader merger cannot be used for readers that return static rows"
                        " or it would give incorrect results.");
            }

            if (mf->is_end_of_partition()) {
                return erase_reader(std::move(it));
            } else {
                _peeked_readers.emplace_back(it);
                boost::range::push_heap(_peeked_readers, _peeked_cmp);
            }

            return make_ready_future<>();
        });
    }

    future<> peek_readers(db::timeout_clock::time_point timeout) {
        return parallel_for_each(_unpeeked_readers, [this, timeout] (reader_iterator it) {
            return peek_reader(it, timeout);
        }).then([this] {
            _unpeeked_readers.clear();
        });
    }

    // Retrieve the next fragment from the galloping reader.
    // The function assumes that we're in galloping mode and all fragments previously returned
    // from the galloping reader have already been returned by operator().
    //
    // If the galloping reader wins with other readers again, the fragment is returned as the next batch.
    // Otherwise, the reader is pushed onto _peeked_readers and we retry in non-galloping mode.
    future<mutation_fragment_batch> peek_galloping_reader(db::timeout_clock::time_point timeout) {
        return _galloping_reader->reader.peek(timeout).then([this, timeout] (mutation_fragment* mf) {
            bool erase = false;
            if (mf) {
                if (mf->is_partition_start()) {
                    on_internal_error(mrlog, format(
                            "clustering_order_reader_merger: double `partition start' encountered"
                            " in partition {} during read.", mf->as_partition_start().key()));
                }

                if (mf->is_static_row()) {
                    on_internal_error(mrlog,
                            "clustering_order_reader_merger: static row encountered."
                            " This reader merger cannot be used for tables that have static columns"
                            " or it would give incorrect results.");
                }

                if (mf->is_end_of_partition()) {
                    erase = true;
                } else {
                    if (_reader_queue->empty(mf->position())
                            && (_peeked_readers.empty()
                                    || _cmp(mf->position(), _peeked_readers.front()->reader.peek_buffer().position()) < 0)) {
                        _current_batch.push_back(_galloping_reader->reader.pop_mutation_fragment());

                        return make_ready_future<mutation_fragment_batch>(_current_batch);
                    }

                    // One of the existing readers won with the galloping reader,
                    // or there is a yet unselected reader which possibly has a smaller position.
                    // In either case we exit the galloping mode.

                    _peeked_readers.emplace_back(std::move(_galloping_reader));
                    boost::range::push_heap(_peeked_readers, _peeked_cmp);
                }
            } else {
                // See comment in `peek_reader`.
                if (_cmp(_galloping_reader->upper_bound, _pr_end) < 0) {
                    erase = true;
                } else {
                    _halted_readers.push_back(std::move(_galloping_reader));
                }
            }

            auto maybe_erase = erase ? erase_reader(std::move(_galloping_reader)) : make_ready_future<>();

            // The galloping reader has either been removed, halted, or lost with the other readers.
            // Proceed with the normal path.
          return maybe_erase.then([this, timeout] {
            _galloping_reader = {};
            _gallop_mode_hits = 0;
            return (*this)(timeout);
          });
        });
    }

public:
    clustering_order_reader_merger(
            schema_ptr schema, reader_permit permit,
            streamed_mutation::forwarding fwd_sm,
            std::unique_ptr<position_reader_queue> reader_queue)
        : _schema(std::move(schema)), _permit(std::move(permit))
        , _cmp(*_schema)
        , _reader_queue(std::move(reader_queue))
        , _peeked_cmp(*_schema)
        , _pr_end(fwd_sm == streamed_mutation::forwarding::yes
                        ? position_in_partition_view::before_all_clustered_rows()
                        : position_in_partition_view::after_all_clustered_rows())
        , _should_emit_partition_end(fwd_sm == streamed_mutation::forwarding::no)
    {
    }

    // We assume that operator() is called sequentially and that the caller doesn't use the batch
    // returned by the previous operator() call after calling operator() again
    // (the data from the previous batch is destroyed).
    future<mutation_fragment_batch> operator()(db::timeout_clock::time_point timeout) {
        _current_batch.clear();

        if (in_gallop_mode()) {
            return peek_galloping_reader(timeout);
        }

        if (!_unpeeked_readers.empty()) {
            return peek_readers(timeout).then([this, timeout] { return (*this)(timeout); });
        }

        // Before we return a batch of fragments using currently opened readers we must check the queue
        // for potential new readers that must be opened. There are three cases which determine how ``far''
        // should we look:
        // - If there are some peeked readers in the heap, we must check for new readers
        //   whose `min_position`s are <= the position of the first peeked reader; there is no need
        //   to check for ``later'' readers (yet).
        // - Otherwise, if we already fetched a partition start fragment, we need to look no further
        //   than the end of the current position range (_pr_end).
        // - Otherwise we need to look for any reader (by calling the queue with `after_all_clustered_rows`),
        //   even for readers whose `min_position`s may be outside the current position range since they
        //   may be the only readers which have a `partition_start` fragment which we need to return
        //   before end-of-stream.
        auto next_peeked_pos =
            _peeked_readers.empty()
                ? (_partition_start_fetched ? _pr_end : position_in_partition_view::after_all_clustered_rows())
                : _peeked_readers.front()->reader.peek_buffer().position();
        if (!_reader_queue->empty(next_peeked_pos)) {
            auto rs = _reader_queue->pop(next_peeked_pos);
            for (auto& r: rs) {
                _all_readers.push_front(std::move(r));
                _unpeeked_readers.push_back(_all_readers.begin());
            }
            return peek_readers(timeout).then([this, timeout] { return (*this)(timeout); });
        }

        if (_peeked_readers.empty()) {
            // We are either in forwarding mode and waiting for a fast-forward,
            // or we've exhausted all the readers.
            if (_should_emit_partition_end) {
                // Not forwarding, so all readers must be exhausted.
                // Return a partition end fragment unless all readers have been empty from the beginning.
                if (_partition_start_fetched) {
                    _current_batch.push_back(mutation_fragment(*_schema, _permit, partition_end()));
                }
                _should_emit_partition_end = false;
            }
            return make_ready_future<mutation_fragment_batch>(_current_batch);
        }

        // Take all fragments with the next smallest position (there may be multiple such fragments).
        do {
            boost::range::pop_heap(_peeked_readers, _peeked_cmp);
            auto r = _peeked_readers.back();
            auto mf = r->reader.pop_mutation_fragment();
            _peeked_readers.pop_back();
            _unpeeked_readers.push_back(std::move(r));
            _current_batch.push_back(std::move(mf));
        } while (!_peeked_readers.empty()
                    && _cmp(_current_batch.back().position(), _peeked_readers.front()->reader.peek_buffer().position()) == 0);

        if (_unpeeked_readers.size() == 1 && _unpeeked_readers.front() == _galloping_reader) {
            // The first condition says that only one reader was moved from the heap,
            // i.e. all other readers had strictly greater positions.
            // The second condition says that this reader already was a galloping candidate,
            // so let's increase his score.
            ++_gallop_mode_hits;

            if (in_gallop_mode()) {
                // We've entered gallop mode with _galloping_reader.
                // In the next operator() call we will peek this reader on a separate codepath,
                // using _galloping_reader instead of _unpeeked_readers.
                _unpeeked_readers.clear();
            }
        } else {
            // Each reader currently in _unpeeked_readers is a potential galloping candidate
            // (they won with all other readers in _peeked_readers). Remember one of them.
            _galloping_reader = _unpeeked_readers.front();
            _gallop_mode_hits = 1;
        }

        return make_ready_future<mutation_fragment_batch>(_current_batch);
    }

    future<> next_partition() {
        throw std::runtime_error(
            "clustering_order_reader_merger::next_partition: this reader works only for single partition queries");
    }


    future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) {
        throw std::runtime_error(
            "clustering_order_reader_merger::fast_forward_to: this reader works only for single partition queries");
    }


    future<> fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
        if (!_partition_start_fetched) {
            on_internal_error(mrlog, "reader was forwarded before returning partition start");
        }

        // Every reader in `_all_readers` has been peeked at least once, so it returned a partition_start.
        // Thus every opened reader is safe to be fast forwarded.
        _unpeeked_readers.clear();
        _peeked_readers.clear();
        _halted_readers.clear();
        _galloping_reader = {};
        _gallop_mode_hits = 0;

        _unpeeked_readers.reserve(_all_readers.size());
        for (auto it = _all_readers.begin(); it != _all_readers.end(); ++it) {
            _unpeeked_readers.push_back(it);
        }

        _forwarded_to = pr;
        _pr_end = _forwarded_to->end();

        return parallel_for_each(_unpeeked_readers, [this, pr = std::move(pr), timeout] (reader_iterator it) {
            return it->reader.fast_forward_to(pr, timeout);
        });
    }

    future<> close() noexcept {
        return parallel_for_each(std::move(_all_readers), [] (reader_and_upper_bound& r) {
            return r.reader.close();
        }).finally([this] {
            return _reader_queue->close();
        });
    }
};

flat_mutation_reader make_clustering_combined_reader(schema_ptr schema,
        reader_permit permit,
        streamed_mutation::forwarding fwd_sm,
        std::unique_ptr<position_reader_queue> rq) {
    return make_flat_mutation_reader<merging_reader<clustering_order_reader_merger>>(
            schema, permit, fwd_sm,
            clustering_order_reader_merger(schema, permit, fwd_sm, std::move(rq)));
}
