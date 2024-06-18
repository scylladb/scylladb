/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <seastar/core/coroutine.hh>

#include "readers/empty_v2.hh"
#include "readers/clustering_combined.hh"
#include "readers/range_tombstone_change_merger.hh"
#include "readers/combined.hh"

extern logging::logger mrlog;

static constexpr size_t merger_small_vector_size = 4;

template<typename T>
using merger_vector = utils::small_vector<T, merger_small_vector_size>;

using stream_id_t = const mutation_reader*;

struct mutation_fragment_and_stream_id {
    mutation_fragment_v2 fragment;
    stream_id_t stream_id;

    mutation_fragment_and_stream_id(mutation_fragment_v2 fragment, stream_id_t stream_id) : fragment(std::move(fragment)), stream_id(stream_id)
    { }
};

using mutation_fragment_batch = boost::iterator_range<merger_vector<mutation_fragment_and_stream_id>::iterator>;
using mutation_fragment_batch_opt = std::optional<mutation_fragment_batch>;

template<typename Producer>
concept FragmentProducer = requires(Producer p, dht::partition_range part_range, position_range pos_range) {
    // The returned fragments are expected to have the same
    // position_in_partition. Iterators and references are expected
    // to be valid until the next call to operator()().
    { p() } -> std::same_as<future<mutation_fragment_batch>>;

    // The following functions have the same semantics as their
    // mutation_reader counterparts.
    { p.next_partition() } -> std::same_as<future<>>;
    { p.fast_forward_to(part_range) } -> std::same_as<future<>>;
    { p.fast_forward_to(pos_range) } -> std::same_as<future<>>;
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
    using iterator = merger_vector<mutation_fragment_and_stream_id>::iterator;

    const schema_ptr _schema;
    reader_permit _permit;
    Producer _producer;
    range_tombstone_change_merger<stream_id_t> _tombstone_merger;
    mutation_fragment_v2_opt _result;

public:
    mutation_fragment_merger(schema_ptr schema, reader_permit permit, Producer&& producer)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _producer(std::move(producer)) {
    }

    future<mutation_fragment_v2_opt> operator()() {
        _result = {};
        return repeat([this] {
            return _producer().then([this] (mutation_fragment_batch fragments) {
                const auto begin = fragments.begin();
                const auto end = fragments.end();
                if (begin == end) {
                    return stop_iteration::yes;
                }
                // If fragment is a range tombstone change, all others in the batch
                // have to be too. This follows from all fragments in the batch
                // having identical positions, and range tombstones never having the
                // same position as a clustering row.
                if (begin->fragment.is_range_tombstone_change()) {
                    for (auto it = begin; it != end; ++it) {
                        _tombstone_merger.apply(it->stream_id, it->fragment.as_range_tombstone_change().tombstone());
                    }
                    if (auto tomb_opt = _tombstone_merger.get()) {
                        _result = mutation_fragment_v2(*_schema, _permit, range_tombstone_change(begin->fragment.position(), *tomb_opt));
                        return stop_iteration::yes;
                    }
                    return stop_iteration::no;
                } else {
                    for (auto it = begin + 1; it != end; ++it) {
                        begin->fragment.apply(*_schema, std::move(it->fragment));
                    }
                    _result = std::move(begin->fragment);
                    return stop_iteration::yes;
                }
            });
        }).then([this] {
            return std::move(_result);
        });
    }

    future<> next_partition() {
        _tombstone_merger.clear();
        return _producer.next_partition();
    }

    future<> fast_forward_to(const dht::partition_range& pr) {
        _tombstone_merger.clear();
        return _producer.fast_forward_to(pr);
    }

    future<> fast_forward_to(position_range pr) {
        _tombstone_merger.clear();
        return _producer.fast_forward_to(std::move(pr));
    }

    future<> close() noexcept {
        return _producer.close();
    }
};

// Merges the output of the sub-readers into a single non-decreasing
// stream of mutation-fragments.
class mutation_reader_merger {
public:
    using reader_iterator = std::list<mutation_reader>::iterator;

    struct reader_and_fragment {
        reader_iterator reader{};
        mutation_fragment_v2 fragment;

        reader_and_fragment(reader_iterator r, mutation_fragment_v2 f)
            : reader(r)
            , fragment(std::move(f)) {
        }
    };

    struct reader_and_last_fragment_kind {
        reader_iterator reader{};
        mutation_fragment_v2::kind last_kind = mutation_fragment_v2::kind::partition_end;

        reader_and_last_fragment_kind() = default;

        reader_and_last_fragment_kind(reader_iterator r, mutation_fragment_v2::kind k)
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
    std::list<mutation_reader> _all_readers;
    // We launch a close call to an unneeded reader one at a time, using
    // a continuation chain. We'll only wait for their completion if the
    // submission rate is higher than the retire rate, to prevent memory
    // usage from growing unbounded.
    future<> _to_close = make_ready_future<>();
    size_t _pending_close = 0;
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
    merger_vector<mutation_fragment_and_stream_id> _current;
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
    future<mutation_fragment_batch_opt> maybe_produce_batch();
    void maybe_add_readers_at_partition_boundary();
    void maybe_add_readers(const std::optional<dht::ring_position_view>& pos);
    void add_readers(std::vector<mutation_reader> new_readers);
    bool in_gallop_mode() const;
    future<needs_merge> prepare_one(reader_and_last_fragment_kind rk, reader_galloping reader_galloping);
    future<needs_merge> advance_galloping_reader();
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
    future<> next_partition();
    future<> fast_forward_to(const dht::partition_range& pr);
    future<> fast_forward_to(position_range pr);
    future<> close() noexcept;
};

/* Merge a non-decreasing stream of mutation fragment batches
 * produced by a FragmentProducer into a non-decreasing stream
 * of mutation fragments.
 *
 * See `mutation_fragment_merger` for details.
 *
 * This class is a simple adapter over `mutation_fragment_merger` that provides
 * a `mutation_reader` interface. */
template <FragmentProducer Producer>
class merging_reader : public mutation_reader::impl {
    mutation_fragment_merger<Producer> _merger;
    streamed_mutation::forwarding _fwd_sm;
public:
    merging_reader(schema_ptr schema,
            reader_permit permit,
            streamed_mutation::forwarding fwd_sm,
            Producer&& producer)
        : impl(std::move(schema), std::move(permit))
        , _merger(_schema, _permit, std::move(producer))
        , _fwd_sm(fwd_sm) {}

    virtual future<> fill_buffer() override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr) override;
    virtual future<> fast_forward_to(position_range pr) override;
    virtual future<> close() noexcept override;
};

// Dumb selector implementation for mutation_reader_merger that simply
// forwards it's list of readers.
class list_reader_selector : public reader_selector {
    std::vector<mutation_reader> _readers;

public:
    explicit list_reader_selector(schema_ptr s, std::vector<mutation_reader> readers)
        : reader_selector(s, dht::ring_position_view::min())
        , _readers(std::move(readers)) {
    }

    list_reader_selector(const list_reader_selector&) = delete;
    list_reader_selector& operator=(const list_reader_selector&) = delete;

    list_reader_selector(list_reader_selector&&) = default;
    list_reader_selector& operator=(list_reader_selector&&) = default;

    virtual std::vector<mutation_reader> create_new_readers(const std::optional<dht::ring_position_view>&) override {
        _selector_position = dht::ring_position_view::max();
        return std::exchange(_readers, {});
    }

    virtual std::vector<mutation_reader> fast_forward_to(const dht::partition_range&) override {
        return {};
    }
};

void mutation_reader_merger::maybe_add_readers(const std::optional<dht::ring_position_view>& pos) {
    if (_selector->has_new_readers(pos)) {
        add_readers(_selector->create_new_readers(pos));
    }
}

void mutation_reader_merger::add_readers(std::vector<mutation_reader> new_readers) {
    for (auto&& new_reader : new_readers) {
        _all_readers.emplace_back(std::move(new_reader));
        _next.emplace_back(std::prev(_all_readers.end()), mutation_fragment_v2::kind::partition_end);
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

future<mutation_reader_merger::needs_merge> mutation_reader_merger::advance_galloping_reader() {
    return prepare_one(_galloping_reader, reader_galloping::yes).then([this] (needs_merge needs_merge) {
        maybe_add_readers_at_partition_boundary();
        return needs_merge;
    });
}

future<> mutation_reader_merger::prepare_next() {
    return parallel_for_each(_next, [this] (reader_and_last_fragment_kind rk) {
        return prepare_one(rk, reader_galloping::no).discard_result();
    }).then([this] {
        _next.clear();
        maybe_add_readers_at_partition_boundary();
    });
}

future<mutation_reader_merger::needs_merge> mutation_reader_merger::prepare_one(
        reader_and_last_fragment_kind rk, reader_galloping reader_galloping) {
    return (*rk.reader)().then([this, rk, reader_galloping] (mutation_fragment_v2_opt mfo) {
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
                        _current.emplace_back(std::move(*mfo), &*_galloping_reader.reader);
                        _galloping_reader.last_kind = _current.back().fragment.mutation_fragment_kind();
                        return make_ready_future<needs_merge>(needs_merge::no);
                    }

                    _gallop_mode_hits = 0;
                }

                _fragment_heap.emplace_back(rk.reader, std::move(*mfo));
                boost::range::push_heap(_fragment_heap, fragment_heap_compare(*_schema));
            }
        } else if (_fwd_sm == streamed_mutation::forwarding::yes && rk.last_kind != mutation_fragment_v2::kind::partition_end) {
            // When in streamed_mutation::forwarding mode we need
            // to keep track of readers that returned
            // end-of-stream to know what readers to ff. We can't
            // just ff all readers as we might drop fragments from
            // partitions we haven't even read yet.
            // Readers whose last emitted fragment was a partition
            // end are out of data for good for the current range.
            _halted_readers.push_back(rk);
        } else if (_fwd_mr == mutation_reader::forwarding::no) {
            mutation_reader r = std::move(*rk.reader);
            _all_readers.erase(rk.reader);
            _pending_close++;
            _to_close = _to_close.then([this, r = std::move(r)] () mutable {
                return r.close().then([this] { _pending_close--; });
            });
            if (reader_galloping) {
                // Galloping reader iterator may have become invalid at this point, so - to be safe - clear it
                auto fut = _galloping_reader.reader->close();
                _to_close = when_all_succeed(std::move(_to_close), std::move(fut)).discard_result();
            }
        }

        if (reader_galloping) {
            _gallop_mode_hits = 0;
        }
        // to_close is a chain of mutation_reader close futures,
        // therefore it can not fail.
        // To prevent memory usage from growing unbounded, we'll wait for pending closes
        // if we're submitting them faster than we can retire them.
        future<> to_close = _pending_close >= 4 ? std::exchange(_to_close, make_ready_future<>()) : make_ready_future<>();
        return to_close.then([] {
            return needs_merge::yes;
        });
    });
}

void mutation_reader_merger::prepare_forwardable_readers() {
    auto prepare_single_reader = _single_reader.reader != reader_iterator{};

    _next.reserve(_halted_readers.size() + _fragment_heap.size() + _next.size() +
        prepare_single_reader + in_gallop_mode());

    std::move(_halted_readers.begin(), _halted_readers.end(), std::back_inserter(_next));
    if (prepare_single_reader) {
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

future<mutation_fragment_batch> mutation_reader_merger::operator()() {
    return repeat_until_value([this] { return maybe_produce_batch(); });
}

future<mutation_fragment_batch_opt> mutation_reader_merger::maybe_produce_batch() {
    // Avoid merging-related logic if we know that only a single reader owns
    // current partition.
    if (_single_reader.reader != reader_iterator{}) {
        if (_single_reader.reader->is_buffer_empty()) {
            if (_single_reader.reader->is_end_of_stream()) {
                _current.clear();
                return make_ready_future<mutation_fragment_batch_opt>(mutation_fragment_batch(_current, &_single_reader));
            }
            return _single_reader.reader->fill_buffer().then([] {
                return make_ready_future<mutation_fragment_batch_opt>();
            });
        }
        _current.clear();
        _current.emplace_back(_single_reader.reader->pop_mutation_fragment(), &*_single_reader.reader);
        _single_reader.last_kind = _current.back().fragment.mutation_fragment_kind();
        if (_current.back().fragment.is_end_of_partition()) {
            _next.emplace_back(std::exchange(_single_reader.reader, {}), mutation_fragment_v2::kind::partition_end);
        }
        return make_ready_future<mutation_fragment_batch_opt>(_current);
    }

    if (in_gallop_mode()) {
        return advance_galloping_reader().then([this] (needs_merge needs_merge) {
            if (!needs_merge) {
                return make_ready_future<mutation_fragment_batch_opt>(_current);
            }
            // Galloping reader may have lost to some other reader. In that case, we should proceed
            // with standard merging logic.
            return make_ready_future<mutation_fragment_batch_opt>();
        });
    }

    if (!_next.empty()) {
        return prepare_next().then([] { return make_ready_future<mutation_fragment_batch_opt>(); });
    }

    _current.clear();

    // If we ran out of fragments for the current partition, select the
    // readers for the next one.
    if (_fragment_heap.empty()) {
        if (!_halted_readers.empty() || _reader_heap.empty()) {
            return make_ready_future<mutation_fragment_batch_opt>(_current);
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
            _single_reader = { _fragment_heap.back().reader, mutation_fragment_v2::kind::partition_start };
            _current.emplace_back(std::move(_fragment_heap.back().fragment), &*_single_reader.reader);
            _fragment_heap.clear();
            _gallop_mode_hits = 0;
            return make_ready_future<mutation_fragment_batch_opt>(_current);
        }
    }

    const auto equal = position_in_partition::equal_compare(*_schema);
    do {
        boost::range::pop_heap(_fragment_heap, fragment_heap_compare(*_schema));
        auto& n = _fragment_heap.back();
        const auto kind = n.fragment.mutation_fragment_kind();
        _current.emplace_back(std::move(n.fragment), &*n.reader);
        _next.emplace_back(n.reader, kind);
        _fragment_heap.pop_back();
    }
    while (!_fragment_heap.empty() && equal(_current.back().fragment.position(), _fragment_heap.front().fragment.position()));

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

    return make_ready_future<mutation_fragment_batch_opt>(_current);
}

future<> mutation_reader_merger::next_partition() {
    // If the last batch of fragments returned by operator() came from partition P,
    // we must forward to the partition immediately following P (as per the `next_partition`
    // contract in `mutation_reader`).
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
        rk.last_kind = mutation_fragment_v2::kind::partition_end;
        co_await rk.reader->next_partition();
    }
}

future<> mutation_reader_merger::fast_forward_to(const dht::partition_range& pr) {
    _single_reader = { };
    _gallop_mode_hits = 0;
    _next.clear();
    _halted_readers.clear();
    _fragment_heap.clear();
    _reader_heap.clear();

    for (auto it = _all_readers.begin(); it != _all_readers.end(); ++it) {
        _next.emplace_back(it, mutation_fragment_v2::kind::partition_end);
    }
    return parallel_for_each(_all_readers, [&pr] (mutation_reader& mr) {
        return mr.fast_forward_to(pr);
    }).then([this, &pr] {
        add_readers(_selector->fast_forward_to(pr));
    });
}

future<> mutation_reader_merger::fast_forward_to(position_range pr) {
    prepare_forwardable_readers();
    return parallel_for_each(_next, [pr = std::move(pr)] (reader_and_last_fragment_kind rk) {
        return rk.reader->fast_forward_to(pr);
    });
}

future<> mutation_reader_merger::close() noexcept {
    return std::exchange(_to_close, make_ready_future<>()).then([this] {
        return parallel_for_each(std::move(_all_readers), [] (mutation_reader& mr) {
            return mr.close();
        });
    });
}

template <FragmentProducer P>
future<> merging_reader<P>::fill_buffer() {
    return repeat([this] {
        return _merger().then([this] (mutation_fragment_v2_opt mfo) {
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
        // of `mutation_reader`, which `FragmentProducer` follows).
        if (is_buffer_empty()) {
            return _merger.next_partition();
        }
    }
    return make_ready_future<>();
}

template <FragmentProducer P>
future<> merging_reader<P>::fast_forward_to(const dht::partition_range& pr) {
    clear_buffer();
    _end_of_stream = false;
    return _merger.fast_forward_to(pr);
}

template <FragmentProducer P>
future<> merging_reader<P>::fast_forward_to(position_range pr) {
    clear_buffer();
    _end_of_stream = false;
    return _merger.fast_forward_to(std::move(pr));
}

template <FragmentProducer P>
future<> merging_reader<P>::close() noexcept {
    return _merger.close();
}

mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        std::unique_ptr<reader_selector> selector,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    return make_mutation_reader<merging_reader<mutation_reader_merger>>(schema,
            std::move(permit),
            fwd_sm,
            mutation_reader_merger(schema, std::move(selector), fwd_sm, fwd_mr));
}

mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        std::vector<mutation_reader> readers,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    if (readers.empty()) {
        return make_empty_flat_reader_v2(std::move(schema), std::move(permit));
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

mutation_reader make_combined_reader(schema_ptr schema,
        reader_permit permit,
        mutation_reader&& a,
        mutation_reader&& b,
        streamed_mutation::forwarding fwd_sm,
        mutation_reader::forwarding fwd_mr) {
    std::vector<mutation_reader> v;
    v.reserve(2);
    v.push_back(std::move(a));
    v.push_back(std::move(b));
    return make_combined_reader(std::move(schema), std::move(permit), std::move(v), fwd_sm, fwd_mr);
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
    merger_vector<mutation_fragment_and_stream_id> _current_batch;

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
    future<> peek_reader(reader_iterator it) {
        return it->reader.peek().then([this, it] (mutation_fragment_v2* mf) {
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
                auto f = _forwarded_to ? it->reader.fast_forward_to(*_forwarded_to) : make_ready_future<>();
                return f.then([this, it] { return peek_reader(it); });
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

    future<> peek_readers() {
        return parallel_for_each(_unpeeked_readers, [this] (reader_iterator it) {
            return peek_reader(it);
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
    future<mutation_fragment_batch_opt> peek_galloping_reader() {
        return _galloping_reader->reader.peek().then([this] (mutation_fragment_v2* mf) {
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
                        _current_batch.emplace_back(_galloping_reader->reader.pop_mutation_fragment(), &_galloping_reader->reader);

                        return make_ready_future<mutation_fragment_batch_opt>(_current_batch);
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
          return maybe_erase.then([this] {
            _galloping_reader = {};
            _gallop_mode_hits = 0;
            return make_ready_future<mutation_fragment_batch_opt>();
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
    future<mutation_fragment_batch> operator()() {
        return repeat_until_value([this] { return maybe_produce_batch(); });
    }

    future<mutation_fragment_batch_opt> maybe_produce_batch() {
        _current_batch.clear();

        if (in_gallop_mode()) {
            return peek_galloping_reader();
        }

        if (!_unpeeked_readers.empty()) {
            return peek_readers().then([] { return make_ready_future<mutation_fragment_batch_opt>(); });
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
            return peek_readers().then([] { return make_ready_future<mutation_fragment_batch_opt>(); });
        }

        if (_peeked_readers.empty()) {
            // We are either in forwarding mode and waiting for a fast-forward,
            // or we've exhausted all the readers.
            if (_should_emit_partition_end) {
                // Not forwarding, so all readers must be exhausted.
                // Return a partition end fragment unless all readers have been empty from the beginning.
                if (_partition_start_fetched) {
                    _current_batch.emplace_back(mutation_fragment_v2(*_schema, _permit, partition_end()), nullptr);
                }
                _should_emit_partition_end = false;
            }
            return make_ready_future<mutation_fragment_batch_opt>(_current_batch);
        }

        // Take all fragments with the next smallest position (there may be multiple such fragments).
        do {
            boost::range::pop_heap(_peeked_readers, _peeked_cmp);
            auto r = _peeked_readers.back();
            auto mf = r->reader.pop_mutation_fragment();
            _peeked_readers.pop_back();
            _unpeeked_readers.push_back(std::move(r));
            _current_batch.emplace_back(std::move(mf), &_unpeeked_readers.back()->reader);
        } while (!_peeked_readers.empty()
                    && _cmp(_current_batch.back().fragment.position(), _peeked_readers.front()->reader.peek_buffer().position()) == 0);

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

        return make_ready_future<mutation_fragment_batch_opt>(_current_batch);
    }

    future<> next_partition() {
        throw std::runtime_error(
            "clustering_order_reader_merger::next_partition: this reader works only for single partition queries");
    }


    future<> fast_forward_to(const dht::partition_range&) {
        throw std::runtime_error(
            "clustering_order_reader_merger::fast_forward_to: this reader works only for single partition queries");
    }


    future<> fast_forward_to(position_range pr) {
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

        return parallel_for_each(_unpeeked_readers, [pr = std::move(pr)] (reader_iterator it) {
            return it->reader.fast_forward_to(pr);
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

mutation_reader make_clustering_combined_reader(schema_ptr schema,
        reader_permit permit,
        streamed_mutation::forwarding fwd_sm,
        std::unique_ptr<position_reader_queue> rq) {
    return make_mutation_reader<merging_reader<clustering_order_reader_merger>>(
            schema, permit, fwd_sm,
            clustering_order_reader_merger(schema, permit, fwd_sm, std::move(rq)));
}
