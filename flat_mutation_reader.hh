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

#pragma once

#include <seastar/util/bool_class.hh>
#include <seastar/core/future.hh>

#include "dht/i_partitioner.hh"
#include "mutation_fragment.hh"
#include "tracing/trace_state.hh"
#include "mutation.hh"
#include "mutation_consumer_concepts.hh"

#include <seastar/core/thread.hh>
#include <seastar/core/file.hh>
#include "reader_permit.hh"

#include <deque>

using seastar::future;

class mutation_source;
class position_in_partition;

/// \brief Represents a stream of mutation fragments.
///
/// Mutation fragments represent writes to the database.
///
/// Each fragment has an implicit position in the database,
/// which also determines its position in the stream relative to other fragments.
/// The global position of a fragment is a tuple ordered lexicographically:
///
///    (ring_position of a partition key, position_in_partition)
///
/// The stream has a hierarchical form. All fragments which occur
/// between partition_start and partition_end represent writes to the partition
/// identified by the partition_start::key(). The partition key is not repeated
/// with inner fragments.
///
/// The stream of mutation fragments conforms to the following form:
///
///   stream ::= partition*
///   partition ::= partition_start static_row? clustered* partition_end
///   clustered ::= clustering_row | range_tombstone
///
/// The range_tombstone fragments can have ranges which overlap with other
/// clustered fragments.
///
/// Consecutive range_tombstone fragments can have the same position(), so they
/// are weakly ordered. This makes merging two streams easier, and is
/// relied upon by combined_mutation_reader.
///
/// \section Clustering restrictions
///
/// A stream may produce writes relevant to only some clustering ranges, for
/// example by specifying clustering ranges in a partition_slice passed to
/// mutation_source::make_reader(). This will make the stream return information
/// for a subset of writes that it would normally return should the stream be
/// unrestricted.
///
/// The restricted stream obeys the following rules:
///
///   0) The stream must contain fragments corresponding to all writes
///      which are relevant to the requested ranges.
///
///   1) The stream _may_ contain fragments with information
///      about _some_ of the writes which are relevant to clustering ranges
///      outside of the requested ranges.
///
///   2) The stream will not contain writes which are absent in the unrestricted stream,
///      both for the requested clustering ranges and not requested ranges.
///      This means that it's safe to populate cache with all the returned information.
///      Even though it may be incomplete for non-requested ranges, it won't contain
///      incorrect information.
///
///   3) All clustered fragments have position() which is within the requested
///      ranges.
///
///   4) range_tombstone ranges are trimmed to the boundaries of requested ranges.
///
/// \section Intra-partition fast-forwarding mode
///
/// The stream can operate in an alternative mode when streamed_mutation::forwarding::yes
/// is passed to the stream constructor (see mutation_source).
///
/// In this mode, the original stream is not produced at once, but divided into sub-streams, where
/// each is produced at a time, ending with the end-of-stream condition (is_end_of_stream()).
/// The user needs to explicitly advance the stream to the next sub-stream by calling
/// fast_forward_to() or next_partition().
///
/// The original stream is divided like this:
///
///    1) For every partition, the first sub-stream will contain
///       partition_start and the static_row
///
///    2) Calling fast_forward_to() moves to the next sub-stream within the
///       current partition. The stream will contain all fragments relevant to
///       the position_range passed to fast_forward_to().
///
///    3) The position_range passed to fast_forward_to() is a clustering key restriction.
///       Same rules apply as with clustering restrictions described above except for point (4) above:
///       range tombstones can extend the range passed to fast_forward_to().
///
///    4) range_tombstones produced in earlier sub-stream which are also relevant
///       for next sub-streams do not have to be repeated. They _may_ be repeated
///       with a starting position trimmed.
///
///    5) partition_end is never emitted, the user needs to call next_partition()
///       to move to the next partition in the original stream, which will open
///       the initial sub-stream of the next partition.
///       An empty sub-stream after next_partition() indicates global end-of-stream (no next partition).
///
/// \section Consuming
///
/// The best way to consume those mutation_fragments is to call
/// flat_mutation_reader::consume with a consumer that receives the fragments.
///
class flat_mutation_reader final {
public:
    using tracked_buffer = circular_buffer<mutation_fragment, tracking_allocator<mutation_fragment>>;

    class impl {
    private:
        tracked_buffer _buffer;
        size_t _buffer_size = 0;
    protected:
        size_t max_buffer_size_in_bytes = default_max_buffer_size_in_bytes();
        bool _end_of_stream = false;
        schema_ptr _schema;
        reader_permit _permit;
        friend class flat_mutation_reader;
    protected:
        template<typename... Args>
        void push_mutation_fragment(Args&&... args) {
            seastar::memory::on_alloc_point(); // for exception safety tests
            _buffer.emplace_back(std::forward<Args>(args)...);
            _buffer_size += _buffer.back().memory_usage();
        }
        void clear_buffer() {
            _buffer.erase(_buffer.begin(), _buffer.end());
            _buffer_size = 0;
        }
        void forward_buffer_to(const position_in_partition& pos);
        void clear_buffer_to_next_partition();
        template<typename Source>
        future<bool> fill_buffer_from(Source&);
        // When succeeds, makes sure that the next push_mutation_fragment() will not fail.
        void reserve_one() {
            if (_buffer.capacity() == _buffer.size()) {
                _buffer.reserve(_buffer.size() * 2 + 1);
            }
        }
        const tracked_buffer& buffer() const {
            return _buffer;
        }
    public:
        impl(schema_ptr s, reader_permit permit) : _buffer(permit), _schema(std::move(s)), _permit(std::move(permit)) { }
        virtual ~impl() {}
        virtual future<> fill_buffer() = 0;
        virtual future<> next_partition() = 0;

        bool is_end_of_stream() const { return _end_of_stream; }
        bool is_buffer_empty() const { return _buffer.empty(); }
        bool is_buffer_full() const { return _buffer_size >= max_buffer_size_in_bytes; }
        static constexpr size_t default_max_buffer_size_in_bytes() { return 8 * 1024; }

        mutation_fragment pop_mutation_fragment() {
            auto mf = std::move(_buffer.front());
            _buffer.pop_front();
            _buffer_size -= mf.memory_usage();
            return mf;
        }

        void unpop_mutation_fragment(mutation_fragment mf) {
            const auto memory_usage = mf.memory_usage();
            _buffer.emplace_front(std::move(mf));
            _buffer_size += memory_usage;
        }

        future<mutation_fragment_opt> operator()() {
            if (is_buffer_empty()) {
                if (is_end_of_stream()) {
                    return make_ready_future<mutation_fragment_opt>();
                }
                return fill_buffer().then([this] { return operator()(); });
            }
            return make_ready_future<mutation_fragment_opt>(pop_mutation_fragment());
        }

        template<typename Consumer>
        requires FlatMutationReaderConsumer<Consumer>
        // Stops when consumer returns stop_iteration::yes or end of stream is reached.
        // Next call will start from the next mutation_fragment in the stream.
        future<> consume_pausable(Consumer consumer) {
            return repeat([this, consumer = std::move(consumer)] () mutable {
                if (is_buffer_empty()) {
                    if (is_end_of_stream()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                    return fill_buffer().then([] {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                }

                if constexpr (std::is_same_v<future<stop_iteration>, decltype(consumer(pop_mutation_fragment()))>) {
                    return consumer(pop_mutation_fragment());
                } else {
                    auto result = stop_iteration::no;
                    while ((result = consumer(pop_mutation_fragment())) != stop_iteration::yes && !is_buffer_empty() && !need_preempt()) {}
                    return make_ready_future<stop_iteration>(result);
                }
            });
        }

        template<typename Consumer, typename Filter>
        requires FlatMutationReaderConsumer<Consumer> && FlattenedConsumerFilter<Filter>
        // A variant of consume_pausable() that expects to be run in
        // a seastar::thread.
        // Partitions for which filter(decorated_key) returns false are skipped
        // entirely and never reach the consumer.
        void consume_pausable_in_thread(Consumer consumer, Filter filter) {
            while (true) {
                if (need_preempt()) {
                    seastar::thread::yield();
                }
                if (is_buffer_empty()) {
                    if (is_end_of_stream()) {
                        return;
                    }
                    fill_buffer().get();
                    continue;
                }
                auto mf = pop_mutation_fragment();
                if (mf.is_partition_start() && !filter(mf.as_partition_start().key())) {
                    next_partition().get();
                    continue;
                }
                if (!filter(mf)) {
                    continue;
                }
                auto do_stop = futurize_invoke([&consumer, mf = std::move(mf)] () mutable {
                    return consumer(std::move(mf));
                });
                if (do_stop.get0()) {
                    return;
                }
            }
        };

    private:
        template<typename Consumer>
        struct consumer_adapter {
            flat_mutation_reader::impl& _reader;
            std::optional<dht::decorated_key> _decorated_key;
            Consumer _consumer;
            consumer_adapter(flat_mutation_reader::impl& reader, Consumer c)
                    : _reader(reader)
                      , _consumer(std::move(c))
            { }
            future<stop_iteration> operator()(mutation_fragment&& mf) {
                return std::move(mf).consume(*this);
            }
            future<stop_iteration> consume(static_row&& sr) {
                return handle_result(_consumer.consume(std::move(sr)));
            }
            future<stop_iteration> consume(clustering_row&& cr) {
                return handle_result(_consumer.consume(std::move(cr)));
            }
            future<stop_iteration> consume(range_tombstone&& rt) {
                return handle_result(_consumer.consume(std::move(rt)));
            }
            future<stop_iteration> consume(partition_start&& ps) {
                _decorated_key.emplace(std::move(ps.key()));
                _consumer.consume_new_partition(*_decorated_key);
                if (ps.partition_tombstone()) {
                    _consumer.consume(ps.partition_tombstone());
                }
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }
            future<stop_iteration> consume(partition_end&& pe) {
              return futurize_invoke([this] {
                return _consumer.consume_end_of_partition();
              });
            }
        private:
            future<stop_iteration> handle_result(stop_iteration si) {
                if (si) {
                    if (_consumer.consume_end_of_partition()) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                    return _reader.next_partition().then([] {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                }
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }
        };
    public:
        template<typename Consumer>
        requires FlattenedConsumer<Consumer>
        // Stops when consumer returns stop_iteration::yes from consume_end_of_partition or end of stream is reached.
        // Next call will receive fragments from the next partition.
        // When consumer returns stop_iteration::yes from methods other than consume_end_of_partition then the read
        // of the current partition is ended, consume_end_of_partition is called and if it returns stop_iteration::no
        // then the read moves to the next partition.
        // Reference to the decorated key that is passed to consume_new_partition() remains valid until after
        // the call to consume_end_of_partition().
        //
        // This method is useful because most of current consumers use this semantic.
        //
        //
        // This method returns whatever is returned from Consumer::consume_end_of_stream().S
        auto consume(Consumer consumer) {
            return do_with(consumer_adapter<Consumer>(*this, std::move(consumer)), [this] (consumer_adapter<Consumer>& adapter) {
                return consume_pausable(std::ref(adapter)).then([this, &adapter] {
                    return adapter._consumer.consume_end_of_stream();
                });
            });
        }

        template<typename Consumer, typename Filter>
        requires FlattenedConsumer<Consumer> && FlattenedConsumerFilter<Filter>
        // A variant of consumee() that expects to be run in a seastar::thread.
        // Partitions for which filter(decorated_key) returns false are skipped
        // entirely and never reach the consumer.
        auto consume_in_thread(Consumer consumer, Filter filter) {
            auto adapter = consumer_adapter<Consumer>(*this, std::move(consumer));
            consume_pausable_in_thread(std::ref(adapter), std::move(filter));
            filter.on_end_of_stream();
            return adapter._consumer.consume_end_of_stream();
        };

        /*
         * fast_forward_to is forbidden on flat_mutation_reader created for a single partition.
         */
        virtual future<> fast_forward_to(const dht::partition_range&) = 0;
        virtual future<> fast_forward_to(position_range) = 0;

        // close should cancel any outstanding background operations,
        // if possible, and wait on them to complete.
        // It should also transitively close underlying resources
        // and wait on them too.
        //
        // Once closed, the reader should be unusable.
        //
        // Similar to destructors, close must never fail.
        virtual future<> close() noexcept = 0;

        size_t buffer_size() const {
            return _buffer_size;
        }

        tracked_buffer detach_buffer() noexcept {
            _buffer_size = 0;
            return std::exchange(_buffer, tracked_buffer(tracking_allocator<mutation_fragment>(_permit)));
        }

        void move_buffer_content_to(impl& other) {
            if (other._buffer.empty()) {
                std::swap(_buffer, other._buffer);
                other._buffer_size = std::exchange(_buffer_size, 0);
            } else {
                seastar::memory::on_alloc_point(); // for exception safety tests
                other._buffer.reserve(other._buffer.size() + _buffer.size());
                std::move(_buffer.begin(), _buffer.end(), std::back_inserter(other._buffer));
                _buffer.clear();
                other._buffer_size += std::exchange(_buffer_size, 0);
            }
        }

        void maybe_timed_out() {
            if (db::timeout_clock::now() >= timeout()) {
                throw timed_out_error();
            }
        }

        db::timeout_clock::time_point timeout() const noexcept {
            return _permit.timeout();
        }

        void set_timeout(db::timeout_clock::time_point timeout) noexcept {
            _permit.set_timeout(timeout);
        }
    };
private:
    std::unique_ptr<impl> _impl;

    flat_mutation_reader() = default;
    explicit operator bool() const noexcept { return bool(_impl); }
    friend class optimized_optional<flat_mutation_reader>;
    void do_upgrade_schema(const schema_ptr&);
    static void on_close_error(std::unique_ptr<impl>, std::exception_ptr ep) noexcept;
public:
    // Documented in mutation_reader::forwarding.
    class partition_range_forwarding_tag;
    using partition_range_forwarding = bool_class<partition_range_forwarding_tag>;

    flat_mutation_reader(std::unique_ptr<impl> impl) noexcept : _impl(std::move(impl)) {}
    flat_mutation_reader(const flat_mutation_reader&) = delete;
    flat_mutation_reader(flat_mutation_reader&&) = default;

    flat_mutation_reader& operator=(const flat_mutation_reader&) = delete;
    flat_mutation_reader& operator=(flat_mutation_reader&& o) noexcept;

    ~flat_mutation_reader();

    future<mutation_fragment_opt> operator()() {
        return _impl->operator()();
    }

    template <typename Consumer>
    requires FlatMutationReaderConsumer<Consumer>
    auto consume_pausable(Consumer consumer) {
        return _impl->consume_pausable(std::move(consumer));
    }

    template <typename Consumer>
    requires FlattenedConsumer<Consumer>
    auto consume(Consumer consumer) {
        return _impl->consume(std::move(consumer));
    }

    class filter {
    private:
        std::function<bool (const dht::decorated_key&)> _partition_filter = [] (const dht::decorated_key&) { return true; };
        std::function<bool (const mutation_fragment&)> _mutation_fragment_filter = [] (const mutation_fragment&) { return true; };
    public:
        filter() = default;

        filter(std::function<bool (const dht::decorated_key&)>&& pf)
            : _partition_filter(std::move(pf))
        { }

        filter(std::function<bool (const dht::decorated_key&)>&& pf,
               std::function<bool (const mutation_fragment&)>&& mf)
            : _partition_filter(std::move(pf))
            , _mutation_fragment_filter(std::move(mf))
        { }

        template <typename Functor>
        filter(Functor&& f)
            : _partition_filter(std::forward<Functor>(f))
        { }

        bool operator()(const dht::decorated_key& dk) const {
            return _partition_filter(dk);
        }

        bool operator()(const mutation_fragment& mf) const {
            return _mutation_fragment_filter(mf);
        }

        void on_end_of_stream() const { }
    };

    struct no_filter {
        bool operator()(const dht::decorated_key& dk) const {
            return true;
        }

        bool operator()(const mutation_fragment& mf) const {
            return true;
        }

        void on_end_of_stream() const { }
    };

    template<typename Consumer, typename Filter>
    requires FlattenedConsumer<Consumer> && FlattenedConsumerFilter<Filter>
    auto consume_in_thread(Consumer consumer, Filter filter) {
        return _impl->consume_in_thread(std::move(consumer), std::move(filter));
    }

    template<typename Consumer>
    requires FlattenedConsumer<Consumer>
    auto consume_in_thread(Consumer consumer) {
        return consume_in_thread(std::move(consumer), no_filter{});
    }

    // Skips to the next partition.
    //
    // Skips over the remaining fragments of the current partitions. If the
    // reader is currently positioned at a partition start nothing is done.
    //
    // If the last produced fragment comes from partition `P`, then the reader
    // is considered to still be in partition `P`, which means that `next_partition`
    // will move the reader to the partition immediately following `P`.
    // This case happens in particular when the last produced fragment was
    // `partition_end` for `P`.
    //
    // Only skips within the current partition range, i.e. if the current
    // partition is the last in the range the reader will be at EOS.
    //
    // Can be used to skip over entire partitions if interleaved with
    // `operator()()` calls.
    future<> next_partition() { return _impl->next_partition(); }

    future<> fill_buffer() { return _impl->fill_buffer(); }

    // Changes the range of partitions to pr. The range can only be moved
    // forwards. pr.begin() needs to be larger than pr.end() of the previousl
    // used range (i.e. either the initial one passed to the constructor or a
    // previous fast forward target).
    // pr needs to be valid until the reader is destroyed or fast_forward_to()
    // is called again.
    future<> fast_forward_to(const dht::partition_range& pr) {
        return _impl->fast_forward_to(pr);
    }
    // Skips to a later range of rows.
    // The new range must not overlap with the current range.
    //
    // In forwarding mode the stream does not return all fragments right away,
    // but only those belonging to the current clustering range. Initially
    // current range only covers the static row. The stream can be forwarded
    // (even before end-of- stream) to a later range with fast_forward_to().
    // Forwarding doesn't change initial restrictions of the stream, it can
    // only be used to skip over data.
    //
    // Monotonicity of positions is preserved by forwarding. That is fragments
    // emitted after forwarding will have greater positions than any fragments
    // emitted before forwarding.
    //
    // For any range, all range tombstones relevant for that range which are
    // present in the original stream will be emitted. Range tombstones
    // emitted before forwarding which overlap with the new range are not
    // necessarily re-emitted.
    //
    // When forwarding mode is not enabled, fast_forward_to()
    // cannot be used.
    //
    // `fast_forward_to` can be called only when the reader is within a partition
    // and it affects the set of fragments returned from that partition.
    // In particular one must first enter a partition by fetching a `partition_start`
    // fragment before calling `fast_forward_to`.
    future<> fast_forward_to(position_range cr) {
        return _impl->fast_forward_to(std::move(cr));
    }
    // Closes the reader.
    //
    // Note: The reader object can can be safely destroyed after close returns.
    // since close makes sure to keep the underlying impl object alive until
    // the latter's close call is resolved.
    future<> close() noexcept {
        if (auto i = std::move(_impl)) {
            auto f = i->close();
            // most close implementations are expexcted to return a ready future
            // so expedite prcessing it.
            if (f.available() && !f.failed()) {
                return f;
            }
            // close must not fail
            return f.handle_exception([i = std::move(i)] (std::exception_ptr ep) mutable {
                on_close_error(std::move(i), std::move(ep));
            });
        }
        return make_ready_future<>();
    }
    bool is_end_of_stream() const { return _impl->is_end_of_stream(); }
    bool is_buffer_empty() const { return _impl->is_buffer_empty(); }
    bool is_buffer_full() const { return _impl->is_buffer_full(); }
    static constexpr size_t default_max_buffer_size_in_bytes() {
        return impl::default_max_buffer_size_in_bytes();
    }
    mutation_fragment pop_mutation_fragment() { return _impl->pop_mutation_fragment(); }
    void unpop_mutation_fragment(mutation_fragment mf) { _impl->unpop_mutation_fragment(std::move(mf)); }
    const schema_ptr& schema() const { return _impl->_schema; }
    const reader_permit& permit() const { return _impl->_permit; }
    db::timeout_clock::time_point timeout() const noexcept { return _impl->timeout(); }
    void set_timeout(db::timeout_clock::time_point timeout) noexcept { _impl->set_timeout(timeout); }
    void set_max_buffer_size(size_t size) {
        _impl->max_buffer_size_in_bytes = size;
    }
    // Resolves with a pointer to the next fragment in the stream without consuming it from the stream,
    // or nullptr if there are no more fragments.
    // The returned pointer is invalidated by any other non-const call to this object.
    future<mutation_fragment*> peek() {
        if (!is_buffer_empty()) {
            return make_ready_future<mutation_fragment*>(&_impl->_buffer.front());
        }
        if (is_end_of_stream()) {
            return make_ready_future<mutation_fragment*>(nullptr);
        }
        return fill_buffer().then([this] {
            return peek();
        });
    }
    // A peek at the next fragment in the buffer.
    // Cannot be called if is_buffer_empty() returns true.
    const mutation_fragment& peek_buffer() const { return _impl->_buffer.front(); }
    // The actual buffer size of the reader.
    // Altough we consistently refer to this as buffer size throught the code
    // we really use "buffer size" as the size of the collective memory
    // used by all the mutation fragments stored in the buffer of the reader.
    size_t buffer_size() const {
        return _impl->buffer_size();
    }
    const tracked_buffer& buffer() const {
        return _impl->buffer();
    }
    // Detach the internal buffer of the reader.
    // Roughly equivalent to depleting it by calling pop_mutation_fragment()
    // until is_buffer_empty() returns true.
    // The reader will need to allocate a new buffer on the next fill_buffer()
    // call.
    tracked_buffer detach_buffer() noexcept {
        return _impl->detach_buffer();
    }
    // Moves the buffer content to `other`.
    //
    // If the buffer of `other` is empty this is very efficient as the buffers
    // are simply swapped. Otherwise the content of the buffer is moved
    // fragmuent-by-fragment.
    // Allows efficient implementation of wrapping readers that do no
    // transformation to the fragment stream.
    void move_buffer_content_to(impl& other) {
        _impl->move_buffer_content_to(other);
    }

    // Causes this reader to conform to s.
    // Multiple calls of upgrade_schema() compose, effects of prior calls on the stream are preserved.
    void upgrade_schema(const schema_ptr& s) {
        if (__builtin_expect(s != schema(), false)) {
            do_upgrade_schema(s);
        }
    }
};

using flat_mutation_reader_opt = optimized_optional<flat_mutation_reader>;

template<typename Impl, typename... Args>
flat_mutation_reader make_flat_mutation_reader(Args &&... args) {
    return flat_mutation_reader(std::make_unique<Impl>(std::forward<Args>(args)...));
}

namespace mutation_reader {
    // mutation_reader::forwarding determines whether fast_forward_to() may
    // be used on the mutation reader to change the partition range being
    // read. Enabling forwarding also changes read policy: forwarding::no
    // means we will stop reading from disk at the end of the given range,
    // but with forwarding::yes we may read ahead, anticipating the user to
    // make a small skip with fast_forward_to() and continuing to read.
    //
    // Note that mutation_reader::forwarding is similarly name but different
    // from streamed_mutation::forwarding - the former is about skipping to
    // a different partition range, while the latter is about skipping
    // inside a large partition.
    using forwarding = flat_mutation_reader::partition_range_forwarding;
}

// Consumes mutation fragments until StopCondition is true.
// The consumer will stop iff StopCondition returns true, in particular
// reaching the end of stream alone won't stop the reader.
template<typename StopCondition, typename ConsumeMutationFragment, typename ConsumeEndOfStream>
requires requires(StopCondition stop, ConsumeMutationFragment consume_mf, ConsumeEndOfStream consume_eos, mutation_fragment mf) {
    { stop() } -> std::same_as<bool>;
    { consume_mf(std::move(mf)) } -> std::same_as<void>;
    { consume_eos() } -> std::same_as<future<>>;
}
future<> consume_mutation_fragments_until(
        flat_mutation_reader& r,
        StopCondition&& stop,
        ConsumeMutationFragment&& consume_mf,
        ConsumeEndOfStream&& consume_eos)
{
    return do_until([stop] { return stop(); }, [&r, stop, consume_mf, consume_eos] {
        while (!r.is_buffer_empty()) {
            consume_mf(r.pop_mutation_fragment());
            if (stop() || need_preempt()) {
                return make_ready_future<>();
            }
        }
        if (r.is_end_of_stream()) {
            return consume_eos();
        }
        return r.fill_buffer();
    });
}

// Creates a stream which is like r but with transformation applied to the elements.
template<typename T>
requires StreamedMutationTranformer<T>
flat_mutation_reader transform(flat_mutation_reader r, T t) {
    class transforming_reader : public flat_mutation_reader::impl {
        flat_mutation_reader _reader;
        T _t;
        struct consumer {
            transforming_reader* _owner;
            stop_iteration operator()(mutation_fragment&& mf) {
                _owner->push_mutation_fragment(_owner->_t(std::move(mf)));
                return stop_iteration(_owner->is_buffer_full());
            }
        };
    public:
        transforming_reader(flat_mutation_reader&& r, T&& t)
            : impl(t(r.schema()), r.permit())
            , _reader(std::move(r))
            , _t(std::move(t))
        {}
        virtual future<> fill_buffer() override {
            if (_end_of_stream) {
                return make_ready_future<>();
            }
            return _reader.consume_pausable(consumer{this}).then([this] {
                if (_reader.is_end_of_stream() && _reader.is_buffer_empty()) {
                    _end_of_stream = true;
                }
            });
        }
        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (is_buffer_empty()) {
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
            forward_buffer_to(pr.start());
            _end_of_stream = false;
            return _reader.fast_forward_to(std::move(pr));
        }
        virtual future<> close() noexcept override {
            return _reader.close();
        }
    };
    return make_flat_mutation_reader<transforming_reader>(std::move(r), std::move(t));
}

class delegating_reader : public flat_mutation_reader::impl {
    flat_mutation_reader_opt _underlying_holder;
    flat_mutation_reader* _underlying;
public:
    // when passed a lvalue reference to the reader
    // we don't own it and the caller is responsible
    // for evenetually closing the reader.
    delegating_reader(flat_mutation_reader& r)
        : impl(r.schema(), r.permit())
        , _underlying_holder()
        , _underlying(&r)
    { }
    // when passed a rvalue reference to the reader
    // we assume ownership of it and will close it
    // in close().
    delegating_reader(flat_mutation_reader&& r)
        : impl(r.schema(), r.permit())
        , _underlying_holder(std::move(r))
        , _underlying(&*_underlying_holder)
    { }
    virtual future<> fill_buffer() override {
        if (is_buffer_full()) {
            return make_ready_future<>();
        }
        return _underlying->fill_buffer().then([this] {
            _end_of_stream = _underlying->is_end_of_stream();
            _underlying->move_buffer_content_to(*this);
        });
    }
    virtual future<> fast_forward_to(position_range pr) override {
        _end_of_stream = false;
        forward_buffer_to(pr.start());
        return _underlying->fast_forward_to(std::move(pr));
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        auto maybe_next_partition = make_ready_future<>();
        if (is_buffer_empty()) {
            maybe_next_partition = _underlying->next_partition();
        }
      return maybe_next_partition.then([this] {
        _end_of_stream = _underlying->is_end_of_stream() && _underlying->is_buffer_empty();
      });
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        _end_of_stream = false;
        clear_buffer();
        return _underlying->fast_forward_to(pr);
    }
    virtual future<> close() noexcept override {
        return _underlying_holder ? _underlying_holder->close() : make_ready_future<>();
    }
};
flat_mutation_reader make_delegating_reader(flat_mutation_reader&);

flat_mutation_reader make_forwardable(flat_mutation_reader m);

flat_mutation_reader make_nonforwardable(flat_mutation_reader, bool);

flat_mutation_reader make_empty_flat_reader(schema_ptr s, reader_permit permit);

// Mutation vector cannot be empty, all mutations should have the same schema.
flat_mutation_reader flat_mutation_reader_from_mutations(reader_permit permit, std::vector<mutation>,
        const dht::partition_range& pr = query::full_partition_range, streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

// Mutation vector cannot be empty, all mutations should have the same schema.
inline flat_mutation_reader flat_mutation_reader_from_mutations(reader_permit permit, std::vector<mutation> ms, streamed_mutation::forwarding fwd) {
    return flat_mutation_reader_from_mutations(std::move(permit), std::move(ms), query::full_partition_range, fwd);
}

// Mutation vector cannot be empty, all mutations should have the same schema.
flat_mutation_reader
flat_mutation_reader_from_mutations(reader_permit permit,
                                    std::vector<mutation> ms,
                                    const query::partition_slice& slice,
                                    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

// Mutation vector cannot be empty, all mutations should have the same schema.
flat_mutation_reader
flat_mutation_reader_from_mutations(reader_permit permit,
                                    std::vector<mutation> ms,
                                    const dht::partition_range& pr,
                                    const query::partition_slice& slice,
                                    streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

/// Make a reader that enables the wrapped reader to work with multiple ranges.
///
/// \param ranges An range vector that has to contain strictly monotonic
///     partition ranges, such that successively calling
///     `flat_mutation_reader::fast_forward_to()` with each one is valid.
///     An range vector range with 0 or 1 elements is also valid.
/// \param fwd_mr It is only respected when `ranges` contains 0 or 1 partition
///     ranges. Otherwise the reader is created with
///     mutation_reader::forwarding::yes.
flat_mutation_reader
make_flat_multi_range_reader(schema_ptr s, reader_permit permit, mutation_source source, const dht::partition_range_vector& ranges,
                             const query::partition_slice& slice, const io_priority_class& pc = default_priority_class(),
                             tracing::trace_state_ptr trace_state = nullptr,
                             flat_mutation_reader::partition_range_forwarding fwd_mr = flat_mutation_reader::partition_range_forwarding::yes);

/// Make a reader that enables the wrapped reader to work with multiple ranges.
///
/// Generator overload. The ranges returned by the generator have to satisfy the
/// same requirements as the `ranges` param of the vector overload.
flat_mutation_reader
make_flat_multi_range_reader(
        schema_ptr s,
        reader_permit permit,
        mutation_source source,
        std::function<std::optional<dht::partition_range>()> generator,
        const query::partition_slice& slice,
        const io_priority_class& pc = default_priority_class(),
        tracing::trace_state_ptr trace_state = nullptr,
        flat_mutation_reader::partition_range_forwarding fwd_mr = flat_mutation_reader::partition_range_forwarding::yes);

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr, reader_permit, std::deque<mutation_fragment>);

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr, reader_permit, std::deque<mutation_fragment>, const dht::partition_range& pr);

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr, reader_permit, std::deque<mutation_fragment>, const dht::partition_range& pr, const query::partition_slice& slice);

// Calls the consumer for each element of the reader's stream until end of stream
// is reached or the consumer requests iteration to stop by returning stop_iteration::yes.
// The consumer should accept mutation as the argument and return stop_iteration.
// The returned future<> resolves when consumption ends.
template <typename Consumer>
inline
future<> consume_partitions(flat_mutation_reader& reader, Consumer consumer) {
    static_assert(std::is_same<future<stop_iteration>, futurize_t<std::result_of_t<Consumer(mutation&&)>>>::value, "bad Consumer signature");

    return do_with(std::move(consumer), [&reader] (Consumer& c) -> future<> {
        return repeat([&reader, &c] () {
            return read_mutation_from_flat_mutation_reader(reader).then([&c] (mutation_opt&& mo) -> future<stop_iteration> {
                if (!mo) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return futurize_invoke(c, std::move(*mo));
            });
        });
    });
}

flat_mutation_reader
make_generating_reader(schema_ptr s, reader_permit permit, std::function<future<mutation_fragment_opt> ()> get_next_fragment);

/// A reader that emits partitions in native reverse order.
///
/// 1. The reader's schema() method will return a reversed schema (see
///    \ref schema::make_reversed()).
/// 2. Static row is still emitted first.
/// 3. Range tombstones' bounds are reversed (see \ref range_tombstone::reverse()).
/// 4. Clustered rows and range tombstones are emitted in descending order.
/// Because of 3 and 4 the guarantee that a range tombstone is emitted before
/// any mutation fragment affected by it still holds.
/// Ordering of partitions themselves remains unchanged.
/// For more details see docs/design-notes/reverse-reads.md.
///
/// \param original the reader to be reversed
/// \param max_size the maximum amount of memory the reader is allowed to use
///     for reversing and conversely the maximum size of the results. The
///     reverse reader reads entire partitions into memory, before reversing
///     them. Since partitions can be larger than the available memory, we need
///     to enforce a limit on memory consumption. When reaching the soft limit
///     a warning will be logged. When reaching the hard limit the read will be
///     aborted.
///
/// FIXME: reversing should be done in the sstable layer, see #1413.
flat_mutation_reader
make_reversing_reader(flat_mutation_reader original, query::max_result_size max_size);

/// A cosumer function that is passed a flat_mutation_reader to be consumed from
/// and returns a future<> resolved when the reader is fully consumed, and closed.
/// Note: the function assumes ownership of the reader and must close it in all cases.
using reader_consumer = noncopyable_function<future<> (flat_mutation_reader)>;
