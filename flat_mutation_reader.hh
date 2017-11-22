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

#pragma once

#include <seastar/util/bool_class.hh>
#include <seastar/core/future.hh>

#include "dht/i_partitioner.hh"
#include "position_in_partition.hh"
#include "streamed_mutation.hh"
#include "tracing/trace_state.hh"

#include <seastar/util/gcc6-concepts.hh>

using seastar::future;

class mutation_source;

GCC6_CONCEPT(
    template<typename Consumer>
    concept bool FlatMutationReaderConsumer() {
        return requires(Consumer c, mutation_fragment mf) {
            { c(std::move(mf)) } -> stop_iteration;
        };
    }
)

GCC6_CONCEPT(
    template<typename T>
    concept bool FlattenedConsumer() {
        return StreamedMutationConsumer<T>() && requires(T obj, const dht::decorated_key& dk) {
            obj.consume_new_partition(dk);
            obj.consume_end_of_partition();
        };
    }
)

/*
 * Allows iteration on mutations using mutation_fragments.
 * It iterates over mutations one by one and for each mutation
 * it returns:
 *      1. partition_start mutation_fragment
 *      2. static_row mutation_fragment if one exists
 *      3. mutation_fragments for all clustering rows and range tombstones
 *         in clustering key order
 *      4. partition_end mutation_fragment
 * The best way to consume those mutation_fragments is to call
 * flat_mutation_reader::consume with a consumer that receives the fragments.
 */
class flat_mutation_reader final {
public:
    // Causes a stream of reversed mutations to be emitted.
    // 1. Static row is still emitted first.
    // 2. Range tombstones are ordered by their end position.
    // 3. Clustered rows and range tombstones are emitted in descending order.
    // Because of 2 and 3 the guarantee that a range tombstone is emitted before
    // any mutation fragment affected by it still holds.
    // Ordering of partitions themselves remains unchanged.
    using consume_reversed_partitions = seastar::bool_class<class consume_reversed_partitions_tag>;

    class impl {
        circular_buffer<mutation_fragment> _buffer;
        size_t _buffer_size = 0;
        bool _consume_done = false;
    protected:
        static constexpr size_t max_buffer_size_in_bytes = 8 * 1024;
        bool _end_of_stream = false;
        schema_ptr _schema;
        friend class flat_mutation_reader;
    protected:
        template<typename... Args>
        void push_mutation_fragment(Args&&... args) {
            _buffer.emplace_back(std::forward<Args>(args)...);
            _buffer_size += _buffer.back().memory_usage();
        }
        void clear_buffer() {
            _buffer.erase(_buffer.begin(), _buffer.end());
            _buffer_size = 0;
        }
        void forward_buffer_to(const position_in_partition& pos);
        void clear_buffer_to_next_partition();
    private:
        static flat_mutation_reader reverse_partitions(flat_mutation_reader::impl&);
    public:
        impl(schema_ptr s) : _schema(std::move(s)) { }
        virtual ~impl() {}
        virtual future<> fill_buffer() = 0;
        virtual void next_partition() = 0;

        bool is_end_of_stream() const { return _end_of_stream; }
        bool is_buffer_empty() const { return _buffer.empty(); }
        bool is_buffer_full() const { return _buffer_size >= max_buffer_size_in_bytes; }

        mutation_fragment pop_mutation_fragment() {
            auto mf = std::move(_buffer.front());
            _buffer.pop_front();
            _buffer_size -= mf.memory_usage();
            return mf;
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
        GCC6_CONCEPT(
            requires FlatMutationReaderConsumer<Consumer>()
        )
        // Stops when consumer returns stop_iteration::yes or end of stream is reached.
        // Next call will start from the next mutation_fragment in the stream.
        future<> consume_pausable(Consumer consumer) {
            _consume_done = false;
            return do_until([this] { return (is_end_of_stream() && is_buffer_empty()) || _consume_done; }, [this, consumer = std::move(consumer)] () mutable {
                if (is_buffer_empty()) {
                    return fill_buffer();
                }

                _consume_done = consumer(pop_mutation_fragment()) == stop_iteration::yes;

                return make_ready_future<>();
            });
        }

        template<typename Consumer>
        GCC6_CONCEPT(
            requires FlattenedConsumer<Consumer>()
        )
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
            struct consumer_adapter {
                flat_mutation_reader::impl& _reader;
                stdx::optional<dht::decorated_key> _decorated_key;
                Consumer _consumer;
                consumer_adapter(flat_mutation_reader::impl& reader, Consumer c)
                    : _reader(reader)
                    , _consumer(std::move(c))
                { }
                stop_iteration operator()(mutation_fragment&& mf) {
                    return std::move(mf).consume(*this);
                }
                stop_iteration consume(static_row&& sr) {
                    return handle_result(_consumer.consume(std::move(sr)));
                }
                stop_iteration consume(clustering_row&& cr) {
                    return handle_result(_consumer.consume(std::move(cr)));
                }
                stop_iteration consume(range_tombstone&& rt) {
                    return handle_result(_consumer.consume(std::move(rt)));
                }
                stop_iteration consume(partition_start&& ps) {
                    _decorated_key.emplace(std::move(ps.key()));
                    _consumer.consume_new_partition(*_decorated_key);
                    if (ps.partition_tombstone()) {
                        _consumer.consume(ps.partition_tombstone());
                    }
                    return stop_iteration::no;
                }
                stop_iteration consume(partition_end&& pe) {
                    return _consumer.consume_end_of_partition();
                }
            private:
                stop_iteration handle_result(stop_iteration si) {
                    if (si) {
                        if (_consumer.consume_end_of_partition()) {
                            return stop_iteration::yes;
                        }
                        _reader.next_partition();
                    }
                    return stop_iteration::no;
                }
            };
            return do_with(consumer_adapter(*this, std::move(consumer)), [this] (consumer_adapter& adapter) {
                return consume_pausable(std::ref(adapter)).then([this, &adapter] {
                    return adapter._consumer.consume_end_of_stream();
                });
            });
        }

        /*
         * fast_forward_to is forbidden on flat_mutation_reader created for a single partition.
         */
        virtual future<> fast_forward_to(const dht::partition_range&) = 0;
        virtual future<> fast_forward_to(position_range) = 0;
    };
private:
    std::unique_ptr<impl> _impl;
public:
    // Documented in mutation_reader::forwarding in mutation_reader.hh.
    class partition_range_forwarding_tag;
    using partition_range_forwarding = bool_class<partition_range_forwarding_tag>;

    flat_mutation_reader(std::unique_ptr<impl> impl) noexcept : _impl(std::move(impl)) {}

    future<mutation_fragment_opt> operator()() {
        return _impl->operator()();
    }

    template <typename Consumer>
    GCC6_CONCEPT(
        requires FlatMutationReaderConsumer<Consumer>()
    )
    auto consume_pausable(Consumer consumer) {
        return _impl->consume_pausable(std::move(consumer));
    }

    template <typename Consumer>
    GCC6_CONCEPT(
        requires FlattenedConsumer<Consumer>()
    )
    auto consume(Consumer consumer, consume_reversed_partitions reversed = consume_reversed_partitions::no) {
        if (reversed) {
            return do_with(impl::reverse_partitions(*_impl), [&] (auto& reversed_partition_stream) {
                return reversed_partition_stream._impl->consume(std::move(consumer));
            });
        }
        return _impl->consume(std::move(consumer));
    }

    void next_partition() { _impl->next_partition(); }

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
    future<> fast_forward_to(position_range cr) {
        return _impl->fast_forward_to(std::move(cr));
    }
    bool is_end_of_stream() const { return _impl->is_end_of_stream(); }
    bool is_buffer_empty() const { return _impl->is_buffer_empty(); }
    bool is_buffer_full() const { return _impl->is_buffer_full(); }
    mutation_fragment pop_mutation_fragment() { return _impl->pop_mutation_fragment(); }
    const schema_ptr& schema() const { return _impl->_schema; }
};

template<typename Impl, typename... Args>
flat_mutation_reader make_flat_mutation_reader(Args &&... args) {
    return flat_mutation_reader(std::make_unique<Impl>(std::forward<Args>(args)...));
}

class mutation_reader;

flat_mutation_reader flat_mutation_reader_from_mutation_reader(schema_ptr, mutation_reader&&, streamed_mutation::forwarding);

flat_mutation_reader make_forwardable(flat_mutation_reader m);

flat_mutation_reader make_empty_flat_reader(schema_ptr s);

flat_mutation_reader flat_mutation_reader_from_mutations(std::vector<mutation>, streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no);

flat_mutation_reader
make_flat_multi_range_reader(schema_ptr s, mutation_source source, const dht::partition_range_vector& ranges,
                             const query::partition_slice& slice, const io_priority_class& pc = default_priority_class(),
                             tracing::trace_state_ptr trace_state = nullptr, streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
                             flat_mutation_reader::partition_range_forwarding fwd_mr = flat_mutation_reader::partition_range_forwarding::yes);
