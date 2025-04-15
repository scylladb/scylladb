#pragma once

#include "mutation_reader.hh"
#include "mutation/mutation_fragment.hh"
#include "mutation/mutation_rebuilder.hh"
#include "reader_permit.hh"
#include "mutation/range_tombstone_assembler.hh"

class mutation_fragment_v1_stream final {
    mutation_reader _reader;
    schema_ptr _schema;
    reader_permit _permit;

    range_tombstone_assembler _rt_assembler;
    std::optional<clustering_row> _row;

    friend class mutation_fragment_v2; // so it sees our consumer methods
    mutation_fragment_opt consume(static_row mf) {
        return wrap(std::move(mf));
    }
    mutation_fragment_opt consume(clustering_row mf) {
        if (_rt_assembler.needs_flush()) [[unlikely]] {
            if (auto rt_opt = _rt_assembler.flush(*_schema, position_in_partition::after_key(*_schema, mf.position()))) [[unlikely]] {
                _row = std::move(mf);
                return wrap(std::move(*rt_opt));
            }
        }
        return wrap(std::move(mf));
    }
    mutation_fragment_opt consume(range_tombstone_change mf) {
        if (auto rt_opt = _rt_assembler.consume(*_schema, std::move(mf))) {
            return wrap(std::move(*rt_opt));
        }
        return std::nullopt;
    }
    mutation_fragment_opt consume(partition_start mf) {
        _rt_assembler.reset();
        return wrap(std::move(mf));
    }
    mutation_fragment_opt consume(partition_end mf) {
        _rt_assembler.on_end_of_stream();
        return wrap(std::move(mf));
    }

    future<mutation_fragment_opt> read_from_underlying() {
        auto mfp = co_await _reader();
        if (!mfp) [[unlikely]] {
            _rt_assembler.on_end_of_stream();
            co_return std::nullopt;
        }
        auto ret = std::move(*mfp).consume(*this);
        if (!ret) [[unlikely]] {
            // swallowed a range tombstone change, have to read more
            co_return co_await read_from_underlying();
        }
        co_return std::move(ret);
    }

    template<typename Arg>
    mutation_fragment wrap(Arg arg) const {
        return {*_schema, _permit, std::move(arg)};
    }

    void reset_state() {
        _rt_assembler.reset();
        _row = std::nullopt;
    }

    future<> next_partition() {
        reset_state();
        return _reader.next_partition();
    }

public:
    explicit mutation_fragment_v1_stream(mutation_reader reader) noexcept
        : _reader(std::move(reader))
        , _schema(_reader.schema())
        , _permit(_reader.permit())
    { }

    mutation_fragment_v1_stream(const mutation_fragment_v1_stream&) = delete;
    mutation_fragment_v1_stream(mutation_fragment_v1_stream&&) = default;

    mutation_fragment_v1_stream& operator=(const mutation_fragment_v1_stream&) = delete;
    mutation_fragment_v1_stream& operator=(mutation_fragment_v1_stream&& o) = default;

    future<> close() noexcept {
        return _reader.close();
    }

    void set_timeout(db::timeout_clock::time_point timeout) noexcept {
        _permit.set_timeout(timeout);
    }

    const schema_ptr& schema() const noexcept { return _schema; }

    future<mutation_fragment_opt> operator()() {
        if (_row) [[unlikely]] {
            co_return wrap(std::move(*std::exchange(_row, std::nullopt)));
        }
        if (_reader.is_end_of_stream()) [[unlikely]] {
            co_return std::nullopt;
        }
        co_return co_await read_from_underlying();
    }

    future<bool> has_more_fragments() {
        if (_row) [[unlikely]] {
            co_return true;
        }
        if (_reader.is_end_of_stream()) [[unlikely]] {
            co_return false;
        }
        co_return bool(co_await _reader.peek());
    }

    future<> fast_forward_to(const dht::partition_range& pr) {
        reset_state();
        return _reader.fast_forward_to(pr);
    }
    future<> fast_forward_to(position_range pr) {
        reset_state();
        return _reader.fast_forward_to(std::move(pr));
    }

    void set_max_buffer_size(size_t size) {
        _reader.set_max_buffer_size(size);
    }
    future<> fill_buffer() {
        return _reader.fill_buffer();
    }

private:
    template<typename Consumer>
    struct consumer_adapter {
        mutation_fragment_v1_stream& _reader;
        std::optional<dht::decorated_key> _decorated_key;
        Consumer _consumer;
        consumer_adapter(mutation_fragment_v1_stream& reader, Consumer c)
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
            co_return stop_iteration::no;
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
                    co_return stop_iteration::yes;
                }
                co_await _reader.next_partition();
                co_return stop_iteration::no;
            }
            co_return stop_iteration::no;
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
    // This method returns whatever is returned from Consumer::consume_end_of_stream().
    auto consume(Consumer consumer) {
        return do_with(consumer_adapter<Consumer>(*this, std::move(consumer)), [this] (consumer_adapter<Consumer>& adapter) {
            return consume_pausable(std::ref(adapter)).then([&adapter] {
                return adapter._consumer.consume_end_of_stream();
            });
        });
    }

    template<typename Consumer>
    requires FlatMutationReaderConsumer<Consumer>
    // Stops when consumer returns stop_iteration::yes or end of stream is reached.
    // Next call will start from the next mutation_fragment in the stream.
    future<> consume_pausable(Consumer consumer) {
        while (true) {
            auto mfp = co_await (*this)();
            if (!mfp) {
                co_return;
            }
            if constexpr (std::is_same_v<future<stop_iteration>, decltype(consumer(wrap(false)))>) {
                if (co_await consumer(std::move(*mfp)) == stop_iteration::yes) {
                    co_return;
                }
            } else if (consumer(std::move(*mfp)) == stop_iteration::yes) {
                co_return;
            }
        }
    }
};

// Reads a single partition from the stream. Returns empty optional if there are no more partitions to be read.
inline future<mutation_opt> read_mutation_from_mutation_reader(mutation_fragment_v1_stream& s) {
    return s.consume(mutation_rebuilder(s.schema()));
}
