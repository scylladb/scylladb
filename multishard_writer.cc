/*
 * Copyright (C) 2018 ScyllaDB
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

#include "multishard_writer.hh"
#include "mutation_reader.hh"
#include "mutation_fragment.hh"
#include "schema_registry.hh"
#include <vector>
#include <seastar/core/future-util.hh>
#include <seastar/core/queue.hh>

class queue_reader final : public flat_mutation_reader::impl {
    seastar::queue<mutation_fragment_opt>& _mq;
public:
    queue_reader(schema_ptr s, seastar::queue<mutation_fragment_opt>& mq)
        : impl(std::move(s))
        , _mq(mq) {
    }
    virtual future<> fill_buffer(db::timeout_clock::time_point) override {
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            return _mq.pop_eventually().then([this] (mutation_fragment_opt mopt) {
                if (!mopt) {
                    _end_of_stream = true;
                } else {
                    push_mutation_fragment(std::move(*mopt));
                }
            });
        });
    }
    virtual void next_partition() override {
        throw std::bad_function_call();
    }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override {
        throw std::bad_function_call();
    }
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override {
        throw std::bad_function_call();
    }
};

class shard_writer {
private:
    schema_ptr _s;
    flat_mutation_reader _reader;
    std::function<future<> (flat_mutation_reader reader)> _consumer;
public:
    shard_writer(schema_ptr s,
        flat_mutation_reader reader,
        std::function<future<> (flat_mutation_reader reader)> consumer);
    future<> consume();
};

// The multishard_writer class gets mutation_fragments generated from
// flat_mutation_reader and consumes the mutation_fragments with
// multishard_writer::_consumer. If the mutation_fragment does not belong to
// the shard multishard_writer is on, it will forward the mutation_fragment to
// the correct shard. Future returned by multishard_writer() becomes
// ready when all the mutation_fragments are consumed.
class multishard_writer {
private:
    schema_ptr _s;
    dht::i_partitioner& _partitioner;
    std::vector<foreign_ptr<std::unique_ptr<shard_writer>>> _shard_writers;
    std::vector<future<>> _pending_consumers;
    std::vector<seastar::queue<mutation_fragment_opt>> _queues;
    unsigned _current_shard = -1;
    uint64_t _consumed_partitions = 0;
    flat_mutation_reader _producer;
    std::function<future<> (flat_mutation_reader)> _consumer;
private:
    unsigned shard_for_mf(const mutation_fragment& mf) {
        return _partitioner.shard_of(mf.as_partition_start().key().token());
    }
    future<> make_shard_writer(unsigned shard);
    future<stop_iteration> handle_mutation_fragment(mutation_fragment mf);
    future<stop_iteration> handle_end_of_stream();
    future<> consume(unsigned shard);
    future<> wait_pending_consumers();
    future<> distribute_mutation_fragments();
public:
    multishard_writer(
        schema_ptr s,
        dht::i_partitioner& partitioner,
        flat_mutation_reader producer,
        std::function<future<> (flat_mutation_reader)> consumer);
    future<uint64_t> operator()();
};

shard_writer::shard_writer(schema_ptr s,
    flat_mutation_reader reader,
    std::function<future<> (flat_mutation_reader reader)> consumer)
    : _s(s)
    , _reader(std::move(reader))
    , _consumer(std::move(consumer)) {
}

future<> shard_writer::consume() {
    return _reader.peek(db::no_timeout).then([this] (mutation_fragment* mf_ptr) {
        if (mf_ptr) {
            return _consumer(std::move(_reader));
        }
        return make_ready_future<>();
    });
}

multishard_writer::multishard_writer(
    schema_ptr s,
    dht::i_partitioner& partitioner,
    flat_mutation_reader producer,
    std::function<future<> (flat_mutation_reader)> consumer)
    : _s(std::move(s))
    , _partitioner(partitioner)
    , _producer(std::move(producer))
    , _consumer(std::move(consumer)) {
    _shard_writers.resize(_partitioner.shard_count());
    _queues.reserve(_partitioner.shard_count());
    for (unsigned shard = 0; shard < _partitioner.shard_count(); shard++) {
        _queues.push_back(seastar::queue<mutation_fragment_opt>{2});
    }
}

future<> multishard_writer::make_shard_writer(unsigned shard) {
    auto this_shard_reader = make_foreign(std::make_unique<flat_mutation_reader>(make_flat_mutation_reader<queue_reader>(_s, _queues[shard])));
    return smp::submit_to(shard, [gs = global_schema_ptr(_s),
            consumer = _consumer,
            reader = std::move(this_shard_reader)] () mutable {
        auto this_shard_reader = make_foreign_reader(gs.get(), std::move(reader));
        return make_foreign(std::make_unique<shard_writer>(gs.get(), std::move(this_shard_reader), consumer));
    }).then([this, shard] (foreign_ptr<std::unique_ptr<shard_writer>> writer) {
        _shard_writers[shard] = std::move(writer);
        _pending_consumers.push_back(consume(shard));
    });
}

future<stop_iteration> multishard_writer::handle_mutation_fragment(mutation_fragment mf) {
    auto f = make_ready_future<>();
    if (mf.is_partition_start()) {
        _consumed_partitions++;
        if (unsigned shard = shard_for_mf(mf); shard != _current_shard) {
            _current_shard = shard;
            if (!bool(_shard_writers[shard])) {
                f = make_shard_writer(shard);
            }
        }
    }
    return f.then([this, mf = std::move(mf)] () mutable {
        assert(_current_shard != -1u);
        return _queues[_current_shard].push_eventually(mutation_fragment_opt(std::move(mf)));
    }).then([] {
        return stop_iteration::no;
    });
}

future<stop_iteration> multishard_writer::handle_end_of_stream() {
    return parallel_for_each(boost::irange(0u, _partitioner.shard_count()), [this] (unsigned shard) {
        if (bool(_shard_writers[shard])) {
            return _queues[shard].push_eventually(mutation_fragment_opt());
        } else {
            return make_ready_future<>();
        }
    }).then([] {
        return stop_iteration::yes;
    });
}

future<> multishard_writer::consume(unsigned shard) {
    return smp::submit_to(shard, [writer = _shard_writers[shard].get()] () mutable {
        return writer->consume();
    }).handle_exception([this] (std::exception_ptr ep) {
        for (auto& q : _queues) {
            q.abort(ep);
        }
        return make_exception_future<>(std::move(ep));
    });
}

future<> multishard_writer::wait_pending_consumers() {
    return seastar::when_all_succeed(_pending_consumers.begin(), _pending_consumers.end());
}

future<> multishard_writer::distribute_mutation_fragments() {
    return repeat([this] () mutable {
        return _producer(db::no_timeout).then([this] (mutation_fragment_opt mf_opt) mutable {
            if (mf_opt) {
                return handle_mutation_fragment(std::move(*mf_opt));
            } else {
                return handle_end_of_stream();
            }
        });
    });
}

future<uint64_t> multishard_writer::operator()() {
    return distribute_mutation_fragments().finally([this] {
        return wait_pending_consumers();
    }).then([this] {
        return _consumed_partitions;
    });
}

future<uint64_t> distribute_reader_and_consume_on_shards(schema_ptr s,
    dht::i_partitioner& partitioner,
    flat_mutation_reader producer,
    std::function<future<> (flat_mutation_reader)> consumer) {
    return do_with(multishard_writer(std::move(s), partitioner, std::move(producer), std::move(consumer)), [] (multishard_writer& writer) {
        return writer();
    });
}
