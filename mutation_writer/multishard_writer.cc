/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "mutation_writer/multishard_writer.hh"
#include "mutation/mutation_fragment_v2.hh"
#include "schema/schema_registry.hh"
#include "reader_concurrency_semaphore.hh"
#include "readers/foreign.hh"
#include "readers/queue.hh"
#include <vector>
#include <seastar/core/future-util.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/smp.hh>

namespace mutation_writer {

class shard_writer {
private:
    schema_ptr _s;
    std::unique_ptr<reader_concurrency_semaphore> _semaphore;
    mutation_reader _reader;
    std::function<future<> (mutation_reader reader)> _consumer;
public:
    shard_writer(schema_ptr s,
        std::unique_ptr<reader_concurrency_semaphore> semaphore,
        mutation_reader reader,
        std::function<future<> (mutation_reader reader)> consumer);
    future<> consume();
    future<> close() noexcept;
};

// The multishard_writer class gets mutation_fragments generated from
// mutation_reader and consumes the mutation_fragments with
// multishard_writer::_consumer. If the mutation_fragment does not belong to
// the shard multishard_writer is on, it will forward the mutation_fragment to
// the correct shard. Future returned by multishard_writer() becomes
// ready when all the mutation_fragments are consumed.
class multishard_writer {
private:
    schema_ptr _s;
    const dht::sharder& _sharder;
    std::vector<foreign_ptr<std::unique_ptr<shard_writer>>> _shard_writers;
    std::vector<future<>> _pending_consumers;
    std::vector<std::optional<queue_reader_handle_v2>> _queue_reader_handles;
    dht::shard_replica_set _current_shards;
    uint64_t _consumed_partitions = 0;
    mutation_reader _producer;
    std::function<future<> (mutation_reader)> _consumer;
private:
    dht::shard_replica_set shard_for_mf(const mutation_fragment_v2& mf) {
        auto token = mf.as_partition_start().key().token();
        return _sharder.shard_for_writes(token);
    }
    future<> make_shard_writer(unsigned shard);
    future<stop_iteration> handle_mutation_fragment(mutation_fragment_v2 mf);
    future<stop_iteration> handle_end_of_stream();
    future<> consume(unsigned shard);
    future<> wait_pending_consumers();
    future<> distribute_mutation_fragments();
public:
    multishard_writer(
        schema_ptr s,
        const dht::sharder& sharder,
        mutation_reader producer,
        std::function<future<> (mutation_reader)> consumer);
    future<uint64_t> operator()();
    future<> close() noexcept;
};

shard_writer::shard_writer(schema_ptr s,
    std::unique_ptr<reader_concurrency_semaphore> semaphore,
    mutation_reader reader,
    std::function<future<> (mutation_reader reader)> consumer)
    : _s(s)
    , _semaphore(std::move(semaphore))
    , _reader(std::move(reader))
    , _consumer(std::move(consumer)) {
}

future<> shard_writer::consume() {
    return _reader.peek().then([this] (mutation_fragment_v2* mf_ptr) {
        if (mf_ptr) {
            return _consumer(std::move(_reader));
        }
        return make_ready_future<>();
    });
}

future<> shard_writer::close() noexcept {
    return _reader.close().finally([this] {
        return _semaphore->stop();
    });
}

multishard_writer::multishard_writer(
    schema_ptr s,
    const dht::sharder& sharder,
    mutation_reader producer,
    std::function<future<> (mutation_reader)> consumer)
    : _s(std::move(s))
    , _sharder(sharder)
    , _queue_reader_handles(_sharder.shard_count())
    , _producer(std::move(producer))
    , _consumer(std::move(consumer)) {
    _shard_writers.resize(_sharder.shard_count());
}

future<> multishard_writer::make_shard_writer(unsigned shard) {
    auto [reader, handle] = make_queue_reader_v2(_s, _producer.permit());
    _queue_reader_handles[shard] = std::move(handle);
    return smp::submit_to(shard, [gs = global_schema_ptr(_s),
            consumer = _consumer,
            reader = make_foreign(std::make_unique<mutation_reader>(std::move(reader)))] () mutable {
        auto s = gs.get();
        auto semaphore = std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, "shard_writer",
                reader_concurrency_semaphore::register_metrics::no);
        auto permit = semaphore->make_tracking_only_permit(s, "multishard-writer", db::no_timeout, {});
        auto this_shard_reader = make_foreign_reader(s, std::move(permit), std::move(reader));
        return make_foreign(std::make_unique<shard_writer>(gs.get(), std::move(semaphore), std::move(this_shard_reader), consumer));
    }).then([this, shard] (foreign_ptr<std::unique_ptr<shard_writer>> writer) {
        _shard_writers[shard] = std::move(writer);
        _pending_consumers.push_back(consume(shard));
    });
}

future<stop_iteration> multishard_writer::handle_mutation_fragment(mutation_fragment_v2 mf) {
    auto f = make_ready_future<>();
    if (mf.is_partition_start()) {
        _consumed_partitions++;
        auto shards = shard_for_mf(mf);
        if (shards.empty()) [[unlikely]] {
            return make_exception_future<stop_iteration>(std::runtime_error(
                    format("multishard_writer: No shards for token {} of {}.{}",
                           mf.as_partition_start().key().token(), _s->ks_name(), _s->cf_name())));
        }
        if (shards != _current_shards) {
            _current_shards = shards;
            for (auto shard : shards) {
                if (!bool(_shard_writers[shard])) {
                    f = f.then([this, shard] {
                        return make_shard_writer(shard);
                    });
                }
            }
        }
    }
    return f.then([this, mf = std::move(mf)] () mutable {
        SCYLLA_ASSERT(!_current_shards.empty());
        if (_current_shards.size() == 1) [[likely]] {
            return _queue_reader_handles[_current_shards[0]]->push(std::move(mf));
        }
        return seastar::parallel_for_each(_current_shards, [this, mf = std::move(mf)] (unsigned shard) {
            return _queue_reader_handles[shard]->push(mutation_fragment_v2(*_s, mf.permit(), mf));
        });
    }).then([] {
        return stop_iteration::no;
    });
}

future<stop_iteration> multishard_writer::handle_end_of_stream() {
    return parallel_for_each(boost::irange(0u, _sharder.shard_count()), [this] (unsigned shard) {
        if (_queue_reader_handles[shard]) {
            _queue_reader_handles[shard]->push_end_of_stream();
        }
        return make_ready_future<>();
    }).then([] {
        return stop_iteration::yes;
    });
}

future<> multishard_writer::consume(unsigned shard) {
    return smp::submit_to(shard, [writer = _shard_writers[shard].get()] () mutable {
        return writer->consume();
    }).handle_exception([this] (std::exception_ptr ep) {
        for (auto& q : _queue_reader_handles) {
            if (q) {
                q->abort(ep);
            }
        }
        return make_exception_future<>(std::move(ep));
    });
}

future<> multishard_writer::wait_pending_consumers() {
    return seastar::when_all_succeed(_pending_consumers.begin(), _pending_consumers.end());
}

future<> multishard_writer::distribute_mutation_fragments() {
    return repeat([this] () mutable {
        return _producer().then([this] (mutation_fragment_v2_opt mf_opt) mutable {
            if (mf_opt) {
                return handle_mutation_fragment(std::move(*mf_opt));
            } else {
                return handle_end_of_stream();
            }
        });
    }).handle_exception([this] (std::exception_ptr ep) {
        for (auto& q : _queue_reader_handles) {
            if (q) {
                q->abort(ep);
            }
        }
        return make_exception_future<>(std::move(ep));
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
    const dht::sharder& sharder,
    mutation_reader producer,
    std::function<future<> (mutation_reader)> consumer,
    utils::phased_barrier::operation&& op) {
    return do_with(multishard_writer(std::move(s), sharder, std::move(producer), std::move(consumer)), std::move(op), [] (multishard_writer& writer, utils::phased_barrier::operation&) {
        return writer().finally([&writer] {
            return writer.close();
        });
    });
}

future<> multishard_writer::close() noexcept {
    return _producer.close().then([this] {
        return parallel_for_each(boost::irange(size_t(0), _shard_writers.size()), [this] (auto shard) {
            if (auto w = std::move(_shard_writers[shard])) {
                return smp::submit_to(shard, [w = std::move(w)] () mutable {
                    return w->close();
                });
            }
            return make_ready_future<>();
        });
    });
}

} // namespace mutation_writer
