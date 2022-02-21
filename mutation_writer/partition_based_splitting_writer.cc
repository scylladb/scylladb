/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mutation_writer/partition_based_splitting_writer.hh"
#include "replica/memtable.hh"

#include <seastar/core/coroutine.hh>

namespace mutation_writer {

class partition_sorting_mutation_writer {
    schema_ptr _schema;
    reader_permit _permit;
    reader_consumer _consumer;
    const io_priority_class& _pc;
    size_t _max_memory;
    bucket_writer _bucket_writer;
    std::optional<dht::decorated_key> _last_bucket_key;
    lw_shared_ptr<replica::memtable> _memtable;
    future<> _background_memtable_flush = make_ready_future<>();
    std::optional<mutation> _current_mut;
    size_t _current_mut_size = 0;

private:
    void init_current_mut(dht::decorated_key key, tombstone tomb) {
        _current_mut_size = key.external_memory_usage();
        _current_mut.emplace(_schema, std::move(key));
        _current_mut->partition().apply(tomb);
    }

    future<> flush_memtable() {
        co_await _consumer(downgrade_to_v1(_memtable->make_flush_reader(_schema, _permit, _pc)));
        _memtable = make_lw_shared<replica::memtable>(_schema);
    }

    future<> maybe_flush_memtable() {
        if (_memtable->occupancy().total_space() + _current_mut_size > _max_memory) {
            if (_current_mut) {
                _memtable->apply(*_current_mut);
                init_current_mut(_current_mut->decorated_key(), _current_mut->partition().partition_tombstone());
            }
            return flush_memtable();
        }
        return make_ready_future<>();
    }

    future<> write(mutation_fragment mf) {
        if (!_current_mut) {
            return _bucket_writer.consume(std::move(mf));
        }
        _current_mut_size += mf.memory_usage();
        _current_mut->apply(mf);
        return maybe_flush_memtable();
    }

public:
    partition_sorting_mutation_writer(schema_ptr schema, reader_permit permit, reader_consumer consumer, const segregate_config& cfg)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _pc(cfg.pc)
        , _max_memory(cfg.max_memory)
        , _bucket_writer(_schema, _permit, _consumer)
        , _memtable(make_lw_shared<replica::memtable>(_schema))
    { }

    future<> consume(partition_start ps) {
        // Fast path: in-order partitions go directly to the output sstable
        if (!_last_bucket_key || dht::ring_position_tri_compare(*_schema, *_last_bucket_key, ps.key()) < 0) {
            _last_bucket_key = ps.key();
            return _bucket_writer.consume(mutation_fragment(*_schema, _permit, std::move(ps)));
        }
        init_current_mut(ps.key(), ps.partition_tombstone());
        return make_ready_future<>();
    }

    future<> consume(static_row&& sr) {
        return write(mutation_fragment(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write(mutation_fragment(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone&& rt) {
        return write(mutation_fragment(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        if (!_current_mut) {
            return _bucket_writer.consume(mutation_fragment(*_schema, _permit, std::move(pe)));
        }
        _memtable->apply(*_current_mut);
        _current_mut.reset();
        _current_mut_size = 0;
        return maybe_flush_memtable();
    }

    void consume_end_of_stream() {
        _bucket_writer.consume_end_of_stream();
        if (!_memtable->empty()) {
            _background_memtable_flush = flush_memtable();
        }
    }
    void abort(std::exception_ptr ep) {
        _bucket_writer.abort(std::move(ep));
    }
    future<> close() noexcept {
        return when_all_succeed(_bucket_writer.close(), std::move(_background_memtable_flush)).discard_result();
    }
};

future<> segregate_by_partition(flat_mutation_reader producer, segregate_config cfg, reader_consumer consumer) {
    auto schema = producer.schema();
    auto permit = producer.permit();
  try {
    return feed_writer(std::move(producer),
            partition_sorting_mutation_writer(std::move(schema), std::move(permit), std::move(consumer), std::move(cfg)));
  } catch (...) {
    return producer.close().then([ex = std::current_exception()] () mutable {
        return make_exception_future<>(std::move(ex));
     });
  }
}

future<> segregate_by_partition(flat_mutation_reader_v2 producer, segregate_config cfg, reader_consumer_v2 consumer) {
    return segregate_by_partition(downgrade_to_v1(std::move(producer)), cfg, [consumer = std::move(consumer)] (flat_mutation_reader reader) {
        return consumer(upgrade_to_v2(std::move(reader)));
    });
}



} // namespace mutation_writer
