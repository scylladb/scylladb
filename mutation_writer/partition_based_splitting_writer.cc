/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mutation_writer/partition_based_splitting_writer.hh"
#include "mutation_writer/feed_writers.hh"
#include "mutation/mutation_rebuilder.hh"
#include "replica/memtable.hh"

#include <seastar/core/coroutine.hh>

namespace mutation_writer {

class partition_sorting_mutation_writer {
    schema_ptr _schema;
    reader_permit _permit;
    reader_consumer_v2 _consumer;
    size_t _max_memory;
    bucket_writer_v2 _bucket_writer;
    std::optional<dht::decorated_key> _last_bucket_key;
    lw_shared_ptr<replica::memtable> _memtable;
    future<> _background_memtable_flush = make_ready_future<>();
    std::optional<mutation_rebuilder_v2> _mut_builder;
    position_in_partition _last_pos;
    tombstone _current_tombstone;
    size_t _current_mut_size = 0;

private:
    void init_current_mut(const dht::decorated_key& key, tombstone tomb) {
        _current_mut_size = key.external_memory_usage();
        _mut_builder.emplace(_schema);
        _mut_builder->consume_new_partition(key);
        _mut_builder->consume(tomb);
        if (_current_tombstone) {
            _mut_builder->consume(range_tombstone_change(position_in_partition::after_key(*_schema, _last_pos), _current_tombstone));
        }
    }

    future<> flush_memtable() {
        co_await _consumer(_memtable->make_flush_reader(_schema, _permit));
        _memtable = make_lw_shared<replica::memtable>(_schema);
    }

    future<> maybe_flush_memtable() {
        if (_memtable->occupancy().total_space() + _current_mut_size > _max_memory) {
            if (_mut_builder) {
                if (_current_tombstone) {
                    _mut_builder->consume(range_tombstone_change(position_in_partition::after_key(*_schema, _last_pos), {}));
                }
                auto mut = _mut_builder->consume_end_of_stream();
                init_current_mut(mut->decorated_key(), mut->partition().partition_tombstone());
                _memtable->apply(std::move(*mut));
            }
            return flush_memtable();
        }
        return make_ready_future<>();
    }

    future<> write(mutation_fragment_v2 mf) {
        if (!_mut_builder) {
            return _bucket_writer.consume(std::move(mf));
        }
        _current_mut_size += mf.memory_usage();
        _last_pos = mf.position();
        _mut_builder->consume(std::move(mf));
        return maybe_flush_memtable();
    }

public:
    partition_sorting_mutation_writer(schema_ptr schema, reader_permit permit, reader_consumer_v2 consumer, const segregate_config& cfg)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _max_memory(cfg.max_memory)
        , _bucket_writer(_schema, _permit, _consumer)
        , _memtable(make_lw_shared<replica::memtable>(_schema))
        , _last_pos(position_in_partition::for_partition_start())
    { }

    future<> consume(partition_start ps) {
        // Fast path: in-order partitions go directly to the output sstable
        if (!_last_bucket_key || dht::ring_position_tri_compare(*_schema, *_last_bucket_key, ps.key()) < 0) {
            _last_bucket_key = ps.key();
            return _bucket_writer.consume(mutation_fragment_v2(*_schema, _permit, std::move(ps)));
        }
        init_current_mut(ps.key(), ps.partition_tombstone());
        return make_ready_future<>();
    }

    future<> consume(static_row&& sr) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone_change&& rtc) {
        _current_tombstone = rtc.tombstone();
        return write(mutation_fragment_v2(*_schema, _permit, std::move(rtc)));
    }

    future<> consume(partition_end&& pe) {
        if (!_mut_builder) {
            return _bucket_writer.consume(mutation_fragment_v2(*_schema, _permit, std::move(pe)));
        }
        _memtable->apply(*_mut_builder->consume_end_of_stream());
        _mut_builder.reset();
        _last_pos = position_in_partition::after_all_clustered_rows();
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

future<> segregate_by_partition(mutation_reader producer, segregate_config cfg, reader_consumer_v2 consumer) {
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

} // namespace mutation_writer
