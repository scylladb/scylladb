/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mutation_writer/shard_based_splitting_writer.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <seastar/core/shared_mutex.hh>
#include "dht/i_partitioner.hh"
#include "mutation_writer/feed_writers.hh"

namespace mutation_writer {

class shard_based_splitting_mutation_writer {
    using shard_writer = bucket_writer_v2;

private:
    schema_ptr _schema;
    reader_permit _permit;
    reader_consumer_v2 _consumer;
    unsigned _current_shard;
    std::vector<std::optional<shard_writer>> _shards;

    future<> write_to_shard(mutation_fragment_v2&& mf) {
        auto& writer = *_shards[_current_shard];
        return writer.consume(std::move(mf));
    }
public:
    shard_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, reader_consumer_v2 consumer)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _shards(smp::count)
    {}

    future<> consume(partition_start&& ps) {
        _current_shard = dht::static_shard_of(*_schema, ps.key().token()); // FIXME: Use table sharder
        if (!_shards[_current_shard]) {
            _shards[_current_shard] = shard_writer(_schema, _permit, _consumer);
        }
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(ps)));
    }

    future<> consume(static_row&& sr) {
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone_change&& rt) {
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return write_to_shard(mutation_fragment_v2(*_schema, _permit, std::move(pe)));
    }

    void consume_end_of_stream() {
        for (auto& shard : _shards) {
            if (shard) {
                shard->consume_end_of_stream();
            }
        }
    }
    void abort(std::exception_ptr ep) {
        for (auto&& shard : _shards) {
            if (shard) {
                shard->abort(ep);
            }
        }
    }
    future<> close() noexcept {
        return parallel_for_each(_shards, [] (std::optional<shard_writer>& shard) {
            return shard ? shard->close() : make_ready_future<>();
        });
    }
};

future<> segregate_by_shard(mutation_reader producer, reader_consumer_v2 consumer) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(
        std::move(producer),
        shard_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(consumer)));
}
} // namespace mutation_writer
