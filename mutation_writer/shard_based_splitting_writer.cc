/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "mutation_writer/shard_based_splitting_writer.hh"

#include <cinttypes>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <seastar/core/shared_mutex.hh>

#include "mutation_reader.hh"

namespace mutation_writer {

class shard_based_splitting_mutation_writer {
    using shard_writer = bucket_writer;

private:
    schema_ptr _schema;
    reader_permit _permit;
    reader_consumer _consumer;
    unsigned _current_shard;
    std::vector<std::optional<shard_writer>> _shards;

    future<> write_to_shard(mutation_fragment&& mf) {
        auto& writer = *_shards[_current_shard];
        return writer.consume(std::move(mf));
    }
public:
    shard_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, reader_consumer consumer)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _shards(smp::count)
    {}

    future<> consume(partition_start&& ps) {
        _current_shard = dht::shard_of(*_schema, ps.key().token());
        if (!_shards[_current_shard]) {
            _shards[_current_shard] = shard_writer(_schema, _permit, _consumer);
        }
        return write_to_shard(mutation_fragment(*_schema, _permit, std::move(ps)));
    }

    future<> consume(static_row&& sr) {
        return write_to_shard(mutation_fragment(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write_to_shard(mutation_fragment(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone&& rt) {
        return write_to_shard(mutation_fragment(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return write_to_shard(mutation_fragment(*_schema, _permit, std::move(pe)));
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

future<> segregate_by_shard(flat_mutation_reader producer, reader_consumer consumer) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(
        std::move(producer),
        shard_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(consumer)));
}
} // namespace mutation_writer
