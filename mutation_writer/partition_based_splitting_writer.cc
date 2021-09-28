/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "mutation_writer/partition_based_splitting_writer.hh"

#include <seastar/core/coroutine.hh>

namespace mutation_writer {

class partition_based_splitting_mutation_writer {
    struct bucket {
        bucket_writer writer;
        dht::decorated_key last_key;
        size_t data_size = 0;
    };

private:
    schema_ptr _schema;
    reader_permit _permit;
    reader_consumer _consumer;
    unsigned _max_buckets;
    std::vector<bucket> _buckets;
    bucket* _current_bucket = nullptr;

    future<> write_to_bucket(mutation_fragment&& mf) {
        _current_bucket->data_size += mf.memory_usage();
        return _current_bucket->writer.consume(std::move(mf));
    }

    future<bucket*> create_bucket_for(const dht::decorated_key& key) {
        if (_buckets.size() < _max_buckets) {
            co_return &_buckets.emplace_back(bucket{bucket_writer(_schema, _permit, _consumer), key});
        }
        auto it = std::max_element(_buckets.begin(), _buckets.end(), [] (const bucket& a, const bucket& b) {
            return a.data_size < b.data_size;
        });
        it->writer.consume_end_of_stream();
        co_await it->writer.close();
        *it = bucket{bucket_writer(_schema, _permit, _consumer), key};
        co_return &*it;
    }
public:
    partition_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, reader_consumer consumer, unsigned max_buckets)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _max_buckets(max_buckets)
    {}

    future<> consume(partition_start ps) {
        if (_buckets.empty()) {
            _current_bucket = co_await create_bucket_for(ps.key());
        } else if (dht::ring_position_tri_compare(*_schema, _current_bucket->last_key, ps.key()) < 0) {
            // No need to change bucket, just update the last key.
            _current_bucket->last_key = ps.key();
        } else {
            // Find the first bucket where this partition doesn't cause
            // monotonicity violations. Prefer the buckets towards the head of the list.
            auto it = std::find_if(_buckets.begin(), _buckets.end(), [this, &ps] (const bucket& b) {
                return dht::ring_position_tri_compare(*_schema, b.last_key, ps.key()) < 0;
            });
            if (it == _buckets.end()) {
                _current_bucket = co_await create_bucket_for(ps.key());
            } else {
                _current_bucket = &*it;
                _current_bucket->last_key = ps.key();
            }
        }
        co_return co_await write_to_bucket(mutation_fragment(*_schema, _permit, std::move(ps)));
    }

    future<> consume(static_row&& sr) {
        return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone&& rt) {
        return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return write_to_bucket(mutation_fragment(*_schema, _permit, std::move(pe)));
    }

    void consume_end_of_stream() {
        for (auto& bucket : _buckets) {
            bucket.writer.consume_end_of_stream();
        }
    }
    void abort(std::exception_ptr ep) {
        for (auto& bucket : _buckets) {
            bucket.writer.abort(ep);
        }
    }
    future<> close() noexcept {
        return parallel_for_each(_buckets, [] (bucket& bucket) {
            return bucket.writer.close();
        });
    }
};

future<> segregate_by_partition(flat_mutation_reader producer, unsigned max_buckets, reader_consumer consumer) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(std::move(producer),
            partition_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(consumer), max_buckets));
}

} // namespace mutation_writer
