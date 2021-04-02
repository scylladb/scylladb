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

#include <stdexcept>
#include "repair/reader.hh"
#include "database.hh"
#include "service/priority_manager.hh"

repair_reader::repair_reader(
    seastar::sharded<database>& db,
    column_family& cf,
    schema_ptr s,
    reader_permit permit,
    dht::token_range range,
    const dht::sharder& remote_sharder,
    unsigned remote_shard,
    uint64_t seed,
    is_local_reader local_reader)
    : _schema(s)
    , _permit(std::move(permit))
    , _range(dht::to_partition_range(range))
    , _sharder(remote_sharder, range, remote_shard)
    , _seed(seed)
    , _local_read_op(local_reader ? std::optional(cf.read_in_progress()) : std::nullopt)
    , _reader(nullptr) {
    if (local_reader) {
        auto ms = mutation_source([&cf] (
            schema_ptr s,
            reader_permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            const io_priority_class& pc,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding,
            mutation_reader::forwarding fwd_mr) {
            return cf.make_streaming_reader(std::move(s), pr, ps, fwd_mr);
        });
        std::tie(_reader, _reader_handle) = make_manually_paused_evictable_reader(
            std::move(ms),
            _schema,
            _permit,
            _range,
            _schema->full_slice(),
            service::get_local_streaming_priority(),
            {},
            mutation_reader::forwarding::no);
    } else {
        _reader = make_multishard_streaming_reader(db, _schema, [this] {
            auto shard_range = _sharder.next();
            if (shard_range) {
                return std::optional<dht::partition_range>(dht::to_partition_range(*shard_range));
            }
            return std::optional<dht::partition_range>();
        });
    }
}

future<mutation_fragment_opt>
repair_reader::read_mutation_fragment() {
    ++_reads_issued;
    return _reader(db::no_timeout).then([this] (mutation_fragment_opt mfopt) {
        ++_reads_finished;
        return mfopt;
    });
}

void repair_reader::on_end_of_stream() {
    _reader = make_empty_flat_reader(_schema, _permit);
    _reader_handle.reset();
}

void repair_reader::set_current_dk(const dht::decorated_key& key) {
    _current_dk = make_lw_shared<const decorated_key_with_hash>(*_schema, key, _seed);
}

void repair_reader::clear_current_dk() {
    _current_dk = {};
}

void repair_reader::check_current_dk() {
    if (!_current_dk) {
        throw std::runtime_error("Current partition_key is unknown");
    }
}

void repair_reader::pause() {
    if (_reader_handle) {
        _reader_handle->pause();
    }
}
