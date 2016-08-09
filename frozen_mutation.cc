/*
 * Copyright (C) 2015 ScyllaDB
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

#include "frozen_mutation.hh"
#include "mutation_partition.hh"
#include "mutation.hh"
#include "partition_builder.hh"
#include "mutation_partition_serializer.hh"
#include "utils/UUID.hh"
#include "utils/data_input.hh"
#include "query-result-set.hh"
#include "utils/UUID.hh"
#include "serializer.hh"
#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/mutation.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"

//
// Representation layout:
//
// <mutation> ::= <column-family-id> <schema-version> <partition-key> <partition>
//

using namespace db;

ser::mutation_view frozen_mutation::mutation_view() const {
    auto in = ser::as_input_stream(_bytes);
    return ser::deserialize(in, boost::type<ser::mutation_view>());
}

utils::UUID
frozen_mutation::column_family_id() const {
    return mutation_view().table_id();
}

utils::UUID
frozen_mutation::schema_version() const {
    return mutation_view().schema_version();
}

partition_key_view
frozen_mutation::key(const schema& s) const {
    return _pk;
}

dht::decorated_key
frozen_mutation::decorated_key(const schema& s) const {
    return dht::global_partitioner().decorate_key(s, key(s));
}

partition_key frozen_mutation::deserialize_key() const {
    return mutation_view().key();
}

frozen_mutation::frozen_mutation(bytes_ostream&& b)
    : _bytes(std::move(b))
    , _pk(deserialize_key())
{ }

frozen_mutation::frozen_mutation(bytes_ostream&& b, partition_key pk)
    : _bytes(std::move(b))
    , _pk(std::move(pk))
{ }

frozen_mutation::frozen_mutation(const mutation& m)
    : _pk(m.key())
{
    mutation_partition_serializer part_ser(*m.schema(), m.partition());

    ser::writer_of_mutation wom(_bytes);
    std::move(wom).write_table_id(m.schema()->id())
                  .write_schema_version(m.schema()->version())
                  .write_key(m.key())
                  .partition([&] (auto wr) {
                      part_ser.write(std::move(wr));
                  }).end_mutation();
}

mutation
frozen_mutation::unfreeze(schema_ptr schema) const {
    mutation m(key(*schema), schema);
    partition_builder b(*schema, m.partition());
    partition().accept(*schema, b);
    return m;
}

frozen_mutation freeze(const mutation& m) {
    return { m };
}

mutation_partition_view frozen_mutation::partition() const {
    return mutation_partition_view::from_view(mutation_view().partition());
}

std::ostream& operator<<(std::ostream& out, const frozen_mutation::printer& pr) {
    return out << pr.self.unfreeze(pr.schema);
}

frozen_mutation::printer frozen_mutation::pretty_printer(schema_ptr s) const {
    return { *this, std::move(s) };
}

stop_iteration streamed_mutation_freezer::consume(tombstone pt) {
    _partition_tombstone = pt;
    return stop_iteration::no;
}

stop_iteration streamed_mutation_freezer::consume(static_row&& sr) {
    _sr = std::move(sr);
    return stop_iteration::no;
}

stop_iteration streamed_mutation_freezer::consume(clustering_row&& cr) {
    if (_reversed) {
        _crs.emplace_front(std::move(cr));
    } else {
        _crs.emplace_back(std::move(cr));
    }
    return stop_iteration::no;
}

stop_iteration streamed_mutation_freezer::consume(range_tombstone&& rt) {
    if (_reversed) {
        rt.flip();
    }
    _rts.apply(_schema, std::move(rt));
    return stop_iteration::no;
}

frozen_mutation streamed_mutation_freezer::consume_end_of_stream() {
    bytes_ostream out;
    ser::writer_of_mutation wom(out);
    std::move(wom).write_table_id(_schema.id())
                  .write_schema_version(_schema.version())
                  .write_key(_key)
                  .partition([&] (auto wr) {
                      serialize_mutation_fragments(_schema, _partition_tombstone,
                                                   std::move(_sr), std::move(_rts),
                                                   std::move(_crs), std::move(wr));
                  }).end_mutation();
    return frozen_mutation(std::move(out), std::move(_key));
}

future<frozen_mutation> freeze(streamed_mutation sm) {
    return do_with(streamed_mutation(std::move(sm)), [] (auto& sm) mutable {
        return consume(sm, streamed_mutation_freezer(*sm.schema(), sm.key()));
    });
}

class fragmenting_mutation_freezer {
    const schema& _schema;
    partition_key _key;

    tombstone _partition_tombstone;
    stdx::optional<static_row> _sr;
    std::deque<clustering_row> _crs;
    range_tombstone_list _rts;

    frozen_mutation_consumer_fn _consumer;

    bool _fragmented = false;
    size_t _dirty_size = 0;
    size_t _fragment_size;
private:
    future<> flush() {
        bytes_ostream out;
        ser::writer_of_mutation wom(out);
        std::move(wom).write_table_id(_schema.id())
                      .write_schema_version(_schema.version())
                      .write_key(_key)
                      .partition([&] (auto wr) {
                          serialize_mutation_fragments(_schema, _partition_tombstone,
                                                       std::move(_sr), std::move(_rts),
                                                       std::move(_crs), std::move(wr));
                      }).end_mutation();

        _sr = { };
        _rts.clear();
        _crs.clear();
        _dirty_size = 0;
        return _consumer(frozen_mutation(std::move(out), _key), _fragmented);
    }

    future<stop_iteration> maybe_flush() {
        if (_dirty_size >= _fragment_size) {
            _fragmented = true;
            return flush().then([] { return stop_iteration::no; });
        }
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
public:
    fragmenting_mutation_freezer(const schema& s, const partition_key& key, frozen_mutation_consumer_fn c, size_t fragment_size)
        : _schema(s), _key(key), _rts(s), _consumer(c), _fragment_size(fragment_size) { }

    void consume(tombstone pt) {
        _dirty_size += sizeof(tombstone);
        _partition_tombstone = pt;
    }

    future<stop_iteration> consume(static_row&& sr) {
        _sr = std::move(sr);
        _dirty_size += _sr->memory_usage() + sizeof(sr);
        return maybe_flush();
    }

    future<stop_iteration> consume(clustering_row&& cr) {
        _dirty_size += cr.memory_usage() + sizeof(cr);
        _crs.emplace_back(std::move(cr));
        return maybe_flush();
    }

    future<stop_iteration> consume(range_tombstone&& rt) {
        _dirty_size += rt.memory_usage() + sizeof(range_tombstone);
        _rts.apply(_schema, std::move(rt));
        return maybe_flush();
    }

    future<stop_iteration> consume_end_of_stream() {
        if (_dirty_size) {
            return flush().then([] { return stop_iteration::yes; });
        }
        return make_ready_future<stop_iteration>(stop_iteration::yes);
    }
};

future<> fragment_and_freeze(streamed_mutation sm, frozen_mutation_consumer_fn c, size_t fragment_size)
{
    fragmenting_mutation_freezer freezer(*sm.schema(), sm.key(), c, fragment_size);
    return do_with(std::move(sm), std::move(freezer), [] (auto& sm, auto& freezer) {
        freezer.consume(sm.partition_tombstone());
        return repeat([&] {
            return sm().then([&] (auto mfopt) {
                if (!mfopt) {
                    return freezer.consume_end_of_stream();
                }
                return std::move(*mfopt).consume(freezer);
            });
        });
    });
}
