/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

utils::UUID
frozen_mutation::column_family_id() const {
    auto in = ser::as_input_stream(_bytes);
    auto mv = ser::deserialize(in, boost::type<ser::mutation_view>());
    return mv.table_id();
}

utils::UUID
frozen_mutation::schema_version() const {
    auto in = ser::as_input_stream(_bytes);
    auto mv = ser::deserialize(in, boost::type<ser::mutation_view>());
    return mv.schema_version();
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
    auto in = ser::as_input_stream(_bytes);
    auto mv = ser::deserialize(in, boost::type<ser::mutation_view>());
    return mv.key();
}

frozen_mutation::frozen_mutation(bytes&& b)
    : _bytes(std::move(b))
    , _pk(deserialize_key())
{ }

frozen_mutation::frozen_mutation(const mutation& m)
    : _pk(m.key())
{
    mutation_partition_serializer part_ser(*m.schema(), m.partition());

    bytes_ostream out;
    ser::writer_of_mutation wom(out);
    std::move(wom).write_table_id(m.schema()->id())
                  .write_schema_version(m.schema()->version())
                  .write_key(m.key())
                  .partition([&] (auto wr) {
                      part_ser.write(std::move(wr));
                  }).end_mutation();

    auto bv = out.linearize();
    _bytes = bytes(bv.begin(), bv.end()); // FIXME: avoid copy
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
    auto in = ser::as_input_stream(_bytes);
    auto mv = ser::deserialize(in, boost::type<ser::mutation_view>());
    return mutation_partition_view::from_view(mv.partition());
}

std::ostream& operator<<(std::ostream& out, const frozen_mutation::printer& pr) {
    return out << pr.self.unfreeze(pr.schema);
}

frozen_mutation::printer frozen_mutation::pretty_printer(schema_ptr s) const {
    return { *this, std::move(s) };
}
