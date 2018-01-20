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

#include "canonical_mutation.hh"
#include "mutation.hh"
#include "mutation_partition_serializer.hh"
#include "counters.hh"
#include "converting_mutation_partition_applier.hh"
#include "hashing_partition_visitor.hh"
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

canonical_mutation::canonical_mutation(bytes data)
        : _data(std::move(data))
{ }

canonical_mutation::canonical_mutation(const mutation& m)
{
    mutation_partition_serializer part_ser(*m.schema(), m.partition());

    bytes_ostream out;
    ser::writer_of_canonical_mutation<bytes_ostream> wr(out);
    std::move(wr).write_table_id(m.schema()->id())
                 .write_schema_version(m.schema()->version())
                 .write_key(m.key())
                 .write_mapping(m.schema()->get_column_mapping())
                 .partition([&] (auto wr) {
                     part_ser.write(std::move(wr));
                 }).end_canonical_mutation();
    _data = to_bytes(out.linearize());
}

utils::UUID canonical_mutation::column_family_id() const {
    auto in = ser::as_input_stream(_data);
    auto mv = ser::deserialize(in, boost::type<ser::canonical_mutation_view>());
    return mv.table_id();
}

mutation canonical_mutation::to_mutation(schema_ptr s) const {
    auto in = ser::as_input_stream(_data);
    auto mv = ser::deserialize(in, boost::type<ser::canonical_mutation_view>());

    auto cf_id = mv.table_id();
    if (s->id() != cf_id) {
        throw std::runtime_error(sprint("Attempted to deserialize canonical_mutation of table %s with schema of table %s (%s.%s)",
                                        cf_id, s->id(), s->ks_name(), s->cf_name()));
    }

    auto version = mv.schema_version();
    auto pk = mv.key();

    mutation m(std::move(s), std::move(pk));

    if (version == m.schema()->version()) {
        auto partition_view = mutation_partition_view::from_view(mv.partition());
        m.partition().apply(*m.schema(), partition_view, *m.schema());
    } else {
        column_mapping cm = mv.mapping();
        converting_mutation_partition_applier v(cm, *m.schema(), m.partition());
        auto partition_view = mutation_partition_view::from_view(mv.partition());
        partition_view.accept(cm, v);
    }
    return m;
}
