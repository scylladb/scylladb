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
#include "converting_mutation_partition_applier.hh"
#include "hashing_partition_visitor.hh"

template class db::serializer<canonical_mutation>;

//
// Representation layout:
//
// <canonical_mutation> ::= <column_family_id> <table_schema_version> <partition_key> <column-mapping> <partition>
//
// For <partition> see mutation_partition_serializer.cc
// For <column-mapping> see db::serializer<column_mapping>
//

canonical_mutation::canonical_mutation(bytes data)
        : _data(std::move(data))
{ }

canonical_mutation::canonical_mutation(const mutation& m)
    : _data([&m] {
        bytes_ostream out;
        db::serializer<utils::UUID>(m.column_family_id()).write(out);
        db::serializer<table_schema_version>(m.schema()->version()).write(out);
        db::serializer<partition_key_view>(m.key()).write(out);
        db::serializer<column_mapping>(m.schema()->get_column_mapping()).write(out);
        mutation_partition_serializer ser(*m.schema(), m.partition());
        ser.write(out);
        return to_bytes(out.linearize());
    }())
{ }

mutation canonical_mutation::to_mutation(schema_ptr s) const {
    data_input in(_data);

    auto cf_id = db::serializer<utils::UUID>::read(in);
    if (s->id() != cf_id) {
        throw std::runtime_error(sprint("Attempted to deserialize canonical_mutation of table %s with schema of table %s (%s.%s)",
                                        cf_id, s->id(), s->ks_name(), s->cf_name()));
    }

    auto version = db::serializer<table_schema_version>::read(in);
    auto pk = partition_key(db::serializer<partition_key_view>::read(in));

    mutation m(std::move(pk), std::move(s));

    if (version == m.schema()->version()) {
        db::serializer<column_mapping>::skip(in);
        auto partition_view = mutation_partition_serializer::read_as_view(in);
        m.partition().apply(*m.schema(), partition_view, *m.schema());
    } else {
        column_mapping cm = db::serializer<column_mapping>::read(in);
        converting_mutation_partition_applier v(cm, *m.schema(), m.partition());
        auto partition_view = mutation_partition_serializer::read_as_view(in);
        partition_view.accept(cm, v);
    }
    return m;
}

template<>
db::serializer<canonical_mutation>::serializer(const canonical_mutation& v)
        : _item(v)
        , _size(db::serializer<bytes>(v._data).size())
{ }

template<>
void
db::serializer<canonical_mutation>::write(output& out, const canonical_mutation& v) {
    db::serializer<bytes>(v._data).write(out);
}

template<>
canonical_mutation db::serializer<canonical_mutation>::read(input& in) {
    return canonical_mutation(db::serializer<bytes>::read(in));
}
