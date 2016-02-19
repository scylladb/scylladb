/*
 * Copyright 2015 ScyllaDB
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

#include "frozen_schema.hh"
#include "db/schema_tables.hh"
#include "canonical_mutation.hh"
#include "schema_mutations.hh"
#include "idl/uuid.dist.hh"
#include "idl/frozen_schema.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_schema.dist.impl.hh"

frozen_schema::frozen_schema(const schema_ptr& s)
    : _data([&s] {
        schema_mutations sm = db::schema_tables::make_table_mutations(s, api::new_timestamp());
        bytes_ostream out;
        ser::writer_of_schema wr(out);
        std::move(wr).write_version(s->version())
                     .write_mutations(sm)
                     .end_schema();
        return to_bytes(out.linearize());
    }())
{ }

schema_ptr frozen_schema::unfreeze() const {
    seastar::simple_input_stream in(reinterpret_cast<const char*>(_data.begin()), _data.size());
    auto sv = ser::deserialize(in, boost::type<ser::schema_view>());
    return db::schema_tables::create_table_from_mutations(sv.mutations(), sv.version());
}

frozen_schema::frozen_schema(bytes b)
    : _data(std::move(b))
{ }

bytes_view frozen_schema::representation() const
{
    return _data;
}
