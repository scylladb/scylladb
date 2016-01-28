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
#include "schema_mutations.hh"

template class db::serializer<frozen_schema>;

frozen_schema::frozen_schema(const schema_ptr& s)
    : _data([&s] {
        bytes_ostream out;
        db::serializer<table_schema_version>(s->version()).write(out);
        schema_mutations sm = db::schema_tables::make_table_mutations(s, api::new_timestamp());
        db::serializer<schema_mutations>(sm).write(out);
        return to_bytes(out.linearize());
    }())
{ }

schema_ptr frozen_schema::unfreeze() const {
    data_input in(_data);
    auto version = db::serializer<table_schema_version>::read(in);
    auto mutations = db::serializer<schema_mutations>::read(in);
    return db::schema_tables::create_table_from_mutations(mutations, version);
}

frozen_schema::frozen_schema(bytes b)
    : _data(std::move(b))
{ }

bytes_view frozen_schema::representation() const
{
    return _data;
}

template<>
db::serializer<frozen_schema>::serializer(const frozen_schema& v)
        : _item(v)
        , _size(db::serializer<bytes>(v._data).size())
{ }

template<>
void
db::serializer<frozen_schema>::write(output& out, const frozen_schema& v) {
    db::serializer<bytes>(v._data).write(out);
}

template<>
frozen_schema db::serializer<frozen_schema>::read(input& in) {
    return frozen_schema(db::serializer<bytes>::read(in));
}
