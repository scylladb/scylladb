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

#include "db/serializer.hh"
#include "frozen_mutation.hh"
#include "partition_builder.hh"
#include "mutation_partition_serializer.hh"
#include "utils/UUID.hh"
#include "utils/data_input.hh"

//
// Representation layout:
//
// <mutation> ::= <column-family-id> <partition-key> <partition>
//

using namespace db;

utils::UUID
frozen_mutation::column_family_id() const {
    data_input in(_bytes);
    return uuid_serializer::read(in);
}

partition_key_view
frozen_mutation::key(const schema& s) const {
    data_input in(_bytes);
    uuid_serializer::skip(in);
    return partition_key_view_serializer::read(in);
}

frozen_mutation::frozen_mutation(bytes&& b)
    : _bytes(std::move(b))
{ }

frozen_mutation::frozen_mutation(const mutation& m) {
    auto&& id = m.schema()->id();
    partition_key_view key_view = m.key();

    uuid_serializer id_ser(id);
    partition_key_view_serializer key_ser(key_view);
    mutation_partition_serializer part_ser(*m.schema(), m.partition());

    bytes buf(bytes::initialized_later(), id_ser.size() + key_ser.size() + part_ser.size_without_framing());
    data_output out(buf);
    id_ser.write(out);
    key_ser.write(out);
    part_ser.write_without_framing(out);

    _bytes = std::move(buf);
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
    data_input in(_bytes);
    uuid_serializer::skip(in);
    partition_key_view_serializer::skip(in);
    return mutation_partition_view::from_bytes(in.read_view(in.avail()));
}
