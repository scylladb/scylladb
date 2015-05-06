/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

static size_t
serialized_size(const mutation& m) {
    return uuid_serializer(m.schema()->id()).size()
           + partition_key_view_serializer(m.key()).size()
           + mutation_partition_serializer::size(*m.schema(), m.partition());
}

frozen_mutation::frozen_mutation(bytes&& b)
    : _bytes(std::move(b))
{ }

frozen_mutation
frozen_mutation::from_bytes(bytes b) {
    return { std::move(b) };
}

frozen_mutation::frozen_mutation(const mutation& m)
    : _bytes(bytes::initialized_later(), serialized_size(m))
{
    data_output out(_bytes);
    uuid_serializer::write(out, m.schema()->id());
    partition_key_view_serializer::write(out, m.key());
    mutation_partition_serializer::write(out, *m.schema(), m.partition());
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
