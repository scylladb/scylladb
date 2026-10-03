/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/record_value.hh"

#include "mutation/converting_mutation_partition_applier.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition_serializer.hh"
#include "mutation/mutation_partition_view.hh"
#include "partition_builder.hh"
#include "schema/schema.hh"
#include "serializer_impl.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"

namespace replica::logstor {

namespace {

// The record value is the partition the record holds, encoded as a canonical_mutation stripped
// of what the record_header already carries: the table id and the partition key. What is
// left is the version of the schema the record was written under, the column mapping of that
// schema, and the IDL-serialized mutation_partition, each written by the serializer that
// canonical_mutation uses for it.
//
//   i64  schema_version, most significant half
//   i64  schema_version, least significant half
//        column_mapping       ser::serializer<column_mapping>, from idl/mutation.idl.hh
//        mutation_partition   ser::serializer<mutation_partition>, as mutation_partition_serializer
//                             writes it
template <typename Output>
void write_record_value(Output& out, const schema& s, const mutation_partition& p) {
    ser::serializer<int64_t>::write(out, s.version().uuid().get_most_significant_bits());
    ser::serializer<int64_t>::write(out, s.version().uuid().get_least_significant_bits());
    ser::serialize(out, s.get_column_mapping());
    mutation_partition_serializer(s, p).write(ser::writer_of_mutation_partition<Output>(out));
}

// Reads a record value field by field, in the order above. Construction reads the schema
// version; the caller then either reads or skips the mapping, and reads the partition.
template <typename Input>
class record_value_reader {
    Input& _in;
    table_schema_version _schema_version;

    static table_schema_version read_schema_version(Input& in) {
        const auto msb = ser::serializer<int64_t>::read(in);
        const auto lsb = ser::serializer<int64_t>::read(in);
        return table_schema_version(utils::UUID(msb, lsb));
    }

public:
    explicit record_value_reader(Input& in)
        : _in(in)
        , _schema_version(read_schema_version(in))
    { }

    table_schema_version schema_version() const noexcept { return _schema_version; }

    column_mapping read_mapping() {
        return ser::deserialize(_in, std::type_identity<column_mapping>{});
    }

    void skip_mapping() {
        ser::skip(_in, std::type_identity<column_mapping>{});
    }

    ser::mutation_partition_view read_partition() {
        return ser::deserialize(_in, std::type_identity<ser::mutation_partition_view>{});
    }
};

} // anonymous namespace

record_value encode_record_value(const mutation& m) {
    bytes_ostream out;
    write_record_value(out, *m.schema(), m.partition());
    return record_value(std::move(out));
}

mutation decode_record_value(const record_value& v, schema_ptr s, const record_header& h) {
    if (s->id() != h.table) {
        throw std::runtime_error(format("logstor: record of table {} decoded with the schema of table {} ({}.{})",
                h.table, s->id(), s->ks_name(), s->cf_name()));
    }

    auto in = ser::as_input_stream(v.representation());
    record_value_reader reader(in);

    mutation m(s, h.key);

    if (reader.schema_version() == s->version()) {
        reader.skip_mapping();
        auto partition_view = mutation_partition_view::from_view(reader.read_partition());
        partition_builder b(*s, m.partition());
        partition_view.accept(*s, b);
    } else {
        column_mapping cm = reader.read_mapping();
        converting_mutation_partition_applier applier(cm, *s, m.partition());
        auto partition_view = mutation_partition_view::from_view(reader.read_partition());
        partition_view.accept(cm, applier);
    }
    return m;
}

mutation to_mutation(const log_record& r, schema_ptr s) {
    return decode_record_value(r.value, std::move(s), r.header);
}

} // namespace replica::logstor
