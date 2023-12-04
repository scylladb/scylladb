/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "counters.hh"
#include "commitlog_entry.hh"
#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"

#include <seastar/core/simple-stream.hh>

template<typename Output>
void commitlog_entry_writer::serialize(Output& out) const {
    [this, wr = ser::writer_of_commitlog_entry<Output>(out)] () mutable {
        if (_with_schema) {
            return std::move(wr).write_mapping(_schema->get_column_mapping());
        } else {
            return std::move(wr).skip_mapping();
        }
    }().write_mutation(_mutation).end_commitlog_entry();
}

void commitlog_entry_writer::compute_size() {
    seastar::measuring_output_stream ms;
    serialize(ms);
    _size = ms.size();
}

void commitlog_entry_writer::write(ostream& out) const {
    serialize(out);
}

commitlog_entry_reader::commitlog_entry_reader(const fragmented_temporary_buffer& buffer)
    : _ce([&] {
    auto in = seastar::fragmented_memory_input_stream(fragmented_temporary_buffer::view(buffer).begin(), buffer.size_bytes());
    return ser::deserialize(in, boost::type<commitlog_entry>());
}())
{
}
