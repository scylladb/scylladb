/*
 * Copyright 2016 ScyllaDB
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

#include "counters.hh"
#include "commitlog_entry.hh"
#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/mutation.dist.hh"
#include "idl/commitlog.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"
#include "idl/commitlog.dist.impl.hh"

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

void commitlog_entry_writer::write(typename seastar::memory_output_stream<std::vector<temporary_buffer<char>>::iterator>& out) const {
    serialize(out);
}

commitlog_entry_reader::commitlog_entry_reader(const temporary_buffer<char>& buffer)
    : _ce([&] {
    seastar::simple_input_stream in(buffer.get(), buffer.size());
    return ser::deserialize(in, boost::type<commitlog_entry>());
}())
{
}
