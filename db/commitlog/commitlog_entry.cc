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

commitlog_entry::commitlog_entry(stdx::optional<column_mapping> mapping, frozen_mutation&& mutation)
    : _mapping(std::move(mapping))
      , _mutation_storage(std::move(mutation))
      , _mutation(*_mutation_storage)
{ }

commitlog_entry::commitlog_entry(stdx::optional<column_mapping> mapping, const frozen_mutation& mutation)
    : _mapping(std::move(mapping))
      , _mutation(mutation)
{ }

commitlog_entry commitlog_entry_writer::get_entry() const {
    if (_with_schema) {
        return commitlog_entry(_schema->get_column_mapping(), _mutation);
    } else {
        return commitlog_entry({}, _mutation);
    }
}

void commitlog_entry_writer::compute_size() {
    _size = ser::get_sizeof(get_entry());
}

void commitlog_entry_writer::write(data_output& out) const {
    seastar::simple_output_stream str(out.reserve(size()));
    ser::serialize(str, get_entry());
}

commitlog_entry_reader::commitlog_entry_reader(const temporary_buffer<char>& buffer)
    : _ce([&] {
    seastar::simple_input_stream in(buffer.get(), buffer.size());
    return ser::deserialize(in, boost::type<commitlog_entry>());
}())
{
}