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

size_t commitlog_entry_writer:size() const
{
    size_t size = data_output::serialized_size<bool>();
    if (_with_schema) {
        size += _column_mapping_serializer.size();
    }
    size += _mutation.representation().size();
    return size;
}

void commitlog_entry_writer::write(data_output& out) const
{
    out.write(_with_schema);
    if (_with_schema) {
        _column_mapping_serializer.write(out);
    }
    auto bv = _mutation.representation();
    out.write(bv.begin(), bv.end());
}

commitlog_entry_reader::commitlog_entry_reader(const temporary_buffer<char>& buffer)
    : _mutation(bytes())
{
    data_input in(buffer);
    bool has_column_mapping = in.read<bool>();
    if (has_column_mapping) {
        _column_mapping = db::serializer<::column_mapping>::read(in);
    }
    auto bv = in.read_view(in.avail());
    _mutation = frozen_mutation(bytes(bv.begin(), bv.end()));
}