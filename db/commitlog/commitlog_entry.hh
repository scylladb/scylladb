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

#pragma once

#include <experimental/optional>

#include "frozen_mutation.hh"
#include "schema.hh"

namespace stdx = std::experimental;

class commitlog_entry_writer {
    schema_ptr _schema;
    db::serializer<column_mapping> _column_mapping_serializer;
    const frozen_mutation& _mutation;
    bool _with_schema = true;
public:
    commitlog_entry_writer(schema_ptr s, const frozen_mutation& fm)
        : _schema(std::move(s)), _column_mapping_serializer(_schema->get_column_mapping()), _mutation(fm)
    { }

    void set_with_schema(bool value) {
        _with_schema = value;
    }
    bool with_schema() {
        return _with_schema;
    }
    schema_ptr schema() const {
        return _schema;
    }

    size_t size() const {
        size_t size = data_output::serialized_size<bool>();
        if (_with_schema) {
            size += _column_mapping_serializer.size();
        }
        size += _mutation.representation().size();
        return size;
    }

    void write(data_output& out) const {
        out.write(_with_schema);
        if (_with_schema) {
            _column_mapping_serializer.write(out);
        }
        auto bv = _mutation.representation();
        out.write(bv.begin(), bv.end());
    }
};

class commitlog_entry_reader {
    frozen_mutation _mutation;
    stdx::optional<column_mapping> _column_mapping;
public:
    commitlog_entry_reader(const temporary_buffer<char>& buffer)
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

    const stdx::optional<column_mapping>& get_column_mapping() const { return _column_mapping; }
    const frozen_mutation& mutation() const { return _mutation; }
};