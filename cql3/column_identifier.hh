/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "cql3/selection/selectable.hh"

#include "schema.hh"

#include <algorithm>
#include <functional>
#include <iosfwd>

namespace cql3 {

/**
 * Represents an identifer for a CQL column definition.
 * TODO : should support light-weight mode without text representation for when not interned
 */
class column_identifier final : public selection::selectable {
public:
    bytes bytes_;
private:
    sstring _text;
public:
    // less comparator sorting by text
    struct text_comparator {
        bool operator()(const column_identifier& c1, const column_identifier& c2) const;
    };

    column_identifier(sstring raw_text, bool keep_case);

    column_identifier(bytes bytes_, data_type type);

    column_identifier(bytes bytes_, sstring text);

    bool operator==(const column_identifier& other) const;

    const sstring& text() const;

    const bytes& name() const;

    sstring to_string() const;

    sstring to_cql_string() const;

    friend std::ostream& operator<<(std::ostream& out, const column_identifier& i) {
        return out << i._text;
    }

#if 0
    public ColumnIdentifier clone(AbstractAllocator allocator)
    {
        return new ColumnIdentifier(allocator.clone(bytes), text);
    }
#endif

    virtual ::shared_ptr<selection::selector::factory> new_selector_factory(database& db, schema_ptr schema,
        std::vector<const column_definition*>& defs) override;

    class raw;
};

/**
 * Because Thrift-created tables may have a non-text comparator, we cannot determine the proper 'key' until
 * we know the comparator. ColumnIdentifier.Raw is a placeholder that can be converted to a real ColumnIdentifier
 * once the comparator is known with prepare(). This should only be used with identifiers that are actual
 * column names. See CASSANDRA-8178 for more background.
 */
class column_identifier::raw final : public selectable::raw {
private:
    const sstring _raw_text;
    sstring _text;
public:
    raw(sstring raw_text, bool keep_case);

    virtual ::shared_ptr<selectable> prepare(const schema& s) const override;

    ::shared_ptr<column_identifier> prepare_column_identifier(const schema& s) const;

    virtual bool processes_selection() const override;

    bool operator==(const raw& other) const;

    bool operator!=(const raw& other) const;

    virtual sstring to_string() const;
    sstring to_cql_string() const;

    friend std::hash<column_identifier::raw>;
    friend std::ostream& operator<<(std::ostream& out, const column_identifier::raw& id);
};

static inline
const column_definition* get_column_definition(const schema& schema, const column_identifier& id) {
    return schema.get_column_definition(id.bytes_);
}

static inline
::shared_ptr<column_identifier> to_identifier(const column_definition& def) {
    return def.column_specification->name;
}

}

namespace std {

template<>
struct hash<cql3::column_identifier> {
    size_t operator()(const cql3::column_identifier& i) const {
        return std::hash<bytes>()(i.bytes_);
    }
};

template<>
struct hash<cql3::column_identifier::raw> {
    size_t operator()(const cql3::column_identifier::raw& r) const {
        return std::hash<sstring>()(r._text);
    }
};

}
