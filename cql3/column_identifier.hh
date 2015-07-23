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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "cql3/selection/selectable.hh"

#include "schema.hh"

#include <algorithm>
#include <functional>
#include <iostream>

namespace cql3 {

/**
 * Represents an identifer for a CQL column definition.
 * TODO : should support light-weight mode without text representation for when not interned
 */
class column_identifier final : public selection::selectable /* implements IMeasurableMemory*/ {
public:
    bytes bytes_;
private:
    sstring _text;
#if 0
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ColumnIdentifier("", true));
#endif
public:
    column_identifier(sstring raw_text, bool keep_case) {
        _text = std::move(raw_text);
        if (!keep_case) {
            std::transform(_text.begin(), _text.end(), _text.begin(), ::tolower);
        }
        bytes_ = to_bytes(_text);
    }

    column_identifier(bytes bytes_, data_type type)
        : bytes_(std::move(bytes_))
        , _text(type->get_string(this->bytes_))
    { }

    column_identifier(bytes bytes_, sstring text)
        : bytes_(std::move(bytes_))
        , _text(std::move(text))
    { }

    bool operator==(const column_identifier& other) const {
        return bytes_ == other.bytes_;
    }

    const sstring& text() const {
        return _text;
    }

    const bytes& name() const {
        return bytes_;
    }

    sstring to_string() const {
        return _text;
    }

    friend std::ostream& operator<<(std::ostream& out, const column_identifier& i) {
        return out << i._text;
    }

#if 0
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapOf(bytes)
             + ObjectSizes.sizeOf(text);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapExcludingData(bytes)
             + ObjectSizes.sizeOf(text);
    }

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
    raw(sstring raw_text, bool keep_case)
        : _raw_text{raw_text}
        , _text{raw_text}
    {
        if (!keep_case) {
            std::transform(_text.begin(), _text.end(), _text.begin(), ::tolower);
        }
    }

    virtual ::shared_ptr<selectable> prepare(schema_ptr s) override {
        return prepare_column_identifier(s);
    }

    ::shared_ptr<column_identifier> prepare_column_identifier(schema_ptr s);

    virtual bool processes_selection() const override {
        return false;
    }

    bool operator==(const raw& other) const {
        return _text == other._text;
    }

    bool operator!=(const raw& other) const {
        return !operator==(other);
    }

    virtual sstring to_string() const {
        return _text;
    }

    friend std::hash<column_identifier::raw>;
    friend std::ostream& operator<<(std::ostream& out, const column_identifier::raw& id);
};

static inline
const column_definition* get_column_definition(schema_ptr schema, const column_identifier& id) {
    return schema->get_column_definition(id.bytes_);
}

static inline
::shared_ptr<column_identifier> to_identifier(const column_definition& def) {
    return def.column_specification->name;
}

static inline
std::vector<::shared_ptr<column_identifier>> to_identifiers(const std::vector<const column_definition*>& defs) {
    std::vector<::shared_ptr<column_identifier>> r;
    r.reserve(defs.size());
    for (auto&& def : defs) {
        r.push_back(to_identifier(*def));
    }
    return r;
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
