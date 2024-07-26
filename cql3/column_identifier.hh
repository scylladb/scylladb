/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "schema/schema.hh"

#include <algorithm>
#include <functional>

namespace cql3 {

class column_identifier_raw;

/**
 * Represents an identifier for a CQL column definition.
 * TODO : should support light-weight mode without text representation for when not interned
 */
class column_identifier final {
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

#if 0
    public ColumnIdentifier clone(AbstractAllocator allocator)
    {
        return new ColumnIdentifier(allocator.clone(bytes), text);
    }
#endif

    using raw = column_identifier_raw;
};

/**
 * Because Thrift-created tables may have a non-text comparator, we cannot determine the proper 'key' until
 * we know the comparator. ColumnIdentifier.Raw is a placeholder that can be converted to a real ColumnIdentifier
 * once the comparator is known with prepare(). This should only be used with identifiers that are actual
 * column names. See CASSANDRA-8178 for more background.
 */
class column_identifier_raw final {
private:
    const sstring _raw_text;
    sstring _text;
public:
    column_identifier_raw(sstring raw_text, bool keep_case);

    // for selectable::with_expression::raw:
    ::shared_ptr<column_identifier> prepare(const schema& s) const;

    ::shared_ptr<column_identifier> prepare_column_identifier(const schema& s) const;

    bool operator==(const column_identifier_raw& other) const;

    const sstring& text() const;

    virtual sstring to_string() const;
    sstring to_cql_string() const;

    friend std::hash<column_identifier_raw>;
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
struct hash<cql3::column_identifier_raw> {
    size_t operator()(const cql3::column_identifier::raw& r) const {
        return std::hash<sstring>()(r._text);
    }
};

}

template <> struct fmt::formatter<cql3::column_identifier> : fmt::formatter<string_view> {
    auto format(const cql3::column_identifier& i, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", i.text());
    }
};

template <> struct fmt::formatter<cql3::column_identifier_raw> : fmt::formatter<string_view> {
    auto format(const cql3::column_identifier_raw& id, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", id.text());
    }
};
