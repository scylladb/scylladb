/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cql3/column_identifier.hh"
#include "cql3/util.hh"

namespace cql3 {

column_identifier::column_identifier(sstring raw_text, bool keep_case) {
    _text = std::move(raw_text);
    if (!keep_case) {
        std::transform(_text.begin(), _text.end(), _text.begin(), ::tolower);
    }
    bytes_ = to_bytes(_text);
}

column_identifier::column_identifier(bytes bytes_, data_type type)
    : bytes_(std::move(bytes_))
    , _text(type->get_string(this->bytes_))
{ }

column_identifier::column_identifier(bytes bytes_, sstring text)
    : bytes_(std::move(bytes_))
    , _text(std::move(text))
{ }

bool column_identifier::operator==(const column_identifier& other) const {
    return bytes_ == other.bytes_;
}

const sstring& column_identifier::text() const {
    return _text;
}

const bytes& column_identifier::name() const {
    return bytes_;
}

sstring column_identifier::to_string() const {
    return _text;
}

sstring column_identifier::to_cql_string() const {
    return util::maybe_quote(_text);
}

sstring column_identifier_raw::to_cql_string() const {
    return util::maybe_quote(_text);
}

column_identifier_raw::column_identifier_raw(sstring raw_text, bool keep_case)
    : _raw_text{raw_text}
    , _text{raw_text}
{
    if (!keep_case) {
        std::transform(_text.begin(), _text.end(), _text.begin(), ::tolower);
    }
}

::shared_ptr<column_identifier> column_identifier_raw::prepare(const schema& s) const {
    return prepare_column_identifier(s);
}

::shared_ptr<column_identifier>
column_identifier_raw::prepare_column_identifier(const schema& schema) const {
    if (schema.regular_column_name_type() == utf8_type) {
        return ::make_shared<column_identifier>(_text, true);
    }

    // We have a Thrift-created table with a non-text comparator.  We need to parse column names with the comparator
    // to get the correct ByteBuffer representation.  However, this doesn't apply to key aliases, so we need to
    // make a special check for those and treat them normally.  See CASSANDRA-8178.
    auto text_bytes = to_bytes(_text);
    auto def = schema.get_column_definition(text_bytes);
    if (def) {
        return ::make_shared<column_identifier>(std::move(text_bytes), _text);
    }

    return ::make_shared<column_identifier>(schema.regular_column_name_type()->from_string(_raw_text), _text);
}

bool column_identifier_raw::operator==(const column_identifier_raw& other) const {
    return _text == other._text;
}

const sstring& column_identifier_raw::text() const {
    return _text;
}

sstring column_identifier_raw::to_string() const {
    return _text;
}

}

bool cql3::column_identifier::text_comparator::operator()(const cql3::column_identifier& c1, const cql3::column_identifier& c2) const {
    return c1.text() < c2.text();
}
