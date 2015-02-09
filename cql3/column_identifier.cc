/*
 * Copyright 2015 Cloudius Systems
 */

#include "cql3/column_identifier.hh"

namespace cql3 {

::shared_ptr<column_identifier>
column_identifier::raw::prepare_column_identifier(schema_ptr schema) {
    if (schema->regular_column_name_type == utf8_type) {
        return ::make_shared<column_identifier>(_text, true);
    }

    // We have a Thrift-created table with a non-text comparator.  We need to parse column names with the comparator
    // to get the correct ByteBuffer representation.  However, this doesn't apply to key aliases, so we need to
    // make a special check for those and treat them normally.  See CASSANDRA-8178.
    auto text_bytes = to_bytes(_text);
    auto def = schema->get_column_definition(text_bytes);
    if (def) {
        return ::make_shared<column_identifier>(std::move(text_bytes), _text);
    }

    return ::make_shared<column_identifier>(schema->regular_column_name_type->from_string(_raw_text), _text);
}

std::ostream& operator<<(std::ostream& out, const column_identifier::raw& id) {
    return out << id._text;
}

}
