/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "validation.hh"
#include "schema/schema.hh"
#include "keys.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"

namespace validation {

/**
 * Based on org.apache.cassandra.thrift.ThriftValidation#validate_key()
 */
std::optional<sstring> is_cql_key_invalid(const schema& schema, partition_key_view key) {
    // C* validates here that the thrift key is not empty.
    // It can only be empty if it is not composite and its only component in CQL form is empty.
    if (schema.partition_key_size() == 1 && key.begin(schema)->empty()) {
        return sstring("Key may not be empty");
    }

    // check that key can be handled by FBUtilities.writeShortByteArray
    auto b = key.representation();
    if (b.size() > max_key_size) {
        return format("Key length of {:d} is longer than maximum of {:d}", b.size(), max_key_size);
    }

    try {
        key.validate(schema);
    } catch (const marshal_exception& e) {
        return sstring(e.what());
    }

    return std::nullopt;
}

void
validate_cql_key(const schema& schema, partition_key_view key) {
    if (const auto err = is_cql_key_invalid(schema, key); err) {
        throw exceptions::invalid_request_exception(std::move(*err));
    }
}

/**
 * Based on org.apache.cassandra.thrift.ThriftValidation#validateColumnFamily(java.lang.String, java.lang.String)
 */
schema_ptr
validate_column_family(data_dictionary::database db, const sstring& keyspace_name, const sstring& cf_name) {
    validate_keyspace(db, keyspace_name);

    if (cf_name.empty()) {
        throw exceptions::invalid_request_exception("non-empty table is required");
    }

    auto t = db.try_find_table(keyspace_name, cf_name);
    if (!t) {
        throw exceptions::invalid_request_exception(format("unconfigured table {}", cf_name));
    }

    return t->schema();
}

void validate_keyspace(data_dictionary::database db, const sstring& keyspace_name) {
    if (keyspace_name.empty()) {
        throw exceptions::invalid_request_exception("Keyspace not set");
    }

    if (!db.has_keyspace(keyspace_name)) {
        throw exceptions::keyspace_not_defined_exception(format("Keyspace {} does not exist", keyspace_name));
    }
}

}
