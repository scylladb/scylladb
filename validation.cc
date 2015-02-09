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

#include "validation.hh"
#include "exceptions/exceptions.hh"

namespace validation {

/**
 * Based on org.apache.cassandra.thrift.ThriftValidation#validate_key()
 */
void
validate_cql_key(schema_ptr schema, const partition_key& key) {
    if (key.empty()) {
        throw exceptions::invalid_request_exception("Key may not be empty");
    }

    // check that key can be handled by FBUtilities.writeShortByteArray
    if (key.size() > max_key_size) {
        throw exceptions::invalid_request_exception(sprint("Key length of %d is longer than maximum of %d", key.size(), max_key_size));
    }

    try {
        schema->partition_key_type->validate(key);
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.why());
    }
}

/**
 * Based on org.apache.cassandra.thrift.ThriftValidation#validateColumnFamily(java.lang.String, java.lang.String)
 */
schema_ptr
validate_column_family(database& db, const sstring& keyspace_name, const sstring& cf_name) {
    if (keyspace_name.empty()) {
        throw exceptions::invalid_request_exception("Keyspace not set");
    }

    keyspace* ks = db.find_keyspace(keyspace_name);
    if (!ks) {
        throw exceptions::keyspace_not_defined_exception(sprint("Keyspace %s does not exist", keyspace_name));
    }

    if (cf_name.empty()) {
        throw exceptions::invalid_request_exception("non-empty table is required");
    }

    auto schema = ks->find_schema(cf_name);
    if (!schema) {
        throw exceptions::invalid_request_exception(sprint("unconfigured table %s", cf_name));
    }

    return schema;
}

}
