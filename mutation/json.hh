/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/token.hh"
#include "schema/schema.hh"
#include "utils/rjson.hh"

/*
 * Utilities for converting mutations, mutation-fragments and their parts into json.
 */

class atomic_cell_or_collection;
class atomic_cell_view;
class counter_cell_view;
class row;
class row_marker;
class tombstone;
struct collection_mutation_view_description;

namespace mutation_json {

class json_writer : public rjson::streaming_writer {
public:
    using rjson::streaming_writer::streaming_writer;

    // scylla-specific extensions (still following rapidjson naming scheme for consistency)
    template <typename T>
    void AsString(const T& obj) {
        String(fmt::format("{}", obj));
    }
    // partition or clustering key
    template <typename KeyType>
    void DataKey(const schema& schema, const KeyType& key, std::optional<dht::token> token = {}) {
        StartObject();
        if (token) {
            Key("token");
            AsString(*token);
        }
        Key("raw");
        String(to_hex(key.representation()));
        Key("value");
        AsString(key.with_schema(schema));
        EndObject();
    }
    void StartStream() {
        StartObject();
        Key("sstables");
        StartObject();
    }
    void EndStream() {
        EndObject();
        EndObject();
    }
};

class mutation_partition_json_writer {
    const schema& _schema;
    json_writer _writer;

private:
    void write_each_collection_cell(const collection_mutation_view_description& mv, data_type type, std::function<void(atomic_cell_view, data_type)> func);

public:
    explicit mutation_partition_json_writer(const schema& s, std::ostream& os = std::cout)
        : _schema(s), _writer(os) {}

    const schema& schema() const { return _schema; }
    json_writer& writer() { return _writer; }

    sstring to_string(gc_clock::time_point tp);
    void write_atomic_cell_value(const atomic_cell_view& cell, data_type type);
    void write_collection_value(const collection_mutation_view_description& mv, data_type type);
    void write(gc_clock::duration ttl, gc_clock::time_point expiry);
    void write(const tombstone& t);
    void write(const row_marker& m);
    void write(counter_cell_view cv);
    void write(const atomic_cell_view& cell, data_type type, bool include_value = true);
    void write(const collection_mutation_view_description& mv, data_type type, bool include_value = true);
    void write(const atomic_cell_or_collection& cell, const column_definition& cdef, bool include_value = true);
    void write(const row& r, column_kind kind, bool include_value = true);
};

} // namespace mutation_json
