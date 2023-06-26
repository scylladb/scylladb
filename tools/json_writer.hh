/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "dht/i_partitioner.hh"
#include "schema/schema_fwd.hh"
#include "sstables/sstables.hh"
#include "utils/rjson.hh"

// has to be below the utils/rjson.hh include
#include <rapidjson/ostreamwrapper.h>

namespace tools {

class json_writer {
    using stream = rapidjson::BasicOStreamWrapper<std::ostream>;
    using writer = rapidjson::Writer<stream, rjson::encoding, rjson::encoding, rjson::allocator>;

    stream _stream;
    writer _writer;

public:
    json_writer(std::ostream& os = std::cout) : _stream(os), _writer(_stream)
    { }

    writer& rjson_writer() { return _writer; }

    // following the rapidjson method names here
    bool Null() { return _writer.Null(); }
    bool Bool(bool b) { return _writer.Bool(b); }
    bool Int(int i) { return _writer.Int(i); }
    bool Uint(unsigned i) { return _writer.Uint(i); }
    bool Int64(int64_t i) { return _writer.Int64(i); }
    bool Uint64(uint64_t i) { return _writer.Uint64(i); }
    bool Double(double d) { return _writer.Double(d); }
    bool RawNumber(std::string_view str) { return _writer.RawNumber(str.data(), str.size(), false); }
    bool String(std::string_view str) { return _writer.String(str.data(), str.size(), false); }
    bool StartObject() { return _writer.StartObject(); }
    bool Key(std::string_view str) { return _writer.Key(str.data(), str.size(), false); }
    bool EndObject(rapidjson::SizeType memberCount = 0) { return _writer.EndObject(memberCount); }
    bool StartArray() { return _writer.StartArray(); }
    bool EndArray(rapidjson::SizeType elementCount = 0) { return _writer.EndArray(elementCount); }

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
    void SstableKey(const sstables::sstable& sst) {
        Key(sst.get_filename());
    }
    void SstableKey(const sstables::sstable* const sst) {
        if (sst) {
            SstableKey(*sst);
        } else {
            Key("anonymous");
        }
    }
};

class mutation_fragment_json_writer {
    const schema& _schema;
    json_writer _writer;
    bool _clustering_array_created;
private:
    sstring to_string(gc_clock::time_point tp);
    void write(gc_clock::duration ttl, gc_clock::time_point expiry);
    void write(const tombstone& t);
    void write(const row_marker& m);
    void write(counter_cell_view cv);
    void write(const atomic_cell_view& cell, data_type type);
    void write(const collection_mutation_view_description& mv, data_type type);
    void write(const atomic_cell_or_collection& cell, const column_definition& cdef);
    void write(const row& r, column_kind kind);
    void write(const clustering_row& cr);
    void write(const range_tombstone_change& rtc);
public:
    explicit mutation_fragment_json_writer(const schema& s, std::ostream& os = std::cout)
        : _schema(s), _writer(os) {}
    json_writer& writer() { return _writer; }
    void start_stream();
    void start_sstable(const sstables::sstable* const sst);
    void start_partition(const partition_start& ps);
    void partition_element(const static_row& sr);
    void partition_element(const clustering_row& cr);
    void partition_element(const range_tombstone_change& rtc);
    void end_partition();
    void end_sstable();
    void end_stream();
};

} // namespace tools
