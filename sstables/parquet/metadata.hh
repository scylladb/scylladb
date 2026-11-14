/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The Parquet metadata structures that our writer and reader use, and their
// Thrift serialization.
//
// The structures cover a subset of parquet.thrift from the Apache
// parquet-format repository. The parsers skip the fields that they do not
// know. The writer only fills the fields that our files need.
//
// The parsers check the Thrift structure: field types, required fields, and
// lengths. They do not check whether the values make sense. That is the
// caller's job, because only the caller knows what to expect.

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include "bytes_ostream.hh"
#include "sstables/parquet/thrift.hh"
#include "utils/chunked_vector.hh"
#include "utils/small_vector.hh"
#include "seastarx.hh"

namespace sstables {
class file_writer;
}

namespace sstables::parquet {

/// The four bytes at the start and at the end of a Parquet file.
inline constexpr std::string_view magic = "PAR1";

// The enumerations below hold the values of the Thrift enums. A parser
// stores any value that it reads, including values that are not listed.

enum class physical_type : int32_t {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    int96 = 3,
    float_ = 4,
    double_ = 5,
    byte_array = 6,
    fixed_len_byte_array = 7,
};

enum class repetition_type : int32_t {
    required = 0,
    optional = 1,
    repeated = 2,
};

/// The legacy logical type annotation. Readers still use it for lists and
/// maps, so the writer sets it next to the logical type where one exists.
enum class converted_type : int32_t {
    utf8 = 0,
    map = 1,
    map_key_value = 2,
    list = 3,
    int_8 = 15,
    int_16 = 16,
};

enum class encoding : int32_t {
    plain = 0,
    plain_dictionary = 2,
    rle = 3,
    bit_packed = 4,
    delta_binary_packed = 5,
    delta_length_byte_array = 6,
    delta_byte_array = 7,
    rle_dictionary = 8,
    byte_stream_split = 9,
};

enum class compression_codec : int32_t {
    uncompressed = 0,
    snappy = 1,
    gzip = 2,
    lzo = 3,
    brotli = 4,
    lz4 = 5,
    zstd = 6,
    lz4_raw = 7,
};

enum class page_type : int32_t {
    data_page = 0,
    index_page = 1,
    dictionary_page = 2,
    data_page_v2 = 3,
};

/// The LogicalType union. Only the members that our writer may use carry
/// their parameters. A parser keeps the kind of any other member and skips
/// its parameters.
struct logical_type {
    /// The field id of the union member.
    enum class kind : int16_t {
        string = 1,
        map = 2,
        list = 3,
        integer = 10,
        uuid = 14,
    };
    kind k{};
    /// Parameters of `integer`.
    int8_t bit_width = 0;
    bool is_signed = false;

    bool operator==(const logical_type&) const = default;
};

/// An element of the schema, which is the depth-first list of the schema tree.
struct schema_element {
    /// Set for leaves only.
    std::optional<physical_type> type;
    /// The length of a fixed_len_byte_array value.
    std::optional<int32_t> type_length;
    /// Unset for the root only.
    std::optional<repetition_type> repetition;
    sstring name;
    /// Set for groups only, including the root.
    std::optional<int32_t> num_children;
    std::optional<converted_type> converted;
    std::optional<logical_type> logical;

    bool operator==(const schema_element&) const = default;
};

/// The largest key or value of key-value metadata that the reader keeps.
/// A key or a value is one contiguous sstring, so this bounds the
/// allocation. A larger key is malformed. A larger value is skipped: the
/// parser keeps its key without a value, and allocates nothing for it. The
/// writer rejects both. Data larger than this, such as a trained zstd
/// dictionary, must be split over several keys.
inline constexpr uint32_t max_key_value_size = 128 << 10;

struct key_value {
    sstring key;
    std::optional<sstring> value;

    bool operator==(const key_value&) const = default;
};

/// Column chunk statistics. Our writer records only the null count, and the
/// parser skips the other statistics.
struct statistics {
    std::optional<int64_t> null_count;

    bool operator==(const statistics&) const = default;
};

/// ColumnMetaData without `path_in_schema`, which follows from the
/// position of the chunk in its row group. See column_chunk.
struct column_metadata {
    physical_type type{};
    utils::small_vector<encoding, 3> encodings;
    compression_codec codec{};
    int64_t num_values{};
    int64_t total_uncompressed_size{};
    int64_t total_compressed_size{};
    int64_t data_page_offset{};
    std::optional<int64_t> dictionary_page_offset;
    std::optional<statistics> stats;

    bool operator==(const column_metadata&) const = default;
};

/// A column chunk's entry in the footer.
///
/// The i-th chunk of a row group belongs to the i-th leaf of the schema,
/// as Parquet requires. The writer therefore takes `path_in_schema` from
/// the schema, which saves the memory of one path per chunk while the
/// footer is built. The streaming parser passes the path that it reads to
/// the visitor, which can check it against the schema.
struct column_chunk {
    /// Deprecated in the specification and ignored by readers, but required in the Thrift.
    int64_t file_offset = 0;
    /// Optional in the Thrift only for Parquet's own encryption, which we do not support.
    column_metadata meta_data;
    std::optional<int64_t> offset_index_offset;
    std::optional<int32_t> offset_index_length;

    bool operator==(const column_chunk&) const = default;
};

struct row_group {
    utils::chunked_vector<column_chunk> columns;
    int64_t total_byte_size{};
    int64_t num_rows{};
    std::optional<int64_t> file_offset;
    std::optional<int64_t> total_compressed_size;
    std::optional<int16_t> ordinal;

    bool operator==(const row_group&) const = default;
};

/// The footer. The writer holds the whole structure in memory until the
/// end of the file. The reader streams it through file_metadata_visitor.
struct file_metadata {
    int32_t version = 2;
    utils::chunked_vector<schema_element> schema;
    int64_t num_rows = 0;
    utils::chunked_vector<row_group> row_groups;
    std::vector<key_value> key_value_metadata;
    std::optional<sstring> created_by;

    bool operator==(const file_metadata&) const = default;
};

struct data_page_header_v2 {
    int32_t num_values{};
    int32_t num_nulls{};
    int32_t num_rows{};
    encoding enc{};
    int32_t definition_levels_byte_length{};
    int32_t repetition_levels_byte_length{};
    bool is_compressed = true;

    bool operator==(const data_page_header_v2&) const = default;
};

struct dictionary_page_header {
    int32_t num_values{};
    encoding enc{};
    std::optional<bool> is_sorted;

    bool operator==(const dictionary_page_header&) const = default;
};

/// A page header. Our files hold Data Page V2 pages and dictionary pages
/// only. The parser skips the header of any other page type, and the caller
/// rejects such a page by its type.
struct page_header {
    page_type type{};
    int32_t uncompressed_page_size{};
    int32_t compressed_page_size{};
    /// The standard CRC-32 of the page bytes that follow the header.
    std::optional<int32_t> crc;
    std::optional<dictionary_page_header> dictionary_page;
    std::optional<data_page_header_v2> data_page_v2;

    bool operator==(const page_header&) const = default;
};

struct page_location {
    int64_t offset{};
    int32_t compressed_page_size{};
    int64_t first_row_index{};

    bool operator==(const page_location&) const = default;
};

struct offset_index {
    utils::chunked_vector<page_location> page_locations;

    bool operator==(const offset_index&) const = default;
};

// Serialization. Each function appends to `out`.

void write_page_header(const page_header& h, bytes_ostream& out);
void write_offset_index(const offset_index& idx, bytes_ostream& out);

/// Serializes the footer into memory.
/// Yields between row groups when it runs in a seastar thread.
bytes_ostream serialize_file_metadata(const file_metadata& md);

/// The size and the checksum of a footer that write_file_metadata() wrote.
struct footer_info {
    uint64_t size;
    /// The standard CRC-32 (crc32_utils in sstables/checksum_utils.hh, the
    /// function of Digest.crc32) of the serialized FileMetaData. It covers
    /// neither the footer length nor the magic bytes that follow the footer.
    uint32_t crc32;
};

/// Serializes the footer and appends it to `out`, a piece at a time, so the
/// serialized footer is never held in memory as a whole.
/// Must run in a seastar thread.
footer_info write_file_metadata(const file_metadata& md, sstables::file_writer& out);

/// Returns row group `index` of a footer. write_file_metadata() calls it
/// once for each index, in increasing order.
using row_group_source = noncopyable_function<row_group(size_t index)>;

/// Like the overload above, but takes the row groups from `source` instead of
/// `md.row_groups`, which must be empty. Only one row group is materialized at
/// a time, so a writer can keep its own compact record of the column chunks
/// until the end of the file.
footer_info write_file_metadata(const file_metadata& md, size_t row_group_count, row_group_source source, sstables::file_writer& out);

// Parsing. The parsers treat their input as untrusted. Each raises the
// malformed sstable error when the input does not decode.

/// Parses a page header. The page follows at `d.position()` of the input.
page_header parse_page_header(thrift::decoder& d);
offset_index parse_offset_index(thrift::decoder& d);

/// Receives the footer from parse_file_metadata(), one piece at a time.
///
/// Thrift does not fix the order of a struct's fields. The callbacks come in
/// the order of the input, which for files from common writers, including
/// ours, is: version, the schema elements, num_rows, the row groups, the key-value
/// metadata, and created_by.
class file_metadata_visitor {
public:
    virtual ~file_metadata_visitor() = default;
    virtual void on_version(int32_t version) {}
    /// Called for each schema element, in schema order.
    virtual void on_schema_element(schema_element&& e) {}
    virtual void on_num_rows(int64_t num_rows) {}
    /// Called for each column chunk of a row group, in the order of the
    /// input, before on_row_group() for the same row group.
    /// `path` is the chunk's path_in_schema, valid during the call only.
    virtual void on_column_chunk(size_t row_group_index, size_t column_index, std::span<const sstring> path, column_chunk&& c) {}
    /// Called after the column chunks of a row group. `rg.columns` is empty:
    /// the chunks went to on_column_chunk().
    virtual void on_row_group(size_t row_group_index, row_group&& rg) {}
    virtual void on_key_value(key_value&& kv) {}
    virtual void on_created_by(sstring&& created_by) {}
};

/// Parses a footer of `length` bytes from `source` and passes it to `v`.
///
/// The whole input must be a single FileMetaData struct. Bytes after the
/// struct are an error. Memory use is bounded by the largest element, such
/// as a column chunk or a key-value pair, plus what the visitor keeps.
/// Must run in a seastar thread if `source` waits for I/O. It yields after
/// each schema element and each column chunk when it runs in a seastar thread.
void parse_file_metadata(thrift::byte_source& source, uint64_t length, file_metadata_visitor& v);

/// Parses a footer of `length` bytes from `in`, which must be positioned at
/// the start of the footer. Runs the parser in a seastar thread.
future<> parse_file_metadata(input_stream<char>& in, uint64_t length, file_metadata_visitor& v);

/// A visitor that builds the whole file_metadata. It keeps everything, so
/// it is meant for tests and tools, not for the read path.
class file_metadata_builder : public file_metadata_visitor {
    file_metadata _md;
    row_group _current;
public:
    void on_version(int32_t version) override;
    void on_schema_element(schema_element&& e) override;
    void on_num_rows(int64_t num_rows) override;
    void on_column_chunk(size_t row_group_index, size_t column_index, std::span<const sstring> path, column_chunk&& c) override;
    void on_row_group(size_t row_group_index, row_group&& rg) override;
    void on_key_value(key_value&& kv) override;
    void on_created_by(sstring&& created_by) override;
    file_metadata get() &&;
};

} // namespace sstables::parquet
