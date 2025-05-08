/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <map>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace gms {
class feature_service;
} // namespace gms

class compression_parameters;

class compressor {
public:
    enum class algorithm {
        lz4,
        lz4_with_dicts,
        zstd,
        zstd_with_dicts,
        snappy,
        deflate,
        none,
    };

    virtual ~compressor() {}

    /**
     * Unpacks data in "input" to output. If output_len is of insufficient size,
     * exception is thrown. I.e. you should keep track of the uncompressed size.
     */
    virtual size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const = 0;
    /**
     * Packs data in "input" to output. If output_len is of insufficient size,
     * exception is thrown. Maximum required size is obtained via "compress_max_size"
     */
    virtual size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const = 0;
    /**
     * Returns the maximum output size for compressing data on "input_len" size.
     */
    virtual size_t compress_max_size(size_t input_len) const = 0;

    /**
     * Returns metadata which must be written together with the compressed
     * data and used to construct a corresponding decompressor.
     */
    virtual std::map<sstring, sstring> options() const;

    static bool is_hidden_option_name(std::string_view sv);

    std::string name() const;

    virtual algorithm get_algorithm() const = 0;

    virtual std::optional<unsigned> get_dict_owner_for_test() const;

    using ptr_type = std::unique_ptr<compressor>;
};

using compressor_ptr = compressor::ptr_type;

compressor_ptr make_lz4_sstable_compressor_for_tests();

// Per-table compression options, parsed and validated.
//
// Compression options are configured through the JSON-like `compression` entry in the schema.
// The CQL layer parses the text of that entry to a `map<string, string>`.
// A `compression_parameters` object is constructed from this map.
// and the passed keys and values are parsed and validated in the constructor.
// This object can be then used to create a `compressor` objects for sstable readers and writers.
class compression_parameters {
public:
    using algorithm = compressor::algorithm;
    static constexpr std::string_view name_prefix = "org.apache.cassandra.io.compress.";

    static constexpr int32_t DEFAULT_CHUNK_LENGTH = 4 * 1024;
    static constexpr double DEFAULT_CRC_CHECK_CHANCE = 1.0;

    static const sstring SSTABLE_COMPRESSION;
    static const sstring CHUNK_LENGTH_KB;
    static const sstring CHUNK_LENGTH_KB_ERR;
    static const sstring CRC_CHECK_CHANCE;
private:
    algorithm _algorithm;
    std::optional<int> _chunk_length;
    std::optional<double> _crc_check_chance;
    std::optional<int> _zstd_compression_level;
public:
    compression_parameters();
    compression_parameters(algorithm);
    compression_parameters(const std::map<sstring, sstring>& options);
    ~compression_parameters();

    int32_t chunk_length() const { return _chunk_length.value_or(int(DEFAULT_CHUNK_LENGTH)); }
    double crc_check_chance() const { return _crc_check_chance.value_or(double(DEFAULT_CRC_CHECK_CHANCE)); }
    algorithm get_algorithm() const { return _algorithm; }
    std::optional<int> zstd_compression_level() const { return _zstd_compression_level; }

    void validate(const gms::feature_service&);
    std::map<sstring, sstring> get_options() const;

    bool compression_enabled() const { 
        return _algorithm != algorithm::none;
    }
    static compression_parameters no_compression() {
        return compression_parameters(algorithm::none);
    }
    bool operator==(const compression_parameters&) const = default;
    static std::string_view algorithm_to_name(algorithm);
    static std::string algorithm_to_qualified_name(algorithm);
private:
    static void validate_options(const std::map<sstring, sstring>&);
    static algorithm name_to_algorithm(std::string_view name);
};
