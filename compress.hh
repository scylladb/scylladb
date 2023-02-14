/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <map>
#include <set>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

#include "exceptions/exceptions.hh"
#include "utils/fragment_range.hh"

template <mutable_view is_mutable_view>
class managed_bytes_basic_view;
using managed_bytes_view = managed_bytes_basic_view<mutable_view::no>;

class bytes_ostream;
class fragmented_temporary_buffer;

class compressor;

// abstract byte source input type for compress/decompress operations
// capable of representing fragmented data.
class bytes_source {
    bool _linearized;
protected:
    bytes_source(bool);
public:
    virtual ~bytes_source() = default;
    // return next available fragment. If at end, return empty view.
    virtual bytes_view next() = 0;
    // return remaining size of data (including last retured view from next())
    virtual size_t size() const = 0;
    // i.e. if next() returns non-empty > 1 times
    bool is_linearized() const {
        return _linearized;
    }
    bool empty() const {
        return size() == 0;
    }
};

class compressor {
    sstring _name;
    // keeps track of whether the linear compressor path of this
    // type already writes its uncompressed size as part of the 
    // packet (i.e. is lz4compressor which for historical reasons
    // w.r. cassandra does this). Used to avoid duplicate info
    // in faking stream/fragmented compression
    bool _writes_uncompressed_size;
public:
    compressor(sstring, bool writes_uncompressed_size = false);

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
     * Decompress a single packet, potentially scattered across multiple fragments.
     * May or may not operate without linearization depending on compressor type.
     */
    virtual size_t uncompress(bytes_source& in, bytes_ostream&) const;

    size_t uncompress(managed_bytes_view in, bytes_ostream&) const;
    size_t uncompress(const fragmented_temporary_buffer& in, bytes_ostream&) const;

    /**
     * Compresses a potentially fragmented buffer into ostream. May or may not operate without
     * linearization.
     */ 
    virtual size_t compress(bytes_source& in, bytes_ostream&) const;

    size_t compress(managed_bytes_view in, bytes_ostream&) const;
    size_t compress(const fragmented_temporary_buffer& in, bytes_ostream&) const;

    /**
     * Returns accepted option names for this compressor
     */
    virtual std::set<sstring> option_names() const;
    /**
     * Returns original options used in instantiating this compressor
     */
    virtual std::map<sstring, sstring> options() const;

    /**
     * Compressor class name.
     */
    const sstring& name() const {
        return _name;
    }

    // to cheaply bridge sstable compression options / maps
    using opt_string = std::optional<sstring>;
    using opt_getter = std::function<opt_string(const sstring&)>;
    using ptr_type = shared_ptr<compressor>;

    static ptr_type create(const sstring& name, const opt_getter&);
    static ptr_type create(const std::map<sstring, sstring>&, const sstring& class_opt);

    static thread_local const ptr_type lz4;
    static thread_local const ptr_type snappy;
    static thread_local const ptr_type deflate;

    static const sstring namespace_prefix;
};

template<typename BaseType, typename... Args>
class class_registry;

using compressor_ptr = compressor::ptr_type;
using compressor_registry = class_registry<compressor, const typename compressor::opt_getter&>;

class compression_parameters {
public:
    static constexpr int32_t DEFAULT_CHUNK_LENGTH = 4 * 1024;
    static constexpr double DEFAULT_CRC_CHECK_CHANCE = 1.0;

    static const sstring SSTABLE_COMPRESSION;
    static const sstring CHUNK_LENGTH_KB;
    static const sstring CHUNK_LENGTH_KB_ERR;
    static const sstring CRC_CHECK_CHANCE;
private:
    compressor_ptr _compressor;
    std::optional<int> _chunk_length;
    std::optional<double> _crc_check_chance;
public:
    compression_parameters();
    compression_parameters(compressor_ptr);
    compression_parameters(const std::map<sstring, sstring>& options);
    ~compression_parameters();

    compressor_ptr get_compressor() const { return _compressor; }
    int32_t chunk_length() const { return _chunk_length.value_or(int(DEFAULT_CHUNK_LENGTH)); }
    double crc_check_chance() const { return _crc_check_chance.value_or(double(DEFAULT_CRC_CHECK_CHANCE)); }

    void validate();
    std::map<sstring, sstring> get_options() const;
    bool operator==(const compression_parameters& other) const;

    static compression_parameters no_compression() {
        return compression_parameters(nullptr);
    }
private:
    void validate_options(const std::map<sstring, sstring>&);
};
