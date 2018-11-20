/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <map>
#include <set>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include "exceptions/exceptions.hh"
#include "stdx.hh"


class compressor {
    sstring _name;
public:
    compressor(sstring);

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
    using opt_string = stdx::optional<sstring>;
    using opt_getter = std::function<opt_string(const sstring&)>;

    static shared_ptr<compressor> create(const sstring& name, const opt_getter&);
    static shared_ptr<compressor> create(const std::map<sstring, sstring>&);

    static thread_local const shared_ptr<compressor> lz4;
    static thread_local const shared_ptr<compressor> snappy;
    static thread_local const shared_ptr<compressor> deflate;

    static const sstring namespace_prefix;
};

template<typename BaseType, typename... Args>
class class_registry;

using compressor_ptr = shared_ptr<compressor>;
using compressor_registry = class_registry<compressor_ptr, const typename compressor::opt_getter&>;

class compression_parameters {
public:
    static constexpr int32_t DEFAULT_CHUNK_LENGTH = 4 * 1024;
    static constexpr double DEFAULT_CRC_CHECK_CHANCE = 1.0;

    static const sstring SSTABLE_COMPRESSION;
    static const sstring CHUNK_LENGTH_KB;
    static const sstring CRC_CHECK_CHANCE;
private:
    compressor_ptr _compressor;
    std::experimental::optional<int> _chunk_length;
    std::experimental::optional<double> _crc_check_chance;
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
    bool operator!=(const compression_parameters& other) const;

    static compression_parameters no_compression() {
        return compression_parameters(nullptr);
    }
private:
    void validate_options(const std::map<sstring, sstring>&);
};
