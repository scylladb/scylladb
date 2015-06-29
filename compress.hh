/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "exceptions/exceptions.hh"

enum class compressor {
    none,
    lz4,
    snappy,
    deflate,
};

class compression_parameters {
public:
    static constexpr int32_t DEFAULT_CHUNK_LENGTH = 64 * 1024;
    static constexpr double DEFAULT_CRC_CHECK_CHANCE = 1.0;

    static constexpr auto SSTABLE_COMPRESSION = "sstable_compression";
    static constexpr auto CHUNK_LENGTH_KB = "chunk_length_kb";
    static constexpr auto CRC_CHECK_CHANCE = "crc_check_chance";
private:
    compressor _compressor = compressor::none;
    int _chunk_length = DEFAULT_CHUNK_LENGTH;
    double _crc_check_chance = DEFAULT_CRC_CHECK_CHANCE;
public:
    compression_parameters() = default;
    compression_parameters(compressor c) : _compressor(c) { }
    compression_parameters(const std::map<sstring, sstring>& options) {
        validate_options(options);

        const auto& compressor_class = options.at(SSTABLE_COMPRESSION);
        if (is_compressor_class(compressor_class, "LZ4Compressor")) {
            _compressor = compressor::lz4;
        } else if (is_compressor_class(compressor_class, "SnappyCompressor")) {
            _compressor = compressor::snappy;
        } else if (is_compressor_class(compressor_class, "DeflateCompressor")) {
            _compressor = compressor::deflate;
        } else {
            throw exceptions::configuration_exception(sstring("Unsupported compression class '") + compressor_class + "'.");
        }
        auto chunk_length = options.find(CHUNK_LENGTH_KB);
        if (chunk_length != options.end()) {
            try {
                _chunk_length = std::stoi(chunk_length->second) * 1024;
            } catch (const std::exception& e) {
                throw exceptions::syntax_exception(sstring("Invalid integer value ") + chunk_length->second + " for " + CHUNK_LENGTH_KB);
            }
        }
        auto crc_chance = options.find(CRC_CHECK_CHANCE);
        if (crc_chance != options.end()) {
            try {
                _crc_check_chance = std::stod(crc_chance->second);
            } catch (const std::exception& e) {
                throw exceptions::syntax_exception(sstring("Invalid double value ") + crc_chance->second + "for " + CRC_CHECK_CHANCE);
            }
        }
    }

    compressor get_compressor() const { return _compressor; }
    int32_t chunk_length() const { return _chunk_length; }
    double crc_check_chance() const { return _crc_check_chance; }

    void validate() {
        if (_chunk_length <= 0) {
            throw exceptions::configuration_exception(sstring("Invalid negative or null ") + CHUNK_LENGTH_KB);
        }
        // _chunk_length must be a power of two
        if (_chunk_length & (_chunk_length - 1)) {
            throw exceptions::configuration_exception(sstring(CHUNK_LENGTH_KB) + " must be a power of 2.");
        }
        if (_crc_check_chance < 0.0 || _crc_check_chance > 1.0) {
            throw exceptions::configuration_exception(sstring(CRC_CHECK_CHANCE) + " must be between 0.0 and 1.0.");
        }
    }
private:
    void validate_options(const std::map<sstring, sstring>& options) {
        // currently, there are no options specific to a particular compressor
        static std::set<sstring> keywords({
            sstring(SSTABLE_COMPRESSION),
            sstring(CHUNK_LENGTH_KB),
            sstring(CRC_CHECK_CHANCE),
        });
        for (auto&& opt : options) {
            if (!keywords.count(opt.first)) {
                throw exceptions::configuration_exception(sprint("Unknown compression option '%s'.", opt.first));
            }
        }
    }
    bool is_compressor_class(const sstring& value, const sstring& class_name) {
        static const sstring namespace_prefix = "org.apache.cassandra.io.compress.";
        return value == class_name || value == namespace_prefix + class_name;
    }
};
