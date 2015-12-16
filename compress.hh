/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
    std::experimental::optional<int> _chunk_length;
    std::experimental::optional<double> _crc_check_chance;
public:
    compression_parameters() = default;
    compression_parameters(compressor c) : _compressor(c) { }
    compression_parameters(const std::map<sstring, sstring>& options) {
        validate_options(options);

        auto it = options.find(SSTABLE_COMPRESSION);
        if (it == options.end() || it->second.empty()) {
            return;
        }
        const auto& compressor_class = it->second;
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
    int32_t chunk_length() const { return _chunk_length.value_or(int(DEFAULT_CHUNK_LENGTH)); }
    double crc_check_chance() const { return _crc_check_chance.value_or(double(DEFAULT_CRC_CHECK_CHANCE)); }

    void validate() {
        if (_chunk_length) {
            auto chunk_length = _chunk_length.value();
            if (chunk_length <= 0) {
                throw exceptions::configuration_exception(sstring("Invalid negative or null ") + CHUNK_LENGTH_KB);
            }
            // _chunk_length must be a power of two
            if (chunk_length & (chunk_length - 1)) {
                throw exceptions::configuration_exception(sstring(CHUNK_LENGTH_KB) + " must be a power of 2.");
            }
        }
        if (_crc_check_chance && (_crc_check_chance.value() < 0.0 || _crc_check_chance.value() > 1.0)) {
            throw exceptions::configuration_exception(sstring(CRC_CHECK_CHANCE) + " must be between 0.0 and 1.0.");
        }
    }

    std::map<sstring, sstring> get_options() const {
        if (_compressor == compressor::none) {
            return std::map<sstring, sstring>();
        }
        std::map<sstring, sstring> opts;
        opts.emplace(sstring(SSTABLE_COMPRESSION), compressor_name());
        if (_chunk_length) {
            opts.emplace(sstring(CHUNK_LENGTH_KB), std::to_string(_chunk_length.value() / 1024));
        }
        if (_crc_check_chance) {
            opts.emplace(sstring(CRC_CHECK_CHANCE), std::to_string(_crc_check_chance.value()));
        }
        return opts;
    }
    bool operator==(const compression_parameters& other) const {
        return _compressor == other._compressor
               && _chunk_length == other._chunk_length
               && _crc_check_chance == other._crc_check_chance;
    }
    bool operator!=(const compression_parameters& other) const {
        return !(*this == other);
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
    sstring compressor_name() const {
        switch (_compressor) {
        case compressor::lz4:
             return "org.apache.cassandra.io.compress.LZ4Compressor";
        case compressor::snappy:
            return "org.apache.cassandra.io.compress.SnappyCompressor";
        case compressor::deflate:
            return "org.apache.cassandra.io.compress.DeflateCompressor";
        default:
            abort();
        }
    }
};
