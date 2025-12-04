/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "alternator/executor.hh"
#include <seastar/http/httpd.hh>
#include "db/config.hh"

namespace alternator {

class response_compressor {
public:
    enum class compression_type {
        gzip,
        deflate,
        compressions_count,
        any = compressions_count,
        none,
        count,
        unknown = count
    };
    static constexpr std::string_view compression_names[] = {
        "gzip",
        "deflate",
        "*",
        "identity"
    };

    static sstring get_encoding_name(compression_type ct) {
        return sstring(compression_names[static_cast<size_t>(ct)]);
    }
    static constexpr compression_type get_compression_type(std::string_view encoding);

    sstring get_accepted_encoding(const http::request& req) {
        if (get_threshold() == 0) {
            return "";
        }
        return req.get_header("Accept-Encoding");
    }
    compression_type find_compression(std::string_view accept_encoding, size_t response_size);

    response_compressor(const db::config& cfg)
        : cfg(cfg)
        ,_gzip_level_observer(
            cfg.alternator_response_gzip_compression_level.observe([this](int v) {
                    update_threshold();
                }))
        ,_gzip_threshold_observer(
            cfg.alternator_response_compression_threshold_in_bytes.observe([this](uint32_t v) {
                    update_threshold();
                }))
    {
        update_threshold();
    }
    response_compressor(const response_compressor& rhs) : response_compressor(rhs.cfg) {}

private:
    const db::config& cfg;
    utils::observable<int>::observer _gzip_level_observer;
    utils::observable<uint32_t>::observer _gzip_threshold_observer;
    uint32_t _threshold[static_cast<size_t>(compression_type::count)];

    size_t get_threshold() { return _threshold[static_cast<size_t>(compression_type::any)]; }
    void update_threshold() {
        _threshold[static_cast<size_t>(compression_type::none)] = std::numeric_limits<uint32_t>::max();
        _threshold[static_cast<size_t>(compression_type::any)] = std::numeric_limits<uint32_t>::max();
        uint32_t gzip = cfg.alternator_response_gzip_compression_level() <= 0 ? std::numeric_limits<uint32_t>::max()
            : cfg.alternator_response_compression_threshold_in_bytes();
        _threshold[static_cast<size_t>(compression_type::gzip)] = gzip;
        _threshold[static_cast<size_t>(compression_type::deflate)] = gzip;
        for (size_t i = 0; i < static_cast<size_t>(compression_type::compressions_count); ++i) {
            if (_threshold[i] < _threshold[static_cast<size_t>(compression_type::any)]) {
                _threshold[static_cast<size_t>(compression_type::any)] = _threshold[i];
            }
        }
    }

public:
    future<std::unique_ptr<http::reply>> generate_reply(std::unique_ptr<http::reply> rep,
         sstring accept_encoding, const char* content_type, std::string&& response_body);
    future<std::unique_ptr<http::reply>> generate_reply(std::unique_ptr<http::reply> rep,
         sstring accept_encoding, const char* content_type, executor::body_writer&& body_writer);
};

}
