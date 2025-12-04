/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "alternator/http_compression.hh"
#include "alternator/server.hh"
#include <seastar/coroutine/maybe_yield.hh>
#include <zlib.h>

static logging::logger slogger("alternator-http-compression");

namespace alternator {


static constexpr size_t compressed_buffer_size = 1024;
class zlib_compressor {
    z_stream _zs;
    temporary_buffer<char> _output_buf;
    noncopyable_function<future<>(temporary_buffer<char>&&)> _write_func;
public:
    zlib_compressor(bool gzip, int compression_level, noncopyable_function<future<>(temporary_buffer<char>&&)> write_func)
     : _write_func(std::move(write_func)) {
        memset(&_zs, 0, sizeof(_zs));
        if (deflateInit2(&_zs, std::clamp(compression_level, Z_NO_COMPRESSION, Z_BEST_COMPRESSION), Z_DEFLATED,
                (gzip ? 16 : 0) + MAX_WBITS, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            // Should only happen if memory allocation fails
            throw std::bad_alloc();
        }
    }
    ~zlib_compressor() {
        deflateEnd(&_zs);
    }
    future<> close() {
        return compress(nullptr, 0, true);
    }

    future<> compress(const char* buf, size_t len, bool is_last_chunk = false) {
        _zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(buf));
        _zs.avail_in = (uInt) len;
        int mode = is_last_chunk ? Z_FINISH : Z_NO_FLUSH;
        while(_zs.avail_in > 0 || is_last_chunk) {
            co_await coroutine::maybe_yield();
            if (_output_buf.empty()) {
                if (is_last_chunk) {
                    uint32_t max_buffer_size = 0;
                    deflatePending(&_zs, &max_buffer_size, nullptr);
                    max_buffer_size += deflateBound(&_zs, _zs.avail_in) + 1;
                    _output_buf = temporary_buffer<char>(std::min(compressed_buffer_size, (size_t) max_buffer_size));
                } else {
                    _output_buf = temporary_buffer<char>(compressed_buffer_size);
                }
                _zs.next_out = reinterpret_cast<unsigned char*>(_output_buf.get_write());
                _zs.avail_out = compressed_buffer_size;
            }
            int e = deflate(&_zs, mode);
            if (e < Z_OK) {
                throw api_error::internal("Error during compression of response body");
            }
            if (e == Z_STREAM_END || _zs.avail_out < compressed_buffer_size / 4) {
                _output_buf.trim(compressed_buffer_size - _zs.avail_out);
                co_await _write_func(std::move(_output_buf));
                if (e == Z_STREAM_END) {
                    break;
                }
            }
        }
    }
};

// Helper string_view functions for parsing Accept-Encoding header
struct case_insensitive_cmp_sv {
    bool operator()(std::string_view s1, std::string_view s2) const {
        return std::equal(s1.begin(), s1.end(), s2.begin(), s2.end(),
            [](char a, char b) { return ::tolower(a) == ::tolower(b); });
    }
};
static inline std::string_view trim_left(std::string_view sv) {
    while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.front())))
        sv.remove_prefix(1);
    return sv;
}
static inline std::string_view trim_right(std::string_view sv) {
    while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.back())))
        sv.remove_suffix(1);
    return sv;
}
static inline std::string_view trim(std::string_view sv) {
    return trim_left(trim_right(sv));
}

inline std::vector<std::string_view> split(std::string_view text, char separator) {
    std::vector<std::string_view> tokens;
    if (text == "") {
        return tokens;
    }

    while (true) {
        auto pos = text.find_first_of(separator);
        if (pos != std::string_view::npos) {
            tokens.emplace_back(text.data(), pos);
            text.remove_prefix(pos + 1);
        } else {
            tokens.emplace_back(text);
            break;
        }
    }
    return tokens;
}

constexpr response_compressor::compression_type response_compressor::get_compression_type(std::string_view encoding) {
    for (size_t i = 0; i < static_cast<size_t>(compression_type::count); ++i) {
        if (case_insensitive_cmp_sv{}(encoding, compression_names[i])) {
            return static_cast<compression_type>(i);
        }
    }
    return compression_type::unknown;
}

response_compressor::compression_type response_compressor::find_compression(std::string_view accept_encoding, size_t response_size) {
    std::optional<float> ct_q[static_cast<size_t>(compression_type::count)];
    ct_q[static_cast<size_t>(compression_type::none)] = std::numeric_limits<float>::min(); // enabled, but lowest priority
    compression_type selected_ct = compression_type::none;

    std::vector<std::string_view> entries = split(accept_encoding, ',');
    for (auto& e : entries) {
        std::vector<std::string_view> params = split(e, ';');
        if (params.size() == 0) {
            continue;
        }
        compression_type ct = get_compression_type(trim(params[0]));
        if (ct == compression_type::unknown) {
            continue; // ignore unknown encoding types
        }
        if (ct_q[static_cast<size_t>(ct)].has_value() && ct_q[static_cast<size_t>(ct)] != 0.0f) {
            continue; // already processed this encoding
        }
        if (response_size < _threshold[static_cast<size_t>(ct)]) {
            continue; // below threshold treat as unknown
        }
        for (size_t i = 1; i < params.size(); ++i) { // find "q=" parameter
            auto pos = params[i].find("q=");
            if (pos == std::string_view::npos) {
                continue;
            }
            std::string_view param = params[i].substr(pos + 2);
            param = trim(param);
            // parse quality value
            float q_value = 1.0f;
            auto [ptr, ec] = std::from_chars(param.data(), param.data() + param.size(), q_value);
            if (ec != std::errc() || ptr != param.data() + param.size()) {
                continue;
            }
            if (q_value < 0.0) {
                q_value = 0.0;
            } else if (q_value > 1.0) {
                q_value = 1.0;
            }
            ct_q[static_cast<size_t>(ct)] = q_value;
            break; // we parsed quality value
        }
        if (!ct_q[static_cast<size_t>(ct)].has_value()) {
            ct_q[static_cast<size_t>(ct)] = 1.0f; // default quality value
        }
        // keep the highest encoding (in the order, unless 'any')
        if (selected_ct == compression_type::any) {
            if (ct_q[static_cast<size_t>(ct)] >= ct_q[static_cast<size_t>(selected_ct)]) {
                selected_ct = ct;
            }
        } else {
            if (ct_q[static_cast<size_t>(ct)] > ct_q[static_cast<size_t>(selected_ct)]) {
                selected_ct = ct;
            }
        }
    }
    if (selected_ct == compression_type::any) {
        // select any not mentioned or highest quality
        selected_ct = compression_type::none;
        for (size_t i = 0; i < static_cast<size_t>(compression_type::compressions_count); ++i) {
            if (!ct_q[i].has_value()) {
                return static_cast<compression_type>(i);
            }
            if (ct_q[i] > ct_q[static_cast<size_t>(selected_ct)]) {
                selected_ct = static_cast<compression_type>(i);
            }
        }
    }
    return selected_ct;
}

static future<chunked_content> compress(response_compressor::compression_type ct, const db::config& cfg, std::string str) {
    chunked_content compressed;
    auto write = [&compressed](temporary_buffer<char>&& buf) -> future<> {
        compressed.push_back(std::move(buf));
        return make_ready_future<>();
    };
    zlib_compressor compressor(ct != response_compressor::compression_type::deflate,
        cfg.alternator_response_gzip_compression_level(), std::move(write));
    co_await compressor.compress(str.data(), str.size(), true);
    co_return compressed;
}

static sstring flatten(chunked_content&& cc) {
    size_t total_size = 0;
    for (const auto& chunk : cc) {
        total_size += chunk.size();
    }
    sstring result = sstring{ sstring::initialized_later{}, total_size };
    size_t offset = 0;
    for (const auto& chunk : cc) {
        std::copy(chunk.begin(), chunk.end(), result.begin() + offset);
        offset += chunk.size();
    }
    return result;
}

future<std::unique_ptr<http::reply>> response_compressor::generate_reply(std::unique_ptr<http::reply> rep, sstring accept_encoding, const char* content_type, std::string&& response_body) {
    response_compressor::compression_type ct = find_compression(accept_encoding, response_body.size());
    if (ct != response_compressor::compression_type::none) {
        rep->add_header("Content-Encoding", get_encoding_name(ct));
        rep->set_content_type(content_type);
        return compress(ct, cfg, std::move(response_body)).then([rep = std::move(rep)] (chunked_content compressed) mutable {
            rep->_content = flatten(std::move(compressed));
            return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
        });
    } else {
        // Note that despite the move, there is a copy here -
        // as str is std::string and rep->_content is sstring.
        rep->_content = std::move(response_body);
        rep->set_content_type(content_type);
    }
    return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
}

} // namespace alternator
