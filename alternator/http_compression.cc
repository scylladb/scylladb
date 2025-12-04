/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "alternator/http_compression.hh"
namespace alternator {

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

} // namespace alternator
