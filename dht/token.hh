/*
 * Copyright (C) 2020 ScyllaDB
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

#include "bytes.hh"
#include "utils/managed_bytes.hh"

#include <fmt/format.h>
#include <array>
#include <functional>
#include <utility>

namespace dht {

class token;

enum class token_kind {
    before_all_keys,
    key,
    after_all_keys,
};

class token_view {
public:
    token_kind _kind;
    std::reference_wrapper<const std::array<uint8_t, 8>> _data;

    token_view(token_kind kind, const std::array<uint8_t, 8>& data) : _kind(kind), _data(data) {}
    explicit token_view(const token& token);

    bool is_minimum() const {
        return _kind == token_kind::before_all_keys;
    }

    bool is_maximum() const {
        return _kind == token_kind::after_all_keys;
    }
};

class token {
public:
    using kind = token_kind;
    kind _kind;
    // _data can be interpreted as a big endian binary fraction
    // in the range [0.0, 1.0).
    //
    // So, [] == 0.0
    //     [0x00] == 0.0
    //     [0x80] == 0.5
    //     [0x00, 0x80] == 1/512
    //     [0xff, 0x80] == 1 - 1/512
    std::array<uint8_t, 8> _data;

    token() : _kind(kind::before_all_keys) {
    }

    token(kind k, const std::array<uint8_t, 8>& d) : _kind(std::move(k)), _data(d) {
    }

    token(kind k, const bytes& b) : _kind(std::move(k)) {
        if (b.size() != 8) {
            throw std::runtime_error(fmt::format("Wrong token bytes size: expected 8 but got {}", b.size()));
        }
        std::copy_n(b.begin(), 8, _data.begin());
    }

    token(kind k, bytes_view b) : _kind(std::move(k)) {
        if (b.size() != 8) {
            throw std::runtime_error(fmt::format("Wrong token bytes size: expected 8 but got {}", b.size()));
        }
        std::copy_n(b.begin(), 8, _data.begin());
    }

    bool is_minimum() const {
        return _kind == kind::before_all_keys;
    }

    bool is_maximum() const {
        return _kind == kind::after_all_keys;
    }

    size_t external_memory_usage() const {
        return 0;
    }

    size_t memory_usage() const {
        return sizeof(token);
    }

    explicit token(token_view v) : _kind(v._kind), _data(v._data) {}

    operator token_view() const {
        return token_view(*this);
    }

    bytes_view data() const {
        return bytes_view(reinterpret_cast<const int8_t*>(_data.data()), _data.size());
    }
};

inline token_view::token_view(const token& token) : _kind(token._kind), _data(token._data) {}

const token& minimum_token();
const token& maximum_token();
int tri_compare(token_view t1, token_view t2);
inline bool operator==(token_view t1, token_view t2) { return tri_compare(t1, t2) == 0; }
inline bool operator<(token_view t1, token_view t2) { return tri_compare(t1, t2) < 0; }

inline bool operator!=(const token& t1, const token& t2) { return std::rel_ops::operator!=(t1, t2); }
inline bool operator>(const token& t1, const token& t2) { return std::rel_ops::operator>(t1, t2); }
inline bool operator<=(const token& t1, const token& t2) { return std::rel_ops::operator<=(t1, t2); }
inline bool operator>=(const token& t1, const token& t2) { return std::rel_ops::operator>=(t1, t2); }
std::ostream& operator<<(std::ostream& out, const token& t);

} // namespace dht
