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
#include "types.hh"

#include <seastar/net/byteorder.hh>
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

class token {
    static inline int64_t normalize(int64_t t) {
        return t == std::numeric_limits<int64_t>::min() ? std::numeric_limits<int64_t>::max() : t;
    }
public:
    using kind = token_kind;
    kind _kind;
    int64_t _data;

    token() : _kind(kind::before_all_keys) {
    }

    token(kind k, int64_t d)
        : _kind(std::move(k))
        , _data(normalize(d)) { }

    token(kind k, const bytes& b) : _kind(std::move(k)) {
        if (_kind != kind::key) {
            _data = 0;
        } else {
            if (b.size() != sizeof(_data)) {
                throw std::runtime_error(fmt::format("Wrong token bytes size: expected {} but got {}", sizeof(_data), b.size()));
            }
            std::copy_n(b.begin(), sizeof(_data), reinterpret_cast<int8_t *>(&_data));
            _data = net::ntoh(_data);
        }
    }

    token(kind k, bytes_view b) : _kind(std::move(k)) {
        if (_kind != kind::key) {
            _data = 0;
        } else {
            if (b.size() != sizeof(_data)) {
                throw std::runtime_error(fmt::format("Wrong token bytes size: expected {} but got {}", sizeof(_data), b.size()));
            }
            std::copy_n(b.begin(), sizeof(_data), reinterpret_cast<int8_t *>(&_data));
            _data = net::ntoh(_data);
        }
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

    bytes data() const {
        auto t = net::hton(_data);
        bytes b(bytes::initialized_later(), sizeof(_data));
        std::copy_n(reinterpret_cast<int8_t*>(&t), sizeof(_data), b.begin());
        return b;
    }

    /**
     * @return a string representation of this token
     */
    sstring to_sstring() const;

    /**
     * Calculate a token representing the approximate "middle" of the given
     * range.
     *
     * @return The approximate midpoint between left and right.
     */
    static token midpoint(const token& left, const token& right);

    /**
     * @return a randomly generated token
     */
    static token get_random_token();

    /**
     * @return a token from string representation
     */
    static dht::token from_sstring(const sstring& t);

    /**
     * @return a token from its byte representation
     */
    static dht::token from_bytes(bytes_view bytes);

    /**
     * Returns int64_t representation of the token
     */
    static int64_t to_int64(token);

    /**
     * Creates token from its int64_t representation
     */
    static dht::token from_int64(int64_t);

    /**
     * Calculate the deltas between tokens in the ring in order to compare
     *  relative sizes.
     *
     * @param sortedtokens a sorted List of tokens
     * @return the mapping from 'token' to 'percentage of the ring owned by that token'.
     */
    static std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens);

    static data_type get_token_validator();

    /**
     * Gets the first shard of the minimum token.
     */
    static unsigned shard_of_minimum_token() {
        return 0;  // hardcoded for now; unlikely to change
    }

};

const token& minimum_token();
const token& maximum_token();
int tri_compare(const token& t1, const token& t2);
inline bool operator==(const token& t1, const token& t2) { return tri_compare(t1, t2) == 0; }
inline bool operator<(const token& t1, const token& t2) { return tri_compare(t1, t2) < 0; }

inline bool operator!=(const token& t1, const token& t2) { return std::rel_ops::operator!=(t1, t2); }
inline bool operator>(const token& t1, const token& t2) { return std::rel_ops::operator>(t1, t2); }
inline bool operator<=(const token& t1, const token& t2) { return std::rel_ops::operator<=(t1, t2); }
inline bool operator>=(const token& t1, const token& t2) { return std::rel_ops::operator>=(t1, t2); }
std::ostream& operator<<(std::ostream& out, const token& t);

} // namespace dht
