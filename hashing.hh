/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <chrono>
#include <map>
#include <optional>
#include <concepts>
#include <seastar/core/byteorder.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

//
// This hashing differs from std::hash<> in that it decouples knowledge about
// type structure from the way the hash value is calculated:
//  * appending_hash<T> instantiation knows about what data should be included in the hash for type T.
//  * Hasher object knows how to combine the data into the final hash.
//
// The appending_hash<T> should always feed some data into the hasher, regardless of the state the object is in,
// in order for the hash to be highly sensitive for value changes. For example, vector<optional<T>> should
// ideally feed different values for empty vector and a vector with a single empty optional.
//
// appending_hash<T> is machine-independent.
//

template<typename H>
concept Hasher =
    requires(H& h, const char* ptr, size_t size) {
        { h.update(ptr, size) } noexcept -> std::same_as<void>;
    };

class hasher {
public:
    virtual ~hasher() = default;
    virtual void update(const char* ptr, size_t size) noexcept = 0;
};

static_assert(Hasher<hasher>);

template<typename T>
struct appending_hash;

template<typename H, typename T, typename... Args>
requires Hasher<H>
inline
void feed_hash(H& h, const T& value, Args&&... args) noexcept {
    appending_hash<T>()(h, value, std::forward<Args>(args)...);
};

template<typename T>
requires std::is_arithmetic_v<T>
struct appending_hash<T> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, T value) const noexcept {
        auto value_le = cpu_to_le(value);
        h.update(reinterpret_cast<const char*>(&value_le), sizeof(T));
    }
};

template<>
struct appending_hash<bool> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, bool value) const noexcept {
        feed_hash(h, static_cast<uint8_t>(value));
    }
};

template<typename T>
requires std::is_enum_v<T>
struct appending_hash<T> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const T& value) const noexcept {
        feed_hash(h, static_cast<std::underlying_type_t<T>>(value));
    }
};

template<typename T>
struct appending_hash<std::optional<T>>  {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const std::optional<T>& value) const noexcept {
        if (value) {
            feed_hash(h, true);
            feed_hash(h, *value);
        } else {
            feed_hash(h, false);
        }
    }
};

template<size_t N>
struct appending_hash<char[N]>  {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const char (&value) [N]) const noexcept {
        feed_hash(h, N);
        h.update(value, N);
    }
};

template<typename T>
struct appending_hash<std::vector<T>> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const std::vector<T>& value) const noexcept {
        feed_hash(h, value.size());
        for (auto&& v : value) {
            appending_hash<T>()(h, v);
        }
    }
};

template<typename K, typename V>
struct appending_hash<std::map<K, V>> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const std::map<K, V>& value) const noexcept {
        feed_hash(h, value.size());
        for (auto&& e : value) {
            appending_hash<K>()(h, e.first);
            appending_hash<V>()(h, e.second);
        }
    }
};

template<>
struct appending_hash<sstring> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const sstring& v) const noexcept {
        feed_hash(h, v.size());
        h.update(reinterpret_cast<const char*>(v.cbegin()), v.size() * sizeof(sstring::value_type));
    }
};

template<>
struct appending_hash<std::string> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const std::string& v) const noexcept {
        feed_hash(h, v.size());
        h.update(reinterpret_cast<const char*>(v.data()), v.size() * sizeof(std::string::value_type));
    }
};

template<typename T, typename R>
struct appending_hash<std::chrono::duration<T, R>> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, std::chrono::duration<T, R> v) const noexcept {
        feed_hash(h, v.count());
    }
};

template<typename Clock, typename Duration>
struct appending_hash<std::chrono::time_point<Clock, Duration>> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, std::chrono::time_point<Clock, Duration> v) const noexcept {
        feed_hash(h, v.time_since_epoch().count());
    }
};
