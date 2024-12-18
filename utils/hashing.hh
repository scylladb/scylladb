/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <concepts>
#include <map>
#include <optional>
#include <memory>
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

template<typename H, typename ValueType>
concept HasherReturning = Hasher<H> &&
    requires (H& h) {
        { h.finalize() } -> std::convertible_to<ValueType>;
    };

class hasher {
public:
    virtual ~hasher() = default;
    virtual void update(const char* ptr, size_t size) noexcept = 0;
};

template<typename T>
struct appending_hash;

template<typename H, typename T, typename... Args>
requires Hasher<H>
inline
void feed_hash(H& h, const T& value, Args&&... args) noexcept(noexcept(std::declval<appending_hash<T>>()(h, value, args...))) {
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

template<>
struct appending_hash<double> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, double d) const noexcept {
        // Mimics serializer for CQL double type, for inter-machine stability.
        if (std::isnan(d)) {
            d = std::numeric_limits<double>::quiet_NaN();
        }
        static_assert(sizeof(double) == sizeof(uint64_t));
        uint64_t i;
        memcpy(&i, &d, sizeof(i));
        feed_hash(h, cpu_to_le(i));
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

template<typename T>
struct appending_hash<std::unique_ptr<T>>  {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const std::unique_ptr<T>& value) const noexcept {
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

template<typename K, typename V>
struct appending_hash<std::unordered_map<K, V>> {
    template<typename H>
    requires Hasher<H>
    void operator()(H& h, const std::unordered_map<K, V>& value) const noexcept {
        std::map<K, V> sorted(value.begin(), value.end());
        feed_hash(h, sorted);
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
