/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <concepts>
#include <cstdint>
#include <optional>
#include <string_view>
#include <type_traits>

#include <fmt/format.h>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>
#include <seastar/util/bool_class.hh>

#include "gms/inet_address.hh"
#include "seastarx.hh"
#include "utils/UUID.hh"
#include "utils/from_chars_exactly.hh"

namespace api {

// Parsers for request parameter values. On a malformed value each one throws
// httpd::bad_param_exception naming the parameter, so the client gets a 400.
// Any other exception type escaping a handler is reported by seastar's httpd
// as a 500, which misattributes a client mistake to the server.
utils::UUID parse_uuid_param(std::string_view name, std::string_view value);

// Accepts true/false, yes/no and 1/0, ignoring case.
bool parse_bool_param(std::string_view name, std::string_view value);

// Accepts an IPv4 or IPv6 literal, or "localhost".
gms::inet_address parse_inet_address_param(std::string_view name, const sstring& value);

template <typename T>
requires (std::integral<T> && !std::same_as<T, bool>) || std::floating_point<T>
T parse_number_param(std::string_view name, std::string_view value) {
    return utils::from_chars_exactly<T>(value, [name] (std::string_view value) {
        return httpd::bad_param_exception{fmt::format("{}: not a valid number: '{}'", name, value)};
    });
}

// Parses a non-empty parameter value as T. Supported types: sstring (returned
// as is), utils::UUID and the tagged uuids, bool and the seastar::bool_class
// types, integral and floating point numbers, std::chrono::duration types
// (parsed as a count of their period) and gms::inet_address.
template <typename T>
T parse_param(std::string_view name, sstring value) {
    if constexpr (std::is_same_v<T, sstring>) {
        return std::move(value);
    } else if constexpr (std::is_same_v<T, gms::inet_address>) {
        return parse_inet_address_param(name, value);
    } else if constexpr (std::is_same_v<T, utils::UUID>) {
        return parse_uuid_param(name, value);
    } else if constexpr (requires(T t) { []<typename Tag>(utils::tagged_uuid<Tag>&){}(t); }) {
        return T{parse_uuid_param(name, value)};
    } else if constexpr (std::is_same_v<T, bool>) {
        return parse_bool_param(name, value);
    } else if constexpr (requires(T t) { []<typename Tag>(bool_class<Tag>&){}(t); }) {
        return T{parse_bool_param(name, value)};
    } else if constexpr (std::integral<T> || std::floating_point<T>) {
        return parse_number_param<T>(name, value);
    } else if constexpr (requires(T t) { []<typename Rep, typename Period>(std::chrono::duration<Rep, Period>&){}(t); }) {
        return T{parse_number_param<typename T::rep>(name, value)};
    } else {
        static_assert(false, "no parser for this parameter type");
    }
}

// The query parameter `name` parsed as T, or std::nullopt when the request
// does not carry it or carries it empty. A malformed value throws
// httpd::bad_param_exception.
template <typename T>
std::optional<T> try_get_query_param(const http::request& req, std::string_view name) {
    auto value = req.get_query_param(name);
    if (value.empty()) {
        return std::nullopt;
    }
    return parse_param<T>(name, std::move(value));
}

// Same as try_get_query_param, but yields `default_value` for a missing or
// empty parameter. A value-initialized T is the null id for utils::UUID and
// the tagged uuids, false for bool and bool_class, zero for numbers and
// durations, and the empty string for sstring.
template <typename T>
T get_query_param(const http::request& req, std::string_view name, T default_value = T{}) {
    return try_get_query_param<T>(req, name).value_or(std::move(default_value));
}

// Same as try_get_query_param, but a missing or empty parameter is a bad
// request.
template <typename T>
T require_query_param(const http::request& req, std::string_view name) {
    auto value = try_get_query_param<T>(req, name);
    if (!value) {
        throw httpd::bad_param_exception{fmt::format("{}: missing required parameter", name)};
    }
    return std::move(*value);
}

// The path parameter `name` parsed as T. The route matched, so the
// parameter is present; an empty or malformed value is a bad request.
// `name` is an sstring because that is what http::request::get_path_param
// takes.
template <typename T>
T require_path_param(const http::request& req, const sstring& name) {
    auto value = req.get_path_param(name);
    if (value.empty()) {
        throw httpd::bad_param_exception{fmt::format("{}: missing path parameter", name)};
    }
    return parse_param<T>(name, std::move(value));
}

}  // namespace api
