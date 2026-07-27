/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>
#include <seastar/util/bool_class.hh>
#include <string>
#include "utils/s3/aws_error_definitions_generated.hh"
#include "utils/http_client_error_processing.hh"

namespace aws {

class aws_error {
    aws_error_type _type{aws_error_type::OK};
    std::string _message;
    utils::http::retryable _is_retryable{utils::http::retryable::no};

public:
    aws_error() = default;
    aws_error(aws_error_type error_type, utils::http::retryable is_retryable);
    aws_error(aws_error_type error_type, std::string&& error_message, utils::http::retryable is_retryable);
    [[nodiscard]] const std::string& get_error_message() const { return _message; }
    [[nodiscard]] aws_error_type get_error_type() const { return _type; }
    [[nodiscard]] utils::http::retryable is_retryable() const { return _is_retryable; }
    static std::optional<aws_error> parse(seastar::sstring&& body);
    static aws_error from_http_code(seastar::http::reply::status_type http_code);
    static aws_error from_system_error(const std::system_error& system_error);
    static aws_error from_exception_ptr(std::exception_ptr exception);
    static const aws_errors& get_errors();
};

class aws_exception : public std::exception {
    aws_error _error;

public:
    explicit aws_exception(const aws_error& error) noexcept : _error(error) {}
    explicit aws_exception(aws_error&& error) noexcept : _error(std::move(error)) {}

    const char* what() const noexcept override { return _error.get_error_message().c_str(); }

    const aws_error& error() const noexcept { return _error; }
};

} // namespace aws

template <>
struct fmt::formatter<aws::aws_error_type> : fmt::formatter<string_view> {
    auto format(const aws::aws_error_type& error_type, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", static_cast<unsigned>(error_type));
    }
};
