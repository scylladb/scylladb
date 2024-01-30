/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/print.hh>

#include "bytes.hh"

#include "seastarx.hh"

class redis_exception : public std::exception {
    sstring _message;
public:
    redis_exception(sstring message) : _message(std::move(message)) {}
    virtual const char* what() const noexcept override { return _message.c_str(); }
    const sstring& what_message() const noexcept { return _message; }
};

class wrong_arguments_exception : public redis_exception {
public:
    wrong_arguments_exception(size_t expected, size_t given, const bytes& command)
        : redis_exception(fmt::format("wrong number of arguments (given {}, expected {}) for '{}' command", given, expected, sstring(reinterpret_cast<const char*>(command.data()), command.size())))
    {
    }
};

class wrong_number_of_arguments_exception : public redis_exception {
public:
    wrong_number_of_arguments_exception(const bytes& command)
        : redis_exception(fmt::format("wrong number of arguments for '{}' command", sstring(reinterpret_cast<const char*>(command.data()), command.size())))
    {
    }
};

class invalid_arguments_exception : public redis_exception {
public:
    invalid_arguments_exception(const bytes& command) : redis_exception(fmt::format("invalid argument for '{}' command", sstring(reinterpret_cast<const char*>(command.data()), command.size()))) {}
};

class invalid_db_index_exception : public redis_exception {
public:
    invalid_db_index_exception() : redis_exception("DB index is out of range") {}
};
