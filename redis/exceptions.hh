/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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

#include <stdexcept>
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
        : redis_exception(sprint("wrong number of arguments (given %ld, expected %ld) for '%s' command", given, expected, sstring(reinterpret_cast<const char*>(command.data()), command.size())))
    {
    }
};

class wrong_number_of_arguments_exception : public redis_exception {
public:
    wrong_number_of_arguments_exception(const bytes& command)
        : redis_exception(sprint("wrong number of arguments for '%s' command", sstring(reinterpret_cast<const char*>(command.data()), command.size())))
    {
    }
};

class invalid_arguments_exception : public redis_exception {
public:
    invalid_arguments_exception(const bytes& command) : redis_exception(sprint("invalid argument for '%s' command", command)) {}
};

class invalid_db_index_exception : public redis_exception {
public:
    invalid_db_index_exception() : redis_exception("DB index is out of range") {}
};
