/*
 * Copyright 2016 ScyllaDB
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

#include <functional>
#include <system_error>

typedef std::function<bool (const std::system_error &)> system_error_lambda_t;

bool check_exception(system_error_lambda_t f);
bool is_system_error_errno(int err_no);

class storage_io_error : public std::exception {
private:
    std::error_code _code;
    std::string _what;
public:
    storage_io_error(std::system_error& e) noexcept
        : _code{e.code()}
        , _what{std::string("Storage I/O error: ") + std::to_string(e.code().value()) + ": " + e.what()}
    { }

    virtual const char* what() const noexcept override {
        return _what.c_str();
    }

    const std::error_code& code() const { return _code; }
};
