/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <string>
#include <exception>

namespace azure {

class auth_error : public std::exception {
    std::string _msg;
public:
    auth_error(std::string_view msg) : _msg(msg) {}
    const char* what() const noexcept override {
        return _msg.c_str();
    }
};

}