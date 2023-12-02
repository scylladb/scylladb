/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string>

namespace utils {

// An exception thrown when a certain process or task is being terminated because
// it has reached the memory limit allotted for said task or task group.
// This is distinct from a regular bad-alloc in that possibly there is still
// memory available but not for the task being terminated.
// Allows code like LSA to tell real alloc failure from artificial one and act
// accordingly (not retry the task).
class memory_limit_reached : public std::bad_alloc {
    std::string _msg;
public:
    memory_limit_reached(std::string_view msg) : _msg(msg) { }
    const char* what() const noexcept override { return _msg.c_str(); }
};

} // namespace utils
