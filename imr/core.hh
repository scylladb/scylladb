/*
 * Copyright (C) 2018 ScyllaDB
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

#include "utils/fragment_range.hh"

namespace imr {

/// No-op deserialisation context
///
/// This is a dummy deserialisation context to be used when there is no need
/// for one, but the interface expects a context object.
static const struct no_context_t {
    template<typename Tag, typename... Args>
    const no_context_t& context_for(Args&&...) const noexcept { return *this; }
} no_context;

struct no_op_continuation {
    template<typename T>
    static T run(T value) noexcept {
        return value;
    }
};

template<typename T>
class placeholder {
    uint8_t* _pointer = nullptr;
public:
    placeholder() = default;
    explicit placeholder(uint8_t* ptr) noexcept : _pointer(ptr) { }
    void set_pointer(uint8_t* ptr) noexcept { _pointer = ptr; }

    template<typename... Args>
    void serialize(Args&&... args) noexcept {
        if (!_pointer) {
            // We lose the information whether we are in the sizing or
            // serializing phase, hence the need for this run-time check.
            return;
        }
        T::serialize(_pointer, std::forward<Args>(args)...);
    }
};

}
