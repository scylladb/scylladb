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

#include <vector>
#include "bytes.hh"

namespace redis {

    enum class request_state {
    error,
    eof,
    ok, 
};

struct request {
    request_state _state; 
    bytes _command;
    uint32_t _args_count;
    std::vector<bytes> _args;
    size_t arguments_size() const { return _args.size(); }
    size_t total_request_size() const {
        size_t r = 0;
        for (size_t i = 0; i < _args.size(); ++i) {
            r += _args[i].size();
        }
        return r;
    }
};

}
