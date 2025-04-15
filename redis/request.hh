/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <vector>
#include "bytes_fwd.hh"

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
