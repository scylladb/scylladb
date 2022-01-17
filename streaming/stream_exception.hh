/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "streaming/stream_state.hh"
#include <seastar/core/sstring.hh>
#include <exception>

namespace streaming {

class stream_exception : public std::exception {
public:
    stream_state state;
    sstring msg;
    stream_exception(stream_state s, sstring m)
        : state(std::move(s))
        , msg(std::move(m)) {
    }
    virtual const char* what() const noexcept override {
        return msg.c_str();
    }
};

} // namespace streaming
