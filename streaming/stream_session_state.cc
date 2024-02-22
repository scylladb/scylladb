/*
 * Modified by ScyllaDB.
 * Copyright 2015-present ScyllaDB.
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "streaming/stream_session_state.hh"
#include <map>
#include "seastarx.hh"

namespace streaming {

static const std::map<stream_session_state, std::string_view> stream_session_state_names = {
    {stream_session_state::INITIALIZED,     "INITIALIZED"},
    {stream_session_state::PREPARING,       "PREPARING"},
    {stream_session_state::STREAMING,       "STREAMING"},
    {stream_session_state::WAIT_COMPLETE,   "WAIT_COMPLETE"},
    {stream_session_state::COMPLETE,        "COMPLETE"},
    {stream_session_state::FAILED,          "FAILED"},
};

}

auto fmt::formatter<streaming::stream_session_state>::format(streaming::stream_session_state s, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", streaming::stream_session_state_names.at(s));
}
