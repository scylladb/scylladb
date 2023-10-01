/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <ostream>
#include <fmt/ostream.h>

namespace streaming {

enum class stream_session_state {
    INITIALIZED,
    PREPARING,
    STREAMING,
    WAIT_COMPLETE,
    COMPLETE,
    FAILED,
};

std::ostream& operator<<(std::ostream& os, const stream_session_state& s);

} // namespace

template <> struct fmt::formatter<streaming::stream_session_state> : fmt::ostream_formatter {};
