/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <fmt/core.h>

namespace streaming {

enum class stream_session_state {
    INITIALIZED,
    PREPARING,
    STREAMING,
    WAIT_COMPLETE,
    COMPLETE,
    FAILED,
};

} // namespace

template <> struct fmt::formatter<streaming::stream_session_state> : fmt::formatter<string_view> {
    auto format(streaming::stream_session_state, fmt::format_context& ctx) const -> decltype(ctx.out());
};
