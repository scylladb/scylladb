/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "gms/inet_address.hh"
#include <seastar/core/sstring.hh>

namespace streaming {

/**
 * ProgressInfo contains file transfer progress.
 */
class progress_info {
public:
    using inet_address = gms::inet_address;
    /**
     * Direction of the stream.
     */
    enum class direction { OUT, IN };

    inet_address peer;
    sstring file_name;
    direction dir;
    long current_bytes;
    long total_bytes;

    progress_info() = default;
    progress_info(inet_address _peer, sstring _file_name, direction _dir, long _current_bytes, long _total_bytes)
        : peer(_peer)
        , file_name(_file_name)
        , dir(_dir)
        , current_bytes(_current_bytes)
        , total_bytes(_total_bytes) {
    }

    /**
     * @return true if file transfer is completed
     */
    bool is_completed() const {
        return current_bytes >= total_bytes;
    }

    friend std::ostream& operator<<(std::ostream& os, const progress_info& x);
};

} // namespace streaming
