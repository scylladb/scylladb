/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "streaming/stream_request.hh"
#include "streaming/stream_summary.hh"

namespace streaming {

class prepare_message {
public:
    /**
     * Streaming requests
     */
    std::vector<stream_request> requests;

    /**
     * Summaries of streaming out
     */
    std::vector<stream_summary> summaries;

    uint32_t dst_cpu_id;

    prepare_message() = default;
    prepare_message(std::vector<stream_request> reqs, std::vector<stream_summary> sums, uint32_t dst_cpu_id_ = -1)
        : requests(std::move(reqs))
        , summaries(std::move(sums))
        , dst_cpu_id(dst_cpu_id_) {
    }
};

} // namespace streaming
