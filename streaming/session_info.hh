/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "gms/inet_address.hh"
#include "streaming/stream_summary.hh"
#include "streaming/stream_session_state.hh"
#include "streaming/progress_info.hh"
#include <vector>
#include <map>
#include "locator/host_id.hh"

namespace streaming {

/**
 * Stream session info.
 */
class session_info {
public:
    locator::host_id peer;
    /** Immutable collection of receiving summaries */
    std::vector<stream_summary> receiving_summaries;
    /** Immutable collection of sending summaries*/
    std::vector<stream_summary> sending_summaries;
    /** Current session state */
    stream_session_state state;

    std::map<sstring, progress_info> receiving_files;
    std::map<sstring, progress_info> sending_files;

    session_info() = default;
    session_info(locator::host_id peer_,
                 std::vector<stream_summary> receiving_summaries_,
                 std::vector<stream_summary> sending_summaries_,
                 stream_session_state state_)
        : peer(peer_)
        , receiving_summaries(std::move(receiving_summaries_))
        , sending_summaries(std::move(sending_summaries_))
        , state(state_) {
    }

    bool is_failed() const {
        return state == stream_session_state::FAILED;
    }

    /**
     * Update progress of receiving/sending file.
     *
     * @param newProgress new progress info
     */
    void update_progress(progress_info new_progress);

    std::vector<progress_info> get_receiving_files() const;

    std::vector<progress_info> get_sending_files() const;

    /**
     * @return total number of files already received.
     */
    long get_total_files_received() const {
        return get_total_files_completed(get_receiving_files());
    }

    /**
     * @return total number of files already sent.
     */
    long get_total_files_sent() const {
        return get_total_files_completed(get_sending_files());
    }

    /**
     * @return total size(in bytes) already received.
     */
    long get_total_size_received() const {
        return get_total_size_in_progress(get_receiving_files());
    }

    /**
     * @return total size(in bytes) already sent.
     */
    long get_total_size_sent() const {
        return get_total_size_in_progress(get_sending_files());
    }

    /**
     * @return total number of files to receive in the session
     */
    long get_total_files_to_receive() const {
        return get_total_files(receiving_summaries);
    }

    /**
     * @return total number of files to send in the session
     */
    long get_total_files_to_send() const {
        return get_total_files(sending_summaries);
    }

    /**
     * @return total size(in bytes) to receive in the session
     */
    long get_total_size_to_receive() const {
        return get_total_sizes(receiving_summaries);
    }

    /**
     * @return total size(in bytes) to send in the session
     */
    long get_total_size_to_send() const {
        return get_total_sizes(sending_summaries);
    }

private:
    long get_total_size_in_progress(std::vector<progress_info> files) const;

    long get_total_files(std::vector<stream_summary> const& summaries) const;

    long get_total_sizes(std::vector<stream_summary> const& summaries) const;

    long get_total_files_completed(std::vector<progress_info> files) const;
};

} // namespace streaming
