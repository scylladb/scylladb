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
#include "streaming/stream_fwd.hh"
#include "streaming/stream_session.hh"
#include "streaming/session_info.hh"
#include <map>

namespace streaming {

/**
 * {@link StreamCoordinator} is a helper class that abstracts away maintaining multiple
 * StreamSession and ProgressInfo instances per peer.
 *
 * This class coordinates multiple SessionStreams per peer in both the outgoing StreamPlan context and on the
 * inbound StreamResultFuture context.
 */
class stream_coordinator {
public:
    using inet_address = gms::inet_address;

private:
    class host_streaming_data;
    std::map<inet_address, shared_ptr<stream_session>> _peer_sessions;
    bool _is_receiving;

public:
    stream_coordinator(bool is_receiving = false)
        : _is_receiving(is_receiving) {
    }
public:
    /**
     * @return true if any stream session is active
     */
    bool has_active_sessions() const;

    std::vector<shared_ptr<stream_session>> get_all_stream_sessions() const;

    bool is_receiving() const;

    void connect_all_stream_sessions();
    std::set<inet_address> get_peers() const;

public:
    shared_ptr<stream_session> get_or_create_session(stream_manager& mgr, inet_address peer) {
        auto& session = _peer_sessions[peer];
        if (!session) {
            session = make_shared<stream_session>(mgr, peer);
        }
        return session;
    }

    std::vector<session_info> get_all_session_info() const;
    std::vector<session_info> get_peer_session_info(inet_address peer) const;

    void abort_all_stream_sessions();
};

} // namespace streaming
