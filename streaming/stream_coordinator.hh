/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
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

#include "gms/inet_address.hh"
#include "streaming/stream_session.hh"
#include "streaming/session_info.hh"
#include <map>
#include <algorithm>

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
    std::map<inet_address, host_streaming_data> _peer_sessions;
    int _connections_per_host;
    bool _keep_ss_table_level;
    bool _is_receiving;

public:
    stream_coordinator(int connections_per_host, bool keep_ss_table_level, bool is_receiving = false)
        : _connections_per_host(connections_per_host)
        , _keep_ss_table_level(keep_ss_table_level)
        , _is_receiving(is_receiving) {
    }
#if 0
    public void setConnectionFactory(StreamConnectionFactory factory)
    {
        this.factory = factory;
    }
#endif
public:
    /**
     * @return true if any stream session is active
     */
    bool has_active_sessions();

    std::vector<shared_ptr<stream_session>> get_all_stream_sessions();

    bool is_receiving();

    void connect_all_stream_sessions();
    std::set<inet_address> get_peers();

public:
    shared_ptr<stream_session> get_or_create_next_session(inet_address peer, inet_address connecting) {
        return get_or_create_host_data(peer).get_or_create_next_session(peer, connecting);
    }

    shared_ptr<stream_session> get_or_create_session_by_id(inet_address peer, int id, inet_address connecting) {
        return get_or_create_host_data(peer).get_or_create_session_by_id(peer, id, connecting);
    }

    void update_progress(progress_info info) {
        get_host_data(info.peer).update_progress(info);
    }

    void add_session_info(session_info session);

    std::vector<session_info> get_all_session_info();

public:
    void transfer_files(inet_address to, std::vector<stream_detail> sstable_details);

private:
#if 0
    private List<List<StreamSession.SSTableStreamingSections>> sliceSSTableDetails(Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        // There's no point in divvying things up into more buckets than we have sstableDetails
        int targetSlices = Math.min(sstableDetails.size(), connectionsPerHost);
        int step = Math.round((float) sstableDetails.size() / (float) targetSlices);
        int index = 0;

        List<List<StreamSession.SSTableStreamingSections>> result = new ArrayList<>();
        List<StreamSession.SSTableStreamingSections> slice = null;
        Iterator<StreamSession.SSTableStreamingSections> iter = sstableDetails.iterator();
        while (iter.hasNext())
        {
            StreamSession.SSTableStreamingSections streamSession = iter.next();

            if (index % step == 0)
            {
                slice = new ArrayList<>();
                result.add(slice);
            }
            slice.add(streamSession);
            ++index;
            iter.remove();
        }

        return result;
    }

#endif
    host_streaming_data& get_host_data(inet_address peer);
    host_streaming_data& get_or_create_host_data(inet_address peer);

private:
    class host_streaming_data {
        using inet_address = gms::inet_address;
    private:
        std::map<int, shared_ptr<stream_session>> _stream_sessions;
        std::map<int, session_info> _session_infos;
        int _last_returned = -1;
        int _connections_per_host;
        bool _keep_ss_table_level;

    public:
        host_streaming_data() = default;

        host_streaming_data(int connections_per_host, bool keep_ss_table_level)
            : _connections_per_host(connections_per_host)
            , _keep_ss_table_level(keep_ss_table_level) {
        }

        bool has_active_sessions();

        shared_ptr<stream_session> get_or_create_next_session(inet_address peer, inet_address connecting);

        void connect_all_stream_sessions();

        std::vector<shared_ptr<stream_session>> get_all_stream_sessions();

        shared_ptr<stream_session> get_or_create_session_by_id(inet_address peer, int id, inet_address connecting);

        void update_progress(progress_info info);

        void add_session_info(session_info info);

        std::vector<session_info> get_all_session_info();
    };
};

} // namespace streaming
