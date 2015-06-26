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

#pragma once

#include "gms/inet_address.hh"
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
    class host_streaming_data;
private:
    using inet_address = gms::inet_address;
    std::map<inet_address, host_streaming_data> _peer_sessions;
    int _connections_per_host;
    bool _keep_ss_table_level;

public:
    stream_coordinator(int connections_per_host, bool keep_ss_table_level)
        : _connections_per_host(connections_per_host)
        , _keep_ss_table_level(keep_ss_table_level) {
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
#if 0
    public synchronized Collection<StreamSession> getAllStreamSessions()
    {
        Collection<StreamSession> results = new ArrayList<>();
        for (HostStreamingData data : peerSessions.values())
        {
            results.addAll(data.getAllStreamSessions());
        }
        return results;
    }

    public boolean isReceiving()
    {
        return connectionsPerHost == 0;
    }

    public void connectAllStreamSessions()
    {
        for (HostStreamingData data : peerSessions.values())
            data.connectAllStreamSessions();
    }

    public synchronized Set<InetAddress> getPeers()
    {
        return new HashSet<>(peerSessions.keySet());
    }
#endif
public:
    stream_session& get_or_create_next_session(inet_address peer, inet_address connecting) {
        return get_or_create_host_data(peer).get_or_create_next_session(peer, connecting);
    }

    stream_session& get_or_create_session_by_id(inet_address peer, int id, inet_address connecting) {
        return get_or_create_host_data(peer).get_or_create_session_by_id(peer, id, connecting);
    }

    void update_progress(progress_info info) {
        get_host_data(info.peer).update_progress(info);
    }

#if 0
    public synchronized void addSessionInfo(SessionInfo session)
    {
        HostStreamingData data = get_or_create_host_data(session.peer);
        data.addSessionInfo(session);
    }

    public synchronized Set<SessionInfo> getAllSessionInfo()
    {
        Set<SessionInfo> result = new HashSet<>();
        for (HostStreamingData data : peerSessions.values())
        {
            result.addAll(data.getAllSessionInfo());
        }
        return result;
    }
#endif
public:
    void transfer_files(inet_address to, std::vector<stream_session::ss_table_streaming_sections> sstable_details) {
        host_streaming_data& session_list = get_or_create_host_data(to);
        if (_connections_per_host > 1) {
            abort();
#if 0
            List<List<StreamSession.SSTableStreamingSections>> buckets = sliceSSTableDetails(sstableDetails);

            for (List<StreamSession.SSTableStreamingSections> subList : buckets)
            {
                StreamSession session = sessionList.get_or_create_next_session(to, to);
                session.addTransferFiles(subList);
            }
#endif
        } else {
            auto& session = session_list.get_or_create_next_session(to, to);
            session.add_transfer_files(sstable_details);
        }
    }

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
    host_streaming_data& get_host_data(inet_address peer) {
        auto it = _peer_sessions.find(peer);
        if (it == _peer_sessions.end()) {
            throw std::runtime_error(sprint("Unknown peer requested: %s", peer));
        }
        return it->second;
    }

    host_streaming_data& get_or_create_host_data(inet_address peer) {
        _peer_sessions[peer] = host_streaming_data(_connections_per_host, _keep_ss_table_level);
        return _peer_sessions[peer];
    }

#if 0
    private static class StreamSessionConnector implements Runnable
    {
        private final StreamSession session;
        public StreamSessionConnector(StreamSession session)
        {
            this.session = session;
        }

        @Override
        public void run()
        {
            session.start();
            logger.info("[Stream #{}, ID#{}] Beginning stream session with {}", session.planId(), session.sessionIndex(), session.peer);
        }
    }
#endif

private:
    class host_streaming_data {
        using inet_address = gms::inet_address;
    private:
        std::map<int, stream_session> _stream_sessions;
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

        stream_session& get_or_create_next_session(inet_address peer, inet_address connecting) {
            // create
            int size = _stream_sessions.size();
            if (size < _connections_per_host) {
                auto session = stream_session(peer, connecting, size, _keep_ss_table_level);
                _stream_sessions.emplace(++_last_returned, std::move(session));
                return _stream_sessions[_last_returned];
            // get
            } else {
                if (_last_returned >= (size - 1)) {
                    _last_returned = 0;
                }
                return _stream_sessions[_last_returned++];
            }
        }
#if 0

        public void connectAllStreamSessions()
        {
            for (StreamSession session : streamSessions.values())
            {
                streamExecutor.execute(new StreamSessionConnector(session));
            }
        }

        public Collection<StreamSession> getAllStreamSessions()
        {
            return Collections.unmodifiableCollection(streamSessions.values());
        }
#endif
        stream_session& get_or_create_session_by_id(inet_address peer, int id, inet_address connecting) {
            auto it = _stream_sessions.find(id);
            if (it == _stream_sessions.end()) {
                it = _stream_sessions.emplace(id, stream_session(peer, connecting, id, _keep_ss_table_level)).first;
            }
            return it->second;
        }

        void update_progress(progress_info info) {
            auto it = _session_infos.find(info.session_index);
            if (it != _session_infos.end()) {
                it->second.update_progress(std::move(info));
            }
        }

#if 0
        public void addSessionInfo(SessionInfo info)
        {
            sessionInfos.put(info.sessionIndex, info);
        }

        public Collection<SessionInfo> getAllSessionInfo()
        {
            return sessionInfos.values();
        }
#endif
    };
};

} // namespace streaming
