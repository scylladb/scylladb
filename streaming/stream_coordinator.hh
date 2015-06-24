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
    bool has_active_sessions() {
        for (auto& x : _peer_sessions) {
            if (x.second.has_active_sessions()) {
                return true;
            }
        }
        return false;
    }

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

    public synchronized StreamSession getOrCreateNextSession(InetAddress peer, InetAddress connecting)
    {
        return getOrCreateHostData(peer).getOrCreateNextSession(peer, connecting);
    }

    public synchronized StreamSession getOrCreateSessionById(InetAddress peer, int id, InetAddress connecting)
    {
        return getOrCreateHostData(peer).getOrCreateSessionById(peer, id, connecting);
    }

    public synchronized void updateProgress(ProgressInfo info)
    {
        getHostData(info.peer).updateProgress(info);
    }

    public synchronized void addSessionInfo(SessionInfo session)
    {
        HostStreamingData data = getOrCreateHostData(session.peer);
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

    public synchronized void transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        HostStreamingData sessionList = getOrCreateHostData(to);

        if (connectionsPerHost > 1)
        {
            List<List<StreamSession.SSTableStreamingSections>> buckets = sliceSSTableDetails(sstableDetails);

            for (List<StreamSession.SSTableStreamingSections> subList : buckets)
            {
                StreamSession session = sessionList.getOrCreateNextSession(to, to);
                session.addTransferFiles(subList);
            }
        }
        else
        {
            StreamSession session = sessionList.getOrCreateNextSession(to, to);
            session.addTransferFiles(sstableDetails);
        }
    }

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

    private HostStreamingData getHostData(InetAddress peer)
    {
        HostStreamingData data = peerSessions.get(peer);
        if (data == null)
            throw new IllegalArgumentException("Unknown peer requested: " + peer);
        return data;
    }

    private HostStreamingData getOrCreateHostData(InetAddress peer)
    {
        HostStreamingData data = peerSessions.get(peer);
        if (data == null)
        {
            data = new HostStreamingData();
            peerSessions.put(peer, data);
        }
        return data;
    }

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
    private:
        std::map<int, stream_session> _stream_sessions;
        std::map<int, session_info> _session_infos;
        int last_returned = -1;
    public:
        bool has_active_sessions() {
            for (auto& x : _stream_sessions) {
                auto state = x.second.get_state();
                if (state != stream_session::state::COMPLETE && state != stream_session::state::FAILED) {
                    return true;
                }
            }
            return false;
        }

#if 0
        public StreamSession getOrCreateNextSession(InetAddress peer, InetAddress connecting)
        {
            // create
            if (streamSessions.size() < connectionsPerHost)
            {
                StreamSession session = new StreamSession(peer, connecting, factory, streamSessions.size(), keepSSTableLevel);
                streamSessions.put(++lastReturned, session);
                return session;
            }
            // get
            else
            {
                if (lastReturned >= streamSessions.size() - 1)
                    lastReturned = 0;

                return streamSessions.get(lastReturned++);
            }
        }

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

        public StreamSession getOrCreateSessionById(InetAddress peer, int id, InetAddress connecting)
        {
            StreamSession session = streamSessions.get(id);
            if (session == null)
            {
                session = new StreamSession(peer, connecting, factory, id, keepSSTableLevel);
                streamSessions.put(id, session);
            }
            return session;
        }

        public void updateProgress(ProgressInfo info)
        {
            sessionInfos.get(info.sessionIndex).updateProgress(info);
        }

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
