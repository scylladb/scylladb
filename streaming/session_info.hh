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
#include "streaming/stream_summary.hh"
#include "streaming/stream_session.hh"
#include "streaming/progress_info.hh"
#include <vector>

namespace streaming {

/**
 * Stream session info.
 */
class session_info {
public:
    using inet_address = gms::inet_address;
    inet_address peer;
    int session_index;
    inet_address connecting;
    /** Immutable collection of receiving summaries */
    std::vector<stream_summary> receiving_summaries;
    /** Immutable collection of sending summaries*/
    std::vector<stream_summary> sending_summaries;
    /** Current session state */
    stream_session::state state;

    std::map<sstring, progress_info> receiving_files;
    std::map<sstring, progress_info> sending_files;

    session_info(inet_address peer_, int session_index_, inet_address connecting_,
                 std::vector<stream_summary> receiving_summaries_,
                 std::vector<stream_summary> sending_summaries_,
                 stream_session::state state_)
        : peer(peer_)
        , connecting(connecting_)
        , receiving_summaries(std::move(receiving_summaries_))
        , sending_summaries(std::move(sending_summaries_))
        , state(state_) {
    }

    bool is_failed() const {
        return state == stream_session::state::FAILED;
    }
#if 0

    /**
     * Update progress of receiving/sending file.
     *
     * @param newProgress new progress info
     */
    public void updateProgress(ProgressInfo newProgress)
    {
        assert peer.equals(newProgress.peer);

        Map<String, ProgressInfo> currentFiles = newProgress.direction == ProgressInfo.Direction.IN
                                                    ? receivingFiles : sendingFiles;
        currentFiles.put(newProgress.fileName, newProgress);
    }

    public Collection<ProgressInfo> getReceivingFiles()
    {
        return receivingFiles.values();
    }

    public Collection<ProgressInfo> getSendingFiles()
    {
        return sendingFiles.values();
    }

    /**
     * @return total number of files already received.
     */
    public long getTotalFilesReceived()
    {
        return getTotalFilesCompleted(receivingFiles.values());
    }

    /**
     * @return total number of files already sent.
     */
    public long getTotalFilesSent()
    {
        return getTotalFilesCompleted(sendingFiles.values());
    }

    /**
     * @return total size(in bytes) already received.
     */
    public long getTotalSizeReceived()
    {
        return getTotalSizeInProgress(receivingFiles.values());
    }

    /**
     * @return total size(in bytes) already sent.
     */
    public long getTotalSizeSent()
    {
        return getTotalSizeInProgress(sendingFiles.values());
    }

    /**
     * @return total number of files to receive in the session
     */
    public long getTotalFilesToReceive()
    {
        return getTotalFiles(receivingSummaries);
    }

    /**
     * @return total number of files to send in the session
     */
    public long getTotalFilesToSend()
    {
        return getTotalFiles(sendingSummaries);
    }

    /**
     * @return total size(in bytes) to receive in the session
     */
    public long getTotalSizeToReceive()
    {
        return getTotalSizes(receivingSummaries);
    }

    /**
     * @return total size(in bytes) to send in the session
     */
    public long getTotalSizeToSend()
    {
        return getTotalSizes(sendingSummaries);
    }

    private long getTotalSizeInProgress(Collection<ProgressInfo> files)
    {
        long total = 0;
        for (ProgressInfo file : files)
            total += file.currentBytes;
        return total;
    }

    private long getTotalFiles(Collection<StreamSummary> summaries)
    {
        long total = 0;
        for (StreamSummary summary : summaries)
            total += summary.files;
        return total;
    }

    private long getTotalSizes(Collection<StreamSummary> summaries)
    {
        long total = 0;
        for (StreamSummary summary : summaries)
            total += summary.totalSize;
        return total;
    }

    private long getTotalFilesCompleted(Collection<ProgressInfo> files)
    {
        Iterable<ProgressInfo> completed = Iterables.filter(files, new Predicate<ProgressInfo>()
        {
            public boolean apply(ProgressInfo input)
            {
                return input.isCompleted();
            }
        });
        return Iterables.size(completed);
    }
#endif
};

} // namespace streaming
