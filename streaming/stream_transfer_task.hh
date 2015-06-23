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

#include "utils/UUID.hh"
#include "streaming/stream_session.hh"
#include "streaming/stream_task.hh"
#include "streaming/messages/outgoing_file_message.hh"
#include "sstables/sstables.hh"
#include <map>

namespace streaming {

/**
 * StreamTransferTask sends sections of SSTable files in certain ColumnFamily.
 */
class stream_transfer_task : public stream_task {
private:
    int32_t sequence_number = 0;
    bool aborted = false;

    std::map<int32_t, messages::outgoing_file_message> files;
    //final Map<Integer, ScheduledFuture> timeoutTasks = new HashMap<>();

    long total_size;
public:
    using UUID = utils::UUID;
    stream_transfer_task(stream_session& session, UUID cf_id)
        : stream_task(session, cf_id) {
    }

    void add_transfer_file(sstables::sstable& sstable, int64_t estimated_keys, std::map<int64_t, int64_t> sections, int64_t repaired_at) {
        //assert sstable != null && cfId.equals(sstable.metadata.cfId);
        auto message = messages::outgoing_file_message(sstable, sequence_number++, estimated_keys, std::move(sections), repaired_at, session.keep_ss_table_level());
        auto size = message.header.size();
        auto seq = message.header.sequence_number;
        files.emplace(seq, std::move(message));
        total_size += size;
    }

#if 0

    /**
     * Received ACK for file at {@code sequenceNumber}.
     *
     * @param sequenceNumber sequence number of file
     */
    public void complete(int sequenceNumber)
    {
        boolean signalComplete;
        synchronized (this)
        {
            ScheduledFuture timeout = timeoutTasks.remove(sequenceNumber);
            if (timeout != null)
                timeout.cancel(false);

            OutgoingFileMessage file = files.remove(sequenceNumber);
            if (file != null)
                file.sstable.releaseReference();

            signalComplete = files.isEmpty();
        }

        // all file sent, notify session this task is complete.
        if (signalComplete)
            session.taskCompleted(this);
    }

    public synchronized void abort()
    {
        if (aborted)
            return;
        aborted = true;

        for (ScheduledFuture future : timeoutTasks.values())
            future.cancel(false);
        timeoutTasks.clear();

        for (OutgoingFileMessage file : files.values())
            file.sstable.releaseReference();
    }

    public synchronized int getTotalNumberOfFiles()
    {
        return files.size();
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public synchronized Collection<OutgoingFileMessage> getFileMessages()
    {
        // We may race between queuing all those messages and the completion of the completion of
        // the first ones. So copy tthe values to avoid a ConcurrentModificationException
        return new ArrayList<>(files.values());
    }

    public synchronized OutgoingFileMessage createMessageForRetry(int sequenceNumber)
    {
        // remove previous time out task to be rescheduled later
        ScheduledFuture future = timeoutTasks.remove(sequenceNumber);
        if (future != null)
            future.cancel(false);
        return files.get(sequenceNumber);
    }

    /**
     * Schedule timeout task to release reference for file sent.
     * When not receiving ACK after sending to receiver in given time,
     * the task will release reference.
     *
     * @param sequenceNumber sequence number of file sent.
     * @param time time to timeout
     * @param unit unit of given time
     * @return scheduled future for timeout task
     */
    public synchronized ScheduledFuture scheduleTimeout(final int sequenceNumber, long time, TimeUnit unit)
    {
        if (!files.containsKey(sequenceNumber))
            return null;

        ScheduledFuture future = timeoutExecutor.schedule(new Runnable()
        {
            public void run()
            {
                synchronized (StreamTransferTask.this)
                {
                    // remove so we don't cancel ourselves
                    timeoutTasks.remove(sequenceNumber);
                    StreamTransferTask.this.complete(sequenceNumber);
                }
            }
        }, time, unit);

        ScheduledFuture prev = timeoutTasks.put(sequenceNumber, future);
        assert prev == null;
        return future;
    }
#endif
};

} // namespace streaming
