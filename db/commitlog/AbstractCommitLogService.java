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
 */
package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.slf4j.*;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;

public abstract class AbstractCommitLogService
{
    // how often should we log syngs that lag behind our desired period
    private static final long LAG_REPORT_INTERVAL = TimeUnit.MINUTES.toMillis(5);

    private final Thread thread;
    private volatile boolean shutdown = false;

    // all Allocations written before this time will be synced
    protected volatile long lastSyncedAt = System.currentTimeMillis();

    // counts of total written, and pending, log messages
    private final AtomicLong written = new AtomicLong(0);
    protected final AtomicLong pending = new AtomicLong(0);

    // signal that writers can wait on to be notified of a completed sync
    protected final WaitQueue syncComplete = new WaitQueue();
    private final Semaphore haveWork = new Semaphore(1);

    private static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogService.class);

    /**
     * CommitLogService provides a fsync service for Allocations, fulfilling either the
     * Batch or Periodic contract.
     *
     * Subclasses may be notified when a sync finishes by using the syncComplete WaitQueue.
     */
    AbstractCommitLogService(final CommitLog commitLog, final String name, final long pollIntervalMillis)
    {
        if (pollIntervalMillis < 1)
            throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %dms", pollIntervalMillis));

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                long firstLagAt = 0;
                long totalSyncDuration = 0; // total time spent syncing since firstLagAt
                long syncExceededIntervalBy = 0; // time that syncs exceeded pollInterval since firstLagAt
                int lagCount = 0;
                int syncCount = 0;

                boolean run = true;
                while (run)
                {
                    try
                    {
                        // always run once after shutdown signalled
                        run = !shutdown;

                        // sync and signal
                        long syncStarted = System.currentTimeMillis();
                        commitLog.sync(shutdown);
                        lastSyncedAt = syncStarted;
                        syncComplete.signalAll();


                        // sleep any time we have left before the next one is due
                        long now = System.currentTimeMillis();
                        long sleep = syncStarted + pollIntervalMillis - now;
                        if (sleep < 0)
                        {
                            // if we have lagged noticeably, update our lag counter
                            if (firstLagAt == 0)
                            {
                                firstLagAt = now;
                                totalSyncDuration = syncExceededIntervalBy = syncCount = lagCount = 0;
                            }
                            syncExceededIntervalBy -= sleep;
                            lagCount++;
                        }
                        syncCount++;
                        totalSyncDuration += now - syncStarted;

                        if (firstLagAt > 0 && now - firstLagAt >= LAG_REPORT_INTERVAL)
                        {
                            logger.warn(String.format("Out of %d commit log syncs over the past %ds with average duration of %.2fms, %d have exceeded the configured commit interval by an average of %.2fms",
                                                      syncCount, (now - firstLagAt) / 1000, (double) totalSyncDuration / syncCount, lagCount, (double) syncExceededIntervalBy / lagCount));
                            firstLagAt = 0;
                        }

                        // if we have lagged this round, we probably have work to do already so we don't sleep
                        if (sleep < 0 || !run)
                            continue;

                        try
                        {
                            haveWork.tryAcquire(sleep, TimeUnit.MILLISECONDS);
                        }
                        catch (InterruptedException e)
                        {
                            throw new AssertionError();
                        }
                    }
                    catch (Throwable t)
                    {
                        if (!CommitLog.handleCommitError("Failed to persist commits to disk", t))
                            break;

                        // sleep for full poll-interval after an error, so we don't spam the log file
                        try
                        {
                            haveWork.tryAcquire(pollIntervalMillis, TimeUnit.MILLISECONDS);
                        }
                        catch (InterruptedException e)
                        {
                            throw new AssertionError();
                        }
                    }
                }
            }
        };

        thread = new Thread(runnable, name);
        thread.start();
    }

    /**
     * Block for @param alloc to be sync'd as necessary, and handle bookkeeping
     */
    public void finishWriteFor(Allocation alloc)
    {
        maybeWaitForSync(alloc);
        written.incrementAndGet();
    }

    protected abstract void maybeWaitForSync(Allocation alloc);

    /**
     * Sync immediately, but don't block for the sync to cmplete
     */
    public WaitQueue.Signal requestExtraSync()
    {
        WaitQueue.Signal signal = syncComplete.register();
        haveWork.release(1);
        return signal;
    }

    public void shutdown()
    {
        shutdown = true;
        haveWork.release(1);
    }

    public void awaitTermination() throws InterruptedException
    {
        thread.join();
    }

    public long getCompletedTasks()
    {
        return written.incrementAndGet();
    }

    public long getPendingTasks()
    {
        return pending.incrementAndGet();
    }
}