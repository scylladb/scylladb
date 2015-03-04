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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.WaitQueue;

class PeriodicCommitLogService extends AbstractCommitLogService
{

    private static final int blockWhenSyncLagsMillis = (int) (DatabaseDescriptor.getCommitLogSyncPeriod() * 1.5);

    public PeriodicCommitLogService(final CommitLog commitLog)
    {
        super(commitLog, "PERIODIC-COMMIT-LOG-SYNCER", DatabaseDescriptor.getCommitLogSyncPeriod());
    }

    protected void maybeWaitForSync(CommitLogSegment.Allocation alloc)
    {
        if (waitForSyncToCatchUp(Long.MAX_VALUE))
        {
            // wait until periodic sync() catches up with its schedule
            long started = System.currentTimeMillis();
            pending.incrementAndGet();
            while (waitForSyncToCatchUp(started))
            {
                WaitQueue.Signal signal = syncComplete.register(CommitLog.instance.metrics.waitingOnCommit.time());
                if (waitForSyncToCatchUp(started))
                    signal.awaitUninterruptibly();
                else
                    signal.cancel();
            }
            pending.decrementAndGet();
        }
    }

    /**
     * @return true if sync is currently lagging behind inserts
     */
    private boolean waitForSyncToCatchUp(long started)
    {
        return started > lastSyncedAt + blockWhenSyncLagsMillis;
    }
}