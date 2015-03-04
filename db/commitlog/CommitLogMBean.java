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


import java.io.IOException;
import java.util.List;

public interface CommitLogMBean
{
    /**
     * Get the number of completed tasks
     * @see org.apache.cassandra.metrics.CommitLogMetrics#completedTasks
     */
    @Deprecated
    public long getCompletedTasks();

    /**
     * Get the number of tasks waiting to be executed
     * @see org.apache.cassandra.metrics.CommitLogMetrics#pendingTasks
     */
    @Deprecated
    public long getPendingTasks();

    /**
     * Get the current size used by all the commitlog segments.
     * @see org.apache.cassandra.metrics.CommitLogMetrics#totalCommitLogSize
     */
    @Deprecated
    public long getTotalCommitlogSize();

    /**
     * Recover a single file.
     */
    public void recover(String path) throws IOException;

    /**
     * @return file names (not full paths) of active commit log segments (segments containing unflushed data)
     */
    public List<String> getActiveSegmentNames();
    
    /**
     * @return Files which are pending for archival attempt.  Does NOT include failed archive attempts.
     */
    public List<String> getArchivingSegmentNames();
}
