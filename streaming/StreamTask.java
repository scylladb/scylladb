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
package org.apache.cassandra.streaming;

import java.util.UUID;

/**
 * StreamTask is an abstraction of the streaming task performed over specific ColumnFamily.
 */
public abstract class StreamTask
{
    /** StreamSession that this task belongs */
    protected final StreamSession session;

    protected final UUID cfId;

    protected StreamTask(StreamSession session, UUID cfId)
    {
        this.session = session;
        this.cfId = cfId;
    }

    /**
     * @return total number of files this task receives/streams.
     */
    public abstract int getTotalNumberOfFiles();

    /**
     * @return total bytes expected to receive
     */
    public abstract long getTotalSize();

    /**
     * Abort the task.
     * Subclass should implement cleaning up resources.
     */
    public abstract void abort();

    /**
     * @return StreamSummary that describes this task
     */
    public StreamSummary getSummary()
    {
        return new StreamSummary(cfId, getTotalNumberOfFiles(), getTotalSize());
    }
}
