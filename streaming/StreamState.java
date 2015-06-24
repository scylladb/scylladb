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

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Current snapshot of streaming progress.
 */
public class StreamState implements Serializable
{
    public final UUID planId;
    public final String description;
    public final Set<SessionInfo> sessions;

    public StreamState(UUID planId, String description, Set<SessionInfo> sessions)
    {
        this.planId = planId;
        this.description = description;
        this.sessions = sessions;
    }

    public boolean hasFailedSession()
    {
        return Iterables.any(sessions, new Predicate<SessionInfo>()
        {
            public boolean apply(SessionInfo session)
            {
                return session.isFailed();
            }
        });
    }
}
