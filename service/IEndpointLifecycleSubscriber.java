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
package org.apache.cassandra.service;

import java.net.InetAddress;

/**
 * Interface on which interested parties can be notified of high level endpoint
 * state changes.
 *
 * Note that while IEndpointStateChangeSubscriber notify about gossip related
 * changes (IEndpointStateChangeSubscriber.onJoin() is called when a node join
 * gossip), this interface allows to be notified about higher level events.
 */
public interface IEndpointLifecycleSubscriber
{
    /**
     * Called when a new node joins the cluster, i.e. either has just been
     * bootstrapped or "instajoins".
     *
     * @param endpoint the newly added endpoint.
     */
    public void onJoinCluster(InetAddress endpoint);

    /**
     * Called when a new node leave the cluster (decommission or removeToken).
     *
     * @param endpoint the endpoint that is leaving.
     */
    public void onLeaveCluster(InetAddress endpoint);

    /**
     * Called when a node is marked UP.
     *
     * @param endpoint the endpoint marked UP.
     */
    public void onUp(InetAddress endpoint);

    /**
     * Called when a node is marked DOWN.
     *
     * @param endpoint the endpoint marked DOWN.
     */
    public void onDown(InetAddress endpoint);

    /**
     * Called when a node has moved (to a new token).
     *
     * @param endpoint the endpoint that has moved.
     */
    public void onMove(InetAddress endpoint);
}
