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
package org.apache.cassandra.locator;

import java.net.InetAddress;

/**
 * An endpoint snitch tells Cassandra information about network topology that it can use to route
 * requests more efficiently.
 */
public abstract class AbstractNetworkTopologySnitch extends AbstractEndpointSnitch
{
    /**
     * Return the rack for which an endpoint resides in
     * @param endpoint a specified endpoint
     * @return string of rack
     */
    abstract public String getRack(InetAddress endpoint);

    /**
     * Return the data center for which an endpoint resides in
     * @param endpoint a specified endpoint
     * @return string of data center
     */
    abstract public String getDatacenter(InetAddress endpoint);

    public int compareEndpoints(InetAddress address, InetAddress a1, InetAddress a2)
    {
        if (address.equals(a1) && !address.equals(a2))
            return -1;
        if (address.equals(a2) && !address.equals(a1))
            return 1;

        String addressDatacenter = getDatacenter(address);
        String a1Datacenter = getDatacenter(a1);
        String a2Datacenter = getDatacenter(a2);
        if (addressDatacenter.equals(a1Datacenter) && !addressDatacenter.equals(a2Datacenter))
            return -1;
        if (addressDatacenter.equals(a2Datacenter) && !addressDatacenter.equals(a1Datacenter))
            return 1;

        String addressRack = getRack(address);
        String a1Rack = getRack(a1);
        String a2Rack = getRack(a2);
        if (addressRack.equals(a1Rack) && !addressRack.equals(a2Rack))
            return -1;
        if (addressRack.equals(a2Rack) && !addressRack.equals(a1Rack))
            return 1;
        return 0;
    }
}
