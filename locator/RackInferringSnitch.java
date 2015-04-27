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
 * A simple endpoint snitch implementation that assumes datacenter and rack information is encoded
 * in the 2nd and 3rd octets of the ip address, respectively.
 */
public class RackInferringSnitch extends AbstractNetworkTopologySnitch
{
    public String getRack(InetAddress endpoint)
    {
        return Integer.toString(endpoint.getAddress()[2] & 0xFF, 10);
    }

    public String getDatacenter(InetAddress endpoint)
    {
        return Integer.toString(endpoint.getAddress()[1] & 0xFF, 10);
    }
}
