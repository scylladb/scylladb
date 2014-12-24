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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;

public class UTName
{
    private String ksName;
    private final ColumnIdentifier utName;

    public UTName(ColumnIdentifier ksName, ColumnIdentifier utName)
    {
        this.ksName = ksName == null ? null : ksName.toString();
        this.utName = utName;
    }

    public boolean hasKeyspace()
    {
        return ksName != null;
    }

    public void setKeyspace(String keyspace)
    {
        this.ksName = keyspace;
    }

    public String getKeyspace()
    {
        return ksName;
    }

    public ByteBuffer getUserTypeName()
    {
        return utName.bytes;
    }

    public String getStringTypeName()
    {
        return utName.toString();
    }

    @Override
    public String toString()
    {
        return (hasKeyspace() ? (ksName + ".") : "") + utName;
    }
}
