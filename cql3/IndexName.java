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

public final class IndexName extends KeyspaceElementName
{
    private String idxName;

    public void setIndex(String idx, boolean keepCase)
    {
        idxName = toInternalName(idx, keepCase);
    }

    public String getIdx()
    {
        return idxName;
    }

    public CFName getCfName()
    {
        CFName cfName = new CFName();
        if (hasKeyspace())
            cfName.setKeyspace(getKeyspace(), true);
    	return cfName;
    }

    @Override
    public String toString()
    {
        return super.toString() + idxName;
    }
}
