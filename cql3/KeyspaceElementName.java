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

import java.util.Locale;

/**
 * Base class for the names of the keyspace elements (e.g. table, index ...)
 */
abstract class KeyspaceElementName
{
    /**
     * The keyspace name as stored internally.
     */
    private String ksName;

    /**
     * Sets the keyspace.
     *
     * @param ks the keyspace name
     * @param keepCase <code>true</code> if the case must be kept, <code>false</code> otherwise.
     */
    public final void setKeyspace(String ks, boolean keepCase)
    {
        ksName = toInternalName(ks, keepCase);
    }

    /**
     * Checks if the keyspace is specified.
     * @return <code>true</code> if the keyspace is specified, <code>false</code> otherwise.
     */
    public final boolean hasKeyspace()
    {
        return ksName != null;
    }

    public final String getKeyspace()
    {
        return ksName;
    }

    /**
     * Converts the specified name into the name used internally.
     *
     * @param name the name
     * @param keepCase <code>true</code> if the case must be kept, <code>false</code> otherwise.
     * @return the name used internally.
     */
    protected static String toInternalName(String name, boolean keepCase)
    {
        return keepCase ? name : name.toLowerCase(Locale.US);
    }

    @Override
    public String toString()
    {
        return hasKeyspace() ? (getKeyspace() + ".") : "";
    }
}
