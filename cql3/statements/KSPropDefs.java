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
package org.apache.cassandra.cql3.statements;

import java.util.*;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.*;

public class KSPropDefs extends PropertyDefinitions
{
    public static final String KW_DURABLE_WRITES = "durable_writes";
    public static final String KW_REPLICATION = "replication";

    public static final String REPLICATION_STRATEGY_CLASS_KEY = "class";

    public static final Set<String> keywords = new HashSet<>();
    public static final Set<String> obsoleteKeywords = new HashSet<>();

    static
    {
        keywords.add(KW_DURABLE_WRITES);
        keywords.add(KW_REPLICATION);
    }

    private String strategyClass;

    public void validate() throws SyntaxException
    {
        // Skip validation if the strategy class is already set as it means we've alreayd
        // prepared (and redoing it would set strategyClass back to null, which we don't want)
        if (strategyClass != null)
            return;

        validate(keywords, obsoleteKeywords);

        Map<String, String> replicationOptions = getReplicationOptions();
        if (!replicationOptions.isEmpty())
        {
            strategyClass = replicationOptions.get(REPLICATION_STRATEGY_CLASS_KEY);
            replicationOptions.remove(REPLICATION_STRATEGY_CLASS_KEY);
        }
    }

    public Map<String, String> getReplicationOptions() throws SyntaxException
    {
        Map<String, String> replicationOptions = getMap(KW_REPLICATION);
        if (replicationOptions == null)
            return Collections.emptyMap();
        return replicationOptions;
    }

    public String getReplicationStrategyClass()
    {
        return strategyClass;
    }

    public KSMetaData asKSMetadata(String ksName) throws RequestValidationException
    {
        return KSMetaData.newKeyspace(ksName, getReplicationStrategyClass(), getReplicationOptions(), getBoolean(KW_DURABLE_WRITES, true));
    }

    public KSMetaData asKSMetadataUpdate(KSMetaData old) throws RequestValidationException
    {
        String sClass = strategyClass;
        Map<String, String> sOptions = getReplicationOptions();
        if (sClass == null)
        {
            sClass = old.strategyClass.getName();
            sOptions = old.strategyOptions;
        }
        return KSMetaData.newKeyspace(old.name, sClass, sOptions, getBoolean(KW_DURABLE_WRITES, old.durableWrites));
    }
}
