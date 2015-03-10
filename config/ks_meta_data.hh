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

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "config/ut_meta_data.hh"
#include "schema.hh"

#include "core/shared_ptr.hh"

namespace config {

class ks_meta_data final {
public:
#if 0
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> strategyClass;
    public final Map<String, String> strategyOptions;
    private final Map<String, CFMetaData> cfMetaData;
    public final boolean durableWrites;

    public final UTMetaData userTypes;

    public KSMetaData(String name,
                      Class<? extends AbstractReplicationStrategy> strategyClass,
                      Map<String, String> strategyOptions,
                      boolean durableWrites)
    {
        this(name, strategyClass, strategyOptions, durableWrites, Collections.<CFMetaData>emptyList(), new UTMetaData());
    }

    public KSMetaData(String name,
                      Class<? extends AbstractReplicationStrategy> strategyClass,
                      Map<String, String> strategyOptions,
                      boolean durableWrites,
                      Iterable<CFMetaData> cfDefs)
    {
        this(name, strategyClass, strategyOptions, durableWrites, cfDefs, new UTMetaData());
    }
#endif
    ks_meta_data(sstring name,
                       sstring strategy_name,
                       std::unordered_map<sstring, sstring> strategy_options,
                       bool durable_writes,
                       std::vector<schema_ptr> cf_defs,
                       shared_ptr<ut_meta_data> user_types)
    {
#if 0
        this.name = name;
        this.strategyClass = strategyClass == null ? NetworkTopologyStrategy.class : strategyClass;
        this.strategyOptions = strategyOptions;
        Map<String, CFMetaData> cfmap = new HashMap<>();
        for (CFMetaData cfm : cfDefs)
            cfmap.put(cfm.cfName, cfm);
        this.cfMetaData = Collections.unmodifiableMap(cfmap);
        this.durableWrites = durableWrites;
        this.userTypes = userTypes;
#endif
    }

    // For new user created keyspaces (through CQL)
    static lw_shared_ptr<ks_meta_data> new_keyspace(sstring name, sstring strategy_name, std::unordered_map<sstring, sstring> options, bool durable_writes) {
#if 0
        Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(strategyName);
        if (cls.equals(LocalStrategy.class))
            throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");
#endif
        return new_keyspace(name, strategy_name, options, durable_writes, std::vector<schema_ptr>{});
    }

    static lw_shared_ptr<ks_meta_data> new_keyspace(sstring name, sstring strategy_name, std::unordered_map<sstring, sstring> options, bool durables_writes, std::vector<schema_ptr> cf_defs)
    {
        return ::make_lw_shared<ks_meta_data>(name, strategy_name, options, durables_writes, cf_defs, ::make_shared<ut_meta_data>());
    }

#if 0
    public KSMetaData cloneWithTableRemoved(CFMetaData table)
    {
        // clone ksm but do not include the new table
        List<CFMetaData> newTables = new ArrayList<>(cfMetaData().values());
        newTables.remove(table);
        assert newTables.size() == cfMetaData().size() - 1;
        return cloneWith(newTables, userTypes);
    }

    public KSMetaData cloneWithTableAdded(CFMetaData table)
    {
        // clone ksm but include the new table
        List<CFMetaData> newTables = new ArrayList<>(cfMetaData().values());
        newTables.add(table);
        assert newTables.size() == cfMetaData().size() + 1;
        return cloneWith(newTables, userTypes);
    }

    public KSMetaData cloneWith(Iterable<CFMetaData> tables, UTMetaData types)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, durableWrites, tables, types);
    }

    public static KSMetaData testMetadata(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, true, Arrays.asList(cfDefs));
    }

    public static KSMetaData testMetadataNotDurable(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, false, Arrays.asList(cfDefs));
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, strategyClass, strategyOptions, cfMetaData, durableWrites, userTypes);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof KSMetaData))
            return false;

        KSMetaData other = (KSMetaData) o;

        return Objects.equal(name, other.name)
            && Objects.equal(strategyClass, other.strategyClass)
            && Objects.equal(strategyOptions, other.strategyOptions)
            && Objects.equal(cfMetaData, other.cfMetaData)
            && Objects.equal(durableWrites, other.durableWrites)
            && Objects.equal(userTypes, other.userTypes);
    }

    public Map<String, CFMetaData> cfMetaData()
    {
        return cfMetaData;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("strategyClass", strategyClass.getSimpleName())
                      .add("strategyOptions", strategyOptions)
                      .add("cfMetaData", cfMetaData)
                      .add("durableWrites", durableWrites)
                      .add("userTypes", userTypes)
                      .toString();
    }

    public static Map<String,String> optsWithRF(final Integer rf)
    {
        return Collections.singletonMap("replication_factor", rf.toString());
    }

    public KSMetaData validate() throws ConfigurationException
    {
        if (!CFMetaData.isNameValid(name))
            throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", Schema.NAME_LENGTH, name));

        // Attempt to instantiate the ARS, which will throw a ConfigException if the strategy_options aren't fully formed
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        IEndpointSnitch eps = DatabaseDescriptor.getEndpointSnitch();
        AbstractReplicationStrategy.validateReplicationStrategy(name, strategyClass, tmd, eps, strategyOptions);

        for (CFMetaData cfm : cfMetaData.values())
            cfm.validate();

        return this;
    }
#endif
};

}
