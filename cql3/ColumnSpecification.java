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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;

public class ColumnSpecification
{
    public final String ksName;
    public final String cfName;
    public final ColumnIdentifier name;
    public final AbstractType<?> type;

    public ColumnSpecification(String ksName, String cfName, ColumnIdentifier name, AbstractType<?> type)
    {
        this.ksName = ksName;
        this.cfName = cfName;
        this.name = name;
        this.type = type;
    }

    /**
     * Returns a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     *
     * @param alias the column alias
     * @return a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     */
    public ColumnSpecification withAlias(ColumnIdentifier alias)
    {
        return new ColumnSpecification(ksName, cfName, alias, type);
    }
    
    public boolean isReversedType()
    {
        return type instanceof ReversedType;
    }
}
