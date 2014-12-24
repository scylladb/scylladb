/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3.selection;

import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class RawSelector
{
    public final Selectable.Raw selectable;
    public final ColumnIdentifier alias;

    public RawSelector(Selectable.Raw selectable, ColumnIdentifier alias)
    {
        this.selectable = selectable;
        this.alias = alias;
    }

    /**
     * Converts the specified list of <code>RawSelector</code>s into a list of <code>Selectable</code>s.
     *
     * @param raws the <code>RawSelector</code>s to converts.
     * @return a list of <code>Selectable</code>s
     */
    public static List<Selectable> toSelectables(List<RawSelector> raws, final CFMetaData cfm)
    {
        return Lists.transform(raws, new Function<RawSelector, Selectable>()
        {
            public Selectable apply(RawSelector raw)
            {
                return raw.selectable.prepare(cfm);
            }
        });
    }

    public boolean processesSelection()
    {
        return selectable.processesSelection();
    }
}
