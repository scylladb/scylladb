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

import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.index.SecondaryIndex;

public class IndexTarget
{
    public final ColumnIdentifier column;
    public final TargetType type;

    private IndexTarget(ColumnIdentifier column, TargetType type)
    {
        this.column = column;
        this.type = type;
    }

    public static class Raw
    {
        private final ColumnIdentifier.Raw column;
        private final TargetType type;

        private Raw(ColumnIdentifier.Raw column, TargetType type)
        {
            this.column = column;
            this.type = type;
        }

        public static Raw valuesOf(ColumnIdentifier.Raw c)
        {
            return new Raw(c, TargetType.VALUES);
        }

        public static Raw keysOf(ColumnIdentifier.Raw c)
        {
            return new Raw(c, TargetType.KEYS);
        }

        public static Raw keysAndValuesOf(ColumnIdentifier.Raw c)
        {
            return new Raw(c, TargetType.KEYS_AND_VALUES);
        }

        public static Raw fullCollection(ColumnIdentifier.Raw c)
        {
            return new Raw(c, TargetType.FULL);
        }

        public IndexTarget prepare(CFMetaData cfm)
        {
            return new IndexTarget(column.prepare(cfm), type);
        }
    }

    public static enum TargetType
    {
        VALUES, KEYS, KEYS_AND_VALUES, FULL;

        public String toString()
        {
            switch (this)
            {
                case KEYS: return "keys";
                case KEYS_AND_VALUES: return "entries";
                case FULL: return "full";
                default: return "values";
            }
        }

        public String indexOption()
        {
            switch (this)
            {
                case KEYS: return SecondaryIndex.INDEX_KEYS_OPTION_NAME;
                case KEYS_AND_VALUES: return SecondaryIndex.INDEX_ENTRIES_OPTION_NAME;
                case VALUES: return SecondaryIndex.INDEX_VALUES_OPTION_NAME;
                default: throw new AssertionError();
            }
        }

        public static TargetType fromColumnDefinition(ColumnDefinition cd)
        {
            Map<String, String> options = cd.getIndexOptions();
            if (options.containsKey(SecondaryIndex.INDEX_KEYS_OPTION_NAME))
                return KEYS;
            else if (options.containsKey(SecondaryIndex.INDEX_ENTRIES_OPTION_NAME))
                return KEYS_AND_VALUES;
            else if (cd.type.isCollection() && !cd.type.isMultiCell())
                return FULL;
            else
                return VALUES;
        }
    }
}
