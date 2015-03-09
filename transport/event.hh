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

#include "core/sstring.hh"

#include <experimental/optional>

namespace transport {

class event {
public:
    enum class event_type { TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE };

    const event_type type;

private:
    event(const event_type& type_)
        : type{type_}
    { }
public:

#if 0
    public static Event deserialize(ByteBuf cb, int version)
    {
        switch (CBUtil.readEnumValue(Type.class, cb))
        {
            case TOPOLOGY_CHANGE:
                return TopologyChange.deserializeEvent(cb, version);
            case STATUS_CHANGE:
                return StatusChange.deserializeEvent(cb, version);
            case SCHEMA_CHANGE:
                return SchemaChange.deserializeEvent(cb, version);
        }
        throw new AssertionError();
    }

    public void serialize(ByteBuf dest, int version)
    {
        CBUtil.writeEnumValue(type, dest);
        serializeEvent(dest, version);
    }

    public int serializedSize(int version)
    {
        return CBUtil.sizeOfEnumValue(type) + eventSerializedSize(version);
    }

    protected abstract void serializeEvent(ByteBuf dest, int version);
    protected abstract int eventSerializedSize(int version);
#endif
    class schema_change;
};

#if 0
    public static class TopologyChange extends Event
    {
        public enum Change { NEW_NODE, REMOVED_NODE, MOVED_NODE }

        public final Change change;
        public final InetSocketAddress node;

        private TopologyChange(Change change, InetSocketAddress node)
        {
            super(Type.TOPOLOGY_CHANGE);
            this.change = change;
            this.node = node;
        }

        public static TopologyChange newNode(InetAddress host, int port)
        {
            return new TopologyChange(Change.NEW_NODE, new InetSocketAddress(host, port));
        }

        public static TopologyChange removedNode(InetAddress host, int port)
        {
            return new TopologyChange(Change.REMOVED_NODE, new InetSocketAddress(host, port));
        }

        public static TopologyChange movedNode(InetAddress host, int port)
        {
            return new TopologyChange(Change.MOVED_NODE, new InetSocketAddress(host, port));
        }

        // Assumes the type has already been deserialized
        private static TopologyChange deserializeEvent(ByteBuf cb, int version)
        {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            InetSocketAddress node = CBUtil.readInet(cb);
            return new TopologyChange(change, node);
        }

        protected void serializeEvent(ByteBuf dest, int version)
        {
            CBUtil.writeEnumValue(change, dest);
            CBUtil.writeInet(node, dest);
        }

        protected int eventSerializedSize(int version)
        {
            return CBUtil.sizeOfEnumValue(change) + CBUtil.sizeOfInet(node);
        }

        @Override
        public String toString()
        {
            return change + " " + node;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(change, node);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof TopologyChange))
                return false;

            TopologyChange tpc = (TopologyChange)other;
            return Objects.equal(change, tpc.change)
                && Objects.equal(node, tpc.node);
        }
    }

    public static class StatusChange extends Event
    {
        public enum Status { UP, DOWN }

        public final Status status;
        public final InetSocketAddress node;

        private StatusChange(Status status, InetSocketAddress node)
        {
            super(Type.STATUS_CHANGE);
            this.status = status;
            this.node = node;
        }

        public static StatusChange nodeUp(InetAddress host, int port)
        {
            return new StatusChange(Status.UP, new InetSocketAddress(host, port));
        }

        public static StatusChange nodeDown(InetAddress host, int port)
        {
            return new StatusChange(Status.DOWN, new InetSocketAddress(host, port));
        }

        // Assumes the type has already been deserialized
        private static StatusChange deserializeEvent(ByteBuf cb, int version)
        {
            Status status = CBUtil.readEnumValue(Status.class, cb);
            InetSocketAddress node = CBUtil.readInet(cb);
            return new StatusChange(status, node);
        }

        protected void serializeEvent(ByteBuf dest, int version)
        {
            CBUtil.writeEnumValue(status, dest);
            CBUtil.writeInet(node, dest);
        }

        protected int eventSerializedSize(int version)
        {
            return CBUtil.sizeOfEnumValue(status) + CBUtil.sizeOfInet(node);
        }

        @Override
        public String toString()
        {
            return status + " " + node;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(status, node);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof StatusChange))
                return false;

            StatusChange stc = (StatusChange)other;
            return Objects.equal(status, stc.status)
                && Objects.equal(node, stc.node);
        }
    }
#endif

    class event::schema_change : public event {
    public:
        enum class change_type { CREATED, UPDATED, DROPPED };
        enum class target_type { KEYSPACE, TABLE, TYPE };

        const change_type change;
        const target_type target;
        const sstring keyspace;
        const std::experimental::optional<sstring> table_or_type_or_function;

        schema_change(const change_type change_, const target_type target_, const sstring& keyspace_, const std::experimental::optional<sstring>& table_or_type_or_function_)
            : event{event_type::SCHEMA_CHANGE}
            , change{change_}
            , target{target_}
            , keyspace{keyspace_}
            , table_or_type_or_function{table_or_type_or_function_}
        {
#if 0
            if (target != Target.KEYSPACE)
                assert this.tableOrTypeOrFunction != null : "Table or type should be set for non-keyspace schema change events";
#endif
        }

        schema_change(const change_type change_, const sstring keyspace_)
            : schema_change{change_, target_type::KEYSPACE, keyspace_, std::experimental::optional<sstring>{}}
        { }
#if 0
        // Assumes the type has already been deserialized
        public static SchemaChange deserializeEvent(ByteBuf cb, int version)
        {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            if (version >= 3)
            {
                Target target = CBUtil.readEnumValue(Target.class, cb);
                String keyspace = CBUtil.readString(cb);
                String tableOrType = target == Target.KEYSPACE ? null : CBUtil.readString(cb);
                return new SchemaChange(change, target, keyspace, tableOrType);
            }
            else
            {
                String keyspace = CBUtil.readString(cb);
                String table = CBUtil.readString(cb);
                return new SchemaChange(change, table.isEmpty() ? Target.KEYSPACE : Target.TABLE, keyspace, table.isEmpty() ? null : table);
            }
        }

        public void serializeEvent(ByteBuf dest, int version)
        {
            if (version >= 3)
            {
                CBUtil.writeEnumValue(change, dest);
                CBUtil.writeEnumValue(target, dest);
                CBUtil.writeString(keyspace, dest);
                if (target != Target.KEYSPACE)
                    CBUtil.writeString(tableOrTypeOrFunction, dest);
            }
            else
            {
                if (target == Target.TYPE)
                {
                    // For the v1/v2 protocol, we have no way to represent type changes, so we simply say the keyspace
                    // was updated.  See CASSANDRA-7617.
                    CBUtil.writeEnumValue(Change.UPDATED, dest);
                    CBUtil.writeString(keyspace, dest);
                    CBUtil.writeString("", dest);
                }
                else
                {
                    CBUtil.writeEnumValue(change, dest);
                    CBUtil.writeString(keyspace, dest);
                    CBUtil.writeString(target == Target.KEYSPACE ? "" : tableOrTypeOrFunction, dest);
                }
            }
        }

        public int eventSerializedSize(int version)
        {
            if (version >= 3)
            {
                int size = CBUtil.sizeOfEnumValue(change)
                         + CBUtil.sizeOfEnumValue(target)
                         + CBUtil.sizeOfString(keyspace);

                if (target != Target.KEYSPACE)
                    size += CBUtil.sizeOfString(tableOrTypeOrFunction);

                return size;
            }
            else
            {
                if (target == Target.TYPE)
                {
                    return CBUtil.sizeOfEnumValue(Change.UPDATED)
                         + CBUtil.sizeOfString(keyspace)
                         + CBUtil.sizeOfString("");
                }
                return CBUtil.sizeOfEnumValue(change)
                     + CBUtil.sizeOfString(keyspace)
                     + CBUtil.sizeOfString(target == Target.KEYSPACE ? "" : tableOrTypeOrFunction);
            }
        }

        @Override
        public String toString()
        {
            return change + " " + target + " " + keyspace + (tableOrTypeOrFunction == null ? "" : "." + tableOrTypeOrFunction);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(change, target, keyspace, tableOrTypeOrFunction);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof SchemaChange))
                return false;

            SchemaChange scc = (SchemaChange)other;
            return Objects.equal(change, scc.change)
                && Objects.equal(target, scc.target)
                && Objects.equal(keyspace, scc.keyspace)
                && Objects.equal(tableOrTypeOrFunction, scc.tableOrTypeOrFunction);
        }
#endif
    };

}
