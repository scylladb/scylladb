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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface CQL3Type
{
    static final Logger logger = LoggerFactory.getLogger(CQL3Type.class);

    public boolean isCollection();
    public AbstractType<?> getType();

    public enum Native implements CQL3Type
    {
        ASCII    (AsciiType.instance),
        BIGINT   (LongType.instance),
        BLOB     (BytesType.instance),
        BOOLEAN  (BooleanType.instance),
        COUNTER  (CounterColumnType.instance),
        DECIMAL  (DecimalType.instance),
        DOUBLE   (DoubleType.instance),
        FLOAT    (FloatType.instance),
        INET     (InetAddressType.instance),
        INT      (Int32Type.instance),
        TEXT     (UTF8Type.instance),
        TIMESTAMP(TimestampType.instance),
        UUID     (UUIDType.instance),
        VARCHAR  (UTF8Type.instance),
        VARINT   (IntegerType.instance),
        TIMEUUID (TimeUUIDType.instance);

        private final AbstractType<?> type;

        private Native(AbstractType<?> type)
        {
            this.type = type;
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }

    public static class Custom implements CQL3Type
    {
        private final AbstractType<?> type;

        public Custom(AbstractType<?> type)
        {
            this.type = type;
        }

        public Custom(String className) throws SyntaxException, ConfigurationException
        {
            this(TypeParser.parse(className));
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Custom))
                return false;

            Custom that = (Custom)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return "'" + type + "'";
        }
    }

    public static class Collection implements CQL3Type
    {
        private final CollectionType type;

        public Collection(CollectionType type)
        {
            this.type = type;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        public boolean isCollection()
        {
            return true;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Collection))
                return false;

            Collection that = (Collection)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            boolean isFrozen = !this.type.isMultiCell();
            StringBuilder sb = new StringBuilder(isFrozen ? "frozen<" : "");
            switch (type.kind)
            {
                case LIST:
                    AbstractType<?> listType = ((ListType)type).getElementsType();
                    sb.append("list<").append(listType.asCQL3Type());
                    break;
                case SET:
                    AbstractType<?> setType = ((SetType)type).getElementsType();
                    sb.append("set<").append(setType.asCQL3Type());
                    break;
                case MAP:
                    AbstractType<?> keysType = ((MapType)type).getKeysType();
                    AbstractType<?> valuesType = ((MapType)type).getValuesType();
                    sb.append("map<").append(keysType.asCQL3Type()).append(", ").append(valuesType.asCQL3Type());
                    break;
                default:
                    throw new AssertionError();
            }
            sb.append(">");
            if (isFrozen)
                sb.append(">");
            return sb.toString();
        }
    }

    public static class UserDefined implements CQL3Type
    {
        // Keeping this separatly from type just to simplify toString()
        private final String name;
        private final UserType type;

        private UserDefined(String name, UserType type)
        {
            this.name = name;
            this.type = type;
        }

        public static UserDefined create(UserType type)
        {
            return new UserDefined(UTF8Type.instance.compose(type.name), type);
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof UserDefined))
                return false;

            UserDefined that = (UserDefined)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public static class Tuple implements CQL3Type
    {
        private final TupleType type;

        private Tuple(TupleType type)
        {
            this.type = type;
        }

        public static Tuple create(TupleType type)
        {
            return new Tuple(type);
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Tuple))
                return false;

            Tuple that = (Tuple)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("tuple<");
            for (int i = 0; i < type.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(type.type(i).asCQL3Type());
            }
            sb.append(">");
            return sb.toString();
        }
    }

    // For UserTypes, we need to know the current keyspace to resolve the
    // actual type used, so Raw is a "not yet prepared" CQL3Type.
    public abstract class Raw
    {
        protected boolean frozen = false;

        protected abstract boolean supportsFreezing();

        public boolean isCollection()
        {
            return false;
        }

        public boolean isCounter()
        {
            return false;
        }

        public String keyspace()
        {
            return null;
        }

        public void freeze() throws InvalidRequestException
        {
            String message = String.format("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", this);
            throw new InvalidRequestException(message);
        }

        public abstract CQL3Type prepare(String keyspace) throws InvalidRequestException;

        public static Raw from(CQL3Type type)
        {
            return new RawType(type);
        }

        public static Raw userType(UTName name)
        {
            return new RawUT(name);
        }

        public static Raw map(CQL3Type.Raw t1, CQL3Type.Raw t2)
        {
            return new RawCollection(CollectionType.Kind.MAP, t1, t2);
        }

        public static Raw list(CQL3Type.Raw t)
        {
            return new RawCollection(CollectionType.Kind.LIST, null, t);
        }

        public static Raw set(CQL3Type.Raw t)
        {
            return new RawCollection(CollectionType.Kind.SET, null, t);
        }

        public static Raw tuple(List<CQL3Type.Raw> ts)
        {
            return new RawTuple(ts);
        }

        public static Raw frozen(CQL3Type.Raw t) throws InvalidRequestException
        {
            t.freeze();
            return t;
        }

        private static class RawType extends Raw
        {
            private CQL3Type type;

            private RawType(CQL3Type type)
            {
                this.type = type;
            }

            public CQL3Type prepare(String keyspace) throws InvalidRequestException
            {
                return type;
            }

            protected boolean supportsFreezing()
            {
                return false;
            }

            public boolean isCounter()
            {
                return type == Native.COUNTER;
            }

            @Override
            public String toString()
            {
                return type.toString();
            }
        }

        private static class RawCollection extends Raw
        {
            private final CollectionType.Kind kind;
            private final CQL3Type.Raw keys;
            private final CQL3Type.Raw values;

            private RawCollection(CollectionType.Kind kind, CQL3Type.Raw keys, CQL3Type.Raw values)
            {
                this.kind = kind;
                this.keys = keys;
                this.values = values;
            }

            public void freeze() throws InvalidRequestException
            {
                if (keys != null && keys.supportsFreezing())
                    keys.freeze();
                if (values != null && values.supportsFreezing())
                    values.freeze();
                frozen = true;
            }

            protected boolean supportsFreezing()
            {
                return true;
            }

            public boolean isCollection()
            {
                return true;
            }

            public CQL3Type prepare(String keyspace) throws InvalidRequestException
            {
                assert values != null : "Got null values type for a collection";

                if (!frozen && values.supportsFreezing() && !values.frozen)
                    throw new InvalidRequestException("Non-frozen collections are not allowed inside collections: " + this);
                if (values.isCounter())
                    throw new InvalidRequestException("Counters are not allowed inside collections: " + this);

                if (keys != null)
                {
                    if (!frozen && keys.supportsFreezing() && !keys.frozen)
                        throw new InvalidRequestException("Non-frozen collections are not allowed inside collections: " + this);
                }

                switch (kind)
                {
                    case LIST:
                        return new Collection(ListType.getInstance(values.prepare(keyspace).getType(), !frozen));
                    case SET:
                        return new Collection(SetType.getInstance(values.prepare(keyspace).getType(), !frozen));
                    case MAP:
                        assert keys != null : "Got null keys type for a collection";
                        return new Collection(MapType.getInstance(keys.prepare(keyspace).getType(), values.prepare(keyspace).getType(), !frozen));
                }
                throw new AssertionError();
            }

            @Override
            public String toString()
            {
                String start = frozen? "frozen<" : "";
                String end = frozen ? ">" : "";
                switch (kind)
                {
                    case LIST: return start + "list<" + values + ">" + end;
                    case SET:  return start + "set<" + values + ">" + end;
                    case MAP:  return start + "map<" + keys + ", " + values + ">" + end;
                }
                throw new AssertionError();
            }
        }

        private static class RawUT extends Raw
        {
            private final UTName name;

            private RawUT(UTName name)
            {
                this.name = name;
            }

            public String keyspace()
            {
                return name.getKeyspace();
            }

            public void freeze()
            {
                frozen = true;
            }

            public CQL3Type prepare(String keyspace) throws InvalidRequestException
            {
                if (name.hasKeyspace())
                {
                    // The provided keyspace is the one of the current statement this is part of. If it's different from the keyspace of
                    // the UTName, we reject since we want to limit user types to their own keyspace (see #6643)
                    if (keyspace != null && !keyspace.equals(name.getKeyspace()))
                        throw new InvalidRequestException(String.format("Statement on keyspace %s cannot refer to a user type in keyspace %s; "
                                                                        + "user types can only be used in the keyspace they are defined in",
                                                                        keyspace, name.getKeyspace()));
                }
                else
                {
                    name.setKeyspace(keyspace);
                }

                KSMetaData ksm = Schema.instance.getKSMetaData(name.getKeyspace());
                if (ksm == null)
                    throw new InvalidRequestException("Unknown keyspace " + name.getKeyspace());
                UserType type = ksm.userTypes.getType(name.getUserTypeName());
                if (type == null)
                    throw new InvalidRequestException("Unknown type " + name);

                if (!frozen)
                    throw new InvalidRequestException("Non-frozen User-Defined types are not supported, please use frozen<>");

                return new UserDefined(name.toString(), type);
            }

            protected boolean supportsFreezing()
            {
                return true;
            }

            @Override
            public String toString()
            {
                return name.toString();
            }
        }

        private static class RawTuple extends Raw
        {
            private final List<CQL3Type.Raw> types;

            private RawTuple(List<CQL3Type.Raw> types)
            {
                this.types = types;
            }

            protected boolean supportsFreezing()
            {
                return true;
            }

            public boolean isCollection()
            {
                return false;
            }

            public void freeze() throws InvalidRequestException
            {
                for (CQL3Type.Raw t : types)
                {
                    if (t.supportsFreezing())
                        t.freeze();
                }
                frozen = true;
            }

            public CQL3Type prepare(String keyspace) throws InvalidRequestException
            {
                if (!frozen)
                    freeze();

                List<AbstractType<?>> ts = new ArrayList<>(types.size());
                for (CQL3Type.Raw t : types)
                {
                    if (t.isCounter())
                        throw new InvalidRequestException("Counters are not allowed inside tuples");

                    ts.add(t.prepare(keyspace).getType());
                }
                return new Tuple(new TupleType(ts));
            }

            @Override
            public String toString()
            {
                StringBuilder sb = new StringBuilder();
                sb.append("tuple<");
                for (int i = 0; i < types.size(); i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(types.get(i));
                }
                sb.append(">");
                return sb.toString();
            }
        }
    }
}
