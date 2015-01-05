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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.TypeSerializer;

public class ReversedType<T> extends AbstractType<T>
{
    // interning instances
    private static final Map<AbstractType<?>, ReversedType> instances = new HashMap<AbstractType<?>, ReversedType>();

    public final AbstractType<T> baseType;

    public static <T> ReversedType<T> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> types = parser.getTypeParameters();
        if (types.size() != 1)
            throw new ConfigurationException("ReversedType takes exactly one argument, " + types.size() + " given");
        return getInstance((AbstractType<T>) types.get(0));
    }

    public static synchronized <T> ReversedType<T> getInstance(AbstractType<T> baseType)
    {
        ReversedType<T> type = instances.get(baseType);
        if (type == null)
        {
            type = new ReversedType<T>(baseType);
            instances.put(baseType, type);
        }
        return type;
    }

    private ReversedType(AbstractType<T> baseType)
    {
        this.baseType = baseType;
    }

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        // An empty byte buffer is always smaller
        if (o1.remaining() == 0)
        {
            return o2.remaining() == 0 ? 0 : -1;
        }
        if (o2.remaining() == 0)
        {
            return 1;
        }

        return baseType.compare(o2, o1);
    }

    public String getString(ByteBuffer bytes)
    {
        return baseType.getString(bytes);
    }

    public ByteBuffer fromString(String source)
    {
        return baseType.fromString(source);
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> otherType)
    {
        if (!(otherType instanceof ReversedType))
            return false;

        return this.baseType.isCompatibleWith(((ReversedType) otherType).baseType);
    }

    @Override
    public boolean isValueCompatibleWith(AbstractType<?> otherType)
    {
        return this.baseType.isValueCompatibleWith(otherType);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return baseType.asCQL3Type();
    }

    public TypeSerializer<T> getSerializer()
    {
        return baseType.getSerializer();
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "(" + baseType + ")";
    }
}
