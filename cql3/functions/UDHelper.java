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
package org.apache.cassandra.cql3.functions;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Helper class for User Defined Functions + Aggregates.
 */
public final class UDHelper
{
    protected static final Logger logger = LoggerFactory.getLogger(UDHelper.class);

    // TODO make these c'tors and methods public in Java-Driver - see https://datastax-oss.atlassian.net/browse/JAVA-502
    static final MethodHandle methodParseOne;
    static
    {
        try
        {
            Class<?> cls = Class.forName("com.datastax.driver.core.CassandraTypeParser");
            Method m = cls.getDeclaredMethod("parseOne", String.class);
            m.setAccessible(true);
            methodParseOne = MethodHandles.lookup().unreflect(m);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Construct an array containing the Java classes for the given Java Driver {@link com.datastax.driver.core.DataType}s.
     *
     * @param dataTypes array with UDF argument types
     * @return array of same size with UDF arguments
     */
    public static Class<?>[] javaTypes(DataType[] dataTypes)
    {
        Class<?> paramTypes[] = new Class[dataTypes.length];
        for (int i = 0; i < paramTypes.length; i++)
            paramTypes[i] = dataTypes[i].asJavaClass();
        return paramTypes;
    }

    /**
     * Construct an array containing the Java Driver {@link com.datastax.driver.core.DataType}s for the
     * C* internal types.
     *
     * @param abstractTypes list with UDF argument types
     * @return array with argument types as {@link com.datastax.driver.core.DataType}
     */
    public static DataType[] driverTypes(List<AbstractType<?>> abstractTypes)
    {
        DataType[] argDataTypes = new DataType[abstractTypes.size()];
        for (int i = 0; i < argDataTypes.length; i++)
            argDataTypes[i] = driverType(abstractTypes.get(i));
        return argDataTypes;
    }

    /**
     * Returns the Java Driver {@link com.datastax.driver.core.DataType} for the C* internal type.
     */
    public static DataType driverType(AbstractType abstractType)
    {
        CQL3Type cqlType = abstractType.asCQL3Type();
        try
        {
            return (DataType) methodParseOne.invoke(cqlType.getType().toString());
        }
        catch (RuntimeException | Error e)
        {
            // immediately rethrow these...
            throw e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException("cannot parse driver type " + cqlType.getType().toString(), e);
        }
    }

    // We allow method overloads, so a function is not uniquely identified by its name only, but
    // also by its argument types. To distinguish overloads of given function name in the schema
    // we use a "signature" which is just a SHA-1 of it's argument types (we could replace that by
    // using a "signature" UDT that would be comprised of the function name and argument types,
    // which we could then use as clustering column. But as we haven't yet used UDT in system tables,
    // We'll leave that decision to #6717).
    public static ByteBuffer calculateSignature(AbstractFunction fun)
    {
        MessageDigest digest = FBUtilities.newMessageDigest("SHA-1");
        digest.update(UTF8Type.instance.decompose(fun.name().name));
        for (AbstractType<?> type : fun.argTypes())
            digest.update(UTF8Type.instance.decompose(type.asCQL3Type().toString()));
        return ByteBuffer.wrap(digest.digest());
    }
}
