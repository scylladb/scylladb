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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UserType;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Base class for User Defined Functions.
 */
public abstract class UDFunction extends AbstractFunction implements ScalarFunction
{
    protected static final Logger logger = LoggerFactory.getLogger(UDFunction.class);

    protected final List<ColumnIdentifier> argNames;

    protected final String language;
    protected final String body;
    protected final boolean isDeterministic;

    protected final DataType[] argDataTypes;
    protected final DataType returnDataType;

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         AbstractType<?> returnType,
                         String language,
                         String body,
                         boolean isDeterministic)
    {
        this(name, argNames, argTypes, UDHelper.driverTypes(argTypes), returnType,
             UDHelper.driverType(returnType), language, body, isDeterministic);
    }

    protected UDFunction(FunctionName name,
                         List<ColumnIdentifier> argNames,
                         List<AbstractType<?>> argTypes,
                         DataType[] argDataTypes,
                         AbstractType<?> returnType,
                         DataType returnDataType,
                         String language,
                         String body,
                         boolean isDeterministic)
    {
        super(name, argTypes, returnType);
        assert new HashSet<>(argNames).size() == argNames.size() : "duplicate argument names";
        this.argNames = argNames;
        this.language = language;
        this.body = body;
        this.isDeterministic = isDeterministic;
        this.argDataTypes = argDataTypes;
        this.returnDataType = returnDataType;
    }

    public static UDFunction create(FunctionName name,
                                    List<ColumnIdentifier> argNames,
                                    List<AbstractType<?>> argTypes,
                                    AbstractType<?> returnType,
                                    String language,
                                    String body,
                                    boolean isDeterministic)
    throws InvalidRequestException
    {
        switch (language)
        {
            case "java": return JavaSourceUDFFactory.buildUDF(name, argNames, argTypes, returnType, body, isDeterministic);
            default: return new ScriptBasedUDF(name, argNames, argTypes, returnType, language, body, isDeterministic);
        }
    }

    /**
     * It can happen that a function has been declared (is listed in the scheam) but cannot
     * be loaded (maybe only on some nodes). This is the case for instance if the class defining
     * the class is not on the classpath for some of the node, or after a restart. In that case,
     * we create a "fake" function so that:
     *  1) the broken function can be dropped easily if that is what people want to do.
     *  2) we return a meaningful error message if the function is executed (something more precise
     *     than saying that the function doesn't exist)
     */
    public static UDFunction createBrokenFunction(FunctionName name,
                                                  List<ColumnIdentifier> argNames,
                                                  List<AbstractType<?>> argTypes,
                                                  AbstractType<?> returnType,
                                                  String language,
                                                  String body,
                                                  final InvalidRequestException reason)
    {
        return new UDFunction(name, argNames, argTypes, returnType, language, body, true)
        {
            public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                throw new InvalidRequestException(String.format("Function '%s' exists but hasn't been loaded successfully "
                                                                + "for the following reason: %s. Please see the server log for details",
                                                                this,
                                                                reason.getMessage()));
            }
        };
    }


    public boolean isAggregate()
    {
        return false;
    }

    public boolean isPure()
    {
        return isDeterministic;
    }

    public boolean isNative()
    {
        return false;
    }

    public List<ColumnIdentifier> argNames()
    {
        return argNames;
    }

    public boolean isDeterministic()
    {
        return isDeterministic;
    }

    public String body()
    {
        return body;
    }

    public String language()
    {
        return language;
    }

    /**
     * Used by UDF implementations (both Java code generated by {@link org.apache.cassandra.cql3.functions.JavaSourceUDFFactory}
     * and script executor {@link org.apache.cassandra.cql3.functions.ScriptBasedUDF}) to convert the C*
     * serialized representation to the Java object representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     * @param argIndex index of the UDF input argument
     */
    protected Object compose(int protocolVersion, int argIndex, ByteBuffer value)
    {
        return value == null ? null : argDataTypes[argIndex].deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    /**
     * Used by UDF implementations (both Java code generated by {@link org.apache.cassandra.cql3.functions.JavaSourceUDFFactory}
     * and script executor {@link org.apache.cassandra.cql3.functions.ScriptBasedUDF}) to convert the Java
     * object representation for the return value to the C* serialized representation.
     *
     * @param protocolVersion the native protocol version used for serialization
     */
    protected ByteBuffer decompose(int protocolVersion, Object value)
    {
        return value == null ? null : returnDataType.serialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof UDFunction))
            return false;

        UDFunction that = (UDFunction)o;
        return Objects.equal(name, that.name)
            && Objects.equal(argNames, that.argNames)
            && Functions.typeEquals(argTypes, that.argTypes)
            && Functions.typeEquals(returnType, that.returnType)
            && Objects.equal(language, that.language)
            && Objects.equal(body, that.body)
            && Objects.equal(isDeterministic, that.isDeterministic);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, argNames, argTypes, returnType, language, body, isDeterministic);
    }

    public void userTypeUpdated(String ksName, String typeName)
    {
        boolean updated = false;

        for (int i = 0; i < argDataTypes.length; i++)
        {
            DataType dataType = argDataTypes[i];
            if (dataType instanceof UserType)
            {
                UserType userType = (UserType) dataType;
                if (userType.getKeyspace().equals(ksName) && userType.getTypeName().equals(typeName))
                {
                    KSMetaData ksm = Schema.instance.getKSMetaData(ksName);
                    assert ksm != null;

                    org.apache.cassandra.db.marshal.UserType ut = ksm.userTypes.getType(ByteBufferUtil.bytes(typeName));

                    DataType newUserType = UDHelper.driverType(ut);
                    argDataTypes[i] = newUserType;

                    argTypes.set(i, ut);

                    updated = true;
                }
            }
        }

        if (updated)
            MigrationManager.announceNewFunction(this, true);
    }
}
