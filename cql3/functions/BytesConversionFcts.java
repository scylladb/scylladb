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
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;

public abstract class BytesConversionFcts
{
    // Most of the XAsBlob and blobAsX functions are basically no-op since everything is
    // bytes internally. They only "trick" the type system.
    public static Function makeToBlobFunction(AbstractType<?> fromType)
    {
        String name = fromType.asCQL3Type() + "asblob";
        return new NativeScalarFunction(name, BytesType.instance, fromType)
        {
            public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters)
            {
                return parameters.get(0);
            }
        };
    }

    public static Function makeFromBlobFunction(final AbstractType<?> toType)
    {
        final String name = "blobas" + toType.asCQL3Type();
        return new NativeScalarFunction(name, toType, BytesType.instance)
        {
            public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                ByteBuffer val = parameters.get(0);
                try
                {
                    if (val != null)
                        toType.validate(val);
                    return val;
                }
                catch (MarshalException e)
                {
                    throw new InvalidRequestException(String.format("In call to function %s, value 0x%s is not a valid binary representation for type %s",
                                                                    name, ByteBufferUtil.bytesToHex(val), toType.asCQL3Type()));
                }
            }
        };
    }

    public static final Function VarcharAsBlobFct = new NativeScalarFunction("varcharasblob", BytesType.instance, UTF8Type.instance)
    {
        public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters)
        {
            return parameters.get(0);
        }
    };

    public static final Function BlobAsVarcharFact = new NativeScalarFunction("blobasvarchar", UTF8Type.instance, BytesType.instance)
    {
        public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters)
        {
            return parameters.get(0);
        }
    };
}
