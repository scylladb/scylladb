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
import java.util.UUID;

import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.serializers.UUIDSerializer;

public abstract class UuidFcts
{
    public static final Function uuidFct = new NativeScalarFunction("uuid", UUIDType.instance)
    {
        public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters)
        {
            return UUIDSerializer.instance.serialize(UUID.randomUUID());
        }

        @Override
        public boolean isPure()
        {
            return false;
        }
    };
}
