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

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Base class for the <code>ScalarFunction</code> native classes.
 */
public abstract class NativeScalarFunction extends NativeFunction implements ScalarFunction
{
    protected NativeScalarFunction(String name, AbstractType<?> returnType, AbstractType<?>... argsType)
    {
        super(name, returnType, argsType);
    }

    public final boolean isAggregate()
    {
        return false;
    }
}
