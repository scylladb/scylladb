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

import java.util.Arrays;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Base class for our native/hardcoded functions.
 */
public abstract class NativeFunction extends AbstractFunction
{
    protected NativeFunction(String name, AbstractType<?> returnType, AbstractType<?>... argTypes)
    {
        super(FunctionName.nativeFunction(name), Arrays.asList(argTypes), returnType);
    }

    // Most of our functions are pure, the other ones should override this
    public boolean isPure()
    {
        return true;
    }

    public boolean isNative()
    {
        return true;
    }
}
