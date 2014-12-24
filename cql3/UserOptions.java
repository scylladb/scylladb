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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

public class UserOptions
{
    private final Map<IAuthenticator.Option, Object> options = new HashMap<IAuthenticator.Option, Object>();

    public void put(String name, Object value)
    {
        options.put(IAuthenticator.Option.valueOf(name.toUpperCase()), value);
    }

    public boolean isEmpty()
    {
        return options.isEmpty();
    }

    public Map<IAuthenticator.Option, Object> getOptions()
    {
        return options;
    }

    public void validate() throws InvalidRequestException
    {
        for (IAuthenticator.Option option : options.keySet())
        {
            if (!DatabaseDescriptor.getAuthenticator().supportedOptions().contains(option))
                throw new InvalidRequestException(String.format("%s doesn't support %s option",
                                                                DatabaseDescriptor.getAuthenticator().getClass().getName(),
                                                                option));
        }
    }

    public String toString()
    {
        return FBUtilities.toString(options);
    }
}
