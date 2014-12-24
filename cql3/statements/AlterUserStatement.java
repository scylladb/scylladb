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
package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UserOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class AlterUserStatement extends AuthenticationStatement
{
    private final String username;
    private final UserOptions opts;
    private final Boolean superuser;

    public AlterUserStatement(String username, UserOptions opts, Boolean superuser)
    {
        this.username = username;
        this.opts = opts;
        this.superuser = superuser;
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        opts.validate();

        if (superuser == null && opts.isEmpty())
            throw new InvalidRequestException("ALTER USER can't be empty");

        // validate login here before checkAccess to avoid leaking user existence to anonymous users.
        state.ensureNotAnonymous();

        if (!Auth.isExistingUser(username))
            throw new InvalidRequestException(String.format("User %s doesn't exist", username));
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        AuthenticatedUser user = state.getUser();

        boolean isSuper = user.isSuper();

        if (superuser != null && user.getName().equals(username))
            throw new UnauthorizedException("You aren't allowed to alter your own superuser status");

        if (superuser != null && !isSuper)
            throw new UnauthorizedException("Only superusers are allowed to alter superuser status");

        if (!user.isSuper() && !user.getName().equals(username))
            throw new UnauthorizedException("You aren't allowed to alter this user");

        if (!isSuper)
        {
            for (IAuthenticator.Option option : opts.getOptions().keySet())
            {
                if (!DatabaseDescriptor.getAuthenticator().alterableOptions().contains(option))
                    throw new UnauthorizedException(String.format("You aren't allowed to alter %s option", option));
            }
        }
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        if (!opts.isEmpty())
            DatabaseDescriptor.getAuthenticator().alter(username, opts.getOptions());
        if (superuser != null)
            Auth.insertUser(username, superuser.booleanValue());
        return null;
    }
}
