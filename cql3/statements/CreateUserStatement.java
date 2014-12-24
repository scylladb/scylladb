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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UserOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class CreateUserStatement extends AuthenticationStatement
{
    private final String username;
    private final UserOptions opts;
    private final boolean superuser;
    private final boolean ifNotExists;

    public CreateUserStatement(String username, UserOptions opts, boolean superuser, boolean ifNotExists)
    {
        this.username = username;
        this.opts = opts;
        this.superuser = superuser;
        this.ifNotExists = ifNotExists;
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        if (username.isEmpty())
            throw new InvalidRequestException("Username can't be an empty string");

        opts.validate();

        // validate login here before checkAccess to avoid leaking user existence to anonymous users.
        state.ensureNotAnonymous();

        if (!ifNotExists && Auth.isExistingUser(username))
            throw new InvalidRequestException(String.format("User %s already exists", username));
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        if (!state.getUser().isSuper())
            throw new UnauthorizedException("Only superusers are allowed to perform CREATE USER queries");
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        // not rejected in validate()
        if (ifNotExists && Auth.isExistingUser(username))
            return null;

        DatabaseDescriptor.getAuthenticator().create(username, opts.getOptions());
        Auth.insertUser(username, superuser);
        return null;
    }
}
