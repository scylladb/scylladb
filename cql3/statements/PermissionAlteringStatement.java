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

import java.util.Set;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;

public abstract class PermissionAlteringStatement extends AuthorizationStatement
{
    protected final Set<Permission> permissions;
    protected DataResource resource;
    protected final String username;

    protected PermissionAlteringStatement(Set<Permission> permissions, IResource resource, String username)
    {
        this.permissions = permissions;
        this.resource = (DataResource) resource;
        this.username = username;
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // validate login here before checkAccess to avoid leaking user existence to anonymous users.
        state.ensureNotAnonymous();

        if (!Auth.isExistingUser(username))
            throw new InvalidRequestException(String.format("User %s doesn't exist", username));

        // if a keyspace is omitted when GRANT/REVOKE ON TABLE <table>, we need to correct the resource.
        resource = maybeCorrectResource(resource, state);
        if (!resource.exists())
            throw new InvalidRequestException(String.format("%s doesn't exist", resource));
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        // check that the user has AUTHORIZE permission on the resource or its parents, otherwise reject GRANT/REVOKE.
        state.ensureHasPermission(Permission.AUTHORIZE, resource);
        // check that the user has [a single permission or all in case of ALL] on the resource or its parents.
        for (Permission p : permissions)
            state.ensureHasPermission(p, resource);
    }
}
