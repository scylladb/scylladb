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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * <code>PrimaryKeyRestrictions</code> decorator that reverse the slices.
 */
final class ReversedPrimaryKeyRestrictions extends ForwardingPrimaryKeyRestrictions
{
    /**
     * The decorated restrictions.
     */
    private PrimaryKeyRestrictions restrictions;

    public ReversedPrimaryKeyRestrictions(PrimaryKeyRestrictions restrictions)
    {
        this.restrictions = restrictions;
    }

    @Override
    public PrimaryKeyRestrictions mergeWith(Restriction restriction) throws InvalidRequestException
    {
        return new ReversedPrimaryKeyRestrictions(this.restrictions.mergeWith(restriction));
    }

    @Override
    public List<ByteBuffer> bounds(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> buffers = restrictions.bounds(bound.reverse(), options);
        Collections.reverse(buffers);
        return buffers;
    }

    @Override
    public List<Composite> boundsAsComposites(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        List<Composite> composites = restrictions.boundsAsComposites(bound.reverse(), options);
        Collections.reverse(composites);
        return composites;
    }

    @Override
    public boolean isInclusive(Bound bound)
    {
        return this.restrictions.isInclusive(bound.reverse());
    }

    @Override
    protected PrimaryKeyRestrictions getDelegate()
    {
        return this.restrictions;
    }
}
