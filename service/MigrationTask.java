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
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.WrappedRunnable;


class MigrationTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);

    private final InetAddress endpoint;

    MigrationTask(InetAddress endpoint)
    {
        this.endpoint = endpoint;
    }

    public void runMayThrow() throws Exception
    {
        MessageOut message = new MessageOut<>(MessagingService.Verb.MIGRATION_REQUEST, null, MigrationManager.MigrationsSerializer.instance);

        if (!FailureDetector.instance.isAlive(endpoint))
        {
            logger.error("Can't send migration request: node {} is down.", endpoint);
            return;
        }

        IAsyncCallback<Collection<Mutation>> cb = new IAsyncCallback<Collection<Mutation>>()
        {
            @Override
            public void response(MessageIn<Collection<Mutation>> message)
            {
                try
                {
                    LegacySchemaTables.mergeSchema(message.payload);
                }
                catch (IOException e)
                {
                    logger.error("IOException merging remote schema", e);
                }
                catch (ConfigurationException e)
                {
                    logger.error("Configuration exception merging remote schema", e);
                }
            }

            @Override
            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        MessagingService.instance().sendRR(message, endpoint, cb);
    }
}
