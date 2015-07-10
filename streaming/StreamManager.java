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
package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.streaming.management.StreamEventJMXNotifier;
import org.apache.cassandra.streaming.management.StreamStateCompositeData;

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming status and progress.
 */
public class StreamManager implements StreamManagerMBean
{
    public static final StreamManager instance = new StreamManager();

    /**
     * Gets streaming rate limiter.
     * When stream_throughput_outbound_megabits_per_sec is 0, this returns rate limiter
     * with the rate of Double.MAX_VALUE bytes per second.
     * Rate unit is bytes per sec.
     *
     * @return StreamRateLimiter with rate limit set based on peer location.
     */
    public static StreamRateLimiter getRateLimiter(InetAddress peer)
    {
        return new StreamRateLimiter(peer);
    }

    public static class StreamRateLimiter
    {
        private static final double ONE_MEGA_BIT = (1024 * 1024) / 8; // from bits
        private static final RateLimiter limiter = RateLimiter.create(Double.MAX_VALUE);
        private static final RateLimiter interDCLimiter = RateLimiter.create(Double.MAX_VALUE);
        private final boolean isLocalDC;

        public StreamRateLimiter(InetAddress peer)
        {
            double throughput = ((double) DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec()) * ONE_MEGA_BIT;
            mayUpdateThroughput(throughput, limiter);

            double interDCThroughput = ((double) DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec()) * ONE_MEGA_BIT;
            mayUpdateThroughput(interDCThroughput, interDCLimiter);

            if (DatabaseDescriptor.getLocalDataCenter() != null && DatabaseDescriptor.getEndpointSnitch() != null)
                isLocalDC = DatabaseDescriptor.getLocalDataCenter().equals(
                            DatabaseDescriptor.getEndpointSnitch().getDatacenter(peer));
            else
                isLocalDC = true;
        }

        private void mayUpdateThroughput(double limit, RateLimiter rateLimiter)
        {
            // if throughput is set to 0, throttling is disabled
            if (limit == 0)
                limit = Double.MAX_VALUE;
            if (rateLimiter.getRate() != limit)
                rateLimiter.setRate(limit);
        }

        public void acquire(int toTransfer)
        {
            limiter.acquire(toTransfer);
            if (!isLocalDC)
                interDCLimiter.acquire(toTransfer);
        }
    }

    private final StreamEventJMXNotifier notifier = new StreamEventJMXNotifier();

    /*
     * Currently running streams. Removed after completion/failure.
     * We manage them in two different maps to distinguish plan from initiated ones to
     * receiving ones withing the same JVM.
     */
    private final Map<UUID, StreamResultFuture> initiatedStreams = new NonBlockingHashMap<>();
    private final Map<UUID, StreamResultFuture> receivingStreams = new NonBlockingHashMap<>();

    public Set<CompositeData> getCurrentStreams()
    {
        return Sets.newHashSet(Iterables.transform(Iterables.concat(initiatedStreams.values(), receivingStreams.values()), new Function<StreamResultFuture, CompositeData>()
        {
            public CompositeData apply(StreamResultFuture input)
            {
                return StreamStateCompositeData.toCompositeData(input.getCurrentState());
            }
        }));
    }

    public void register(final StreamResultFuture result)
    {
        result.addEventListener(notifier);
        // Make sure we remove the stream on completion (whether successful or not)
        result.addListener(new Runnable()
        {
            public void run()
            {
                initiatedStreams.remove(result.planId);
            }
        }, MoreExecutors.sameThreadExecutor());

        initiatedStreams.put(result.planId, result);
    }

    public void registerReceiving(final StreamResultFuture result)
    {
        result.addEventListener(notifier);
        // Make sure we remove the stream on completion (whether successful or not)
        result.addListener(new Runnable()
        {
            public void run()
            {
                receivingStreams.remove(result.planId);
            }
        }, MoreExecutors.sameThreadExecutor());

        receivingStreams.put(result.planId, result);
    }

    public StreamResultFuture getReceivingStream(UUID planId)
    {
        return receivingStreams.get(planId);
    }

    public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
    {
        notifier.addNotificationListener(listener, filter, handback);
    }

    public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException
    {
        notifier.removeNotificationListener(listener);
    }

    public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException
    {
        notifier.removeNotificationListener(listener, filter, handback);
    }

    public MBeanNotificationInfo[] getNotificationInfo()
    {
        return notifier.getNotificationInfo();
    }
}
