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
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once
#include "core/shared_ptr.hh"
#include "core/distributed.hh"
#include "utils/UUID.hh"
#include <seastar/core/semaphore.hh>
#include <map>

namespace streaming {

class stream_result_future;

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming status and progress.
 */
class stream_manager {
    using UUID = utils::UUID;
#if 0
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
#endif

    /*
     * Currently running streams. Removed after completion/failure.
     * We manage them in two different maps to distinguish plan from initiated ones to
     * receiving ones withing the same JVM.
     */
private:
    std::unordered_map<UUID, shared_ptr<stream_result_future>> _initiated_streams;
    std::unordered_map<UUID, shared_ptr<stream_result_future>> _receiving_streams;
    semaphore _mutation_send_limiter{10};
public:
    semaphore& mutation_send_limiter() { return _mutation_send_limiter; }
#if  0
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
#endif

    void register_sending(shared_ptr<stream_result_future> result);

    void register_receiving(shared_ptr<stream_result_future> result);

    shared_ptr<stream_result_future> get_sending_stream(UUID plan_id);

    shared_ptr<stream_result_future> get_receiving_stream(UUID plan_id);

    void remove_stream(UUID plan_id);

    void show_streams();
#if 0

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
#endif
    future<> stop() {
        return make_ready_future<>();
    }
};

extern distributed<stream_manager> _the_stream_manager;

inline distributed<stream_manager>& get_stream_manager() {
    return _the_stream_manager;
}

inline stream_manager& get_local_stream_manager() {
    return _the_stream_manager.local();
}

} // namespace streaming
