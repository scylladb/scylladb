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
package org.apache.cassandra.utils;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * bounded threadsafe deque
 */
public class BoundedStatsDeque implements Iterable<Long>
{
    private final LinkedBlockingDeque<Long> deque;
    private final AtomicLong sum;

    public BoundedStatsDeque(int size)
    {
        deque = new LinkedBlockingDeque<>(size);
        sum = new AtomicLong(0);
    }

    public Iterator<Long> iterator()
    {
        return deque.iterator();
    }

    public int size()
    {
        return deque.size();
    }

    public void add(long i)
    {
        if (!deque.offer(i))
        {
            Long removed = deque.remove();
            sum.addAndGet(-removed);
            deque.offer(i);
        }
        sum.addAndGet(i);
    }

    public long sum()
    {
        return sum.get();
    }

    public double mean()
    {
        return size() > 0 ? ((double) sum()) / size() : 0;
    }
}
