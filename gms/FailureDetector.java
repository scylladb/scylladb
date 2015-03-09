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
package org.apache.cassandra.gms;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.BoundedStatsDeque;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara.
 * Check the paper and the <i>IFailureDetector</i> interface for details.
 */
public class FailureDetector implements IFailureDetector, FailureDetectorMBean
{
    private static final Logger logger = LoggerFactory.getLogger(FailureDetector.class);
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=FailureDetector";
    private static final int SAMPLE_SIZE = 1000;
    protected static final long INITIAL_VALUE_NANOS = TimeUnit.NANOSECONDS.convert(getInitialValue(), TimeUnit.MILLISECONDS);

    public static final IFailureDetector instance = new FailureDetector();

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    private final double PHI_FACTOR = 1.0 / Math.log(10.0); // 0.434...

    private final Map<InetAddress, ArrivalWindow> arrivalSamples = new Hashtable<InetAddress, ArrivalWindow>();
    private final List<IFailureDetectionEventListener> fdEvntListeners = new CopyOnWriteArrayList<IFailureDetectionEventListener>();

    public FailureDetector()
    {
        // Register this instance with JMX
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static long getInitialValue()
    {
        String newvalue = System.getProperty("cassandra.fd_initial_value_ms");
        if (newvalue == null)
        {
            return Gossiper.intervalInMillis * 2;
        }
        else
        {
            logger.info("Overriding FD INITIAL_VALUE to {}ms", newvalue);
            return Integer.parseInt(newvalue);
        }
    }

    public String getAllEndpointStates()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            sb.append(entry.getKey()).append("\n");
            appendEndpointState(sb, entry.getValue());
        }
        return sb.toString();
    }

    public Map<String, String> getSimpleStates()
    {
        Map<String, String> nodesStatus = new HashMap<String, String>(Gossiper.instance.endpointStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (entry.getValue().isAlive())
                nodesStatus.put(entry.getKey().toString(), "UP");
            else
                nodesStatus.put(entry.getKey().toString(), "DOWN");
        }
        return nodesStatus;
    }

    public int getDownEndpointCount()
    {
        int count = 0;
        for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (!entry.getValue().isAlive())
                count++;
        }
        return count;
    }

    public int getUpEndpointCount()
    {
        int count = 0;
        for (Map.Entry<InetAddress, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            if (entry.getValue().isAlive())
                count++;
        }
        return count;
    }

    public String getEndpointState(String address) throws UnknownHostException
    {
        StringBuilder sb = new StringBuilder();
        EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(InetAddress.getByName(address));
        appendEndpointState(sb, endpointState);
        return sb.toString();
    }

    private void appendEndpointState(StringBuilder sb, EndpointState endpointState)
    {
        sb.append("  generation:").append(endpointState.getHeartBeatState().getGeneration()).append("\n");
        sb.append("  heartbeat:").append(endpointState.getHeartBeatState().getHeartBeatVersion()).append("\n");
        for (Map.Entry<ApplicationState, VersionedValue> state : endpointState.applicationState.entrySet())
        {
            if (state.getKey() == ApplicationState.TOKENS)
                continue;
            sb.append("  ").append(state.getKey()).append(":").append(state.getValue().value).append("\n");
        }
    }

    /**
     * Dump the inter arrival times for examination if necessary.
     */
    public void dumpInterArrivalTimes()
    {
        File file = FileUtils.createTempFile("failuredetector-", ".dat");

        OutputStream os = null;
        try
        {
            os = new BufferedOutputStream(new FileOutputStream(file, true));
            os.write(toString().getBytes());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
        finally
        {
            FileUtils.closeQuietly(os);
        }
    }

    public void setPhiConvictThreshold(double phi)
    {
        DatabaseDescriptor.setPhiConvictThreshold(phi);
    }

    public double getPhiConvictThreshold()
    {
        return DatabaseDescriptor.getPhiConvictThreshold();
    }

    public boolean isAlive(InetAddress ep)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
            return true;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
        // we could assert not-null, but having isAlive fail screws a node over so badly that
        // it's worth being defensive here so minor bugs don't cause disproportionate
        // badness.  (See CASSANDRA-1463 for an example).
        if (epState == null)
            logger.error("unknown endpoint {}", ep);
        return epState != null && epState.isAlive();
    }

    public void report(InetAddress ep)
    {
        if (logger.isTraceEnabled())
            logger.trace("reporting {}", ep);
        long now = System.nanoTime();
        ArrivalWindow heartbeatWindow = arrivalSamples.get(ep);
        if (heartbeatWindow == null)
        {
            // avoid adding an empty ArrivalWindow to the Map
            heartbeatWindow = new ArrivalWindow(SAMPLE_SIZE);
            heartbeatWindow.add(now);
            arrivalSamples.put(ep, heartbeatWindow);
        }
        else
        {
            heartbeatWindow.add(now);
        }
    }

    public void interpret(InetAddress ep)
    {
        ArrivalWindow hbWnd = arrivalSamples.get(ep);
        if (hbWnd == null)
        {
            return;
        }
        long now = System.nanoTime();
        double phi = hbWnd.phi(now);
        if (logger.isTraceEnabled())
            logger.trace("PHI for {} : {}", ep, phi);

        if (PHI_FACTOR * phi > getPhiConvictThreshold())
        {
            logger.trace("notifying listeners that {} is down", ep);
            logger.trace("intervals: {} mean: {}", hbWnd, hbWnd.mean());
            for (IFailureDetectionEventListener listener : fdEvntListeners)
            {
                listener.convict(ep, phi);
            }
        }
    }

    public void forceConviction(InetAddress ep)
    {
        logger.debug("Forcing conviction of {}", ep);
        for (IFailureDetectionEventListener listener : fdEvntListeners)
        {
            listener.convict(ep, getPhiConvictThreshold());
        }
    }

    public void remove(InetAddress ep)
    {
        arrivalSamples.remove(ep);
    }

    public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        fdEvntListeners.add(listener);
    }

    public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        fdEvntListeners.remove(listener);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Set<InetAddress> eps = arrivalSamples.keySet();

        sb.append("-----------------------------------------------------------------------");
        for (InetAddress ep : eps)
        {
            ArrivalWindow hWnd = arrivalSamples.get(ep);
            sb.append(ep + " : ");
            sb.append(hWnd);
            sb.append(System.getProperty("line.separator"));
        }
        sb.append("-----------------------------------------------------------------------");
        return sb.toString();
    }

    public static void main(String[] args)
    {
    }
}

class ArrivalWindow
{
    private static final Logger logger = LoggerFactory.getLogger(ArrivalWindow.class);
    private long tLast = 0L;
    private final BoundedStatsDeque arrivalIntervals;

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    private final double PHI_FACTOR = 1.0 / Math.log(10.0);

    // in the event of a long partition, never record an interval longer than the rpc timeout,
    // since if a host is regularly experiencing connectivity problems lasting this long we'd
    // rather mark it down quickly instead of adapting
    // this value defaults to the same initial value the FD is seeded with
    private final long MAX_INTERVAL_IN_NANO = getMaxInterval();

    ArrivalWindow(int size)
    {
        arrivalIntervals = new BoundedStatsDeque(size);
    }

    private static long getMaxInterval()
    {
        String newvalue = System.getProperty("cassandra.fd_max_interval_ms");
        if (newvalue == null)
        {
            return FailureDetector.INITIAL_VALUE_NANOS;
        }
        else
        {
            logger.info("Overriding FD MAX_INTERVAL to {}ms", newvalue);
            return TimeUnit.NANOSECONDS.convert(Integer.parseInt(newvalue), TimeUnit.MILLISECONDS);
        }
    }

    synchronized void add(long value)
    {
        assert tLast >= 0;
        if (tLast > 0L)
        {
            long interArrivalTime = (value - tLast);
            if (interArrivalTime <= MAX_INTERVAL_IN_NANO)
                arrivalIntervals.add(interArrivalTime);
            else
                logger.debug("Ignoring interval time of {}", interArrivalTime);
        }
        else
        {
            // We use a very large initial interval since the "right" average depends on the cluster size
            // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
            // than low (false positives, which cause "flapping").
            arrivalIntervals.add(FailureDetector.INITIAL_VALUE_NANOS);
        }
        tLast = value;
    }

    double mean()
    {
        return arrivalIntervals.mean();
    }

    // see CASSANDRA-2597 for an explanation of the math at work here.
    double phi(long tnow)
    {
        assert arrivalIntervals.size() > 0 && tLast > 0; // should not be called before any samples arrive
        long t = tnow - tLast;
        return t / mean();
    }

    public String toString()
    {
        return StringUtils.join(arrivalIntervals.iterator(), " ");
    }
}

