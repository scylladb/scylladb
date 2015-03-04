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
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;

/**
 * Performs eager-creation of commit log segments in a background thread. All the
 * public methods are thread safe.
 */
public class CommitLogSegmentManager
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManager.class);

    /**
     * Queue of work to be done by the manager thread.  This is usually a recycle operation, which returns
     * a CommitLogSegment, or a delete operation, which returns null.
     */
    private final BlockingQueue<Callable<CommitLogSegment>> segmentManagementTasks = new LinkedBlockingQueue<>();

    /** Segments that are ready to be used. Head of the queue is the one we allocate writes to */
    private final ConcurrentLinkedQueue<CommitLogSegment> availableSegments = new ConcurrentLinkedQueue<>();

    /** Active segments, containing unflushed data */
    private final ConcurrentLinkedQueue<CommitLogSegment> activeSegments = new ConcurrentLinkedQueue<>();

    /** The segment we are currently allocating commit log records to */
    private volatile CommitLogSegment allocatingFrom = null;

    private final WaitQueue hasAvailableSegments = new WaitQueue();

    /**
     * Tracks commitlog size, in multiples of the segment size.  We need to do this so we can "promise" size
     * adjustments ahead of actually adding/freeing segments on disk, so that the "evict oldest segment" logic
     * can see the effect of recycling segments immediately (even though they're really happening asynchronously
     * on the manager thread, which will take a ms or two).
     */
    private final AtomicLong size = new AtomicLong();

    /**
     * New segment creation is initially disabled because we'll typically get some "free" segments
     * recycled after log replay.
     */
    private volatile boolean createReserveSegments = false;

    private final Thread managerThread;
    private volatile boolean run = true;

    public CommitLogSegmentManager()
    {
        // The run loop for the manager thread
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (run)
                {
                    try
                    {
                        Callable<CommitLogSegment> task = segmentManagementTasks.poll();
                        if (task == null)
                        {
                            // if we have no more work to do, check if we should create a new segment
                            if (availableSegments.isEmpty() && (activeSegments.isEmpty() || createReserveSegments))
                            {
                                logger.debug("No segments in reserve; creating a fresh one");
                                size.addAndGet(DatabaseDescriptor.getCommitLogSegmentSize());
                                // TODO : some error handling in case we fail to create a new segment
                                availableSegments.add(CommitLogSegment.freshSegment());
                                hasAvailableSegments.signalAll();
                            }

                            // flush old Cfs if we're full
                            long unused = unusedCapacity();
                            if (unused < 0)
                            {
                                List<CommitLogSegment> segmentsToRecycle = new ArrayList<>();
                                long spaceToReclaim = 0;
                                for (CommitLogSegment segment : activeSegments)
                                {
                                    if (segment == allocatingFrom)
                                        break;
                                    segmentsToRecycle.add(segment);
                                    spaceToReclaim += DatabaseDescriptor.getCommitLogSegmentSize();
                                    if (spaceToReclaim + unused >= 0)
                                        break;
                                }
                                flushDataFrom(segmentsToRecycle, false);
                            }

                            try
                            {
                                // wait for new work to be provided
                                task = segmentManagementTasks.take();
                            }
                            catch (InterruptedException e)
                            {
                                // shutdown signal; exit cleanly
                                continue;
                            }
                        }

                        CommitLogSegment recycled = task.call();
                        if (recycled != null)
                        {
                            // if the work resulted in a segment to recycle, publish it
                            availableSegments.add(recycled);
                            hasAvailableSegments.signalAll();
                        }
                    }
                    catch (Throwable t)
                    {
                        JVMStabilityInspector.inspectThrowable(t);
                        if (!CommitLog.handleCommitError("Failed managing commit log segments", t))
                            return;
                        // sleep some arbitrary period to avoid spamming CL
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                    }
                }
            }
        };

        managerThread = new Thread(runnable, "COMMIT-LOG-ALLOCATOR");
        managerThread.start();
    }

    /**
     * Reserve space in the current segment for the provided mutation or, if there isn't space available,
     * create a new segment.
     *
     * @return the provided Allocation object
     */
    public Allocation allocate(Mutation mutation, int size)
    {
        CommitLogSegment segment = allocatingFrom();

        Allocation alloc;
        while ( null == (alloc = segment.allocate(mutation, size)) )
        {
            // failed to allocate, so move to a new segment with enough room
            advanceAllocatingFrom(segment);
            segment = allocatingFrom;
        }

        return alloc;
    }

    // simple wrapper to ensure non-null value for allocatingFrom; only necessary on first call
    CommitLogSegment allocatingFrom()
    {
        CommitLogSegment r = allocatingFrom;
        if (r == null)
        {
            advanceAllocatingFrom(null);
            r = allocatingFrom;
        }
        return r;
    }

    /**
     * Fetches a new segment from the queue, creating a new one if necessary, and activates it
     */
    private void advanceAllocatingFrom(CommitLogSegment old)
    {
        while (true)
        {
            CommitLogSegment next;
            synchronized (this)
            {
                // do this in a critical section so we can atomically remove from availableSegments and add to allocatingFrom/activeSegments
                // see https://issues.apache.org/jira/browse/CASSANDRA-6557?focusedCommentId=13874432&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13874432
                if (allocatingFrom != old)
                    return;
                next = availableSegments.poll();
                if (next != null)
                {
                    allocatingFrom = next;
                    activeSegments.add(next);
                }
            }

            if (next != null)
            {
                if (old != null)
                {
                    // Now we can run the user defined command just after switching to the new commit log.
                    // (Do this here instead of in the recycle call so we can get a head start on the archive.)
                    CommitLog.instance.archiver.maybeArchive(old);

                    // ensure we don't continue to use the old file; not strictly necessary, but cleaner to enforce it
                    old.discardUnusedTail();
                }

                // request that the CL be synced out-of-band, as we've finished a segment
                CommitLog.instance.requestExtraSync();
                return;
            }

            // no more segments, so register to receive a signal when not empty
            WaitQueue.Signal signal = hasAvailableSegments.register(CommitLog.instance.metrics.waitingOnSegmentAllocation.time());

            // trigger the management thread; this must occur after registering
            // the signal to ensure we are woken by any new segment creation
            wakeManager();

            // check if the queue has already been added to before waiting on the signal, to catch modifications
            // that happened prior to registering the signal; *then* check to see if we've been beaten to making the change
            if (!availableSegments.isEmpty() || allocatingFrom != old)
            {
                signal.cancel();
                // if we've been beaten, just stop immediately
                if (allocatingFrom != old)
                    return;
                // otherwise try again, as there should be an available segment
                continue;
            }

            // can only reach here if the queue hasn't been inserted into
            // before we registered the signal, as we only remove items from the queue
            // after updating allocatingFrom. Can safely block until we are signalled
            // by the allocator that new segments have been published
            signal.awaitUninterruptibly();
        }
    }

    private void wakeManager()
    {
        // put a NO-OP on the queue, to trigger management thread (and create a new segment if necessary)
        segmentManagementTasks.add(new Callable<CommitLogSegment>()
        {
            public CommitLogSegment call()
            {
                return null;
            }
        });
    }

    /**
     * Switch to a new segment, regardless of how much is left in the current one.
     *
     * Flushes any dirty CFs for this segment and any older segments, and then recycles
     * the segments
     */
    void forceRecycleAll(Iterable<UUID> droppedCfs)
    {
        List<CommitLogSegment> segmentsToRecycle = new ArrayList<>(activeSegments);
        CommitLogSegment last = segmentsToRecycle.get(segmentsToRecycle.size() - 1);
        advanceAllocatingFrom(last);

        // wait for the commit log modifications
        last.waitForModifications();

        // make sure the writes have materialized inside of the memtables by waiting for all outstanding writes
        // on the relevant keyspaces to complete
        Set<Keyspace> keyspaces = new HashSet<>();
        for (UUID cfId : last.getDirtyCFIDs())
        {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(cfId);
            if (cfs != null)
                keyspaces.add(cfs.keyspace);
        }
        for (Keyspace keyspace : keyspaces)
            keyspace.writeOrder.awaitNewBarrier();

        // flush and wait for all CFs that are dirty in segments up-to and including 'last'
        Future<?> future = flushDataFrom(segmentsToRecycle, true);
        try
        {
            future.get();

            for (CommitLogSegment segment : activeSegments)
                for (UUID cfId : droppedCfs)
                    segment.markClean(cfId, segment.getContext());

            // now recycle segments that are unused, as we may not have triggered a discardCompletedSegments()
            // if the previous active segment was the only one to recycle (since an active segment isn't
            // necessarily dirty, and we only call dCS after a flush).
            for (CommitLogSegment segment : activeSegments)
                if (segment.isUnused())
                    recycleSegment(segment);

            CommitLogSegment first;
            if ((first = activeSegments.peek()) != null && first.id <= last.id)
                logger.error("Failed to force-recycle all segments; at least one segment is still in use with dirty CFs.");
        }
        catch (Throwable t)
        {
            // for now just log the error and return false, indicating that we failed
            logger.error("Failed waiting for a forced recycle of in-use commit log segments", t);
        }
    }

    /**
     * Indicates that a segment is no longer in use and that it should be recycled.
     *
     * @param segment segment that is no longer in use
     */
    void recycleSegment(final CommitLogSegment segment)
    {
        boolean archiveSuccess = CommitLog.instance.archiver.maybeWaitForArchiving(segment.getName());
        activeSegments.remove(segment);
        if (!archiveSuccess)
        {
            // if archiving (command) was not successful then leave the file alone. don't delete or recycle.
            discardSegment(segment, false);
            return;
        }
        if (isCapExceeded())
        {
            discardSegment(segment, true);
            return;
        }

        logger.debug("Recycling {}", segment);
        segmentManagementTasks.add(new Callable<CommitLogSegment>()
        {
            public CommitLogSegment call()
            {
                return segment.recycle();
            }
        });
    }

    /**
     * Differs from the above because it can work on any file instead of just existing
     * commit log segments managed by this manager.
     *
     * @param file segment file that is no longer in use.
     */
    void recycleSegment(final File file)
    {
        if (isCapExceeded()
            || CommitLogDescriptor.fromFileName(file.getName()).getMessagingVersion() != MessagingService.current_version)
        {
            // (don't decrease managed size, since this was never a "live" segment)
            logger.debug("(Unopened) segment {} is no longer needed and will be deleted now", file);
            FileUtils.deleteWithConfirm(file);
            return;
        }

        logger.debug("Recycling {}", file);
        // this wasn't previously a live segment, so add it to the managed size when we make it live
        size.addAndGet(DatabaseDescriptor.getCommitLogSegmentSize());
        segmentManagementTasks.add(new Callable<CommitLogSegment>()
        {
            public CommitLogSegment call()
            {
                return new CommitLogSegment(file.getPath());
            }
        });
    }

    /**
     * Indicates that a segment file should be deleted.
     *
     * @param segment segment to be discarded
     */
    private void discardSegment(final CommitLogSegment segment, final boolean deleteFile)
    {
        logger.debug("Segment {} is no longer active and will be deleted {}", segment, deleteFile ? "now" : "by the archive script");
        size.addAndGet(-DatabaseDescriptor.getCommitLogSegmentSize());

        segmentManagementTasks.add(new Callable<CommitLogSegment>()
        {
            public CommitLogSegment call()
            {
                segment.close();
                if (deleteFile)
                    segment.delete();
                return null;
            }
        });
    }

    /**
     * @return the space (in bytes) used by all segment files.
     */
    public long bytesUsed()
    {
        return size.get();
    }

    /**
     * @param name the filename to check
     * @return true if file is managed by this manager.
     */
    public boolean manages(String name)
    {
        for (CommitLogSegment segment : Iterables.concat(activeSegments, availableSegments))
            if (segment.getName().equals(name))
                return true;
        return false;
    }

    /**
     * Check to see if the speculative current size exceeds the cap.
     *
     * @return true if cap is exceeded
     */
    private boolean isCapExceeded()
    {
        return unusedCapacity() < 0;
    }

    private long unusedCapacity()
    {
        long currentSize = size.get();
        logger.debug("Total active commitlog segment space used is {}", currentSize);
        return DatabaseDescriptor.getTotalCommitlogSpaceInMB() * 1024 * 1024 - currentSize;
    }

    /**
     * Throws a flag that enables the behavior of keeping at least one spare segment
     * available at all times.
     */
    public void enableReserveSegmentCreation()
    {
        createReserveSegments = true;
        wakeManager();
    }

    /**
     * Force a flush on all CFs that are still dirty in @param segments.
     *
     * @return a Future that will finish when all the flushes are complete.
     */
    private Future<?> flushDataFrom(List<CommitLogSegment> segments, boolean force)
    {
        if (segments.isEmpty())
            return Futures.immediateFuture(null);
        final ReplayPosition maxReplayPosition = segments.get(segments.size() - 1).getContext();

        // a map of CfId -> forceFlush() to ensure we only queue one flush per cf
        final Map<UUID, ListenableFuture<?>> flushes = new LinkedHashMap<>();

        for (CommitLogSegment segment : segments)
        {
            for (UUID dirtyCFId : segment.getDirtyCFIDs())
            {
                Pair<String,String> pair = Schema.instance.getCF(dirtyCFId);
                if (pair == null)
                {
                    // even though we remove the schema entry before a final flush when dropping a CF,
                    // it's still possible for a writer to race and finish his append after the flush.
                    logger.debug("Marking clean CF {} that doesn't exist anymore", dirtyCFId);
                    segment.markClean(dirtyCFId, segment.getContext());
                }
                else if (!flushes.containsKey(dirtyCFId))
                {
                    String keyspace = pair.left;
                    final ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(dirtyCFId);
                    // can safely call forceFlush here as we will only ever block (briefly) for other attempts to flush,
                    // no deadlock possibility since switchLock removal
                    flushes.put(dirtyCFId, force ? cfs.forceFlush() : cfs.forceFlush(maxReplayPosition));
                }
            }
        }

        return Futures.allAsList(flushes.values());
    }

    /**
     * Resets all the segments, for testing purposes. DO NOT USE THIS OUTSIDE OF TESTS.
     */
    public void resetUnsafe()
    {
        logger.debug("Closing and clearing existing commit log segments...");

        while (!segmentManagementTasks.isEmpty())
            Thread.yield();

        for (CommitLogSegment segment : activeSegments)
            segment.close();
        activeSegments.clear();

        for (CommitLogSegment segment : availableSegments)
            segment.close();
        availableSegments.clear();

        allocatingFrom = null;
    }

    /**
     * Initiates the shutdown process for the management thread.
     */
    public void shutdown()
    {
        run = false;
        managerThread.interrupt();
    }

    /**
     * Returns when the management thread terminates.
     */
    public void awaitTermination() throws InterruptedException
    {
        managerThread.join();
    }

    /**
     * @return a read-only collection of the active commit log segments
     */
    Collection<CommitLogSegment> getActiveSegments()
    {
        return Collections.unmodifiableCollection(activeSegments);
    }
}

