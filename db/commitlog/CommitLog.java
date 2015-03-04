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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.*;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.DataOutputByteBuffer;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.PureJavaCrc32;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.*;

/*
 * Commit Log tracks every write operation into the system. The aim of the commit log is to be able to
 * successfully recover data that was not stored to disk via the Memtable.
 */
public class CommitLog implements CommitLogMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    public static final CommitLog instance = new CommitLog();

    // we only permit records HALF the size of a commit log, to ensure we don't spin allocating many mostly
    // empty segments when writing large records
    private static final long MAX_MUTATION_SIZE = DatabaseDescriptor.getCommitLogSegmentSize() >> 1;

    public final CommitLogSegmentManager allocator;
    public final CommitLogArchiver archiver = new CommitLogArchiver();
    final CommitLogMetrics metrics;
    final AbstractCommitLogService executor;

    private CommitLog()
    {
        DatabaseDescriptor.createAllDirectories();

        allocator = new CommitLogSegmentManager();

        executor = DatabaseDescriptor.getCommitLogSync() == Config.CommitLogSync.batch
                 ? new BatchCommitLogService(this)
                 : new PeriodicCommitLogService(this);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=Commitlog"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        // register metrics
        metrics = new CommitLogMetrics(executor, allocator);
    }

    /**
     * Perform recovery on commit logs located in the directory specified by the config file.
     *
     * @return the number of mutations replayed
     */
    public int recover() throws IOException
    {
        FilenameFilter unmanagedFilesFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                // we used to try to avoid instantiating commitlog (thus creating an empty segment ready for writes)
                // until after recover was finished.  this turns out to be fragile; it is less error-prone to go
                // ahead and allow writes before recover(), and just skip active segments when we do.
                return CommitLogDescriptor.isValid(name) && !instance.allocator.manages(name);
            }
        };

        // submit all existing files in the commit log dir for archiving prior to recovery - CASSANDRA-6904
        for (File file : new File(DatabaseDescriptor.getCommitLogLocation()).listFiles(unmanagedFilesFilter))
        {
            archiver.maybeArchive(file.getPath(), file.getName());
            archiver.maybeWaitForArchiving(file.getName());
        }

        assert archiver.archivePending.isEmpty() : "Not all commit log archive tasks were completed before restore";
        archiver.maybeRestoreArchive();

        File[] files = new File(DatabaseDescriptor.getCommitLogLocation()).listFiles(unmanagedFilesFilter);
        int replayed = 0;
        if (files.length == 0)
        {
            logger.info("No commitlog files found; skipping replay");
        }
        else
        {
            Arrays.sort(files, new CommitLogSegmentFileComparator());
            logger.info("Replaying {}", StringUtils.join(files, ", "));
            replayed = recover(files);
            logger.info("Log replay complete, {} replayed mutations", replayed);

            for (File f : files)
                CommitLog.instance.allocator.recycleSegment(f);
        }

        allocator.enableReserveSegmentCreation();
        return replayed;
    }

    /**
     * Perform recovery on a list of commit log files.
     *
     * @param clogs   the list of commit log files to replay
     * @return the number of mutations replayed
     */
    public int recover(File... clogs) throws IOException
    {
        CommitLogReplayer recovery = new CommitLogReplayer();
        recovery.recover(clogs);
        return recovery.blockForWrites();
    }

    /**
     * Perform recovery on a single commit log.
     */
    public void recover(String path) throws IOException
    {
        recover(new File(path));
    }

    /**
     * @return a ReplayPosition which, if >= one returned from add(), implies add() was started
     * (but not necessarily finished) prior to this call
     */
    public ReplayPosition getContext()
    {
        return allocator.allocatingFrom().getContext();
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments(Iterable<UUID> droppedCfs)
    {
        allocator.forceRecycleAll(droppedCfs);
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments()
    {
        allocator.forceRecycleAll(Collections.<UUID>emptyList());
    }

    /**
     * Forces a disk flush on the commit log files that need it.  Blocking.
     */
    public void sync(boolean syncAllSegments)
    {
        CommitLogSegment current = allocator.allocatingFrom();
        for (CommitLogSegment segment : allocator.getActiveSegments())
        {
            if (!syncAllSegments && segment.id > current.id)
                return;
            segment.sync();
        }
    }

    /**
     * Preempts the CLExecutor, telling to to sync immediately
     */
    public void requestExtraSync()
    {
        executor.requestExtraSync();
    }

    /**
     * Add a Mutation to the commit log.
     *
     * @param mutation the Mutation to add to the log
     */
    public ReplayPosition add(Mutation mutation)
    {
        assert mutation != null;

        long size = Mutation.serializer.serializedSize(mutation, MessagingService.current_version);

        long totalSize = size + ENTRY_OVERHEAD_SIZE;
        if (totalSize > MAX_MUTATION_SIZE)
        {
            throw new IllegalArgumentException(String.format("Mutation of %s bytes is too large for the maxiumum size of %s",
                                                             totalSize, MAX_MUTATION_SIZE));
        }

        Allocation alloc = allocator.allocate(mutation, (int) totalSize);
        try
        {
            PureJavaCrc32 checksum = new PureJavaCrc32();
            final ByteBuffer buffer = alloc.getBuffer();
            DataOutputByteBuffer dos = new DataOutputByteBuffer(buffer);

            // checksummed length
            dos.writeInt((int) size);
            checksum.update(buffer, buffer.position() - 4, 4);
            buffer.putInt(checksum.getCrc());

            int start = buffer.position();
            // checksummed mutation
            Mutation.serializer.serialize(mutation, dos, MessagingService.current_version);
            checksum.update(buffer, start, (int) size);
            buffer.putInt(checksum.getCrc());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, alloc.getSegment().getPath());
        }
        finally
        {
            alloc.markWritten();
        }

        executor.finishWriteFor(alloc);
        return alloc.getReplayPosition();
    }

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param cfId    the column family ID that was flushed
     * @param context the replay position of the flush
     */
    public void discardCompletedSegments(final UUID cfId, final ReplayPosition context)
    {
        logger.debug("discard completed log segments for {}, table {}", context, cfId);

        // Go thru the active segment files, which are ordered oldest to newest, marking the
        // flushed CF as clean, until we reach the segment file containing the ReplayPosition passed
        // in the arguments. Any segments that become unused after they are marked clean will be
        // recycled or discarded.
        for (Iterator<CommitLogSegment> iter = allocator.getActiveSegments().iterator(); iter.hasNext();)
        {
            CommitLogSegment segment = iter.next();
            segment.markClean(cfId, context);

            if (segment.isUnused())
            {
                logger.debug("Commit log segment {} is unused", segment);
                allocator.recycleSegment(segment);
            }
            else
            {
                logger.debug("Not safe to delete{} commit log segment {}; dirty is {}",
                        (iter.hasNext() ? "" : " active"), segment, segment.dirtyString());
            }

            // Don't mark or try to delete any newer segments once we've reached the one containing the
            // position of the flush.
            if (segment.contains(context))
                break;
        }
    }

    @Override
    public long getCompletedTasks()
    {
        return metrics.completedTasks.value();
    }

    @Override
    public long getPendingTasks()
    {
        return metrics.pendingTasks.value();
    }

    /**
     * @return the total size occupied by commitlog segments expressed in bytes. (used by MBean)
     */
    public long getTotalCommitlogSize()
    {
        return metrics.totalCommitLogSize.value();
    }

    public List<String> getActiveSegmentNames()
    {
        List<String> segmentNames = new ArrayList<>();
        for (CommitLogSegment segment : allocator.getActiveSegments())
            segmentNames.add(segment.getName());
        return segmentNames;
    }

    public List<String> getArchivingSegmentNames()
    {
        return new ArrayList<>(archiver.archivePending.keySet());
    }

    /**
     * Shuts down the threads used by the commit log, blocking until completion.
     */
    public void shutdownBlocking() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination();
        allocator.shutdown();
        allocator.awaitTermination();
    }

    /**
     * FOR TESTING PURPOSES. See CommitLogAllocator.
     */
    public void resetUnsafe()
    {
        allocator.resetUnsafe();
    }

    /**
     * Used by tests.
     *
     * @return the number of active segments (segments with unflushed data in them)
     */
    public int activeSegments()
    {
        return allocator.getActiveSegments().size();
    }

    @VisibleForTesting
    public static boolean handleCommitError(String message, Throwable t)
    {
        JVMStabilityInspector.inspectCommitLogThrowable(t);
        switch (DatabaseDescriptor.getCommitFailurePolicy())
        {
            // Needed here for unit tests to not fail on default assertion
            case die:
            case stop:
                StorageService.instance.stopTransports();
            case stop_commit:
                logger.error(String.format("%s. Commit disk failure policy is %s; terminating thread", message, DatabaseDescriptor.getCommitFailurePolicy()), t);
                return false;
            case ignore:
                logger.error(message, t);
                return true;
            default:
                throw new AssertionError(DatabaseDescriptor.getCommitFailurePolicy());
        }
    }

}
