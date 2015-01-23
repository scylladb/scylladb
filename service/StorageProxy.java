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
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Predicate;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.metrics.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.paxos.*;
import org.apache.cassandra.sink.SinkManager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.*;

public class StorageProxy implements StorageProxyMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);
    static final boolean OPTIMIZE_LOCAL_REQUESTS = true; // set to false to test messagingservice path on single node

    public static final String UNREACHABLE = "UNREACHABLE";

    private static final WritePerformer standardWritePerformer;
    private static final WritePerformer counterWritePerformer;
    private static final WritePerformer counterWriteOnCoordinatorPerformer;

    public static final StorageProxy instance = new StorageProxy();

    private static volatile int maxHintsInProgress = 128 * FBUtilities.getAvailableProcessors();
    private static final CacheLoader<InetAddress, AtomicInteger> hintsInProgress = new CacheLoader<InetAddress, AtomicInteger>()
    {
        public AtomicInteger load(InetAddress inetAddress)
        {
            return new AtomicInteger(0);
        }
    };
    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");
    private static final ClientRequestMetrics rangeMetrics = new ClientRequestMetrics("RangeSlice");
    private static final ClientRequestMetrics writeMetrics = new ClientRequestMetrics("Write");
    private static final CASClientRequestMetrics casWriteMetrics = new CASClientRequestMetrics("CASWrite");
    private static final CASClientRequestMetrics casReadMetrics = new CASClientRequestMetrics("CASRead");

    private static final double CONCURRENT_SUBREQUESTS_MARGIN = 0.10;

    private StorageProxy() {}

    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        standardWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws OverloadedException
            {
                assert mutation instanceof Mutation;
                sendToHintedEndpoints((Mutation) mutation, targets, responseHandler, localDataCenter);
            }
        };

        /*
         * We execute counter writes in 2 places: either directly in the coordinator node if it is a replica, or
         * in CounterMutationVerbHandler on a replica othewise. The write must be executed on the COUNTER_MUTATION stage
         * but on the latter case, the verb handler already run on the COUNTER_MUTATION stage, so we must not execute the
         * underlying on the stage otherwise we risk a deadlock. Hence two different performer.
         */
        counterWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistencyLevel)
            {
                counterWriteTask(mutation, targets, responseHandler, localDataCenter).run();
            }
        };

        counterWriteOnCoordinatorPerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistencyLevel)
            {
                StageManager.getStage(Stage.COUNTER_MUTATION)
                            .execute(counterWriteTask(mutation, targets, responseHandler, localDataCenter));
            }
        };
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any row,
     * which (that is, the row) is the unit of values being proposed, not single columns.
     *
     * The Paxos cohort is only the replicas for the given key, not the entire cluster.
     * So we expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
     *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
     *     accepted.
     *  2. Accept: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
     *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     *     value.
     *
     *  Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     *  so here is our approach:
     *   3a. The coordinator sends a commit message to all replicas with the ballot and value.
     *   3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     *       and send it with subsequent promise replies.  This allows us to discard acceptance records
     *       for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     *       later on.
     *
     *  Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
     *  values) between the prepare and accept phases.  This gives us a slightly longer window for another
     *  coordinator to come along and trump our own promise with a newer one but is otherwise safe.
     *
     * @param keyspaceName the keyspace for the CAS
     * @param cfName the column family for the CAS
     * @param key the row key for the row to CAS
     * @param request the conditions for the CAS to apply as well as the update to perform if the conditions hold.
     * @param consistencyForPaxos the consistency for the paxos prepare and propose round. This can only be either SERIAL or LOCAL_SERIAL.
     * @param consistencyForCommit the consistency for write done during the commit phase. This can be anything, except SERIAL or LOCAL_SERIAL.
     *
     * @return null if the operation succeeds in updating the row, or the current values corresponding to conditions.
     * (since, if the CAS doesn't succeed, it means the current value do not match the conditions).
     */
    public static ColumnFamily cas(String keyspaceName,
                                   String cfName,
                                   ByteBuffer key,
                                   CASRequest request,
                                   ConsistencyLevel consistencyForPaxos,
                                   ConsistencyLevel consistencyForCommit,
                                   ClientState state)
    throws UnavailableException, IsBootstrappingException, ReadTimeoutException, WriteTimeoutException, InvalidRequestException
    {
        final long start = System.nanoTime();
        int contentions = 0;
        try
        {
            consistencyForPaxos.validateForCas();
            consistencyForCommit.validateForCasCommit(keyspaceName);

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);

            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());
            while (System.nanoTime() - start < timeout)
            {
                // for simplicity, we'll do a single liveness check at the start of each attempt
                Pair<List<InetAddress>, Integer> p = getPaxosParticipants(keyspaceName, key, consistencyForPaxos);
                List<InetAddress> liveEndpoints = p.left;
                int requiredParticipants = p.right;

                final Pair<UUID, Integer> pair = beginAndRepairPaxos(start, key, metadata, liveEndpoints, requiredParticipants, consistencyForPaxos, consistencyForCommit, true, state);
                final UUID ballot = pair.left;
                contentions += pair.right;
                // read the current values and check they validate the conditions
                Tracing.trace("Reading existing values for CAS precondition");
                long timestamp = System.currentTimeMillis();
                ReadCommand readCommand = ReadCommand.create(keyspaceName, key, cfName, timestamp, request.readFilter());
                List<Row> rows = read(Arrays.asList(readCommand), consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM);
                ColumnFamily current = rows.get(0).cf;
                if (!request.appliesTo(current))
                {
                    Tracing.trace("CAS precondition does not match current values {}", current);
                    // We should not return null as this means success
                    casWriteMetrics.conditionNotMet.inc();
                    return current == null ? ArrayBackedSortedColumns.factory.create(metadata) : current;
                }

                // finish the paxos round w/ the desired updates
                // TODO turn null updates into delete?
                ColumnFamily updates = request.makeUpdates(current);

                // Apply triggers to cas updates. A consideration here is that
                // triggers emit Mutations, and so a given trigger implementation
                // may generate mutations for partitions other than the one this
                // paxos round is scoped for. In this case, TriggerExecutor will
                // validate that the generated mutations are targetted at the same
                // partition as the initial updates and reject (via an
                // InvalidRequestException) any which aren't.
                updates = TriggerExecutor.instance.execute(key, updates);

                Commit proposal = Commit.newProposal(key, ballot, updates);
                Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                if (proposePaxos(proposal, liveEndpoints, requiredParticipants, true, consistencyForPaxos))
                {
                    commitPaxos(proposal, consistencyForCommit);
                    Tracing.trace("CAS successful");
                    return null;
                }

                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                contentions++;
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                // continue to retry
            }

            throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(keyspaceName)));
        }
        catch (WriteTimeoutException|ReadTimeoutException e)
        {
            casWriteMetrics.timeouts.mark();
            throw e;
        }
        catch(UnavailableException e)
        {
            casWriteMetrics.unavailables.mark();
            throw e;
        }
        finally
        {
            if(contentions > 0)
                casWriteMetrics.contention.update(contentions);
            casWriteMetrics.addNano(System.nanoTime() - start);
        }
    }

    private static Predicate<InetAddress> sameDCPredicateFor(final String dc)
    {
        final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        return new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress host)
            {
                return dc.equals(snitch.getDatacenter(host));
            }
        };
    }

    private static Pair<List<InetAddress>, Integer> getPaxosParticipants(String keyspaceName, ByteBuffer key, ConsistencyLevel consistencyForPaxos) throws UnavailableException
    {
        Token tk = StorageService.getPartitioner().getToken(key);
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

        if (consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL)
        {
            // Restrict naturalEndpoints and pendingEndpoints to node in the local DC only
            String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            Predicate<InetAddress> isLocalDc = sameDCPredicateFor(localDc);
            naturalEndpoints = ImmutableList.copyOf(Iterables.filter(naturalEndpoints, isLocalDc));
            pendingEndpoints = ImmutableList.copyOf(Iterables.filter(pendingEndpoints, isLocalDc));
        }
        int participants = pendingEndpoints.size() + naturalEndpoints.size();
        int requiredParticipants = participants + 1  / 2; // See CASSANDRA-833
        List<InetAddress> liveEndpoints = ImmutableList.copyOf(Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), IAsyncCallback.isAlive));
        if (liveEndpoints.size() < requiredParticipants)
            throw new UnavailableException(consistencyForPaxos, requiredParticipants, liveEndpoints.size());

        // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
        // Note that we fake an impossible number of required nodes in the unavailable exception
        // to nail home the point that it's an impossible operation no matter how many nodes are live.
        if (pendingEndpoints.size() > 1)
            throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", pendingEndpoints.size()),
                                           consistencyForPaxos,
                                           participants + 1,
                                           liveEndpoints.size());

        return Pair.create(liveEndpoints, requiredParticipants);
    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static Pair<UUID, Integer> beginAndRepairPaxos(long start,
                                                           ByteBuffer key,
                                                           CFMetaData metadata,
                                                           List<InetAddress> liveEndpoints,
                                                           int requiredParticipants,
                                                           ConsistencyLevel consistencyForPaxos,
                                                           ConsistencyLevel consistencyForCommit,
                                                           final boolean isWrite,
                                                           ClientState state)
    throws WriteTimeoutException
    {
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());

        PrepareCallback summary = null;
        int contentions = 0;
        while (System.nanoTime() - start < timeout)
        {
            // We don't want to use a timestamp that is older than the last one assigned by the ClientState or operations
            // may appear out-of-order (#7801). But note that state.getTimestamp() is in microseconds while the ballot
            // timestamp is only in milliseconds
            long currentTime = (state.getTimestamp() / 1000) + 1;
            long ballotMillis = summary == null
                              ? currentTime
                              : Math.max(currentTime, 1 + UUIDGen.unixTimestamp(summary.mostRecentInProgressCommit.ballot));
            UUID ballot = UUIDGen.getTimeUUID(ballotMillis);

            // prepare
            Tracing.trace("Preparing {}", ballot);
            Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
            summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos);
            if (!summary.promised)
            {
                Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                contentions++;
                // sleep a random amount to give the other proposer a chance to finish
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                continue;
            }

            Commit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            Commit mostRecent = summary.mostRecentCommit;

            // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
            // needs to be completed, so do it.
            if (!inProgress.update.isEmpty() && inProgress.isAfter(mostRecent))
            {
                Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                if(isWrite)
                    casWriteMetrics.unfinishedCommit.inc();
                else
                    casReadMetrics.unfinishedCommit.inc();
                Commit refreshedInProgress = Commit.newProposal(inProgress.key, ballot, inProgress.update);
                if (proposePaxos(refreshedInProgress, liveEndpoints, requiredParticipants, false, consistencyForPaxos))
                {
                    commitPaxos(refreshedInProgress, consistencyForCommit);
                }
                else
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    contentions++;
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
                continue;
            }

            // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
            // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
            // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
            // mean we lost messages), we pro-actively "repair" those nodes, and retry.
            Iterable<InetAddress> missingMRC = summary.replicasMissingMostRecentCommit();
            if (Iterables.size(missingMRC) > 0)
            {
                Tracing.trace("Repairing replicas that missed the most recent commit");
                sendCommit(mostRecent, missingMRC);
                // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                continue;
            }

            // We might commit this ballot and we want to ensure operations starting after this CAS succeed will be assigned
            // a timestamp greater that the one of this ballot, so operation order is preserved (#7801)
            state.updateLastTimestamp(ballotMillis * 1000);

            return Pair.create(ballot, contentions);
        }

        throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.ksName)));
    }

    /**
     * Unlike commitPaxos, this does not wait for replies
     */
    private static void sendCommit(Commit commit, Iterable<InetAddress> replicas)
    {
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, commit, Commit.serializer);
        for (InetAddress target : replicas)
            MessagingService.instance().sendOneWay(message, target);
    }

    private static PrepareCallback preparePaxos(Commit toPrepare, List<InetAddress> endpoints, int requiredParticipants, ConsistencyLevel consistencyForPaxos)
    throws WriteTimeoutException
    {
        PrepareCallback callback = new PrepareCallback(toPrepare.key, toPrepare.update.metadata(), requiredParticipants, consistencyForPaxos);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PREPARE, toPrepare, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);
        callback.await();
        return callback;
    }

    private static boolean proposePaxos(Commit proposal, List<InetAddress> endpoints, int requiredParticipants, boolean timeoutIfPartial, ConsistencyLevel consistencyLevel)
    throws WriteTimeoutException
    {
        ProposeCallback callback = new ProposeCallback(endpoints.size(), requiredParticipants, !timeoutIfPartial, consistencyLevel);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PROPOSE, proposal, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);

        callback.await();

        if (callback.isSuccessful())
            return true;

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, consistencyLevel, callback.getAcceptCount(), requiredParticipants);

        return false;
    }

    private static void commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel) throws WriteTimeoutException
    {
        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
        Keyspace keyspace = Keyspace.open(proposal.update.metadata().ksName);

        Token tk = StorageService.getPartitioner().getToken(proposal.key);
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspace.getName(), tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace.getName());

        AbstractWriteResponseHandler responseHandler = null;
        if (shouldBlock)
        {
            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
            responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistencyLevel, null, WriteType.SIMPLE);
        }

        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, proposal, Commit.serializer);
        for (InetAddress destination : Iterables.concat(naturalEndpoints, pendingEndpoints))
        {
            if (FailureDetector.instance.isAlive(destination))
            {
                if (shouldBlock)
                    MessagingService.instance().sendRR(message, destination, responseHandler);
                else
                    MessagingService.instance().sendOneWay(message, destination);
            }
        }

        if (shouldBlock)
            responseHandler.get();
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     */
    public static void mutate(Collection<? extends IMutation> mutations, ConsistencyLevel consistency_level)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for mutation");
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        long startTime = System.nanoTime();
        List<AbstractWriteResponseHandler> responseHandlers = new ArrayList<>(mutations.size());

        try
        {
            for (IMutation mutation : mutations)
            {
                if (mutation instanceof CounterMutation)
                {
                    responseHandlers.add(mutateCounter((CounterMutation)mutation, localDataCenter));
                }
                else
                {
                    WriteType wt = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;
                    responseHandlers.add(performWrite(mutation, consistency_level, localDataCenter, standardWritePerformer, null, wt));
                }
            }

            // wait for writes.  throws TimeoutException if necessary
            for (AbstractWriteResponseHandler responseHandler : responseHandlers)
            {
                responseHandler.get();
            }
        }
        catch (WriteTimeoutException ex)
        {
            if (consistency_level == ConsistencyLevel.ANY)
            {
                // hint all the mutations (except counters, which can't be safely retried).  This means
                // we'll re-hint any successful ones; doesn't seem worth it to track individual success
                // just for this unusual case.
                for (IMutation mutation : mutations)
                {
                    if (mutation instanceof CounterMutation)
                        continue;

                    Token tk = StorageService.getPartitioner().getToken(mutation.key());
                    List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(mutation.getKeyspaceName(), tk);
                    Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, mutation.getKeyspaceName());
                    for (InetAddress target : Iterables.concat(naturalEndpoints, pendingEndpoints))
                    {
                        // local writes can timeout, but cannot be dropped (see LocalMutationRunnable and
                        // CASSANDRA-6510), so there is no need to hint or retry
                        if (!target.equals(FBUtilities.getBroadcastAddress()) && shouldHint(target))
                            submitHint((Mutation) mutation, target, null);
                    }
                }
                Tracing.trace("Wrote hint to satisfy CL.ANY after no replicas acknowledged the write");
            }
            else
            {
                writeMetrics.timeouts.mark();
                ClientRequestMetrics.writeTimeouts.inc();
                Tracing.trace("Write timeout; received {} of {} required replies", ex.received, ex.blockFor);
                throw ex;
            }
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            ClientRequestMetrics.writeUnavailables.inc();
            Tracing.trace("Unavailable");
            throw e;
        }
        catch (OverloadedException e)
        {
            ClientRequestMetrics.writeUnavailables.inc();
            Tracing.trace("Overloaded");
            throw e;
        }
        finally
        {
            writeMetrics.addNano(System.nanoTime() - startTime);
        }
    }

    @SuppressWarnings("unchecked")
    public static void mutateWithTriggers(Collection<? extends IMutation> mutations,
                                          ConsistencyLevel consistencyLevel,
                                          boolean mutateAtomically)
    throws WriteTimeoutException, UnavailableException, OverloadedException, InvalidRequestException
    {
        Collection<Mutation> augmented = TriggerExecutor.instance.execute(mutations);

        if (augmented != null)
            mutateAtomically(augmented, consistencyLevel);
        else if (mutateAtomically)
            mutateAtomically((Collection<Mutation>) mutations, consistencyLevel);
        else
            mutate(mutations, consistencyLevel);
    }

    /**
     * See mutate. Adds additional steps before and after writing a batch.
     * Before writing the batch (but after doing availability check against the FD for the row replicas):
     *      write the entire batch to a batchlog elsewhere in the cluster.
     * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
     *
     * @param mutations the Mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     */
    public static void mutateAtomically(Collection<Mutation> mutations, ConsistencyLevel consistency_level)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for atomic batch");
        long startTime = System.nanoTime();

        List<WriteResponseHandlerWrapper> wrappers = new ArrayList<WriteResponseHandlerWrapper>(mutations.size());
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        try
        {
            // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
            for (Mutation mutation : mutations)
            {
                WriteResponseHandlerWrapper wrapper = wrapResponseHandler(mutation, consistency_level, WriteType.BATCH);
                // exit early if we can't fulfill the CL at this time.
                wrapper.handler.assureSufficientLiveNodes();
                wrappers.add(wrapper);
            }

            // write to the batchlog
            Collection<InetAddress> batchlogEndpoints = getBatchlogEndpoints(localDataCenter, consistency_level);
            UUID batchUUID = UUIDGen.getTimeUUID();
            syncWriteToBatchlog(mutations, batchlogEndpoints, batchUUID);

            // now actually perform the writes and wait for them to complete
            syncWriteBatchedMutations(wrappers, localDataCenter);

            // remove the batchlog entries asynchronously
            asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID);
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            ClientRequestMetrics.writeUnavailables.inc();
            Tracing.trace("Unavailable");
            throw e;
        }
        catch (WriteTimeoutException e)
        {
            writeMetrics.timeouts.mark();
            ClientRequestMetrics.writeTimeouts.inc();
            Tracing.trace("Write timeout; received {} of {} required replies", e.received, e.blockFor);
            throw e;
        }
        finally
        {
            writeMetrics.addNano(System.nanoTime() - startTime);
        }
    }

    private static void syncWriteToBatchlog(Collection<Mutation> mutations, Collection<InetAddress> endpoints, UUID uuid)
    throws WriteTimeoutException
    {
        AbstractWriteResponseHandler handler = new WriteResponseHandler(endpoints,
                                                                        Collections.<InetAddress>emptyList(),
                                                                        ConsistencyLevel.ONE,
                                                                        Keyspace.open(SystemKeyspace.NAME),
                                                                        null,
                                                                        WriteType.BATCH_LOG);

        MessageOut<Mutation> message = BatchlogManager.getBatchlogMutationFor(mutations, uuid, MessagingService.current_version)
                                                      .createMessage();
        for (InetAddress target : endpoints)
        {
            int targetVersion = MessagingService.instance().getVersion(target);
            if (target.equals(FBUtilities.getBroadcastAddress()) && OPTIMIZE_LOCAL_REQUESTS)
            {
                insertLocal(message.payload, handler);
            }
            else if (targetVersion == MessagingService.current_version)
            {
                MessagingService.instance().sendRR(message, target, handler, false);
            }
            else
            {
                MessagingService.instance().sendRR(BatchlogManager.getBatchlogMutationFor(mutations, uuid, targetVersion)
                                                                  .createMessage(),
                                                   target,
                                                   handler,
                                                   false);
            }
        }

        handler.get();
    }

    private static void asyncRemoveFromBatchlog(Collection<InetAddress> endpoints, UUID uuid)
    {
        AbstractWriteResponseHandler handler = new WriteResponseHandler(endpoints,
                                                                        Collections.<InetAddress>emptyList(),
                                                                        ConsistencyLevel.ANY,
                                                                        Keyspace.open(SystemKeyspace.NAME),
                                                                        null,
                                                                        WriteType.SIMPLE);
        Mutation mutation = new Mutation(SystemKeyspace.NAME, UUIDType.instance.decompose(uuid));
        mutation.delete(SystemKeyspace.BATCHLOG, FBUtilities.timestampMicros());
        MessageOut<Mutation> message = mutation.createMessage();
        for (InetAddress target : endpoints)
        {
            if (target.equals(FBUtilities.getBroadcastAddress()) && OPTIMIZE_LOCAL_REQUESTS)
                insertLocal(message.payload, handler);
            else
                MessagingService.instance().sendRR(message, target, handler, false);
        }
    }

    private static void syncWriteBatchedMutations(List<WriteResponseHandlerWrapper> wrappers, String localDataCenter)
    throws WriteTimeoutException, OverloadedException
    {
        for (WriteResponseHandlerWrapper wrapper : wrappers)
        {
            Iterable<InetAddress> endpoints = Iterables.concat(wrapper.handler.naturalEndpoints, wrapper.handler.pendingEndpoints);
            sendToHintedEndpoints(wrapper.mutation, endpoints, wrapper.handler, localDataCenter);
        }

        for (WriteResponseHandlerWrapper wrapper : wrappers)
            wrapper.handler.get();
    }

    /**
     * Perform the write of a mutation given a WritePerformer.
     * Gather the list of write endpoints, apply locally and/or forward the mutation to
     * said write endpoint (deletaged to the actual WritePerformer) and wait for the
     * responses based on consistency level.
     *
     * @param mutation the mutation to be applied
     * @param consistency_level the consistency level for the write operation
     * @param performer the WritePerformer in charge of appliying the mutation
     * given the list of write endpoints (either standardWritePerformer for
     * standard writes or counterWritePerformer for counter writes).
     * @param callback an optional callback to be run if and when the write is
     * successful.
     */
    public static AbstractWriteResponseHandler performWrite(IMutation mutation,
                                                            ConsistencyLevel consistency_level,
                                                            String localDataCenter,
                                                            WritePerformer performer,
                                                            Runnable callback,
                                                            WriteType writeType)
    throws UnavailableException, OverloadedException
    {
        String keyspaceName = mutation.getKeyspaceName();
        AbstractReplicationStrategy rs = Keyspace.open(keyspaceName).getReplicationStrategy();

        Token tk = StorageService.getPartitioner().getToken(mutation.key());
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

        AbstractWriteResponseHandler responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, callback, writeType);

        // exit early if we can't fulfill the CL at this time
        responseHandler.assureSufficientLiveNodes();

        performer.apply(mutation, Iterables.concat(naturalEndpoints, pendingEndpoints), responseHandler, localDataCenter, consistency_level);
        return responseHandler;
    }

    // same as above except does not initiate writes (but does perform availability checks).
    private static WriteResponseHandlerWrapper wrapResponseHandler(Mutation mutation, ConsistencyLevel consistency_level, WriteType writeType)
    {
        AbstractReplicationStrategy rs = Keyspace.open(mutation.getKeyspaceName()).getReplicationStrategy();
        String keyspaceName = mutation.getKeyspaceName();
        Token tk = StorageService.getPartitioner().getToken(mutation.key());
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);
        AbstractWriteResponseHandler responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, null, writeType);
        return new WriteResponseHandlerWrapper(responseHandler, mutation);
    }

    // used by atomic_batch_mutate to decouple availability check from the write itself, caches consistency level and endpoints.
    private static class WriteResponseHandlerWrapper
    {
        final AbstractWriteResponseHandler handler;
        final Mutation mutation;

        WriteResponseHandlerWrapper(AbstractWriteResponseHandler handler, Mutation mutation)
        {
            this.handler = handler;
            this.mutation = mutation;
        }
    }

    /*
     * Replicas are picked manually:
     * - replicas should be alive according to the failure detector
     * - replicas should be in the local datacenter
     * - choose min(2, number of qualifying candiates above)
     * - allow the local node to be the only replica only if it's a single-node DC
     */
    private static Collection<InetAddress> getBatchlogEndpoints(String localDataCenter, ConsistencyLevel consistencyLevel)
    throws UnavailableException
    {
        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().getTopology();
        Multimap<String, InetAddress> localEndpoints = HashMultimap.create(topology.getDatacenterRacks().get(localDataCenter));
        String localRack = DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddress());

        Collection<InetAddress> chosenEndpoints = new BatchlogManager.EndpointFilter(localRack, localEndpoints).filter();
        if (chosenEndpoints.isEmpty())
        {
            if (consistencyLevel == ConsistencyLevel.ANY)
                return Collections.singleton(FBUtilities.getBroadcastAddress());

            throw new UnavailableException(ConsistencyLevel.ONE, 1, 0);
        }

        return chosenEndpoints;
    }

    /**
     * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
     * is not available.
     *
     * Note about hints:
     *
     * | Hinted Handoff | Consist. Level |
     * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
     * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
     * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     *
     * @throws OverloadedException if the hints cannot be written/enqueued
     */
    public static void sendToHintedEndpoints(final Mutation mutation,
                                             Iterable<InetAddress> targets,
                                             AbstractWriteResponseHandler responseHandler,
                                             String localDataCenter)
    throws OverloadedException
    {
        // extra-datacenter replicas, grouped by dc
        Map<String, Collection<InetAddress>> dcGroups = null;
        // only need to create a Message for non-local writes
        MessageOut<Mutation> message = null;

        boolean insertLocal = false;


        for (InetAddress destination : targets)
        {
            // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
            // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
            // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
            // a small number of nodes causing problems, so we should avoid shutting down writes completely to
            // healthy nodes.  Any node with no hintsInProgress is considered healthy.
            if (StorageMetrics.totalHintsInProgress.count() > maxHintsInProgress
                    && (getHintsInProgressFor(destination).get() > 0 && shouldHint(destination)))
            {
                throw new OverloadedException("Too many in flight hints: " + StorageMetrics.totalHintsInProgress.count());
            }

            if (FailureDetector.instance.isAlive(destination))
            {
                if (destination.equals(FBUtilities.getBroadcastAddress()) && OPTIMIZE_LOCAL_REQUESTS)
                {
                    insertLocal = true;
                } else
                {
                    // belongs on a different server
                    if (message == null)
                        message = mutation.createMessage();
                    String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);
                    // direct writes to local DC or old Cassandra versions
                    // (1.1 knows how to forward old-style String message IDs; updated to int in 2.0)
                    if (localDataCenter.equals(dc))
                    {
                        MessagingService.instance().sendRR(message, destination, responseHandler, true);
                    } else
                    {
                        Collection<InetAddress> messages = (dcGroups != null) ? dcGroups.get(dc) : null;
                        if (messages == null)
                        {
                            messages = new ArrayList<InetAddress>(3); // most DCs will have <= 3 replicas
                            if (dcGroups == null)
                                dcGroups = new HashMap<String, Collection<InetAddress>>();
                            dcGroups.put(dc, messages);
                        }
                        messages.add(destination);
                    }
                }
            } else
            {
                if (!shouldHint(destination))
                    continue;

                // Schedule a local hint
                submitHint(mutation, destination, responseHandler);
            }
        }

        if (insertLocal)
            insertLocal(mutation, responseHandler);

        if (dcGroups != null)
        {
            // for each datacenter, send the message to one node to relay the write to other replicas
            if (message == null)
                message = mutation.createMessage();

            for (Collection<InetAddress> dcTargets : dcGroups.values())
                sendMessagesToNonlocalDC(message, dcTargets, responseHandler);
        }
    }

    private static AtomicInteger getHintsInProgressFor(InetAddress destination)
    {
        try
        {
            return hintsInProgress.load(destination);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public static Future<Void> submitHint(final Mutation mutation,
                                          final InetAddress target,
                                          final AbstractWriteResponseHandler responseHandler)
    {
        // local write that time out should be handled by LocalMutationRunnable
        assert !target.equals(FBUtilities.getBroadcastAddress()) : target;

        HintRunnable runnable = new HintRunnable(target)
        {
            public void runMayThrow()
            {
                int ttl = HintedHandOffManager.calculateHintTTL(mutation);
                if (ttl > 0)
                {
                    logger.debug("Adding hint for {}", target);
                    writeHintForMutation(mutation, System.currentTimeMillis(), ttl, target);
                    // Notify the handler only for CL == ANY
                    if (responseHandler != null && responseHandler.consistencyLevel == ConsistencyLevel.ANY)
                        responseHandler.response(null);
                } else
                {
                    logger.debug("Skipped writing hint for {} (ttl {})", target, ttl);
                }
            }
        };

        return submitHint(runnable);
    }

    private static Future<Void> submitHint(HintRunnable runnable)
    {
        StorageMetrics.totalHintsInProgress.inc();
        getHintsInProgressFor(runnable.target).incrementAndGet();
        return (Future<Void>) StageManager.getStage(Stage.MUTATION).submit(runnable);
    }

    /**
     * @param now current time in milliseconds - relevant for hint replay handling of truncated CFs
     */
    public static void writeHintForMutation(Mutation mutation, long now, int ttl, InetAddress target)
    {
        assert ttl > 0;
        UUID hostId = StorageService.instance.getTokenMetadata().getHostId(target);
        assert hostId != null : "Missing host ID for " + target.getHostAddress();
        HintedHandOffManager.instance.hintFor(mutation, now, ttl, hostId).apply();
        StorageMetrics.totalHints.inc();
    }

    private static void sendMessagesToNonlocalDC(MessageOut<? extends IMutation> message, Collection<InetAddress> targets, AbstractWriteResponseHandler handler)
    {
        Iterator<InetAddress> iter = targets.iterator();
        InetAddress target = iter.next();

        // Add the other destinations of the same message as a FORWARD_HEADER entry
        DataOutputBuffer out = new DataOutputBuffer();
        try
        {
            out.writeInt(targets.size() - 1);
            while (iter.hasNext())
            {
                InetAddress destination = iter.next();
                CompactEndpointSerializationHelper.serialize(destination, out);
                int id = MessagingService.instance().addCallback(handler,
                                                                 message,
                                                                 destination,
                                                                 message.getTimeout(),
                                                                 handler.consistencyLevel,
                                                                 true);
                out.writeInt(id);
                logger.trace("Adding FWD message to {}@{}", id, destination);
            }
            message = message.withParameter(Mutation.FORWARD_TO, out.getData());
            // send the combined message + forward headers
            int id = MessagingService.instance().sendRR(message, target, handler, true);
            logger.trace("Sending message to {}@{}", id, target);
        }
        catch (IOException e)
        {
            // DataOutputBuffer is in-memory, doesn't throw IOException
            throw new AssertionError(e);
        }
    }

    private static void insertLocal(final Mutation mutation, final AbstractWriteResponseHandler responseHandler)
    {

        StageManager.getStage(Stage.MUTATION).maybeExecuteImmediately(new LocalMutationRunnable()
        {
            public void runMayThrow()
            {
                IMutation processed = SinkManager.processWriteRequest(mutation);
                if (processed != null)
                {
                    ((Mutation) processed).apply();
                    responseHandler.response(null);
                }
            }
        });
    }

    /**
     * Handle counter mutation on the coordinator host.
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnCoordinator
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    public static AbstractWriteResponseHandler mutateCounter(CounterMutation cm, String localDataCenter) throws UnavailableException, OverloadedException
    {
        InetAddress endpoint = findSuitableEndpoint(cm.getKeyspaceName(), cm.key(), localDataCenter, cm.consistency());

        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String keyspaceName = cm.getKeyspaceName();
            AbstractReplicationStrategy rs = Keyspace.open(keyspaceName).getReplicationStrategy();
            Token tk = StorageService.getPartitioner().getToken(cm.key());
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

            rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, cm.consistency(), null, WriteType.COUNTER).assureSufficientLiveNodes();

            // Forward the actual update to the chosen leader replica
            AbstractWriteResponseHandler responseHandler = new WriteResponseHandler(endpoint, WriteType.COUNTER);

            Tracing.trace("Enqueuing counter update to {}", endpoint);
            MessagingService.instance().sendRR(cm.makeMutationMessage(), endpoint, responseHandler, false);
            return responseHandler;
        }
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     * TODO: if we track the latency of the counter writes (which makes sense
     * contrarily to standard writes since there is a read involved), we could
     * trust the dynamic snitch entirely, which may be a better solution. It
     * is unclear we want to mix those latencies with read latencies, so this
     * may be a bit involved.
     */
    private static InetAddress findSuitableEndpoint(String keyspaceName, ByteBuffer key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, key);
        if (endpoints.isEmpty())
            // TODO have a way to compute the consistency level
            throw new UnavailableException(cl, cl.blockFor(keyspace), 0);

        List<InetAddress> localEndpoints = new ArrayList<InetAddress>();
        for (InetAddress endpoint : endpoints)
        {
            if (snitch.getDatacenter(endpoint).equals(localDataCenter))
                localEndpoints.add(endpoint);
        }
        if (localEndpoints.isEmpty())
        {
            // No endpoint in local DC, pick the closest endpoint according to the snitch
            snitch.sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);
            return endpoints.get(0);
        }
        else
        {
            return localEndpoints.get(ThreadLocalRandom.current().nextInt(localEndpoints.size()));
        }
    }

    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static AbstractWriteResponseHandler applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter, Runnable callback)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWritePerformer, callback, WriteType.COUNTER);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static AbstractWriteResponseHandler applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWriteOnCoordinatorPerformer, null, WriteType.COUNTER);
    }

    private static Runnable counterWriteTask(final IMutation mutation,
                                             final Iterable<InetAddress> targets,
                                             final AbstractWriteResponseHandler responseHandler,
                                             final String localDataCenter)
    {
        return new DroppableRunnable(MessagingService.Verb.COUNTER_MUTATION)
        {
            @Override
            public void runMayThrow() throws OverloadedException, WriteTimeoutException
            {
                IMutation processed = SinkManager.processWriteRequest(mutation);
                if (processed == null)
                    return;

                assert processed instanceof CounterMutation;
                CounterMutation cm = (CounterMutation) processed;

                Mutation result = cm.apply();
                responseHandler.response(null);

                Set<InetAddress> remotes = Sets.difference(ImmutableSet.copyOf(targets),
                            ImmutableSet.of(FBUtilities.getBroadcastAddress()));
                if (!remotes.isEmpty())
                    sendToHintedEndpoints(result, remotes, responseHandler, localDataCenter);
            }
        };
    }

    private static boolean systemKeyspaceQuery(List<ReadCommand> cmds)
    {
        for (ReadCommand cmd : cmds)
            if (!cmd.ksName.equals(SystemKeyspace.NAME))
                return false;
        return true;
    }

    public static List<Row> read(List<ReadCommand> commands, ConsistencyLevel consistencyLevel)
    throws UnavailableException, IsBootstrappingException, ReadTimeoutException, InvalidRequestException
    {
        // When using serial CL, the ClientState should be provided
        assert !consistencyLevel.isSerialConsistency();
        return read(commands, consistencyLevel, null);
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static List<Row> read(List<ReadCommand> commands, ConsistencyLevel consistencyLevel, ClientState state)
    throws UnavailableException, IsBootstrappingException, ReadTimeoutException, InvalidRequestException
    {
        if (StorageService.instance.isBootstrapMode() && !systemKeyspaceQuery(commands))
        {
            readMetrics.unavailables.mark();
            ClientRequestMetrics.readUnavailables.inc();
            throw new IsBootstrappingException();
        }

        return consistencyLevel.isSerialConsistency()
             ? readWithPaxos(commands, consistencyLevel, state)
             : readRegular(commands, consistencyLevel);
    }

    private static List<Row> readWithPaxos(List<ReadCommand> commands, ConsistencyLevel consistencyLevel, ClientState state)
    throws InvalidRequestException, UnavailableException, ReadTimeoutException
    {
        assert state != null;

        long start = System.nanoTime();
        List<Row> rows = null;

        try
        {
            // make sure any in-progress paxos writes are done (i.e., committed to a majority of replicas), before performing a quorum read
            if (commands.size() > 1)
                throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one row at a time");
            ReadCommand command = commands.get(0);

            CFMetaData metadata = Schema.instance.getCFMetaData(command.ksName, command.cfName);
            Pair<List<InetAddress>, Integer> p = getPaxosParticipants(command.ksName, command.key, consistencyLevel);
            List<InetAddress> liveEndpoints = p.left;
            int requiredParticipants = p.right;

            // does the work of applying in-progress writes; throws UAE or timeout if it can't
            final ConsistencyLevel consistencyForCommitOrFetch = consistencyLevel == ConsistencyLevel.LOCAL_SERIAL
                                                                                   ? ConsistencyLevel.LOCAL_QUORUM
                                                                                   : ConsistencyLevel.QUORUM;
            try
            {
                final Pair<UUID, Integer> pair = beginAndRepairPaxos(start, command.key, metadata, liveEndpoints, requiredParticipants, consistencyLevel, consistencyForCommitOrFetch, false, state);
                if (pair.right > 0)
                    casReadMetrics.contention.update(pair.right);
            }
            catch (WriteTimeoutException e)
            {
                throw new ReadTimeoutException(consistencyLevel, 0, consistencyLevel.blockFor(Keyspace.open(command.ksName)), false);
            }

            rows = fetchRows(commands, consistencyForCommitOrFetch);
        }
        catch (UnavailableException e)
        {
            readMetrics.unavailables.mark();
            ClientRequestMetrics.readUnavailables.inc();
            casReadMetrics.unavailables.mark();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            readMetrics.timeouts.mark();
            ClientRequestMetrics.readTimeouts.inc();
            casReadMetrics.timeouts.mark();
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            casReadMetrics.addNano(latency);
            // TODO avoid giving every command the same latency number.  Can fix this in CASSADRA-5329
            for (ReadCommand command : commands)
                Keyspace.open(command.ksName).getColumnFamilyStore(command.cfName).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }

        return rows;
    }

    private static List<Row> readRegular(List<ReadCommand> commands, ConsistencyLevel consistencyLevel)
    throws UnavailableException, ReadTimeoutException
    {
        long start = System.nanoTime();
        List<Row> rows = null;

        try
        {
            rows = fetchRows(commands, consistencyLevel);
        }
        catch (UnavailableException e)
        {
            readMetrics.unavailables.mark();
            ClientRequestMetrics.readUnavailables.inc();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            readMetrics.timeouts.mark();
            ClientRequestMetrics.readTimeouts.inc();
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            // TODO avoid giving every command the same latency number.  Can fix this in CASSADRA-5329
            for (ReadCommand command : commands)
                Keyspace.open(command.ksName).getColumnFamilyStore(command.cfName).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }

        return rows;
    }

    /**
     * This function executes local and remote reads, and blocks for the results:
     *
     * 1. Get the replica locations, sorted by response time according to the snitch
     * 2. Send a data request to the closest replica, and digest requests to either
     *    a) all the replicas, if read repair is enabled
     *    b) the closest R-1 replicas, where R is the number required to satisfy the ConsistencyLevel
     * 3. Wait for a response from R replicas
     * 4. If the digests (if any) match the data return the data
     * 5. else carry out read repair by getting data from all the nodes.
     */
    private static List<Row> fetchRows(List<ReadCommand> initialCommands, ConsistencyLevel consistencyLevel)
    throws UnavailableException, ReadTimeoutException
    {
        List<Row> rows = new ArrayList<>(initialCommands.size());
        // (avoid allocating a new list in the common case of nothing-to-retry)
        List<ReadCommand> commandsToRetry = Collections.emptyList();

        do
        {
            List<ReadCommand> commands = commandsToRetry.isEmpty() ? initialCommands : commandsToRetry;
            AbstractReadExecutor[] readExecutors = new AbstractReadExecutor[commands.size()];

            if (!commandsToRetry.isEmpty())
                Tracing.trace("Retrying {} commands", commandsToRetry.size());

            // send out read requests
            for (int i = 0; i < commands.size(); i++)
            {
                ReadCommand command = commands.get(i);
                assert !command.isDigestQuery();

                AbstractReadExecutor exec = AbstractReadExecutor.getReadExecutor(command, consistencyLevel);
                exec.executeAsync();
                readExecutors[i] = exec;
            }

            for (AbstractReadExecutor exec : readExecutors)
                exec.maybeTryAdditionalReplicas();

            // read results and make a second pass for any digest mismatches
            List<ReadCommand> repairCommands = null;
            List<ReadCallback<ReadResponse, Row>> repairResponseHandlers = null;
            for (AbstractReadExecutor exec: readExecutors)
            {
                try
                {
                    Row row = exec.get();
                    if (row != null)
                    {
                        exec.command.maybeTrim(row);
                        rows.add(row);
                    }

                    if (logger.isDebugEnabled())
                        logger.debug("Read: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - exec.handler.start));
                }
                catch (ReadTimeoutException ex)
                {
                    int blockFor = consistencyLevel.blockFor(Keyspace.open(exec.command.getKeyspace()));
                    int responseCount = exec.handler.getReceivedCount();
                    String gotData = responseCount > 0
                                   ? exec.resolver.isDataPresent() ? " (including data)" : " (only digests)"
                                   : "";

                    if (Tracing.isTracing())
                    {
                        Tracing.trace("Timed out; received {} of {} responses{}",
                                      new Object[]{ responseCount, blockFor, gotData });
                    }
                    else if (logger.isDebugEnabled())
                    {
                        logger.debug("Read timeout; received {} of {} responses{}", responseCount, blockFor, gotData);
                    }
                    throw ex;
                }
                catch (DigestMismatchException ex)
                {
                    Tracing.trace("Digest mismatch: {}", ex);

                    ReadRepairMetrics.repairedBlocking.mark();

                    // Do a full data read to resolve the correct response (and repair node that need be)
                    RowDataResolver resolver = new RowDataResolver(exec.command.ksName, exec.command.key, exec.command.filter(), exec.command.timestamp, exec.handler.endpoints.size());
                    ReadCallback<ReadResponse, Row> repairHandler = new ReadCallback<>(resolver,
                                                                                       ConsistencyLevel.ALL,
                                                                                       exec.getContactedReplicas().size(),
                                                                                       exec.command,
                                                                                       Keyspace.open(exec.command.getKeyspace()),
                                                                                       exec.handler.endpoints);

                    if (repairCommands == null)
                    {
                        repairCommands = new ArrayList<>();
                        repairResponseHandlers = new ArrayList<>();
                    }
                    repairCommands.add(exec.command);
                    repairResponseHandlers.add(repairHandler);

                    MessageOut<ReadCommand> message = exec.command.createMessage();
                    for (InetAddress endpoint : exec.getContactedReplicas())
                    {
                        Tracing.trace("Enqueuing full data read to {}", endpoint);
                        MessagingService.instance().sendRR(message, endpoint, repairHandler);
                    }
                }
            }

            commandsToRetry.clear();

            // read the results for the digest mismatch retries
            if (repairResponseHandlers != null)
            {
                for (int i = 0; i < repairCommands.size(); i++)
                {
                    ReadCommand command = repairCommands.get(i);
                    ReadCallback<ReadResponse, Row> handler = repairResponseHandlers.get(i);

                    Row row;
                    try
                    {
                        row = handler.get();
                    }
                    catch (DigestMismatchException e)
                    {
                        throw new AssertionError(e); // full data requested from each node here, no digests should be sent
                    }
                    catch (ReadTimeoutException e)
                    {
                        if (Tracing.isTracing())
                            Tracing.trace("Timed out waiting on digest mismatch repair requests");
                        else
                            logger.debug("Timed out waiting on digest mismatch repair requests");
                        // the caught exception here will have CL.ALL from the repair command,
                        // not whatever CL the initial command was at (CASSANDRA-7947)
                        int blockFor = consistencyLevel.blockFor(Keyspace.open(command.getKeyspace()));
                        throw new ReadTimeoutException(consistencyLevel, blockFor-1, blockFor, true);
                    }

                    RowDataResolver resolver = (RowDataResolver)handler.resolver;
                    try
                    {
                        // wait for the repair writes to be acknowledged, to minimize impact on any replica that's
                        // behind on writes in case the out-of-sync row is read multiple times in quick succession
                        FBUtilities.waitOnFutures(resolver.repairResults, DatabaseDescriptor.getWriteRpcTimeout());
                    }
                    catch (TimeoutException e)
                    {
                        if (Tracing.isTracing())
                            Tracing.trace("Timed out waiting on digest mismatch repair acknowledgements");
                        else
                            logger.debug("Timed out waiting on digest mismatch repair acknowledgements");
                        int blockFor = consistencyLevel.blockFor(Keyspace.open(command.getKeyspace()));
                        throw new ReadTimeoutException(consistencyLevel, blockFor-1, blockFor, true);
                    }

                    // retry any potential short reads
                    ReadCommand retryCommand = command.maybeGenerateRetryCommand(resolver, row);
                    if (retryCommand != null)
                    {
                        Tracing.trace("Issuing retry for read command");
                        if (commandsToRetry == Collections.EMPTY_LIST)
                            commandsToRetry = new ArrayList<>();
                        commandsToRetry.add(retryCommand);
                        continue;
                    }

                    if (row != null)
                    {
                        command.maybeTrim(row);
                        rows.add(row);
                    }
                }
            }
        } while (!commandsToRetry.isEmpty());

        return rows;
    }

    static class LocalReadRunnable extends DroppableRunnable
    {
        private final ReadCommand command;
        private final ReadCallback<ReadResponse, Row> handler;
        private final long start = System.nanoTime();

        LocalReadRunnable(ReadCommand command, ReadCallback<ReadResponse, Row> handler)
        {
            super(MessagingService.Verb.READ);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow()
        {
            Keyspace keyspace = Keyspace.open(command.ksName);
            Row r = command.getRow(keyspace);
            ReadResponse result = ReadVerbHandler.getResponse(command, r);
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            handler.response(result);
        }
    }

    static class LocalRangeSliceRunnable extends DroppableRunnable
    {
        private final AbstractRangeCommand command;
        private final ReadCallback<RangeSliceReply, Iterable<Row>> handler;
        private final long start = System.nanoTime();

        LocalRangeSliceRunnable(AbstractRangeCommand command, ReadCallback<RangeSliceReply, Iterable<Row>> handler)
        {
            super(MessagingService.Verb.RANGE_SLICE);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow()
        {
            RangeSliceReply result = new RangeSliceReply(command.executeLocally());
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            handler.response(result);
        }
    }

    public static List<InetAddress> getLiveSortedEndpoints(Keyspace keyspace, ByteBuffer key)
    {
        return getLiveSortedEndpoints(keyspace, StorageService.getPartitioner().decorateKey(key));
    }

    private static List<InetAddress> getLiveSortedEndpoints(Keyspace keyspace, RingPosition pos)
    {
        List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, pos);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), liveEndpoints);
        return liveEndpoints;
    }

    private static List<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2)
    {
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.
        List<InetAddress> inter = new ArrayList<InetAddress>(l1);
        inter.retainAll(l2);
        return inter;
    }

    /**
     * Estimate the number of result rows (either cql3 rows or storage rows, as called for by the command) per
     * range in the ring based on our local data.  This assumes that ranges are uniformly distributed across the cluster
     * and that the queried data is also uniformly distributed.
     */
    private static float estimateResultRowsPerRange(AbstractRangeCommand command, Keyspace keyspace)
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.columnFamily);
        float resultRowsPerRange = Float.POSITIVE_INFINITY;
        if (command.rowFilter != null && !command.rowFilter.isEmpty())
        {
            List<SecondaryIndexSearcher> searchers = cfs.indexManager.getIndexSearchersForQuery(command.rowFilter);
            if (searchers.isEmpty())
            {
                resultRowsPerRange = calculateResultRowsUsingEstimatedKeys(cfs);
            }
            else
            {
                // Secondary index query (cql3 or otherwise).  Estimate result rows based on most selective 2ary index.
                for (SecondaryIndexSearcher searcher : searchers)
                {
                    // use our own mean column count as our estimate for how many matching rows each node will have
                    SecondaryIndex highestSelectivityIndex = searcher.highestSelectivityIndex(command.rowFilter);
                    resultRowsPerRange = Math.min(resultRowsPerRange, highestSelectivityIndex.estimateResultRows());
                }
            }
        }
        else if (!command.countCQL3Rows())
        {
            // non-cql3 query
            resultRowsPerRange = cfs.estimateKeys();
        }
        else
        {
            resultRowsPerRange = calculateResultRowsUsingEstimatedKeys(cfs);
        }

        // adjust resultRowsPerRange by the number of tokens this node has and the replication factor for this ks
        return (resultRowsPerRange / DatabaseDescriptor.getNumTokens()) / keyspace.getReplicationStrategy().getReplicationFactor();
    }

    private static float calculateResultRowsUsingEstimatedKeys(ColumnFamilyStore cfs)
    {
        if (cfs.metadata.comparator.isDense())
        {
            // one storage row per result row, so use key estimate directly
            return cfs.estimateKeys();
        }
        else
        {
            float resultRowsPerStorageRow = ((float) cfs.getMeanColumns()) / cfs.metadata.regularColumns().size();
            return resultRowsPerStorageRow * (cfs.estimateKeys());
        }
    }

    public static List<Row> getRangeSlice(AbstractRangeCommand command, ConsistencyLevel consistency_level)
    throws UnavailableException, ReadTimeoutException
    {
        Tracing.trace("Computing ranges to query");
        long startTime = System.nanoTime();

        Keyspace keyspace = Keyspace.open(command.keyspace);
        List<Row> rows;
        // now scan until we have enough results
        try
        {
            int cql3RowCount = 0;
            rows = new ArrayList<>();

            // when dealing with LocalStrategy keyspaces, we can skip the range splitting and merging (which can be
            // expensive in clusters with vnodes)
            List<? extends AbstractBounds<RowPosition>> ranges;
            if (keyspace.getReplicationStrategy() instanceof LocalStrategy)
                ranges = command.keyRange.unwrap();
            else
                ranges = getRestrictedRanges(command.keyRange);

            // our estimate of how many result rows there will be per-range
            float resultRowsPerRange = estimateResultRowsPerRange(command, keyspace);
            // underestimate how many rows we will get per-range in order to increase the likelihood that we'll
            // fetch enough rows in the first round
            resultRowsPerRange -= resultRowsPerRange * CONCURRENT_SUBREQUESTS_MARGIN;
            int concurrencyFactor = resultRowsPerRange == 0.0
                                  ? 1
                                  : Math.max(1, Math.min(ranges.size(), (int) Math.ceil(command.limit() / resultRowsPerRange)));
            logger.debug("Estimated result rows per range: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
                         resultRowsPerRange, command.limit(), ranges.size(), concurrencyFactor);
            Tracing.trace("Submitting range requests on {} ranges with a concurrency of {} ({} rows per range expected)", new Object[]{ ranges.size(), concurrencyFactor, resultRowsPerRange});

            boolean haveSufficientRows = false;
            int i = 0;
            AbstractBounds<RowPosition> nextRange = null;
            List<InetAddress> nextEndpoints = null;
            List<InetAddress> nextFilteredEndpoints = null;
            while (i < ranges.size())
            {
                List<Pair<AbstractRangeCommand, ReadCallback<RangeSliceReply, Iterable<Row>>>> scanHandlers = new ArrayList<>(concurrencyFactor);
                int concurrentFetchStartingIndex = i;
                int concurrentRequests = 0;
                while ((i - concurrentFetchStartingIndex) < concurrencyFactor)
                {
                    AbstractBounds<RowPosition> range = nextRange == null
                                                      ? ranges.get(i)
                                                      : nextRange;
                    List<InetAddress> liveEndpoints = nextEndpoints == null
                                                    ? getLiveSortedEndpoints(keyspace, range.right)
                                                    : nextEndpoints;
                    List<InetAddress> filteredEndpoints = nextFilteredEndpoints == null
                                                        ? consistency_level.filterForQuery(keyspace, liveEndpoints)
                                                        : nextFilteredEndpoints;
                    ++i;
                    ++concurrentRequests;

                    // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
                    // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
                    // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
                    while (i < ranges.size())
                    {
                        nextRange = ranges.get(i);
                        nextEndpoints = getLiveSortedEndpoints(keyspace, nextRange.right);
                        nextFilteredEndpoints = consistency_level.filterForQuery(keyspace, nextEndpoints);

                        // If the current range right is the min token, we should stop merging because CFS.getRangeSlice
                        // don't know how to deal with a wrapping range.
                        // Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
                        // the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
                        // wire compatibility, so It's likely easier not to bother;
                        if (range.right.isMinimum())
                            break;

                        List<InetAddress> merged = intersection(liveEndpoints, nextEndpoints);

                        // Check if there is enough endpoint for the merge to be possible.
                        if (!consistency_level.isSufficientLiveNodes(keyspace, merged))
                            break;

                        List<InetAddress> filteredMerged = consistency_level.filterForQuery(keyspace, merged);

                        // Estimate whether merging will be a win or not
                        if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(filteredMerged, filteredEndpoints, nextFilteredEndpoints))
                            break;

                        // If we get there, merge this range and the next one
                        range = range.withNewRight(nextRange.right);
                        liveEndpoints = merged;
                        filteredEndpoints = filteredMerged;
                        ++i;
                    }

                    AbstractRangeCommand nodeCmd = command.forSubRange(range);

                    // collect replies and resolve according to consistency level
                    RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(nodeCmd.keyspace, command.timestamp);
                    List<InetAddress> minimalEndpoints = filteredEndpoints.subList(0, Math.min(filteredEndpoints.size(), consistency_level.blockFor(keyspace)));
                    ReadCallback<RangeSliceReply, Iterable<Row>> handler = new ReadCallback<>(resolver, consistency_level, nodeCmd, minimalEndpoints);
                    handler.assureSufficientLiveNodes();
                    resolver.setSources(filteredEndpoints);
                    if (filteredEndpoints.size() == 1
                        && filteredEndpoints.get(0).equals(FBUtilities.getBroadcastAddress())
                        && OPTIMIZE_LOCAL_REQUESTS)
                    {
                        StageManager.getStage(Stage.READ).execute(new LocalRangeSliceRunnable(nodeCmd, handler), Tracing.instance.get());
                    }
                    else
                    {
                        MessageOut<? extends AbstractRangeCommand> message = nodeCmd.createMessage();
                        for (InetAddress endpoint : filteredEndpoints)
                        {
                            Tracing.trace("Enqueuing request to {}", endpoint);
                            MessagingService.instance().sendRR(message, endpoint, handler);
                        }
                    }
                    scanHandlers.add(Pair.create(nodeCmd, handler));
                }
                Tracing.trace("Submitted {} concurrent range requests covering {} ranges", concurrentRequests, i - concurrentFetchStartingIndex);

                List<AsyncOneResponse> repairResponses = new ArrayList<>();
                for (Pair<AbstractRangeCommand, ReadCallback<RangeSliceReply, Iterable<Row>>> cmdPairHandler : scanHandlers)
                {
                    AbstractRangeCommand nodeCmd = cmdPairHandler.left;
                    ReadCallback<RangeSliceReply, Iterable<Row>> handler = cmdPairHandler.right;
                    RangeSliceResponseResolver resolver = (RangeSliceResponseResolver)handler.resolver;

                    try
                    {
                        for (Row row : handler.get())
                        {
                            rows.add(row);
                            if (nodeCmd.countCQL3Rows())
                                cql3RowCount += row.getLiveCount(command.predicate, command.timestamp);
                        }
                        repairResponses.addAll(resolver.repairResults);
                    }
                    catch (ReadTimeoutException ex)
                    {
                        // we timed out waiting for responses
                        int blockFor = consistency_level.blockFor(keyspace);
                        int responseCount = resolver.responses.size();
                        String gotData = responseCount > 0
                                         ? resolver.isDataPresent() ? " (including data)" : " (only digests)"
                                         : "";

                        if (Tracing.isTracing())
                        {
                            Tracing.trace("Timed out; received {} of {} responses{} for range {} of {}",
                                          new Object[]{ responseCount, blockFor, gotData, i, ranges.size() });
                        }
                        else if (logger.isDebugEnabled())
                        {
                            logger.debug("Range slice timeout; received {} of {} responses{} for range {} of {}",
                                         responseCount, blockFor, gotData, i, ranges.size());
                        }
                        throw ex;
                    }
                    catch (DigestMismatchException e)
                    {
                        throw new AssertionError(e); // no digests in range slices yet
                    }

                    // if we're done, great, otherwise, move to the next range
                    int count = nodeCmd.countCQL3Rows() ? cql3RowCount : rows.size();
                    if (count >= nodeCmd.limit())
                    {
                        haveSufficientRows = true;
                        break;
                    }
                }

                try
                {
                    FBUtilities.waitOnFutures(repairResponses, DatabaseDescriptor.getWriteRpcTimeout());
                }
                catch (TimeoutException ex)
                {
                    // We got all responses, but timed out while repairing
                    int blockFor = consistency_level.blockFor(keyspace);
                    if (Tracing.isTracing())
                        Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
                    else
                        logger.debug("Range slice timeout while read-repairing after receiving all {} data and digest responses", blockFor);
                    throw new ReadTimeoutException(consistency_level, blockFor-1, blockFor, true);
                }

                if (haveSufficientRows)
                    return trim(command, rows);

                // we didn't get enough rows in our concurrent fetch; recalculate our concurrency factor
                // based on the results we've seen so far (as long as we still have ranges left to query)
                if (i < ranges.size())
                {
                    float fetchedRows = command.countCQL3Rows() ? cql3RowCount : rows.size();
                    float remainingRows = command.limit() - fetchedRows;
                    float actualRowsPerRange;
                    if (fetchedRows == 0.0)
                    {
                        // we haven't actually gotten any results, so query all remaining ranges at once
                        actualRowsPerRange = 0.0f;
                        concurrencyFactor = ranges.size() - i;
                    }
                    else
                    {
                        actualRowsPerRange = i / fetchedRows;
                        concurrencyFactor = Math.max(1, Math.min(ranges.size() - i, Math.round(remainingRows / actualRowsPerRange)));
                    }
                    logger.debug("Didn't get enough response rows; actual rows per range: {}; remaining rows: {}, new concurrent requests: {}",
                                 actualRowsPerRange, (int) remainingRows, concurrencyFactor);
                }
            }
        }
        finally
        {
            long latency = System.nanoTime() - startTime;
            rangeMetrics.addNano(latency);
            Keyspace.open(command.keyspace).getColumnFamilyStore(command.columnFamily).metric.coordinatorScanLatency.update(latency, TimeUnit.NANOSECONDS);
        }
        return trim(command, rows);
    }

    private static List<Row> trim(AbstractRangeCommand command, List<Row> rows)
    {
        // When maxIsColumns, we let the caller trim the result.
        if (command.countCQL3Rows())
            return rows;
        else
            return rows.size() > command.limit() ? rows.subList(0, command.limit()) : rows;
    }

    public Map<String, List<String>> getSchemaVersions()
    {
        return describeSchemaVersions();
    }

    /**
     * initiate a request/response session with each live node to check whether or not everybody is using the same
     * migration id. This is useful for determining if a schema change has propagated through the cluster. Disagreement
     * is assumed if any node fails to respond.
     */
    public static Map<String, List<String>> describeSchemaVersions()
    {
        final String myVersion = Schema.instance.getVersion().toString();
        final Map<InetAddress, UUID> versions = new ConcurrentHashMap<InetAddress, UUID>();
        final Set<InetAddress> liveHosts = Gossiper.instance.getLiveMembers();
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());

        IAsyncCallback<UUID> cb = new IAsyncCallback<UUID>()
        {
            public void response(MessageIn<UUID> message)
            {
                // record the response from the remote node.
                versions.put(message.from, message.payload);
                latch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        // an empty message acts as a request to the SchemaCheckVerbHandler.
        MessageOut message = new MessageOut(MessagingService.Verb.SCHEMA_CHECK);
        for (InetAddress endpoint : liveHosts)
            MessagingService.instance().sendRR(message, endpoint, cb);

        try
        {
            // wait for as long as possible. timeout-1s if possible.
            latch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        // maps versions to hosts that are on that version.
        Map<String, List<String>> results = new HashMap<String, List<String>>();
        Iterable<InetAddress> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());
        for (InetAddress host : allHosts)
        {
            UUID version = versions.get(host);
            String stringVersion = version == null ? UNREACHABLE : version.toString();
            List<String> hosts = results.get(stringVersion);
            if (hosts == null)
            {
                hosts = new ArrayList<String>();
                results.put(stringVersion, hosts);
            }
            hosts.add(host.getHostAddress());
        }

        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        if (results.get(UNREACHABLE) != null)
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", StringUtils.join(results.get(UNREACHABLE), ","));
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.getKey().equals(UNREACHABLE) || entry.getKey().equals(myVersion))
                continue;
            for (String host : entry.getValue())
                logger.debug("{} disagrees ({})", host, entry.getKey());
        }
        if (results.size() == 1)
            logger.debug("Schemas are in agreement.");

        return results;
    }

    /**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    static <T extends RingPosition<T>> List<AbstractBounds<T>> getRestrictedRanges(final AbstractBounds<T> queryRange)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.isMinimum())
        {
            return Collections.singletonList(queryRange);
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<AbstractBounds<T>> ranges = new ArrayList<AbstractBounds<T>>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left.getToken(), true);
        AbstractBounds<T> remainder = queryRange;
        while (ringIter.hasNext())
        {
            /*
             * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
             *   - if remainder is tokens, then we'll just split using the provided token.
             *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
             *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
             *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
             *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
             *     keys having 15 as token and B include none of those (since that is what our node owns).
             * asSplitValue() abstracts that choice.
             */
            Token upperBoundToken = ringIter.next();
            T upperBound = (T)upperBoundToken.upperBound(queryRange.left.getClass());
            if (!remainder.left.equals(upperBound) && !remainder.contains(upperBound))
                // no more splits
                break;
            Pair<AbstractBounds<T>,AbstractBounds<T>> splits = remainder.split(upperBound);
            if (splits == null)
                continue;

            ranges.add(splits.left);
            remainder = splits.right;
        }
        ranges.add(remainder);

        return ranges;
    }

    public long getReadOperations()
    {
        return readMetrics.latency.count();
    }

    public long getTotalReadLatencyMicros()
    {
        return readMetrics.totalLatency.count();
    }

    public double getRecentReadLatencyMicros()
    {
        return readMetrics.getRecentLatency();
    }

    public long[] getTotalReadLatencyHistogramMicros()
    {
        return readMetrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentReadLatencyHistogramMicros()
    {
        return readMetrics.recentLatencyHistogram.getBuckets(true);
    }

    public long getRangeOperations()
    {
        return rangeMetrics.latency.count();
    }

    public long getTotalRangeLatencyMicros()
    {
        return rangeMetrics.totalLatency.count();
    }

    public double getRecentRangeLatencyMicros()
    {
        return rangeMetrics.getRecentLatency();
    }

    public long[] getTotalRangeLatencyHistogramMicros()
    {
        return rangeMetrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentRangeLatencyHistogramMicros()
    {
        return rangeMetrics.recentLatencyHistogram.getBuckets(true);
    }

    public long getWriteOperations()
    {
        return writeMetrics.latency.count();
    }

    public long getTotalWriteLatencyMicros()
    {
        return writeMetrics.totalLatency.count();
    }

    public double getRecentWriteLatencyMicros()
    {
        return writeMetrics.getRecentLatency();
    }

    public long[] getTotalWriteLatencyHistogramMicros()
    {
        return writeMetrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentWriteLatencyHistogramMicros()
    {
        return writeMetrics.recentLatencyHistogram.getBuckets(true);
    }

    public boolean getHintedHandoffEnabled()
    {
        return DatabaseDescriptor.hintedHandoffEnabled();
    }

    public Set<String> getHintedHandoffEnabledByDC()
    {
        return DatabaseDescriptor.hintedHandoffEnabledByDC();
    }

    public void setHintedHandoffEnabled(boolean b)
    {
        DatabaseDescriptor.setHintedHandoffEnabled(b);
    }

    public void setHintedHandoffEnabledByDCList(String dcNames)
    {
        DatabaseDescriptor.setHintedHandoffEnabled(dcNames);
    }

    public int getMaxHintWindow()
    {
        return DatabaseDescriptor.getMaxHintWindow();
    }

    public void setMaxHintWindow(int ms)
    {
        DatabaseDescriptor.setMaxHintWindow(ms);
    }

    public static boolean shouldHint(InetAddress ep)
    {
        if (DatabaseDescriptor.shouldHintByDC())
        {
            final String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(ep);
            //Disable DC specific hints
            if(!DatabaseDescriptor.hintedHandoffEnabled(dc))
            {
                HintedHandOffManager.instance.metrics.incrPastWindow(ep);
                return false;
            }
        }
        else if (!DatabaseDescriptor.hintedHandoffEnabled())
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            return false;
        }

        boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(ep) > DatabaseDescriptor.getMaxHintWindow();
        if (hintWindowExpired)
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            Tracing.trace("Not hinting {} which has been down {}ms", ep, Gossiper.instance.getEndpointDowntime(ep));
        }
        return !hintWindowExpired;
    }

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     * @throws UnavailableException If some of the hosts in the ring are down.
     * @throws TimeoutException
     * @throws IOException
     */
    public static void truncateBlocking(String keyspace, String cfname) throws UnavailableException, TimeoutException, IOException
    {
        logger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);
        if (isAnyStorageHostDown())
        {
            logger.info("Cannot perform truncate, some hosts are down");
            // Since the truncate operation is so aggressive and is typically only
            // invoked by an admin, for simplicity we require that all nodes are up
            // to perform the operation.
            int liveMembers = Gossiper.instance.getLiveMembers().size();
            throw new UnavailableException(ConsistencyLevel.ALL, liveMembers + Gossiper.instance.getUnreachableMembers().size(), liveMembers);
        }

        Set<InetAddress> allEndpoints = Gossiper.instance.getLiveTokenOwners();
        
        int blockFor = allEndpoints.size();
        final TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);

        // Send out the truncate calls and track the responses with the callbacks.
        Tracing.trace("Enqueuing truncate messages to hosts {}", allEndpoints);
        final Truncation truncation = new Truncation(keyspace, cfname);
        MessageOut<Truncation> message = truncation.createMessage();
        for (InetAddress endpoint : allEndpoints)
            MessagingService.instance().sendRR(message, endpoint, responseHandler);

        // Wait for all
        try
        {
            responseHandler.get();
        }
        catch (TimeoutException e)
        {
            Tracing.trace("Timed out");
            throw e;
        }
    }

    /**
     * Asks the gossiper if there are any nodes that are currently down.
     * @return true if the gossiper thinks all nodes are up.
     */
    private static boolean isAnyStorageHostDown()
    {
        return !Gossiper.instance.getUnreachableTokenOwners().isEmpty();
    }
    
    public interface WritePerformer
    {
        public void apply(IMutation mutation,
                          Iterable<InetAddress> targets,
                          AbstractWriteResponseHandler responseHandler,
                          String localDataCenter,
                          ConsistencyLevel consistencyLevel) throws OverloadedException;
    }

    /**
     * A Runnable that aborts if it doesn't start running before it times out
     */
    private static abstract class DroppableRunnable implements Runnable
    {
        private final long constructionTime = System.nanoTime();
        private final MessagingService.Verb verb;

        public DroppableRunnable(MessagingService.Verb verb)
        {
            this.verb = verb;
        }

        public final void run()
        {

            if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - constructionTime) > DatabaseDescriptor.getTimeout(verb))
            {
                MessagingService.instance().incrementDroppedMessages(verb);
                return;
            }
            try
            {
                runMayThrow();
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * Like DroppableRunnable, but if it aborts, it will rerun (on the mutation stage) after
     * marking itself as a hint in progress so that the hint backpressure mechanism can function.
     */
    private static abstract class LocalMutationRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();

        public final void run()
        {
            if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getTimeout(MessagingService.Verb.MUTATION))
            {
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb.MUTATION);
                HintRunnable runnable = new HintRunnable(FBUtilities.getBroadcastAddress())
                {
                    protected void runMayThrow() throws Exception
                    {
                        LocalMutationRunnable.this.runMayThrow();
                    }
                };
                submitHint(runnable);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * HintRunnable will decrease totalHintsInProgress and targetHints when finished.
     * It is the caller's responsibility to increment them initially.
     */
    private abstract static class HintRunnable implements Runnable
    {
        public final InetAddress target;

        protected HintRunnable(InetAddress target)
        {
            this.target = target;
        }

        public void run()
        {
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                StorageMetrics.totalHintsInProgress.dec();
                getHintsInProgressFor(target).decrementAndGet();
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    public long getTotalHints()
    {
        return StorageMetrics.totalHints.count();
    }

    public int getMaxHintsInProgress()
    {
        return maxHintsInProgress;
    }

    public void setMaxHintsInProgress(int qs)
    {
        maxHintsInProgress = qs;
    }

    public int getHintsInProgress()
    {
        return (int) StorageMetrics.totalHintsInProgress.count();
    }

    public void verifyNoHintsInProgress()
    {
        if (getHintsInProgress() > 0)
            logger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
    }

    public Long getRpcTimeout() { return DatabaseDescriptor.getRpcTimeout(); }
    public void setRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRpcTimeout(timeoutInMillis); }

    public Long getReadRpcTimeout() { return DatabaseDescriptor.getReadRpcTimeout(); }
    public void setReadRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis); }

    public Long getWriteRpcTimeout() { return DatabaseDescriptor.getWriteRpcTimeout(); }
    public void setWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis); }

    public Long getCounterWriteRpcTimeout() { return DatabaseDescriptor.getCounterWriteRpcTimeout(); }
    public void setCounterWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCounterWriteRpcTimeout(timeoutInMillis); }

    public Long getCasContentionTimeout() { return DatabaseDescriptor.getCasContentionTimeout(); }
    public void setCasContentionTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCasContentionTimeout(timeoutInMillis); }

    public Long getRangeRpcTimeout() { return DatabaseDescriptor.getRangeRpcTimeout(); }
    public void setRangeRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis); }

    public Long getTruncateRpcTimeout() { return DatabaseDescriptor.getTruncateRpcTimeout(); }
    public void setTruncateRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis); }
    public void reloadTriggerClasses() { TriggerExecutor.instance.reloadClasses(); }

    
    public long getReadRepairAttempted() {
        return ReadRepairMetrics.attempted.count();
    }
    
    public long getReadRepairRepairedBlocking() {
        return ReadRepairMetrics.repairedBlocking.count();
    }
    
    public long getReadRepairRepairedBackground() {
        return ReadRepairMetrics.repairedBackground.count();
    }
}
