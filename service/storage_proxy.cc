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

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "db/serializer.hh"
#include "storage_proxy.hh"
#include "unimplemented.hh"
#include "frozen_mutation.hh"
#include "query_result_merger.hh"
#include "core/do_with.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "db/serializer.hh"
#include "storage_service.hh"
#include "core/future-util.hh"
#include "db/read_repair_decision.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/algorithm/count_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm/sort.hpp>
#include "utils/latency.hh"
#include "schema.hh"

namespace service {

static logging::logger logger("storage_proxy");

distributed<service::storage_proxy> _the_storage_proxy;

using namespace exceptions;

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

static inline
const dht::token& start_token(const query::partition_range& r) {
    static const dht::token min_token = dht::minimum_token();
    return r.start() ? r.start()->value().token() : min_token;
}

static inline
const dht::token& end_token(const query::partition_range& r) {
    static const dht::token max_token = dht::maximum_token();
    return r.end() ? r.end()->value().token() : max_token;
}

class abstract_write_response_handler {
protected:
    semaphore _ready; // available when cl is achieved
    db::consistency_level _cl;
    keyspace& _ks;
    db::write_type _type;
    lw_shared_ptr<const frozen_mutation> _mutation;
    std::unordered_set<gms::inet_address> _targets; // who we sent this mutation to
    size_t _pending_endpoints; // how many endpoints in bootstrap state there is
    // added dead_endpoints as a memeber here as well. This to be able to carry the info across
    // calls in helper methods in a convinient way. Since we hope this will be empty most of the time
    // it should not be a huge burden. (flw)
    std::vector<gms::inet_address> _dead_endpoints;
    size_t _cl_acks = 0;
    virtual size_t total_block_for() {
        // original comment from cassandra:
        // during bootstrap, include pending endpoints in the count
        // or we may fail the consistency level guarantees (see #833, #8058)
        return db::block_for(_ks, _cl) + _pending_endpoints;
    }
    virtual void signal(gms::inet_address from) {
        signal();
    }
public:
    abstract_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type,
            lw_shared_ptr<const frozen_mutation> mutation,
            std::unordered_set<gms::inet_address> targets,
            size_t pending_endpoints = 0,
            std::vector<gms::inet_address> dead_endpoints = {})
            : _ready(0), _cl(cl), _ks(ks), _type(type), _mutation(std::move(mutation)), _targets(
                    std::move(targets)), _pending_endpoints(pending_endpoints), _dead_endpoints(
                    std::move(dead_endpoints)) {
    }
    virtual ~abstract_write_response_handler() {};
    void signal(size_t nr = 1) {
        _cl_acks += nr;
        _ready.signal(nr);
    }
    // return true on last ack
    bool response(gms::inet_address from) {
        signal(from);
        auto it = _targets.find(from);
        assert(it != _targets.end());
        _targets.erase(it);
        return _targets.size() == 0;
    }
    future<> wait() {
        return _ready.wait(total_block_for());
    }
    const std::unordered_set<gms::inet_address>& get_targets() const {
        return _targets;
    }
    const std::vector<gms::inet_address>& get_dead_endpoints() const {
        return _dead_endpoints;
    }
    lw_shared_ptr<const frozen_mutation> get_mutation() {
        return _mutation;
    }
    friend storage_proxy;
};

class datacenter_write_response_handler : public abstract_write_response_handler {
    void signal(gms::inet_address from) override {
        if (is_me(from) || db::is_local(from)) {
            abstract_write_response_handler::signal();
        }
    }
public:
    using abstract_write_response_handler::abstract_write_response_handler;
};

class write_response_handler : public abstract_write_response_handler {
public:
    using abstract_write_response_handler::abstract_write_response_handler;
};

class datacenter_sync_write_response_handler : public abstract_write_response_handler {
    std::unordered_map<sstring, size_t> _dc_responses;
    void signal(gms::inet_address from) override {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
        sstring data_center = snitch_ptr->get_datacenter(from);
        auto dc_resp = _dc_responses.find(data_center);

        if (dc_resp->second > 0) {
            --dc_resp->second;
            abstract_write_response_handler::signal();
        }
    }
public:
    datacenter_sync_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, lw_shared_ptr<const frozen_mutation> mutation, std::unordered_set<gms::inet_address> targets, size_t pending_endpoints, std::vector<gms::inet_address> dead_endpoints) :
        abstract_write_response_handler(ks, cl, type, std::move(mutation), targets, pending_endpoints, dead_endpoints) {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();

        for (auto& target : targets) {
            auto dc = snitch_ptr->get_datacenter(target);

            if (_dc_responses.find(dc) == _dc_responses.end()) {
                _dc_responses.emplace(dc, db::local_quorum_for(ks, dc));
            }
        }
    }
};

storage_proxy::response_id_type storage_proxy::register_response_handler(std::unique_ptr<abstract_write_response_handler>&& h) {
    auto id = _next_response_id++;
    auto e = _response_handlers.emplace(id, rh_entry(std::move(h), shared_from_this(), [this, id] {
        auto& e = _response_handlers.find(id)->second;
        auto block_for = e.handler->total_block_for();
        auto left_for_cl = block_for - e.handler->_cl_acks;
        if (left_for_cl <= 0 || e.handler->_cl == db::consistency_level::ANY) {
            // we are here because either cl was achieved, but targets left in the handler are not
            // responding, so a hint should be written for them, or cl == any in which case
            // hints are counted towards consistency, so we need to write hints and count how much was written
            e.handler->signal(hint_to_dead_endpoints(e.handler->get_mutation(), e.handler->get_targets()));
            logger.trace("Wrote hint to satisfy CL.ANY after no replicas acknowledged the write");
            // check cl status after hints are written (can change for cl == any)
            left_for_cl = block_for - e.handler->_cl_acks;
        }

        if (left_for_cl > 0) {
            // timeout happened before cl was achieved, throw exception
            e.handler->_ready.broken(mutation_write_timeout_exception(e.handler->_cl, e.handler->_cl_acks, block_for, e.handler->_type));
        } else {
            remove_response_handler(id);
        }
    }));
    assert(e.second);
    return id;
}

void storage_proxy::remove_response_handler(storage_proxy::response_id_type id) {
    _response_handlers.erase(id);
}

void storage_proxy::got_response(storage_proxy::response_id_type id, gms::inet_address from) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        if (it->second.handler->response(from)) {
            remove_response_handler(id); // last one, remove entry. Will cancel expiration timer too.
        }
    }
}

future<> storage_proxy::response_wait(storage_proxy::response_id_type id) {
    auto& e = _response_handlers.find(id)->second;

    e.expire_timer.arm(std::chrono::milliseconds(_db.local().get_config().write_request_timeout_in_ms()));

    return e.handler->wait();
}

abstract_write_response_handler& storage_proxy::get_write_response_handler(storage_proxy::response_id_type id) {
        return *_response_handlers.find(id)->second.handler;
}

storage_proxy::response_id_type storage_proxy::create_write_response_handler(keyspace& ks, db::consistency_level cl, db::write_type type, frozen_mutation&& mutation, std::unordered_set<gms::inet_address> targets, std::vector<gms::inet_address>& pending_endpoints, std::vector<gms::inet_address> dead_endpoints)
{
    std::unique_ptr<abstract_write_response_handler> h;
    auto& rs = ks.get_replication_strategy();
    size_t pending_count = pending_endpoints.size();

    auto m = make_lw_shared<const frozen_mutation>(std::move(mutation));

    // for now make is simple
    if (db::is_datacenter_local(cl)) {
        pending_count = std::count_if(pending_endpoints.begin(), pending_endpoints.end(), db::is_local);
        h = std::make_unique<datacenter_write_response_handler>(ks, cl, type, std::move(m), std::move(targets), pending_count, std::move(dead_endpoints));
    } else if (cl == db::consistency_level::EACH_QUORUM && rs.get_type() == locator::replication_strategy_type::network_topology){
        h = std::make_unique<datacenter_sync_write_response_handler>(ks, cl, type, std::move(m), std::move(targets), pending_count, std::move(dead_endpoints));
    } else {
        h = std::make_unique<write_response_handler>(ks, cl, type, std::move(m), std::move(targets), pending_count, std::move(dead_endpoints));
    }
    return register_response_handler(std::move(h));
}

storage_proxy::~storage_proxy() {}
storage_proxy::storage_proxy(distributed<database>& db) : _db(db) {
    init_messaging_service();
}

storage_proxy::rh_entry::rh_entry(std::unique_ptr<abstract_write_response_handler>&& h, shared_ptr<storage_proxy> p, std::function<void()>&& cb) : handler(std::move(h)), proxy(p), expire_timer(std::move(cb)) {}

#if 0
    static
    {
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
#endif


future<>
storage_proxy::mutate_locally(const mutation& m) {
    auto shard = _db.local().shard_of(m);
    return _db.invoke_on(shard, [m = freeze(m)] (database& db) -> future<> {
        return db.apply(m);
    });
}

future<>
storage_proxy::mutate_locally(const frozen_mutation& m) {
    auto shard = _db.local().shard_of(m);
    return _db.invoke_on(shard, [&m] (database& db) -> future<> {
        return db.apply(m);
    });
}

future<>
storage_proxy::mutate_locally(std::vector<mutation> mutations) {
    return do_with(std::move(mutations), [this] (std::vector<mutation>& pmut){
        return parallel_for_each(pmut.begin(), pmut.end(), [this] (const mutation& m) {
            return mutate_locally(m);
        });
    });
}

/**
 * Helper for create_write_response_handler, shared across mutate/mutate_atomically.
 * Both methods do roughly the same thing, with the latter intermixing batch log ops
 * in the logic.
 * Since ordering is (maybe?) significant, we need to carry some info across from here
 * to the hint method below (dead nodes).
 */
storage_proxy::response_id_type
storage_proxy::create_write_response_handler(const mutation& m, db::consistency_level cl, db::write_type type) {
    keyspace& ks = _db.local().find_keyspace(m.schema()->ks_name());
    auto& rs = ks.get_replication_strategy();
    std::vector<gms::inet_address> natural_endpoints = rs.get_natural_endpoints(m.token());
    std::vector<gms::inet_address> pending_endpoints = get_local_storage_service().get_token_metadata().pending_endpoints_for(m.token(), ks);

    auto all = boost::range::join(natural_endpoints, pending_endpoints);

    if (std::find_if(all.begin(), all.end(), std::bind1st(std::mem_fn(&storage_proxy::cannot_hint), this)) != all.end()) {
        // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
        // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
        // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
        // a small number of nodes causing problems, so we should avoid shutting down writes completely to
        // healthy nodes.  Any node with no hintsInProgress is considered healthy.
        throw overloaded_exception(_total_hints_in_progress);
    }

    // filter live endpoints from dead ones
    std::unordered_set<gms::inet_address> live_endpoints;
    std::vector<gms::inet_address> dead_endpoints;
    live_endpoints.reserve(all.size());
    dead_endpoints.reserve(all.size());
    std::partition_copy(all.begin(), all.end(), std::inserter(live_endpoints, live_endpoints.begin()), std::back_inserter(dead_endpoints),
            std::bind1st(std::mem_fn(&gms::failure_detector::is_alive), &gms::get_local_failure_detector()));

    db::assure_sufficient_live_nodes(cl, ks, live_endpoints);

    return create_write_response_handler(ks, cl, type, freeze(m), std::move(live_endpoints), pending_endpoints, dead_endpoints);
}

void
storage_proxy::hint_to_dead_endpoints(response_id_type id, db::consistency_level cl) {
    auto& h = get_write_response_handler(id);

    size_t hints = hint_to_dead_endpoints(h.get_mutation(), h.get_dead_endpoints());

    if (cl == db::consistency_level::ANY) {
        // for cl==ANY hints are counted towards consistency
        h.signal(hints);
    }
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
future<>
storage_proxy::mutate(std::vector<mutation> mutations, db::consistency_level cl) {
    auto local_addr = utils::fb_utilities::get_broadcast_address();
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    sstring local_dc = snitch_ptr->get_datacenter(local_addr);
    auto type = mutations.size() == 1 ? db::write_type::SIMPLE : db::write_type::UNLOGGED_BATCH;
    utils::latency_counter lc;
    lc.start();

    return parallel_for_each(mutations, [this, cl, type, &local_dc] (mutation& m) {
        storage_proxy::response_id_type response_id = create_write_response_handler(m, cl, type);
        // it is better to send first and hint afterwards to reduce latency
        // but request may complete before hint_to_dead_endpoints() is called and
        // response_id handler will be removed, so we will have to do hint with separate
        // frozen_mutation copy, or manage handler live time differently.
        hint_to_dead_endpoints(response_id, cl);

        // call before send_to_live_endpoints() for the same reason as above
        auto f = response_wait(response_id);
        send_to_live_endpoints(response_id, local_dc);
        return f.handle_exception([this, response_id, cl] (std::exception_ptr exp) {
            remove_response_handler(response_id); // cancel expire_timer, so no hint will happen
            //std::rethrow_exception(ex);
            return make_exception_future<>(exp);
        });
    }).then_wrapped([this, p = shared_from_this(), lc] (future<>&& f) mutable {
        _stats.write.mark(lc.stop().latency_in_nano());
        try {
            f.get();
            return make_ready_future<>();
        } catch (no_such_keyspace& ex) {
            logger.trace("Write to non existing keyspace: {}", ex.what());
            return make_exception_future<>(std::current_exception());
        } catch(mutation_write_timeout_exception& ex) {
            // timeout
            logger.trace("Write timeout; received {} of {} required replies", ex.received, ex.block_for);
            _stats.write_timeouts++;
            return make_exception_future<>(std::current_exception());
        } catch (exceptions::unavailable_exception& ex) {
            _stats.write_unavailables++;
            logger.trace("Unavailable");
            return make_exception_future<>(std::current_exception());
        }  catch(overloaded_exception& ex) {
            _stats.write_unavailables++;
            logger.trace("Overloaded");
            return make_exception_future<>(std::current_exception());
        }
    });
}

future<>
storage_proxy::mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl,
        bool should_mutate_atomically) {
    warn(unimplemented::cause::TRIGGERS);
#if 0
        Collection<Mutation> augmented = TriggerExecutor.instance.execute(mutations);
        if (augmented != null) {
            return mutate_atomically(augmented, consistencyLevel);
        } else {
#endif
    if (should_mutate_atomically) {
        return mutate_atomically(std::move(mutations), cl);
    }
    return mutate(std::move(mutations), cl);
#if 0
    }
#endif
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
future<>
storage_proxy::mutate_atomically(std::vector<mutation> mutations, db::consistency_level cl) {

    utils::latency_counter lc;
    lc.start();

    class context {
        storage_proxy & _p;
        std::vector<mutation> _mutations;
        db::consistency_level _cl;

        const gms::inet_address _local_addr;
        const sstring _local_dc;

        const utils::UUID _batch_uuid;
        const std::unordered_set<gms::inet_address> _batchlog_endpoints;

    public:
        context(storage_proxy & p, std::vector<mutation>&& mutations,
                db::consistency_level cl)
                : _p(p), _mutations(std::move(mutations)), _cl(cl), _local_addr(
                        utils::fb_utilities::get_broadcast_address()), _local_dc(
                        locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(
                                _local_addr)), _batch_uuid(
                        utils::UUID_gen::get_time_UUID()), _batchlog_endpoints(
                        [this]() -> std::unordered_set<gms::inet_address> {
                            auto topology = service::get_storage_service().local().get_token_metadata().get_topology();
                            auto local_endpoints = topology.get_datacenter_racks().at(_local_dc); // note: origin copies, so do that here too...
                            auto local_rack = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_rack(_local_addr);
                            auto chosen_endpoints = db::get_batchlog_manager().local().endpoint_filter(local_rack, local_endpoints);

                            if (chosen_endpoints.empty()) {
                                if (_cl == db::consistency_level::ANY) {
                                    return {_local_addr};
                                }
                                throw exceptions::unavailable_exception(db::consistency_level::ONE, 1, 0);
                            }
                            return chosen_endpoints;
                        }()) {
        }
        future<> sync_write_to_batchlog() {
            auto m = db::get_batchlog_manager().local().get_batch_log_mutation_for(_mutations, _batch_uuid, net::messaging_service::current_version);
            auto h = std::make_unique<write_response_handler>(_p._db.local().find_keyspace(db::system_keyspace::NAME), db::consistency_level::ONE, db::write_type::BATCH_LOG, make_lw_shared<frozen_mutation>(freeze(m)), _batchlog_endpoints);
            response_id_type response_id = _p.register_response_handler(std::move(h));

            auto f = _p.response_wait(response_id);
            _p.send_to_live_endpoints(response_id, _local_dc);
            return f.handle_exception([this, response_id](auto ex) {
                _p.remove_response_handler(response_id); // cancel expire_timer, so no hint will happen
                //return make_exception_future(ex);
                std::rethrow_exception(ex);
            });
        };
        void async_remove_from_batchlog() {
            // delete batch
            auto schema = _p._db.local().find_schema(db::system_keyspace::NAME, db::system_keyspace::BATCHLOG);
            auto key = partition_key::from_exploded(*schema, {uuid_type->decompose(_batch_uuid)});
            auto now = service::client_state(service::client_state::internal_tag()).get_timestamp();
            mutation m(key, schema);
            m.partition().apply_delete(*schema, {}, tombstone(now, gc_clock::now()));

            auto h = std::make_unique<write_response_handler>(_p._db.local().find_keyspace(db::system_keyspace::NAME), db::consistency_level::ONE, db::write_type::BATCH_LOG, make_lw_shared<frozen_mutation>(freeze(m)), _batchlog_endpoints);
            auto response_id = _p.register_response_handler(std::move(h));

            auto f = _p.response_wait(response_id);
            _p.send_to_live_endpoints(response_id, _local_dc);
            f.handle_exception([&p = _p, response_id](std::exception_ptr ex) {
                p.remove_response_handler(response_id); // cancel expire_timer, so no hint will happen
                std::rethrow_exception(ex);
            });
        };

        future<> run() {
            std::vector<response_id_type> ids;

            ids.reserve(_mutations.size());
            try {
                for (auto& m : _mutations) {
                    ids.emplace_back(_p.create_write_response_handler(m, _cl, db::write_type::BATCH));
                }
            } catch(...) {
                boost::for_each(ids, std::bind(&storage_proxy::remove_response_handler, &_p, std::placeholders::_1));
                throw;
            }

            return sync_write_to_batchlog().then([this, ids = std::move(ids)] {
                return parallel_for_each(ids.begin(), ids.end(), [this](auto response_id) {
                    // it is better to send first and hint afterwards to reduce latency
                    // but request may complete before hint_to_dead_endpoints() is called and
                    // response_id handler will be removed, so we will have to do hint with separate
                    // frozen_mutation copy, or manage handler live time differently.
                    _p.hint_to_dead_endpoints(response_id, _cl);
                    // call before send_to_live_endpoints() for the same reason as above
                    auto f = _p.response_wait(response_id);
                    _p.send_to_live_endpoints(response_id, _local_dc);
                    return f.handle_exception([this, response_id](std::exception_ptr p) {
                        _p.remove_response_handler(response_id); // cancel expire_timer, so no hint will happen
                        try {
                            std::rethrow_exception(p);
                        } catch (mutation_write_timeout_exception& ex) {
                            logger.trace("Write timeout; received {} of {} required replies", ex.received, ex.block_for);
                            throw;
                        }
                    });
                });
            }).finally(std::bind(&context::async_remove_from_batchlog, this));
        }
    };
    try {
        auto ctxt = make_lw_shared<context>(*this, std::move(mutations), cl);

        return ctxt->run().finally([p = shared_from_this(), lc, ctxt]() mutable {
            p->_stats.write.mark(lc.stop().latency_in_nano());
        });
    } catch (no_such_keyspace& ex) {
        return make_exception_future<>(std::current_exception());
    } catch (exceptions::unavailable_exception& ex) {
        _stats.write_unavailables++;
        logger.trace("Unavailable");
        return make_exception_future<>(std::current_exception());
    } catch (overloaded_exception& ex) {
        _stats.write_unavailables++;
        logger.trace("Overloaded");
        return make_exception_future<>(std::current_exception());
    }
}

bool storage_proxy::cannot_hint(gms::inet_address target) {
    return _total_hints_in_progress > _max_hints_in_progress
            && (get_hints_in_progress_for(target) > 0 && should_hint(target));
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
 // returned future is ready when sent is complete, not when mutation is executed on all (or any) targets!
future<> storage_proxy::send_to_live_endpoints(storage_proxy::response_id_type response_id, sstring local_dc)
{
    // extra-datacenter replicas, grouped by dc
    std::unordered_map<sstring, std::vector<gms::inet_address>> dc_groups;
    std::vector<std::pair<const sstring, std::vector<gms::inet_address>>> local;
    local.reserve(3);
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();

    for(auto dest: get_write_response_handler(response_id).get_targets()) {
        sstring dc = snitch_ptr->get_datacenter(dest);
        if (dc == local_dc) {
            local.emplace_back("", std::vector<gms::inet_address>({dest}));
        } else {
            dc_groups[dc].push_back(dest);
        }
    }

    auto mptr = get_write_response_handler(response_id).get_mutation();
    auto& m = *mptr;
    auto all = boost::range::join(local, dc_groups);

    // OK, now send and/or apply locally
    return parallel_for_each(all.begin(), all.end(), [response_id, &m, this] (typename decltype(dc_groups)::value_type& dc_targets) {
        auto my_address = utils::fb_utilities::get_broadcast_address();
        auto& forward = dc_targets.second;

        // last one in forward list is a coordinator
        auto coordinator = forward.back();
        forward.pop_back();

        if (coordinator == my_address) {
            return mutate_locally(m).then([response_id, this, my_address] {
                got_response(response_id, my_address);
            });
        } else {
            auto& ms = net::get_local_messaging_service();
            return ms.send_mutation(net::messaging_service::shard_id{coordinator, 0}, m,
                std::move(forward), my_address, engine().cpu_id(), response_id);
        }
    }).finally([mptr] {
        // make mutation alive until it is sent or processed locally, otherwise it
        // may disappear if write timeouts before this future is ready
    });
}

// returns number of hints stored
template<typename Range>
size_t storage_proxy::hint_to_dead_endpoints(lw_shared_ptr<const frozen_mutation> m, const Range& targets)
{
    return boost::count_if(targets | boost::adaptors::filtered(std::bind1st(std::mem_fn(&storage_proxy::should_hint), this)),
            std::bind(std::mem_fn(&storage_proxy::submit_hint), this, m, std::placeholders::_1));
}

size_t storage_proxy::get_hints_in_progress_for(gms::inet_address target) {
    auto it = _hints_in_progress.find(target);

    if (it == _hints_in_progress.end()) {
        return 0;
    }

    return it->second;
}

bool storage_proxy::submit_hint(lw_shared_ptr<const frozen_mutation> m, gms::inet_address target)
{
    warn(unimplemented::cause::HINT);
    // local write that time out should be handled by LocalMutationRunnable
    assert(is_me(target));
    return false;
#if 0
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
#endif
}

#if 0
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
#endif

class digest_mismatch_exception : public std::runtime_error {
public:
    digest_mismatch_exception() : std::runtime_error("Digest mismatch") {}
};

class abstract_read_resolver {
protected:
    db::consistency_level _cl;
    size_t _targets_count;
    promise<> _done_promise; // all target responded
    bool _timedout = false; // will be true if request timeouts
    timer<> _timeout;
    size_t _responses = 0;

    virtual void on_timeout() {}
    virtual size_t response_count() const = 0;
public:
    abstract_read_resolver(db::consistency_level cl, size_t target_count, std::chrono::high_resolution_clock::time_point timeout)
        : _cl(cl)
        , _targets_count(target_count)
    {
        _timeout.set_callback([this] {
            _timedout = true;
            _done_promise.set_exception(read_timeout_exception(_cl, response_count(), _targets_count, _responses != 0));
            on_timeout();
        });
        _timeout.arm(timeout);
    }
    virtual ~abstract_read_resolver() {};
    future<> done() {
        return _done_promise.get_future();
    }
    virtual void error(gms::inet_address ep, std::exception_ptr eptr) {
        sstring why;
        try {
            std::rethrow_exception(eptr);
        } catch (rpc::closed_error&) {
            return; // do not report connection closed exception, gossiper does that
        } catch(std::exception& e) {
            why = e.what();
        } catch(...) {
            why = "Unknown exception";
        }

        // do nothing other than log for now, request will timeout eventually
        logger.error("Exception when communicating with {}: {}", ep, why);
    }
};

class digest_read_resolver : public abstract_read_resolver {
    size_t _block_for;
    size_t _cl_responses = 0;
    promise<> _cl_promise; // cl is reached
    bool _cl_reported = false;
    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> _data_results;
    std::vector<query::result_digest> _digest_results;

    virtual void on_timeout() override {
        if (_cl_responses < _block_for) {
            _cl_promise.set_exception(read_timeout_exception(_cl, _cl_responses, _block_for, _data_results.size() != 0));
        }
        // we will not need them any more
        _data_results.clear();
        _digest_results.clear();
    }
    virtual size_t response_count() const override {
        return _digest_results.size();
    }
    bool digests_match() const {
        assert(_digest_results.size());
        auto& first = *_digest_results.begin();
        return std::find_if(_digest_results.begin() + 1, _digest_results.end(), [&first] (query::result_digest digest) { return digest != first; }) == _digest_results.end();
    }
public:
    digest_read_resolver(db::consistency_level cl, size_t block_for, std::chrono::high_resolution_clock::time_point timeout) : abstract_read_resolver(cl, 0, timeout), _block_for(block_for) {}
    void add_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<query::result>> result) {
        if (!_timedout) {
            _digest_results.emplace_back(result->digest());
            _data_results.emplace_back(std::move(result));
            got_response(from);
        }
    }
    void add_digest(gms::inet_address from, query::result_digest digest) {
        if (!_timedout) {
            _digest_results.emplace_back(std::move(digest));
            got_response(from);
        }
    }
    foreign_ptr<lw_shared_ptr<query::result>> resolve() {
        assert(_data_results.size());
        if (!digests_match()) {
            throw digest_mismatch_exception();
        }
        return  std::move(*_data_results.begin());
    }
    bool waiting_for(gms::inet_address ep) {
        return db::is_datacenter_local(_cl) ? is_me(ep) || db::is_local(ep) : true;
    }
    void got_response(gms::inet_address ep) {
        if (!_cl_reported) {
            if (waiting_for(ep)) {
                _cl_responses++;
            }
            if (_cl_responses >= _block_for && _data_results.size()) {
                _cl_reported = true;
                _cl_promise.set_value();
            }
        }
        if (_digest_results.size() == _targets_count) {
            _timeout.cancel();
            _done_promise.set_value();
        }
    }
    future<> has_cl() {
        return _cl_promise.get_future();
    }
    bool has_data() {
        return _data_results.size() != 0;
    }
    void add_wait_targets(size_t targets_count) {
        _targets_count += targets_count;
    }
    bool is_completed() {
        return _digest_results.size() == _targets_count;
    }
};

class data_read_resolver : public abstract_read_resolver {
    struct reply {
        gms::inet_address from;
        foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
        reply(gms::inet_address from_, foreign_ptr<lw_shared_ptr<reconcilable_result>> result_) : from(std::move(from_)), result(std::move(result_)) {}
    };
    struct version {
        gms::inet_address from;
        partition par;
        version(gms::inet_address from_, partition par_) : from(std::move(from_)), par(std::move(par_)) {}
    };

    uint32_t _max_live_count = 0;
    std::vector<reply> _data_results;
private:
    virtual void on_timeout() override {
        // we will not need them any more
        _data_results.clear();
    }
    virtual size_t response_count() const override {
        return _data_results.size();
    }

public:
    data_read_resolver(db::consistency_level cl, size_t targets_count, std::chrono::high_resolution_clock::time_point timeout) : abstract_read_resolver(cl, targets_count, timeout) {
        _data_results.reserve(targets_count);
    }
    void add_mutate_data(gms::inet_address from, foreign_ptr<lw_shared_ptr<reconcilable_result>> result) {
        if (!_timedout) {
            _max_live_count = std::max(result->row_count(), _max_live_count);
            _data_results.emplace_back(std::move(from), std::move(result));
            if (_data_results.size() == _targets_count) {
                _timeout.cancel();
                _done_promise.set_value();
            }
        }
    }
    uint32_t max_live_count() const {
        return _max_live_count;
    }
    reconcilable_result resolve(schema_ptr schema) {
        assert(_data_results.size());
        const auto& s = *schema;

        // return true if lh > rh
        auto cmp = [&s](reply& lh, reply& rh) {
            if (lh.result->partitions().size() == 0) {
                return false; // reply with empty partition array goes to the end of the sorted array
            } else if (rh.result->partitions().size() == 0) {
                return true;
            } else {
                auto lhk = lh.result->partitions().back().mut().key(s);
                auto rhk = rh.result->partitions().back().mut().key(s);
                return lhk.ring_order_tri_compare(s, rhk) > 0;
            }
        };

        // this array will have an entry for each partition which will hold all available versions
        std::vector<std::vector<version>> versions;
        versions.reserve(_data_results.front().result->partitions().size());

        do {
            // after this sort reply with largest key is at the beginning
            boost::sort(_data_results, cmp);
            if (_data_results.front().result->partitions().empty()) {
                break; // if top of the heap is empty all others are empty too
            }
            const auto& max_key = _data_results.front().result->partitions().back().mut().key(s);
            versions.emplace_back();
            std::vector<version>& v = versions.back();
            v.reserve(_targets_count);
            for (reply& r : _data_results) {
                auto pit = r.result->partitions().rbegin();
                if (pit != r.result->partitions().rend() && pit->mut().key(s).legacy_equal(s, max_key)) {
                    v.emplace_back(r.from, std::move(*pit));
                    r.result->partitions().pop_back();
                } else {
                    // put empty partition for destination without result
                    v.emplace_back(r.from, partition(0, freeze(mutation(max_key, schema))));
                }
            }
        } while(true);

        std::vector<partition> reconciliated_partitions;
        reconciliated_partitions.reserve(versions.size());
        uint32_t row_count = 0;

        // traverse backwards since large keys are at the start
        boost::range::transform(boost::make_iterator_range(versions.rbegin(), versions.rend()), std::back_inserter(reconciliated_partitions), [this, &row_count, schema] (std::vector<version>& v) {
            mutation m = std::accumulate(std::begin(v), std::end(v), mutation(v.front().par.mut().key(*schema), schema), [this, schema = std::move(schema)] (mutation& m, const version& ver) {
                m.partition().apply(*schema, ver.par.mut().partition());
                return std::move(m);
            });
            auto count = m.live_row_count();
            row_count += count;
            return partition(count, freeze(m));
        });

        return reconcilable_result(row_count, std::move(reconciliated_partitions));
    }
};

class abstract_read_executor : public enable_shared_from_this<abstract_read_executor> {
protected:
    using targets_iterator = std::vector<gms::inet_address>::iterator;
    using digest_resolver_ptr = ::shared_ptr<digest_read_resolver>;
    using data_resolver_ptr = ::shared_ptr<data_read_resolver>;

    shared_ptr<storage_proxy> _proxy;
    lw_shared_ptr<query::read_command> _cmd;
    lw_shared_ptr<query::read_command> _retry_cmd;
    query::partition_range _partition_range;
    db::consistency_level _cl;
    size_t _block_for;
    std::vector<gms::inet_address> _targets;
    promise<foreign_ptr<lw_shared_ptr<query::result>>> _result_promise;

public:
    abstract_read_executor(shared_ptr<storage_proxy> proxy, lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl, size_t block_for,
            std::vector<gms::inet_address> targets) :
                           _proxy(std::move(proxy)), _cmd(std::move(cmd)), _partition_range(std::move(pr)), _cl(cl), _block_for(block_for), _targets(std::move(targets)) {}
    virtual ~abstract_read_executor() {};

protected:
    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> make_mutation_data_request(lw_shared_ptr<query::read_command> cmd, gms::inet_address ep) {
        if (is_me(ep)) {
            return _proxy->query_mutations_locally(cmd, _partition_range);
        } else {
            auto& ms = net::get_local_messaging_service();
            return ms.send_read_mutation_data(net::messaging_service::shard_id{ep, 0}, *cmd, _partition_range).then([this](reconcilable_result&& result) {
                    return make_foreign(::make_lw_shared<reconcilable_result>(std::move(result)));
            });
        }
    }
    future<foreign_ptr<lw_shared_ptr<query::result>>> make_data_request(gms::inet_address ep) {
        if (is_me(ep)) {
            return _proxy->query_singular_local(_cmd, _partition_range);
        } else {
            auto& ms = net::get_local_messaging_service();
            return ms.send_read_data(net::messaging_service::shard_id{ep, 0}, *_cmd, _partition_range).then([this](query::result&& result) {
                return make_foreign(::make_lw_shared<query::result>(std::move(result)));
            });
        }
    }
    future<query::result_digest> make_digest_request(gms::inet_address ep) {
        if (is_me(ep)) {
            return _proxy->query_singular_local_digest(_cmd, _partition_range);
        } else {
            auto& ms = net::get_local_messaging_service();
            return ms.send_read_digest(net::messaging_service::shard_id{ep, 0}, *_cmd, _partition_range);
        }
    }
    future<> make_mutation_data_requests(lw_shared_ptr<query::read_command> cmd, data_resolver_ptr resolver, targets_iterator begin, targets_iterator end) {
        return parallel_for_each(begin, end, [this, &cmd, resolver = std::move(resolver)] (gms::inet_address ep) {
            return make_mutation_data_request(cmd, ep).then_wrapped([resolver, ep] (future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> f) {
                try {
                    resolver->add_mutate_data(ep, f.get0());
                } catch(...) {
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    future<> make_data_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end) {
        return parallel_for_each(begin, end, [this, resolver = std::move(resolver)] (gms::inet_address ep) {
            return make_data_request(ep).then_wrapped([resolver, ep] (future<foreign_ptr<lw_shared_ptr<query::result>>> f) {
                try {
                    resolver->add_data(ep, f.get0());
                } catch(...) {
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    future<> make_digest_requests(digest_resolver_ptr resolver, targets_iterator begin, targets_iterator end) {
        return parallel_for_each(begin, end, [this, resolver = std::move(resolver)] (gms::inet_address ep) {
            return make_digest_request(ep).then_wrapped([resolver, ep] (future<query::result_digest> f) {
                try {
                    resolver->add_digest(ep, f.get0());
                } catch(...) {
                    resolver->error(ep, std::current_exception());
                }
            });
        });
    }
    virtual future<> make_requests(digest_resolver_ptr resolver) {
        resolver->add_wait_targets(_targets.size());
        return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 1),
                        make_digest_requests(resolver, _targets.begin() + 1, _targets.end())).discard_result();
    }
    virtual void got_cl() {}
    uint32_t original_row_limit() const {
        return _cmd->row_limit;
    }
    void reconciliate(db::consistency_level cl, std::chrono::high_resolution_clock::time_point timeout, lw_shared_ptr<query::read_command> cmd) {
        data_resolver_ptr data_resolver = ::make_shared<data_read_resolver>(cl, _targets.size(), timeout);
        auto exec = shared_from_this();

        make_mutation_data_requests(cmd, data_resolver, _targets.begin(), _targets.end()).finally([exec]{});

        data_resolver->done().then_wrapped([this, exec, data_resolver, cmd = std::move(cmd), cl, timeout] (future<> f) {
            try {
                f.get();
                schema_ptr s = _proxy->_db.local().find_schema(_cmd->cf_id);
                auto rr = data_resolver->resolve(s); // reconciliation happens here

                // We generate a retry if at least one node reply with count live columns but after merge we have less
                // than the total number of column we are interested in (which may be < count on a retry).
                // So in particular, if no host returned count live columns, we know it's not a short read.
                if (data_resolver->max_live_count() < cmd->row_limit || rr.row_count() >= original_row_limit()) {
                    auto result = ::make_foreign(::make_lw_shared(to_data_query_result(std::move(rr), std::move(s), _cmd->slice)));
                    _result_promise.set_value(std::move(result));
                } else {
                    _retry_cmd = make_lw_shared<query::read_command>(*cmd);
                    // We asked t (= _cmd->row_limit) live columns and got l (=rr.row_count) ones.
                    // From that, we can estimate that on this row, for x requested
                    // columns, only l/t end up live after reconciliation. So for next
                    // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
                    _retry_cmd->row_limit = rr.row_count() == 0 ? cmd->row_limit + 1 : ((cmd->row_limit * cmd->row_limit) / rr.row_count()) + 1;
                    reconciliate(cl, timeout, _retry_cmd);
                }
            } catch(read_timeout_exception& ex) {
                _result_promise.set_exception(ex);
            }
        });
    }
    void reconciliate(db::consistency_level cl, std::chrono::high_resolution_clock::time_point timeout) {
        reconciliate(cl, timeout, _cmd);
    }

public:
    virtual future<foreign_ptr<lw_shared_ptr<query::result>>> execute(std::chrono::high_resolution_clock::time_point timeout) {
        digest_resolver_ptr digest_resolver = ::make_shared<digest_read_resolver>(_cl, _block_for, timeout);
        auto exec = shared_from_this();

        make_requests(digest_resolver).finally([exec]() {
            // hold on to executor until all queries are complete
        });

        digest_resolver->has_cl().then_wrapped([this, exec, digest_resolver, timeout] (future<> f) {
            try {
                got_cl();
                f.get();
                exec->_result_promise.set_value(digest_resolver->resolve()); // can throw digest missmatch exception
                auto done = digest_resolver->done();
                if (exec->_block_for < exec->_targets.size()) { // if there are more targets then needed for cl, check digest in background
                    done.then_wrapped([exec, digest_resolver] (future<>&& f){
                        try {
                            f.get();
                            digest_resolver->resolve();
                        } catch(digest_mismatch_exception& ex) {
                            // FIXME: do read repair here
                        } catch(...) {
                            // ignore all exception besides digest mismatch during background check
                        }
                    });
                } else {
                    done.discard_result(); // no need for background check, discard done future explicitly
                }
            } catch (digest_mismatch_exception& ex) {
                exec->reconciliate(_cl, timeout);
            } catch (read_timeout_exception& ex) {
                exec->_result_promise.set_exception(ex);
            }
        });

        return _result_promise.get_future();
    }
};

class never_speculating_read_executor : public abstract_read_executor {
public:
    using abstract_read_executor::abstract_read_executor;
};

// this executor always asks for one additional data reply
class always_speculating_read_executor : public abstract_read_executor {
public:
    using abstract_read_executor::abstract_read_executor;
    virtual future<> make_requests(digest_resolver_ptr resolver) {
        resolver->add_wait_targets(_targets.size());
        return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 2),
                        make_digest_requests(resolver, _targets.begin() + 2, _targets.end())).discard_result();
    }
};

// this executor sends request to an additional replica after some time below timeout
class speculating_read_executor : public abstract_read_executor {
    timer<> _speculate_timer;
public:
    using abstract_read_executor::abstract_read_executor;
    virtual future<> make_requests(digest_resolver_ptr resolver) {
        _speculate_timer.set_callback([this, resolver] {
            if (!resolver->is_completed()) { // at the time the callback runs request may be completed already
                resolver->add_wait_targets(1); // we send one more request so wait for it too
                future<> f = resolver->has_data() ?
                        make_digest_requests(resolver, _targets.end() - 1, _targets.end()) :
                        make_data_requests(resolver, _targets.end() - 1, _targets.end());
                f.finally([exec = shared_from_this()]{});
            }
        });
        // FIXME: the timeout should come from previous latency statistics for a partition
        auto timeout = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(_proxy->get_db().local().get_config().read_request_timeout_in_ms()/2);
        _speculate_timer.arm(timeout);

        // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
        // that the last replica in our list is "extra."
        resolver->add_wait_targets(_targets.size() - 1);
        if (_block_for < _targets.size() - 1) {
            // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
            // preferred by the snitch, we do an extra data read to start with against a replica more
            // likely to reply; better to let RR fail than the entire query.
            return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 2),
                            make_digest_requests(resolver, _targets.begin() + 2, _targets.end())).discard_result();
        } else {
            // not doing read repair; all replies are important, so it doesn't matter which nodes we
            // perform data reads against vs digest.
            return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 1),
                            make_digest_requests(resolver, _targets.begin() + 1, _targets.end() - 1)).discard_result();
        }
    }
    virtual void got_cl() override {
        _speculate_timer.cancel();
    }
};

class range_slice_read_executor : public abstract_read_executor {
public:
    range_slice_read_executor(shared_ptr<storage_proxy> proxy, lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl, std::vector<gms::inet_address> targets) :
                                    abstract_read_executor(std::move(proxy), std::move(cmd), std::move(pr), cl, targets.size(), std::move(targets)) {}
    virtual future<foreign_ptr<lw_shared_ptr<query::result>>> execute(std::chrono::high_resolution_clock::time_point timeout) override {
        reconciliate(_cl, timeout);
        return _result_promise.get_future();
    }
};

db::read_repair_decision storage_proxy::new_read_repair_decision(const schema& s) {
    double chance = _read_repair_chance(_urandom);
    if (s.read_repair_chance() > chance) {
        return db::read_repair_decision::GLOBAL;
    }

    if (s.dc_local_read_repair_chance() > chance) {
        return db::read_repair_decision::DC_LOCAL;
    }

    return db::read_repair_decision::NONE;
}

::shared_ptr<abstract_read_executor> storage_proxy::get_read_executor(lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl) {
    const dht::token& token = pr.start()->value().token();
    schema_ptr schema = _db.local().find_schema(cmd->cf_id);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());

    std::vector<gms::inet_address> all_replicas = get_live_sorted_endpoints(ks, token);
    db::read_repair_decision repair_decision = new_read_repair_decision(*schema);
    std::vector<gms::inet_address> target_replicas = db::filter_for_query(cl, ks, all_replicas, repair_decision);

    // Throw UAE early if we don't have enough replicas.
    db::assure_sufficient_live_nodes(cl, ks, target_replicas);

#if 0
    if (repair_decision != read_repair_decision::NONE)
        ReadRepairMetrics.attempted.mark();
#endif

#if 0
    ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.cfName);
#endif
    speculative_retry::type retry_type = schema->speculative_retry().get_type();

    size_t block_for = db::block_for(ks, cl);
    auto p = shared_from_this();
    // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
    if (retry_type == speculative_retry::type::NONE || db::block_for(ks, cl) == all_replicas.size()) {
        return ::make_shared<never_speculating_read_executor>(p, cmd, std::move(pr), cl, block_for, std::move(target_replicas));
    }

    if (target_replicas.size() == all_replicas.size()) {
        // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
        // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
        // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
        return ::make_shared<always_speculating_read_executor>(/*cfs, */p, cmd, std::move(pr), cl, block_for, std::move(target_replicas));
    }

    // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
    gms::inet_address extra_replica = all_replicas[target_replicas.size()];
    // With repair decision DC_LOCAL all replicas/target replicas may be in different order, so
    // we might have to find a replacement that's not already in targetReplicas.
    if (repair_decision == db::read_repair_decision::DC_LOCAL && boost::range::find(target_replicas, extra_replica) != target_replicas.end()) {
        auto it = boost::range::find_if(all_replicas, [&target_replicas] (gms::inet_address& a){
            return boost::range::find(target_replicas, a) == target_replicas.end();
        });
        extra_replica = *it;
    }
    target_replicas.push_back(extra_replica);

    if (retry_type == speculative_retry::type::ALWAYS) {
        return ::make_shared<always_speculating_read_executor>(/*cfs,*/p, cmd, std::move(pr), cl, block_for, std::move(target_replicas));
    } else {// PERCENTILE or CUSTOM.
        return ::make_shared<speculating_read_executor>(/*cfs,*/p, cmd, std::move(pr), cl, block_for, std::move(target_replicas));
    }
}

future<query::result_digest>
storage_proxy::query_singular_local_digest(lw_shared_ptr<query::read_command> cmd, const query::partition_range& pr) {
    return query_singular_local(cmd, pr).then([] (foreign_ptr<lw_shared_ptr<query::result>> result) {
        return result->digest();
    });
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::query_singular_local(lw_shared_ptr<query::read_command> cmd, const query::partition_range& pr) {
    unsigned shard = _db.local().shard_of(pr.start()->value().token());
    return _db.invoke_on(shard, [prv = std::vector<query::partition_range>({pr}) /* FIXME: pr is copied */, cmd] (database& db) {
        return db.query(*cmd, prv).then([](auto&& f) {
            return make_foreign(std::move(f));
        });
    });
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::query_singular(lw_shared_ptr<query::read_command> cmd, std::vector<query::partition_range>&& partition_ranges, db::consistency_level cl) {
    std::vector<::shared_ptr<abstract_read_executor>> exec;
    exec.reserve(partition_ranges.size());
    auto timeout = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(_db.local().get_config().read_request_timeout_in_ms());

    for (auto&& pr: partition_ranges) {
        if (!pr.is_singular()) {
            throw std::runtime_error("mixed singular and non singular range are not supported");
        }
        exec.push_back(get_read_executor(cmd, std::move(pr), cl));
    }

    query::result_merger merger;
    merger.reserve(exec.size());

    auto f = ::map_reduce(exec.begin(), exec.end(), [timeout] (::shared_ptr<abstract_read_executor>& rex) {
        return rex->execute(timeout);
    }, std::move(merger));

    return f.finally([exec = std::move(exec)] {
        // hold onto exec until read is complete
    });
}

future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>>
storage_proxy::query_partition_key_range_concurrent(std::chrono::high_resolution_clock::time_point timeout, std::vector<foreign_ptr<lw_shared_ptr<query::result>>>&& results,
        lw_shared_ptr<query::read_command> cmd, db::consistency_level cl, std::vector<query::partition_range>::iterator&& i,
        std::vector<query::partition_range>&& ranges, int concurrency_factor) {
    schema_ptr schema = _db.local().find_schema(cmd->cf_id);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    std::vector<::shared_ptr<abstract_read_executor>> exec;
    auto concurrent_fetch_starting_index = i;
    auto p = shared_from_this();

    while (i != ranges.end() && std::distance(i, concurrent_fetch_starting_index) < concurrency_factor) {
        query::partition_range& range = *i;
        std::vector<gms::inet_address> live_endpoints = get_live_sorted_endpoints(ks, end_token(range));
        std::vector<gms::inet_address> filtered_endpoints = filter_for_query(cl, ks, live_endpoints);
        ++i;

        // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
        // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
        // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
        while (i != ranges.end())
        {
            query::partition_range& next_range = *i;
            std::vector<gms::inet_address> next_endpoints = get_live_sorted_endpoints(ks, end_token(next_range));
            std::vector<gms::inet_address> next_filtered_endpoints = filter_for_query(cl, ks, next_endpoints);

            // Origin has this to say here:
            // *  If the current range right is the min token, we should stop merging because CFS.getRangeSlice
            // *  don't know how to deal with a wrapping range.
            // *  Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
            // *  the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
            // *  wire compatibility, so It's likely easier not to bother;
            // It obviously not apply for us(?), but lets follow origin for now
            if (end_token(range) == dht::maximum_token()) {
                break;
            }

            std::vector<gms::inet_address> merged = intersection(live_endpoints, next_endpoints);

            // Check if there is enough endpoint for the merge to be possible.
            if (!is_sufficient_live_nodes(cl, ks, merged)) {
                break;
            }

            std::vector<gms::inet_address> filtered_merged = filter_for_query(cl, ks, merged);

            // Estimate whether merging will be a win or not
            if (!locator::i_endpoint_snitch::get_local_snitch_ptr()->is_worth_merging_for_range_query(filtered_merged, filtered_endpoints, next_filtered_endpoints)) {
                break;
            }

            // If we get there, merge this range and the next one
            range = query::partition_range(range.start(), next_range.end());
            live_endpoints = std::move(merged);
            filtered_endpoints = std::move(filtered_merged);
            ++i;
        }
        db::assure_sufficient_live_nodes(cl, ks, filtered_endpoints);
        exec.push_back(::make_shared<range_slice_read_executor>(p, cmd, std::move(range), cl, std::move(filtered_endpoints)));
    }

    query::result_merger merger;
    merger.reserve(exec.size());

    auto f = ::map_reduce(exec.begin(), exec.end(), [timeout] (::shared_ptr<abstract_read_executor>& rex) {
        return rex->execute(timeout);
    }, std::move(merger));

    return f.then([p, exec = std::move(exec), results = std::move(results), i = std::move(i), ranges = std::move(ranges), cl, cmd, concurrency_factor, timeout]
                   (foreign_ptr<lw_shared_ptr<query::result>>&& result) mutable {
        results.emplace_back(std::move(result));
        if (i == ranges.end()) {
            return make_ready_future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>>(std::move(results));
        } else {
            return p->query_partition_key_range_concurrent(timeout, std::move(results), cmd, cl, std::move(i), std::move(ranges), concurrency_factor);
        }
    });
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::query_partition_key_range(lw_shared_ptr<query::read_command> cmd, query::partition_range&& range, db::consistency_level cl) {
    schema_ptr schema = _db.local().find_schema(cmd->cf_id);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());
    std::vector<query::partition_range> ranges;
    auto timeout = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(_db.local().get_config().read_request_timeout_in_ms());

    // when dealing with LocalStrategy keyspaces, we can skip the range splitting and merging (which can be
    // expensive in clusters with vnodes)
    if (ks.get_replication_strategy().get_type() == locator::replication_strategy_type::local) {
        ranges.emplace_back(std::move(range));
    } else {
        ranges = get_restricted_ranges(ks, *schema, std::move(range));
    }

    // our estimate of how many result rows there will be per-range
    float result_rows_per_range = estimate_result_rows_per_range(cmd, ks);
    // underestimate how many rows we will get per-range in order to increase the likelihood that we'll
    // fetch enough rows in the first round
    result_rows_per_range -= result_rows_per_range * CONCURRENT_SUBREQUESTS_MARGIN;
    int concurrency_factor = result_rows_per_range == 0.0 ? 1 : std::max(1, std::min(int(ranges.size()), int(std::ceil(cmd->row_limit / result_rows_per_range))));

    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results;
    results.reserve(ranges.size()/concurrency_factor + 1);

    return query_partition_key_range_concurrent(timeout, std::move(results), cmd, cl, ranges.begin(), std::move(ranges), concurrency_factor)
            .then([](std::vector<foreign_ptr<lw_shared_ptr<query::result>>> results) {
        query::result_merger merger;
        merger.reserve(results.size());

        for (auto&& r: results) {
            merger(std::move(r));
        }

        return merger.get();
    });
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    std::vector<query::partition_range>&& partition_ranges,
    db::consistency_level cl)
{
    if (logger.is_enabled(logging::log_level::trace)) {
        static thread_local int next_id = 0;
        auto query_id = next_id++;

        logger.trace("query {}.{} cmd={}, ranges={}, id={}", s->ks_name(), s->cf_name(), *cmd, ::join(", ", partition_ranges), query_id);
        return do_query(s, cmd, std::move(partition_ranges), cl).then([query_id, cmd, s] (foreign_ptr<lw_shared_ptr<query::result>>&& res) {
            logger.trace("query_result id={}, {}", query_id, res->pretty_print(s, cmd->slice));
            return std::move(res);
        });
    }

    return do_query(s, cmd, std::move(partition_ranges), cl);
}

future<foreign_ptr<lw_shared_ptr<query::result>>>
storage_proxy::do_query(schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    std::vector<query::partition_range>&& partition_ranges,
    db::consistency_level cl)
{
    static auto make_empty = [] {
        return make_ready_future<foreign_ptr<lw_shared_ptr<query::result>>>(make_foreign(make_lw_shared<query::result>()));
    };

    if (partition_ranges.empty()) {
        return make_empty();
    }
    utils::latency_counter lc;
    lc.start();
    auto p = shared_from_this();

    if (partition_ranges[0].is_singular() && partition_ranges[0].start()->value().has_key()) { // do not support mixed partitions (yet?)
        try {
            return query_singular(cmd, std::move(partition_ranges), cl).finally([lc, p] () mutable {
                    p->_stats.read.mark(lc.stop().latency_in_nano());
            });
        } catch (const no_such_column_family&) {
            _stats.read.mark(lc.stop().latency_in_nano());
            return make_empty();
        }
    }

    if (partition_ranges.size() != 1) {
        // FIXME: implement
        throw std::runtime_error("more than one non singular range not supported yet");
        _stats.read.mark(lc.stop().latency_in_nano());
    }

    return query_partition_key_range(cmd, std::move(partition_ranges[0]), cl).finally([lc, p] () mutable {
        p->_stats.read.mark(lc.stop().latency_in_nano());
    });
}

#if 0
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
#endif

std::vector<gms::inet_address> storage_proxy::get_live_sorted_endpoints(keyspace& ks, const dht::token& token) {
    auto& rs = ks.get_replication_strategy();
    std::vector<gms::inet_address> eps = rs.get_natural_endpoints(token);
    auto itend = boost::range::remove_if(eps, std::not1(std::bind1st(std::mem_fn(&gms::failure_detector::is_alive), &gms::get_local_failure_detector())));
    eps.erase(itend, eps.end());
//    DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), liveEndpoints);
    return eps;
}

std::vector<gms::inet_address> storage_proxy::intersection(const std::vector<gms::inet_address>& l1, const std::vector<gms::inet_address>& l2) {
    std::vector<gms::inet_address> inter;
    inter.reserve(l1.size());
    std::remove_copy_if(l1.begin(), l1.end(), std::back_inserter(inter), [&l2] (const gms::inet_address& a) {
        return std::find(l2.begin(), l2.end(), a) == l2.end();
    });
    return inter;
}

/**
 * Estimate the number of result rows (either cql3 rows or storage rows, as called for by the command) per
 * range in the ring based on our local data.  This assumes that ranges are uniformly distributed across the cluster
 * and that the queried data is also uniformly distributed.
 */
float storage_proxy::estimate_result_rows_per_range(lw_shared_ptr<query::read_command> cmd, keyspace& ks)
{
    return 1.0;
#if 0
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
#endif
}

#if 0
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

    private static List<Row> trim(AbstractRangeCommand command, List<Row> rows)
    {
        // When maxIsColumns, we let the caller trim the result.
        if (command.countCQL3Rows())
            return rows;
        else
            return rows.size() > command.limit() ? rows.subList(0, command.limit()) : rows;
    }
#endif

/**
 * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
 * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
 */
std::vector<query::partition_range>
storage_proxy::get_restricted_ranges(keyspace& ks, const schema& s, query::partition_range range) {
    // special case for bounds containing exactly 1 token
    if (start_token(range) == end_token(range) && !range.is_wrap_around(dht::ring_position_comparator(s))) {
        if (start_token(range).is_minimum()) {
            return {};
        }
        return std::vector<query::partition_range>({std::move(range)});
    }

    locator::token_metadata& tm = get_local_storage_service().get_token_metadata();

    std::vector<query::partition_range> ranges;

    // divide the queryRange into pieces delimited by the ring and minimum tokens
    auto ring_iter = tm.ring_range(range.start(), true);
    query::partition_range remainder = std::move(range);
    for (const dht::token& upper_bound_token : ring_iter)
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

        dht::ring_position split_point(upper_bound_token, dht::ring_position::token_bound::end);
        if (!remainder.contains(split_point, dht::ring_position_comparator(s))) {
            break; // no more splits
        }

        if (upper_bound_token.is_minimum()) {
            ranges.emplace_back(query::partition_range(remainder.start(), {}));
            remainder = query::partition_range({}, remainder.end());
        } else {
            // We shouldn't attempt to split on upper bound, because it may result in
            // an ambiguous range of the form (x; x]
            if (end_token(remainder) == upper_bound_token) {
                break;
            }

            std::pair<query::partition_range, query::partition_range> splits =
                remainder.split(split_point, dht::ring_position_comparator(s));

            ranges.emplace_back(std::move(splits.first));
            remainder = std::move(splits.second);
        }
    }
    ranges.emplace_back(std::move(remainder));

    return ranges;
}

bool storage_proxy::should_hint(gms::inet_address ep) {
    if (is_me(ep)) { // do not hint to local address
        return false;
    }

    return false;
#if 0
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
#endif
}

#if 0
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
#endif

void storage_proxy::init_messaging_service() {
    auto& ms = net::get_local_messaging_service();
    ms.register_definitions_update( [] (std::vector<frozen_mutation> m) {
        do_with(std::move(m), get_local_shared_storage_proxy(), [] (const std::vector<frozen_mutation>& mutations, shared_ptr<storage_proxy>& p) {
            std::vector<mutation> schema;
            for (auto& m : mutations) {
                schema_ptr s = p->get_db().local().find_schema(m.column_family_id());
                schema.emplace_back(m.unfreeze(s));
            }
            return db::schema_tables::merge_schema(get_storage_proxy(), std::move(schema));
        }).discard_result();
        return net::messaging_service::no_wait();
    });
    ms.register_migration_request([] (gms::inet_address reply_to, unsigned shard) {
        return db::schema_tables::convert_schema_to_mutations(get_storage_proxy()).finally([p = get_local_shared_storage_proxy()] {
            // keep local proxy alive
        });
    });
    ms.register_mutation([] (frozen_mutation in, std::vector<gms::inet_address> forward, gms::inet_address reply_to, unsigned shard, storage_proxy::response_id_type response_id) {
        do_with(std::move(in), get_local_shared_storage_proxy(), [forward = std::move(forward), reply_to, shard, response_id] (const frozen_mutation& m, shared_ptr<storage_proxy>& p) {
            return make_ready_future<>().then([&p, &m, reply_to, shard, response_id, forward = std::move(forward)] () mutable {
                return when_all(
                    p->mutate_locally(m).then([reply_to, shard, response_id] () mutable {
                        auto& ms = net::get_local_messaging_service();
                        ms.send_mutation_done(net::messaging_service::shard_id{reply_to, shard}, shard, response_id);
                        // return void, no need to wait for send to complete
                    }),
                    parallel_for_each(forward.begin(), forward.end(), [reply_to, shard, response_id, &m] (gms::inet_address forward) {
                        auto& ms = net::get_local_messaging_service();
                        return ms.send_mutation(net::messaging_service::shard_id{forward, 0}, m, {}, reply_to, shard, response_id);
                    })
                );
            }).then_wrapped([] (auto&& f) {
                    try {
                        f.get();
                    } catch (std::exception& e){
                        logger.warn("MUTATION verb handler: {}", e.what());
                    } catch(...) {
                        logger.warn("MUTATION verb handler: unknown exception is thrown");
                    }

                    // don't propagate the exception further
                    return make_ready_future<>();
            });
        }).discard_result();

        return net::messaging_service::no_wait();
    });
    ms.register_mutation_done([] (rpc::client_info cinfo, unsigned shard, storage_proxy::response_id_type response_id) {
        gms::inet_address from(net::ntoh(cinfo.addr.as_posix_sockaddr_in().sin_addr.s_addr));
        get_storage_proxy().invoke_on(shard, [from, response_id] (storage_proxy& sp) {
            sp.got_response(response_id, from);
        });
        return net::messaging_service::no_wait();
    });
    ms.register_read_data([] (query::read_command cmd, query::partition_range pr) {
        return do_with(std::move(pr), get_local_shared_storage_proxy(), [cmd = make_lw_shared<query::read_command>(std::move(cmd))] (const query::partition_range& pr, shared_ptr<storage_proxy>& p) {
            return p->query_singular_local(cmd, pr);
        });
    });
    ms.register_read_mutation_data([] (query::read_command cmd, query::partition_range pr) {
        return do_with(std::move(pr), get_local_shared_storage_proxy(), [cmd = make_lw_shared<query::read_command>(std::move(cmd))] (const query::partition_range& pr, shared_ptr<storage_proxy>& p) {
            return p->query_mutations_locally(cmd, pr);
        });
    });
    ms.register_read_digest([] (query::read_command cmd, query::partition_range pr) {
        return do_with(std::move(pr), get_local_shared_storage_proxy(), [cmd = make_lw_shared<query::read_command>(std::move(cmd))] (const query::partition_range& pr, shared_ptr<storage_proxy>& p) {
            return p->query_singular_local_digest(cmd, pr);
        });
    });
}

void storage_proxy::uninit_messaging_service() {
    auto& ms = net::get_local_messaging_service();
    ms.unregister_definitions_update();
    ms.unregister_migration_request();
    ms.unregister_mutation();
    ms.unregister_mutation_done();
    ms.unregister_read_data();
    ms.unregister_read_mutation_data();
    ms.unregister_read_digest();
}

// Merges reconcilable_result:s from different shards into one
// Drops partitions which exceed the limit.
class mutation_result_merger {
    // Adapts reconcilable_result to a consumable sequence of partitions.
    struct partition_run {
        foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
        size_t index = 0;

        partition_run(foreign_ptr<lw_shared_ptr<reconcilable_result>> result)
            : result(std::move(result))
        { }

        const partition& current() const {
            return result->partitions()[index];
        }

        bool has_more() const {
            return index < result->partitions().size();
        }

        void advance() {
            ++index;
        }
    };

    lw_shared_ptr<query::read_command> _cmd;
    schema_ptr _schema;
    std::vector<partition_run> _runs;
public:
    mutation_result_merger(lw_shared_ptr<query::read_command> cmd, schema_ptr schema)
        : _cmd(std::move(cmd))
        , _schema(std::move(schema))
    { }

    void reserve(size_t shard_count) {
        _runs.reserve(shard_count);
    }

    void operator()(foreign_ptr<lw_shared_ptr<reconcilable_result>> result) {
        if (result->partitions().size() > 0) {
            _runs.emplace_back(partition_run(std::move(result)));
        }
    }

    reconcilable_result get() && {
        std::vector<partition> partitions;
        uint32_t row_count = 0;

        auto cmp = [this] (const partition_run& r1, const partition_run& r2) {
            const partition& p1 = r1.current();
            const partition& p2 = r2.current();
            return p1._m.key(*_schema).ring_order_tri_compare(*_schema, p2._m.key(*_schema)) > 0;
        };

        boost::range::make_heap(_runs, cmp);

        while (!_runs.empty()) {
            boost::range::pop_heap(_runs, cmp);
            partition_run& next = _runs.back();
            const partition& p = next.current();
            unsigned limit_left = _cmd->row_limit - row_count;
            if (p._row_count > limit_left) {
                // no space for all rows in the mutation
                // unfreeze -> trim -> freeze
                mutation m = p.mut().unfreeze(_schema);
                static const std::vector<query::clustering_range> all(1, query::clustering_range::make_open_ended_both_sides());
                auto rc = m.partition().compact_for_query(*_schema, _cmd->timestamp, all, limit_left);
                partitions.push_back(partition(rc, freeze(m)));
                row_count += rc;
            } else {
                partitions.push_back(p);
                row_count += p._row_count;
            }
            if (row_count >= _cmd->row_limit) {
                break;
            }
            next.advance();
            if (next.has_more()) {
                boost::range::push_heap(_runs, cmp);
            } else {
                _runs.pop_back();
            }
        }

        return { row_count, std::move(partitions) };
    }
};

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
storage_proxy::query_mutations_locally(lw_shared_ptr<query::read_command> cmd, const query::partition_range& pr) {
    if (pr.is_singular()) {
        unsigned shard = _db.local().shard_of(pr.start()->value().token());
        return _db.invoke_on(shard, [cmd, &pr] (database& db) {
            return db.query_mutations(*cmd, pr).then([] (reconcilable_result&& result) {
                return make_foreign(make_lw_shared(std::move(result)));
            });
        });
    } else {
        auto schema = _db.local().find_schema(cmd->cf_id);
        return _db.map_reduce(mutation_result_merger{cmd, schema}, [cmd, &pr] (database& db) {
            return db.query_mutations(*cmd, pr).then([] (reconcilable_result&& result) {
                return make_foreign(make_lw_shared(std::move(result)));
            });
        }).then([] (reconcilable_result&& result) {
            return make_foreign(make_lw_shared(std::move(result)));
        });
    }
}

future<>
storage_proxy::stop() {
    uninit_messaging_service();
    return make_ready_future<>();
}

class shard_reader final : public mutation_reader::impl {
    distributed<database>& _db;
    unsigned _shard;
    utils::UUID _cf_id;
    const query::partition_range _range;
    schema_ptr _local_schema;
    struct remote_state {
        mutation_reader reader;
        std::experimental::optional<frozen_mutation> _m;
    };
    foreign_ptr<std::unique_ptr<remote_state>> _remote;
private:
    future<> init() {
        _local_schema = _db.local().find_column_family(_cf_id).schema();
        return _db.invoke_on(_shard, [this] (database& db) {
            column_family& cf = db.find_column_family(_cf_id);
            return make_foreign(std::make_unique<remote_state>(remote_state{cf.make_reader(_range)}));
        }).then([this] (auto&& ptr) {
            _remote = std::move(ptr);
        });
    }
public:
    shard_reader(utils::UUID cf_id, distributed<database>& db, unsigned shard, const query::partition_range& range)
        : _db(db)
        , _shard(shard)
        , _cf_id(cf_id)
        , _range(range)
    { }

    virtual future<mutation_opt> operator()() override {
        if (!_remote) {
            return init().then([this] {
                return (*this)();
            });
        }

        // FIXME: batching
        return _db.invoke_on(_shard, [this] (database&) {
            return _remote->reader().then([this] (mutation_opt&& m) {
                if (!m) {
                    _remote->_m = {};
                } else {
                    _remote->_m = freeze(*m);
                }
            });
        }).then([this] () -> mutation_opt {
            if (!_remote->_m) {
                return {};
            }
            return { _remote->_m->unfreeze(_local_schema) };
        });
    }
};

mutation_reader
storage_proxy::make_local_reader(utils::UUID cf_id, const query::partition_range& range) {
    // Split ranges which wrap around, because the individual readers created
    // by shard_reader do not support them:
    auto schema = _db.local().find_column_family(cf_id).schema();
    if (range.is_wrap_around(dht::ring_position_comparator(*schema))) {
        auto unwrapped = range.unwrap();
        std::vector<mutation_reader> both;
        both.reserve(2);
        both.push_back(make_local_reader(cf_id, unwrapped.second));
        both.push_back(make_local_reader(cf_id, unwrapped.first));
        return make_joining_reader(std::move(both));
    }

    unsigned first_shard = range.start() ? dht::shard_of(range.start()->value().token()) : 0;
    unsigned last_shard = range.end() ? dht::shard_of(range.end()->value().token()) : smp::count - 1;
    std::vector<mutation_reader> readers;
    for (auto cpu = first_shard; cpu <= last_shard; ++cpu) {
        readers.emplace_back(make_mutation_reader<shard_reader>(cf_id, _db, cpu, range));
    }
    return make_joining_reader(std::move(readers));
}

}
