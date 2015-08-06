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

struct mutation_write_timeout_error : public cassandra_exception {
    size_t to_block_for;
    size_t received;
    mutation_write_timeout_error(size_t tbf, size_t acks) :
        cassandra_exception(exception_code::WRITE_TIMEOUT, sprint("Mutation write timeout: waited for %lu got %lu acks", tbf, acks)) , to_block_for(tbf), received(acks) {}
};

struct overloaded_exception : public cassandra_exception {
    overloaded_exception(size_t c) :
        cassandra_exception(exception_code::OVERLOADED, sprint("Too many in flight hints: %lu", c)) {}
};

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
    lw_shared_ptr<const frozen_mutation> _mutation;
    std::unordered_set<gms::inet_address> _targets; // who we sent this mutation to
    size_t _pending_endpoints; // how many endpoints in bootstrap state there is
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
    abstract_write_response_handler(keyspace& ks, db::consistency_level cl, lw_shared_ptr<const frozen_mutation> mutation, std::unordered_set<gms::inet_address> targets, size_t pending_endpoints) :
        _ready(0), _cl(cl), _ks(ks), _mutation(std::move(mutation)), _targets(targets), _pending_endpoints(pending_endpoints) {}
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
    const std::unordered_set<gms::inet_address>& get_targets() {
        return _targets;
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
    datacenter_write_response_handler(keyspace& ks, db::consistency_level cl, lw_shared_ptr<const frozen_mutation> mutation, std::unordered_set<gms::inet_address> targets, size_t pending_endpoints) :
        abstract_write_response_handler(ks, cl, std::move(mutation), targets, pending_endpoints) {}
};

class write_response_handler : public abstract_write_response_handler {
public:
    write_response_handler(keyspace& ks, db::consistency_level cl, lw_shared_ptr<const frozen_mutation> mutation, std::unordered_set<gms::inet_address> targets, size_t pending_endpoints) :
        abstract_write_response_handler(ks, cl, std::move(mutation), targets, pending_endpoints) {}
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
    datacenter_sync_write_response_handler(keyspace& ks, db::consistency_level cl, lw_shared_ptr<const frozen_mutation> mutation, std::unordered_set<gms::inet_address> targets, size_t pending_endpoints) :
        abstract_write_response_handler(ks, cl, std::move(mutation), targets, pending_endpoints) {
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
    auto e = _response_handlers.emplace(id, rh_entry(std::move(h), [this, id] {
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
            e.handler->_ready.broken(mutation_write_timeout_error(block_for, e.handler->_cl_acks));
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

storage_proxy::response_id_type storage_proxy::create_write_response_handler(keyspace& ks, db::consistency_level cl, frozen_mutation&& mutation, std::unordered_set<gms::inet_address> targets, std::vector<gms::inet_address>& pending_endpoints)
{
    std::unique_ptr<abstract_write_response_handler> h;
    auto& rs = ks.get_replication_strategy();
    size_t pending_count = pending_endpoints.size();

    auto m = make_lw_shared<const frozen_mutation>(std::move(mutation));

    // for now make is simple
    if (db::is_datacenter_local(cl)) {
        pending_count = std::count_if(pending_endpoints.begin(), pending_endpoints.end(), db::is_local);
        h = std::make_unique<datacenter_write_response_handler>(ks, cl, std::move(m), std::move(targets), pending_count);
    } else if (cl == db::consistency_level::EACH_QUORUM &&
               rs.get_type() == locator::replication_strategy_type::network_topology){
        h = std::make_unique<datacenter_sync_write_response_handler>(ks, cl, std::move(m), std::move(targets), pending_count);
    } else {
        h = std::make_unique<write_response_handler>(ks, cl, std::move(m), std::move(targets), pending_count);
    }
    return register_response_handler(std::move(h));
}

storage_proxy::~storage_proxy() {}
storage_proxy::storage_proxy(distributed<database>& db) : _db(db) {
    init_messaging_service();
}

storage_proxy::rh_entry::rh_entry(std::unique_ptr<abstract_write_response_handler>&& h, std::function<void()>&& cb) : handler(std::move(h)), expire_timer(std::move(cb)) {}

#if 0
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
    auto pmut = make_lw_shared(std::move(mutations));
    return parallel_for_each(pmut->begin(), pmut->end(), [this, pmut] (const mutation& m) {
        return mutate_locally(m);
    }).finally([pmut]{});
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
    auto have_cl = make_lw_shared<semaphore>(0);
    auto local_addr = utils::fb_utilities::get_broadcast_address();
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    sstring local_dc = snitch_ptr->get_datacenter(local_addr);
    utils::latency_counter lc;
    lc.start();
    for (auto& m : mutations) {
        try {
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

            storage_proxy::response_id_type response_id = create_write_response_handler(ks, cl, freeze(m), std::move(live_endpoints), pending_endpoints);
            // it is better to send first and hint afterwards to reduce latency
            // but request may complete before hint_to_dead_endpoints() is called and
            // response_id handler will be removed, so we will have to do hint with separate
            // frozen_mutation copy, or manage handler live time differently.
            size_t hints = hint_to_dead_endpoints(get_write_response_handler(response_id).get_mutation(), dead_endpoints);

            if (cl == db::consistency_level::ANY) {
                // for cl==ANY hints are counted towards consistency
                get_write_response_handler(response_id).signal(hints);
            }

            // call before send_to_live_endpoints() for the same reason as above
            auto f = response_wait(response_id);
            send_to_live_endpoints(response_id, local_dc);
            f.then_wrapped([this, have_cl, response_id, cl] (future<>&& f) mutable {
                try {
                    f.get();
                    have_cl->signal();
                    return;
                } catch(mutation_write_timeout_error& ex) {
                    // timeout
                    logger.trace("Write timeout; received {} of {} required replies", ex.received, ex.to_block_for);
                    _stats.write_timeouts++;
                    have_cl->broken(ex);
                } catch(...) {
                    have_cl->broken(std::current_exception());
                }
                remove_response_handler(response_id); // cancel expire_timer, so no hint will happen
            });
        } catch (no_such_keyspace& ex) {
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
    }

    return have_cl->wait(mutations.size()).finally([this, lc]() mutable {
        _stats.write.mark(lc.stop().latency_in_nano());
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
    fail(unimplemented::cause::LWT);
#if 0
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
#endif
}

#if 0
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
#endif

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
            auto response_id_ = response_id; // make local copy since capture is reused
            auto& ms = net::get_local_messaging_service();
            return ms.send_mutation(net::messaging_service::shard_id{coordinator, 0}, m,
                std::move(forward), my_address, engine().cpu_id(), response_id_);
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
            _done_promise.set_exception(read_timeout_exception(_cl, response_count(), _targets_count, false /* FIXME */));
            on_timeout();
        });
        _timeout.arm(timeout);
    }
    virtual ~abstract_read_resolver() {};
    future<> done() {
        return _done_promise.get_future();
    }
    virtual void error(gms::inet_address ep) {
        // do nothing for now, request will timeout eventually
    }
};

class digest_read_resolver : public abstract_read_resolver {
    size_t _block_for;
    size_t _cl_responses = 0;
    promise<> _cl_promise; // cl is reached
    std::vector<foreign_ptr<lw_shared_ptr<query::result>>> _data_results;
    std::vector<query::result_digest> _digest_results;

    virtual void on_timeout() override {
        _cl_promise.set_exception(read_timeout_exception(_cl, _cl_responses, _block_for, false /* FIXME */));
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
    digest_read_resolver(db::consistency_level cl, size_t block_for, size_t targets_count, std::chrono::high_resolution_clock::time_point timeout) : abstract_read_resolver(cl, targets_count, timeout), _block_for(block_for) {}
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
        if (_cl_responses < _block_for) {
            if (waiting_for(ep)) {
                _cl_responses++;
            }
            if (_cl_responses == _block_for && _data_results.size()) {
                _cl_promise.set_value();
            }
        }
        if (_digest_results.size() == _targets_count) {
            _done_promise.set_value();
        }
    }
    future<> has_cl() {
        return _cl_promise.get_future();
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
            _data_results.emplace_back(std::move(from), std::move(result));
            if (_data_results.size() == _targets_count) {
                _done_promise.set_value();
            }
        }
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
            mutation m = boost::accumulate(v, mutation(v.front().par.mut().key(*schema), schema), [this, schema = std::move(schema)] (mutation& m, const version& ver) {
                m.partition().apply(*schema, ver.par.mut().partition());
                return m;
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

    lw_shared_ptr<query::read_command> _cmd;
    query::partition_range _partition_range;
    db::consistency_level _cl;
    size_t _block_for;
    std::vector<gms::inet_address> _targets;
    promise<foreign_ptr<lw_shared_ptr<query::result>>> _result_promise;

public:
    abstract_read_executor(lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl, size_t block_for,
            std::vector<gms::inet_address> targets) :
                           _cmd(std::move(cmd)), _partition_range(std::move(pr)), _cl(cl), _block_for(block_for), _targets(std::move(targets)) {}
    virtual ~abstract_read_executor() {};

protected:
    future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> make_mutation_data_request(gms::inet_address ep) {
        if (is_me(ep)) {
            return get_local_storage_proxy().query_mutations_locally(_cmd, _partition_range);
        } else {
            auto& ms = net::get_local_messaging_service();
            return ms.send_read_mutation_data(net::messaging_service::shard_id{ep, 0}, *_cmd, _partition_range).then([this](reconcilable_result&& result) {
                    return make_foreign(::make_lw_shared<reconcilable_result>(std::move(result)));
            });
        }
    }
    future<foreign_ptr<lw_shared_ptr<query::result>>> make_data_request(gms::inet_address ep) {
        if (is_me(ep)) {
            return get_local_storage_proxy().query_singular_local(_cmd, _partition_range);
        } else {
            auto& ms = net::get_local_messaging_service();
            return ms.send_read_data(net::messaging_service::shard_id{ep, 0}, *_cmd, _partition_range).then([this](query::result&& result) {
                return make_foreign(::make_lw_shared<query::result>(std::move(result)));
            });
        }
    }
    future<query::result_digest> make_digest_request(gms::inet_address ep) {
        if (is_me(ep)) {
            return get_local_storage_proxy().query_singular_local_digest(_cmd, _partition_range);
        } else {
            auto& ms = net::get_local_messaging_service();
            return ms.send_read_digest(net::messaging_service::shard_id{ep, 0}, *_cmd, _partition_range);
        }
    }
    future<> make_mutation_data_requests(data_resolver_ptr resolver, targets_iterator begin, targets_iterator end) {
        return parallel_for_each(begin, end, [this, resolver = std::move(resolver)] (gms::inet_address ep) {
            return make_mutation_data_request(ep).then_wrapped([resolver, ep] (future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> f) {
                try {
                    resolver->add_mutate_data(ep, f.get0());
                } catch(...) {
                    resolver->error(ep);
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
                    resolver->error(ep);
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
                    resolver->error(ep);
                }
            });
        });
    }
    virtual future<> make_requests(digest_resolver_ptr resolver) {
        return when_all(make_data_requests(resolver, _targets.begin(), _targets.begin() + 1),
                        _targets.size() > 1 ? make_digest_requests(resolver, _targets.begin() + 1, _targets.end()) : make_ready_future()).discard_result();
    }
    void reconciliate(db::consistency_level cl, std::chrono::high_resolution_clock::time_point timeout) {
        data_resolver_ptr data_resolver = ::make_shared<data_read_resolver>(cl, _targets.size(), timeout);
        auto exec = shared_from_this();

        make_mutation_data_requests(data_resolver, _targets.begin(), _targets.end()).finally([exec]{});

        data_resolver->done().then_wrapped([exec, data_resolver] (future<> f) {
            try {
                f.get();
                schema_ptr s = get_local_storage_proxy()._db.local().find_schema(exec->_cmd->cf_id);
                auto rr = data_resolver->resolve(s); // reconciliation happens here
                auto result = ::make_foreign(::make_lw_shared(to_data_query_result(std::move(rr), std::move(s), exec->_cmd->slice)));
                exec->_result_promise.set_value(std::move(result));
            } catch(read_timeout_exception& ex) {
                exec->_result_promise.set_exception(ex);
            }
        });
    }
public:
    virtual future<foreign_ptr<lw_shared_ptr<query::result>>> execute(std::chrono::high_resolution_clock::time_point timeout) {
        digest_resolver_ptr digest_resolver = ::make_shared<digest_read_resolver>(_cl, _block_for, _targets.size(), timeout);
        auto exec = shared_from_this();

        make_requests(digest_resolver).finally([exec]() {
            // hold on to executor until all queries are complete
        });

        digest_resolver->has_cl().then_wrapped([this, exec, digest_resolver, timeout] (future<> f) {
            try {
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
    never_speculating_read_executor(lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl, size_t block_for, std::vector<gms::inet_address> targets) :
                                    abstract_read_executor(std::move(cmd), std::move(pr), cl, block_for, std::move(targets)) {}
};

class always_speculating_read_executor : public abstract_read_executor {
public:
    always_speculating_read_executor(lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl, size_t block_for, std::vector<gms::inet_address> targets) :
                                    abstract_read_executor(std::move(cmd), std::move(pr), cl, block_for, std::move(targets)) {}
};

class speculating_read_executor : public abstract_read_executor {
public:
    speculating_read_executor(lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl, size_t block_for, std::vector<gms::inet_address> targets) :
                              abstract_read_executor(std::move(cmd), std::move(pr), cl, block_for, std::move(targets)) {}
};

class range_slice_read_executor : public abstract_read_executor {
public:
    range_slice_read_executor(lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl, std::vector<gms::inet_address> targets) :
                                    abstract_read_executor(std::move(cmd), std::move(pr), cl, targets.size(), std::move(targets)) {}
    virtual future<foreign_ptr<lw_shared_ptr<query::result>>> execute(std::chrono::high_resolution_clock::time_point timeout) override {
        reconciliate(_cl, timeout);
        return _result_promise.get_future();
    }
};

::shared_ptr<abstract_read_executor> storage_proxy::get_read_executor(lw_shared_ptr<query::read_command> cmd, query::partition_range pr, db::consistency_level cl) {
    const dht::token& token = pr.start()->value().token();
    schema_ptr schema = _db.local().find_schema(cmd->cf_id);
    keyspace& ks = _db.local().find_keyspace(schema->ks_name());

    std::vector<gms::inet_address> all_replicas = get_live_sorted_endpoints(ks, token);
    db::read_repair_decision repair_decision = db::read_repair_decision::NONE;//Schema.instance.getCFMetaData(command.ksName, command.cfName).newReadRepairDecision();
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
    speculative_retry::type retry_type = speculative_retry::type::NONE;//cfs.metadata.getSpeculativeRetry().type;

    size_t block_for = db::block_for(ks, cl);
    // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
    if (retry_type == speculative_retry::type::NONE || db::block_for(ks, cl) == all_replicas.size()) {
        return ::make_shared<never_speculating_read_executor>(cmd, std::move(pr), cl, block_for, std::move(target_replicas));
    }

    if (target_replicas.size() == all_replicas.size()) {
        // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
        // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
        // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
        return ::make_shared<always_speculating_read_executor>(/*cfs, */cmd, std::move(pr), cl, block_for, std::move(target_replicas));
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
        return ::make_shared<always_speculating_read_executor>(/*cfs,*/cmd, std::move(pr), cl, block_for, std::move(target_replicas));
    } else {// PERCENTILE or CUSTOM.
        return ::make_shared<speculating_read_executor>(/*cfs,*/cmd, std::move(pr), cl, block_for, std::move(target_replicas));
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

    auto f = ::map_reduce(exec.begin(), exec.end(), [this, cmd, timeout] (::shared_ptr<abstract_read_executor>& rex) {
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
        exec.push_back(::make_shared<range_slice_read_executor>(cmd, std::move(range), cl, std::move(filtered_endpoints)));
    }

    query::result_merger merger;
    merger.reserve(exec.size());

    auto f = ::map_reduce(exec.begin(), exec.end(), [this, cmd, timeout] (::shared_ptr<abstract_read_executor>& rex) {
        return rex->execute(timeout);
    }, std::move(merger));

    return f.then([this, exec = std::move(exec), results = std::move(results), i = std::move(i), ranges = std::move(ranges), cl, cmd, concurrency_factor, timeout]
                   (foreign_ptr<lw_shared_ptr<query::result>>&& result) mutable {
        results.emplace_back(std::move(result));
        if (i == ranges.end()) {
            return make_ready_future<std::vector<foreign_ptr<lw_shared_ptr<query::result>>>>(std::move(results));
        } else {
            return query_partition_key_range_concurrent(timeout, std::move(results), cmd, cl, std::move(i), std::move(ranges), concurrency_factor);
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
        return do_query(s, cmd, std::move(partition_ranges), cl).then([query_id, cmd, s] (auto&& res) {
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

    if (partition_ranges[0].is_singular() && partition_ranges[0].start()->value().has_key()) { // do not support mixed partitions (yet?)
        try {
            return query_singular(cmd, std::move(partition_ranges), cl);
        } catch (const no_such_column_family&) {
            return make_empty();
        }
    }

    if (partition_ranges.size() != 1) {
        // FIXME: implement
        throw std::runtime_error("more than one non singular range not supported yet");
    }

    return query_partition_key_range(cmd, std::move(partition_ranges[0]), cl);
}

#if 0
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

#if 0
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
#endif

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
    ms.register_definitions_update( [this] (std::vector<frozen_mutation> m) {
        do_with(std::move(m), [this] (const std::vector<frozen_mutation>& mutations) mutable {
            std::vector<mutation> schema;
            for (auto& m : mutations) {
                schema_ptr s = get_db().local().find_schema(m.column_family_id());
                schema.emplace_back(m.unfreeze(s));
            }
            return db::schema_tables::merge_schema(*this, schema);
        }).discard_result();
        return net::messaging_service::no_wait();
    });
    ms.register_migration_request([this] (gms::inet_address reply_to, unsigned shard) {
        return db::schema_tables::convert_schema_to_mutations(*this);
    });
    ms.register_mutation([this] (frozen_mutation in, std::vector<gms::inet_address> forward, gms::inet_address reply_to, unsigned shard, storage_proxy::response_id_type response_id) {
        do_with(std::move(in), [this, forward = std::move(forward), reply_to, shard, response_id] (const frozen_mutation& m) mutable {
            return when_all(
                    mutate_locally(m).then([reply_to, shard, response_id] () mutable {
                auto& ms = net::get_local_messaging_service();
                ms.send_mutation_done(net::messaging_service::shard_id{reply_to, shard}, shard, response_id);
                // return void, no need to wait for send to complete
            }),
            parallel_for_each(forward.begin(), forward.end(), [reply_to, shard, response_id, &m] (gms::inet_address forward) {
                auto& ms = net::get_local_messaging_service();
                // we need to get copy of all captured element since lambda is used more than one, so we cannot move from it
                // we cannot pass references to send_message either since lambda may be destroyed before send completes
                auto reply_to_ = reply_to;
                auto shard_ = shard;
                auto response_id_ = response_id;
                return ms.send_mutation(net::messaging_service::shard_id{forward, 0}, m, {}, reply_to_, shard_, response_id_);
            })
            );
        }).discard_result();
        return net::messaging_service::no_wait();
    });
    ms.register_mutation_done([this] (rpc::client_info cinfo, unsigned shard, storage_proxy::response_id_type response_id) {
        gms::inet_address from(net::ntoh(cinfo.addr.as_posix_sockaddr_in().sin_addr.s_addr));
        get_storage_proxy().invoke_on(shard, [from, response_id] (storage_proxy& sp) {
            sp.got_response(response_id, from);
        });
        return net::messaging_service::no_wait();
    });
    ms.register_read_data([this] (query::read_command cmd, query::partition_range pr) {
        return do_with(std::move(pr), [this, cmd = make_lw_shared<query::read_command>(std::move(cmd))] (const query::partition_range& pr) {
            return query_singular_local(cmd, pr);
        });
    });
    ms.register_read_mutation_data([this] (query::read_command cmd, query::partition_range pr) {
        return do_with(std::move(pr), [this, cmd = make_lw_shared<query::read_command>(std::move(cmd))] (const query::partition_range& pr) {
            return query_mutations_locally(cmd, pr);
        });
    });
    ms.register_read_digest([this] (query::read_command cmd, query::partition_range pr) {
        return do_with(std::move(pr), [this, cmd = make_lw_shared<query::read_command>(std::move(cmd))] (const query::partition_range& pr) {
            return query_singular_local_digest(cmd, pr);
        });
    });
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

    uint32_t _limit;
    schema_ptr _schema;
    std::vector<partition_run> _runs;
public:
    mutation_result_merger(uint32_t limit, schema_ptr schema)
        : _limit(limit)
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
            partitions.push_back(p);
            row_count += p._row_count;
            if (row_count >= _limit) {
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
        return _db.map_reduce(mutation_result_merger{cmd->row_limit, schema}, [cmd, &pr] (database& db) {
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
    return make_ready_future<>();
}

class shard_reader {
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

    future<mutation_opt> operator()() {
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
    unsigned first_shard = range.start() ? dht::shard_of(range.start()->value().token()) : 0;
    unsigned last_shard = range.end() ? dht::shard_of(range.end()->value().token()) : smp::count - 1;
    std::vector<mutation_reader> readers;
    for (auto cpu = first_shard; cpu <= last_shard; ++cpu) {
        readers.emplace_back([reader = make_lw_shared<shard_reader>(cf_id, _db, cpu, range)] () mutable {
            return (*reader)();
        });
    }
    return make_joining_reader(std::move(readers));
}

}
